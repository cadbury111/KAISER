"""KAISER — Investigation Reporting & Evidence Review Audit Service.

Coordinates the deterministic generation, validation, persistence, and querying
of analytical investigation reports based on completed investigation sessions.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support only: NEVER outputs BUY, SELL, HOLD, or trading execution instructions.
2. Complete provenance & lineage: Every report traces back through session, priority,
   candidate, evidence synthesis, and analysis runs.
3. Strict separation: Distinguishes Machine Generated Evidence, Human Review, and Derived Analysis.
4. Epistemological enforcement: FACT observations must cite verified evidence references.
5. Numerical safety: Rejects NaN, Infinity, -Infinity; preserves genuine 0 and 0.0.
6. Immutability & Revisions: Reports are immutable once generated. New sessions/revisions
   supersede older reports without destroying historical traceability.
7. Thread-safe registry: Protected by re-entrant mutex locks.
"""

from __future__ import annotations

import collections
import copy
import hashlib
import json
import logging
import math
import re
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.opportunities.investigation.models import (
    AnalyticalConclusion,
    ConclusionState,
    EvidenceReviewDisposition,
    EvidenceReviewRecord,
    InvestigationAuditRecord,
    InvestigationLimitation,
    InvestigationObservation,
    InvestigationReviewNote,
    InvestigationSession,
    InvestigationUncertainty,
    ObservationType,
    SessionState,
    SourceType,
    VALID_SESSION_TRANSITIONS,
    sanitize_reviewer_id,
)
from app.analysis.opportunities.investigation.reporting.models import (
    DEFAULT_MAX_REPORT_HISTORY,
    AttributionType,
    AuditSummary,
    EvidenceMatrixEntry,
    InvestigationReport,
    InvestigationReportVoiceSummary,
    ObservationMatrixEntry,
    ReportStatus,
    ReproducibilityRecord,
    compute_deterministic_report_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.models import (
    ConfidenceState,
    OpportunityCandidate,
    OpportunityState,
)
from app.analysis.opportunities.prioritization.models import (
    OpportunityPriorityRecord,
    PriorityState,
)
from app.analysis.evidence.models import (
    EvidenceItem,
    EvidenceStatus,
    EvidenceSynthesisReport,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import (
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)

logger = logging.getLogger("kaiser.analysis.opportunities.investigation.reporting.service")


class InvestigationReportingService:
    """Thread-safe service generating and managing immutable investigation reports."""

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        max_history: int = DEFAULT_MAX_REPORT_HISTORY,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        """Initialize the investigation reporting service.

        Args:
            event_bus: Optional EventBus for telemetry notifications.
            state_store: Optional StateStore for state persistence.
            max_history: Maximum number of historical reports retained in memory.
            clock: Time provider function (defaults to time.time).
        """
        self.event_bus = event_bus
        self.state_store = state_store
        self.max_history = max(1, min(1000, max_history))
        self.clock = clock or time.time

        self._lock = threading.RLock()
        self._reports: Dict[str, InvestigationReport] = {}
        self._report_order: List[str] = []
        self._session_to_reports: Dict[str, List[str]] = collections.defaultdict(list)
        self._candidate_to_reports: Dict[str, List[str]] = collections.defaultdict(list)

    def generate_report(
        self,
        session: InvestigationSession,
        candidate: OpportunityCandidate,
        priority_record: Optional[OpportunityPriorityRecord] = None,
        evidence_report: Optional[EvidenceSynthesisReport] = None,
        audit_trail: Optional[Sequence[InvestigationAuditRecord]] = None,
        reviewer_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> InvestigationReport:
        """Generate a structured, traceable, reproducible analytical investigation report.

        Args:
            session: Source InvestigationSession.
            candidate: OpportunityCandidate being evaluated.
            priority_record: Optional OpportunityPriorityRecord for prioritization context.
            evidence_report: Optional EvidenceSynthesisReport for upstream evidence items.
            audit_trail: Optional sequence of InvestigationAuditRecord instances.
            reviewer_id: Optional analyst identifier override.
            metadata: Optional additional metadata dict.

        Returns:
            Immutable InvestigationReport.

        Raises:
            ValidationError: If input validation, linkage, numerical bounds, or safety rules fail.
        """
        now = self.clock()

        # Step 1: Input Validation
        if session is None:
            raise ValidationError("Source investigation session must be provided.")
        if candidate is None:
            raise ValidationError("Opportunity candidate must be provided.")

        clean_meta = metadata or {}
        validate_no_prohibited_fields(clean_meta, path="metadata")

        # Validate Candidate Linkage
        cand_id = getattr(candidate, "candidate_id", None)
        if not cand_id:
            raise ValidationError("Candidate candidate_id must be a non-empty string.")
        if session.candidate_id != cand_id:
            raise ValidationError(
                f"Candidate linkage mismatch: Session candidate_id '{session.candidate_id}' "
                f"does not match candidate '{cand_id}'."
            )

        # Validate Priority Linkage if provided
        prio_id = getattr(session, "priority_id", None)
        if priority_record is not None:
            pr_cand_id = getattr(priority_record, "candidate_id", None)
            pr_prio_id = getattr(priority_record, "priority_id", None)
            if pr_cand_id and pr_cand_id != cand_id:
                raise ValidationError(
                    f"Candidate linkage mismatch on priority record: "
                    f"Priority record candidate_id '{pr_cand_id}' does not match candidate '{cand_id}'."
                )
            if prio_id and pr_prio_id and prio_id != pr_prio_id:
                raise ValidationError(
                    f"Priority linkage mismatch: Session priority_id '{prio_id}' "
                    f"does not match priority record '{pr_prio_id}'."
                )
            if not prio_id and pr_prio_id:
                prio_id = pr_prio_id

        # Determine effective reviewer
        effective_reviewer = sanitize_reviewer_id(reviewer_id or session.reviewer_id)

        # Step 2: Audit Integrity & Summary Verification
        audit_summary = self._verify_audit_trail(session=session, audit_trail=audit_trail)

        # Step 3: Determine Initial Report Status
        initial_status = ReportStatus.GENERATED
        if session.status == SessionState.INVALIDATED:
            initial_status = ReportStatus.INVALID
        elif session.status == SessionState.CANCELLED:
            initial_status = ReportStatus.INCOMPLETE
        elif not audit_summary.sequence_integrity:
            initial_status = ReportStatus.INVALID
        elif not audit_summary.audit_completeness:
            initial_status = ReportStatus.PARTIAL
        elif session.analytical_conclusion is None:
            initial_status = ReportStatus.INCOMPLETE
        elif session.status != SessionState.REVIEW_COMPLETE:
            # Session is still ACTIVE, PAUSED, or REQUIRES_MORE_EVIDENCE
            initial_status = ReportStatus.INCOMPLETE
        elif evidence_report is None:
            # Report exists and session is complete, but optional upstream evidence synthesis report is unavailable
            initial_status = ReportStatus.PARTIAL

        # Step 4: Build Summaries with Explicit Attribution
        cand_summary = self._build_candidate_summary(candidate)
        prio_summary = self._build_priority_summary(priority_record) if priority_record else None
        evidence_summary = self._build_evidence_summary(candidate=candidate, evidence_report=evidence_report)

        # Step 5: Build Structured Matrices
        evidence_matrix, supporting_refs, conflicting_refs, missing_refs = self._build_evidence_matrix(
            session=session,
            candidate=candidate,
            evidence_report=evidence_report,
        )
        observation_matrix, uncertainties, limitations = self._build_observation_matrix(session=session)

        # Step 6: Conclusion Preservation
        conclusion_dict = None
        if session.analytical_conclusion:
            conclusion_dict = session.analytical_conclusion.to_dict()
            conclusion_dict["attribution"] = AttributionType.HUMAN_REVIEW.value

        # Step 7: Reproducibility Record Construction
        reproducibility_record = self._build_reproducibility_record(
            session=session,
            candidate=candidate,
            priority_record=priority_record,
            evidence_report=evidence_report,
            generated_at=now,
        )

        # Step 8: Provenance Lineage
        report_provenance = {
            "session_id": session.session_id,
            "session_status": session.status.value,
            "candidate_id": cand_id,
            "priority_id": prio_id,
            "source_evidence_report_id": getattr(candidate, "source_evidence_report_id", None) or session.source_report_id,
            "reproducibility_id": session.reproducibility_id or getattr(candidate, "reproducibility_id", None),
            "generated_at": now,
            "attribution": AttributionType.DERIVED_ANALYSIS.value,
        }

        # Step 9: Report Versioning & Deterministic ID
        version = session.revision
        parent_report_id = None
        parent_session_id = session.parent_session_id

        report_id = compute_deterministic_report_id(
            session_id=session.session_id,
            candidate_id=cand_id,
            priority_id=prio_id,
            source_report_id=session.source_report_id or getattr(candidate, "source_evidence_report_id", None),
            reproducibility_id=reproducibility_record.reproducibility_id,
            version=version,
        )

        # Step 10: Speech-Ready Voice Summary
        voice_summary = self._build_voice_summary(
            report_id=report_id,
            candidate_symbol=cand_summary.get("symbol", "UNKNOWN"),
            report_status=initial_status,
            conclusion=session.analytical_conclusion,
            supporting_count=len(supporting_refs),
            conflicting_count=len(conflicting_refs),
            limitation_count=len(limitations),
        )

        # Step 11: Construct Report Model
        report = InvestigationReport(
            report_id=report_id,
            session_id=session.session_id,
            candidate_id=cand_id,
            priority_id=prio_id,
            report_status=initial_status,
            generated_at=now,
            reviewer_id=effective_reviewer,
            candidate_summary=cand_summary,
            priority_summary=prio_summary,
            evidence_summary=evidence_summary,
            supporting_evidence=supporting_refs,
            conflicting_evidence=conflicting_refs,
            missing_evidence=missing_refs,
            uncertainty_summary=uncertainties,
            limitation_summary=limitations,
            observations=observation_matrix,
            evidence_matrix=evidence_matrix,
            analytical_conclusion=conclusion_dict,
            audit_summary=audit_summary,
            reproducibility_record=reproducibility_record,
            source_report_id=session.source_report_id or getattr(candidate, "source_evidence_report_id", None),
            source_analysis_run_ids=tuple(session.source_analysis_run_ids or getattr(candidate, "source_analysis_run_ids", ())),
            reproducibility_id=reproducibility_record.reproducibility_id,
            report_provenance=report_provenance,
            version=version,
            parent_report_id=parent_report_id,
            parent_session_id=parent_session_id,
            voice_summary=voice_summary,
            metadata=clean_meta,
        )

        # Step 12: Thread-Safe Registration & Superseding Management
        with self._lock:
            # Check for existing reports for this session or candidate lineage
            existing_session_report_ids = self._session_to_reports.get(session.session_id, [])
            superseded_reports: List[InvestigationReport] = []

            # Check if this exact report ID already exists
            if report_id in self._reports:
                existing = self._reports[report_id]
                # Return existing report if identical and already stored
                return existing

            # If there's an older report for this session or parent session, supersede it
            for old_id in list(existing_session_report_ids):
                old_rep = self._reports.get(old_id)
                if old_rep and old_rep.report_status != ReportStatus.SUPERSEDED and old_rep.version < version:
                    superseded = self._supersede_report_locked(old_rep)
                    superseded_reports.append(superseded)
                    parent_report_id = old_rep.report_id

            if parent_session_id:
                for parent_rep_id in self._session_to_reports.get(parent_session_id, []):
                    old_rep = self._reports.get(parent_rep_id)
                    if old_rep and old_rep.report_status != ReportStatus.SUPERSEDED:
                        superseded = self._supersede_report_locked(old_rep)
                        superseded_reports.append(superseded)
                        if not parent_report_id:
                            parent_report_id = old_rep.report_id

            if parent_report_id and report.parent_report_id != parent_report_id:
                object.__setattr__(report, "parent_report_id", parent_report_id)

            # Store new report
            self._reports[report_id] = report
            self._report_order.append(report_id)
            self._session_to_reports[session.session_id].append(report_id)
            self._candidate_to_reports[cand_id].append(report_id)

            # Bounded retention pruning
            if len(self._report_order) > self.max_history:
                evicted_id = self._report_order.pop(0)
                # Keep in _reports if needed or prune safely
                if evicted_id in self._reports and evicted_id != report_id:
                    evicted_rep = self._reports.pop(evicted_id, None)
                    if evicted_rep:
                        if evicted_id in self._session_to_reports[evicted_rep.session_id]:
                            self._session_to_reports[evicted_rep.session_id].remove(evicted_id)
                        if evicted_id in self._candidate_to_reports[evicted_rep.candidate_id]:
                            self._candidate_to_reports[evicted_rep.candidate_id].remove(evicted_id)

            # Persistence to StateStore
            self._persist_state(report)

            # EventBus notifications
            self._dispatch_report_events(report, superseded_reports)

        return report

    def _supersede_report_locked(self, old_report: InvestigationReport) -> InvestigationReport:
        """Mark an older report as SUPERSEDED while holding the lock."""
        updated = InvestigationReport(
            report_id=old_report.report_id,
            session_id=old_report.session_id,
            candidate_id=old_report.candidate_id,
            priority_id=old_report.priority_id,
            report_status=ReportStatus.SUPERSEDED,
            generated_at=old_report.generated_at,
            reviewer_id=old_report.reviewer_id,
            candidate_summary=old_report.candidate_summary,
            priority_summary=old_report.priority_summary,
            evidence_summary=old_report.evidence_summary,
            supporting_evidence=old_report.supporting_evidence,
            conflicting_evidence=old_report.conflicting_evidence,
            missing_evidence=old_report.missing_evidence,
            uncertainty_summary=old_report.uncertainty_summary,
            limitation_summary=old_report.limitation_summary,
            observations=old_report.observations,
            evidence_matrix=old_report.evidence_matrix,
            analytical_conclusion=old_report.analytical_conclusion,
            audit_summary=old_report.audit_summary,
            reproducibility_record=old_report.reproducibility_record,
            source_report_id=old_report.source_report_id,
            source_analysis_run_ids=old_report.source_analysis_run_ids,
            reproducibility_id=old_report.reproducibility_id,
            report_provenance=old_report.report_provenance,
            version=old_report.version,
            parent_report_id=old_report.parent_report_id,
            parent_session_id=old_report.parent_session_id,
            voice_summary=old_report.voice_summary,
            metadata=old_report.metadata,
        )
        self._reports[old_report.report_id] = updated
        return updated

    def _verify_audit_trail(
        self,
        session: InvestigationSession,
        audit_trail: Optional[Sequence[InvestigationAuditRecord]],
    ) -> AuditSummary:
        """Verify the integrity, chronology, and transitions of the session audit trail."""
        if audit_trail is None:
            # If not provided, construct empty/partial audit summary
            return AuditSummary(
                total_events=0,
                evidence_review_events=len(session.evidence_reviews),
                observations_added=len(session.observations),
                conclusion_changes=1 if session.analytical_conclusion else 0,
                session_state_transitions=0,
                reviewer_identifiers=(session.reviewer_id,) if session.reviewer_id else (),
                first_event_timestamp=session.created_at,
                last_event_timestamp=session.updated_at or session.created_at,
                sequence_integrity=True,
                audit_completeness=False,
                integrity_issues=("Detailed audit trail records not supplied; summary inferred from session collections.",),
            )

        audits = list(audit_trail)
        if not audits:
            return AuditSummary(
                total_events=0,
                evidence_review_events=len(session.evidence_reviews),
                observations_added=len(session.observations),
                conclusion_changes=1 if session.analytical_conclusion else 0,
                session_state_transitions=0,
                reviewer_identifiers=(session.reviewer_id,) if session.reviewer_id else (),
                first_event_timestamp=session.created_at,
                last_event_timestamp=session.updated_at or session.created_at,
                sequence_integrity=True,
                audit_completeness=False,
                integrity_issues=("Audit trail records list is empty.",),
            )

        issues: List[str] = []
        review_count = 0
        observation_count = 0
        conclusion_changes = 0
        state_transitions = 0
        reviewers: Set[str] = set()
        seen_audit_ids: Set[str] = set()
        prev_ts = 0.0

        for idx, rec in enumerate(audits):
            # 1. Structural check
            if not getattr(rec, "audit_id", None) or not isinstance(rec.audit_id, str):
                issues.append(f"Audit record at index {idx} has invalid or missing audit_id.")
            elif rec.audit_id in seen_audit_ids:
                issues.append(f"Duplicate audit_id '{rec.audit_id}' detected at index {idx}.")
            else:
                seen_audit_ids.add(rec.audit_id)

            # 2. Session ID check
            if getattr(rec, "session_id", None) != session.session_id:
                issues.append(
                    f"Audit record at index {idx} session_id mismatch: '{rec.session_id}' != '{session.session_id}'."
                )

            # 3. Timestamp sanity and monotonicity
            ts = getattr(rec, "timestamp", None)
            if ts is None or not isinstance(ts, (int, float)) or math.isnan(ts) or math.isinf(ts) or ts < 0.0:
                issues.append(f"Audit record at index {idx} has invalid timestamp: {ts}.")
            else:
                if ts < prev_ts - 1.0:  # Allow 1s tolerance for distributed clock skew
                    issues.append(
                        f"Audit timestamp backwards jump at index {idx}: {ts} < previous {prev_ts}."
                    )
                prev_ts = max(prev_ts, ts)

            # 4. Reviewer tracking
            r_id = getattr(rec, "reviewer_id", None)
            if r_id:
                reviewers.add(str(r_id))

            # 5. Event counting
            ev_type = str(getattr(rec, "event_type", "")).upper()
            if "EVIDENCE_REVIEWED" in ev_type:
                review_count += 1
            elif "OBSERVATION_ADDED" in ev_type:
                observation_count += 1
            elif "CONCLUSION_SET" in ev_type or "SESSION_COMPLETED" in ev_type:
                conclusion_changes += 1

            # 6. State transition integrity
            prev_st = getattr(rec, "previous_state", None)
            new_st = getattr(rec, "new_state", None)
            if prev_st is not None and new_st is not None and prev_st != new_st:
                state_transitions += 1
                try:
                    p_state = SessionState(prev_st)
                    n_state = SessionState(new_st)
                    valid_next = VALID_SESSION_TRANSITIONS.get(p_state, set())
                    if n_state not in valid_next:
                        issues.append(
                            f"Invalid state transition at index {idx}: '{prev_st}' -> '{new_st}'."
                        )
                except ValueError:
                    issues.append(f"Unrecognized state names in transition: '{prev_st}' -> '{new_st}'.")

        first_ts = audits[0].timestamp if audits else None
        last_ts = audits[-1].timestamp if audits else None

        return AuditSummary(
            total_events=len(audits),
            evidence_review_events=review_count,
            observations_added=observation_count,
            conclusion_changes=conclusion_changes,
            session_state_transitions=state_transitions,
            reviewer_identifiers=tuple(sorted(reviewers)),
            first_event_timestamp=first_ts,
            last_event_timestamp=last_ts,
            sequence_integrity=len(issues) == 0,
            audit_completeness=True,
            integrity_issues=tuple(issues),
        )

    def _build_candidate_summary(self, candidate: OpportunityCandidate) -> Dict[str, Any]:
        """Build machine-generated candidate summary."""
        return {
            "candidate_id": getattr(candidate, "candidate_id", ""),
            "symbol": getattr(candidate, "symbol", "UNKNOWN"),
            "timeframe": getattr(candidate, "timeframe", "UNKNOWN"),
            "assessment_state": (
                candidate.assessment_state.value
                if hasattr(candidate.assessment_state, "value")
                else str(candidate.assessment_state)
            ),
            "evidence_status": (
                candidate.evidence_status.value
                if hasattr(candidate.evidence_status, "value")
                else str(candidate.evidence_status)
            ),
            "evidence_completeness_score": getattr(candidate, "evidence_completeness_score", 0.0),
            "confidence_state": (
                candidate.confidence_state.value
                if hasattr(candidate.confidence_state, "value")
                else str(candidate.confidence_state)
            ),
            "data_quality_status": (
                candidate.data_quality_status.value
                if candidate.data_quality_status and hasattr(candidate.data_quality_status, "value")
                else (str(candidate.data_quality_status) if candidate.data_quality_status else None)
            ),
            "provenance_status": getattr(candidate, "provenance_status", "UNVERIFIED"),
            "reproducibility_status": getattr(candidate, "reproducibility_status", "UNVERIFIED"),
            "source_evidence_report_id": getattr(candidate, "source_evidence_report_id", None),
            "generated_at": getattr(candidate, "generated_at", 0.0),
            "attribution": AttributionType.MACHINE_GENERATED_EVIDENCE.value,
        }

    def _build_priority_summary(self, priority: OpportunityPriorityRecord) -> Dict[str, Any]:
        """Build machine-generated priority summary."""
        return {
            "priority_id": getattr(priority, "priority_id", ""),
            "investigation_priority": getattr(priority, "investigation_priority", 1),
            "priority_state": (
                priority.priority_state.value
                if hasattr(priority.priority_state, "value")
                else str(priority.priority_state)
            ),
            "evidence_priority_score": getattr(priority, "evidence_priority_score", 0.0),
            "evidence_completeness": getattr(priority, "evidence_completeness", 0.0),
            "priority_reasons": list(getattr(priority, "priority_reasons", ())),
            "deprioritization_reasons": list(getattr(priority, "deprioritization_reasons", ())),
            "source_report_id": getattr(priority, "source_report_id", ""),
            "attribution": AttributionType.MACHINE_GENERATED_EVIDENCE.value,
        }

    def _build_evidence_summary(
        self,
        candidate: OpportunityCandidate,
        evidence_report: Optional[EvidenceSynthesisReport],
    ) -> Dict[str, Any]:
        """Build structured evidence summary combining candidate and report."""
        summary: Dict[str, Any] = {
            "supporting_evidence_count": len(getattr(candidate, "supporting_evidence_refs", ())),
            "conflicting_evidence_count": len(getattr(candidate, "conflicting_evidence_refs", ())),
            "uncertainty_count": len(getattr(candidate, "uncertainty_refs", ())),
            "limitation_count": len(getattr(candidate, "limitation_refs", ())),
            "key_observations_count": len(getattr(candidate, "key_observations", ())),
            "attribution": AttributionType.MACHINE_GENERATED_EVIDENCE.value,
        }
        if evidence_report is not None:
            summary["upstream_report_id"] = getattr(evidence_report, "report_id", None)
            summary["total_evidence_items"] = len(getattr(evidence_report, "evidence_items", ()))
            summary["data_quality_summary"] = getattr(evidence_report, "data_quality_summary", {})
            summary["provenance_summary"] = getattr(evidence_report, "provenance_summary", {})
            summary["risk_summary"] = getattr(evidence_report, "risk_summary", {})
        return summary

    def _build_evidence_matrix(
        self,
        session: InvestigationSession,
        candidate: OpportunityCandidate,
        evidence_report: Optional[EvidenceSynthesisReport],
    ) -> Tuple[Tuple[EvidenceMatrixEntry, ...], Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]:
        """Construct structured evidence matrix preserving human review dispositions."""
        entries: List[EvidenceMatrixEntry] = []
        supporting_refs: List[str] = []
        conflicting_refs: List[str] = []
        missing_refs: List[str] = []

        # Map human review evaluations by evidence ID
        human_evals: Dict[str, EvidenceReviewRecord] = {
            rev.evidence_id: rev for rev in session.evidence_reviews
        }

        # Map upstream evidence items if available
        upstream_items: Dict[str, EvidenceItem] = {}
        if evidence_report and hasattr(evidence_report, "evidence_items"):
            for itm in evidence_report.evidence_items:
                upstream_items[itm.evidence_id] = itm

        # Collect all candidate and session evidence IDs
        cand_supp = set(getattr(candidate, "supporting_evidence_refs", ()))
        cand_conf = set(getattr(candidate, "conflicting_evidence_refs", ()))
        cand_uncert = set(getattr(candidate, "uncertainty_refs", ()))
        cand_limit = set(getattr(candidate, "limitation_refs", ()))

        all_evidence_ids: List[str] = []
        seen: Set[str] = set()
        for eid in list(session.known_evidence_ids) + list(human_evals.keys()) + list(cand_supp) + list(cand_conf) + list(upstream_items.keys()):
            s_eid = str(eid).strip()
            if s_eid and s_eid not in seen:
                seen.add(s_eid)
                all_evidence_ids.append(s_eid)

        # Build each matrix row
        for eid in all_evidence_ids:
            up_item = upstream_items.get(eid)
            h_rev = human_evals.get(eid)

            # Determine type, status, and source
            if up_item:
                ev_type = up_item.evidence_type.value if hasattr(up_item.evidence_type, "value") else str(up_item.evidence_type)
                ev_status = up_item.status.value if hasattr(up_item.status, "value") else str(up_item.status)
                ev_source = up_item.source_reference
                prov_ref = up_item.provenance_id
                qual_ref = up_item.quality_assessment_id
            else:
                if eid in cand_supp:
                    ev_type = "OPPORTUNITY_SUPPORTING"
                    ev_status = "AVAILABLE"
                elif eid in cand_conf:
                    ev_type = "OPPORTUNITY_CONFLICTING"
                    ev_status = "CONFLICTING"
                elif eid in cand_uncert:
                    ev_type = "UNCERTAINTY"
                    ev_status = "PARTIAL"
                elif eid in cand_limit:
                    ev_type = "LIMITATION"
                    ev_status = "AVAILABLE"
                else:
                    ev_type = "ANALYTICAL_EVIDENCE"
                    ev_status = "AVAILABLE"
                ev_source = "OPPORTUNITY_PIPELINE"
                prov_ref = None
                qual_ref = None

            # Determine reviewer disposition
            if h_rev:
                disp = h_rev.disposition.value if hasattr(h_rev.disposition, "value") else str(h_rev.disposition)
                notes = h_rev.notes
            else:
                if eid in cand_conf:
                    disp = EvidenceReviewDisposition.CONFLICTING.value
                elif eid in cand_supp:
                    disp = EvidenceReviewDisposition.SUPPORTING.value
                elif ev_status == "UNAVAILABLE":
                    disp = EvidenceReviewDisposition.MISSING.value
                else:
                    disp = EvidenceReviewDisposition.REVIEWED.value
                notes = None

            # Collect observations citing this evidence ID
            obs_refs = tuple(
                o.observation_id for o in session.observations if eid in o.evidence_refs
            )

            # Categorize into supporting / conflicting / missing lists
            if disp == EvidenceReviewDisposition.SUPPORTING.value:
                supporting_refs.append(eid)
            elif disp == EvidenceReviewDisposition.CONFLICTING.value:
                conflicting_refs.append(eid)
            elif disp == EvidenceReviewDisposition.MISSING.value:
                missing_refs.append(eid)

            entry = EvidenceMatrixEntry(
                evidence_id=eid,
                evidence_type=ev_type,
                status=ev_status,
                source=ev_source,
                reviewer_disposition=disp,
                observation_refs=obs_refs,
                conflict_refs=(eid,) if disp == EvidenceReviewDisposition.CONFLICTING.value else (),
                provenance_ref=prov_ref,
                quality_ref=qual_ref,
                notes=notes,
                attribution=AttributionType.MACHINE_GENERATED_EVIDENCE.value if not h_rev else AttributionType.HUMAN_REVIEW.value,
            )
            entries.append(entry)

        return tuple(entries), tuple(supporting_refs), tuple(conflicting_refs), tuple(missing_refs)

    def _build_observation_matrix(
        self, session: InvestigationSession
    ) -> Tuple[Tuple[ObservationMatrixEntry, ...], Tuple[str, ...], Tuple[str, ...]]:
        """Construct observation matrix preserving FACT vs INTERPRETATION separation."""
        entries: List[ObservationMatrixEntry] = []
        uncertainties: List[str] = [u.description for u in session.uncertainties]
        limitations: List[str] = [l.description for l in session.limitations]

        for obs in session.observations:
            obs_type_val = obs.observation_type.value if hasattr(obs.observation_type, "value") else str(obs.observation_type)
            entries.append(
                ObservationMatrixEntry(
                    observation_id=obs.observation_id,
                    observation_type=obs_type_val,
                    statement=obs.statement,
                    evidence_refs=obs.evidence_refs,
                    author_id=obs.reviewer_id,
                    source_type=AttributionType.HUMAN_REVIEW.value,
                    timestamp=obs.created_at,
                )
            )

        return tuple(entries), tuple(uncertainties), tuple(limitations)

    def _build_reproducibility_record(
        self,
        session: InvestigationSession,
        candidate: OpportunityCandidate,
        priority_record: Optional[OpportunityPriorityRecord],
        evidence_report: Optional[EvidenceSynthesisReport],
        generated_at: float,
    ) -> ReproducibilityRecord:
        """Construct reproducibility record with provenance linkage."""
        source_runs = tuple(
            session.source_analysis_run_ids
            or getattr(candidate, "source_analysis_run_ids", ())
            or (getattr(evidence_report, "source_run_ids", ()) if evidence_report else ())
        )
        repro_id = (
            session.reproducibility_id
            or getattr(candidate, "reproducibility_id", None)
            or (getattr(priority_record, "reproducibility_id", None) if priority_record else None)
        )
        source_rep_id = (
            session.source_report_id
            or getattr(candidate, "source_evidence_report_id", None)
            or (getattr(evidence_report, "report_id", None) if evidence_report else None)
        )

        notes: List[str] = []
        is_reproducible = True

        if not source_runs:
            notes.append("Source analysis run IDs not recorded; empirical re-execution unverified.")
            is_reproducible = False
        if not repro_id:
            notes.append("Reproducibility fingerprint ID unavailable from upstream pipeline.")
            is_reproducible = False
        if not source_rep_id:
            notes.append("Source evidence synthesis report ID missing from candidate lineage.")
            is_reproducible = False

        config_fingerprint = f"cfg_{hashlib.sha256(str(source_runs).encode()).hexdigest()[:12]}" if source_runs else None

        return ReproducibilityRecord(
            source_analysis_run_ids=source_runs,
            reproducibility_id=repro_id,
            source_report_id=source_rep_id,
            candidate_id=getattr(candidate, "candidate_id", session.candidate_id),
            priority_id=session.priority_id or (getattr(priority_record, "priority_id", None) if priority_record else None),
            session_id=session.session_id,
            generated_at=generated_at,
            config_fingerprint=config_fingerprint,
            is_reproducible=is_reproducible,
            reproducibility_notes=tuple(notes),
        )

    def _build_voice_summary(
        self,
        report_id: str,
        candidate_symbol: str,
        report_status: ReportStatus,
        conclusion: Optional[AnalyticalConclusion],
        supporting_count: int,
        conflicting_count: int,
        limitation_count: int,
    ) -> InvestigationReportVoiceSummary:
        """Generate deterministic speech-ready summary with zero trading directives."""
        c_state_str = conclusion.conclusion_state.value if conclusion else "INSUFFICIENT_EVIDENCE"
        headline = f"Investigation report {report_id} generated for {candidate_symbol} with status {report_status.value}."
        status_speech = f"Investigation session completed with status {report_status.value}."
        candidate_speech = f"Evaluated candidate {candidate_symbol} across empirical evidence items."
        evidence_speech = f"Evidence review documented {supporting_count} supporting items and {conflicting_count} conflicting items."
        conclusion_speech = f"Analytical conclusion reached: {c_state_str}. Requires human analyst review."
        caveats_speech = f"Report documents {limitation_count} analytical limitations. Execution engine remains locked."

        script = (
            f"{headline} {status_speech} {candidate_speech} {evidence_speech} "
            f"{conclusion_speech} {caveats_speech}"
        )

        return InvestigationReportVoiceSummary(
            headline=headline,
            status_speech=status_speech,
            candidate_speech=candidate_speech,
            evidence_speech=evidence_speech,
            conclusion_speech=conclusion_speech,
            caveats_speech=caveats_speech,
            full_speech_script=script,
        )

    def _persist_state(self, report: InvestigationReport) -> None:
        """Persist bounded report state to StateStore."""
        if not self.state_store:
            return
        try:
            r_dict = report.to_dict()
            self.state_store.set("investigation_report:latest", r_dict)
            self.state_store.set(f"investigation_report:{report.report_id}", r_dict)

            history = self.state_store.get("investigation_report:history", [])
            if not isinstance(history, list):
                history = []

            updated_history = [r for r in history if r.get("report_id") != report.report_id]
            updated_history.append(r_dict)
            if len(updated_history) > self.max_history:
                updated_history = updated_history[-self.max_history:]
            self.state_store.set("investigation_report:history", updated_history)
        except Exception as exc:
            logger.error("Failed to persist investigation report state: %s", exc)

    def _dispatch_report_events(
        self,
        report: InvestigationReport,
        superseded: Sequence[InvestigationReport],
    ) -> None:
        """Dispatch sanitized lifecycle events to the EventBus."""
        if not self.event_bus:
            return
        try:
            # Superseded events
            for sup in superseded:
                sup_event = Event.create(
                    event_type=EventType.INVESTIGATION_REPORT_SUPERSEDED,
                    source="investigation_reporting_service",
                    payload={
                        "report_id": sup.report_id,
                        "session_id": sup.session_id,
                        "candidate_id": sup.candidate_id,
                        "version": sup.version,
                        "new_version": report.version,
                        "superseded_by": report.report_id,
                    },
                    correlation_id=sup.report_id,
                )
                self.event_bus.publish(sup_event)

            # Primary report event
            ev_type = (
                EventType.INVESTIGATION_REPORT_INVALID
                if report.report_status == ReportStatus.INVALID
                else EventType.INVESTIGATION_REPORT_GENERATED
            )
            event = Event.create(
                event_type=ev_type,
                source="investigation_reporting_service",
                payload={
                    "report_id": report.report_id,
                    "session_id": report.session_id,
                    "candidate_id": report.candidate_id,
                    "priority_id": report.priority_id,
                    "report_status": report.report_status.value,
                    "reviewer_id": report.reviewer_id,
                    "version": report.version,
                    "supporting_evidence_count": len(report.supporting_evidence),
                    "conflicting_evidence_count": len(report.conflicting_evidence),
                    "uncertainty_count": len(report.uncertainty_summary),
                    "limitation_count": len(report.limitation_summary),
                    "conclusion_state": (
                        report.analytical_conclusion.get("conclusion_state")
                        if report.analytical_conclusion
                        else None
                    ),
                },
                correlation_id=report.report_id,
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.error("Failed to dispatch investigation report event: %s", exc)

    def get_report(self, report_id: str) -> Optional[InvestigationReport]:
        """Retrieve an immutable report by its identifier."""
        clean_id = report_id.strip() if report_id else ""
        with self._lock:
            return self._reports.get(clean_id)

    def list_reports(
        self,
        candidate_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[Union[str, ReportStatus]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[InvestigationReport]:
        """Query investigation reports with optional filtering and pagination."""
        st_enum = ReportStatus.from_string(status) if status else None
        clean_cand = candidate_id.strip() if candidate_id else None
        clean_sess = session_id.strip() if session_id else None

        with self._lock:
            filtered: List[InvestigationReport] = []
            for rid in self._report_order:
                rep = self._reports.get(rid)
                if not rep:
                    continue
                if clean_cand and rep.candidate_id != clean_cand:
                    continue
                if clean_sess and rep.session_id != clean_sess:
                    continue
                if st_enum and rep.report_status != st_enum:
                    continue
                filtered.append(rep)

            # Sort descending by generated_at
            filtered.sort(key=lambda r: r.generated_at, reverse=True)
            return filtered[offset : offset + limit]

    def get_report_evidence(self, report_id: str) -> Optional[List[Dict[str, Any]]]:
        """Retrieve structured evidence matrix for a report."""
        rep = self.get_report(report_id)
        if not rep:
            return None
        return [e.to_dict() for e in rep.evidence_matrix]

    def get_report_audit(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve audit summary for a report."""
        rep = self.get_report(report_id)
        if not rep:
            return None
        return rep.audit_summary.to_dict() if rep.audit_summary else None

    def validate_report(self, report_id: str) -> Dict[str, Any]:
        """Validate integrity and reproducibility of a generated report."""
        rep = self.get_report(report_id)
        if not rep:
            return {
                "report_id": report_id,
                "valid": False,
                "error": f"Report '{report_id}' not found.",
            }

        issues: List[str] = []
        if rep.report_status == ReportStatus.INVALID:
            issues.append("Report marked with INVALID status.")
        if rep.audit_summary and not rep.audit_summary.sequence_integrity:
            issues.extend(rep.audit_summary.integrity_issues)
        if rep.reproducibility_record and not rep.reproducibility_record.is_reproducible:
            issues.extend(rep.reproducibility_record.reproducibility_notes)

        return {
            "report_id": rep.report_id,
            "session_id": rep.session_id,
            "candidate_id": rep.candidate_id,
            "report_status": rep.report_status.value,
            "valid": len(issues) == 0,
            "issues": issues,
            "reproducible": rep.reproducibility_record.is_reproducible if rep.reproducibility_record else False,
            "audit_intact": rep.audit_summary.sequence_integrity if rep.audit_summary else False,
        }
