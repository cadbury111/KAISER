"""KAISER — Investigation Discrepancy Resolution & Evidence Reconciliation Service.

Phase 2, Step 29: Deterministic, auditable service managing human-driven
discrepancy resolution cases, evidence reconciliation, and lifecycle transitions.

SAFETY INVARIANTS:
- All resolutions are strictly human-reviewed.
- The system NEVER resolves discrepancies or selects winning reports autonomously.
- Prohibited from trading execution, orders, broker integration, and profitability prediction.
"""

from __future__ import annotations

import copy
import logging
import threading
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from app.analysis.opportunities.resolution.models import (
    DEFAULT_MAX_RESOLUTION_HISTORY,
    SAFE_ID_PATTERN,
    TERMINAL_RESOLUTION_STATES,
    VALID_RESOLUTION_TRANSITIONS,
    DecisionClaimType,
    DiscrepancyResolutionCase,
    EvidenceReconciliationRecord,
    EvidenceRelationshipType,
    ReconciliationOutcome,
    ResolutionAuditRecord,
    ResolutionDecision,
    ResolutionState,
    ResolutionVoiceSummary,
    compute_deterministic_audit_id,
    compute_deterministic_case_id,
    compute_deterministic_decision_id,
    compute_deterministic_reconciliation_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.core.exceptions import ValidationError
from app.core.events.event import Event, EventType

logger = logging.getLogger("kaiser.analysis.opportunities.resolution.service")


class DiscrepancyResolutionService:
    """Thread-safe, deterministic service managing investigation discrepancy resolutions."""

    def __init__(
        self,
        state_store: Optional[Any] = None,
        event_bus: Optional[Any] = None,
        comparison_engine: Optional[Any] = None,
        max_history: int = DEFAULT_MAX_RESOLUTION_HISTORY,
    ) -> None:
        """Initialize the DiscrepancyResolutionService.

        Args:
            state_store: Optional persistent StateStore instance.
            event_bus: Optional EventBus for publishing lifecycle events.
            comparison_engine: Optional InvestigationComparisonEngine for verifying comparisons.
            max_history: Maximum bounded resolution cases to retain in history ring buffer.
        """
        self.state_store = state_store
        self.event_bus = event_bus
        self.comparison_engine = comparison_engine
        self.max_history = max(10, min(1000, max_history))

        self._cases: Dict[str, DiscrepancyResolutionCase] = {}
        self._lock = threading.RLock()

    def create_resolution_case(
        self,
        comparison_id: str,
        discrepancy_id: str,
        scope: Optional[str] = None,
        discrepancy_category: Optional[str] = None,
        discrepancy_severity: Optional[str] = None,
        discrepancy_description: Optional[str] = None,
        report_ids: Optional[Sequence[str]] = None,
        notes: Optional[Sequence[str]] = None,
        actor: str = "human_reviewer",
        correlation_id: Optional[str] = None,
    ) -> DiscrepancyResolutionCase:
        """Create a new human discrepancy resolution case from an investigation discrepancy.

        Args:
            comparison_id: Identifier of the source InvestigationComparison.
            discrepancy_id: Identifier of the detected InvestigationDiscrepancy.
            scope: Optional scope description (defaults to comparison scope).
            discrepancy_category: Optional discrepancy category name.
            discrepancy_severity: Optional discrepancy severity rating.
            discrepancy_description: Optional text description of the discrepancy.
            report_ids: Optional reports involved in the discrepancy.
            notes: Optional preliminary review notes.
            actor: Identifier of the actor initiating the case.
            correlation_id: Tracing correlation ID.

        Returns:
            DiscrepancyResolutionCase: The newly created immutable resolution case.
        """
        self._validate_id(comparison_id, "comparison_id")
        self._validate_id(discrepancy_id, "discrepancy_id")

        with self._lock:
            case_id = compute_deterministic_case_id(comparison_id, discrepancy_id)

            # Idempotency check: if case already exists in memory or state store, return it
            existing = self.get_case(case_id)
            if existing is not None:
                logger.info("Resolution case '%s' already exists; returning existing instance.", case_id)
                return existing

            # Attempt to populate discrepancy metadata from comparison_engine if available
            cat = discrepancy_category or "UNSPECIFIED"
            sev = discrepancy_severity or "WARNING"
            desc = discrepancy_description or f"Discrepancy '{discrepancy_id}' detected in comparison '{comparison_id}'."
            reps: Tuple[str, ...] = tuple(report_ids or ())

            if self.comparison_engine is not None:
                cmp_obj = self.comparison_engine.get_comparison(comparison_id)
                if cmp_obj is not None:
                    matched_disc = next((d for d in cmp_obj.discrepancy_summary if d.discrepancy_id == discrepancy_id), None)
                    if matched_disc is not None:
                        cat = matched_disc.category.value if hasattr(matched_disc.category, "value") else str(matched_disc.category)
                        sev = matched_disc.severity.value if hasattr(matched_disc.severity, "value") else str(matched_disc.severity)
                        desc = matched_disc.description
                        if not reps:
                            reps = tuple(matched_disc.report_ids)

            now = time.time()
            clean_scope = (scope or f"investigation_comparison:{comparison_id}").strip()
            initial_audit = ResolutionAuditRecord(
                audit_id=compute_deterministic_audit_id(case_id, "NONE", ResolutionState.OPEN.value, now),
                resolution_case_id=case_id,
                previous_state="NONE",
                new_state=ResolutionState.OPEN.value,
                actor=actor,
                timestamp=now,
                reason="Discrepancy resolution case opened for human review.",
                referenced_evidence=(),
                correlation_id=correlation_id or "",
            )

            case = DiscrepancyResolutionCase(
                resolution_case_id=case_id,
                comparison_id=comparison_id,
                discrepancy_id=discrepancy_id,
                created_at=now,
                updated_at=now,
                state=ResolutionState.OPEN,
                scope=clean_scope,
                discrepancy_category=cat,
                discrepancy_severity=sev,
                discrepancy_description=desc,
                report_ids=reps,
                notes=tuple(notes or ()),
                audit_trail=(initial_audit,),
            )

            self._save_case(case)
            self._dispatch_event(
                event_type=EventType.DISCREPANCY_RESOLUTION_CASE_CREATED,
                payload={
                    "resolution_case_id": case.resolution_case_id,
                    "comparison_id": case.comparison_id,
                    "discrepancy_id": case.discrepancy_id,
                    "state": case.state.value,
                    "category": case.discrepancy_category,
                    "severity": case.discrepancy_severity,
                    "created_at": case.created_at,
                },
                correlation_id=correlation_id,
            )
            return case

    def get_case(self, case_id: str) -> Optional[DiscrepancyResolutionCase]:
        """Retrieve a discrepancy resolution case by its ID."""
        self._validate_id(case_id, "case_id")
        with self._lock:
            if case_id in self._cases:
                return self._cases[case_id]

            if self.state_store is not None:
                item = self.state_store.get(f"discrepancy_resolution:{case_id}", None)
                if isinstance(item, dict):
                    case = self._deserialize_case(item)
                    self._cases[case_id] = case
                    return case
            return None

    def list_cases(
        self,
        comparison_id: Optional[str] = None,
        discrepancy_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[DiscrepancyResolutionCase]:
        """List discrepancy resolution cases with pagination and filtering."""
        with self._lock:
            all_cases = list(self._cases.values())
            if not all_cases and self.state_store is not None:
                history = self.state_store.get("discrepancy_resolution:history", [])
                if isinstance(history, list):
                    for h in history:
                        if isinstance(h, dict) and "resolution_case_id" in h:
                            c = self.get_case(h["resolution_case_id"])
                            if c and c.resolution_case_id not in self._cases:
                                self._cases[c.resolution_case_id] = c
                    all_cases = list(self._cases.values())

            filtered = all_cases
            if comparison_id:
                filtered = [c for c in filtered if c.comparison_id == comparison_id]
            if discrepancy_id:
                filtered = [c for c in filtered if c.discrepancy_id == discrepancy_id]
            if state:
                state_upper = state.strip().upper()
                filtered = [c for c in filtered if c.state.value == state_upper]

            filtered.sort(key=lambda c: c.created_at, reverse=True)
            clamped_limit = max(1, min(100, limit))
            clamped_offset = max(0, offset)
            return filtered[clamped_offset : clamped_offset + clamped_limit]

    def transition_case(
        self,
        case_id: str,
        target_state: ResolutionState | str,
        reason: str,
        actor: str = "human_reviewer",
        correlation_id: Optional[str] = None,
    ) -> DiscrepancyResolutionCase:
        """Validate and execute a deterministic state transition for a resolution case."""
        self._validate_id(case_id, "case_id")
        if not reason or not isinstance(reason, str):
            raise ValidationError("Transition reason must be a non-empty string.")

        resolved_target = (
            target_state
            if isinstance(target_state, ResolutionState)
            else ResolutionState(str(target_state).strip().upper())
        )

        with self._lock:
            case = self.get_case(case_id)
            if case is None:
                raise ValidationError(f"Resolution case '{case_id}' not found.")

            if case.is_terminal():
                raise ValidationError(
                    f"Terminal case immutability violation: Case '{case_id}' is in terminal state '{case.state.value}' "
                    f"and cannot be transitioned."
                )

            valid_targets = VALID_RESOLUTION_TRANSITIONS.get(case.state, ())
            if resolved_target not in valid_targets:
                valid_names = [s.value for s in valid_targets]
                raise ValidationError(
                    f"Invalid state transition: Cannot transition resolution case '{case_id}' from '{case.state.value}' "
                    f"to '{resolved_target.value}'. Allowed transitions: {valid_names}."
                )

            now = time.time()
            audit = ResolutionAuditRecord(
                audit_id=compute_deterministic_audit_id(case_id, case.state.value, resolved_target.value, now),
                resolution_case_id=case_id,
                previous_state=case.state.value,
                new_state=resolved_target.value,
                actor=actor,
                timestamp=now,
                reason=reason,
                correlation_id=correlation_id or "",
            )

            updated = DiscrepancyResolutionCase(
                resolution_case_id=case.resolution_case_id,
                comparison_id=case.comparison_id,
                discrepancy_id=case.discrepancy_id,
                created_at=case.created_at,
                updated_at=now,
                state=resolved_target,
                scope=case.scope,
                discrepancy_category=case.discrepancy_category,
                discrepancy_severity=case.discrepancy_severity,
                discrepancy_description=case.discrepancy_description,
                report_ids=case.report_ids,
                notes=case.notes,
                resolved_by=case.resolved_by,
                resolution_reason=case.resolution_reason,
                supporting_evidence_ids=case.supporting_evidence_ids,
                reconciliation_records=case.reconciliation_records,
                decision=case.decision,
                audit_trail=case.audit_trail + (audit,),
                supersedes_case_id=case.supersedes_case_id,
                metadata=case.metadata,
            )

            self._save_case(updated)
            self._dispatch_event(
                event_type=EventType.DISCREPANCY_RESOLUTION_STATE_CHANGED,
                payload={
                    "resolution_case_id": updated.resolution_case_id,
                    "previous_state": case.state.value,
                    "new_state": updated.state.value,
                    "reason": reason,
                    "actor": actor,
                    "updated_at": updated.updated_at,
                },
                correlation_id=correlation_id,
            )
            return updated

    def attach_evidence(
        self,
        case_id: str,
        evidence_ids: Sequence[str],
        reviewer_id: str = "human_reviewer",
        notes: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> DiscrepancyResolutionCase:
        """Attach supporting evidence identifiers and analyst review notes to a case."""
        self._validate_id(case_id, "case_id")
        if not evidence_ids:
            raise ValidationError("evidence_ids must not be empty.")

        with self._lock:
            case = self.get_case(case_id)
            if case is None:
                raise ValidationError(f"Resolution case '{case_id}' not found.")

            if case.is_terminal():
                raise ValidationError(f"Cannot attach evidence to terminal case '{case_id}' (state: {case.state.value}).")

            clean_eids = set(case.supporting_evidence_ids)
            for eid in evidence_ids:
                if eid and isinstance(eid, str):
                    clean_eids.add(eid.strip())

            now = time.time()
            new_notes = list(case.notes)
            if notes and isinstance(notes, str) and notes.strip():
                new_notes.append(f"[{reviewer_id} @ {now:.1f}]: {notes.strip()}")

            audit = ResolutionAuditRecord(
                audit_id=compute_deterministic_audit_id(case_id, case.state.value, case.state.value, now),
                resolution_case_id=case_id,
                previous_state=case.state.value,
                new_state=case.state.value,
                actor=reviewer_id,
                timestamp=now,
                reason=f"Attached {len(evidence_ids)} evidence references.",
                referenced_evidence=tuple(sorted(evidence_ids)),
                correlation_id=correlation_id or "",
            )

            updated = DiscrepancyResolutionCase(
                resolution_case_id=case.resolution_case_id,
                comparison_id=case.comparison_id,
                discrepancy_id=case.discrepancy_id,
                created_at=case.created_at,
                updated_at=now,
                state=case.state,
                scope=case.scope,
                discrepancy_category=case.discrepancy_category,
                discrepancy_severity=case.discrepancy_severity,
                discrepancy_description=case.discrepancy_description,
                report_ids=case.report_ids,
                notes=tuple(new_notes),
                resolved_by=case.resolved_by,
                resolution_reason=case.resolution_reason,
                supporting_evidence_ids=tuple(sorted(clean_eids)),
                reconciliation_records=case.reconciliation_records,
                decision=case.decision,
                audit_trail=case.audit_trail + (audit,),
                supersedes_case_id=case.supersedes_case_id,
                metadata=case.metadata,
            )

            self._save_case(updated)
            self._dispatch_event(
                event_type=EventType.EVIDENCE_ATTACHED_TO_RESOLUTION,
                payload={
                    "resolution_case_id": updated.resolution_case_id,
                    "evidence_ids": list(evidence_ids),
                    "reviewer_id": reviewer_id,
                    "updated_at": updated.updated_at,
                },
                correlation_id=correlation_id,
            )
            return updated

    def record_reconciliation(
        self,
        case_id: str,
        evidence_refs: Sequence[str],
        relationship_type: EvidenceRelationshipType | str,
        reconciliation_outcome: ReconciliationOutcome | str,
        reviewer_notes: str,
        rationale: str,
        reviewer_id: str = "human_reviewer",
        evidence_classifications: Optional[Mapping[str, str]] = None,
        source_provenance_refs: Optional[Sequence[str]] = None,
        correlation_id: Optional[str] = None,
    ) -> DiscrepancyResolutionCase:
        """Record a human-reviewed evidence reconciliation entry between conflicting evidence."""
        self._validate_id(case_id, "case_id")
        if not evidence_refs:
            raise ValidationError("evidence_refs must contain at least one evidence identifier.")
        if not rationale or not isinstance(rationale, str):
            raise ValidationError("Reconciliation rationale must be a non-empty string.")

        rel_type = (
            relationship_type
            if isinstance(relationship_type, EvidenceRelationshipType)
            else EvidenceRelationshipType(str(relationship_type).strip().upper())
        )
        outcome = (
            reconciliation_outcome
            if isinstance(reconciliation_outcome, ReconciliationOutcome)
            else ReconciliationOutcome(str(reconciliation_outcome).strip().upper())
        )

        with self._lock:
            case = self.get_case(case_id)
            if case is None:
                raise ValidationError(f"Resolution case '{case_id}' not found.")

            if case.is_terminal():
                raise ValidationError(f"Cannot record reconciliation for terminal case '{case_id}' (state: {case.state.value}).")

            now = time.time()
            rec_id = compute_deterministic_reconciliation_id(case_id, evidence_refs, rel_type.value)

            rec_record = EvidenceReconciliationRecord(
                reconciliation_id=rec_id,
                resolution_case_id=case_id,
                evidence_refs=tuple(sorted(evidence_refs)),
                evidence_classifications=evidence_classifications or {},
                relationship_type=rel_type,
                reconciliation_outcome=outcome,
                reviewer_notes=reviewer_notes or "",
                rationale=rationale,
                created_at=now,
                reviewer_id=reviewer_id,
                source_provenance_refs=tuple(source_provenance_refs or ()),
            )

            # Auto-advance to RECONCILIATION_RECORDED if currently OPEN, UNDER_REVIEW, or EVIDENCE_REQUESTED
            new_state = case.state
            if case.state in (ResolutionState.OPEN, ResolutionState.UNDER_REVIEW, ResolutionState.EVIDENCE_REQUESTED):
                new_state = ResolutionState.RECONCILIATION_RECORDED

            audit = ResolutionAuditRecord(
                audit_id=compute_deterministic_audit_id(case_id, case.state.value, new_state.value, now),
                resolution_case_id=case_id,
                previous_state=case.state.value,
                new_state=new_state.value,
                actor=reviewer_id,
                timestamp=now,
                reason=f"Evidence reconciliation recorded ({rel_type.value} -> {outcome.value}).",
                referenced_evidence=tuple(sorted(evidence_refs)),
                correlation_id=correlation_id or "",
            )

            updated = DiscrepancyResolutionCase(
                resolution_case_id=case.resolution_case_id,
                comparison_id=case.comparison_id,
                discrepancy_id=case.discrepancy_id,
                created_at=case.created_at,
                updated_at=now,
                state=new_state,
                scope=case.scope,
                discrepancy_category=case.discrepancy_category,
                discrepancy_severity=case.discrepancy_severity,
                discrepancy_description=case.discrepancy_description,
                report_ids=case.report_ids,
                notes=case.notes,
                resolved_by=case.resolved_by,
                resolution_reason=case.resolution_reason,
                supporting_evidence_ids=case.supporting_evidence_ids,
                reconciliation_records=case.reconciliation_records + (rec_record,),
                decision=case.decision,
                audit_trail=case.audit_trail + (audit,),
                supersedes_case_id=case.supersedes_case_id,
                metadata=case.metadata,
            )

            self._save_case(updated)
            self._dispatch_event(
                event_type=EventType.EVIDENCE_RECONCILIATION_RECORDED,
                payload={
                    "resolution_case_id": updated.resolution_case_id,
                    "reconciliation_id": rec_record.reconciliation_id,
                    "relationship_type": rec_record.relationship_type.value,
                    "reconciliation_outcome": rec_record.reconciliation_outcome.value,
                    "reviewer_id": reviewer_id,
                    "evidence_refs": list(rec_record.evidence_refs),
                    "new_state": updated.state.value,
                },
                correlation_id=correlation_id,
            )
            return updated

    def record_human_resolution(
        self,
        case_id: str,
        summary: str,
        rationale: str,
        reviewer_id: str,
        accepted_evidence_ids: Sequence[str] = (),
        rejected_evidence_ids: Sequence[str] = (),
        residual_uncertainty: Optional[str] = None,
        resolution_state: ResolutionState | str = ResolutionState.RESOLVED,
        action_required: bool = False,
        correlation_id: Optional[str] = None,
    ) -> DiscrepancyResolutionCase:
        """Record an explicit human decision resolving or rejecting a discrepancy case.

        SAFETY INVARIANT:
        The reviewer MUST be an identified human actor. AI or autonomous resolution is rejected.
        Resolution target MUST be a valid terminal state (RESOLVED or REJECTED).
        """
        self._validate_id(case_id, "case_id")
        if not reviewer_id or not isinstance(reviewer_id, str) or reviewer_id.strip().lower() in ("ai", "autonomous", "system", "bot"):
            raise ValidationError(
                f"Human review boundary violation: Resolution must be explicitly recorded by a human reviewer. "
                f"Reviewer ID '{reviewer_id}' is invalid."
            )
        if not rationale or not isinstance(rationale, str):
            raise ValidationError("Resolution rationale must be a non-empty string.")

        target = (
            resolution_state
            if isinstance(resolution_state, ResolutionState)
            else ResolutionState(str(resolution_state).strip().upper())
        )
        if target not in (ResolutionState.RESOLVED, ResolutionState.REJECTED):
            raise ValidationError(
                f"Invalid resolution terminal state: Must resolve to 'RESOLVED' or 'REJECTED', not '{target.value}'."
            )

        with self._lock:
            case = self.get_case(case_id)
            if case is None:
                raise ValidationError(f"Resolution case '{case_id}' not found.")

            if case.is_terminal():
                raise ValidationError(
                    f"Terminal case immutability violation: Case '{case_id}' is already in terminal state '{case.state.value}'."
                )

            now = time.time()
            decision_id = compute_deterministic_decision_id(case_id, reviewer_id, now)

            decision = ResolutionDecision(
                decision_id=decision_id,
                resolution_case_id=case_id,
                claim_type=DecisionClaimType.HUMAN_REVIEW_DECISION,
                summary=summary or f"Human-reviewed resolution recorded by {reviewer_id}.",
                rationale=rationale,
                reviewer_id=reviewer_id,
                decided_at=now,
                accepted_evidence_ids=tuple(sorted(accepted_evidence_ids)),
                rejected_evidence_ids=tuple(sorted(rejected_evidence_ids)),
                residual_uncertainty=residual_uncertainty,
                action_required=action_required,
            )

            audit = ResolutionAuditRecord(
                audit_id=compute_deterministic_audit_id(case_id, case.state.value, target.value, now),
                resolution_case_id=case_id,
                previous_state=case.state.value,
                new_state=target.value,
                actor=reviewer_id,
                timestamp=now,
                reason=f"Human-reviewed decision recorded: {target.value}. Rationale: {rationale}",
                referenced_evidence=tuple(sorted(list(accepted_evidence_ids) + list(rejected_evidence_ids))),
                correlation_id=correlation_id or "",
            )

            updated = DiscrepancyResolutionCase(
                resolution_case_id=case.resolution_case_id,
                comparison_id=case.comparison_id,
                discrepancy_id=case.discrepancy_id,
                created_at=case.created_at,
                updated_at=now,
                state=target,
                scope=case.scope,
                discrepancy_category=case.discrepancy_category,
                discrepancy_severity=case.discrepancy_severity,
                discrepancy_description=case.discrepancy_description,
                report_ids=case.report_ids,
                notes=case.notes,
                resolved_by=reviewer_id,
                resolution_reason=rationale,
                supporting_evidence_ids=case.supporting_evidence_ids,
                reconciliation_records=case.reconciliation_records,
                decision=decision,
                audit_trail=case.audit_trail + (audit,),
                supersedes_case_id=case.supersedes_case_id,
                metadata=case.metadata,
            )

            self._save_case(updated)
            self._dispatch_event(
                event_type=EventType.HUMAN_RESOLUTION_RECORDED,
                payload={
                    "resolution_case_id": updated.resolution_case_id,
                    "decision_id": decision.decision_id,
                    "state": updated.state.value,
                    "reviewer_id": reviewer_id,
                    "claim_type": decision.claim_type.value,
                    "accepted_evidence_count": len(decision.accepted_evidence_ids),
                    "rejected_evidence_count": len(decision.rejected_evidence_ids),
                    "decided_at": decision.decided_at,
                },
                correlation_id=correlation_id,
            )
            return updated

    def defer_resolution(
        self,
        case_id: str,
        reason: str,
        reviewer_id: str = "human_reviewer",
        correlation_id: Optional[str] = None,
    ) -> DiscrepancyResolutionCase:
        """Defer resolution of a discrepancy case pending future evidence or analysis."""
        return self.transition_case(
            case_id=case_id,
            target_state=ResolutionState.DEFERRED,
            reason=reason,
            actor=reviewer_id,
            correlation_id=correlation_id,
        )

    def invalidate_resolution(
        self,
        case_id: str,
        reason: str,
        reviewer_id: str = "human_reviewer",
        correlation_id: Optional[str] = None,
    ) -> DiscrepancyResolutionCase:
        """Invalidate a discrepancy case if the discrepancy was discovered to be spurious or corrupted."""
        return self.transition_case(
            case_id=case_id,
            target_state=ResolutionState.INVALIDATED,
            reason=reason,
            actor=reviewer_id,
            correlation_id=correlation_id,
        )

    def get_voice_summary(self, case_id: str) -> ResolutionVoiceSummary:
        """Generate a deterministic, voice-safe summary for a discrepancy resolution case."""
        case = self.get_case(case_id)
        if case is None:
            raise ValidationError(f"Resolution case '{case_id}' not found.")

        rec_count = len(case.reconciliation_records)
        ev_count = len(case.supporting_evidence_ids)

        if case.state == ResolutionState.RESOLVED:
            text = (
                f"Discrepancy {case.discrepancy_id} has been resolved by human review. "
                f"{rec_count} evidence reconciliations recorded."
            )
        elif case.state == ResolutionState.REJECTED:
            text = (
                f"Discrepancy {case.discrepancy_id} resolution was rejected by human review. "
                f"Analytical hypothesis remains invalidated."
            )
        elif case.state == ResolutionState.DEFERRED:
            text = (
                f"Discrepancy {case.discrepancy_id} resolution has been deferred pending further evidence."
            )
        elif case.state == ResolutionState.INVALIDATED:
            text = (
                f"Discrepancy {case.discrepancy_id} has been invalidated as spurious or corrupted."
            )
        else:
            text = (
                f"Discrepancy {case.discrepancy_id} remains unresolved in {case.state.value} state. "
                f"{ev_count} evidence items attached. Human review is required."
            )

        return ResolutionVoiceSummary(
            case_id=case.resolution_case_id,
            state=case.state.value,
            summary_text=text,
            voice_safe=True,
        )

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------
    def _validate_id(self, identifier: str, name: str) -> None:
        """Enforce strict identifier validation preventing nulls, traversal, and injection."""
        if (
            not identifier
            or not isinstance(identifier, str)
            or len(identifier) > 128
            or ".." in identifier
            or "/" in identifier
            or "\\" in identifier
            or "\0" in identifier
            or not SAFE_ID_PATTERN.match(identifier.strip())
        ):
            raise ValidationError(f"Invalid {name} format: '{identifier}'. Must match {SAFE_ID_PATTERN.pattern}.")

    def _save_case(self, case: DiscrepancyResolutionCase) -> None:
        """Cache case in memory and persist to bounded StateStore."""
        self._cases[case.resolution_case_id] = case

        if self.state_store is None:
            return

        try:
            c_dict = case.to_dict()
            self.state_store.set("discrepancy_resolution:latest", c_dict)
            self.state_store.set(f"discrepancy_resolution:{case.resolution_case_id}", c_dict)
            self.state_store.set(f"discrepancy_resolution:by_discrepancy:{case.discrepancy_id}", c_dict)

            history = self.state_store.get("discrepancy_resolution:history", [])
            if not isinstance(history, list):
                history = []
            # Keep history unique by resolution_case_id, updating existing or appending
            history = [h for h in history if isinstance(h, dict) and h.get("resolution_case_id") != case.resolution_case_id]
            history.append({
                "resolution_case_id": case.resolution_case_id,
                "comparison_id": case.comparison_id,
                "discrepancy_id": case.discrepancy_id,
                "state": case.state.value,
                "updated_at": case.updated_at,
                "is_terminal": case.is_terminal(),
            })
            if len(history) > self.max_history:
                history = history[-self.max_history:]
            self.state_store.set("discrepancy_resolution:history", history)
        except Exception as exc:
            logger.warning("Failed to persist discrepancy resolution state: %s", exc)

    def _deserialize_case(self, data: Mapping[str, Any]) -> DiscrepancyResolutionCase:
        """Reconstruct immutable DiscrepancyResolutionCase from serialized dictionary."""
        reconciliations: List[EvidenceReconciliationRecord] = []
        for r in data.get("reconciliation_records", []):
            if isinstance(r, dict):
                reconciliations.append(
                    EvidenceReconciliationRecord(
                        reconciliation_id=str(r["reconciliation_id"]),
                        resolution_case_id=str(r["resolution_case_id"]),
                        evidence_refs=tuple(r.get("evidence_refs", ())),
                        evidence_classifications=dict(r.get("evidence_classifications", {})),
                        relationship_type=EvidenceRelationshipType(r["relationship_type"]),
                        reconciliation_outcome=ReconciliationOutcome(r["reconciliation_outcome"]),
                        reviewer_notes=str(r.get("reviewer_notes", "")),
                        rationale=str(r.get("rationale", "")),
                        created_at=float(r.get("created_at", 0.0)),
                        reviewer_id=str(r.get("reviewer_id", "human_reviewer")),
                        source_provenance_refs=tuple(r.get("source_provenance_refs", ())),
                        metadata=dict(r.get("metadata", {})),
                    )
                )

        audits: List[ResolutionAuditRecord] = []
        for a in data.get("audit_trail", []):
            if isinstance(a, dict):
                audits.append(
                    ResolutionAuditRecord(
                        audit_id=str(a["audit_id"]),
                        resolution_case_id=str(a["resolution_case_id"]),
                        previous_state=str(a["previous_state"]),
                        new_state=str(a["new_state"]),
                        actor=str(a["actor"]),
                        timestamp=float(a["timestamp"]),
                        reason=str(a.get("reason", "")),
                        referenced_evidence=tuple(a.get("referenced_evidence", ())),
                        correlation_id=str(a.get("correlation_id", "")),
                    )
                )

        decision = None
        d_raw = data.get("decision")
        if isinstance(d_raw, dict):
            decision = ResolutionDecision(
                decision_id=str(d_raw["decision_id"]),
                resolution_case_id=str(d_raw["resolution_case_id"]),
                claim_type=DecisionClaimType(d_raw.get("claim_type", "HUMAN_REVIEW_DECISION")),
                summary=str(d_raw.get("summary", "")),
                rationale=str(d_raw.get("rationale", "")),
                reviewer_id=str(d_raw.get("reviewer_id", "human_reviewer")),
                decided_at=float(d_raw.get("decided_at", 0.0)),
                accepted_evidence_ids=tuple(d_raw.get("accepted_evidence_ids", ())),
                rejected_evidence_ids=tuple(d_raw.get("rejected_evidence_ids", ())),
                residual_uncertainty=d_raw.get("residual_uncertainty"),
                action_required=bool(d_raw.get("action_required", False)),
                metadata=dict(d_raw.get("metadata", {})),
            )

        return DiscrepancyResolutionCase(
            resolution_case_id=str(data["resolution_case_id"]),
            comparison_id=str(data["comparison_id"]),
            discrepancy_id=str(data["discrepancy_id"]),
            created_at=float(data["created_at"]),
            updated_at=float(data["updated_at"]),
            state=ResolutionState(data["state"]),
            scope=str(data.get("scope", "")),
            discrepancy_category=str(data.get("discrepancy_category", "UNSPECIFIED")),
            discrepancy_severity=str(data.get("discrepancy_severity", "WARNING")),
            discrepancy_description=str(data.get("discrepancy_description", "")),
            report_ids=tuple(data.get("report_ids", ())),
            notes=tuple(data.get("notes", ())),
            resolved_by=data.get("resolved_by"),
            resolution_reason=data.get("resolution_reason"),
            supporting_evidence_ids=tuple(data.get("supporting_evidence_ids", ())),
            reconciliation_records=tuple(reconciliations),
            decision=decision,
            audit_trail=tuple(audits),
            supersedes_case_id=data.get("supersedes_case_id"),
            metadata=dict(data.get("metadata", {})),
        )

    def _dispatch_event(
        self,
        event_type: EventType,
        payload: Mapping[str, Any],
        correlation_id: Optional[str] = None,
    ) -> None:
        """Publish sanitized lifecycle event to the EventBus."""
        if self.event_bus is None:
            return
        try:
            self.event_bus.publish(
                Event.create(
                    event_type=event_type,
                    payload=payload,
                    source="discrepancy_resolution_service",
                    correlation_id=correlation_id,
                )
            )
        except Exception as exc:
            logger.warning("Failed to publish resolution event '%s': %s", event_type, exc)
