"""KAISER — Investigation Evidence Consolidation & Review Outcome Service.

Phase 2, Step 30: Deterministic service for assembling the current review state
of an investigation from comparison, discrepancies, resolution cases, evidence,
reconciliation, human review decisions, provenance, and data quality.

SAFETY INVARIANTS:
1. Decision support only: NEVER decides what to trade, ranks strategies, predicts
   profitability, or authorizes order execution.
2. Does NOT manufacture new truth: Represents the current reviewed evidence state only.
3. Preserves original evidence and conflicts: Never overwrites historical evidence
   or merges conflicting claims into a single synthetic claim.
4. Bounded, thread-safe StateStore retention and sanitized EventBus events.
"""

from __future__ import annotations

import copy
import logging
import threading
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from app.analysis.opportunities.comparison.models import (
    ComparabilityState,
    InvestigationComparison,
    InvestigationDiscrepancy,
)
from app.analysis.opportunities.consolidation.models import (
    DEFAULT_MAX_CONSOLIDATION_HISTORY,
    SAFE_ID_PATTERN,
    ConsolidatedConflictItem,
    ConsolidatedEvidenceItem,
    ConsolidatedInvestigationOutcome,
    ConsolidatedReviewDecision,
    ConsolidationLimitation,
    ConsolidationLimitationItem,
    ConsolidationState,
    ConsolidationVoiceSummary,
    DecisionClaimType,
    QualityAggregationSummary,
    ReproducibilityConsolidationState,
    ReproducibilitySummary,
    compute_deterministic_conflict_id,
    compute_deterministic_outcome_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.investigation.reporting.models import (
    AttributionType,
    EvidenceMatrixEntry,
    InvestigationReport,
    ObservationMatrixEntry,
)
from app.analysis.opportunities.resolution.models import (
    DiscrepancyResolutionCase,
    ResolutionState,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore

logger = logging.getLogger("kaiser.analysis.opportunities.consolidation")


class InvestigationOutcomeConsolidationService:
    """Thread-safe, deterministic service for consolidating human-reviewed investigation evidence."""

    def __init__(
        self,
        state_store: Optional[StateStore] = None,
        event_bus: Optional[EventBus] = None,
        reporting_service: Optional[Any] = None,
        comparison_engine: Optional[Any] = None,
        resolution_service: Optional[Any] = None,
        max_history: int = DEFAULT_MAX_CONSOLIDATION_HISTORY,
    ) -> None:
        """Initialize the consolidation service.

        Args:
            state_store: Optional StateStore for bounded state persistence.
            event_bus: Optional EventBus for emitting sanitized consolidation events.
            reporting_service: Optional InvestigationReportingService for report lookup.
            comparison_engine: Optional InvestigationComparisonEngine for comparison lookup.
            resolution_service: Optional DiscrepancyResolutionService for resolution case lookup.
            max_history: Maximum number of historical outcomes to retain in ring buffer.
        """
        self.state_store = state_store
        self.event_bus = event_bus
        self.reporting_service = reporting_service
        self.comparison_engine = comparison_engine
        self.resolution_service = resolution_service
        self.max_history = max(1, min(1000, int(max_history)))

        self._lock = threading.RLock()
        self._outcomes: Dict[str, ConsolidatedInvestigationOutcome] = {}
        self._history: List[str] = []

    def consolidate_investigation_outcome(
        self,
        investigation_id: str,
        report_ids: Optional[Sequence[str]] = None,
        comparison_ids: Optional[Sequence[str]] = None,
        resolution_case_ids: Optional[Sequence[str]] = None,
        correlation_id: Optional[str] = None,
    ) -> ConsolidatedInvestigationOutcome:
        """Assembles a deterministic consolidated review outcome for an investigation.

        Args:
            investigation_id: Identifier of the investigation session or candidate.
            report_ids: Optional sequence of specific investigation report IDs.
            comparison_ids: Optional sequence of cross-investigation comparison IDs.
            resolution_case_ids: Optional sequence of discrepancy resolution case IDs.
            correlation_id: Optional correlation ID for tracing.

        Returns:
            Immutable ConsolidatedInvestigationOutcome representing current evidence state.

        Raises:
            ValidationError: If input identifiers are invalid or contain prohibited fields.
        """
        corr_id = correlation_id or f"corr_cons_{int(time.time() * 1000)}"

        if not investigation_id or not isinstance(investigation_id, str):
            raise ValidationError("investigation_id must be a non-empty string.")
        clean_inv_id = investigation_id.strip()
        if not SAFE_ID_PATTERN.match(clean_inv_id):
            raise ValidationError(
                f"Invalid investigation_id format '{investigation_id}'. Must match {SAFE_ID_PATTERN.pattern}"
            )

        # Emit start event
        if self.event_bus:
            try:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.INVESTIGATION_OUTCOME_CONSOLIDATION_STARTED,
                        source="investigation_consolidation_service",
                        correlation_id=corr_id,
                        payload={
                            "investigation_id": clean_inv_id,
                            "report_ids": list(report_ids or ()),
                            "comparison_ids": list(comparison_ids or ()),
                            "resolution_case_ids": list(resolution_case_ids or ()),
                        },
                    )
                )
            except Exception as exc:
                logger.debug("Failed to emit consolidation started event: %s", exc)

        try:
            with self._lock:
                outcome = self._assemble_outcome(
                    investigation_id=clean_inv_id,
                    report_ids=report_ids,
                    comparison_ids=comparison_ids,
                    resolution_case_ids=resolution_case_ids,
                    correlation_id=corr_id,
                )

                # Persist in cache and StateStore
                self._outcomes[outcome.outcome_id] = outcome
                self._history.append(outcome.outcome_id)
                if len(self._history) > self.max_history:
                    oldest_id = self._history.pop(0)
                    self._outcomes.pop(oldest_id, None)

                self._persist_to_statestore(outcome)

            # Emit completion event
            if self.event_bus:
                try:
                    ev_type = EventType.INVESTIGATION_OUTCOME_CONSOLIDATED
                    if outcome.outcome_state == ConsolidationState.CONFLICTING:
                        self.event_bus.publish(
                            Event.create(
                                event_type=EventType.INVESTIGATION_OUTCOME_CONFLICT_DETECTED,
                                source="investigation_consolidation_service",
                                correlation_id=corr_id,
                                payload={
                                    "outcome_id": outcome.outcome_id,
                                    "investigation_id": outcome.investigation_id,
                                    "outcome_state": outcome.outcome_state.value,
                                    "conflicts_count": len(outcome.conflicts),
                                },
                            )
                        )
                    elif outcome.outcome_state == ConsolidationState.INCOMPLETE:
                        self.event_bus.publish(
                            Event.create(
                                event_type=EventType.INVESTIGATION_OUTCOME_INCOMPLETE,
                                source="investigation_consolidation_service",
                                correlation_id=corr_id,
                                payload={
                                    "outcome_id": outcome.outcome_id,
                                    "investigation_id": outcome.investigation_id,
                                    "outcome_state": outcome.outcome_state.value,
                                },
                            )
                        )

                    self.event_bus.publish(
                        Event.create(
                            event_type=ev_type,
                            source="investigation_consolidation_service",
                            correlation_id=corr_id,
                            payload={
                                "outcome_id": outcome.outcome_id,
                                "investigation_id": outcome.investigation_id,
                                "outcome_state": outcome.outcome_state.value,
                                "facts_count": len(outcome.facts),
                                "interpretations_count": len(outcome.interpretations),
                                "human_decisions_count": len(outcome.human_review_decisions),
                                "resolved_count": len(outcome.resolved_items),
                                "unresolved_count": len(outcome.unresolved_items),
                            },
                        )
                    )
                except Exception as exc:
                    logger.debug("Failed to emit outcome consolidated event: %s", exc)

            return outcome

        except Exception as exc:
            logger.error("Investigation outcome consolidation failed: %s", exc, exc_info=True)
            if self.event_bus:
                try:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.INVESTIGATION_OUTCOME_CONSOLIDATION_FAILED,
                            source="investigation_consolidation_service",
                            correlation_id=corr_id,
                            payload={
                                "investigation_id": clean_inv_id,
                                "error": str(exc),
                            },
                        )
                    )
                except Exception:
                    pass
            raise

    def _assemble_outcome(
        self,
        investigation_id: str,
        report_ids: Optional[Sequence[str]],
        comparison_ids: Optional[Sequence[str]],
        resolution_case_ids: Optional[Sequence[str]],
        correlation_id: str,
    ) -> ConsolidatedInvestigationOutcome:
        """Internal assembly logic for deterministic consolidation."""
        now = time.time()
        limitations: List[ConsolidationLimitationItem] = []
        source_unavailable = False

        # 1. Load Investigation Reports
        loaded_reports: List[InvestigationReport] = []
        target_report_ids = [str(r).strip() for r in (report_ids or ())]
        
        if target_report_ids:
            for rid in target_report_ids:
                rep = self._fetch_report(rid)
                if rep is not None:
                    loaded_reports.append(rep)
                else:
                    source_unavailable = True
                    limitations.append(
                        ConsolidationLimitationItem(
                            limitation_type=ConsolidationLimitation.UNAVAILABLE_SOURCE,
                            description=f"Requested source report '{rid}' could not be retrieved.",
                            affected_items=(rid,),
                            severity="ERROR",
                        )
                    )
        else:
            # Query reports by investigation_id / session_id
            loaded_reports = self._query_reports_for_investigation(investigation_id)

        # 2. Load Comparisons
        loaded_comparisons: List[InvestigationComparison] = []
        target_comp_ids = [str(c).strip() for c in (comparison_ids or ())]
        
        if target_comp_ids:
            for cid in target_comp_ids:
                comp = self._fetch_comparison(cid)
                if comp is not None:
                    loaded_comparisons.append(comp)
                else:
                    limitations.append(
                        ConsolidationLimitationItem(
                            limitation_type=ConsolidationLimitation.UNAVAILABLE_SOURCE,
                            description=f"Requested comparison '{cid}' could not be retrieved.",
                            affected_items=(cid,),
                            severity="WARNING",
                        )
                    )
        else:
            # Query comparisons involving loaded reports
            loaded_comparisons = self._query_comparisons_for_reports(loaded_reports)

        # 3. Load Resolution Cases
        loaded_cases: List[DiscrepancyResolutionCase] = []
        target_case_ids = [str(rc).strip() for rc in (resolution_case_ids or ())]

        if target_case_ids:
            for case_id in target_case_ids:
                rc = self._fetch_resolution_case(case_id)
                if rc is not None:
                    loaded_cases.append(rc)
                else:
                    limitations.append(
                        ConsolidationLimitationItem(
                            limitation_type=ConsolidationLimitation.UNAVAILABLE_SOURCE,
                            description=f"Requested discrepancy resolution case '{case_id}' could not be retrieved.",
                            affected_items=(case_id,),
                            severity="WARNING",
                        )
                    )
        else:
            # Query resolution cases involving comparisons or reports
            loaded_cases = self._query_resolution_cases(loaded_comparisons, loaded_reports)

        # 4. Classify Evidence into FACTS, INTERPRETATIONS, and HUMAN REVIEW DECISIONS
        facts: List[ConsolidatedEvidenceItem] = []
        interpretations: List[ConsolidatedEvidenceItem] = []
        human_decisions: List[ConsolidatedReviewDecision] = []
        seen_evidence_ids: Set[str] = set()

        for report in loaded_reports:
            for entry in report.evidence_matrix:
                if entry.evidence_id in seen_evidence_ids:
                    continue
                seen_evidence_ids.add(entry.evidence_id)

                # Epistemological claim classification:
                # Dispositions and notes from reviewers are INTERPRETATIONS.
                # Machine observations and verified metrics are FACTS.
                if (
                    entry.attribution == AttributionType.HUMAN_REVIEW.value
                    or entry.reviewer_disposition in ("ACCEPTED", "REJECTED", "DISPUTED")
                ):
                    if entry.status in ("VERIFIED", "OBSERVED", "CALCULATED"):
                        claim_type = DecisionClaimType.FACT
                    else:
                        claim_type = DecisionClaimType.INTERPRETATION
                elif entry.attribution == AttributionType.MACHINE_GENERATED_EVIDENCE.value:
                    claim_type = DecisionClaimType.FACT
                else:
                    claim_type = DecisionClaimType.FACT

                rep_rec = getattr(report, "reproducibility_record", getattr(report, "reproducibility", None))
                rep_time = getattr(report, "generated_at", 0.0) or (rep_rec.generated_at if rep_rec else 0.0)
                scope_val = getattr(report, "scope", "") or (report.candidate_summary.get("scope", "") if hasattr(report, "candidate_summary") and isinstance(report.candidate_summary, dict) else "")

                ev_item = ConsolidatedEvidenceItem(
                    evidence_id=entry.evidence_id,
                    source_id=report.report_id,
                    claim_type=claim_type,
                    evidence_relationship=None,
                    review_status=entry.reviewer_disposition or "UNREVIEWED",
                    provenance_reference=entry.provenance_ref or "MISSING_PROVENANCE",
                    quality_status=entry.quality_ref or "VALID",
                    timestamp=rep_time,
                    scope=scope_val,
                    notes=(entry.notes,) if entry.notes else (),
                    metadata={"attribution": entry.attribution, "status": entry.status},
                )
                if claim_type == DecisionClaimType.FACT:
                    facts.append(ev_item)
                else:
                    interpretations.append(ev_item)

            obs_list = getattr(report, "observations", getattr(report, "observation_matrix", ()))
            for obs in obs_list:
                obs_key = f"obs:{report.report_id}:{obs.observation_id}"
                if obs_key in seen_evidence_ids:
                    continue
                seen_evidence_ids.add(obs_key)

                obs_claim_type = (
                    DecisionClaimType.FACT
                    if str(obs.observation_type).upper() in ("EMPIRICAL_FACT", "FACT")
                    else DecisionClaimType.INTERPRETATION
                )
                obs_item = ConsolidatedEvidenceItem(
                    evidence_id=obs.observation_id,
                    source_id=report.report_id,
                    claim_type=obs_claim_type,
                    review_status=str(obs.observation_type),
                    provenance_reference="MISSING_PROVENANCE",
                    quality_status="VALID",
                    timestamp=0.0,
                    scope=scope_val,
                    notes=(obs.statement,),
                    metadata={"evidence_refs": list(obs.evidence_refs)},
                )
                if obs_claim_type == DecisionClaimType.FACT:
                    facts.append(obs_item)
                else:
                    interpretations.append(obs_item)

        # 5. Extract Human Review Decisions from Resolution Cases
        for case in loaded_cases:
            if case.decision:
                human_decisions.append(
                    ConsolidatedReviewDecision(
                        decision_id=case.decision.decision_id,
                        source_resolution_case_id=case.resolution_case_id,
                        claim_type=DecisionClaimType.HUMAN_REVIEW_DECISION,
                        decision_text=case.decision.summary,
                        rationale=case.decision.rationale,
                        supporting_evidence_ids=tuple(case.decision.accepted_evidence_ids),
                        reviewer_id=case.decision.reviewer_id,
                        timestamp=case.decision.decided_at,
                        metadata={
                            "rejected_evidence_ids": list(case.decision.rejected_evidence_ids),
                            "residual_uncertainty": case.decision.residual_uncertainty,
                        },
                    )
                )

        # 6. Discrepancy & Conflict Resolution Impact
        resolved_items: List[str] = []
        unresolved_items: List[str] = []
        conflicts: List[ConsolidatedConflictItem] = []
        case_map: Dict[str, DiscrepancyResolutionCase] = {c.discrepancy_id: c for c in loaded_cases}

        for comp in loaded_comparisons:
            for disc in comp.discrepancy_summary:
                matching_case = case_map.get(disc.discrepancy_id)
                conflict_id = compute_deterministic_conflict_id(disc.discrepancy_id, disc.evidence_refs)

                if matching_case is None or matching_case.state in (
                    ResolutionState.OPEN,
                    ResolutionState.UNDER_REVIEW,
                    ResolutionState.EVIDENCE_REQUESTED,
                ):
                    # Unresolved discrepancy -> active conflict
                    unresolved_items.append(disc.discrepancy_id)
                    conflicts.append(
                        ConsolidatedConflictItem(
                            conflict_id=conflict_id,
                            discrepancy_id=disc.discrepancy_id,
                            conflicting_evidence_ids=tuple(disc.evidence_refs),
                            conflicting_report_ids=tuple(disc.report_ids),
                            description=disc.description,
                            conflict_nature=str(disc.category.value if hasattr(disc.category, "value") else disc.category),
                            resolution_case_id=matching_case.resolution_case_id if matching_case else None,
                            is_active=True,
                            reconciliation_outcome=None,
                        )
                    )
                    limitations.append(
                        ConsolidationLimitationItem(
                            limitation_type=ConsolidationLimitation.UNRESOLVED_DISCREPANCY,
                            description=(
                                f"Discrepancy '{disc.discrepancy_id}' ({disc.description}) remains unresolved "
                                f"(state: {matching_case.state.value if matching_case else 'NO_RESOLUTION_CASE'})."
                            ),
                            affected_items=(disc.discrepancy_id,),
                            severity="ERROR" if str(getattr(disc, "severity", "")).upper() == "ERROR" else "WARNING",
                        )
                    )

                elif matching_case.state == ResolutionState.DEFERRED:
                    # Deferred -> unresolved but not an active contradiction block
                    unresolved_items.append(disc.discrepancy_id)
                    conflicts.append(
                        ConsolidatedConflictItem(
                            conflict_id=conflict_id,
                            discrepancy_id=disc.discrepancy_id,
                            conflicting_evidence_ids=tuple(disc.evidence_refs),
                            conflicting_report_ids=tuple(disc.report_ids),
                            description=disc.description,
                            conflict_nature="DEFERRED_DISCREPANCY",
                            resolution_case_id=matching_case.resolution_case_id,
                            is_active=False,
                            reconciliation_outcome="DEFERRED",
                        )
                    )
                    limitations.append(
                        ConsolidationLimitationItem(
                            limitation_type=ConsolidationLimitation.UNRESOLVED_DISCREPANCY,
                            description=f"Discrepancy '{disc.discrepancy_id}' review is DEFERRED by human reviewer.",
                            affected_items=(disc.discrepancy_id,),
                            severity="WARNING",
                        )
                    )

                elif matching_case.state == ResolutionState.REJECTED:
                    # Rejected resolution -> remains an active conflict
                    unresolved_items.append(disc.discrepancy_id)
                    conflicts.append(
                        ConsolidatedConflictItem(
                            conflict_id=conflict_id,
                            discrepancy_id=disc.discrepancy_id,
                            conflicting_evidence_ids=tuple(disc.evidence_refs),
                            conflicting_report_ids=tuple(disc.report_ids),
                            description=disc.description,
                            conflict_nature="REJECTED_RESOLUTION",
                            resolution_case_id=matching_case.resolution_case_id,
                            is_active=True,
                            reconciliation_outcome="REJECTED",
                        )
                    )
                    limitations.append(
                        ConsolidationLimitationItem(
                            limitation_type=ConsolidationLimitation.CONFLICTING_EVIDENCE,
                            description=f"Proposed resolution for discrepancy '{disc.discrepancy_id}' was REJECTED by human review.",
                            affected_items=(disc.discrepancy_id,),
                            severity="ERROR",
                        )
                    )

                elif matching_case.state in (
                    ResolutionState.RESOLVED,
                    ResolutionState.RECONCILIATION_RECORDED,
                ):
                    # Reconciled / Resolved -> conflict no longer active, original evidence preserved
                    resolved_items.append(disc.discrepancy_id)
                    rec_outcome = (
                        matching_case.reconciliation_records[0].reconciliation_outcome.value
                        if matching_case.reconciliation_records
                        else "RESOLVED"
                    )
                    conflicts.append(
                        ConsolidatedConflictItem(
                            conflict_id=conflict_id,
                            discrepancy_id=disc.discrepancy_id,
                            conflicting_evidence_ids=tuple(disc.evidence_refs),
                            conflicting_report_ids=tuple(disc.report_ids),
                            description=disc.description,
                            conflict_nature="RECONCILED",
                            resolution_case_id=matching_case.resolution_case_id,
                            is_active=False,
                            reconciliation_outcome=rec_outcome,
                        )
                    )

                elif matching_case.state == ResolutionState.INVALIDATED:
                    resolved_items.append(disc.discrepancy_id)
                    conflicts.append(
                        ConsolidatedConflictItem(
                            conflict_id=conflict_id,
                            discrepancy_id=disc.discrepancy_id,
                            conflicting_evidence_ids=tuple(disc.evidence_refs),
                            conflicting_report_ids=tuple(disc.report_ids),
                            description=disc.description,
                            conflict_nature="INVALIDATED",
                            resolution_case_id=matching_case.resolution_case_id,
                            is_active=False,
                            reconciliation_outcome="EVIDENCE_INVALIDATED",
                        )
                    )
                    limitations.append(
                        ConsolidationLimitationItem(
                            limitation_type=ConsolidationLimitation.DIFFERENT_ANALYTICAL_SCOPE,
                            description=f"Discrepancy '{disc.discrepancy_id}' was INVALIDATED as inapplicable.",
                            affected_items=(disc.discrepancy_id,),
                            severity="INFO",
                        )
                    )

        # 7. Quality Aggregation
        quality_statuses: Dict[str, str] = {}
        stale_sources: List[str] = []
        missing_sources: List[str] = []
        degraded_sources: List[str] = []
        invalid_sources: List[str] = []

        for rep in loaded_reports:
            status = (
                getattr(rep, "quality_status", None)
                or (rep.metadata.get("quality_status") if hasattr(rep, "metadata") and isinstance(rep.metadata, dict) else None)
                or (rep.report_provenance.get("quality_status") if hasattr(rep, "report_provenance") and isinstance(rep.report_provenance, dict) else None)
                or (rep.traceability.get("quality_status") if hasattr(rep, "traceability") and isinstance(rep.traceability, dict) else None)
                or "VALID"
            )
            quality_statuses[rep.report_id] = status
            if status == "INVALID":
                invalid_sources.append(rep.report_id)
            elif status == "DEGRADED":
                degraded_sources.append(rep.report_id)
            elif status == "STALE":
                stale_sources.append(rep.report_id)
            elif status == "MISSING":
                missing_sources.append(rep.report_id)

        for comp in loaded_comparisons:
            if hasattr(comp, "quality_comparison") and comp.quality_comparison:
                for src_id, q_stat in comp.quality_comparison.quality_statuses.items():
                    quality_statuses[src_id] = q_stat
                    if q_stat == "INVALID" and src_id not in invalid_sources:
                        invalid_sources.append(src_id)
                    elif q_stat == "DEGRADED" and src_id not in degraded_sources:
                        degraded_sources.append(src_id)
                    elif q_stat == "STALE" and src_id not in stale_sources:
                        stale_sources.append(src_id)

        if invalid_sources:
            overall_q_state = "INVALID"
            limitations.append(
                ConsolidationLimitationItem(
                    limitation_type=ConsolidationLimitation.DEGRADED_DATA_QUALITY,
                    description=f"Invalid data sources detected: {', '.join(invalid_sources)}.",
                    affected_items=tuple(invalid_sources),
                    severity="ERROR",
                )
            )
        elif degraded_sources:
            overall_q_state = "DEGRADED"
            limitations.append(
                ConsolidationLimitationItem(
                    limitation_type=ConsolidationLimitation.DEGRADED_DATA_QUALITY,
                    description=f"Degraded data quality present in sources: {', '.join(degraded_sources)}.",
                    affected_items=tuple(degraded_sources),
                    severity="WARNING",
                )
            )
        elif stale_sources:
            overall_q_state = "STALE"
            limitations.append(
                ConsolidationLimitationItem(
                    limitation_type=ConsolidationLimitation.STALE_DATA,
                    description=f"Stale market data sources: {', '.join(stale_sources)}.",
                    affected_items=tuple(stale_sources),
                    severity="WARNING",
                )
            )
        elif quality_statuses:
            overall_q_state = "VALID"
        else:
            overall_q_state = "UNKNOWN"

        quality_summary = QualityAggregationSummary(
            overall_quality_state=overall_q_state,
            source_quality_statuses=quality_statuses,
            stale_sources=tuple(stale_sources),
            missing_sources=tuple(missing_sources),
            degraded_sources=tuple(degraded_sources),
            invalid_sources=tuple(invalid_sources),
            coverage_limitations=(),
            notes=(),
        )

        # 8. Reproducibility Aggregation
        reproducible_sources: List[str] = []
        non_reproducible_sources: List[str] = []
        reproducibility_ids: Dict[str, Optional[str]] = {}
        config_fingerprints: Dict[str, Optional[str]] = {}

        for rep in loaded_reports:
            rep_rec = getattr(rep, "reproducibility_record", getattr(rep, "reproducibility", None))
            if rep_rec:
                reproducibility_ids[rep.report_id] = rep_rec.reproducibility_id
                config_fingerprints[rep.report_id] = rep_rec.config_fingerprint
                if rep_rec.is_reproducible:
                    reproducible_sources.append(rep.report_id)
                else:
                    non_reproducible_sources.append(rep.report_id)
            else:
                non_reproducible_sources.append(rep.report_id)

        if not loaded_reports:
            repro_state = ReproducibilityConsolidationState.UNKNOWN
        elif len(reproducible_sources) == len(loaded_reports):
            repro_state = ReproducibilityConsolidationState.REPRODUCIBLE
        elif len(non_reproducible_sources) == len(loaded_reports):
            repro_state = ReproducibilityConsolidationState.NON_REPRODUCIBLE
            limitations.append(
                ConsolidationLimitationItem(
                    limitation_type=ConsolidationLimitation.NON_REPRODUCIBLE_SOURCE,
                    description="All source reports lack complete reproducibility lineage.",
                    affected_items=tuple(non_reproducible_sources),
                    severity="WARNING",
                )
            )
        else:
            repro_state = ReproducibilityConsolidationState.PARTIALLY_REPRODUCIBLE
            limitations.append(
                ConsolidationLimitationItem(
                    limitation_type=ConsolidationLimitation.NON_REPRODUCIBLE_SOURCE,
                    description="Some source reports lack reproducibility lineage.",
                    affected_items=tuple(non_reproducible_sources),
                    severity="WARNING",
                )
            )

        repro_summary = ReproducibilitySummary(
            reproducibility_state=repro_state,
            reproducible_sources=tuple(reproducible_sources),
            non_reproducible_sources=tuple(non_reproducible_sources),
            reproducibility_identifiers=reproducibility_ids,
            config_fingerprints=config_fingerprints,
            notes=(),
        )

        # 9. Provenance Lineage Mapping
        def _get_analysis_runs(r: InvestigationReport) -> List[str]:
            rec = getattr(r, "reproducibility_record", getattr(r, "reproducibility", None))
            if rec and rec.source_analysis_run_ids:
                return list(rec.source_analysis_run_ids)
            src_runs = getattr(r, "source_analysis_run_ids", ())
            if src_runs:
                return list(src_runs)
            return ["MISSING_PROVENANCE"]

        provenance_summary: Dict[str, Any] = {
            "investigation_id": investigation_id,
            "report_lineage": {
                r.report_id: {
                    "session_id": r.session_id,
                    "candidate_id": r.candidate_id,
                    "priority_id": r.priority_id,
                    "analysis_runs": _get_analysis_runs(r),
                }
                for r in loaded_reports
            },
            "comparison_lineage": {
                c.comparison_id: {
                    "report_ids": list(c.report_ids),
                    "discrepancies": [d.discrepancy_id for d in c.discrepancy_summary],
                }
                for c in loaded_comparisons
            },
            "resolution_lineage": {
                rc.resolution_case_id: {
                    "discrepancy_id": rc.discrepancy_id,
                    "comparison_id": rc.comparison_id,
                    "reviewer_id": rc.resolved_by or (rc.decision.reviewer_id if rc.decision else "MISSING_PROVENANCE"),
                    "state": rc.state.value,
                }
                for rc in loaded_cases
            },
        }

        # Check for missing provenance in evidence items
        missing_prov_items = [ev.evidence_id for ev in facts + interpretations if ev.provenance_reference == "MISSING_PROVENANCE"]
        if missing_prov_items:
            limitations.append(
                ConsolidationLimitationItem(
                    limitation_type=ConsolidationLimitation.MISSING_PROVENANCE,
                    description=f"{len(missing_prov_items)} evidence item(s) lack provenance lineage.",
                    affected_items=tuple(missing_prov_items[:10]),
                    severity="INFO",
                )
            )

        # 10. Determine Deterministic ConsolidationState
        active_conflicts = [c for c in conflicts if c.is_active]

        if source_unavailable and not loaded_reports:
            outcome_state = ConsolidationState.UNAVAILABLE
        elif not loaded_reports or (not facts and not interpretations):
            outcome_state = ConsolidationState.INCOMPLETE
            limitations.append(
                ConsolidationLimitationItem(
                    limitation_type=ConsolidationLimitation.INCOMPLETE_EVIDENCE,
                    description="Investigation lacks sufficient evidence items.",
                    affected_items=(investigation_id,),
                    severity="WARNING",
                )
            )
        elif active_conflicts:
            outcome_state = ConsolidationState.CONFLICTING
        elif unresolved_items:
            outcome_state = ConsolidationState.PARTIALLY_RECONCILED
        elif resolved_items or (loaded_reports and not conflicts):
            outcome_state = ConsolidationState.RECONCILED
        else:
            outcome_state = ConsolidationState.INCOMPLETE

        # 11. Evidence Summary & Traceability
        evidence_summary = {
            "total_evidence_items": len(facts) + len(interpretations),
            "facts_count": len(facts),
            "interpretations_count": len(interpretations),
            "human_decisions_count": len(human_decisions),
            "discrepancies_detected": len(conflicts),
            "discrepancies_resolved": len(resolved_items),
            "discrepancies_unresolved": len(unresolved_items),
            "active_conflicts_count": len(active_conflicts),
            "limitations_count": len(limitations),
        }

        traceability = {
            "investigation_id": investigation_id,
            "correlation_id": correlation_id,
            "report_ids": [r.report_id for r in loaded_reports],
            "comparison_ids": [c.comparison_id for c in loaded_comparisons],
            "resolution_case_ids": [rc.resolution_case_id for rc in loaded_cases],
            "timestamp": now,
        }

        # 12. Deterministic ID Computation
        outcome_id = compute_deterministic_outcome_id(
            investigation_id=investigation_id,
            report_ids=[r.report_id for r in loaded_reports],
            resolution_case_ids=[rc.resolution_case_id for rc in loaded_cases],
            timestamp=now,
        )

        # 13. Voice-Safe Summary Script Generation
        voice_text_parts: List[str] = []
        if outcome_state == ConsolidationState.RECONCILED:
            voice_text_parts.append("Investigation evidence is reconciled.")
            if resolved_items:
                voice_text_parts.append(f"{len(resolved_items)} discrepancy item(s) resolved by human review.")
        elif outcome_state == ConsolidationState.PARTIALLY_RECONCILED:
            voice_text_parts.append("Investigation evidence is partially reconciled.")
            if unresolved_items:
                voice_text_parts.append(f"{len(unresolved_items)} item(s) remain deferred or unresolved.")
            if human_decisions:
                voice_text_parts.append("Human review has been recorded for the resolved evidence.")
        elif outcome_state == ConsolidationState.CONFLICTING:
            voice_text_parts.append("Investigation evidence contains active conflicts.")
            voice_text_parts.append(f"{len(active_conflicts)} conflict(s) remain unresolved across source reports.")
        elif outcome_state == ConsolidationState.INCOMPLETE:
            voice_text_parts.append("Investigation evidence is incomplete. Required source data is missing.")
        elif outcome_state == ConsolidationState.UNAVAILABLE:
            voice_text_parts.append("Investigation evidence is unavailable. Required source reports could not be loaded.")
        else:
            voice_text_parts.append("Investigation outcome state is invalid.")

        if overall_q_state == "DEGRADED":
            voice_text_parts.append("Source data quality is degraded.")
        elif overall_q_state == "STALE":
            voice_text_parts.append("Source market data is stale.")

        voice_summary = ConsolidationVoiceSummary(
            outcome_id=outcome_id,
            outcome_state=outcome_state.value,
            summary_text=" ".join(voice_text_parts),
            voice_safe=True,
        )

        return ConsolidatedInvestigationOutcome(
            outcome_id=outcome_id,
            investigation_id=investigation_id,
            source_report_ids=tuple(r.report_id for r in loaded_reports),
            source_comparison_ids=tuple(c.comparison_id for c in loaded_comparisons),
            source_resolution_case_ids=tuple(rc.resolution_case_id for rc in loaded_cases),
            generated_at=now,
            outcome_state=outcome_state,
            evidence_summary=evidence_summary,
            resolved_items=tuple(resolved_items),
            unresolved_items=tuple(unresolved_items),
            conflicts=tuple(conflicts),
            facts=tuple(facts),
            interpretations=tuple(interpretations),
            human_review_decisions=tuple(human_decisions),
            quality_summary=quality_summary,
            provenance_summary=provenance_summary,
            reproducibility_summary=repro_summary,
            limitations=tuple(limitations),
            traceability=traceability,
            voice_summary=voice_summary,
            metadata={"correlation_id": correlation_id},
        )

    def _persist_to_statestore(self, outcome: ConsolidatedInvestigationOutcome) -> None:
        """Persist outcome into StateStore with bounded history."""
        if not self.state_store:
            return

        try:
            o_dict = outcome.to_dict()
            self.state_store.set("investigation_outcome:latest", o_dict)
            self.state_store.set(f"investigation_outcome:{outcome.outcome_id}", o_dict)
            self.state_store.set(f"investigation_outcome:by_investigation:{outcome.investigation_id}", o_dict)

            history = self.state_store.get("investigation_outcome:history", [])
            if not isinstance(history, list):
                history = []
            history.append(o_dict)
            if len(history) > self.max_history:
                history = history[-self.max_history:]
            self.state_store.set("investigation_outcome:history", history)
        except Exception as exc:
            logger.debug("Failed to persist outcome to StateStore: %s", exc)

    def _fetch_report(self, report_id: str) -> Optional[InvestigationReport]:
        """Fetch investigation report from reporting service or StateStore."""
        if self.reporting_service and hasattr(self.reporting_service, "get_report"):
            rep = self.reporting_service.get_report(report_id)
            if rep is not None:
                return rep

        if self.state_store:
            raw = self.state_store.get(f"investigation_report:{report_id}")
            if isinstance(raw, dict):
                # Return object if cached or construct if possible
                pass
        return None

    def _query_reports_for_investigation(self, investigation_id: str) -> List[InvestigationReport]:
        """Query reports matching investigation_id."""
        if self.reporting_service and hasattr(self.reporting_service, "list_reports"):
            reports = self.reporting_service.list_reports(session_id=investigation_id)
            if reports:
                return reports
            # Fallback search by candidate_id
            reports_cand = self.reporting_service.list_reports(candidate_id=investigation_id)
            if reports_cand:
                return reports_cand
        return []

    def _fetch_comparison(self, comparison_id: str) -> Optional[InvestigationComparison]:
        """Fetch comparison from comparison engine or StateStore."""
        if self.comparison_engine and hasattr(self.comparison_engine, "get_comparison"):
            comp = self.comparison_engine.get_comparison(comparison_id)
            if comp is not None:
                return comp

        if self.state_store:
            raw = self.state_store.get(f"investigation_comparison:{comparison_id}")
            if isinstance(raw, dict):
                pass
        return None

    def _query_comparisons_for_reports(self, reports: Sequence[InvestigationReport]) -> List[InvestigationComparison]:
        """Query comparisons for given reports."""
        if not reports or not self.comparison_engine or not hasattr(self.comparison_engine, "list_comparisons"):
            return []

        rep_ids = {r.report_id for r in reports}
        comparisons = self.comparison_engine.list_comparisons(limit=100)
        return [c for c in comparisons if any(rid in rep_ids for rid in c.report_ids)]

    def _fetch_resolution_case(self, case_id: str) -> Optional[DiscrepancyResolutionCase]:
        """Fetch resolution case from resolution service or StateStore."""
        if self.resolution_service and hasattr(self.resolution_service, "get_case"):
            return self.resolution_service.get_case(case_id)
        return None

    def _query_resolution_cases(
        self,
        comparisons: Sequence[InvestigationComparison],
        reports: Sequence[InvestigationReport],
    ) -> List[DiscrepancyResolutionCase]:
        """Query resolution cases involving comparisons or reports."""
        if not self.resolution_service or not hasattr(self.resolution_service, "list_cases"):
            return []

        comp_ids = {c.comparison_id for c in comparisons}
        rep_ids = {r.report_id for r in reports}
        all_cases = self.resolution_service.list_cases(limit=100)
        return [
            rc
            for rc in all_cases
            if rc.comparison_id in comp_ids or any(rid in rep_ids for rid in rc.report_ids)
        ]

    def get_outcome(self, outcome_id: str) -> Optional[ConsolidatedInvestigationOutcome]:
        """Retrieve a consolidated outcome by its ID."""
        if not outcome_id or not isinstance(outcome_id, str):
            return None
        clean_id = outcome_id.strip()
        if not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            if clean_id in self._outcomes:
                return self._outcomes[clean_id]

        if self.state_store:
            raw = self.state_store.get(f"investigation_outcome:{clean_id}")
            if isinstance(raw, dict):
                # When retrieved from StateStore, returning dict or rebuilding model
                pass
        return None

    def get_latest_outcome(self, investigation_id: Optional[str] = None) -> Optional[ConsolidatedInvestigationOutcome]:
        """Retrieve the latest consolidated outcome, optionally filtered by investigation ID."""
        with self._lock:
            if investigation_id:
                for oid in reversed(self._history):
                    outcome = self._outcomes.get(oid)
                    if outcome and outcome.investigation_id == investigation_id:
                        return outcome
            elif self._history:
                return self._outcomes.get(self._history[-1])
        return None

    def list_outcomes(
        self,
        investigation_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ConsolidatedInvestigationOutcome]:
        """List historical consolidated outcomes with filtering and pagination."""
        clamped_limit = max(1, min(100, int(limit)))
        clamped_offset = max(0, int(offset))

        with self._lock:
            matched: List[ConsolidatedInvestigationOutcome] = []
            for oid in reversed(self._history):
                outcome = self._outcomes.get(oid)
                if not outcome:
                    continue
                if investigation_id and outcome.investigation_id != investigation_id:
                    continue
                if state and outcome.outcome_state.value != state:
                    continue
                matched.append(outcome)

            return matched[clamped_offset : clamped_offset + clamped_limit]

    def refresh_outcome(
        self,
        outcome_id: str,
        correlation_id: Optional[str] = None,
    ) -> ConsolidatedInvestigationOutcome:
        """Refresh an outcome by generating a NEW outcome version from updated evidence states.

        Never mutates the historical outcome.
        """
        existing = self.get_outcome(outcome_id)
        if not existing:
            raise ValidationError(f"Outcome '{outcome_id}' not found for refresh.")

        return self.consolidate_investigation_outcome(
            investigation_id=existing.investigation_id,
            report_ids=existing.source_report_ids,
            comparison_ids=existing.source_comparison_ids,
            resolution_case_ids=existing.source_resolution_case_ids,
            correlation_id=correlation_id,
        )

    def get_voice_summary(self, outcome_id: str) -> Optional[ConsolidationVoiceSummary]:
        """Retrieve voice-safe summary for an outcome."""
        outcome = self.get_outcome(outcome_id)
        if outcome:
            return outcome.voice_summary
        return None
