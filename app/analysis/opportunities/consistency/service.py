"""KAISER — Cross-Investigation Consolidated Outcome Consistency Service.

Phase 2, Step 31: Deterministic analytical service for comparing multiple
ConsolidatedInvestigationOutcome records.

CORE ARCHITECTURAL INVARIANT:
This consistency layer identifies structural similarities, differences,
review coverage gaps, unresolved conflicts, and evidence-state divergence
across consolidated investigations. It does NOT determine which
investigation is correct, rank investigations, predict profitability,
or recommend trading actions.
Settings.execution_locked remains True.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.analysis.opportunities.consolidation.models import (
    ConsolidatedInvestigationOutcome,
    ConsolidationState,
    DecisionClaimType,
)
from app.analysis.opportunities.consolidation.service import (
    InvestigationOutcomeConsolidationService,
)
from app.analysis.opportunities.consistency.models import (
    DEFAULT_MAX_CONSISTENCY_HISTORY,
    MAX_COMPARISON_OUTCOMES,
    MIN_COMPARISON_OUTCOMES,
    SAFE_ID_PATTERN,
    ComparisonDimensionName,
    ComparisonLifecycleState,
    ConsolidatedOutcomeComparability,
    ConsolidatedOutcomeComparison,
    CoverageStatus,
    DimensionMatchStatus,
    DivergenceSeverity,
    DivergenceStatus,
    EvidenceConsistencyItem,
    EvidenceConsistencyStatus,
    FactConsistencyItem,
    FactConsistencyStatus,
    HumanReviewConsistencyItem,
    HumanReviewConsistencyStatus,
    InterpretationConsistencyItem,
    InterpretationConsistencyStatus,
    OutcomeComparisonDimensionRecord,
    OutcomeComparisonLimitationItem,
    OutcomeComparisonVoiceSummary,
    OutcomeDivergenceItem,
    ProvenanceComparisonStatus,
    QualityComparisonStatus,
    ReproducibilityComparisonStatus,
    ReviewCoverageSummary,
    VALID_COMPARISON_LIFECYCLE_TRANSITIONS,
    compute_deterministic_comparison_id,
    compute_deterministic_divergence_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore

logger = logging.getLogger(__name__)


class ConsolidatedOutcomeConsistencyService:
    """Deterministic analytical service comparing multiple ConsolidatedInvestigationOutcome records.

    Produces auditable consistency records without subjective scoring, ranking, or trade recommendations.
    """

    def __init__(
        self,
        state_store: Optional[StateStore] = None,
        event_bus: Optional[EventBus] = None,
        consolidation_service: Optional[InvestigationOutcomeConsolidationService] = None,
        max_history: int = DEFAULT_MAX_CONSISTENCY_HISTORY,
        name: str = "consolidated_outcome_consistency_service",
    ) -> None:
        self.state_store = state_store
        self.event_bus = event_bus
        self.consolidation_service = consolidation_service
        self.max_history = max(10, min(500, max_history))
        self.name = name

        self._lock = threading.RLock()
        self._comparisons: Dict[str, ConsolidatedOutcomeComparison] = {}
        self._history: List[str] = []

    def compare_consolidated_outcomes(
        self,
        outcome_ids: Sequence[str],
        comparison_scope: str = "STANDARD",
        correlation_id: Optional[str] = None,
    ) -> ConsolidatedOutcomeComparison:
        """Compare multiple consolidated investigation outcomes deterministically."""
        corr_id = correlation_id or f"corr_cmp_{int(time.time() * 1000)}"

        # 1. Input Validation
        if not outcome_ids:
            raise ValidationError("outcome_ids cannot be empty. Minimum 2 outcomes required.")

        clean_outcomes: List[str] = []
        seen_outcomes: Set[str] = set()
        for oid in outcome_ids:
            if not oid or not isinstance(oid, str):
                raise ValidationError("Each outcome_id must be a non-empty string.")
            c_oid = oid.strip()
            if not SAFE_ID_PATTERN.match(c_oid):
                raise ValidationError(f"Invalid outcome_id format: '{c_oid}'.")
            if c_oid in seen_outcomes:
                raise ValidationError(f"Duplicate outcome_id detected: '{c_oid}'. Duplicate outcomes prohibited.")
            seen_outcomes.add(c_oid)
            clean_outcomes.append(c_oid)

        if len(clean_outcomes) < MIN_COMPARISON_OUTCOMES:
            raise ValidationError(
                f"Consolidated outcome comparison requires at least {MIN_COMPARISON_OUTCOMES} outcomes (got {len(clean_outcomes)})."
            )
        if len(clean_outcomes) > MAX_COMPARISON_OUTCOMES:
            raise ValidationError(
                f"Consolidated outcome comparison exceeds maximum limit of {MAX_COMPARISON_OUTCOMES} outcomes (got {len(clean_outcomes)})."
            )

        clean_scope = str(comparison_scope).strip().upper() if comparison_scope else "STANDARD"
        if not re.match(r"^[A-Z0-9_\-]+$", clean_scope):
            raise ValidationError(f"Invalid comparison_scope format: '{clean_scope}'.")

        # 2. Lifecycle: Emit STARTED event
        if self.event_bus:
            try:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.CONSOLIDATED_OUTCOMES_COMPARISON_STARTED,
                        source="consolidated_outcome_consistency_service",
                        correlation_id=corr_id,
                        payload={
                            "outcome_ids": clean_outcomes,
                            "comparison_scope": clean_scope,
                        },
                    )
                )
            except Exception as exc:
                logger.debug("Failed to emit comparison started event: %s", exc)

        try:
            with self._lock:
                comparison = self._execute_comparison(
                    outcome_ids=clean_outcomes,
                    comparison_scope=clean_scope,
                    correlation_id=corr_id,
                )

                # Persist in-memory ring buffer
                self._comparisons[comparison.comparison_id] = comparison
                self._history.append(comparison.comparison_id)
                if len(self._history) > self.max_history:
                    oldest_id = self._history.pop(0)
                    self._comparisons.pop(oldest_id, None)

                # Persist to StateStore
                self._persist_to_statestore(comparison)

            # 3. Emit Completion & Divergence Events
            if self.event_bus:
                try:
                    if comparison.divergence_items:
                        self.event_bus.publish(
                            Event.create(
                                event_type=EventType.CONSOLIDATED_OUTCOME_DIVERGENCE_DETECTED,
                                source="consolidated_outcome_consistency_service",
                                correlation_id=corr_id,
                                payload={
                                    "comparison_id": comparison.comparison_id,
                                    "divergences_count": len(comparison.divergence_items),
                                    "divergence_ids": [d.divergence_id for d in comparison.divergence_items],
                                },
                            )
                        )

                    if comparison.review_coverage.overall_coverage != CoverageStatus.COMPLETE:
                        self.event_bus.publish(
                            Event.create(
                                event_type=EventType.CONSOLIDATED_OUTCOME_REVIEW_GAP_DETECTED,
                                source="consolidated_outcome_consistency_service",
                                correlation_id=corr_id,
                                payload={
                                    "comparison_id": comparison.comparison_id,
                                    "coverage_status": comparison.review_coverage.overall_coverage.value,
                                    "unreviewed_evidence_count": comparison.review_coverage.evidence_unreviewed_count,
                                    "unresolved_discrepancies_count": comparison.review_coverage.discrepancies_unresolved_count,
                                },
                            )
                        )

                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.CONSOLIDATED_OUTCOMES_COMPARED,
                            source="consolidated_outcome_consistency_service",
                            correlation_id=corr_id,
                            payload={
                                "comparison_id": comparison.comparison_id,
                                "comparability_state": comparison.comparability_state.value,
                                "lifecycle_state": comparison.lifecycle_state.value,
                                "divergences_count": len(comparison.divergence_items),
                                "limitations_count": len(comparison.limitations),
                            },
                        )
                    )
                except Exception as exc:
                    logger.debug("Failed to emit comparison completion events: %s", exc)

            return comparison

        except Exception as exc:
            logger.error("Consolidated outcome comparison failed: %s", exc, exc_info=True)
            if self.event_bus:
                try:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.CONSOLIDATED_OUTCOMES_COMPARISON_FAILED,
                            source="consolidated_outcome_consistency_service",
                            correlation_id=corr_id,
                            payload={
                                "outcome_ids": clean_outcomes,
                                "error": str(exc),
                            },
                        )
                    )
                except Exception:
                    pass
            raise

    def _execute_comparison(
        self,
        outcome_ids: List[str],
        comparison_scope: str,
        correlation_id: str,
    ) -> ConsolidatedOutcomeComparison:
        """Internal deterministic comparison execution across validated outcomes."""
        now = time.time()
        limitations: List[OutcomeComparisonLimitationItem] = []
        divergences: List[OutcomeDivergenceItem] = []

        # 1. Fetch Outcome Records
        loaded_outcomes: List[ConsolidatedInvestigationOutcome] = []
        missing_outcomes: List[str] = []

        for oid in outcome_ids:
            outcome = self._fetch_outcome(oid)
            if outcome is not None:
                loaded_outcomes.append(outcome)
            else:
                missing_outcomes.append(oid)

        if missing_outcomes:
            limitations.append(
                OutcomeComparisonLimitationItem(
                    limitation_type="UNAVAILABLE_OUTCOME",
                    description=f"Outcomes could not be retrieved: {', '.join(missing_outcomes)}.",
                    affected_outcomes=tuple(missing_outcomes),
                    severity="ERROR",
                )
            )

        if len(loaded_outcomes) < MIN_COMPARISON_OUTCOMES:
            # Cannot compare fewer than 2 valid outcomes
            comp_id = compute_deterministic_comparison_id(outcome_ids, comparison_scope)
            empty_coverage = ReviewCoverageSummary(
                overall_coverage=CoverageStatus.UNKNOWN,
                evidence_reviewed_count=0,
                evidence_unreviewed_count=0,
                discrepancies_reviewed_count=0,
                discrepancies_unresolved_count=0,
                limitations_acknowledged_count=0,
                provenance_verified=False,
                reproducibility_checked=False,
                quality_checked=False,
            )
            return ConsolidatedOutcomeComparison(
                comparison_id=comp_id,
                outcome_ids=tuple(sorted(outcome_ids)),
                investigation_ids=tuple(),
                generated_at=now,
                comparison_scope=comparison_scope,
                lifecycle_state=ComparisonLifecycleState.INCOMPLETE,
                comparability_state=ConsolidatedOutcomeComparability.INVALID_INPUT,
                dimensions=(),
                evidence_consistency=(),
                fact_consistency=(),
                interpretation_consistency=(),
                human_review_consistency=(),
                review_coverage=empty_coverage,
                divergence_items=tuple(divergences),
                conflict_summary={"active_conflicts": 0},
                quality_comparison={"status": QualityComparisonStatus.UNAVAILABLE.value},
                provenance_comparison={"status": ProvenanceComparisonStatus.MISSING.value},
                reproducibility_comparison={"status": ReproducibilityComparisonStatus.UNKNOWN.value},
                limitations=tuple(limitations),
                traceability={"missing_outcomes": missing_outcomes, "correlation_id": correlation_id},
                voice_summary=OutcomeComparisonVoiceSummary(
                    comparison_id=comp_id,
                    headline="Comparison incomplete.",
                    script="Comparison incomplete. Required outcomes could not be retrieved.",
                ),
            )

        # 2. Extract Investigation IDs
        investigation_ids = tuple(sorted(list({o.investigation_id for o in loaded_outcomes})))

        # 3. Dimensional Comparison
        dimensions = self._compare_dimensions(loaded_outcomes)

        # 4. Determine Comparability State
        comparability_state = self._determine_comparability(loaded_outcomes, dimensions)

        # 5. Evidence Consistency Analysis
        evidence_consistency, ev_divergences = self._compare_evidence(loaded_outcomes)
        divergences.extend(ev_divergences)

        # 6. Fact Consistency Analysis
        fact_consistency, fact_divergences = self._compare_facts(loaded_outcomes)
        divergences.extend(fact_divergences)

        # 7. Interpretation Consistency Analysis
        interp_consistency, interp_divergences = self._compare_interpretations(loaded_outcomes)
        divergences.extend(interp_divergences)

        # 8. Human Review Consistency Analysis
        human_consistency, human_divergences = self._compare_human_reviews(loaded_outcomes)
        divergences.extend(human_divergences)

        # 9. Review Coverage Analysis
        review_coverage, cov_divergences, cov_limits = self._analyze_review_coverage(loaded_outcomes)
        divergences.extend(cov_divergences)
        limitations.extend(cov_limits)

        # 10. Conflict Summary & Divergences from Outcomes
        conflict_summary, conflict_divergences = self._analyze_conflicts(loaded_outcomes)
        divergences.extend(conflict_divergences)

        # 11. Quality Comparison
        quality_comparison, q_limits = self._compare_quality(loaded_outcomes)
        limitations.extend(q_limits)

        # 12. Provenance Comparison
        provenance_comparison, prov_limits = self._compare_provenance(loaded_outcomes)
        limitations.extend(prov_limits)

        # 13. Reproducibility Comparison
        repro_comparison, repro_limits = self._compare_reproducibility(loaded_outcomes)
        limitations.extend(repro_limits)

        # 14. Deterministic Comparison ID
        comparison_id = compute_deterministic_comparison_id(outcome_ids, comparison_scope)

        # 15. Lifecycle State
        if missing_outcomes or any(o.outcome_state == ConsolidationState.INCOMPLETE for o in loaded_outcomes):
            lifecycle_state = ComparisonLifecycleState.INCOMPLETE
        else:
            lifecycle_state = ComparisonLifecycleState.COMPLETED

        # 16. Voice-Safe Analytical Speech Script
        voice_summary = self._generate_voice_summary(
            comparison_id=comparison_id,
            loaded_outcomes=loaded_outcomes,
            comparability_state=comparability_state,
            review_coverage=review_coverage,
            divergences=divergences,
        )

        # 17. Traceability Mapping
        traceability = {
            "comparison_id": comparison_id,
            "correlation_id": correlation_id,
            "outcome_ids": [o.outcome_id for o in loaded_outcomes],
            "investigation_ids": list(investigation_ids),
            "generated_at": now,
            "outcomes_count": len(loaded_outcomes),
            "divergences_count": len(divergences),
            "limitations_count": len(limitations),
        }

        return ConsolidatedOutcomeComparison(
            comparison_id=comparison_id,
            outcome_ids=tuple(sorted([o.outcome_id for o in loaded_outcomes])),
            investigation_ids=investigation_ids,
            generated_at=now,
            comparison_scope=comparison_scope,
            lifecycle_state=lifecycle_state,
            comparability_state=comparability_state,
            dimensions=tuple(dimensions),
            evidence_consistency=tuple(evidence_consistency),
            fact_consistency=tuple(fact_consistency),
            interpretation_consistency=tuple(interp_consistency),
            human_review_consistency=tuple(human_consistency),
            review_coverage=review_coverage,
            divergence_items=tuple(divergences),
            conflict_summary=conflict_summary,
            quality_comparison=quality_comparison,
            provenance_comparison=provenance_comparison,
            reproducibility_comparison=repro_comparison,
            limitations=tuple(limitations),
            traceability=traceability,
            voice_summary=voice_summary,
            metadata={"comparison_scope": comparison_scope},
        )

    # ==========================================================================
    # Dimension Comparison Logic
    # ==========================================================================
    def _compare_dimensions(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> List[OutcomeComparisonDimensionRecord]:
        """Compare the 12 explicit analytical dimensions across outcomes."""
        records: List[OutcomeComparisonDimensionRecord] = []

        # 1. Symbol / Subject
        scopes = [o.traceability.get("scope", "") for o in outcomes]
        symbols: Set[str] = set()
        for o in outcomes:
            for ev in o.facts:
                if ev.scope:
                    symbols.add(ev.scope.split()[0])
        sym_map = {o.outcome_id: (scopes[i] or "UNKNOWN") for i, o in enumerate(outcomes)}
        if len(set(sym_map.values())) <= 1:
            sym_status = DimensionMatchStatus.MATCH
            sym_desc = "All outcomes address the same analytical subject."
        else:
            sym_status = DimensionMatchStatus.MISMATCH
            sym_desc = "Outcomes address differing analytical subjects or scopes."
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.SYMBOL_SUBJECT,
                status=sym_status,
                description=sym_desc,
                details_by_outcome=sym_map,
            )
        )

        # 2. Timeframe
        tf_map = {o.outcome_id: o.traceability.get("timeframe", "1h") for o in outcomes}
        tf_status = DimensionMatchStatus.MATCH if len(set(tf_map.values())) <= 1 else DimensionMatchStatus.MISMATCH
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.TIMEFRAME,
                status=tf_status,
                description="Timeframe dimension alignment.",
                details_by_outcome=tf_map,
            )
        )

        # 3. Observation Period
        ts_map = {o.outcome_id: o.generated_at for o in outcomes}
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.OBSERVATION_PERIOD,
                status=DimensionMatchStatus.MATCH,
                description="Observation period timestamps preserved.",
                details_by_outcome=ts_map,
            )
        )

        # 4. Analytical Scope
        scope_map = {o.outcome_id: str(o.traceability.get("comparison_ids", ())) for o in outcomes}
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.ANALYTICAL_SCOPE,
                status=DimensionMatchStatus.MATCH if len(set(scope_map.values())) <= 1 else DimensionMatchStatus.PARTIAL_MATCH,
                description="Analytical scope evaluation.",
                details_by_outcome=scope_map,
            )
        )

        # 5. Evidence Schema
        schema_map = {o.outcome_id: f"{len(o.facts)}_facts_{len(o.interpretations)}_interps" for o in outcomes}
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.EVIDENCE_SCHEMA,
                status=DimensionMatchStatus.MATCH if len(set(schema_map.values())) <= 1 else DimensionMatchStatus.PARTIAL_MATCH,
                description="Evidence schema partition.",
                details_by_outcome=schema_map,
            )
        )

        # 6. Report Version
        rep_map = {o.outcome_id: len(o.source_report_ids) for o in outcomes}
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.REPORT_VERSION,
                status=DimensionMatchStatus.MATCH,
                description="Source report counts.",
                details_by_outcome=rep_map,
            )
        )

        # 7. Provenance
        prov_map = {o.outcome_id: len(o.provenance_summary.get("report_lineage", {})) for o in outcomes}
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.PROVENANCE,
                status=DimensionMatchStatus.MATCH if len(set(prov_map.values())) <= 1 else DimensionMatchStatus.PARTIAL_MATCH,
                description="Provenance lineage depth.",
                details_by_outcome=prov_map,
            )
        )

        # 8. Data Quality
        q_map = {o.outcome_id: o.quality_summary.overall_quality_state for o in outcomes}
        q_status = DimensionMatchStatus.MATCH if len(set(q_map.values())) <= 1 else DimensionMatchStatus.PARTIAL_MATCH
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.DATA_QUALITY,
                status=q_status,
                description="Data quality categorical alignment.",
                details_by_outcome=q_map,
            )
        )

        # 9. Reproducibility
        r_map = {o.outcome_id: o.reproducibility_summary.reproducibility_state.value for o in outcomes}
        r_status = DimensionMatchStatus.MATCH if len(set(r_map.values())) <= 1 else DimensionMatchStatus.PARTIAL_MATCH
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.REPRODUCIBILITY,
                status=r_status,
                description="Reproducibility status alignment.",
                details_by_outcome=r_map,
            )
        )

        # 10. Resolution State
        res_map = {o.outcome_id: o.outcome_state.value for o in outcomes}
        res_status = DimensionMatchStatus.MATCH if len(set(res_map.values())) <= 1 else DimensionMatchStatus.PARTIAL_MATCH
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.RESOLUTION_STATE,
                status=res_status,
                description="Consolidation outcome state alignment.",
                details_by_outcome=res_map,
            )
        )

        # 11. Review Coverage
        cov_map = {o.outcome_id: f"{len(o.human_review_decisions)}_decisions" for o in outcomes}
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.REVIEW_COVERAGE,
                status=DimensionMatchStatus.MATCH if len(set(cov_map.values())) <= 1 else DimensionMatchStatus.PARTIAL_MATCH,
                description="Review coverage decision count.",
                details_by_outcome=cov_map,
            )
        )

        # 12. Limitation Profile
        lim_map = {o.outcome_id: len(o.limitations) for o in outcomes}
        records.append(
            OutcomeComparisonDimensionRecord(
                dimension_name=ComparisonDimensionName.LIMITATION_PROFILE,
                status=DimensionMatchStatus.MATCH if len(set(lim_map.values())) <= 1 else DimensionMatchStatus.PARTIAL_MATCH,
                description="Limitation item counts.",
                details_by_outcome=lim_map,
            )
        )

        return records

    # ==========================================================================
    # Comparability State Classification
    # ==========================================================================
    def _determine_comparability(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
        dimensions: Sequence[OutcomeComparisonDimensionRecord],
    ) -> ConsolidatedOutcomeComparability:
        """Categorically determine comparability without subjective scoring."""
        if any(o.outcome_state == ConsolidationState.INVALID for o in outcomes):
            return ConsolidatedOutcomeComparability.INVALID_INPUT

        if any(o.outcome_state in (ConsolidationState.INCOMPLETE, ConsolidationState.UNAVAILABLE) for o in outcomes):
            return ConsolidatedOutcomeComparability.INCOMPLETE_EVIDENCE

        # Check symbol dimension
        sym_rec = next((d for d in dimensions if d.dimension_name == ComparisonDimensionName.SYMBOL_SUBJECT), None)
        if sym_rec and sym_rec.status == DimensionMatchStatus.MISMATCH:
            return ConsolidatedOutcomeComparability.NOT_COMPARABLE

        mismatches = [d for d in dimensions if d.status == DimensionMatchStatus.MISMATCH]
        if len(mismatches) >= 3:
            return ConsolidatedOutcomeComparability.NOT_COMPARABLE

        partial_matches = [d for d in dimensions if d.status == DimensionMatchStatus.PARTIAL_MATCH]
        if partial_matches or mismatches:
            return ConsolidatedOutcomeComparability.PARTIALLY_COMPARABLE

        return ConsolidatedOutcomeComparability.COMPARABLE

    # ==========================================================================
    # Evidence Consistency Analysis
    # ==========================================================================
    def _compare_evidence(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[List[EvidenceConsistencyItem], List[OutcomeDivergenceItem]]:
        """Compare evidence items across outcomes."""
        items: List[EvidenceConsistencyItem] = []
        divergences: List[OutcomeDivergenceItem] = []

        all_outcome_ids = [o.outcome_id for o in outcomes]
        evidence_map: Dict[str, Dict[str, Any]] = {}

        for o in outcomes:
            for ev in list(o.facts) + list(o.interpretations):
                if ev.evidence_id not in evidence_map:
                    evidence_map[ev.evidence_id] = {
                        "claim_type": ev.claim_type.value if hasattr(ev.claim_type, "value") else str(ev.claim_type),
                        "provenance_ref": ev.provenance_reference,
                        "present_in": [],
                        "review_statuses": {},
                    }
                evidence_map[ev.evidence_id]["present_in"].append(o.outcome_id)
                evidence_map[ev.evidence_id]["review_statuses"][o.outcome_id] = ev.review_status

        for ev_id, data in sorted(evidence_map.items()):
            present_in = tuple(sorted(data["present_in"]))
            missing_in = tuple(sorted([oid for oid in all_outcome_ids if oid not in present_in]))
            statuses = set(data["review_statuses"].values())

            if len(present_in) == len(all_outcome_ids):
                if len(statuses) > 1:
                    ev_status = EvidenceConsistencyStatus.CONFLICTING
                    divergences.append(
                        OutcomeDivergenceItem(
                            divergence_id=compute_deterministic_divergence_id("EV_STATUS_CONFLICT", present_in, [ev_id]),
                            source_outcome_ids=present_in,
                            source_evidence_ids=(ev_id,),
                            discrepancy_ids=(),
                            resolution_ids=(),
                            severity=DivergenceSeverity.WARNING,
                            status=DivergenceStatus.ACTIVE,
                            explanation=f"Evidence '{ev_id}' has conflicting reviewer dispositions across outcomes: {data['review_statuses']}.",
                            provenance={"evidence_id": ev_id, "provenance_ref": data["provenance_ref"]},
                        )
                    )
                else:
                    ev_status = EvidenceConsistencyStatus.SHARED
            elif len(present_in) == 1:
                ev_status = EvidenceConsistencyStatus.UNIQUE_TO_OUTCOME
            else:
                ev_status = EvidenceConsistencyStatus.MISSING

            items.append(
                EvidenceConsistencyItem(
                    evidence_id=ev_id,
                    status=ev_status,
                    present_in_outcome_ids=present_in,
                    missing_in_outcome_ids=missing_in,
                    claim_type=data["claim_type"],
                    provenance_reference=data["provenance_ref"],
                )
            )

        return items, divergences

    # ==========================================================================
    # Fact Consistency Analysis
    # ==========================================================================
    def _compare_facts(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[List[FactConsistencyItem], List[OutcomeDivergenceItem]]:
        """Compare FACT claims across outcomes without automated resolution."""
        items: List[FactConsistencyItem] = []
        divergences: List[OutcomeDivergenceItem] = []

        all_outcome_ids = [o.outcome_id for o in outcomes]
        fact_dict: Dict[str, Dict[str, Any]] = {}

        for o in outcomes:
            for f in o.facts:
                stmt = f.notes[0] if f.notes else f.evidence_id
                if stmt not in fact_dict:
                    fact_dict[stmt] = {
                        "supporting": [],
                        "opposing": [],
                        "evidence_refs": set(),
                    }
                fact_dict[stmt]["supporting"].append(o.outcome_id)
                fact_dict[stmt]["evidence_refs"].add(f.evidence_id)

        for stmt, data in sorted(fact_dict.items()):
            supp = tuple(sorted(data["supporting"]))
            ev_refs = tuple(sorted(list(data["evidence_refs"])))
            opp = tuple(sorted([oid for oid in all_outcome_ids if oid not in supp]))

            if len(supp) == len(all_outcome_ids):
                f_status = FactConsistencyStatus.CONSISTENT
            elif len(supp) > 1 and opp:
                f_status = FactConsistencyStatus.PARTIALLY_SUPPORTED
            else:
                f_status = FactConsistencyStatus.CONSISTENT

            items.append(
                FactConsistencyItem(
                    fact_statement=stmt,
                    status=f_status,
                    supporting_outcome_ids=supp,
                    opposing_outcome_ids=opp,
                    evidence_refs=ev_refs,
                )
            )

        return items, divergences

    # ==========================================================================
    # Interpretation Consistency Analysis
    # ==========================================================================
    def _compare_interpretations(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[List[InterpretationConsistencyItem], List[OutcomeDivergenceItem]]:
        """Compare analytical INTERPRETATIONS without conflating with fact contradictions."""
        items: List[InterpretationConsistencyItem] = []
        divergences: List[OutcomeDivergenceItem] = []

        interp_topics: Dict[str, Dict[str, str]] = {}
        for o in outcomes:
            for interp in o.interpretations:
                topic = interp.evidence_id
                note_text = interp.notes[0] if interp.notes else "INTERPRETATION"
                if topic not in interp_topics:
                    interp_topics[topic] = {}
                interp_topics[topic][o.outcome_id] = note_text

        for topic, outcome_map in sorted(interp_topics.items()):
            unique_notes = set(outcome_map.values())
            if len(unique_notes) == 1 and len(outcome_map) == len(outcomes):
                stat = InterpretationConsistencyStatus.SAME_INTERPRETATION
            elif len(unique_notes) > 1:
                stat = InterpretationConsistencyStatus.DIFFERENT_INTERPRETATION
                divergences.append(
                    OutcomeDivergenceItem(
                        divergence_id=compute_deterministic_divergence_id("INTERP_DIFF", list(outcome_map.keys()), [topic]),
                        source_outcome_ids=tuple(sorted(outcome_map.keys())),
                        source_evidence_ids=(topic,),
                        discrepancy_ids=(),
                        resolution_ids=(),
                        severity=DivergenceSeverity.INFO,
                        status=DivergenceStatus.ACTIVE,
                        explanation=f"Outcomes have differing interpretations on topic '{topic}': {outcome_map}.",
                    )
                )
            else:
                stat = InterpretationConsistencyStatus.PARTIALLY_ALIGNED

            items.append(
                InterpretationConsistencyItem(
                    interpretation_topic=topic,
                    status=stat,
                    outcome_interpretations=outcome_map,
                )
            )

        return items, divergences

    # ==========================================================================
    # Human Review Consistency Analysis
    # ==========================================================================
    def _compare_human_reviews(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[List[HumanReviewConsistencyItem], List[OutcomeDivergenceItem]]:
        """Compare explicit human review decisions across outcomes."""
        items: List[HumanReviewConsistencyItem] = []
        divergences: List[OutcomeDivergenceItem] = []

        decisions_by_case: Dict[str, Dict[str, str]] = {}
        reviewer_by_case: Dict[str, Set[str]] = {}
        evidence_by_case: Dict[str, Set[str]] = {}

        for o in outcomes:
            for dec in o.human_review_decisions:
                case_id = dec.source_resolution_case_id
                if case_id not in decisions_by_case:
                    decisions_by_case[case_id] = {}
                    reviewer_by_case[case_id] = set()
                    evidence_by_case[case_id] = set()
                decisions_by_case[case_id][o.outcome_id] = dec.decision_text
                reviewer_by_case[case_id].add(dec.reviewer_id)
                evidence_by_case[case_id].update(dec.supporting_evidence_ids)

        for case_id, o_dec_map in sorted(decisions_by_case.items()):
            unique_texts = set(o_dec_map.values())
            if len(unique_texts) == 1 and len(o_dec_map) == len(outcomes):
                stat = HumanReviewConsistencyStatus.SAME_REVIEW_OUTCOME
            elif len(unique_texts) > 1:
                stat = HumanReviewConsistencyStatus.DIFFERENT_REVIEW_OUTCOME
                divergences.append(
                    OutcomeDivergenceItem(
                        divergence_id=compute_deterministic_divergence_id("HUMAN_DEC_DIFF", list(o_dec_map.keys()), [case_id]),
                        source_outcome_ids=tuple(sorted(o_dec_map.keys())),
                        source_evidence_ids=tuple(sorted(evidence_by_case[case_id])),
                        discrepancy_ids=(),
                        resolution_ids=(case_id,),
                        severity=DivergenceSeverity.WARNING,
                        status=DivergenceStatus.ACTIVE,
                        explanation=f"Human review decisions differ across outcomes for case '{case_id}': {o_dec_map}.",
                    )
                )
            else:
                stat = HumanReviewConsistencyStatus.PARTIALLY_ALIGNED

            items.append(
                HumanReviewConsistencyItem(
                    decision_topic=case_id,
                    status=stat,
                    outcome_decisions=o_dec_map,
                    supporting_evidence_ids=tuple(sorted(evidence_by_case[case_id])),
                    reviewer_ids=tuple(sorted(reviewer_by_case[case_id])),
                )
            )

        return items, divergences

    # ==========================================================================
    # Review Coverage Analysis
    # ==========================================================================
    def _analyze_review_coverage(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[ReviewCoverageSummary, List[OutcomeDivergenceItem], List[OutcomeComparisonLimitationItem]]:
        """Deterministic review coverage aggregation without invented percentage scores."""
        divergences: List[OutcomeDivergenceItem] = []
        limitations: List[OutcomeComparisonLimitationItem] = []

        ev_reviewed_total = 0
        ev_unreviewed_total = 0
        disc_resolved_total = 0
        disc_unresolved_total = 0
        limits_acknowledged_total = 0

        cov_by_outcome: Dict[str, str] = {}

        for o in outcomes:
            rev_decisions = len(o.human_review_decisions)
            ev_reviewed_total += rev_decisions
            unrev = len(o.interpretations)
            ev_unreviewed_total += unrev
            disc_resolved_total += len(o.resolved_items)
            disc_unresolved_total += len(o.unresolved_items)
            limits_acknowledged_total += len(o.limitations)

            if len(o.unresolved_items) == 0 and rev_decisions > 0:
                cov_by_outcome[o.outcome_id] = CoverageStatus.COMPLETE.value
            elif rev_decisions > 0:
                cov_by_outcome[o.outcome_id] = CoverageStatus.PARTIAL.value
            else:
                cov_by_outcome[o.outcome_id] = CoverageStatus.MINIMAL.value

        all_statuses = set(cov_by_outcome.values())
        if all_statuses == {CoverageStatus.COMPLETE.value}:
            overall = CoverageStatus.COMPLETE
        elif CoverageStatus.MINIMAL.value in all_statuses or CoverageStatus.NONE.value in all_statuses:
            overall = CoverageStatus.PARTIAL
            limitations.append(
                OutcomeComparisonLimitationItem(
                    limitation_type="INCOMPLETE_REVIEW_COVERAGE",
                    description=f"{disc_unresolved_total} unresolved discrepancies remain unreviewed across outcomes.",
                    affected_outcomes=tuple([oid for oid, c in cov_by_outcome.items() if c != CoverageStatus.COMPLETE.value]),
                    severity="WARNING",
                )
            )
        else:
            overall = CoverageStatus.PARTIAL

        all_prov_verified = all(
            len(o.provenance_summary.get("report_lineage", {})) > 0 for o in outcomes
        )
        all_repro_checked = all(
            o.reproducibility_summary.reproducibility_state != ReproducibilityComparisonStatus.UNKNOWN.value for o in outcomes
        )
        all_quality_checked = all(
            o.quality_summary.overall_quality_state != "UNKNOWN" for o in outcomes
        )

        summary = ReviewCoverageSummary(
            overall_coverage=overall,
            evidence_reviewed_count=ev_reviewed_total,
            evidence_unreviewed_count=ev_unreviewed_total,
            discrepancies_reviewed_count=disc_resolved_total,
            discrepancies_unresolved_count=disc_unresolved_total,
            limitations_acknowledged_count=limits_acknowledged_total,
            provenance_verified=all_prov_verified,
            reproducibility_checked=all_repro_checked,
            quality_checked=all_quality_checked,
            coverage_by_outcome=cov_by_outcome,
        )

        return summary, divergences, limitations

    # ==========================================================================
    # Conflict Analysis
    # ==========================================================================
    def _analyze_conflicts(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[Dict[str, Any], List[OutcomeDivergenceItem]]:
        """Identify active, deferred, and repeated conflicts across outcomes."""
        divergences: List[OutcomeDivergenceItem] = []

        active_conflicts = 0
        deferred_conflicts = 0
        resolved_conflicts = 0
        conflict_ids_seen: Dict[str, List[str]] = {}

        for o in outcomes:
            for conf in o.conflicts:
                if conf.conflict_id not in conflict_ids_seen:
                    conflict_ids_seen[conf.conflict_id] = []
                conflict_ids_seen[conf.conflict_id].append(o.outcome_id)

                if conf.is_active:
                    active_conflicts += 1
                elif conf.reconciliation_outcome == "DEFERRED":
                    deferred_conflicts += 1
                else:
                    resolved_conflicts += 1

        # Check for repeated conflicts across multiple investigations
        for conf_id, oids in conflict_ids_seen.items():
            if len(oids) > 1:
                divergences.append(
                    OutcomeDivergenceItem(
                        divergence_id=compute_deterministic_divergence_id("REPEATED_CONFLICT", oids, [conf_id]),
                        source_outcome_ids=tuple(sorted(oids)),
                        source_evidence_ids=(),
                        discrepancy_ids=(conf_id,),
                        resolution_ids=(),
                        severity=DivergenceSeverity.WARNING,
                        status=DivergenceStatus.ACTIVE,
                        explanation=f"Conflict '{conf_id}' reoccurs across {len(oids)} outcomes.",
                    )
                )

        summary = {
            "active_conflicts": active_conflicts,
            "deferred_conflicts": deferred_conflicts,
            "resolved_conflicts": resolved_conflicts,
            "repeated_conflicts_count": sum(1 for oids in conflict_ids_seen.values() if len(oids) > 1),
        }

        return summary, divergences

    # ==========================================================================
    # Quality / Provenance / Reproducibility Comparison
    # ==========================================================================
    def _compare_quality(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[Dict[str, Any], List[OutcomeComparisonLimitationItem]]:
        """Compare data quality across outcomes."""
        limits: List[OutcomeComparisonLimitationItem] = []
        q_states = {o.outcome_id: o.quality_summary.overall_quality_state for o in outcomes}

        if any(s == "INVALID" for s in q_states.values()):
            q_stat = QualityComparisonStatus.INVALID
            limits.append(
                OutcomeComparisonLimitationItem(
                    limitation_type="INVALID_SOURCE_QUALITY",
                    description="One or more outcomes have INVALID source data quality.",
                    affected_outcomes=tuple([oid for oid, s in q_states.items() if s == "INVALID"]),
                    severity="ERROR",
                )
            )
        elif any(s == "DEGRADED" for s in q_states.values()):
            q_stat = QualityComparisonStatus.DEGRADED
            limits.append(
                OutcomeComparisonLimitationItem(
                    limitation_type="DEGRADED_SOURCE_QUALITY",
                    description="Degraded data quality present in one or more outcomes.",
                    affected_outcomes=tuple([oid for oid, s in q_states.items() if s == "DEGRADED"]),
                    severity="WARNING",
                )
            )
        elif len(set(q_states.values())) == 1:
            q_stat = QualityComparisonStatus.SAME
        else:
            q_stat = QualityComparisonStatus.DIFFERENT

        return {"status": q_stat.value, "quality_by_outcome": q_states}, limits

    def _compare_provenance(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[Dict[str, Any], List[OutcomeComparisonLimitationItem]]:
        """Compare provenance across outcomes."""
        limits: List[OutcomeComparisonLimitationItem] = []
        lineage_depths = {o.outcome_id: len(o.provenance_summary.get("report_lineage", {})) for o in outcomes}

        if any(d == 0 for d in lineage_depths.values()):
            stat = ProvenanceComparisonStatus.MISSING
            limits.append(
                OutcomeComparisonLimitationItem(
                    limitation_type="MISSING_PROVENANCE",
                    description="One or more outcomes lack complete provenance lineage.",
                    affected_outcomes=tuple([oid for oid, d in lineage_depths.items() if d == 0]),
                    severity="WARNING",
                )
            )
        elif len(set(lineage_depths.values())) == 1:
            stat = ProvenanceComparisonStatus.CONSISTENT
        else:
            stat = ProvenanceComparisonStatus.PARTIALLY_TRACEABLE

        return {"status": stat.value, "lineage_depth_by_outcome": lineage_depths}, limits

    def _compare_reproducibility(
        self,
        outcomes: Sequence[ConsolidatedInvestigationOutcome],
    ) -> Tuple[Dict[str, Any], List[OutcomeComparisonLimitationItem]]:
        """Compare reproducibility across outcomes."""
        limits: List[OutcomeComparisonLimitationItem] = []
        r_states = {o.outcome_id: o.reproducibility_summary.reproducibility_state.value for o in outcomes}

        if len(set(r_states.values())) == 1:
            stat = ReproducibilityComparisonStatus.CONSISTENT
        elif any(s == "NON_REPRODUCIBLE" for s in r_states.values()):
            stat = ReproducibilityComparisonStatus.DIVERGENT
            limits.append(
                OutcomeComparisonLimitationItem(
                    limitation_type="NON_REPRODUCIBLE_SOURCE",
                    description="One or more outcomes cannot be independently reproduced.",
                    affected_outcomes=tuple([oid for oid, s in r_states.items() if s == "NON_REPRODUCIBLE"]),
                    severity="WARNING",
                )
            )
        else:
            stat = ReproducibilityComparisonStatus.PARTIALLY_REPRODUCIBLE

        return {"status": stat.value, "reproducibility_by_outcome": r_states}, limits

    # ==========================================================================
    # Voice-Safe Summary Script Generation
    # ==========================================================================
    def _generate_voice_summary(
        self,
        comparison_id: str,
        loaded_outcomes: Sequence[ConsolidatedInvestigationOutcome],
        comparability_state: ConsolidatedOutcomeComparability,
        review_coverage: ReviewCoverageSummary,
        divergences: Sequence[OutcomeDivergenceItem],
    ) -> OutcomeComparisonVoiceSummary:
        """Deterministic speech script summarizing comparison without trading directives."""
        count = len(loaded_outcomes)
        lines: List[str] = []

        if comparability_state == ConsolidatedOutcomeComparability.COMPARABLE:
            lines.append(f"{count} investigations share the same evidence scope.")
        elif comparability_state == ConsolidatedOutcomeComparability.PARTIALLY_COMPARABLE:
            lines.append(f"{count} investigations are partially comparable with differing scopes.")
        elif comparability_state == ConsolidatedOutcomeComparability.NOT_COMPARABLE:
            lines.append(f"{count} investigations are not comparable due to scope divergence.")
        else:
            lines.append("Investigation outcome comparison input is incomplete.")

        if any(d.severity in (DivergenceSeverity.ERROR, DivergenceSeverity.CRITICAL) for d in divergences):
            lines.append("Conflicting evidence remains present across investigations.")
        elif divergences:
            lines.append("The investigations differ in analytical interpretation.")

        if review_coverage.overall_coverage != CoverageStatus.COMPLETE:
            lines.append("Human review coverage is incomplete.")
        else:
            lines.append("Human review coverage is complete across all investigations.")

        full_script = " ".join(lines)
        return OutcomeComparisonVoiceSummary(
            comparison_id=comparison_id,
            headline="Consolidated outcome comparison complete.",
            script=full_script,
            voice_safe=True,
        )

    # ==========================================================================
    # Persistence & Lookup
    # ==========================================================================
    def _persist_to_statestore(self, comparison: ConsolidatedOutcomeComparison) -> None:
        """Persist comparison record to StateStore with bounded history."""
        if not self.state_store:
            return

        try:
            c_dict = comparison.to_dict()
            self.state_store.set("consolidated_outcome_comparison:latest", c_dict)
            self.state_store.set(f"consolidated_outcome_comparison:{comparison.comparison_id}", c_dict)

            history = self.state_store.get("consolidated_outcome_comparison:history", [])
            if not isinstance(history, list):
                history = []
            history.append(c_dict)
            if len(history) > self.max_history:
                history = history[-self.max_history:]
            self.state_store.set("consolidated_outcome_comparison:history", history)
        except Exception as exc:
            logger.debug("Failed to persist comparison to StateStore: %s", exc)

    def _fetch_outcome(self, outcome_id: str) -> Optional[ConsolidatedInvestigationOutcome]:
        """Fetch outcome from consolidation service or StateStore."""
        if self.consolidation_service and hasattr(self.consolidation_service, "get_outcome"):
            out = self.consolidation_service.get_outcome(outcome_id)
            if out is not None:
                return out

        if self.state_store:
            raw = self.state_store.get(f"investigation_outcome:{outcome_id}")
            if isinstance(raw, dict):
                # We could reconstruct model if needed, but if consolidation_service is attached it will return model
                pass
        return None

    def get_comparison(self, comparison_id: str) -> Optional[ConsolidatedOutcomeComparison]:
        """Retrieve comparison by ID."""
        if not comparison_id or not isinstance(comparison_id, str):
            return None
        clean_id = comparison_id.strip()
        if not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            if clean_id in self._comparisons:
                return self._comparisons[clean_id]

        if self.state_store:
            raw = self.state_store.get(f"consolidated_outcome_comparison:{clean_id}")
            if isinstance(raw, dict):
                pass
        return None

    def list_comparisons(
        self,
        comparability_state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ConsolidatedOutcomeComparison]:
        """List comparisons from memory or StateStore."""
        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)

        with self._lock:
            items = list(self._comparisons.values())

        if comparability_state:
            items = [c for c in items if c.comparability_state.value == comparability_state.upper()]

        return items[clamped_offset : clamped_offset + clamped_limit]

    def get_comparison_dimensions(self, comparison_id: str) -> Optional[List[OutcomeComparisonDimensionRecord]]:
        """Retrieve comparison dimension records."""
        comp = self.get_comparison(comparison_id)
        if comp:
            return list(comp.dimensions)
        return None

    def get_comparison_evidence(self, comparison_id: str) -> Optional[List[EvidenceConsistencyItem]]:
        """Retrieve evidence consistency items."""
        comp = self.get_comparison(comparison_id)
        if comp:
            return list(comp.evidence_consistency)
        return None

    def get_comparison_review_coverage(self, comparison_id: str) -> Optional[ReviewCoverageSummary]:
        """Retrieve review coverage summary."""
        comp = self.get_comparison(comparison_id)
        if comp:
            return comp.review_coverage
        return None

    def get_comparison_divergences(self, comparison_id: str) -> Optional[List[OutcomeDivergenceItem]]:
        """Retrieve identified divergences."""
        comp = self.get_comparison(comparison_id)
        if comp:
            return list(comp.divergence_items)
        return None

    def get_comparison_provenance(self, comparison_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve provenance comparison summary and traceability."""
        comp = self.get_comparison(comparison_id)
        if comp:
            return {
                "provenance_comparison": comp.provenance_comparison,
                "reproducibility_comparison": comp.reproducibility_comparison,
                "traceability": comp.traceability,
            }
        return None
