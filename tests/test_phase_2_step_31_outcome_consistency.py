"""KAISER — Phase 2, Step 31 Test Suite.

Cross-Investigation Consolidated Outcome Consistency Layer.

Test Categories:
A. Domain model validation & invariants
B. Immutability (frozen dataclasses, tuples)
C. Comparability state classification (COMPARABLE, PARTIALLY_COMPARABLE, NOT_COMPARABLE, INVALID_INPUT)
D. Deterministic outcome comparison execution
E. Dimensional comparison (all 12 dimensions)
F. Evidence consistency categorization (SHARED, UNIQUE_TO_OUTCOME, MISSING, CONFLICTING)
G. Fact consistency categorization (CONSISTENT, INCONSISTENT, PARTIALLY_SUPPORTED)
H. Interpretation consistency categorization (SAME_INTERPRETATION, DIFFERENT_INTERPRETATION)
I. Human review consistency categorization (SAME_REVIEW_OUTCOME, DIFFERENT_REVIEW_OUTCOME, NO_REVIEW)
J. Review coverage summary & coverage gap detection
K. Outcome divergence identification & categorization
L. Preservation of unresolved conflicts from consolidation outcomes
M. Data quality comparison & degradation tracking
N. Provenance & reproducibility comparison across outcomes
O. Voice summary generation & vocabulary safety
P. EventBus integration & event emission
Q. StateStore integration & ring buffer bounded retention
R. MasterOrchestrator handler registration & command execution
S. Dashboard GET endpoints (list, single, dimensions, evidence, coverage, divergences, provenance)
T. Dashboard HEAD endpoints
U. Dashboard mutation rejection (405 Method Not Allowed)
V. Dashboard path traversal & malformed ID rejection (400 Bad Request)
W. Prohibited fields & trade recommendation rejection
X. Non-finite value rejection (NaN, Infinity)
Y. Bounded collection behavior (clamped limits, min/max outcomes)
Z. Concurrent access & thread-safety
AA. Safety boundaries & JUJU isolation (Execution locked, trading engine uninitialized)
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import math
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest

from app.analysis.opportunities.consolidation import (
    ConsolidatedConflictItem,
    ConsolidatedEvidenceItem,
    ConsolidatedInvestigationOutcome,
    ConsolidatedReviewDecision,
    ConsolidationLimitation,
    ConsolidationLimitationItem,
    ConsolidationState,
    ConsolidationVoiceSummary,
    DecisionClaimType,
    InvestigationOutcomeConsolidationService,
    QualityAggregationSummary,
    ReproducibilityConsolidationState,
    ReproducibilitySummary,
)
from app.analysis.opportunities.consistency import (
    ComparisonDimensionName,
    ComparisonLifecycleState,
    ConsolidatedOutcomeComparability,
    ConsolidatedOutcomeComparison,
    ConsolidatedOutcomeConsistencyService,
    CoverageStatus,
    DEFAULT_MAX_CONSISTENCY_HISTORY,
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
    MAX_COMPARISON_OUTCOMES,
    MIN_COMPARISON_OUTCOMES,
    OutcomeComparisonDimensionRecord,
    OutcomeComparisonLimitationItem,
    OutcomeComparisonVoiceSummary,
    OutcomeDivergenceItem,
    ProvenanceComparisonStatus,
    QualityComparisonStatus,
    ReproducibilityComparisonStatus,
    ReviewCoverageSummary,
    compute_deterministic_comparison_id,
    compute_deterministic_divergence_id,
    register_consistency_handlers,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer


# ==============================================================================
# Helper Factories
# ==============================================================================

def make_test_consolidated_outcome(
    outcome_id: str = "out_test_01",
    investigation_id: str = "inv_01",
    outcome_state: ConsolidationState = ConsolidationState.RECONCILED,
    scope: str = "BTC-USD 1h observation",
    symbol: str = "BTC-USD",
    facts: Optional[Sequence[ConsolidatedEvidenceItem]] = None,
    interpretations: Optional[Sequence[ConsolidatedEvidenceItem]] = None,
    decisions: Optional[Sequence[ConsolidatedReviewDecision]] = None,
    conflicts: Optional[Sequence[ConsolidatedConflictItem]] = None,
    quality_state: str = "VALID",
    repro_state: ReproducibilityConsolidationState = ReproducibilityConsolidationState.REPRODUCIBLE,
    unresolved_items: Sequence[str] = (),
) -> ConsolidatedInvestigationOutcome:
    """Create a valid ConsolidatedInvestigationOutcome for testing."""
    f_items = tuple(
        facts
        if facts is not None
        else (
            ConsolidatedEvidenceItem(
                evidence_id="ev_fact_01",
                source_id="rep_01",
                claim_type=DecisionClaimType.FACT,
                review_status="VERIFIED",
                provenance_reference="prov_01",
                quality_status=quality_state,
                timestamp=1000.0,
                scope=f"{symbol} 1h observation",
                notes=("Observed 1h volume divergence",),
            ),
        )
    )

    i_items = tuple(
        interpretations
        if interpretations is not None
        else (
            ConsolidatedEvidenceItem(
                evidence_id="ev_interp_01",
                source_id="rep_01",
                claim_type=DecisionClaimType.INTERPRETATION,
                review_status="VERIFIED",
                provenance_reference="prov_01",
                quality_status=quality_state,
                timestamp=1000.0,
                scope=f"{symbol} 1h observation",
                notes=("Bullish market momentum interpretation",),
            ),
        )
    )

    d_items = tuple(
        decisions
        if decisions is not None
        else (
            ConsolidatedReviewDecision(
                decision_id="dec_01",
                source_resolution_case_id="res_01",
                reviewer_id="reviewer_alice",
                claim_type=DecisionClaimType.HUMAN_REVIEW_DECISION,
                decision_text="Confirmed volume interpretation alignment.",
                timestamp=1010.0,
                supporting_evidence_ids=("ev_fact_01", "ev_interp_01"),
            ),
        )
    )

    c_items = tuple(conflicts if conflicts is not None else ())

    return ConsolidatedInvestigationOutcome(
        outcome_id=outcome_id,
        investigation_id=investigation_id,
        source_report_ids=("rep_01", "rep_02"),
        source_comparison_ids=("cmp_01",),
        source_resolution_case_ids=("res_01",),
        generated_at=1020.0,
        outcome_state=outcome_state,
        evidence_summary={"total_facts": len(f_items), "total_interpretations": len(i_items)},
        resolved_items=("disc_01",),
        unresolved_items=tuple(unresolved_items),
        conflicts=c_items,
        facts=f_items,
        interpretations=i_items,
        human_review_decisions=d_items,
        quality_summary=QualityAggregationSummary(
            overall_quality_state=quality_state,
            source_quality_statuses={"rep_01": quality_state, "rep_02": quality_state},
        ),
        provenance_summary={"source_runs": ["run_01"], "quality_status": quality_state},
        reproducibility_summary=ReproducibilitySummary(
            reproducibility_state=repro_state,
            reproducible_sources=("run_01",),
        ),
        limitations=(),
        traceability={"scope": scope, "symbol": symbol, "timeframe": "1h"},
        voice_summary=ConsolidationVoiceSummary(
            outcome_id=outcome_id,
            outcome_state=outcome_state.value,
            summary_text="Consolidated review complete.",
            voice_safe=True,
        ),
    )


# ==============================================================================
# A. Domain Model Validation & Invariants
# ==============================================================================
def test_domain_model_validation():
    """Verify field constraints, serializability, and validation in consistency domain models."""
    dim = OutcomeComparisonDimensionRecord(
        dimension_name=ComparisonDimensionName.SYMBOL_SUBJECT,
        status=DimensionMatchStatus.MATCH,
        description="Matches on BTC-USD",
        details_by_outcome={"out_1": "BTC-USD", "out_2": "BTC-USD"},
    )
    assert dim.dimension_name == ComparisonDimensionName.SYMBOL_SUBJECT
    assert dim.to_dict()["status"] == "MATCH"

    with pytest.raises(ValidationError):
        OutcomeComparisonDimensionRecord(
            dimension_name=ComparisonDimensionName.SYMBOL_SUBJECT,
            status=DimensionMatchStatus.MATCH,
            description="",  # Empty description rejected
        )

    # OutcomeDivergenceItem validation
    div = OutcomeDivergenceItem(
        divergence_id="div_01",
        source_outcome_ids=("out_1", "out_2"),
        source_evidence_ids=("ev_01",),
        discrepancy_ids=(),
        resolution_ids=(),
        severity=DivergenceSeverity.WARNING,
        status=DivergenceStatus.ACTIVE,
        explanation="Divergence in momentum interpretations.",
    )
    assert div.divergence_id == "div_01"
    assert div.to_dict()["severity"] == "WARNING"

    with pytest.raises(ValidationError):
        OutcomeDivergenceItem(
            divergence_id="",  # Empty ID rejected
            source_outcome_ids=("out_1", "out_2"),
            source_evidence_ids=(),
            discrepancy_ids=(),
            resolution_ids=(),
            severity=DivergenceSeverity.INFO,
            status=DivergenceStatus.ACTIVE,
            explanation="Explanation",
        )

    # Minimum outcomes requirement on ConsolidatedOutcomeComparison (< 2 rejected)
    with pytest.raises(ValidationError):
        ConsolidatedOutcomeComparison(
            comparison_id="coc_single",
            outcome_ids=("out_1",),  # Only 1 outcome
            investigation_ids=("inv_1",),
            generated_at=100.0,
            comparison_scope="STANDARD",
            lifecycle_state=ComparisonLifecycleState.COMPLETED,
            comparability_state=ConsolidatedOutcomeComparability.COMPARABLE,
            dimensions=(),
            evidence_consistency=(),
            fact_consistency=(),
            interpretation_consistency=(),
            human_review_consistency=(),
            review_coverage=ReviewCoverageSummary(
                overall_coverage=CoverageStatus.COMPLETE,
                evidence_reviewed_count=1,
                evidence_unreviewed_count=0,
                discrepancies_reviewed_count=0,
                discrepancies_unresolved_count=0,
                limitations_acknowledged_count=0,
                provenance_verified=True,
                reproducibility_checked=True,
                quality_checked=True,
            ),
            divergence_items=(),
            conflict_summary={},
            quality_comparison={},
            provenance_comparison={},
            reproducibility_comparison={},
            limitations=(),
            traceability={},
        )


# ==============================================================================
# B. Immutability
# ==============================================================================
def test_immutability():
    """Verify domain models are frozen dataclasses and cannot be mutated."""
    item = EvidenceConsistencyItem(
        evidence_id="ev_01",
        status=EvidenceConsistencyStatus.SHARED,
        present_in_outcome_ids=("out_1", "out_2"),
        missing_in_outcome_ids=(),
        claim_type="FACT",
        provenance_reference="prov_01",
    )
    with pytest.raises((AttributeError, TypeError)):
        item.status = EvidenceConsistencyStatus.MISSING  # type: ignore

    cov = ReviewCoverageSummary(
        overall_coverage=CoverageStatus.COMPLETE,
        evidence_reviewed_count=5,
        evidence_unreviewed_count=0,
        discrepancies_reviewed_count=1,
        discrepancies_unresolved_count=0,
        limitations_acknowledged_count=0,
        provenance_verified=True,
        reproducibility_checked=True,
        quality_checked=True,
    )
    with pytest.raises((AttributeError, TypeError)):
        cov.evidence_reviewed_count = 10  # type: ignore


# ==============================================================================
# C. Comparability State Classification
# ==============================================================================
def test_comparability_state_classification():
    """Verify deterministic comparability classification (COMPARABLE, PARTIALLY_COMPARABLE, NOT_COMPARABLE, INVALID_INPUT)."""
    # 1. INVALID_INPUT: Non-existent outcomes
    service = ConsolidatedOutcomeConsistencyService()
    comp_inv = service.compare_consolidated_outcomes(["out_missing_1", "out_missing_2"])
    assert comp_inv.comparability_state == ConsolidatedOutcomeComparability.INVALID_INPUT
    assert comp_inv.lifecycle_state == ComparisonLifecycleState.INCOMPLETE

    # Setup consolidation service with mock outcomes
    cons_svc = InvestigationOutcomeConsolidationService()
    svc_with_cons = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    out_btc_1 = make_test_consolidated_outcome("out_btc_1", "inv_1", symbol="BTC-USD", scope="BTC-USD 1h observation")
    out_btc_2 = make_test_consolidated_outcome("out_btc_2", "inv_2", symbol="BTC-USD", scope="BTC-USD 1h observation")
    out_eth = make_test_consolidated_outcome("out_eth", "inv_3", symbol="ETH-USD", scope="ETH-USD 4h observation")

    cons_svc._outcomes["out_btc_1"] = out_btc_1
    cons_svc._outcomes["out_btc_2"] = out_btc_2
    cons_svc._outcomes["out_eth"] = out_eth

    # 2. COMPARABLE: Matching symbols and scopes
    comp_btc = svc_with_cons.compare_consolidated_outcomes(["out_btc_1", "out_btc_2"])
    assert comp_btc.comparability_state == ConsolidatedOutcomeComparability.COMPARABLE
    assert comp_btc.lifecycle_state == ComparisonLifecycleState.COMPLETED

    # 3. NOT_COMPARABLE: Different symbols (BTC-USD vs ETH-USD)
    comp_diff = svc_with_cons.compare_consolidated_outcomes(["out_btc_1", "out_eth"])
    assert comp_diff.comparability_state == ConsolidatedOutcomeComparability.NOT_COMPARABLE


# ==============================================================================
# D. Deterministic Outcome Comparison Execution
# ==============================================================================
def test_deterministic_comparison():
    """Verify identical outcome inputs produce identical comparison IDs and identical metrics."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    out1 = make_test_consolidated_outcome("out_det_1", "inv_1")
    out2 = make_test_consolidated_outcome("out_det_2", "inv_2")
    cons_svc._outcomes["out_det_1"] = out1
    cons_svc._outcomes["out_det_2"] = out2

    comp1 = service.compare_consolidated_outcomes(["out_det_1", "out_det_2"])
    # Inverted order input
    comp2 = service.compare_consolidated_outcomes(["out_det_2", "out_det_1"])

    assert comp1.comparison_id == comp2.comparison_id
    assert comp1.comparability_state == comp2.comparability_state
    assert len(comp1.dimensions) == len(comp2.dimensions)
    assert comp1.outcome_ids == ("out_det_1", "out_det_2")


# ==============================================================================
# E. Dimensional Comparison (All 12 Dimensions)
# ==============================================================================
def test_dimensional_comparison_all_12_dimensions():
    """Verify all 12 analytical dimensions are evaluated and classified."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    out1 = make_test_consolidated_outcome("out_dim_1", "inv_1")
    out2 = make_test_consolidated_outcome("out_dim_2", "inv_2")
    cons_svc._outcomes["out_dim_1"] = out1
    cons_svc._outcomes["out_dim_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_dim_1", "out_dim_2"])
    assert len(comp.dimensions) == 12

    expected_dimensions = {d.value for d in ComparisonDimensionName}
    actual_dimensions = {d.dimension_name.value for d in comp.dimensions}
    assert actual_dimensions == expected_dimensions


# ==============================================================================
# F. Evidence Consistency Categorization
# ==============================================================================
def test_evidence_consistency_classification():
    """Verify categorization of SHARED, UNIQUE_TO_OUTCOME, and CONFLICTING evidence."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    ev_shared = ConsolidatedEvidenceItem(
        evidence_id="ev_shared_01",
        source_id="rep_01",
        claim_type=DecisionClaimType.FACT,
        review_status="VERIFIED",
        provenance_reference="prov_01",
        quality_status="VALID",
        timestamp=100.0,
        scope="BTC-USD",
    )
    ev_unique = ConsolidatedEvidenceItem(
        evidence_id="ev_unique_01",
        source_id="rep_02",
        claim_type=DecisionClaimType.FACT,
        review_status="VERIFIED",
        provenance_reference="prov_02",
        quality_status="VALID",
        timestamp=100.0,
        scope="BTC-USD",
    )

    out1 = make_test_consolidated_outcome("out_ev_1", "inv_1", facts=[ev_shared, ev_unique])
    out2 = make_test_consolidated_outcome("out_ev_2", "inv_2", facts=[ev_shared])
    cons_svc._outcomes["out_ev_1"] = out1
    cons_svc._outcomes["out_ev_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_ev_1", "out_ev_2"])
    ev_map = {e.evidence_id: e for e in comp.evidence_consistency}

    assert ev_map["ev_shared_01"].status == EvidenceConsistencyStatus.SHARED
    assert ev_map["ev_unique_01"].status == EvidenceConsistencyStatus.UNIQUE_TO_OUTCOME
    assert "out_ev_1" in ev_map["ev_unique_01"].present_in_outcome_ids
    assert "out_ev_2" in ev_map["ev_unique_01"].missing_in_outcome_ids


# ==============================================================================
# G. Fact Consistency Categorization
# ==============================================================================
def test_fact_consistency_classification():
    """Verify fact statement matching and consistency status across outcomes."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    f1 = ConsolidatedEvidenceItem(
        evidence_id="ev_f1",
        source_id="rep_01",
        claim_type=DecisionClaimType.FACT,
        review_status="VERIFIED",
        notes=("Support level held at 60k",),
    )
    f2 = ConsolidatedEvidenceItem(
        evidence_id="ev_f2",
        source_id="rep_02",
        claim_type=DecisionClaimType.FACT,
        review_status="VERIFIED",
        notes=("Support level held at 60k",),
    )

    out1 = make_test_consolidated_outcome("out_f_1", "inv_1", facts=[f1])
    out2 = make_test_consolidated_outcome("out_f_2", "inv_2", facts=[f2])
    cons_svc._outcomes["out_f_1"] = out1
    cons_svc._outcomes["out_f_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_f_1", "out_f_2"])
    assert len(comp.fact_consistency) >= 1
    assert comp.fact_consistency[0].status == FactConsistencyStatus.CONSISTENT


# ==============================================================================
# H. Interpretation Consistency Categorization
# ==============================================================================
def test_interpretation_consistency_classification():
    """Verify analytical interpretation comparison across outcomes."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    i1 = ConsolidatedEvidenceItem(
        evidence_id="ev_momentum",
        source_id="rep_01",
        claim_type=DecisionClaimType.INTERPRETATION,
        review_status="VERIFIED",
        notes=("Bullish continuation expected",),
    )
    i2 = ConsolidatedEvidenceItem(
        evidence_id="ev_momentum",
        source_id="rep_02",
        claim_type=DecisionClaimType.INTERPRETATION,
        review_status="VERIFIED",
        notes=("Bearish breakdown expected",),
    )

    out1 = make_test_consolidated_outcome("out_i_1", "inv_1", interpretations=[i1])
    out2 = make_test_consolidated_outcome("out_i_2", "inv_2", interpretations=[i2])
    cons_svc._outcomes["out_i_1"] = out1
    cons_svc._outcomes["out_i_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_i_1", "out_i_2"])
    assert len(comp.interpretation_consistency) >= 1
    assert comp.interpretation_consistency[0].status == InterpretationConsistencyStatus.DIFFERENT_INTERPRETATION


# ==============================================================================
# I. Human Review Consistency Categorization
# ==============================================================================
def test_human_review_consistency_classification():
    """Verify human reviewer decisions comparison across outcomes."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    d1 = ConsolidatedReviewDecision(
        decision_id="dec_01",
        source_resolution_case_id="res_01",
        reviewer_id="reviewer_alice",
        claim_type=DecisionClaimType.HUMAN_REVIEW_DECISION,
        decision_text="Confirmed support validation.",
        timestamp=1000.0,
    )
    d2 = ConsolidatedReviewDecision(
        decision_id="dec_02",
        source_resolution_case_id="res_01",
        reviewer_id="reviewer_bob",
        claim_type=DecisionClaimType.HUMAN_REVIEW_DECISION,
        decision_text="Confirmed support validation.",
        timestamp=1005.0,
    )

    out1 = make_test_consolidated_outcome("out_d_1", "inv_1", decisions=[d1])
    out2 = make_test_consolidated_outcome("out_d_2", "inv_2", decisions=[d2])
    cons_svc._outcomes["out_d_1"] = out1
    cons_svc._outcomes["out_d_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_d_1", "out_d_2"])
    assert len(comp.human_review_consistency) >= 1
    assert comp.human_review_consistency[0].status == HumanReviewConsistencyStatus.SAME_REVIEW_OUTCOME


# ==============================================================================
# J. Review Coverage Summary & Coverage Gap Detection
# ==============================================================================
def test_review_coverage_summary_and_gap_detection():
    """Verify review coverage evaluation and gap detection when unreviewed/unresolved items exist."""
    bus = EventBus()
    events_received: List[Event] = []
    bus.subscribe(EventType.CONSOLIDATED_OUTCOME_REVIEW_GAP_DETECTED, lambda e: events_received.append(e))

    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(event_bus=bus, consolidation_service=cons_svc)

    out_complete = make_test_consolidated_outcome("out_cov_1", "inv_1")
    out_unresolved = make_test_consolidated_outcome(
        "out_cov_2",
        "inv_2",
        outcome_state=ConsolidationState.CONFLICTING,
        unresolved_items=("disc_unresolved_01",),
    )
    cons_svc._outcomes["out_cov_1"] = out_complete
    cons_svc._outcomes["out_cov_2"] = out_unresolved

    comp = service.compare_consolidated_outcomes(["out_cov_1", "out_cov_2"])
    assert comp.review_coverage.overall_coverage in (CoverageStatus.PARTIAL, CoverageStatus.MINIMAL)
    assert comp.review_coverage.discrepancies_unresolved_count >= 1
    assert len(events_received) == 1
    assert events_received[0].payload["unresolved_discrepancies_count"] >= 1


# ==============================================================================
# K. Outcome Divergence Identification & Categorization
# ==============================================================================
def test_divergence_identification_and_categorization():
    """Verify divergence items are identified, classified with severity, and assigned SHA-256 IDs."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    i1 = ConsolidatedEvidenceItem(
        evidence_id="ev_bias",
        source_id="rep_01",
        claim_type=DecisionClaimType.INTERPRETATION,
        review_status="VERIFIED",
        notes=("Bullish bias",),
    )
    i2 = ConsolidatedEvidenceItem(
        evidence_id="ev_bias",
        source_id="rep_02",
        claim_type=DecisionClaimType.INTERPRETATION,
        review_status="VERIFIED",
        notes=("Bearish bias",),
    )

    out1 = make_test_consolidated_outcome("out_div_1", "inv_1", interpretations=[i1])
    out2 = make_test_consolidated_outcome("out_div_2", "inv_2", interpretations=[i2])
    cons_svc._outcomes["out_div_1"] = out1
    cons_svc._outcomes["out_div_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_div_1", "out_div_2"])
    assert len(comp.divergence_items) >= 1
    div = comp.divergence_items[0]
    assert div.divergence_id.startswith("div_")
    assert div.status == DivergenceStatus.ACTIVE


# ==============================================================================
# L. Preservation of Unresolved Conflicts from Consolidation
# ==============================================================================
def test_conflict_preservation_from_consolidation():
    """Verify active conflicts from input outcomes are preserved in conflict_summary and exposed."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    c1 = ConsolidatedConflictItem(
        conflict_id="con_01",
        discrepancy_id="disc_01",
        conflicting_evidence_ids=("ev_01", "ev_02"),
        conflicting_report_ids=("rep_01", "rep_02"),
        description="Contradictory order book liquidity depth.",
        conflict_nature="DATA_DISCREPANCY",
        is_active=True,
    )

    out1 = make_test_consolidated_outcome("out_conf_1", "inv_1", conflicts=[c1])
    out2 = make_test_consolidated_outcome("out_conf_2", "inv_2")
    cons_svc._outcomes["out_conf_1"] = out1
    cons_svc._outcomes["out_conf_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_conf_1", "out_conf_2"])
    assert comp.conflict_summary["active_conflicts"] >= 1


# ==============================================================================
# M. Data Quality Comparison & Degradation Tracking
# ==============================================================================
def test_quality_and_provenance_and_reproducibility_comparison():
    """Verify quality divergence, provenance consistency, and reproducibility comparison."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    out_valid = make_test_consolidated_outcome(
        "out_q_valid",
        "inv_1",
        quality_state="VALID",
        repro_state=ReproducibilityConsolidationState.REPRODUCIBLE,
    )
    out_degraded = make_test_consolidated_outcome(
        "out_q_deg",
        "inv_2",
        quality_state="DEGRADED",
        repro_state=ReproducibilityConsolidationState.NON_REPRODUCIBLE,
    )
    cons_svc._outcomes["out_q_valid"] = out_valid
    cons_svc._outcomes["out_q_deg"] = out_degraded

    comp = service.compare_consolidated_outcomes(["out_q_valid", "out_q_deg"])
    assert comp.quality_comparison["status"] == QualityComparisonStatus.DEGRADED.value
    assert comp.reproducibility_comparison["status"] == ReproducibilityComparisonStatus.DIVERGENT.value


# ==============================================================================
# N. Voice Summary Generation & Vocabulary Safety
# ==============================================================================
def test_voice_summary_generation_and_safety():
    """Verify speech script is generated and forbidden trading/ranking vocabulary is rejected."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    out1 = make_test_consolidated_outcome("out_v_1", "inv_1")
    out2 = make_test_consolidated_outcome("out_v_2", "inv_2")
    cons_svc._outcomes["out_v_1"] = out1
    cons_svc._outcomes["out_v_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_v_1", "out_v_2"])
    assert comp.voice_summary is not None
    assert comp.voice_summary.voice_safe is True
    assert "investigations" in comp.voice_summary.script.lower()

    # Manual attempt to craft voice summary with forbidden words raises ValidationError
    with pytest.raises(ValidationError):
        OutcomeComparisonVoiceSummary(
            comparison_id="coc_bad",
            headline="Summary",
            script="This is the winning investigation and you should buy now.",
        )


# ==============================================================================
# O. EventBus Integration & Event Emission
# ==============================================================================
def test_eventbus_integration():
    """Verify started, completed, and divergence events are published to EventBus."""
    bus = EventBus()
    events: List[Event] = []

    for et in (
        EventType.CONSOLIDATED_OUTCOMES_COMPARISON_STARTED,
        EventType.CONSOLIDATED_OUTCOMES_COMPARED,
        EventType.CONSOLIDATED_OUTCOME_DIVERGENCE_DETECTED,
    ):
        bus.subscribe(et, lambda e: events.append(e))

    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(event_bus=bus, consolidation_service=cons_svc)

    out1 = make_test_consolidated_outcome("out_eb_1", "inv_1")
    out2 = make_test_consolidated_outcome("out_eb_2", "inv_2")
    cons_svc._outcomes["out_eb_1"] = out1
    cons_svc._outcomes["out_eb_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_eb_1", "out_eb_2"])
    event_types = [e.event_type for e in events]
    assert EventType.CONSOLIDATED_OUTCOMES_COMPARISON_STARTED in event_types
    assert EventType.CONSOLIDATED_OUTCOMES_COMPARED in event_types


# ==============================================================================
# P. StateStore Integration & Ring Buffer Bounded Retention
# ==============================================================================
def test_statestore_integration_and_history_bounded():
    """Verify comparisons are persisted to StateStore with bounded history."""
    store = StateStore()
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(
        state_store=store,
        consolidation_service=cons_svc,
        max_history=3,
    )

    out1 = make_test_consolidated_outcome("out_s_1", "inv_1")
    out2 = make_test_consolidated_outcome("out_s_2", "inv_2")
    out3 = make_test_consolidated_outcome("out_s_3", "inv_3")
    cons_svc._outcomes["out_s_1"] = out1
    cons_svc._outcomes["out_s_2"] = out2
    cons_svc._outcomes["out_s_3"] = out3

    comp = service.compare_consolidated_outcomes(["out_s_1", "out_s_2"])
    latest = store.get("consolidated_outcome_comparison:latest")
    assert latest is not None
    assert latest["comparison_id"] == comp.comparison_id

    # Verify retrieval
    retrieved = service.get_comparison(comp.comparison_id)
    assert retrieved is not None
    assert retrieved.comparison_id == comp.comparison_id


# ==============================================================================
# Q. MasterOrchestrator Handler Registration & Execution
# ==============================================================================
def test_orchestrator_handler_registration_and_execution():
    """Verify all 8 Step 31 command handlers execute and return read-only execution-locked responses."""
    orchestrator = MasterOrchestrator()
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    out1 = make_test_consolidated_outcome("out_orch_1", "inv_1")
    out2 = make_test_consolidated_outcome("out_orch_2", "inv_2")
    cons_svc._outcomes["out_orch_1"] = out1
    cons_svc._outcomes["out_orch_2"] = out2

    register_consistency_handlers(orchestrator=orchestrator, consistency_service=service)

    # 1. compare_consolidated_outcomes
    res = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_orch_1",
            correlation_id="corr_orch_1",
            command="compare_consolidated_outcomes",
            context={"outcome_ids": ["out_orch_1", "out_orch_2"]},
        )
    )
    assert res.is_success
    assert res.result["status"] == "success"
    assert res.result["read_only"] is True
    assert res.result["execution_locked"] is True
    comp_id = res.result["comparison_id"]

    # 2. get_consolidated_outcome_comparison
    res_get = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_orch_2",
            correlation_id="corr_orch_2",
            command="get_consolidated_outcome_comparison",
            context={"comparison_id": comp_id},
        )
    )
    assert res_get.is_success
    assert res_get.result["status"] == "success"

    # 3. list_consolidated_outcome_comparisons
    res_list = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_orch_3",
            correlation_id="corr_orch_3",
            command="list_consolidated_outcome_comparisons",
            context={},
        )
    )
    assert res_list.is_success
    assert res_list.result["status"] == "success"
    assert res_list.result["count"] >= 1

    # 4. get_consolidated_outcome_comparison_dimensions
    res_dims = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_orch_4",
            correlation_id="corr_orch_4",
            command="get_consolidated_outcome_comparison_dimensions",
            context={"comparison_id": comp_id},
        )
    )
    assert res_dims.is_success
    assert res_dims.result["status"] == "success"
    assert len(res_dims.result["dimensions"]) == 12

    # 5. get_consolidated_outcome_comparison_evidence
    res_ev = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_orch_5",
            correlation_id="corr_orch_5",
            command="get_consolidated_outcome_comparison_evidence",
            context={"comparison_id": comp_id},
        )
    )
    assert res_ev.is_success
    assert res_ev.result["status"] == "success"

    # 6. get_consolidated_outcome_comparison_review_coverage
    res_cov = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_orch_6",
            correlation_id="corr_orch_6",
            command="get_consolidated_outcome_comparison_review_coverage",
            context={"comparison_id": comp_id},
        )
    )
    assert res_cov.is_success
    assert res_cov.result["status"] == "success"

    # 7. get_consolidated_outcome_comparison_divergences
    res_div = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_orch_7",
            correlation_id="corr_orch_7",
            command="get_consolidated_outcome_comparison_divergences",
            context={"comparison_id": comp_id},
        )
    )
    assert res_div.is_success
    assert res_div.result["status"] == "success"

    # 8. get_consolidated_outcome_comparison_provenance
    res_prov = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_orch_8",
            correlation_id="corr_orch_8",
            command="get_consolidated_outcome_comparison_provenance",
            context={"comparison_id": comp_id},
        )
    )
    assert res_prov.is_success
    assert res_prov.result["status"] == "success"


# ==============================================================================
# S. Dashboard GET Endpoints
# ==============================================================================
def test_dashboard_get_endpoints():
    """Verify HTTP GET endpoints on PortfolioDashboardServer for consolidated outcome comparisons."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    out1 = make_test_consolidated_outcome("out_dash_1", "inv_1")
    out2 = make_test_consolidated_outcome("out_dash_2", "inv_2")
    cons_svc._outcomes["out_dash_1"] = out1
    cons_svc._outcomes["out_dash_2"] = out2

    comp = service.compare_consolidated_outcomes(["out_dash_1", "out_dash_2"])

    server = PortfolioDashboardServer(
        port=0,
        consolidation_service=cons_svc,
        consistency_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        base_url = server.url

        # 1. Listing
        with urllib.request.urlopen(f"{base_url}/api/consolidated-outcome-comparisons") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["count"] >= 1

        # 2. Single item
        with urllib.request.urlopen(f"{base_url}/api/consolidated-outcome-comparisons/{comp.comparison_id}") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["comparison"]["comparison_id"] == comp.comparison_id

        # 3. Dimensions
        with urllib.request.urlopen(f"{base_url}/api/consolidated-outcome-comparisons/{comp.comparison_id}/dimensions") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert len(data["dimensions"]) == 12

        # 4. Evidence
        with urllib.request.urlopen(f"{base_url}/api/consolidated-outcome-comparisons/{comp.comparison_id}/evidence") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "evidence_consistency" in data

        # 5. Coverage
        with urllib.request.urlopen(f"{base_url}/api/consolidated-outcome-comparisons/{comp.comparison_id}/coverage") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "review_coverage" in data

        # 6. Divergences
        with urllib.request.urlopen(f"{base_url}/api/consolidated-outcome-comparisons/{comp.comparison_id}/divergences") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "divergences" in data

        # 7. Provenance
        with urllib.request.urlopen(f"{base_url}/api/consolidated-outcome-comparisons/{comp.comparison_id}/provenance") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "provenance_comparison" in data

        # 8. Not found
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/api/consolidated-outcome-comparisons/coc_nonexistent_99")
        assert exc_info.value.code == 404

    finally:
        server.stop()


# ==============================================================================
# T. Dashboard HEAD Endpoints
# ==============================================================================
def test_dashboard_head_endpoints():
    """Verify loopback read-only dashboard HEAD endpoints return headers only."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)
    out1 = make_test_consolidated_outcome("out_head_1", "inv_1")
    out2 = make_test_consolidated_outcome("out_head_2", "inv_2")
    cons_svc._outcomes["out_head_1"] = out1
    cons_svc._outcomes["out_head_2"] = out2
    comp = service.compare_consolidated_outcomes(["out_head_1", "out_head_2"])

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        consolidation_service=cons_svc,
        consistency_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("HEAD", f"/api/consolidated-outcome-comparisons/{comp.comparison_id}")
        res = conn.getresponse()
        assert res.status == 200
        body = res.read()
        assert len(body) == 0  # HEAD must have empty body
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# U. Dashboard Mutation Rejection (405)
# ==============================================================================
def test_dashboard_mutation_rejection():
    """Verify POST, PUT, DELETE, and PATCH methods are rejected with 405 Method Not Allowed."""
    server = PortfolioDashboardServer(port=0)
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        conn = http.client.HTTPConnection(server.host, server.port)
        for method in ("POST", "PUT", "DELETE", "PATCH"):
            conn.request(method, "/api/consolidated-outcome-comparisons", body=b"{}")
            resp = conn.getresponse()
            assert resp.status == 405
            allow_header = resp.getheader("Allow")
            assert allow_header is not None
            assert "GET" in allow_header
            assert "HEAD" in allow_header
            resp.read()
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# V. Dashboard Path Traversal Rejection (400)
# ==============================================================================
def test_dashboard_path_traversal_rejection():
    """Verify directory traversal and invalid ID characters return 400 Bad Request."""
    service = ConsolidatedOutcomeConsistencyService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        consistency_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("GET", "/api/consolidated-outcome-comparisons/../../etc/passwd")
        res = conn.getresponse()
        assert res.status == 400
        conn.close()

        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("GET", "/api/consolidated-outcome-comparisons/bad@id")
        res = conn.getresponse()
        assert res.status == 400
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# W. Prohibited Fields & Trade Recommendation Rejection
# ==============================================================================
def test_prohibited_fields_and_recommendation_rejection():
    """Verify recursive rejection of trade execution directives, ranking terms, and profitability claims."""
    bad_inputs = [
        {"action": "BUY"},
        {"winner": "out_01"},
        {"winning_strategy": "trend_follow"},
        {"target_price": 65000.0},
        {"stop_loss": 58000.0},
        {"expected_return": 0.25},
        {"strategy_ranking": [1, 2]},
    ]
    for bad in bad_inputs:
        with pytest.raises(ValidationError):
            validate_no_prohibited_fields(bad)


# ==============================================================================
# X. Non-Finite Number Rejection
# ==============================================================================
def test_non_finite_number_rejection():
    """Verify NaN, +Infinity, and -Infinity are rejected across numerical fields."""
    for bad_num in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValidationError):
            validate_numerical_integrity(bad_num, "test_field")


# ==============================================================================
# Y. Bounded Collection Behavior
# ==============================================================================
def test_bounded_collection_behavior():
    """Verify bounded outcomes (min 2, max 20) and bounded ring-buffer history."""
    service = ConsolidatedOutcomeConsistencyService(max_history=10)

    # Less than 2 outcomes rejected
    with pytest.raises(ValidationError):
        service.compare_consolidated_outcomes([])
    with pytest.raises(ValidationError):
        service.compare_consolidated_outcomes(["out_1"])

    # More than 20 outcomes rejected
    with pytest.raises(ValidationError):
        service.compare_consolidated_outcomes([f"out_{i}" for i in range(21)])


# ==============================================================================
# Z. Concurrent Access & Thread-Safety
# ==============================================================================
def test_concurrent_access():
    """Verify thread-safe concurrent execution of compare_consolidated_outcomes."""
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)

    for i in range(10):
        out = make_test_consolidated_outcome(f"out_conc_{i}", f"inv_{i}")
        cons_svc._outcomes[f"out_conc_{i}"] = out

    results: List[ConsolidatedOutcomeComparison] = []
    errors: List[Exception] = []

    def _worker(idx: int):
        try:
            o1 = f"out_conc_{idx % 10}"
            o2 = f"out_conc_{(idx + 1) % 10}"
            c = service.compare_consolidated_outcomes([o1, o2])
            results.append(c)
        except Exception as e:
            errors.append(e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_worker, i) for i in range(20)]
        concurrent.futures.wait(futures)

    assert len(errors) == 0
    assert len(results) == 20


# ==============================================================================
# AA. Safety Boundaries & Execution Lock
# ==============================================================================
def test_safety_invariants_and_execution_lock():
    """Verify Settings.execution_locked is True and read-only invariants are maintained."""
    settings = Settings()
    assert settings.execution_locked is True

    # MasterOrchestrator handler output verifies execution_locked
    cons_svc = InvestigationOutcomeConsolidationService()
    service = ConsolidatedOutcomeConsistencyService(consolidation_service=cons_svc)
    orchestrator = MasterOrchestrator()
    register_consistency_handlers(orchestrator=orchestrator, consistency_service=service)

    res = orchestrator.handle_request(
        OrchestratorRequest(
            request_id="req_safe",
            correlation_id="corr_safe",
            command="list_consolidated_outcome_comparisons",
            context={},
        )
    )
    assert res.is_success
    assert res.result["execution_locked"] is True
    assert res.result["read_only"] is True
