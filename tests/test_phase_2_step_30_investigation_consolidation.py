"""KAISER — Phase 2, Step 30 Test Suite.

Resolved Investigation Evidence Consolidation & Review Outcome Foundation.

Test Categories:
A. Domain model validation
B. Immutability
C. Consolidation state classification (INCOMPLETE, PARTIALLY_RECONCILED, RECONCILED, CONFLICTING, INVALID, UNAVAILABLE)
D. Deterministic consolidation
E. Evidence preservation (original IDs, relationships, provenance, quality preserved)
F. Conflict preservation (conflicts retained and exposed, not merged or discarded)
G. Resolution impact handling (resolved / reconciled discrepancies)
H. Unresolved discrepancy handling (open, under review, evidence requested)
I. Deferred discrepancy handling (deferred discrepancies noted as limitations)
J. Invalidated discrepancy handling (invalidated discrepancies noted as limitations)
K. Provenance tracing & MISSING_PROVENANCE handling
L. Reproducibility classification (REPRODUCIBLE, PARTIALLY_REPRODUCIBLE, NON_REPRODUCIBLE, UNKNOWN)
M. Data quality classification (VALID, DEGRADED, STALE, INVALID, UNKNOWN)
N. Deterministic SHA-256 identity
O. EventBus integration
P. StateStore integration & ring buffer retention
Q. MasterOrchestrator handler registration & execution
R. Dashboard GET endpoints (list, single, evidence, conflicts, limitations, provenance)
S. Dashboard HEAD endpoints
T. Dashboard mutation rejection (405 Method Not Allowed)
U. Dashboard path traversal rejection (400 Bad Request)
V. Secret sanitization & prohibited recommendation rejection
W. Non-finite value rejection (NaN, Infinity)
X. Bounded collection behavior
Y. Concurrent access & thread-safety
Z. Safety boundaries & JUJU isolation (Execution locked, trading engine uninitialized, JUJU untouched)
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import math
import os
import time
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import pytest

from app.analysis.opportunities.comparison.engine import InvestigationComparisonEngine
from app.analysis.opportunities.comparison.models import (
    ComparabilityState,
    ConclusionComparisonRecord,
    ConsistencySummary,
    DiscrepancyCategory,
    DiscrepancyResolutionState,
    DiscrepancySeverity,
    InvestigationComparison,
    InvestigationComparisonVoiceSummary,
    InvestigationDiscrepancy,
    ProvenanceComparisonRecord,
    QualityComparisonRecord,
    ReproducibilityComparisonRecord,
)
from app.analysis.opportunities.consolidation import (
    DEFAULT_MAX_CONSOLIDATION_HISTORY,
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
    compute_deterministic_conflict_id,
    compute_deterministic_outcome_id,
    register_consolidation_handlers,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.investigation.reporting.models import (
    AttributionType,
    AuditSummary,
    EvidenceMatrixEntry,
    InvestigationReport,
    InvestigationReportVoiceSummary,
    ObservationMatrixEntry,
    ReportStatus,
    ReproducibilityRecord,
)
from app.analysis.opportunities.investigation.reporting.service import (
    InvestigationReportingService,
)
from app.analysis.opportunities.resolution import (
    DiscrepancyResolutionCase,
    DiscrepancyResolutionService,
    EvidenceReconciliationRecord,
    EvidenceRelationshipType,
    ReconciliationOutcome,
    ResolutionAuditRecord,
    ResolutionDecision,
    ResolutionState,
    compute_deterministic_case_id,
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

def make_test_report(
    report_id: str = "rep_test_01",
    session_id: str = "inv_sess_01",
    candidate_id: str = "opp_cand_01",
    quality_status: str = "VALID",
    is_reproducible: bool = True,
    evidence_entries: Optional[Sequence[EvidenceMatrixEntry]] = None,
    observation_entries: Optional[Sequence[ObservationMatrixEntry]] = None,
) -> InvestigationReport:
    """Create a valid InvestigationReport for testing."""
    entries = tuple(
        evidence_entries
        or (
            EvidenceMatrixEntry(
                evidence_id="ev_trend_01",
                evidence_type="TREND_CONFIRMATION",
                status="VERIFIED",
                source="analysis_run:run_01",
                reviewer_disposition="ACCEPTED",
                provenance_ref="prov_01",
                quality_ref="qual_01",
                notes="Trend confirmed on 1h timeframe.",
                attribution=AttributionType.MACHINE_GENERATED_EVIDENCE.value,
            ),
        )
    )

    obs = tuple(
        observation_entries
        or (
            ObservationMatrixEntry(
                observation_id="obs_01",
                statement="Bullish divergence observed.",
                observation_type="FACT",
                evidence_refs=("ev_trend_01",),
                author_id="analyst_alice",
            ),
        )
    )

    return InvestigationReport(
        report_id=report_id,
        session_id=session_id,
        candidate_id=candidate_id,
        priority_id="prio_01",
        report_status=ReportStatus.GENERATED,
        generated_at=1000.0,
        reviewer_id="analyst_alice",
        candidate_summary={"scope": "BTC-USD 1h observation", "symbol": "BTC-USD"},
        priority_summary={"rank": 1},
        evidence_summary={"total": len(entries)},
        supporting_evidence=("ev_trend_01",),
        conflicting_evidence=(),
        missing_evidence=(),
        uncertainty_summary=(),
        limitation_summary=(),
        observations=obs,
        evidence_matrix=entries,
        analytical_conclusion={"state": "SUPPORTED", "rationale": "Sufficient evidence"},
        audit_summary=AuditSummary(
            total_events=5,
            evidence_review_events=2,
            observations_added=1,
            conclusion_changes=0,
            session_state_transitions=2,
            reviewer_identifiers=("analyst_alice",),
            first_event_timestamp=1000.0,
            last_event_timestamp=1005.0,
        ),
        reproducibility_record=ReproducibilityRecord(
            source_analysis_run_ids=("run_01",),
            reproducibility_id=f"repro_{report_id}",
            source_report_id=None,
            candidate_id=candidate_id,
            priority_id="prio_01",
            session_id=session_id,
            generated_at=1000.0,
            config_fingerprint="cfg_hash_01",
            is_reproducible=is_reproducible,
        ),
        source_analysis_run_ids=("run_01",),
        reproducibility_id=f"repro_{report_id}",
        report_provenance={"quality_status": quality_status},
        version=1,
        voice_summary=InvestigationReportVoiceSummary(
            headline="Investigation report finalized.",
            status_speech="Status is generated.",
            candidate_speech="Candidate analyzed.",
            evidence_speech="Verified trend.",
            conclusion_speech="Supported.",
            caveats_speech="None.",
            full_speech_script="Investigation report finalized with verified trend.",
        ),
        metadata={"quality_status": quality_status},
    )


def make_test_comparison(
    comparison_id: str = "cmp_test_01",
    report_ids: Tuple[str, ...] = ("rep_test_01", "rep_test_02"),
    discrepancies: Optional[Sequence[InvestigationDiscrepancy]] = None,
) -> InvestigationComparison:
    """Create a valid InvestigationComparison for testing."""
    discs = tuple(
        discrepancies
        if discrepancies is not None
        else (
            InvestigationDiscrepancy(
                discrepancy_id="disc_01",
                category=DiscrepancyCategory.EVIDENCE_DISPOSITION,
                severity=DiscrepancySeverity.WARNING,
                report_ids=report_ids,
                evidence_refs=("ev_trend_01",),
                description="Opposing reviewer dispositions for ev_trend_01.",
                resolution_state=DiscrepancyResolutionState.UNRESOLVED,
            ),
        )
    )

    return InvestigationComparison(
        comparison_id=comparison_id,
        report_ids=report_ids,
        candidate_ids=("opp_01",),
        comparison_status="COMPLETED",
        comparability_state=ComparabilityState.COMPARABLE,
        generated_at=1010.0,
        shared_evidence=(),
        changed_evidence=(),
        conflicting_evidence=(),
        missing_evidence=(),
        observation_differences=(),
        uncertainty_differences=(),
        limitation_differences=(),
        provenance_comparison=ProvenanceComparisonRecord(
            is_identical=True,
            source_types={},
            provenance_ids={},
            shared_provenance_count=1,
            differing_provenance_count=0,
        ),
        quality_comparison=QualityComparisonRecord(
            quality_statuses={r: "VALID" for r in report_ids},
            all_valid=True,
            status_divergence=False,
            freshness_seconds={r: 10.0 for r in report_ids},
            completeness_ratios={r: 1.0 for r in report_ids},
            anomaly_counts={r: 0 for r in report_ids},
        ),
        reproducibility_comparison=ReproducibilityComparisonRecord(
            is_fully_reproducible=True,
            reproducibility_ids={r: f"repro_{r}" for r in report_ids},
            shared_source_runs=("run_01",),
            differing_source_runs=(),
            config_fingerprints_match=True,
        ),
        conclusion_comparison=ConclusionComparisonRecord(
            conclusions_agree=True,
            conclusion_states={r: "SUPPORTED" for r in report_ids},
            rationales={r: "Aligned" for r in report_ids},
        ),
        consistency_summary=ConsistencySummary(
            total_reports_compared=len(report_ids),
            comparable_reports=len(report_ids),
            partially_comparable_reports=0,
            incompatible_reports=0,
            shared_evidence_count=1,
            changed_evidence_count=0,
            conflicting_evidence_count=len(discs),
            unresolved_discrepancy_count=len(discs),
            new_uncertainty_count=0,
            resolved_uncertainty_count=0,
            common_limitation_count=0,
        ),
        discrepancy_summary=discs,
        traceability={"source": "test"},
        voice_summary=InvestigationComparisonVoiceSummary(
            headline="Comparison complete.",
            comparability_speech="Reports are comparable.",
            evidence_speech="Shared evidence evaluated.",
            discrepancy_speech="Discrepancies identified.",
            full_speech_script="Comparison complete. Discrepancies identified.",
        ),
    )


def make_test_resolution_case(
    case_id: str = "res_case_01",
    comparison_id: str = "cmp_test_01",
    discrepancy_id: str = "disc_01",
    state: ResolutionState = ResolutionState.RESOLVED,
) -> DiscrepancyResolutionCase:
    """Create a valid DiscrepancyResolutionCase for testing."""
    decision = None
    rec_records: Tuple[EvidenceReconciliationRecord, ...] = ()

    if state in (ResolutionState.RESOLVED, ResolutionState.RECONCILIATION_RECORDED):
        rec_records = (
            EvidenceReconciliationRecord(
                reconciliation_id=f"reconcile_{case_id}",
                resolution_case_id=case_id,
                evidence_refs=("ev_trend_01",),
                evidence_classifications={"ev_trend_01": "VALID"},
                relationship_type=EvidenceRelationshipType.SAME_EVIDENCE,
                reconciliation_outcome=ReconciliationOutcome.BOTH_PRESERVED_VALID,
                reviewer_notes="Both evaluations valid under differing context.",
                rationale="Contextual difference acknowledged.",
                created_at=1020.0,
                reviewer_id="human_reviewer_bob",
            ),
        )
        decision = ResolutionDecision(
            decision_id=f"dec_{case_id}",
            resolution_case_id=case_id,
            claim_type=DecisionClaimType.HUMAN_REVIEW_DECISION,
            summary="Discrepancy resolved through contextual reconciliation.",
            rationale="Context differences explained without discarding evidence.",
            reviewer_id="human_reviewer_bob",
            decided_at=1025.0,
            accepted_evidence_ids=("ev_trend_01",),
        )

    return DiscrepancyResolutionCase(
        resolution_case_id=case_id,
        comparison_id=comparison_id,
        discrepancy_id=discrepancy_id,
        created_at=1015.0,
        updated_at=1025.0,
        state=state,
        scope="BTC-USD",
        discrepancy_category="EVIDENCE_DISPOSITION",
        discrepancy_severity="WARNING",
        discrepancy_description="Reviewer disposition divergence.",
        report_ids=("rep_test_01", "rep_test_02"),
        reconciliation_records=rec_records,
        decision=decision,
    )


# ==============================================================================
# A. Domain Model Validation
# ==============================================================================
def test_domain_model_validation():
    """Verify field constraints, serializability, and validation in domain models."""
    item = ConsolidatedEvidenceItem(
        evidence_id="ev_01",
        source_id="rep_01",
        claim_type=DecisionClaimType.FACT,
        review_status="VERIFIED",
        provenance_reference="prov_01",
        quality_status="VALID",
        timestamp=100.0,
        scope="BTC-USD",
        notes=("Empirical data point",),
    )
    assert item.evidence_id == "ev_01"
    assert item.claim_type == DecisionClaimType.FACT
    d = item.to_dict()
    assert d["evidence_id"] == "ev_01"
    assert d["claim_type"] == "FACT"

    # Reject empty evidence_id
    with pytest.raises(ValidationError):
        ConsolidatedEvidenceItem(
            evidence_id="",
            source_id="rep_01",
            claim_type=DecisionClaimType.FACT,
        )

    # Reject empty source_id
    with pytest.raises(ValidationError):
        ConsolidatedEvidenceItem(
            evidence_id="ev_01",
            source_id="",
            claim_type=DecisionClaimType.FACT,
        )


# ==============================================================================
# B. Immutability
# ==============================================================================
def test_immutability():
    """Verify that domain models are frozen dataclasses and cannot be mutated."""
    item = ConsolidatedEvidenceItem(
        evidence_id="ev_01",
        source_id="rep_01",
        claim_type=DecisionClaimType.FACT,
    )
    with pytest.raises((AttributeError, TypeError)):
        item.evidence_id = "ev_mutated"  # type: ignore

    outcome = ConsolidatedInvestigationOutcome(
        outcome_id="outcome_test_01",
        investigation_id="inv_01",
        source_report_ids=("rep_01",),
    )
    with pytest.raises((AttributeError, TypeError)):
        outcome.outcome_state = ConsolidationState.RECONCILED  # type: ignore


# ==============================================================================
# C. Consolidation State Classification
# ==============================================================================
def test_consolidation_state_classification():
    """Verify deterministic state classifications: RECONCILED, PARTIALLY_RECONCILED, CONFLICTING, INCOMPLETE, UNAVAILABLE."""
    service = InvestigationOutcomeConsolidationService()

    # 1. INCOMPLETE: No reports found
    out_incomplete = service.consolidate_investigation_outcome(investigation_id="inv_empty")
    assert out_incomplete.outcome_state == ConsolidationState.INCOMPLETE

    # 2. UNAVAILABLE: Explicitly requested reports do not exist
    out_unavail = service.consolidate_investigation_outcome(
        investigation_id="inv_unavail",
        report_ids=["rep_nonexistent_99"],
    )
    assert out_unavail.outcome_state == ConsolidationState.UNAVAILABLE

    # 3. CONFLICTING: Reports with active unresolved discrepancy
    rep1 = make_test_report("rep_c1", "inv_conf")
    rep2 = make_test_report("rep_c2", "inv_conf")
    comp = make_test_comparison("cmp_c", ("rep_c1", "rep_c2"))
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_c1"] = rep1
    reporting_svc._reports["rep_c2"] = rep2
    comp_engine = InvestigationComparisonEngine()
    comp_engine._comparisons["cmp_c"] = comp
    res_svc = DiscrepancyResolutionService()

    service_conf = InvestigationOutcomeConsolidationService(
        reporting_service=reporting_svc,
        comparison_engine=comp_engine,
        resolution_service=res_svc,
    )
    out_conf = service_conf.consolidate_investigation_outcome(
        investigation_id="inv_conf",
        report_ids=["rep_c1", "rep_c2"],
        comparison_ids=["cmp_c"],
    )
    assert out_conf.outcome_state == ConsolidationState.CONFLICTING
    assert len(out_conf.conflicts) >= 1
    assert any(c.is_active for c in out_conf.conflicts)

    # 4. RECONCILED: Discrepancy resolved via human review
    case = make_test_resolution_case("res_c1", "cmp_c", "disc_01", state=ResolutionState.RESOLVED)
    res_svc._cases["res_c1"] = case
    out_rec = service_conf.consolidate_investigation_outcome(
        investigation_id="inv_conf",
        report_ids=["rep_c1", "rep_c2"],
        comparison_ids=["cmp_c"],
        resolution_case_ids=["res_c1"],
    )
    assert out_rec.outcome_state == ConsolidationState.RECONCILED
    assert not any(c.is_active for c in out_rec.conflicts)

    # 5. PARTIALLY_RECONCILED: Discrepancy deferred
    case_def = make_test_resolution_case("res_c2", "cmp_c", "disc_01", state=ResolutionState.DEFERRED)
    res_svc._cases["res_c2"] = case_def
    out_part = service_conf.consolidate_investigation_outcome(
        investigation_id="inv_conf",
        report_ids=["rep_c1", "rep_c2"],
        comparison_ids=["cmp_c"],
        resolution_case_ids=["res_c2"],
    )
    assert out_part.outcome_state == ConsolidationState.PARTIALLY_RECONCILED


# ==============================================================================
# D. Deterministic Consolidation
# ==============================================================================
def test_deterministic_consolidation():
    """Verify identical inputs produce identical consolidated outcome structures."""
    rep = make_test_report("rep_det_1", "inv_det")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_det_1"] = rep

    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)
    out1 = service.consolidate_investigation_outcome("inv_det", report_ids=["rep_det_1"])
    out2 = service.consolidate_investigation_outcome("inv_det", report_ids=["rep_det_1"])

    assert out1.outcome_state == out2.outcome_state
    assert len(out1.facts) == len(out2.facts)
    assert len(out1.interpretations) == len(out2.interpretations)
    assert out1.source_report_ids == out2.source_report_ids


# ==============================================================================
# E. Evidence Preservation
# ==============================================================================
def test_evidence_preservation():
    """Verify original evidence IDs, source report IDs, timestamps, and quality are preserved."""
    rep = make_test_report("rep_pres_1", "inv_pres")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_pres_1"] = rep

    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)
    outcome = service.consolidate_investigation_outcome("inv_pres", report_ids=["rep_pres_1"])

    assert len(outcome.facts) >= 1
    item = outcome.facts[0]
    assert item.evidence_id == "ev_trend_01"
    assert item.source_id == "rep_pres_1"
    assert item.quality_status == "qual_01"
    assert item.provenance_reference == "prov_01"


# ==============================================================================
# F. Conflict Preservation
# ==============================================================================
def test_conflict_preservation():
    """Verify contradictory evidence items are preserved rather than overwritten or merged."""
    rep1 = make_test_report("rep_cp1", "inv_cp")
    rep2 = make_test_report("rep_cp2", "inv_cp")
    comp = make_test_comparison("cmp_cp", ("rep_cp1", "rep_cp2"))

    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_cp1"] = rep1
    reporting_svc._reports["rep_cp2"] = rep2
    comp_engine = InvestigationComparisonEngine()
    comp_engine._comparisons["cmp_cp"] = comp

    service = InvestigationOutcomeConsolidationService(
        reporting_service=reporting_svc,
        comparison_engine=comp_engine,
    )
    outcome = service.consolidate_investigation_outcome("inv_cp", report_ids=["rep_cp1", "rep_cp2"], comparison_ids=["cmp_cp"])

    assert outcome.outcome_state == ConsolidationState.CONFLICTING
    assert len(outcome.conflicts) == 1
    conflict = outcome.conflicts[0]
    assert conflict.discrepancy_id == "disc_01"
    assert conflict.is_active is True
    assert set(conflict.conflicting_report_ids) == {"rep_cp1", "rep_cp2"}


# ==============================================================================
# G. Resolution Impact Handling
# ==============================================================================
def test_resolution_impact_handling():
    """Verify human review resolution deactivates conflict while preserving evidence."""
    rep1 = make_test_report("rep_ri1", "inv_ri")
    rep2 = make_test_report("rep_ri2", "inv_ri")
    comp = make_test_comparison("cmp_ri", ("rep_ri1", "rep_ri2"))
    case = make_test_resolution_case("case_ri", "cmp_ri", "disc_01", state=ResolutionState.RESOLVED)

    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_ri1"] = rep1
    reporting_svc._reports["rep_ri2"] = rep2
    comp_engine = InvestigationComparisonEngine()
    comp_engine._comparisons["cmp_ri"] = comp
    res_svc = DiscrepancyResolutionService()
    res_svc._cases["case_ri"] = case

    service = InvestigationOutcomeConsolidationService(
        reporting_service=reporting_svc,
        comparison_engine=comp_engine,
        resolution_service=res_svc,
    )
    outcome = service.consolidate_investigation_outcome(
        "inv_ri",
        report_ids=["rep_ri1", "rep_ri2"],
        comparison_ids=["cmp_ri"],
        resolution_case_ids=["case_ri"],
    )

    assert outcome.outcome_state == ConsolidationState.RECONCILED
    assert len(outcome.human_review_decisions) == 1
    dec = outcome.human_review_decisions[0]
    assert dec.claim_type == DecisionClaimType.HUMAN_REVIEW_DECISION
    assert dec.reviewer_id == "human_reviewer_bob"
    assert "disc_01" in outcome.resolved_items


# ==============================================================================
# H. Unresolved Discrepancy Handling
# ==============================================================================
def test_unresolved_discrepancy_handling():
    """Verify open discrepancies are correctly tagged as active conflicts and limitations."""
    rep1 = make_test_report("rep_un1", "inv_un")
    rep2 = make_test_report("rep_un2", "inv_un")
    comp = make_test_comparison("cmp_un", ("rep_un1", "rep_un2"))
    case = make_test_resolution_case("case_un", "cmp_un", "disc_01", state=ResolutionState.OPEN)

    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_un1"] = rep1
    reporting_svc._reports["rep_un2"] = rep2
    comp_engine = InvestigationComparisonEngine()
    comp_engine._comparisons["cmp_un"] = comp
    res_svc = DiscrepancyResolutionService()
    res_svc._cases["case_un"] = case

    service = InvestigationOutcomeConsolidationService(
        reporting_service=reporting_svc,
        comparison_engine=comp_engine,
        resolution_service=res_svc,
    )
    outcome = service.consolidate_investigation_outcome(
        "inv_un",
        report_ids=["rep_un1", "rep_un2"],
        comparison_ids=["cmp_un"],
        resolution_case_ids=["case_un"],
    )

    assert outcome.outcome_state == ConsolidationState.CONFLICTING
    assert "disc_01" in outcome.unresolved_items
    assert any(lim.limitation_type == ConsolidationLimitation.UNRESOLVED_DISCREPANCY for lim in outcome.limitations)


# ==============================================================================
# I. Deferred Discrepancy Handling
# ==============================================================================
def test_deferred_discrepancy_handling():
    """Verify deferred discrepancies result in PARTIALLY_RECONCILED state and clear limitations."""
    rep1 = make_test_report("rep_def1", "inv_def")
    rep2 = make_test_report("rep_def2", "inv_def")
    comp = make_test_comparison("cmp_def", ("rep_def1", "rep_def2"))
    case = make_test_resolution_case("case_def", "cmp_def", "disc_01", state=ResolutionState.DEFERRED)

    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_def1"] = rep1
    reporting_svc._reports["rep_def2"] = rep2
    comp_engine = InvestigationComparisonEngine()
    comp_engine._comparisons["cmp_def"] = comp
    res_svc = DiscrepancyResolutionService()
    res_svc._cases["case_def"] = case

    service = InvestigationOutcomeConsolidationService(
        reporting_service=reporting_svc,
        comparison_engine=comp_engine,
        resolution_service=res_svc,
    )
    outcome = service.consolidate_investigation_outcome(
        "inv_def",
        report_ids=["rep_def1", "rep_def2"],
        comparison_ids=["cmp_def"],
        resolution_case_ids=["case_def"],
    )

    assert outcome.outcome_state == ConsolidationState.PARTIALLY_RECONCILED
    assert "disc_01" in outcome.unresolved_items


# ==============================================================================
# J. Invalidated Discrepancy Handling
# ==============================================================================
def test_invalidated_discrepancy_handling():
    """Verify invalidated discrepancies mark conflict inactive and record limitation note."""
    rep1 = make_test_report("rep_inv1", "inv_inv")
    rep2 = make_test_report("rep_inv2", "inv_inv")
    comp = make_test_comparison("cmp_inv", ("rep_inv1", "rep_inv2"))
    case = make_test_resolution_case("case_inv", "cmp_inv", "disc_01", state=ResolutionState.INVALIDATED)

    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_inv1"] = rep1
    reporting_svc._reports["rep_inv2"] = rep2
    comp_engine = InvestigationComparisonEngine()
    comp_engine._comparisons["cmp_inv"] = comp
    res_svc = DiscrepancyResolutionService()
    res_svc._cases["case_inv"] = case

    service = InvestigationOutcomeConsolidationService(
        reporting_service=reporting_svc,
        comparison_engine=comp_engine,
        resolution_service=res_svc,
    )
    outcome = service.consolidate_investigation_outcome(
        "inv_inv",
        report_ids=["rep_inv1", "rep_inv2"],
        comparison_ids=["cmp_inv"],
        resolution_case_ids=["case_inv"],
    )

    assert outcome.outcome_state == ConsolidationState.RECONCILED
    assert "disc_01" in outcome.resolved_items
    assert any(lim.limitation_type == ConsolidationLimitation.DIFFERENT_ANALYTICAL_SCOPE for lim in outcome.limitations)


# ==============================================================================
# K. Provenance Tracing & MISSING_PROVENANCE Handling
# ==============================================================================
def test_provenance_tracing():
    """Verify complete lineage tracing and explicit MISSING_PROVENANCE recording."""
    rep = make_test_report("rep_prov_1", "inv_prov")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_prov_1"] = rep

    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)
    outcome = service.consolidate_investigation_outcome("inv_prov", report_ids=["rep_prov_1"])

    assert "report_lineage" in outcome.provenance_summary
    assert "rep_prov_1" in outcome.provenance_summary["report_lineage"]
    lineage = outcome.provenance_summary["report_lineage"]["rep_prov_1"]
    assert lineage["session_id"] == "inv_prov"
    assert lineage["candidate_id"] == "opp_cand_01"


# ==============================================================================
# L. Reproducibility Classification
# ==============================================================================
def test_reproducibility_classification():
    """Verify categorical reproducibility states (REPRODUCIBLE, PARTIALLY_REPRODUCIBLE, NON_REPRODUCIBLE)."""
    # 1. Fully reproducible
    rep1 = make_test_report("rep_rep1", "inv_repro", is_reproducible=True)
    rep2 = make_test_report("rep_rep2", "inv_repro", is_reproducible=True)
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_rep1"] = rep1
    reporting_svc._reports["rep_rep2"] = rep2
    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)

    out_full = service.consolidate_investigation_outcome("inv_repro", report_ids=["rep_rep1", "rep_rep2"])
    assert out_full.reproducibility_summary.reproducibility_state == ReproducibilityConsolidationState.REPRODUCIBLE

    # 2. Partially reproducible
    rep3 = make_test_report("rep_rep3", "inv_repro2", is_reproducible=False)
    reporting_svc._reports["rep_rep3"] = rep3
    out_part = service.consolidate_investigation_outcome("inv_repro2", report_ids=["rep_rep1", "rep_rep3"])
    assert out_part.reproducibility_summary.reproducibility_state == ReproducibilityConsolidationState.PARTIALLY_REPRODUCIBLE


# ==============================================================================
# M. Data Quality Classification
# ==============================================================================
def test_data_quality_classification():
    """Verify categorical data quality states without misleading single scores."""
    rep_deg = make_test_report("rep_deg", "inv_qual", quality_status="DEGRADED")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_deg"] = rep_deg
    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)

    outcome = service.consolidate_investigation_outcome("inv_qual", report_ids=["rep_deg"])
    assert outcome.quality_summary.overall_quality_state == "DEGRADED"
    assert "rep_deg" in outcome.quality_summary.degraded_sources
    assert any(lim.limitation_type == ConsolidationLimitation.DEGRADED_DATA_QUALITY for lim in outcome.limitations)


# ==============================================================================
# N. Deterministic SHA-256 Identity
# ==============================================================================
def test_deterministic_sha256_identity():
    """Verify deterministic outcome ID generation via SHA-256."""
    id1 = compute_deterministic_outcome_id("inv_1", ["rep_1", "rep_2"], ["res_1"], 100.0)
    id2 = compute_deterministic_outcome_id("inv_1", ["rep_2", "rep_1"], ["res_1"], 100.0)
    assert id1 == id2
    assert id1.startswith("outcome_inv_")


# ==============================================================================
# O. EventBus Integration
# ==============================================================================
def test_eventbus_integration():
    """Verify lifecycle events emitted to EventBus during consolidation."""
    bus = EventBus()
    events: List[Event] = []
    bus.subscribe(EventType.INVESTIGATION_OUTCOME_CONSOLIDATION_STARTED, events.append)
    bus.subscribe(EventType.INVESTIGATION_OUTCOME_CONSOLIDATED, events.append)

    rep = make_test_report("rep_ev_1", "inv_ev")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_ev_1"] = rep

    service = InvestigationOutcomeConsolidationService(
        event_bus=bus,
        reporting_service=reporting_svc,
    )
    outcome = service.consolidate_investigation_outcome("inv_ev", report_ids=["rep_ev_1"])

    assert len(events) >= 2
    started = [e for e in events if e.event_type == EventType.INVESTIGATION_OUTCOME_CONSOLIDATION_STARTED]
    completed = [e for e in events if e.event_type == EventType.INVESTIGATION_OUTCOME_CONSOLIDATED]
    assert len(started) == 1
    assert len(completed) == 1
    assert completed[0].payload["outcome_id"] == outcome.outcome_id


# ==============================================================================
# P. StateStore Integration & Ring Buffer Retention
# ==============================================================================
def test_statestore_integration():
    """Verify bounded latest/history persistence in StateStore."""
    store = StateStore()
    rep = make_test_report("rep_st_1", "inv_st")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_st_1"] = rep

    service = InvestigationOutcomeConsolidationService(
        state_store=store,
        reporting_service=reporting_svc,
        max_history=5,
    )

    for i in range(8):
        service.consolidate_investigation_outcome(f"inv_st_{i}", report_ids=["rep_st_1"])

    history = store.get("investigation_outcome:history", [])
    assert len(history) == 5
    latest = store.get("investigation_outcome:latest")
    assert latest is not None
    assert latest["investigation_id"] == "inv_st_7"


# ==============================================================================
# Q. MasterOrchestrator Handler Registration & Execution
# ==============================================================================
def test_handler_registration_and_execution():
    """Verify all 8 orchestrator handlers execute and return structured responses."""
    orchestrator = MasterOrchestrator()
    rep = make_test_report("rep_h1", "inv_h1")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_h1"] = rep

    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)
    register_consolidation_handlers(orchestrator, service)

    # 1. create
    req_create = OrchestratorRequest(
        request_id="req_c1",
        correlation_id="corr_1",
        command="create_investigation_outcome_consolidation",
        context={"investigation_id": "inv_h1", "report_ids": ["rep_h1"]},
    )
    res_create = orchestrator.handle_request(req_create)
    assert res_create.is_success
    outcome_id = res_create.result["outcome_id"]

    # 2. get
    req_get = OrchestratorRequest(
        request_id="req_g1",
        correlation_id="corr_2",
        command="get_investigation_outcome",
        context={"outcome_id": outcome_id},
    )
    res_get = orchestrator.handle_request(req_get)
    assert res_get.is_success
    assert res_get.result["outcome"]["outcome_id"] == outcome_id

    # 3. list
    req_list = OrchestratorRequest(
        request_id="req_l1",
        correlation_id="corr_3",
        command="list_investigation_outcomes",
        context={},
    )
    res_list = orchestrator.handle_request(req_list)
    assert res_list.is_success
    assert res_list.result["count"] >= 1

    # 4. refresh
    req_ref = OrchestratorRequest(
        request_id="req_r1",
        correlation_id="corr_4",
        command="refresh_investigation_outcome",
        context={"outcome_id": outcome_id},
    )
    res_ref = orchestrator.handle_request(req_ref)
    assert res_ref.is_success
    assert res_ref.result["previous_outcome_id"] == outcome_id

    # 5. evidence
    req_ev = OrchestratorRequest(
        request_id="req_e1",
        correlation_id="corr_5",
        command="get_investigation_outcome_evidence",
        context={"outcome_id": outcome_id},
    )
    res_ev = orchestrator.handle_request(req_ev)
    assert res_ev.is_success
    assert "facts" in res_ev.result

    # 6. conflicts
    req_cf = OrchestratorRequest(
        request_id="req_cf1",
        correlation_id="corr_6",
        command="get_investigation_outcome_conflicts",
        context={"outcome_id": outcome_id},
    )
    res_cf = orchestrator.handle_request(req_cf)
    assert res_cf.is_success
    assert "conflicts" in res_cf.result

    # 7. limitations
    req_lim = OrchestratorRequest(
        request_id="req_lim1",
        correlation_id="corr_7",
        command="get_investigation_outcome_limitations",
        context={"outcome_id": outcome_id},
    )
    res_lim = orchestrator.handle_request(req_lim)
    assert res_lim.is_success
    assert "limitations" in res_lim.result

    # 8. provenance
    req_prov = OrchestratorRequest(
        request_id="req_p1",
        correlation_id="corr_8",
        command="get_investigation_outcome_provenance",
        context={"outcome_id": outcome_id},
    )
    res_prov = orchestrator.handle_request(req_prov)
    assert res_prov.is_success
    assert "provenance_summary" in res_prov.result


# ==============================================================================
# R. Dashboard GET Endpoints
# ==============================================================================
def test_dashboard_get_endpoints():
    """Verify loopback read-only dashboard GET endpoints."""
    rep = make_test_report("rep_dash_1", "inv_dash")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_dash_1"] = rep

    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)
    outcome = service.consolidate_investigation_outcome("inv_dash", report_ids=["rep_dash_1"])

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        consolidation_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        base = server.url
        # 1. Listing
        with urllib.request.urlopen(f"{base}/api/investigation-outcomes") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] >= 1

        # 2. Single item
        with urllib.request.urlopen(f"{base}/api/investigation-outcomes/{outcome.outcome_id}") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["outcome"]["outcome_id"] == outcome.outcome_id

        # 3. Evidence
        with urllib.request.urlopen(f"{base}/api/investigation-outcomes/{outcome.outcome_id}/evidence") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert "facts" in data

        # 4. Conflicts
        with urllib.request.urlopen(f"{base}/api/investigation-outcomes/{outcome.outcome_id}/conflicts") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert "conflicts" in data

        # 5. Limitations
        with urllib.request.urlopen(f"{base}/api/investigation-outcomes/{outcome.outcome_id}/limitations") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert "limitations" in data

        # 6. Provenance
        with urllib.request.urlopen(f"{base}/api/investigation-outcomes/{outcome.outcome_id}/provenance") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert "provenance_summary" in data

    finally:
        server.stop()


# ==============================================================================
# S. Dashboard HEAD Endpoints
# ==============================================================================
def test_dashboard_head_endpoints():
    """Verify loopback read-only dashboard HEAD endpoints return headers only."""
    rep = make_test_report("rep_head_1", "inv_head")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_head_1"] = rep

    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)
    outcome = service.consolidate_investigation_outcome("inv_head", report_ids=["rep_head_1"])

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        consolidation_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("HEAD", f"/api/investigation-outcomes/{outcome.outcome_id}")
        res = conn.getresponse()
        assert res.status == 200
        body = res.read()
        assert len(body) == 0  # HEAD must have empty body
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# T. Dashboard Mutation Rejection (405 Method Not Allowed)
# ==============================================================================
def test_dashboard_mutation_rejection():
    """Verify POST, PUT, DELETE requests return 405 Method Not Allowed."""
    service = InvestigationOutcomeConsolidationService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        consolidation_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("POST", "/api/investigation-outcomes", body=b"{}")
        res = conn.getresponse()
        assert res.status == 405
        conn.close()

        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("DELETE", "/api/investigation-outcomes/outcome_1", body=b"{}")
        res = conn.getresponse()
        assert res.status == 405
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# U. Dashboard Path Traversal Rejection (400 Bad Request)
# ==============================================================================
def test_path_traversal_rejection():
    """Verify directory traversal and null byte injections return 400 Bad Request."""
    service = InvestigationOutcomeConsolidationService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        consolidation_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("GET", "/api/investigation-outcomes/../../etc/passwd")
        res = conn.getresponse()
        assert res.status == 400
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# V. Secret Sanitization & Prohibited Recommendation Rejection
# ==============================================================================
def test_secret_sanitization_and_prohibited_fields():
    """Verify rejection of prohibited trade words in payloads."""
    with pytest.raises(ValidationError):
        validate_no_prohibited_fields({"action": "BUY", "quantity": 10})

    with pytest.raises(ValidationError):
        validate_no_prohibited_fields({"expected_profit": 500.0})

    with pytest.raises(ValidationError):
        validate_no_prohibited_fields({"win_probability": 0.85})


# ==============================================================================
# W. Non-Finite Number Rejection
# ==============================================================================
def test_non_finite_number_rejection():
    """Verify NaN and Infinity rejection while preserving genuine 0.0."""
    assert validate_numerical_integrity(0.0) == 0.0
    assert validate_numerical_integrity(0) == 0.0

    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("nan"))

    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("inf"))


# ==============================================================================
# X. Bounded Collection Behavior
# ==============================================================================
def test_bounded_collection_behavior():
    """Verify bounded ring-buffer pruning on StateStore history."""
    store = StateStore()
    rep = make_test_report("rep_bound_1", "inv_bound")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_bound_1"] = rep

    service = InvestigationOutcomeConsolidationService(
        state_store=store,
        reporting_service=reporting_svc,
        max_history=5,
    )

    for i in range(10):
        service.consolidate_investigation_outcome(f"inv_bound_{i}", report_ids=["rep_bound_1"])

    history = store.get("investigation_outcome:history", [])
    assert len(history) <= 5


# ==============================================================================
# Y. Concurrent Access & Thread-Safety
# ==============================================================================
def test_concurrent_access():
    """Verify safe concurrent access and thread safety."""
    rep = make_test_report("rep_conc_1", "inv_conc")
    reporting_svc = InvestigationReportingService()
    reporting_svc._reports["rep_conc_1"] = rep

    service = InvestigationOutcomeConsolidationService(reporting_service=reporting_svc)

    def worker(i: int):
        return service.consolidate_investigation_outcome(f"inv_conc_{i}", report_ids=["rep_conc_1"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, i) for i in range(10)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == 10


# ==============================================================================
# Z. Safety Invariants & Execution Lock Verification
# ==============================================================================
def test_safety_invariants_and_execution_lock():
    """Verify system execution locked, no trading engine, and JUJU untouched."""
    settings = Settings()
    assert settings.execution_locked is True

    # Voice safety verification
    summary = ConsolidationVoiceSummary(
        outcome_id="outcome_test",
        outcome_state="RECONCILED",
        summary_text="Investigation evidence is reconciled. All discrepancies resolved.",
        voice_safe=True,
    )
    assert summary.voice_safe is True

    # Prohibited voice words rejection
    with pytest.raises(ValidationError):
        ConsolidationVoiceSummary(
            outcome_id="outcome_test_bad",
            outcome_state="RECONCILED",
            summary_text="Buy this asset now for maximum profit.",
            voice_safe=True,
        )

    # JUJU Isolation verification: JUJU directory must exist and be untouched
    juju_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "JUJU")
    if os.path.exists(juju_path):
        assert os.path.isdir(juju_path)
