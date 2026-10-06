"""KAISER — Phase 2 Step 28 Test Suite.

Cross-Investigation Evidence Comparison & Review Consistency Foundation.

Tests all 41 requirements:
1. valid comparable reports
2. partially comparable reports
3. non-comparable reports
4. invalid report input
5. incomplete evidence
6. shared evidence
7. added evidence
8. removed evidence
9. modified evidence
10. conflicting evidence
11. observation differences
12. FACT vs INTERPRETATION classification changes
13. uncertainty changes
14. limitation changes
15. provenance comparison
16. data-quality comparison
17. reproducibility comparison
18. conclusion comparison
19. review consistency
20. discrepancy generation
21. discrepancy resolution state
22. deterministic comparison ID
23. repeated identical comparison
24. stable report ordering
25. NaN rejection
26. Infinity rejection
27. zero preservation
28. numeric zero-baseline safety
29. EventBus events
30. StateStore persistence
31. handler registration
32. concurrency
33. dashboard GET
34. dashboard HEAD
35. dashboard 405
36. traversal protection
37. prohibited field rejection
38. execution lock invariant
39. no broker initialization
40. no autonomous execution
41. JUJU isolation
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import math
import os
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple
import urllib.request

import pytest

from app.analysis.evidence.models import EvidenceItem, EvidenceStatus, EvidenceSynthesisReport, EvidenceType
from app.analysis.opportunities.comparison import (
    ComparabilityState,
    DiscrepancyCategory,
    DiscrepancyResolutionState,
    DiscrepancySeverity,
    EvidenceDiffCategory,
    InvestigationComparison,
    InvestigationComparisonEngine,
    ObservationDiffType,
    compute_deterministic_comparison_id,
    register_investigation_comparison_handlers,
    safe_percentage_delta,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.investigation.models import (
    ConclusionState,
    EvidenceReviewDisposition,
    InvestigationSession,
    ObservationType,
)
from app.analysis.opportunities.investigation.session import InvestigationSessionManager
from app.analysis.opportunities.investigation.reporting.models import (
    AttributionType,
    EvidenceMatrixEntry,
    InvestigationReport,
    InvestigationReportVoiceSummary,
    ObservationMatrixEntry,
    ReportStatus,
)
from app.analysis.opportunities.investigation.reporting.service import InvestigationReportingService
from app.analysis.opportunities.models import ConfidenceState, OpportunityCandidate, OpportunityState
from app.analysis.opportunities.prioritization.engine import OpportunityPrioritizationEngine
from app.analysis.opportunities.prioritization.models import OpportunityPriorityRecord
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer


# ==============================================================================
# Helper Factories
# ==============================================================================

def make_test_candidate(
    candidate_id: str = "cand_cmp_01",
    symbol: str = "BTC-USD",
    timeframe: str = "1h",
    supporting_eids: Sequence[str] = ("ev_trend_01", "ev_mom_02"),
    conflicting_eids: Sequence[str] = ("ev_vol_03",),
) -> OpportunityCandidate:
    """Helper to construct a valid OpportunityCandidate."""
    return OpportunityCandidate(
        candidate_id=candidate_id,
        symbol=symbol,
        timeframe=timeframe,
        assessment_state=OpportunityState.ANALYTICALLY_SUPPORTED,
        evidence_status=EvidenceStatus.AVAILABLE,
        evidence_completeness_score=0.85,
        confidence_state=ConfidenceState.MODERATE,
        source_evidence_report_id="rep_synth_01",
        generated_at=1000.0,
        supporting_evidence_refs=tuple(supporting_eids),
        conflicting_evidence_refs=tuple(conflicting_eids),
        uncertainty_refs=("uncertainty_01",),
        limitation_refs=("limitation_01",),
        invalidation_conditions=("stale_feed",),
        source_analysis_run_ids=("run_01", "run_02"),
        reproducibility_id="repro_cmp_123",
    )


def make_test_priority(candidate: OpportunityCandidate) -> OpportunityPriorityRecord:
    """Helper to construct a valid OpportunityPriorityRecord."""
    engine = OpportunityPrioritizationEngine()
    return engine.prioritize_candidate(candidate)


def make_test_evidence_report(report_id: str = "rep_synth_01") -> EvidenceSynthesisReport:
    """Helper to construct a valid EvidenceSynthesisReport."""
    item1 = EvidenceItem(
        evidence_id="ev_trend_01",
        evidence_type=EvidenceType.TECHNICAL_ANALYSIS,
        source_reference="indicator:ema_crossover",
        observation="Bullish trend alignment across hourly timeframes",
        timestamp=990.0,
        status=EvidenceStatus.AVAILABLE,
        source_run_id="run_01",
    )
    item2 = EvidenceItem(
        evidence_id="ev_vol_03",
        evidence_type=EvidenceType.RISK,
        source_reference="indicator:atr_volatility",
        observation="Elevated downside tail risk",
        timestamp=995.0,
        status=EvidenceStatus.CONFLICTING,
        source_run_id="run_02",
    )
    return EvidenceSynthesisReport(
        report_id=report_id,
        generated_at=998.0,
        subject="BTC-USD",
        scope="HOURLY_WINDOW",
        source_run_ids=("run_01", "run_02"),
        evidence_items=(item1, item2),
        key_observations=(),
    )


def make_test_report(
    candidate_id: str = "cand_cmp_01",
    symbol: str = "BTC-USD",
    timeframe: str = "1h",
    supp_disp: EvidenceReviewDisposition = EvidenceReviewDisposition.SUPPORTING,
    obs_type: ObservationType = ObservationType.FACT,
    conclusion_state: ConclusionState = ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
    reviewer_id: str = "analyst_alice",
    mgr: Optional[InvestigationSessionManager] = None,
    rep_svc: Optional[InvestigationReportingService] = None,
) -> Tuple[InvestigationReport, InvestigationSessionManager, InvestigationReportingService]:
    """Helper to generate a completed InvestigationReport."""
    manager = mgr or InvestigationSessionManager()
    service = rep_svc or InvestigationReportingService()

    cand = make_test_candidate(candidate_id=candidate_id, symbol=symbol, timeframe=timeframe)
    prio = make_test_priority(cand)
    ev_rep = make_test_evidence_report()

    sess = manager.create_session(candidate=cand, priority_record=prio, reviewer_id=reviewer_id)
    sess = manager.start_session(sess.session_id, reviewer_id=reviewer_id)

    manager.review_evidence(
        session_id=sess.session_id,
        evidence_id="ev_trend_01",
        disposition=supp_disp,
        notes="Trend review note.",
        reviewer_id=reviewer_id,
    )
    manager.review_evidence(
        session_id=sess.session_id,
        evidence_id="ev_vol_03",
        disposition=EvidenceReviewDisposition.CONFLICTING,
        notes="Volatility review note.",
        reviewer_id=reviewer_id,
    )

    manager.add_observation(
        session_id=sess.session_id,
        observation_type=obs_type,
        statement="Trend confirmed on horizon.",
        evidence_refs=("ev_trend_01",),
        reviewer_id=reviewer_id,
    )

    manager.add_uncertainty(
        session_id=sess.session_id,
        description="Macro news event pending.",
        severity="MEDIUM",
        reviewer_id=reviewer_id,
    )
    manager.add_limitation(
        session_id=sess.session_id,
        description="Replay sample size 30 days.",
        category="SAMPLE_COVERAGE",
        reviewer_id=reviewer_id,
    )

    completed = manager.complete_session(
        session_id=sess.session_id,
        conclusion_state=conclusion_state,
        rationale="Analytical review concluded with supporting evidence.",
        reviewer_id=reviewer_id,
    )
    audits = manager.get_audit_trail(completed.session_id)

    report = service.generate_report(
        session=completed,
        candidate=cand,
        priority_record=prio,
        evidence_report=ev_rep,
        audit_trail=audits,
        reviewer_id=reviewer_id,
    )
    return report, manager, service


# ==============================================================================
# 1. Valid Comparable Reports
# ==============================================================================
def test_valid_comparable_reports():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_comp_1", symbol="BTC-USD", timeframe="1h")
    rep2, _, _ = make_test_report(candidate_id="cand_comp_1", symbol="BTC-USD", timeframe="1h", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    assert cmp_res.comparison_status == "COMPLETED"
    assert cmp_res.comparability_state == ComparabilityState.COMPARABLE
    assert len(cmp_res.report_ids) == 2
    assert cmp_res.shared_evidence is not None


# ==============================================================================
# 2. Partially Comparable Reports
# ==============================================================================
def test_partially_comparable_reports():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_part_1", symbol="BTC-USD", timeframe="1h")
    # Same symbol, different timeframe -> PARTIALLY_COMPARABLE
    rep2, _, _ = make_test_report(candidate_id="cand_part_2", symbol="BTC-USD", timeframe="4h", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    assert cmp_res.comparability_state == ComparabilityState.PARTIALLY_COMPARABLE


# ==============================================================================
# 3. Non-Comparable Reports
# ==============================================================================
def test_non_comparable_reports():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_btc", symbol="BTC-USD", timeframe="1h")
    # Different symbol -> NOT_COMPARABLE
    rep2, _, _ = make_test_report(candidate_id="cand_eth", symbol="ETH-USD", timeframe="1h", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    assert cmp_res.comparability_state == ComparabilityState.NOT_COMPARABLE


# ==============================================================================
# 4. Invalid Report Input
# ==============================================================================
def test_invalid_report_input():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_inv_1")

    # Replaces report_status with INVALID
    object.__setattr__(rep1, "report_status", ReportStatus.INVALID)
    rep2, _, _ = make_test_report(candidate_id="cand_inv_2", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    assert cmp_res.comparability_state == ComparabilityState.INVALID_INPUT
    assert cmp_res.comparison_status == "INVALID"


# ==============================================================================
# 5. Incomplete Evidence
# ==============================================================================
def test_incomplete_evidence():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_inc_1")
    rep2, _, _ = make_test_report(candidate_id="cand_inc_2", mgr=mgr, rep_svc=svc)

    object.__setattr__(rep1, "report_status", ReportStatus.INCOMPLETE)
    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    assert cmp_res.comparability_state == ComparabilityState.INCOMPLETE_EVIDENCE


# ==============================================================================
# 6. Shared Evidence
# ==============================================================================
def test_shared_evidence():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_sh_1")
    rep2, _, _ = make_test_report(candidate_id="cand_sh_1", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    shared_ids = [e.evidence_id for e in cmp_res.shared_evidence]
    assert "ev_trend_01" in shared_ids
    assert "ev_vol_03" in shared_ids


# ==============================================================================
# 7. Added Evidence
# ==============================================================================
def test_added_evidence():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_add_1")
    rep2, _, _ = make_test_report(candidate_id="cand_add_2", mgr=mgr, rep_svc=svc)

    # In rep2, add an additional evidence matrix entry
    new_entry = EvidenceMatrixEntry(
        evidence_id="ev_extra_01",
        evidence_type="MOMENTUM",
        source="indicator:rsi",
        status="VALID",
        reviewer_disposition="SUPPORTING",
    )
    object.__setattr__(rep2, "evidence_matrix", rep2.evidence_matrix + (new_entry,))

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    changed_ids = [e.evidence_id for e in cmp_res.changed_evidence]
    assert "ev_extra_01" in changed_ids


# ==============================================================================
# 8. Removed Evidence
# ==============================================================================
def test_removed_evidence():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_rem_1")
    rep2, _, _ = make_test_report(candidate_id="cand_rem_2", mgr=mgr, rep_svc=svc)

    # In rep2, strip ev_vol_03
    filtered = tuple(e for e in rep2.evidence_matrix if e.evidence_id != "ev_vol_03")
    object.__setattr__(rep2, "evidence_matrix", filtered)
    object.__setattr__(rep2, "conflicting_evidence", tuple(e for e in rep2.conflicting_evidence if e != "ev_vol_03"))

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    changed_ids = [e.evidence_id for e in cmp_res.changed_evidence]
    assert "ev_vol_03" in changed_ids
    assert any(e.diff_category == EvidenceDiffCategory.REMOVED for e in cmp_res.changed_evidence)


# ==============================================================================
# 9. Modified Evidence
# ==============================================================================
def test_modified_evidence():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_mod_1", supp_disp=EvidenceReviewDisposition.SUPPORTING)
    rep2, _, _ = make_test_report(candidate_id="cand_mod_2", supp_disp=EvidenceReviewDisposition.INCONCLUSIVE, mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    changed_ids = [e.evidence_id for e in cmp_res.changed_evidence]
    assert "ev_trend_01" in changed_ids


# ==============================================================================
# 10. Conflicting Evidence
# ==============================================================================
def test_conflicting_evidence():
    engine = InvestigationComparisonEngine()
    # rep1: SUPPORTING, rep2: CONFLICTING for ev_trend_01
    rep1, mgr, svc = make_test_report(candidate_id="cand_conf_1", supp_disp=EvidenceReviewDisposition.SUPPORTING)
    rep2, _, _ = make_test_report(candidate_id="cand_conf_2", supp_disp=EvidenceReviewDisposition.CONFLICTING, mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    conf_ids = [e.evidence_id for e in cmp_res.conflicting_evidence]
    assert "ev_trend_01" in conf_ids
    assert any(e.diff_category == EvidenceDiffCategory.CONFLICTING for e in cmp_res.conflicting_evidence)


# ==============================================================================
# 11. Observation Differences
# ==============================================================================
def test_observation_differences():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_obs_1")
    rep2, _, _ = make_test_report(candidate_id="cand_obs_2", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    assert len(cmp_res.observation_differences) > 0


# ==============================================================================
# 12. FACT vs INTERPRETATION Classification Changes
# ==============================================================================
def test_fact_interpretation_classification_change():
    engine = InvestigationComparisonEngine()
    # rep1 has FACT, rep2 has INTERPRETATION on the same observation
    rep1, mgr, svc = make_test_report(candidate_id="cand_class_1", obs_type=ObservationType.FACT)
    rep2, _, _ = make_test_report(candidate_id="cand_class_2", obs_type=ObservationType.INTERPRETATION, mgr=mgr, rep_svc=svc)

    # Align observation IDs so they can be matched
    obs1 = rep1.observations[0]
    obs2_modified = ObservationMatrixEntry(
        observation_id=obs1.observation_id,
        observation_type=ObservationType.INTERPRETATION.value,
        statement=obs1.statement,
        evidence_refs=(),
        author_id="analyst_bob",
    )
    object.__setattr__(rep2, "observations", (obs2_modified,))

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    class_diffs = [o for o in cmp_res.observation_differences if o.classification_change]
    assert len(class_diffs) == 1
    assert class_diffs[0].old_type == "FACT"
    assert class_diffs[0].new_type == "INTERPRETATION"
    assert class_diffs[0].diff_type == ObservationDiffType.CLASSIFICATION_CHANGE


# ==============================================================================
# 13. Uncertainty Changes
# ==============================================================================
def test_uncertainty_changes():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_unc_1")
    rep2, _, _ = make_test_report(candidate_id="cand_unc_2", mgr=mgr, rep_svc=svc)

    object.__setattr__(rep2, "uncertainty_summary", rep2.uncertainty_summary + ("New macro release pending",))

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    uncs = cmp_res.uncertainty_differences
    assert len(uncs) >= 2


# ==============================================================================
# 14. Limitation Changes
# ==============================================================================
def test_limitation_changes():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_lim_1")
    rep2, _, _ = make_test_report(candidate_id="cand_lim_2", mgr=mgr, rep_svc=svc)

    object.__setattr__(rep2, "limitation_summary", ("Single exchange venue only",))

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    lims = cmp_res.limitation_differences
    assert any(l["status"] == "VARYING" for l in lims)


# ==============================================================================
# 15. Provenance Comparison
# ==============================================================================
def test_provenance_comparison():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_prov_1")
    rep2, _, _ = make_test_report(candidate_id="cand_prov_2", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    p_comp = cmp_res.provenance_comparison
    assert p_comp is not None
    assert isinstance(p_comp.is_identical, bool)


# ==============================================================================
# 16. Data-Quality Comparison
# ==============================================================================
def test_data_quality_comparison():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_qual_1")
    rep2, _, _ = make_test_report(candidate_id="cand_qual_2", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    q_comp = cmp_res.quality_comparison
    assert q_comp is not None
    assert q_comp.all_valid is True
    assert q_comp.status_divergence is False


# ==============================================================================
# 17. Reproducibility Comparison
# ==============================================================================
def test_reproducibility_comparison():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_repro_1")
    rep2, _, _ = make_test_report(candidate_id="cand_repro_2", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    r_comp = cmp_res.reproducibility_comparison
    assert r_comp is not None
    assert r_comp.is_fully_reproducible is True


# ==============================================================================
# 18. Conclusion Comparison
# ==============================================================================
def test_conclusion_comparison():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(
        candidate_id="cand_conc_1", conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS
    )
    rep2, _, _ = make_test_report(
        candidate_id="cand_conc_2", conclusion_state=ConclusionState.EVIDENCE_REMAINS_CONFLICTING, mgr=mgr, rep_svc=svc
    )

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    c_comp = cmp_res.conclusion_comparison
    assert c_comp.conclusions_agree is False
    assert c_comp.divergence_summary is not None


# ==============================================================================
# 19. Review Consistency
# ==============================================================================
def test_review_consistency():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_cons_1")
    rep2, _, _ = make_test_report(candidate_id="cand_cons_2", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    summary = cmp_res.consistency_summary
    assert summary.total_reports_compared == 2
    assert summary.shared_evidence_count > 0


# ==============================================================================
# 20. Discrepancy Generation
# ==============================================================================
def test_discrepancy_generation():
    engine = InvestigationComparisonEngine()
    # Provoke multiple discrepancies: conflicting evidence and conflicting conclusion
    rep1, mgr, svc = make_test_report(
        candidate_id="cand_disc_1",
        supp_disp=EvidenceReviewDisposition.SUPPORTING,
        conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
    )
    rep2, _, _ = make_test_report(
        candidate_id="cand_disc_2",
        supp_disp=EvidenceReviewDisposition.CONFLICTING,
        conclusion_state=ConclusionState.INVALIDATED_BY_EVIDENCE,
        mgr=mgr,
        rep_svc=svc,
    )

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    discs = cmp_res.discrepancy_summary
    assert len(discs) >= 2
    categories = [d.category for d in discs]
    assert DiscrepancyCategory.EVIDENCE_DISPOSITION in categories
    assert DiscrepancyCategory.CONCLUSION_DIVERGENCE in categories


# ==============================================================================
# 21. Discrepancy Resolution State
# ==============================================================================
def test_discrepancy_resolution_state():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_res_1", supp_disp=EvidenceReviewDisposition.SUPPORTING)
    rep2, _, _ = make_test_report(candidate_id="cand_res_1", supp_disp=EvidenceReviewDisposition.CONFLICTING, mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    disc = next(d for d in cmp_res.discrepancy_summary if d.category == DiscrepancyCategory.EVIDENCE_DISPOSITION)
    assert disc.resolution_state == DiscrepancyResolutionState.UNRESOLVED


# ==============================================================================
# 22. Deterministic Comparison ID
# ==============================================================================
def test_deterministic_comparison_id():
    id1 = compute_deterministic_comparison_id(report_ids=["rep_02", "rep_01"])
    id2 = compute_deterministic_comparison_id(report_ids=["rep_01", "rep_02"])
    assert id1 == id2
    assert id1.startswith("cmp_")


# ==============================================================================
# 23. Repeated Identical Comparison
# ==============================================================================
def test_repeated_identical_comparison():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_rep_1")
    rep2, _, _ = make_test_report(candidate_id="cand_rep_2", mgr=mgr, rep_svc=svc)

    cmp1 = engine.compare_reports(reports=[rep1, rep2])
    cmp2 = engine.compare_reports(reports=[rep1, rep2])
    assert cmp1.comparison_id == cmp2.comparison_id
    assert cmp1.comparability_state == cmp2.comparability_state
    assert cmp1.consistency_summary.to_dict() == cmp2.consistency_summary.to_dict()


# ==============================================================================
# 24. Stable Report Ordering
# ==============================================================================
def test_stable_report_ordering():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_order_1")
    rep2, _, _ = make_test_report(candidate_id="cand_order_2", mgr=mgr, rep_svc=svc)

    cmp_forward = engine.compare_reports(reports=[rep1, rep2])
    cmp_reverse = engine.compare_reports(reports=[rep2, rep1])
    assert cmp_forward.comparison_id == cmp_reverse.comparison_id


# ==============================================================================
# 25. NaN Rejection
# ==============================================================================
def test_nan_rejection():
    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("nan"), "test_nan")


# ==============================================================================
# 26. Infinity Rejection
# ==============================================================================
def test_infinity_rejection():
    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("inf"), "test_inf")


# ==============================================================================
# 27. Zero Preservation
# ==============================================================================
def test_zero_preservation():
    # 0 and 0.0 must not be rejected or altered
    validate_numerical_integrity(0, "test_zero_int")
    validate_numerical_integrity(0.0, "test_zero_float")


# ==============================================================================
# 28. Numeric Zero-Baseline Safety
# ==============================================================================
def test_numeric_zero_baseline_safety():
    delta, reason = safe_percentage_delta(0.0, 10.0)
    assert delta is None
    assert reason is not None
    assert "Baseline value is zero" in reason


# ==============================================================================
# 29. EventBus Events
# ==============================================================================
def test_eventbus_events():
    bus = EventBus()
    engine = InvestigationComparisonEngine(event_bus=bus)

    published: List[Event] = []
    bus.subscribe(EventType.INVESTIGATION_REPORTS_COMPARED, lambda e: published.append(e))

    rep1, mgr, svc = make_test_report(candidate_id="cand_eb_1")
    rep2, _, _ = make_test_report(candidate_id="cand_eb_2", mgr=mgr, rep_svc=svc)

    engine.compare_reports(reports=[rep1, rep2])
    assert len(published) == 1
    assert published[0].event_type == EventType.INVESTIGATION_REPORTS_COMPARED


# ==============================================================================
# 30. StateStore Persistence
# ==============================================================================
def test_statestore_persistence():
    store = StateStore()
    engine = InvestigationComparisonEngine(state_store=store)

    rep1, mgr, svc = make_test_report(candidate_id="cand_ss_1")
    rep2, _, _ = make_test_report(candidate_id="cand_ss_2", mgr=mgr, rep_svc=svc)

    cmp_res = engine.compare_reports(reports=[rep1, rep2])
    latest = store.get("investigation_comparison:latest")
    assert latest is not None
    assert latest["comparison_id"] == cmp_res.comparison_id


# ==============================================================================
# 31. Handler Registration
# ==============================================================================
def test_handler_registration():
    orch = MasterOrchestrator()
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    comp_engine = InvestigationComparisonEngine(reporting_service=rep_svc)

    register_investigation_comparison_handlers(
        orchestrator=orch,
        comparison_engine=comp_engine,
        reporting_service=rep_svc,
    )

    rep1, _, _ = make_test_report(candidate_id="cand_orch_1", mgr=mgr, rep_svc=rep_svc)
    rep2, _, _ = make_test_report(candidate_id="cand_orch_2", mgr=mgr, rep_svc=rep_svc)

    # 1. compare_investigation_reports
    req_cmp = OrchestratorRequest(
        request_id="req_cmp_01",
        correlation_id="corr_01",
        command="compare_investigation_reports",
        context={"reports": [rep1, rep2]},
    )
    res_cmp = orch.handle_request(req_cmp)
    assert res_cmp.is_success
    cmp_id = res_cmp.result["comparison_id"]

    # 2. get_investigation_comparison
    req_get = OrchestratorRequest(
        request_id="req_get_01",
        correlation_id="corr_02",
        command="get_investigation_comparison",
        context={"comparison_id": cmp_id},
    )
    res_get = orch.handle_request(req_get)
    assert res_get.is_success

    # 3. list_investigation_comparisons
    req_list = OrchestratorRequest(
        request_id="req_list_01",
        correlation_id="corr_03",
        command="list_investigation_comparisons",
        context={},
    )
    res_list = orch.handle_request(req_list)
    assert res_list.is_success
    assert res_list.result["count"] >= 1

    # 4. get_investigation_comparison_discrepancies
    req_disc = OrchestratorRequest(
        request_id="req_disc_01",
        correlation_id="corr_04",
        command="get_investigation_comparison_discrepancies",
        context={"comparison_id": cmp_id},
    )
    res_disc = orch.handle_request(req_disc)
    assert res_disc.is_success

    # 5. validate_investigation_comparison
    req_val = OrchestratorRequest(
        request_id="req_val_01",
        correlation_id="corr_05",
        command="validate_investigation_comparison",
        context={"comparison_id": cmp_id},
    )
    res_val = orch.handle_request(req_val)
    assert res_val.is_success
    assert res_val.result["validation"]["valid"] is True


# ==============================================================================
# 32. Concurrency
# ==============================================================================
def test_concurrency():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_conc_1")
    rep2, _, _ = make_test_report(candidate_id="cand_conc_2", mgr=mgr, rep_svc=svc)

    results = []

    def task():
        c = engine.compare_reports(reports=[rep1, rep2])
        return c.comparison_id

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(task) for _ in range(10)]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    # All concurrent runs must yield the identical deterministic comparison_id
    assert len(results) == 10
    assert len(set(results)) == 1


# ==============================================================================
# 33. Dashboard GET
# ==============================================================================
def test_dashboard_get():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_dget_1")
    rep2, _, _ = make_test_report(candidate_id="cand_dget_2", mgr=mgr, rep_svc=svc)
    cmp_res = engine.compare_reports(reports=[rep1, rep2])

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        investigation_comparison_engine=engine,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        url_list = f"{server.url}/api/investigation-comparisons"
        req = urllib.request.Request(url_list, headers={"User-Agent": "KaiserTest/1.0"})
        with urllib.request.urlopen(req) as response:
            assert response.status == 200
            data = json.loads(response.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] >= 1

        url_item = f"{server.url}/api/investigation-comparisons/{cmp_res.comparison_id}"
        req_item = urllib.request.Request(url_item, headers={"User-Agent": "KaiserTest/1.0"})
        with urllib.request.urlopen(req_item) as response:
            assert response.status == 200
            data_item = json.loads(response.read().decode("utf-8"))
            assert data_item["status"] == "success"
            assert data_item["comparison"]["comparison_id"] == cmp_res.comparison_id
    finally:
        server.stop()


# ==============================================================================
# 34. Dashboard HEAD
# ==============================================================================
def test_dashboard_head():
    engine = InvestigationComparisonEngine()
    rep1, mgr, svc = make_test_report(candidate_id="cand_dhead_1")
    rep2, _, _ = make_test_report(candidate_id="cand_dhead_2", mgr=mgr, rep_svc=svc)
    cmp_res = engine.compare_reports(reports=[rep1, rep2])

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        investigation_comparison_engine=engine,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("HEAD", f"/api/investigation-comparisons/{cmp_res.comparison_id}")
        res = conn.getresponse()
        assert res.status == 200
        assert res.read() == b""
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 35. Dashboard 405 Method Not Allowed
# ==============================================================================
def test_dashboard_405():
    engine = InvestigationComparisonEngine()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        investigation_comparison_engine=engine,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("POST", "/api/investigation-comparisons", body=b"{}")
        res = conn.getresponse()
        assert res.status == 405
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 36. Dashboard Traversal Protection
# ==============================================================================
def test_dashboard_traversal_protection():
    engine = InvestigationComparisonEngine()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        investigation_comparison_engine=engine,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("GET", "/api/investigation-comparisons/..%2f..%2fetc%2fpasswd")
        res = conn.getresponse()
        assert res.status in (400, 404)
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 37. Prohibited Field Rejection
# ==============================================================================
def test_prohibited_field_rejection():
    prohibited_keys = [
        "action",
        "side",
        "order",
        "entry",
        "exit",
        "quantity",
        "position_size",
        "leverage",
        "stop_loss",
        "take_profit",
        "trade_ranking",
        "profitability_ranking",
    ]
    for key in prohibited_keys:
        with pytest.raises(ValidationError):
            validate_no_prohibited_fields({key: "prohibited_val"})


# ==============================================================================
# 38. Execution Lock Invariant
# ==============================================================================
def test_execution_lock_invariant():
    settings = Settings()
    assert settings.execution_locked is True


# ==============================================================================
# 39. No Broker Initialization
# ==============================================================================
def test_no_broker_initialization():
    settings = Settings()
    broker_keys = [k for k in settings.to_dict().keys() if "broker" in k.lower()]
    for k in broker_keys:
        val = getattr(settings, k)
        assert val in (None, False, "", "none") or str(val).lower() == "none"


# ==============================================================================
# 40. No Autonomous Execution
# ==============================================================================
def test_no_autonomous_execution():
    settings = Settings()
    assert getattr(settings, "autonomous_trading_enabled", False) is False


# ==============================================================================
# 41. JUJU Isolation
# ==============================================================================
def test_juju_isolation():
    juju_dir = os.path.join(os.path.dirname(__file__), "..", "juju")
    assert not os.path.exists(juju_dir)
