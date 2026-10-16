"""KAISER — Phase 2 Step 27: Investigation Reporting & Evidence Review Audit Tests.

Covers all 38 required verification areas:
1. valid report generation
2. missing session
3. invalid session
4. candidate linkage validation
5. priority linkage validation
6. evidence preservation
7. supporting evidence preservation
8. conflicting evidence preservation
9. missing evidence preservation
10. uncertainty preservation
11. limitation preservation
12. fact/interpretation separation
13. human vs machine attribution
14. audit summary
15. audit sequence validation
16. malformed audit detection
17. reproducibility record
18. unavailable reproducibility handling
19. deterministic report ID
20. report versioning
21. superseded report handling
22. voice summary safety
23. prohibited field rejection
24. NaN rejection
25. Infinity rejection
26. zero preservation
27. StateStore persistence
28. EventBus lifecycle events
29. handler registration
30. concurrent report generation
31. dashboard GET
32. dashboard HEAD
33. dashboard 405
34. traversal protection
35. execution lock
36. no broker initialization
37. no autonomous execution
38. JUJU isolation
"""

import copy
import http.client
import math
import os
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, cast
import urllib.request

import pytest

from app.analysis.opportunities.investigation import (
    AnalyticalConclusion,
    ConclusionState,
    EvidenceReviewDisposition,
    EvidenceReviewRecord,
    InvestigationAuditRecord,
    InvestigationLimitation,
    InvestigationObservation,
    InvestigationReviewNote,
    InvestigationSession,
    InvestigationSessionManager,
    InvestigationUncertainty,
    ObservationType,
    SessionState,
    SourceType,
)
from app.analysis.opportunities.investigation.reporting import (
    AttributionType,
    AuditSummary,
    EvidenceMatrixEntry,
    InvestigationReport,
    InvestigationReportVoiceSummary,
    InvestigationReportingService,
    ObservationMatrixEntry,
    ReportStatus,
    ReproducibilityRecord,
    compute_deterministic_report_id,
    register_investigation_reporting_handlers,
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
from app.analysis.opportunities.prioritization.engine import (
    OpportunityPrioritizationEngine,
)
from app.analysis.evidence.models import (
    EvidenceItem,
    EvidenceStatus,
    EvidenceSynthesisReport,
    EvidenceType,
)
from app.core.config import Settings, get_settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer


def make_test_candidate(
    candidate_id: str = "cand_test_01",
    supporting_eids: Sequence[str] = ("ev_trend_01", "ev_mom_02"),
    conflicting_eids: Sequence[str] = ("ev_vol_03",),
) -> OpportunityCandidate:
    """Helper to construct a valid OpportunityCandidate."""
    return OpportunityCandidate(
        candidate_id=candidate_id,
        symbol="BTC-USD",
        timeframe="1h",
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
        reproducibility_id="repro_abc_123",
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


def make_completed_session(
    candidate: OpportunityCandidate,
    priority: Optional[OpportunityPriorityRecord] = None,
    mgr: Optional[InvestigationSessionManager] = None,
) -> InvestigationSession:
    """Helper to run a session to REVIEW_COMPLETE with conclusion."""
    manager = mgr or InvestigationSessionManager()
    sess = manager.create_session(candidate=candidate, priority_record=priority, reviewer_id="analyst_alice")
    sess = manager.start_session(sess.session_id, reviewer_id="analyst_alice")

    supp_id = candidate.supporting_evidence_refs[0] if candidate.supporting_evidence_refs else "ev_trend_01"
    conf_id = (
        candidate.conflicting_evidence_refs[0]
        if candidate.conflicting_evidence_refs
        else (candidate.supporting_evidence_refs[1] if len(candidate.supporting_evidence_refs) > 1 else supp_id)
    )

    # Add evidence reviews
    manager.review_evidence(
        session_id=sess.session_id,
        evidence_id=supp_id,
        disposition=EvidenceReviewDisposition.SUPPORTING,
        notes="Trend verified across multiple metrics.",
        reviewer_id="analyst_alice",
    )
    if conf_id != supp_id:
        manager.review_evidence(
            session_id=sess.session_id,
            evidence_id=conf_id,
            disposition=EvidenceReviewDisposition.CONFLICTING,
            notes="Tail volatility elevated; creates conflicting risk profile.",
            reviewer_id="analyst_alice",
        )

    # Add observation
    manager.add_observation(
        session_id=sess.session_id,
        observation_type=ObservationType.FACT,
        statement="Trend confirmed on hourly horizon.",
        evidence_refs=(supp_id,),
        reviewer_id="analyst_alice",
    )

    # Add uncertainty & limitation
    manager.add_uncertainty(
        session_id=sess.session_id,
        description="Macro news release expected within 4 hours.",
        severity="MEDIUM",
        reviewer_id="analyst_alice",
    )
    manager.add_limitation(
        session_id=sess.session_id,
        description="Historical replay limited to 30 days.",
        category="SAMPLE_COVERAGE",
        reviewer_id="analyst_alice",
    )

    # Set conclusion and complete
    completed = manager.complete_session(
        session_id=sess.session_id,
        conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
        rationale="Strong technical evidence supports further analytical inspection.",
        reviewer_id="analyst_alice",
    )
    return completed


# ==============================================================================
# 1. Valid Report Generation
# ==============================================================================
def test_valid_report_generation():
    bus = EventBus()
    store = StateStore()
    mgr = InvestigationSessionManager(event_bus=bus, state_store=store)
    rep_svc = InvestigationReportingService(event_bus=bus, state_store=store)

    cand = make_test_candidate("cand_rep_01")
    prio = make_test_priority(cand)
    ev_rep = make_test_evidence_report("rep_synth_01")
    sess = make_completed_session(cand, prio, mgr=mgr)
    audits = mgr.get_audit_trail(sess.session_id)

    report = rep_svc.generate_report(
        session=sess,
        candidate=cand,
        priority_record=prio,
        evidence_report=ev_rep,
        audit_trail=audits,
    )

    assert report.report_id.startswith("rep_inv_")
    assert report.session_id == sess.session_id
    assert report.candidate_id == "cand_rep_01"
    assert report.priority_id == prio.priority_id
    assert report.report_status == ReportStatus.GENERATED
    assert report.reviewer_id == "analyst_alice"
    assert report.version == 1

    # Check evidence preservation
    assert len(report.evidence_matrix) >= 2
    assert "ev_trend_01" in report.supporting_evidence
    assert "ev_vol_03" in report.conflicting_evidence

    # Check observations & uncertainties & limitations
    assert len(report.observations) == 1
    assert "Macro news release expected within 4 hours." in report.uncertainty_summary
    assert "Historical replay limited to 30 days." in report.limitation_summary

    # Check analytical conclusion
    assert report.analytical_conclusion is not None
    assert report.analytical_conclusion["conclusion_state"] == ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS.value

    # Check reproducibility record
    assert report.reproducibility_record is not None
    assert report.reproducibility_record.is_reproducible is True
    assert report.reproducibility_record.reproducibility_id == "repro_abc_123"

    # Check audit summary
    assert report.audit_summary is not None
    assert report.audit_summary.sequence_integrity is True
    assert report.audit_summary.total_events > 0

    # Serialization roundtrip
    d = report.to_dict()
    assert d["report_id"] == report.report_id
    assert d["report_status"] == "GENERATED"


# ==============================================================================
# 2. Missing Session
# ==============================================================================
def test_missing_session():
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_missing_sess")
    with pytest.raises(ValidationError, match="Source investigation session must be provided"):
        rep_svc.generate_report(session=cast(Any, None), candidate=cand)


# ==============================================================================
# 3. Invalid Session
# ==============================================================================
def test_invalid_session():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_inv_sess")
    sess = mgr.create_session(cand)
    sess = mgr.start_session(sess.session_id)
    # Invalidate session
    sess = mgr.invalidate_session(sess.session_id, reason="Corrupted market feed")

    report = rep_svc.generate_report(session=sess, candidate=cand)
    assert report.report_status == ReportStatus.INVALID


# ==============================================================================
# 4. Candidate Linkage Validation
# ==============================================================================
def test_candidate_linkage_validation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand1 = make_test_candidate("cand_01")
    cand2 = make_test_candidate("cand_02")
    sess = mgr.create_session(cand1)

    with pytest.raises(ValidationError, match="Candidate linkage mismatch"):
        rep_svc.generate_report(session=sess, candidate=cand2)


# ==============================================================================
# 5. Priority Linkage Validation
# ==============================================================================
def test_priority_linkage_validation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_prio_val")
    cand_other = make_test_candidate("cand_other")
    prio_other = make_test_priority(cand_other)

    sess = mgr.create_session(cand)

    with pytest.raises(ValidationError, match="Candidate linkage mismatch on priority record"):
        rep_svc.generate_report(session=sess, candidate=cand, priority_record=prio_other)


# ==============================================================================
# 6. Evidence Preservation
# ==============================================================================
def test_evidence_preservation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_ev_pres", supporting_eids=("ev_a", "ev_b"), conflicting_eids=("ev_c",))
    sess = make_completed_session(cand, mgr=mgr)

    report = rep_svc.generate_report(session=sess, candidate=cand)
    eids = {e.evidence_id for e in report.evidence_matrix}
    assert "ev_a" in eids
    assert "ev_b" in eids
    assert "ev_c" in eids


# ==============================================================================
# 7. Supporting Evidence Preservation
# ==============================================================================
def test_supporting_evidence_preservation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_supp_pres", supporting_eids=("ev_supp_1", "ev_supp_2"))
    sess = make_completed_session(cand, mgr=mgr)

    report = rep_svc.generate_report(session=sess, candidate=cand)
    assert "ev_supp_1" in report.supporting_evidence
    assert "ev_supp_2" in report.supporting_evidence


# ==============================================================================
# 8. Conflicting Evidence Preservation
# ==============================================================================
def test_conflicting_evidence_preservation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_conf_pres", conflicting_eids=("ev_conflict_1",))
    sess = make_completed_session(cand, mgr=mgr)

    report = rep_svc.generate_report(session=sess, candidate=cand)
    assert "ev_conflict_1" in report.conflicting_evidence


# ==============================================================================
# 9. Missing Evidence Preservation
# ==============================================================================
def test_missing_evidence_preservation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_miss_pres")
    sess = mgr.create_session(cand)
    sess = mgr.start_session(sess.session_id)
    # Review with disposition MISSING
    mgr.review_evidence(
        session_id=sess.session_id,
        evidence_id="ev_trend_01",
        disposition=EvidenceReviewDisposition.MISSING,
        notes="Orderbook depth missing from data vendor.",
    )
    completed = mgr.complete_session(
        session_id=sess.session_id,
        conclusion_state=ConclusionState.INSUFFICIENT_EVIDENCE,
        rationale="Orderbook evidence missing.",
    )

    report = rep_svc.generate_report(session=completed, candidate=cand)
    assert "ev_trend_01" in report.missing_evidence


# ==============================================================================
# 10. Uncertainty Preservation
# ==============================================================================
def test_uncertainty_preservation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_uncert_pres")
    sess = make_completed_session(cand, mgr=mgr)

    report = rep_svc.generate_report(session=sess, candidate=cand)
    assert len(report.uncertainty_summary) >= 1
    assert any("Macro news" in u for u in report.uncertainty_summary)


# ==============================================================================
# 11. Limitation Preservation
# ==============================================================================
def test_limitation_preservation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_lim_pres")
    sess = make_completed_session(cand, mgr=mgr)

    report = rep_svc.generate_report(session=sess, candidate=cand)
    assert len(report.limitation_summary) >= 1
    assert any("Historical replay limited" in l for l in report.limitation_summary)


# ==============================================================================
# 12. Fact / Interpretation Separation
# ==============================================================================
def test_fact_interpretation_separation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_fact_interp")
    sess = mgr.create_session(cand)
    sess = mgr.start_session(sess.session_id)

    # 1 Fact citing verified evidence
    mgr.add_observation(
        session_id=sess.session_id,
        observation_type=ObservationType.FACT,
        statement="200-period moving average slope is positive.",
        evidence_refs=("ev_trend_01",),
    )
    # 1 Interpretation
    mgr.add_observation(
        session_id=sess.session_id,
        observation_type=ObservationType.INTERPRETATION,
        statement="Market might be transitioning to momentum regime.",
        evidence_refs=(),
    )
    completed = mgr.complete_session(
        session_id=sess.session_id,
        conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
        rationale="Observations recorded.",
    )

    report = rep_svc.generate_report(session=completed, candidate=cand)
    types = [o.observation_type for o in report.observations]
    assert ObservationType.FACT.value in types
    assert ObservationType.INTERPRETATION.value in types

    fact_obs = [o for o in report.observations if o.observation_type == ObservationType.FACT.value][0]
    assert len(fact_obs.evidence_refs) > 0


# ==============================================================================
# 13. Human vs Machine Attribution
# ==============================================================================
def test_human_vs_machine_attribution():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_attrib")
    sess = make_completed_session(cand, mgr=mgr)

    report = rep_svc.generate_report(session=sess, candidate=cand)
    assert report.candidate_summary["attribution"] == AttributionType.MACHINE_GENERATED_EVIDENCE.value
    assert report.evidence_summary["attribution"] == AttributionType.MACHINE_GENERATED_EVIDENCE.value
    assert report.observations[0].source_type == AttributionType.HUMAN_REVIEW.value
    assert report.analytical_conclusion is not None
    assert report.analytical_conclusion["attribution"] == AttributionType.HUMAN_REVIEW.value
    assert report.report_provenance["attribution"] == AttributionType.DERIVED_ANALYSIS.value


# ==============================================================================
# 14. Audit Summary
# ==============================================================================
def test_audit_summary():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_audit_sum")
    sess = make_completed_session(cand, mgr=mgr)
    audits = mgr.get_audit_trail(sess.session_id)

    report = rep_svc.generate_report(session=sess, candidate=cand, audit_trail=audits)
    summary = report.audit_summary
    assert summary is not None
    assert summary.total_events == len(audits)
    assert summary.evidence_review_events == 2
    assert summary.observations_added == 1
    assert summary.conclusion_changes == 1
    assert summary.sequence_integrity is True


# ==============================================================================
# 15. Audit Sequence Validation
# ==============================================================================
def test_audit_sequence_validation():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_audit_seq")
    sess = make_completed_session(cand, mgr=mgr)
    audits = mgr.get_audit_trail(sess.session_id)

    report = rep_svc.generate_report(session=sess, candidate=cand, audit_trail=audits)
    assert report.audit_summary is not None
    assert report.audit_summary.sequence_integrity is True
    assert len(report.audit_summary.integrity_issues) == 0


# ==============================================================================
# 16. Malformed Audit Detection
# ==============================================================================
def test_malformed_audit_detection():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_malformed_audit")
    sess = make_completed_session(cand, mgr=mgr)
    audits = list(mgr.get_audit_trail(sess.session_id))

    # Inject malformed record with invalid backward timestamp and duplicate audit_id
    bad_rec = InvestigationAuditRecord(
        audit_id=audits[0].audit_id,  # Duplicate!
        session_id=sess.session_id,
        event_type="CORRUPT_MUTATION",
        timestamp=audits[0].timestamp - 1000.0,  # Backward jump!
        reviewer_id="attacker",
        previous_state=SessionState.REVIEW_COMPLETE.value,
        new_state=SessionState.ACTIVE.value,  # Invalid transition!
        action_summary="Illegal state revert",
    )
    audits.append(bad_rec)

    report = rep_svc.generate_report(session=sess, candidate=cand, audit_trail=audits)
    assert report.audit_summary is not None
    assert report.audit_summary.sequence_integrity is False
    assert len(report.audit_summary.integrity_issues) >= 2
    assert report.report_status == ReportStatus.INVALID


# ==============================================================================
# 17. Reproducibility Record
# ==============================================================================
def test_reproducibility_record():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_repro_valid")
    sess = make_completed_session(cand, mgr=mgr)

    report = rep_svc.generate_report(session=sess, candidate=cand)
    rec = report.reproducibility_record
    assert rec is not None
    assert rec.is_reproducible is True
    assert rec.source_analysis_run_ids == ("run_01", "run_02")
    assert rec.reproducibility_id == "repro_abc_123"
    assert rec.source_report_id == "rep_synth_01"


# ==============================================================================
# 18. Unavailable Reproducibility Handling
# ==============================================================================
def test_unavailable_reproducibility_handling():
    rep_svc = InvestigationReportingService()
    # Candidate without reproducibility metadata
    cand = OpportunityCandidate(
        candidate_id="cand_no_repro",
        symbol="ETH-USD",
        timeframe="1h",
        assessment_state=OpportunityState.ANALYTICALLY_SUPPORTED,
        evidence_status=EvidenceStatus.AVAILABLE,
        evidence_completeness_score=0.8,
        confidence_state=ConfidenceState.LOW,
        source_evidence_report_id="rep_placeholder",
        generated_at=1000.0,
        source_analysis_run_ids=(),  # Empty!
        reproducibility_id=None,  # None!
    )
    mgr = InvestigationSessionManager()
    sess = mgr.create_session(cand)
    sess = mgr.start_session(sess.session_id)
    sess = mgr.complete_session(sess.session_id, conclusion_state=ConclusionState.INSUFFICIENT_EVIDENCE, rationale="No runs")

    report = rep_svc.generate_report(session=sess, candidate=cand)
    assert report.reproducibility_record is not None
    assert report.reproducibility_record.is_reproducible is False
    assert len(report.reproducibility_record.reproducibility_notes) > 0


# ==============================================================================
# 19. Deterministic Report ID
# ==============================================================================
def test_deterministic_report_id():
    id1 = compute_deterministic_report_id(
        session_id="sess_abc",
        candidate_id="cand_123",
        priority_id="prio_456",
        source_report_id="rep_789",
        reproducibility_id="repro_999",
        version=1,
    )
    id2 = compute_deterministic_report_id(
        session_id="sess_abc",
        candidate_id="cand_123",
        priority_id="prio_456",
        source_report_id="rep_789",
        reproducibility_id="repro_999",
        version=1,
    )
    assert id1 == id2
    assert id1.startswith("rep_inv_")


# ==============================================================================
# 20. Report Versioning
# ==============================================================================
def test_report_versioning():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_versioning")
    sess1 = make_completed_session(cand, mgr=mgr)
    rep1 = rep_svc.generate_report(session=sess1, candidate=cand)
    assert rep1.version == 1

    # Reopen follow-up session (revision 2)
    sess2 = mgr.reopen_session(sess1.session_id, reviewer_id="analyst_bob")
    sess2 = mgr.start_session(sess2.session_id, reviewer_id="analyst_bob")
    sess2 = mgr.complete_session(sess2.session_id, conclusion_state=ConclusionState.REVIEW_COMPLETED_WITH_LIMITATIONS, rationale="Follow-up complete")

    rep2 = rep_svc.generate_report(session=sess2, candidate=cand)
    assert rep2.version == 2
    assert rep2.parent_report_id == rep1.report_id


# ==============================================================================
# 21. Superseded Report Handling
# ==============================================================================
def test_superseded_report_handling():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_superseded")
    ev_rep = make_test_evidence_report()
    sess1 = make_completed_session(cand, mgr=mgr)
    audits1 = mgr.get_audit_trail(sess1.session_id)
    rep1 = rep_svc.generate_report(session=sess1, candidate=cand, evidence_report=ev_rep, audit_trail=audits1)
    assert rep1.report_status == ReportStatus.GENERATED

    # Generate follow-up revision
    sess2 = mgr.reopen_session(sess1.session_id)
    sess2 = mgr.start_session(sess2.session_id)
    sess2 = mgr.complete_session(sess2.session_id, conclusion_state=ConclusionState.EVIDENCE_REQUIRES_FURTHER_REVIEW, rationale="Updated")
    audits2 = mgr.get_audit_trail(sess2.session_id)

    rep2 = rep_svc.generate_report(session=sess2, candidate=cand, evidence_report=ev_rep, audit_trail=audits2)
    assert rep2.version == 2

    # Check that rep1 is now SUPERSEDED
    old_rep = rep_svc.get_report(rep1.report_id)
    assert old_rep is not None
    assert old_rep.report_status == ReportStatus.SUPERSEDED


# ==============================================================================
# 22. Voice Summary Safety
# ==============================================================================
def test_voice_summary_safety():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_voice_safe")
    sess = make_completed_session(cand, mgr=mgr)

    report = rep_svc.generate_report(session=sess, candidate=cand)
    vs = report.voice_summary
    assert vs is not None
    assert "investigation report" in vs.headline.lower()

    # Reject forbidden trading terms
    forbidden = ["buy", "sell", "hold", "long", "short", "execute", "order", "leverage"]
    for word in forbidden:
        assert word not in vs.full_speech_script.lower().split()


# ==============================================================================
# 23. Prohibited Field Rejection
# ==============================================================================
def test_prohibited_field_rejection():
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_prohibited")
    sess = make_completed_session(cand, mgr=mgr)

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
        "broker_order",
        "execution_instruction",
        "expected_profit",
        "profit_probability",
        "trade_probability",
    ]

    for bad_key in prohibited_keys:
        with pytest.raises(ValidationError, match="Prohibited trade recommendation field"):
            rep_svc.generate_report(
                session=sess,
                candidate=cand,
                metadata={bad_key: "forbidden_value"},
            )


# ==============================================================================
# 24. NaN Rejection
# ==============================================================================
def test_nan_rejection():
    with pytest.raises(ValidationError, match="cannot be NaN"):
        validate_numerical_integrity(float("nan"), "test_nan")


# ==============================================================================
# 25. Infinity Rejection
# ==============================================================================
def test_infinity_rejection():
    with pytest.raises(ValidationError, match="cannot be Infinity"):
        validate_numerical_integrity(float("inf"), "test_pos_inf")
    with pytest.raises(ValidationError, match="cannot be Infinity"):
        validate_numerical_integrity(float("-inf"), "test_neg_inf")


# ==============================================================================
# 26. Zero Preservation
# ==============================================================================
def test_zero_preservation():
    # 0 and 0.0 must be accepted without error
    validate_numerical_integrity(0.0, "zero_float")
    validate_numerical_integrity(0, "zero_int")

    sum_zero = AuditSummary(
        total_events=0,
        evidence_review_events=0,
        observations_added=0,
        conclusion_changes=0,
        session_state_transitions=0,
        reviewer_identifiers=(),
        first_event_timestamp=0.0,
        last_event_timestamp=0.0,
    )
    assert sum_zero.total_events == 0
    assert sum_zero.first_event_timestamp == 0.0


# ==============================================================================
# 27. StateStore Persistence
# ==============================================================================
def test_statestore_persistence():
    store = StateStore()
    rep_svc = InvestigationReportingService(state_store=store)
    cand = make_test_candidate("cand_store_persist")
    sess = make_completed_session(cand)

    report = rep_svc.generate_report(session=sess, candidate=cand)

    latest = store.get("investigation_report:latest")
    assert latest is not None
    assert latest["report_id"] == report.report_id

    direct = store.get(f"investigation_report:{report.report_id}")
    assert direct is not None
    assert direct["report_id"] == report.report_id

    history = store.get("investigation_report:history")
    assert isinstance(history, list)
    assert any(h["report_id"] == report.report_id for h in history)


# ==============================================================================
# 28. EventBus Lifecycle Events
# ==============================================================================
def test_eventbus_lifecycle_events():
    bus = EventBus()
    rep_svc = InvestigationReportingService(event_bus=bus)
    cand = make_test_candidate("cand_eventbus")
    sess = make_completed_session(cand)

    events_received: List[Event] = []
    bus.subscribe(EventType.INVESTIGATION_REPORT_GENERATED, lambda e: events_received.append(e))

    report = rep_svc.generate_report(session=sess, candidate=cand)
    assert len(events_received) == 1
    assert events_received[0].payload["report_id"] == report.report_id


# ==============================================================================
# 29. Handler Registration
# ==============================================================================
def test_handler_registration():
    orch = MasterOrchestrator()
    mgr = InvestigationSessionManager()
    rep_svc = InvestigationReportingService()

    register_investigation_reporting_handlers(
        orchestrator=orch,
        reporting_service=rep_svc,
        session_manager=mgr,
    )

    cand = make_test_candidate("cand_orch_test")
    sess = make_completed_session(cand, mgr=mgr)

    # 1. generate_investigation_report
    req_gen = OrchestratorRequest(
        request_id="req_gen_01",
        correlation_id="corr_01",
        command="generate_investigation_report",
        context={"session_id": sess.session_id, "candidate": cand},
    )
    res_gen = orch.handle_request(req_gen)
    assert res_gen.is_success
    resp_gen = res_gen.result
    assert resp_gen["status"] == "success"
    rep_id = resp_gen["report_id"]

    # 2. get_investigation_report
    req_get = OrchestratorRequest(
        request_id="req_get_01",
        correlation_id="corr_02",
        command="get_investigation_report",
        context={"report_id": rep_id},
    )
    res_get = orch.handle_request(req_get)
    assert res_get.is_success
    resp_get = res_get.result
    assert resp_get["status"] == "success"

    # 3. list_investigation_reports
    req_list = OrchestratorRequest(
        request_id="req_list_01",
        correlation_id="corr_03",
        command="list_investigation_reports",
        context={"candidate_id": "cand_orch_test"},
    )
    res_list = orch.handle_request(req_list)
    assert res_list.is_success
    resp_list = res_list.result
    assert resp_list["status"] == "success"
    assert resp_list["count"] == 1

    # 4. get_investigation_report_audit
    req_audit = OrchestratorRequest(
        request_id="req_audit_01",
        correlation_id="corr_04",
        command="get_investigation_report_audit",
        context={"report_id": rep_id},
    )
    res_audit = orch.handle_request(req_audit)
    assert res_audit.is_success
    resp_audit = res_audit.result
    assert resp_audit["status"] == "success"
    assert resp_audit["audit_summary"] is not None

    # 5. get_investigation_report_evidence
    req_ev = OrchestratorRequest(
        request_id="req_ev_01",
        correlation_id="corr_05",
        command="get_investigation_report_evidence",
        context={"report_id": rep_id},
    )
    res_ev = orch.handle_request(req_ev)
    assert res_ev.is_success
    resp_ev = res_ev.result
    assert resp_ev["status"] == "success"
    assert resp_ev["count"] > 0

    # 6. validate_investigation_report
    req_val = OrchestratorRequest(
        request_id="req_val_01",
        correlation_id="corr_06",
        command="validate_investigation_report",
        context={"report_id": rep_id},
    )
    res_val = orch.handle_request(req_val)
    assert res_val.is_success
    resp_val = res_val.result
    assert resp_val["status"] == "success"
    assert resp_val["validation"]["valid"] is True


# ==============================================================================
# 30. Concurrent Report Generation
# ==============================================================================
def test_concurrent_report_generation():
    rep_svc = InvestigationReportingService()
    cand = make_test_candidate("cand_concurrent")
    mgr = InvestigationSessionManager()
    sess = make_completed_session(cand, mgr=mgr)

    reports: List[InvestigationReport] = []
    errors: List[Exception] = []

    def worker():
        try:
            r = rep_svc.generate_report(session=sess, candidate=cand)
            reports.append(r)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    assert len(reports) == 10
    # Deterministic report IDs
    assert all(r.report_id == reports[0].report_id for r in reports)


# ==============================================================================
# 31. Dashboard GET
# ==============================================================================
def test_dashboard_get():
    store = StateStore()
    mgr = InvestigationSessionManager(state_store=store)
    rep_svc = InvestigationReportingService(state_store=store)
    cand = make_test_candidate("cand_dash_get")
    sess = make_completed_session(cand, mgr=mgr)
    report = rep_svc.generate_report(session=sess, candidate=cand)

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        investigation_reporting_service=rep_svc,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        url_base = server.url

        # 1. Listing
        with urllib.request.urlopen(f"{url_base}/api/investigation-reports") as resp:
            assert resp.status == 200
            import json
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["count"] >= 1

        # 2. Single Report
        with urllib.request.urlopen(f"{url_base}/api/investigation-reports/{report.report_id}") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["report"]["report_id"] == report.report_id

        # 3. Evidence Sub-resource
        with urllib.request.urlopen(f"{url_base}/api/investigation-reports/{report.report_id}/evidence") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert "evidence_matrix" in data

        # 4. Audit Sub-resource
        with urllib.request.urlopen(f"{url_base}/api/investigation-reports/{report.report_id}/audit") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert "audit_summary" in data

    finally:
        server.stop()


# ==============================================================================
# 32. Dashboard HEAD
# ==============================================================================
def test_dashboard_head():
    rep_svc = InvestigationReportingService()
    server = PortfolioDashboardServer(host="127.0.0.1", port=0, investigation_reporting_service=rep_svc)
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("HEAD", "/api/investigation-reports")
        resp = conn.getresponse()
        assert resp.status == 200
        # HEAD returns empty body
        body = resp.read()
        assert len(body) == 0
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 33. Dashboard 405 Method Not Allowed
# ==============================================================================
def test_dashboard_405():
    rep_svc = InvestigationReportingService()
    server = PortfolioDashboardServer(host="127.0.0.1", port=0, investigation_reporting_service=rep_svc)
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("POST", "/api/investigation-reports", body="{}")
        resp = conn.getresponse()
        assert resp.status == 405
        assert "Allow" in resp.headers
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 34. Traversal Protection
# ==============================================================================
def test_dashboard_traversal_protection():
    rep_svc = InvestigationReportingService()
    server = PortfolioDashboardServer(host="127.0.0.1", port=0, investigation_reporting_service=rep_svc)
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("GET", "/api/investigation-reports/..%2fetc%2fpasswd")
        resp = conn.getresponse()
        assert resp.status in (400, 404)
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 35. Execution Lock
# ==============================================================================
def test_execution_lock_invariant():
    settings = get_settings()
    assert settings.execution_locked is True


# ==============================================================================
# 36. No Broker Initialization
# ==============================================================================
def test_no_broker_initialization():
    settings = get_settings()
    broker_keys = [k for k in settings.to_dict().keys() if "broker" in k.lower()]
    for k in broker_keys:
        val = getattr(settings, k)
        assert val in (None, False, "", "none") or str(val).lower() == "none"


# ==============================================================================
# 37. No Autonomous Execution
# ==============================================================================
def test_no_autonomous_execution():
    from app.bootstrap import bootstrap_system
    status = bootstrap_system()
    assert status.trading_engine == "not initialized"
    assert status.execution_engine == "locked"


# ==============================================================================
# 38. JUJU Isolation
# ==============================================================================
def test_juju_isolation():
    import subprocess
    diff = subprocess.check_output(["git", "status", "--porcelain"], text=True)
    juju_mods = [line for line in diff.splitlines() if "juju" in line.lower()]
    assert len(juju_mods) == 0, f"JUJU files were modified: {juju_mods}"
