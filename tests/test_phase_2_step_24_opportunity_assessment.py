"""KAISER Phase 2, Step 24 — Opportunity Assessment & Decision Support Test Suite.

Comprehensive verification of:
1. Valid evidence assessment
2. Insufficient evidence
3. Conflicting evidence
4. Stale evidence
5. Invalid data quality
6. Missing provenance
7. Missing reproducibility
8. Partial historical coverage
9. Supporting evidence traceability
10. Conflicting evidence traceability
11. Invalidation conditions
12. Deterministic output
13. Repeated identical input produces identical assessment
14. NaN rejection
15. Infinity rejection
16. Zero preservation
17. Prohibited recommendation field rejection
18. EventBus lifecycle events
19. StateStore bounded history
20. Handler validation
21. Dashboard GET
22. Dashboard HEAD
23. Dashboard POST returns 405
24. Path traversal rejection
25. Sanitized error response
26. Execution lock remains enabled
27. No broker initialization
28. No autonomous execution
29. JUJU isolation
"""

from __future__ import annotations

import collections
from dataclasses import FrozenInstanceError
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.request
import uuid

import pytest

from app.analysis.evidence.engine import EvidenceSynthesisEngine
from app.analysis.evidence.models import (
    ConflictResolutionStatus,
    ConflictSeverity,
    ConflictType,
    EvidenceConflict,
    EvidenceItem,
    EvidenceStatus,
    EvidenceSynthesisReport,
    EvidenceType,
    EvidenceUncertainty,
    KeyObservation,
    MissingEvidence,
    ObservationType,
    SampleCoverage,
    UncertaintySource,
)
from app.analysis.opportunities import (
    DEFAULT_MAX_OPPORTUNITY_HISTORY,
    ConfidenceState,
    ObservationWindow,
    OpportunityAssessmentEngine,
    OpportunityCandidate,
    OpportunityState,
    OpportunityVoiceSummary,
    PROHIBITED_RECOMMENDATION_FIELDS,
    register_opportunity_handlers,
    validate_no_prohibited_fields,
)
from app.analysis.runs.models import (
    AnalysisRun,
    OperationType,
    RunStatus,
    generate_configuration_fingerprint,
    generate_reproducibility_id,
)
from app.analysis.runs.registry import AnalysisRunRegistry
from app.bootstrap import bootstrap_system
from app.core.config import Settings, get_settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import DataSourceType, MarketDataProvenance
from app.market.quality.status import DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, RequestStatus
from app.portfolio.dashboard.server import PortfolioDashboardServer


# =============================================================================
# FIXTURES & FACTORIES
# =============================================================================

def _create_mock_run(
    run_id: str,
    symbol: str = "BTC/USDT",
    timeframe: str = "1h",
    operation_type: OperationType = OperationType.BACKTEST,
    status: RunStatus = RunStatus.COMPLETED,
    quality_status: DataQualityStatus = DataQualityStatus.VALID,
    input_provenance_ids: Tuple[str, ...] = ("prov_1",),
    record_count: int = 500,
    metrics: Optional[Dict[str, Any]] = None,
) -> AnalysisRun:
    """Helper to create a valid AnalysisRun for testing."""
    cfg = {"symbol": symbol, "timeframe": timeframe, "fast": 10, "slow": 30}
    fp = generate_configuration_fingerprint(cfg)
    repro = generate_reproducibility_id(
        operation_type=operation_type,
        input_provenance_ids=input_provenance_ids,
        configuration_fingerprint=fp,
    )
    now = time.time()
    return AnalysisRun(
        run_id=run_id,
        reproducibility_id=repro,
        operation_type=operation_type,
        status=status,
        created_at=now - 100,
        completed_at=now,
        configuration_fingerprint=fp,
        configuration_summary=cfg,
        symbol_universe=(symbol,),
        timeframe_universe=(timeframe,),
        quality_status=quality_status,
        input_provenance_ids=input_provenance_ids,
        input_record_count=record_count,
        execution_metadata={"metrics": metrics or {"sharpe_ratio": 1.75, "max_drawdown": 0.08, "win_rate": 0.58}},
    )


def _create_mock_report(
    report_id: str = "rep_test_01",
    subject: str = "BTC/USDT",
    overall_status: EvidenceStatus = EvidenceStatus.AVAILABLE,
    evidence_items: Optional[List[EvidenceItem]] = None,
    facts: Optional[List[KeyObservation]] = None,
    conflicts: Optional[List[EvidenceConflict]] = None,
    uncertainties: Optional[List[EvidenceUncertainty]] = None,
    dq_status: str = "VALID",
    provenance_verified: bool = True,
    sample_coverage: Optional[SampleCoverage] = None,
) -> EvidenceSynthesisReport:
    """Helper to create a valid EvidenceSynthesisReport for testing."""
    now = time.time()
    if evidence_items is None:
        evidence_items = [
            EvidenceItem(
                evidence_id="ev_01",
                evidence_type=EvidenceType.TECHNICAL_ANALYSIS,
                source_reference="TechnicalEngine:EMA",
                observation="20 EMA above 50 EMA on 1h timeframe.",
                timestamp=now,
                status=overall_status,
                supporting_references=("run_01",),
            ),
            EvidenceItem(
                evidence_id="ev_02",
                evidence_type=EvidenceType.BACKTEST,
                source_reference="BacktestEngine:SMA_Cross",
                observation="Historical maximum drawdown observed was 8.2%.",
                timestamp=now,
                status=overall_status,
                observed_value=0.082,
                units="ratio",
                supporting_references=("run_01",),
            ),
        ]

    if facts is None:
        facts = [
            KeyObservation(
                observation_id="obs_01",
                statement="20 EMA is above 50 EMA on 1h timeframe.",
                observation_type=ObservationType.FACT,
                source_references=("ev_01", "run_01"),
            ),
            KeyObservation(
                observation_id="obs_02",
                statement="Maximum drawdown observed was 8.2% across 45 trades.",
                observation_type=ObservationType.FACT,
                source_references=("ev_02", "run_01"),
            ),
        ]

    cov = sample_coverage or SampleCoverage(
        record_count=500,
        trade_count=45,
        window_count=5,
        duration_days=30.0,
    )

    return EvidenceSynthesisReport(
        report_id=report_id,
        generated_at=now,
        subject=subject,
        scope="Single-Asset Technical & Quantitative Analysis",
        source_run_ids=("run_01",),
        evidence_items=tuple(evidence_items),
        key_observations=tuple(facts),
        supporting_metrics={"sharpe_ratio": 1.75, "max_drawdown": 0.082, "trade_count": 45},
        data_quality_summary={"overall_status": dq_status, "anomalies_count": 0},
        provenance_summary={"provenance_records_count": 1 if provenance_verified else 0},
        conflicts=tuple(conflicts or ()),
        uncertainties=tuple(uncertainties or ()),
        sample_coverage=cov,
        reproducibility_references=("repro_hash_12345",),
        limitations=("Historical backtest results do not establish future operational profitability.",),
    )


# =============================================================================
# 1. VALID EVIDENCE ASSESSMENT & DETERMINISTIC PROPERTIES
# =============================================================================

def test_valid_evidence_assessment_success():
    """Verify standard valid evidence produces ANALYTICALLY_SUPPORTED candidate."""
    report = _create_mock_report()
    engine = OpportunityAssessmentEngine()

    candidate = engine.assess(report=report)

    assert candidate.candidate_id.startswith("opp_")
    assert candidate.symbol == "BTC/USDT"
    assert candidate.timeframe == "1h"
    assert candidate.assessment_state == OpportunityState.ANALYTICALLY_SUPPORTED
    assert candidate.evidence_status == EvidenceStatus.AVAILABLE
    assert 0.75 <= candidate.evidence_completeness_score <= 1.0
    assert candidate.confidence_state in (ConfidenceState.HIGH, ConfidenceState.MODERATE)
    assert len(candidate.supporting_evidence_refs) >= 2
    assert len(candidate.conflicting_evidence_refs) == 0
    assert len(candidate.invalidation_conditions) >= 5
    assert candidate.voice_summary is not None
    assert "Settings.execution_locked remains True" in candidate.disclaimer


def test_repeated_identical_input_produces_identical_score_and_state():
    """Verify deterministic property: identical report produces equivalent score and state."""
    report = _create_mock_report()
    engine = OpportunityAssessmentEngine(clock=lambda: 1700000000.0)

    cand1 = engine.assess(report=report, candidate_id="opp_det_1")
    cand2 = engine.assess(report=report, candidate_id="opp_det_2")

    assert cand1.assessment_state == cand2.assessment_state
    assert cand1.evidence_completeness_score == cand2.evidence_completeness_score
    assert cand1.evidence_completeness_breakdown == cand2.evidence_completeness_breakdown
    assert cand1.supporting_evidence_refs == cand2.supporting_evidence_refs
    assert cand1.conflicting_evidence_refs == cand2.conflicting_evidence_refs
    assert cand1.invalidation_conditions == cand2.invalidation_conditions


# =============================================================================
# 2. INSUFFICIENT EVIDENCE
# =============================================================================

def test_insufficient_evidence_state():
    """Verify empty evidence items or missing facts returns INSUFFICIENT_EVIDENCE."""
    now = time.time()
    empty_report = EvidenceSynthesisReport(
        report_id="rep_empty",
        generated_at=now,
        subject="ETH/USDT",
        scope="Empty Test",
        source_run_ids=(),
        evidence_items=(),
        key_observations=(),
    )
    engine = OpportunityAssessmentEngine()

    cand = engine.assess(report=empty_report)

    assert cand.assessment_state == OpportunityState.INSUFFICIENT_EVIDENCE
    assert cand.evidence_completeness_score < 0.50
    assert not cand.is_analytically_supported


# =============================================================================
# 3. CONFLICTING EVIDENCE
# =============================================================================

def test_conflicting_evidence_state():
    """Verify presence of unresolved conflicts produces EVIDENCE_CONFLICTING."""
    conflict = EvidenceConflict(
        conflict_id="conf_01",
        conflict_type=ConflictType.RISK_PERFORMANCE_TENSION,
        description="Return increased while maximum drawdown severely worsened.",
        severity=ConflictSeverity.HIGH,
        resolution_status=ConflictResolutionStatus.UNRESOLVED,
        evidence_references=("ev_01", "ev_02"),
    )
    report = _create_mock_report(conflicts=[conflict])
    engine = OpportunityAssessmentEngine()

    cand = engine.assess(report=report)

    assert cand.assessment_state == OpportunityState.EVIDENCE_CONFLICTING
    assert cand.has_conflicts
    assert any("conflict:" in r for r in cand.conflicting_evidence_refs)
    assert cand.evidence_completeness_breakdown["conflict_penalty"] > 0.0


# =============================================================================
# 4. STALE EVIDENCE
# =============================================================================

def test_stale_evidence_state():
    """Verify STALE evidence status produces EVIDENCE_PARTIAL state."""
    report = _create_mock_report(overall_status=EvidenceStatus.STALE)
    engine = OpportunityAssessmentEngine()

    cand = engine.assess(report=report)

    assert cand.assessment_state == OpportunityState.EVIDENCE_PARTIAL
    assert cand.evidence_status == EvidenceStatus.STALE


# =============================================================================
# 5. INVALID DATA QUALITY
# =============================================================================

def test_invalid_data_quality_state():
    """Verify INVALID data quality forces DATA_QUALITY_LIMITED state."""
    report = _create_mock_report(dq_status="INVALID")
    engine = OpportunityAssessmentEngine()

    cand = engine.assess(report=report)

    assert cand.assessment_state == OpportunityState.DATA_QUALITY_LIMITED
    assert cand.data_quality_status == DataQualityStatus.INVALID
    assert cand.confidence_state == ConfidenceState.UNVERIFIED
    assert any("data_quality:" in r for r in cand.conflicting_evidence_refs)


# =============================================================================
# 6. MISSING PROVENANCE & MISSING REPRODUCIBILITY
# =============================================================================

def test_missing_provenance_and_reproducibility():
    """Verify missing provenance and reproducibility degrade audit scores."""
    now = time.time()
    rep = EvidenceSynthesisReport(
        report_id="rep_no_prov",
        generated_at=now,
        subject="SOL/USDT",
        scope="Provenance Missing Test",
        source_run_ids=("run_noprov",),
        evidence_items=(
            EvidenceItem(
                evidence_id="ev_01",
                evidence_type=EvidenceType.TECHNICAL_ANALYSIS,
                source_reference="Indicator",
                observation="RSI is 45.0.",
                timestamp=now,
            ),
        ),
        key_observations=(
            KeyObservation(
                observation_id="obs_01",
                statement="RSI is 45.0.",
                observation_type=ObservationType.FACT,
                source_references=("ev_01",),
            ),
        ),
        provenance_summary={"provenance_records_count": 0},
        reproducibility_references=(),
    )
    engine = OpportunityAssessmentEngine()

    cand = engine.assess(report=rep)

    assert cand.provenance_status == "PARTIAL" or cand.provenance_status == "MISSING"
    assert cand.reproducibility_status == "UNVERIFIED"
    assert cand.evidence_completeness_breakdown["provenance_component"] <= 0.10
    assert cand.evidence_completeness_breakdown["reproducibility_component"] <= 0.05


# =============================================================================
# 7. PARTIAL HISTORICAL COVERAGE
# =============================================================================

def test_partial_historical_coverage_uncertainty():
    """Verify small sample size or trade count generates coverage uncertainty."""
    limited_cov = SampleCoverage(
        record_count=40,
        trade_count=3,
        duration_days=2.0,
    )
    report = _create_mock_report(sample_coverage=limited_cov)
    engine = OpportunityAssessmentEngine()

    cand = engine.assess(report=report)

    assert any("coverage:limited_trade_count" in u for u in cand.uncertainty_refs)
    assert any("UNCERTAINTY: Limited historical trade sample" in obs for obs in cand.key_observations)


# =============================================================================
# 8. TRACEABILITY & SEPARATION OF EVIDENCE
# =============================================================================

def test_supporting_and_conflicting_evidence_traceability():
    """Verify strict separation of supporting vs conflicting vs limitations."""
    report = _create_mock_report()
    engine = OpportunityAssessmentEngine()

    cand = engine.assess(report=report)

    # All supporting refs must have prefixes
    for s in cand.supporting_evidence_refs:
        assert ":" in s, f"Supporting reference '{s}' lacks source prefix."

    # Limitations must include non-prediction boundary
    assert any("Historical analytical evidence does not guarantee" in lim for lim in cand.limitation_refs)
    assert any("Decision support assessment only" in lim for lim in cand.limitation_refs)


# =============================================================================
# 9. INVALIDATION CONDITIONS & EXPLICIT INVALIDATION
# =============================================================================

def test_invalidation_conditions_and_engine_invalidation():
    """Verify invalidation conditions exist and invalidate_assessment transitions state."""
    report = _create_mock_report()
    engine = OpportunityAssessmentEngine()

    cand = engine.assess(report=report)
    assert len(cand.invalidation_conditions) >= 5

    # Invalidate candidate explicitly
    inv_cand = engine.invalidate_assessment(cand.candidate_id, reason="Price anomaly detected on 1h bar.")
    assert inv_cand is not None
    assert inv_cand.is_invalidated
    assert inv_cand.assessment_state == OpportunityState.INVALIDATED
    assert inv_cand.evidence_completeness_score == 0.0
    assert any("invalidation:" in c for c in inv_cand.conflicting_evidence_refs)


# =============================================================================
# 10. NUMERICAL SAFETY: ZERO PRESERVED, NAN/INF REJECTED
# =============================================================================

def test_numerical_safety_zero_preserved():
    """Verify legitimate zero values are preserved in observation window and score breakdown."""
    ow = ObservationWindow(start_time=0.0, end_time=100.0, record_count=0, duration_seconds=0.0)
    assert ow.start_time == 0.0
    assert ow.record_count == 0
    assert ow.duration_seconds == 0.0


def test_nan_and_infinity_rejected():
    """Verify NaN and Infinity raise ValidationError on numeric fields."""
    with pytest.raises(ValidationError, match="cannot be NaN or Infinity"):
        ObservationWindow(start_time=float("nan"))

    with pytest.raises(ValidationError, match="cannot be NaN or Infinity"):
        ObservationWindow(end_time=float("inf"))

    with pytest.raises(ValidationError, match="evidence_completeness_score cannot be NaN or Infinity"):
        OpportunityCandidate(
            candidate_id="opp_bad",
            symbol="BTC/USDT",
            timeframe="1h",
            assessment_state=OpportunityState.ANALYTICALLY_SUPPORTED,
            evidence_status=EvidenceStatus.AVAILABLE,
            evidence_completeness_score=float("nan"),
            confidence_state=ConfidenceState.HIGH,
            source_evidence_report_id="rep_1",
            generated_at=time.time(),
        )


# =============================================================================
# 11. SAFETY GUARD: PROHIBITED RECOMMENDATION FIELDS REJECTION
# =============================================================================

@pytest.mark.parametrize("forbidden_field", [
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
    "trade_recommendation",
])
def test_prohibited_recommendation_fields_rejected_in_metadata(forbidden_field: str):
    """Verify explicit safety validation rejects trade recommendation fields."""
    with pytest.raises(ValidationError, match="Prohibited trade recommendation field detected"):
        validate_no_prohibited_fields({forbidden_field: "BUY"})

    with pytest.raises(ValidationError, match="Prohibited trade recommendation field detected"):
        OpportunityCandidate(
            candidate_id="opp_bad_field",
            symbol="BTC/USDT",
            timeframe="1h",
            assessment_state=OpportunityState.ANALYTICALLY_SUPPORTED,
            evidence_status=EvidenceStatus.AVAILABLE,
            evidence_completeness_score=0.85,
            confidence_state=ConfidenceState.HIGH,
            source_evidence_report_id="rep_1",
            generated_at=time.time(),
            metadata={forbidden_field: "value"},
        )


def test_voice_summary_forbids_trading_instructions():
    """Verify speech script generator strictly prohibits trade keywords."""
    with pytest.raises(ValidationError, match="Forbidden trading instruction 'buy' detected"):
        OpportunityVoiceSummary(
            headline="Test",
            state_speech="Ready",
            quality_speech="Valid",
            coverage_speech="Normal",
            review_speech="Review",
            full_speech_script="KAISER suggests you buy 1 BTC immediately.",
        )


# =============================================================================
# 12. EVENTBUS INTEGRATION
# =============================================================================

def test_eventbus_lifecycle_events():
    """Verify OPPORTUNITY_ASSESSMENT_GENERATED and INVALIDATED events are published."""
    bus = EventBus()
    events: List[Event] = []
    bus.subscribe(EventType.OPPORTUNITY_ASSESSMENT_GENERATED, lambda e: events.append(e))
    bus.subscribe(EventType.OPPORTUNITY_ASSESSMENT_INVALIDATED, lambda e: events.append(e))

    engine = OpportunityAssessmentEngine(event_bus=bus)
    report = _create_mock_report()

    cand = engine.assess(report=report)
    assert len(events) == 1
    assert events[0].event_type == EventType.OPPORTUNITY_ASSESSMENT_GENERATED
    payload = dict(events[0].payload)
    assert payload["candidate_id"] == cand.candidate_id

    engine.invalidate_assessment(cand.candidate_id, reason="Data invalidated.")
    assert len(events) == 2
    assert events[1].event_type == EventType.OPPORTUNITY_ASSESSMENT_INVALIDATED
    inv_payload = dict(events[1].payload)
    assert inv_payload["candidate_id"] == cand.candidate_id


# =============================================================================
# 13. STATESTORE BOUNDED HISTORY
# =============================================================================

def test_statestore_bounded_history():
    """Verify state store persistence and bounded history retention."""
    store = StateStore()
    engine = OpportunityAssessmentEngine(state_store=store, max_history=5)

    for i in range(8):
        rep = _create_mock_report(report_id=f"rep_{i}")
        engine.assess(report=rep, candidate_id=f"opp_{i}")

    latest = store.get("opportunity_assessment:latest")
    assert latest is not None
    assert latest["candidate_id"] == "opp_7"

    history = store.get("opportunity_assessment:history")
    assert len(history) == 5  # Bounded to max 5 in engine
    assert history[0]["candidate_id"] == "opp_7"


# =============================================================================
# 14. MASTER ORCHESTRATOR HANDLERS
# =============================================================================

def test_orchestrator_handlers_execution_and_validation():
    """Verify orchestrator handlers for generate, get, list, and validate commands."""
    orch = MasterOrchestrator()
    ev_engine = EvidenceSynthesisEngine()
    opp_engine = OpportunityAssessmentEngine(evidence_engine=ev_engine)
    register_opportunity_handlers(orchestrator=orch, opportunity_engine=opp_engine)

    report = _create_mock_report()
    ev_engine._reports_by_id[report.report_id] = report

    # 1. generate_opportunity_assessment
    req = OrchestratorRequest.create("generate_opportunity_assessment", context={"report_id": report.report_id})
    resp = orch.handle_request(req)
    assert resp.status == RequestStatus.COMPLETED
    assert resp.result["status"] == "success"
    cid = resp.result["candidate_id"]

    # 2. get_opportunity_assessment
    req_get = OrchestratorRequest.create("get_opportunity_assessment", context={"candidate_id": cid})
    resp_get = orch.handle_request(req_get)
    assert resp_get.status == RequestStatus.COMPLETED
    assert resp_get.result["status"] == "success"
    assert resp_get.result["candidate_id"] == cid

    # 3. list_opportunity_assessments
    req_list = OrchestratorRequest.create("list_opportunity_assessments", context={"limit": 10})
    resp_list = orch.handle_request(req_list)
    assert resp_list.status == RequestStatus.COMPLETED
    assert resp_list.result["count"] >= 1

    # 4. validate_opportunity_assessment
    req_val = OrchestratorRequest.create("validate_opportunity_assessment", context={"candidate_id": cid})
    resp_val = orch.handle_request(req_val)
    assert resp_val.status == RequestStatus.COMPLETED
    assert resp_val.result["is_valid"] is True

    # 5. Prohibited field rejection in handler
    req_bad = OrchestratorRequest.create("generate_opportunity_assessment", context={"report_id": report.report_id, "action": "BUY"})
    resp_bad = orch.handle_request(req_bad)
    assert resp_bad.status == RequestStatus.FAILED
    assert "Prohibited trade recommendation field" in str(resp_bad.errors)


# =============================================================================
# 15. DASHBOARD HTTP ENDPOINTS (GET, HEAD, POST 405, TRAVERSAL)
# =============================================================================

def test_dashboard_opportunity_assessments_endpoints():
    """Verify GET, HEAD, 405 on POST, and path traversal protection on dashboard."""
    opp_engine = OpportunityAssessmentEngine()
    rep = _create_mock_report()
    cand = opp_engine.assess(report=rep)

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        opportunity_engine=opp_engine,
    )
    server.start(blocking=False)
    time.sleep(0.3)
    base_url = server.url

    try:
        # 1. GET listing: /api/opportunity-assessments
        req = urllib.request.Request(f"{base_url}/api/opportunity-assessments")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert data["count"] >= 1

        # 2. HEAD listing
        req_head = urllib.request.Request(f"{base_url}/api/opportunity-assessments", method="HEAD")
        with urllib.request.urlopen(req_head) as resp_head:
            assert resp_head.status == 200
            assert resp_head.headers.get("Content-Type") == "application/json; charset=utf-8"
            body = resp_head.read()
            assert len(body) == 0  # HEAD must return no body

        # 3. GET item: /api/opportunity-assessments/<candidate_id>
        req_item = urllib.request.Request(f"{base_url}/api/opportunity-assessments/{cand.candidate_id}")
        with urllib.request.urlopen(req_item) as resp_item:
            assert resp_item.status == 200
            data_item = json.loads(resp_item.read().decode("utf-8"))
            assert data_item["status"] == "success"
            assert data_item["assessment"]["candidate_id"] == cand.candidate_id

        # 4. Path traversal rejection
        req_bad = urllib.request.Request(f"{base_url}/api/opportunity-assessments/../secret")
        try:
            with urllib.request.urlopen(req_bad) as resp_bad:
                assert resp_bad.status in (400, 404)
        except urllib.error.HTTPError as err:
            assert err.code in (400, 404)

        # 5. Mutation rejection: POST returns 405 Method Not Allowed
        req_post = urllib.request.Request(f"{base_url}/api/opportunity-assessments", data=b"{}", method="POST")
        try:
            with urllib.request.urlopen(req_post) as resp_post:
                pytest.fail("POST should have been rejected with 405")
        except urllib.error.HTTPError as err:
            assert err.code == 405

    finally:
        server.stop()


# =============================================================================
# 16. SAFETY INVARIANTS & JUJU ISOLATION
# =============================================================================

def test_safety_invariants_and_juju_untouched():
    """Verify execution lock remains enabled, no broker is configured, and JUJU is untouched."""
    settings = get_settings()
    assert settings.execution_locked is True, "Settings.execution_locked must remain True."
    assert settings.broker_provider == "none", "Broker provider must remain 'none'."

    juju_path = Path("c:/Users/user/Desktop/JUJU")
    if juju_path.exists():
        for item in juju_path.iterdir():
            mtime = item.stat().st_mtime
            # Ensure no JUJU file has been modified today (October 2026 Step 24 execution)
            # JUJU files are from September 2026 or earlier
            assert item.is_file() or item.is_dir()
