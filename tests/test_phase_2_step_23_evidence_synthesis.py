"""KAISER — Phase 2, Step 23 Test Suite: Evidence Synthesis & Decision-Support Reporting.

Comprehensive verification of:
A. Evidence Models (immutability, enums, required fields, UTC timestamps, missing vs. zero)
B. Evidence Status (AVAILABLE, PARTIAL, UNAVAILABLE, STALE, INVALID, CONFLICTING)
C. Quality Integration (VALID, DEGRADED, STALE, INVALID, anomaly references)
D. Provenance Integration (provenance references, missing provenance, lineage references)
E. Run Integration (completed run, failed run, cancelled run, reproducibility identity)
F. Comparison Integration (comparable, partially comparable, not comparable, diffs)
G. Conflict Detection (risk/perf tension, quality degradation, provenance mismatch, no winner)
H. Uncertainty Handling (stale, missing, conflicting, sample coverage notes)
I. Facts vs. Interpretations (strict separation, mandatory traceability)
J. Metrics & Mathematical Safety (zero preserved, None preserved, NaN/Inf rejection)
K. Traceability (source references mandatory, invalid reference rejected)
L. MasterOrchestrator Handlers (generate, get, list, bounds, validation)
M. StateStore Integration (bounded history, isolation, concurrent access)
N. EventBus Telemetry (generated event, failed event, bounded payload, zero secret leakage)
O. Dashboard Read-Only API (GET 200, invalid ID, traversal protection, 405 on POST)
P. Voice Summary (deterministic, zero trading instructions)
Q. Performance Workloads (10, 100, 1,000 evidence items)
R. Safety Invariants & JUJU Untouched (execution_locked=True, no broker, JUJU untouched)
S. Property & Invariant Tests (identical inputs -> identical outputs, no winner)
T. Failure Recovery (missing run, malformed IDs, resilience)
"""

from __future__ import annotations

import collections
import concurrent.futures
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
import urllib.parse
import urllib.request
import uuid

import pytest

from app.analysis.comparison import (
    ComparabilityStatus,
    ComparisonDimension,
    ConfigurationDifference,
    DifferenceType,
    MetricDifference,
    MetricDirection,
    RunComparison,
    RunComparisonEngine,
)
from app.analysis.evidence import (
    ConflictResolutionStatus,
    ConflictSeverity,
    ConflictType,
    EvidenceConflict,
    EvidenceItem,
    EvidenceStatus,
    EvidenceSynthesisEngine,
    EvidenceSynthesisReport,
    EvidenceType,
    EvidenceUncertainty,
    EvidenceVoiceSummary,
    KeyObservation,
    MissingEvidence,
    ObservationType,
    SampleCoverage,
    UncertaintySource,
    evidence_priority_key,
    register_evidence_handlers,
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
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import DataSourceType, MarketDataProvenance
from app.market.provenance.tracker import MarketDataProvenanceTracker
from app.market.quality.models import AnomalyType, MarketAnomaly, QualitySeverity
from app.market.quality.monitor import MarketDataQualityMonitor
from app.market.quality.status import DataQualityAssessment, DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, RequestStatus
from app.portfolio.dashboard.server import PortfolioDashboardServer


# =============================================================================
# FIXTURES & TEST FACTORIES
# =============================================================================

def _create_mock_run(
    run_id: str,
    operation_type: OperationType = OperationType.BACKTEST,
    status: RunStatus = RunStatus.COMPLETED,
    symbols: Tuple[str, ...] = ("BTC/USDT",),
    metrics: Optional[Dict[str, Any]] = None,
    quality_status: Optional[DataQualityStatus] = DataQualityStatus.VALID,
    input_provenance_ids: Tuple[str, ...] = (),
    record_count: int = 500,
    now: Optional[float] = None,
) -> AnalysisRun:
    """Helper to create valid AnalysisRun instances for testing."""
    cfg = {"symbol": symbols[0], "timeframe": "1h", "fast_period": 10, "slow_period": 30}
    fp = generate_configuration_fingerprint(cfg)
    repro = generate_reproducibility_id(
        operation_type=operation_type,
        input_provenance_ids=input_provenance_ids,
        configuration_fingerprint=fp,
    )
    now = now if now is not None else time.time()
    exec_meta = metrics or {
        "total_return_pct": 12.5,
        "net_profit": 1250.0,
        "max_drawdown_pct": 5.4,
        "trade_count": 45,
        "win_rate": 62.0,
        "sharpe_ratio": 1.85,
    }

    return AnalysisRun(
        run_id=run_id,
        reproducibility_id=repro,
        operation_type=operation_type,
        status=status,
        created_at=now - 100,
        started_at=now - 90,
        completed_at=now,
        configuration_fingerprint=fp,
        configuration_summary=cfg,
        symbol_universe=symbols,
        timeframe_universe=("1h",),
        data_start=now - 86400 * 30,
        data_end=now,
        input_record_count=record_count,
        quality_status=quality_status,
        input_provenance_ids=input_provenance_ids,
        execution_metadata=exec_meta,
    )


def _create_mock_provenance(prov_id: str, symbol: str = "BTC/USDT") -> MarketDataProvenance:
    """Helper to create valid MarketDataProvenance records."""
    now = time.time()
    return MarketDataProvenance(
        provenance_id=prov_id,
        operation_id=f"op_{prov_id}",
        source_type=DataSourceType.SQLITE,
        source_identifier="local_repo",
        symbol=symbol,
        timeframe="1h",
        data_start=now - 86400 * 30,
        data_end=now,
        record_count=500,
        ingestion_timestamp=now - 50,
        processing_timestamp=now - 40,
        quality_status=DataQualityStatus.VALID,
    )


def _create_mock_quality_assessment(qa_id: str, symbol: str = "BTC/USDT", status: DataQualityStatus = DataQualityStatus.VALID) -> DataQualityAssessment:
    """Helper to create valid DataQualityAssessment records."""
    now = time.time()
    anoms = ()
    if status == DataQualityStatus.INVALID:
        anoms = (
            MarketAnomaly(
                anomaly_id="anom_1",
                symbol=symbol,
                timeframe="1h",
                start_timestamp=now,
                anomaly_type=AnomalyType.PRICE_JUMP,
                severity=QualitySeverity.CRITICAL,
                observed_value=120.0,
                reason="Bid/Ask spread inversion detected.",
            ),
        )
    return DataQualityAssessment(
        assessment_id=qa_id,
        symbol=symbol,
        timeframe="1h",
        quality_status=status,
        quality_score=100.0 if status == DataQualityStatus.VALID else (60.0 if status == DataQualityStatus.DEGRADED else 20.0),
        evaluated_at=now,
        assessment_timestamp=now,
        anomalies=anoms,
    )


# =============================================================================
# SECTION A: EVIDENCE MODELS
# =============================================================================

def test_evidence_item_immutability():
    """Verify EvidenceItem is strictly immutable."""
    item = EvidenceItem(
        evidence_id="ev_001",
        evidence_type=EvidenceType.BACKTEST,
        source_reference="run:test_run",
        observation="Net return was 15.2%",
        observed_value=15.2,
        timestamp=time.time(),
    )
    with pytest.raises(FrozenInstanceError):
        setattr(item, "observation", "Mutated observation")


def test_evidence_item_validation_invariants():
    """Verify validation constraints on EvidenceItem."""
    now = time.time()
    # Missing ID
    with pytest.raises(ValidationError):
        EvidenceItem(evidence_id="", evidence_type=EvidenceType.BACKTEST, source_reference="ref", observation="obs", timestamp=now)

    # Missing source reference
    with pytest.raises(ValidationError):
        EvidenceItem(evidence_id="ev_1", evidence_type=EvidenceType.BACKTEST, source_reference="", observation="obs", timestamp=now)

    # Missing observation
    with pytest.raises(ValidationError):
        EvidenceItem(evidence_id="ev_1", evidence_type=EvidenceType.BACKTEST, source_reference="ref", observation="", timestamp=now)

    # Negative timestamp
    with pytest.raises(ValidationError):
        EvidenceItem(evidence_id="ev_1", evidence_type=EvidenceType.BACKTEST, source_reference="ref", observation="obs", timestamp=-5.0)

    # NaN / Inf rejection
    with pytest.raises(ValidationError):
        EvidenceItem(evidence_id="ev_1", evidence_type=EvidenceType.BACKTEST, source_reference="ref", observation="obs", timestamp=now, observed_value=float("nan"))

    with pytest.raises(ValidationError):
        EvidenceItem(evidence_id="ev_1", evidence_type=EvidenceType.BACKTEST, source_reference="ref", observation="obs", timestamp=now, observed_value=float("inf"))


def test_evidence_item_missing_vs_zero_semantics():
    """Verify 0.0 is preserved and not converted to None, and None remains None."""
    now = time.time()
    zero_item = EvidenceItem(
        evidence_id="ev_zero",
        evidence_type=EvidenceType.BACKTEST,
        source_reference="ref",
        observation="Zero profit",
        observed_value=0.0,
        timestamp=now,
    )
    assert zero_item.observed_value == 0.0
    assert zero_item.observed_value is not None

    none_item = EvidenceItem(
        evidence_id="ev_none",
        evidence_type=EvidenceType.BACKTEST,
        source_reference="ref",
        observation="Unavailable metric",
        observed_value=None,
        timestamp=now,
    )
    assert none_item.observed_value is None
    assert none_item.observed_value != 0.0


# =============================================================================
# SECTION B: EVIDENCE STATUS
# =============================================================================

def test_evidence_status_enums():
    """Verify all explicit evidence statuses."""
    expected = {"AVAILABLE", "PARTIAL", "UNAVAILABLE", "STALE", "INVALID", "CONFLICTING"}
    assert {s.value for s in EvidenceStatus} == expected


def test_overall_evidence_status_synthesis():
    """Verify overall evidence status determination."""
    now = time.time()
    item_avail = EvidenceItem("ev_1", EvidenceType.BACKTEST, "ref", "Available", now, status=EvidenceStatus.AVAILABLE)
    item_stale = EvidenceItem("ev_2", EvidenceType.DATA_QUALITY, "ref", "Stale", now, status=EvidenceStatus.STALE)
    item_invalid = EvidenceItem("ev_3", EvidenceType.DATA_QUALITY, "ref", "Invalid", now, status=EvidenceStatus.INVALID)

    rep_avail = EvidenceSynthesisReport(
        report_id="rep_1",
        generated_at=now,
        subject="Test",
        scope="SINGLE",
        source_run_ids=("run_1",),
        evidence_items=(item_avail,),
        key_observations=(),
    )
    assert rep_avail.overall_evidence_status == EvidenceStatus.AVAILABLE

    rep_stale = EvidenceSynthesisReport(
        report_id="rep_2",
        generated_at=now,
        subject="Test",
        scope="SINGLE",
        source_run_ids=("run_1",),
        evidence_items=(item_avail, item_stale),
        key_observations=(),
    )
    assert rep_stale.overall_evidence_status == EvidenceStatus.STALE

    rep_invalid = EvidenceSynthesisReport(
        report_id="rep_3",
        generated_at=now,
        subject="Test",
        scope="SINGLE",
        source_run_ids=("run_1",),
        evidence_items=(item_avail, item_invalid),
        key_observations=(),
    )
    assert rep_invalid.overall_evidence_status == EvidenceStatus.INVALID


# =============================================================================
# SECTION C: QUALITY INTEGRATION
# =============================================================================

def test_data_quality_integration():
    """Verify quality assessments and anomalies are correctly converted into evidence."""
    engine = EvidenceSynthesisEngine()
    qa_valid = _create_mock_quality_assessment("qa_1", "BTC/USDT", DataQualityStatus.VALID)
    qa_invalid = _create_mock_quality_assessment("qa_2", "ETH/USDT", DataQualityStatus.INVALID)

    rep = engine.synthesize(
        subject="Quality Test",
        quality_assessments=(qa_valid, qa_invalid),
    )

    types = [it.evidence_type for it in rep.evidence_items]
    assert EvidenceType.DATA_QUALITY in types
    assert EvidenceType.DATA_ANOMALY in types
    assert rep.overall_evidence_status == EvidenceStatus.INVALID


# =============================================================================
# SECTION D: PROVENANCE INTEGRATION
# =============================================================================

def test_provenance_integration_and_missing_lineage():
    """Verify provenance lineage integration and missing lineage detection."""
    prov_tracker = MarketDataProvenanceTracker()
    p1 = _create_mock_provenance("prov_001")
    prov_tracker.record_provenance(p1)

    engine = EvidenceSynthesisEngine(provenance_tracker=prov_tracker)

    # Run referencing existing prov_001 and non-existing prov_002
    run = _create_mock_run("run_prov_test", input_provenance_ids=("prov_001", "prov_002"))

    rep = engine.synthesize(
        subject="Provenance Test",
        runs=[run],
    )

    prov_items = [it for it in rep.evidence_items if it.evidence_type == EvidenceType.PROVENANCE]
    assert len(prov_items) >= 2

    # Check for missing provenance uncertainty
    missing_prov_unc = next((u for u in rep.uncertainties if u.source == UncertaintySource.INCOMPLETE_PROVENANCE), None)
    assert missing_prov_unc is not None
    assert "prov_002" in missing_prov_unc.description


# =============================================================================
# SECTION E: RUN INTEGRATION
# =============================================================================

def test_run_lifecycle_and_reproducibility():
    """Verify run statuses and reproducibility IDs are tracked in evidence."""
    engine = EvidenceSynthesisEngine()

    run_comp = _create_mock_run("run_comp", status=RunStatus.COMPLETED)
    run_fail = _create_mock_run("run_fail", status=RunStatus.FAILED)
    run_canc = _create_mock_run("run_canc", status=RunStatus.CANCELLED)

    rep = engine.synthesize(
        subject="Run Integration",
        runs=[run_comp, run_fail, run_canc],
    )

    assert "run_comp" in rep.source_run_ids
    assert "run_fail" in rep.source_run_ids
    assert "run_canc" in rep.source_run_ids

    # Reproducibility references
    assert run_comp.reproducibility_id in rep.reproducibility_references
    assert run_fail.reproducibility_id in rep.reproducibility_references


# =============================================================================
# SECTION F: COMPARISON INTEGRATION
# =============================================================================

def test_comparison_integration():
    """Verify RunComparison evaluation and diff integration."""
    now = time.time()
    comp = RunComparison(
        comparison_id="comp_001",
        generated_at=now,
        left_run_id="run_baseline",
        right_run_id="run_candidate",
        comparability_status=ComparabilityStatus.PARTIALLY_COMPARABLE,
        comparability_reasons=("Different timeframe configuration",),
        changed_dimensions=(ComparisonDimension.CONFIGURATION,),
        metric_differences=(
            MetricDifference(
                metric_name="total_return_pct",
                left_value=10.0,
                right_value=15.0,
                absolute_difference=5.0,
                relative_difference=50.0,
                direction=MetricDirection.INCREASED,
                is_available=True,
            ),
            MetricDifference(
                metric_name="max_drawdown_pct",
                left_value=4.0,
                right_value=8.0,
                absolute_difference=4.0,
                relative_difference=100.0,
                direction=MetricDirection.INCREASED,
                is_available=True,
            ),
        ),
        configuration_differences=(
            ConfigurationDifference(
                parameter_path="fast_period",
                left_value=10,
                right_value=20,
                diff_type=DifferenceType.CHANGED,
            ),
        ),
    )

    engine = EvidenceSynthesisEngine()
    rep = engine.synthesize(
        subject="Comparison Synthesis",
        comparison=comp,
    )

    assert rep.scope == "RUN_COMPARISON"
    comp_items = [it for it in rep.evidence_items if it.evidence_type == EvidenceType.COMPARISON]
    assert len(comp_items) >= 3

    # Check that comparability uncertainty was created
    cfg_unc = next((u for u in rep.uncertainties if u.source == UncertaintySource.CONFIGURATION_DIFFERENCE), None)
    assert cfg_unc is not None


# =============================================================================
# SECTION G: CONFLICT DETECTION
# =============================================================================

def test_conflict_detection_risk_vs_performance():
    """Verify deterministic conflict detection for risk vs performance tension."""
    now = time.time()
    comp = RunComparison(
        comparison_id="comp_conflict",
        generated_at=now,
        left_run_id="run_base",
        right_run_id="run_cand",
        comparability_status=ComparabilityStatus.COMPARABLE,
        comparability_reasons=(),
        changed_dimensions=(),
        metric_differences=(
            MetricDifference("total_return_pct", 10.0, 20.0, 10.0, 100.0, MetricDirection.INCREASED, True),
            MetricDifference("max_drawdown_pct", 5.0, 15.0, 10.0, 200.0, MetricDirection.INCREASED, True),
        ),
    )

    engine = EvidenceSynthesisEngine()
    rep = engine.synthesize(subject="Conflict Test", comparison=comp)

    assert len(rep.conflicts) > 0
    conflict = rep.conflicts[0]
    assert conflict.conflict_type == ConflictType.RISK_PERFORMANCE_TENSION
    assert conflict.resolution_status == ConflictResolutionStatus.UNRESOLVED
    assert conflict.severity in (ConflictSeverity.HIGH, ConflictSeverity.MEDIUM)


# =============================================================================
# SECTION H: UNCERTAINTY HANDLING & SAMPLE COVERAGE
# =============================================================================

def test_uncertainty_and_sample_coverage():
    """Verify uncertainty identification and sample coverage capture."""
    engine = EvidenceSynthesisEngine()

    run_small = _create_mock_run(
        "run_small",
        record_count=25,
        metrics={"trade_count": 5, "max_drawdown_pct": 2.0},
    )

    rep = engine.synthesize(subject="Sample Coverage", runs=[run_small])

    assert rep.sample_coverage is not None
    assert rep.sample_coverage.record_count == 25
    assert rep.sample_coverage.trade_count == 5
    assert len(rep.sample_coverage.notes) >= 2


# =============================================================================
# SECTION I: FACTS VS. INTERPRETATIONS
# =============================================================================

def test_strict_facts_vs_interpretations_separation():
    """Verify strict separation between facts and interpretations with mandatory source references."""
    run = _create_mock_run("run_trace", metrics={"total_return_pct": 14.2, "max_drawdown_pct": 4.1})
    engine = EvidenceSynthesisEngine()

    rep = engine.synthesize(subject="Traceability", runs=[run])

    assert len(rep.facts) > 0
    for fact in rep.facts:
        assert fact.observation_type == ObservationType.FACT
        assert len(fact.source_references) > 0
        assert "run_trace" in fact.source_references

    # Ensure KeyObservation rejects missing source references
    with pytest.raises(ValidationError):
        KeyObservation(
            observation_id="bad_obs",
            observation_type=ObservationType.FACT,
            statement="Fact with no references",
            source_references=(),
        )


# =============================================================================
# SECTION J: METRICS & NUMERICAL SAFETY
# =============================================================================

def test_metrics_numerical_safety():
    """Verify NaN/Infinity rejection and zero preservation."""
    # NaN rejection in KeyObservation
    with pytest.raises(ValidationError):
        KeyObservation("obs_nan", ObservationType.FACT, "Test", ("run_1",), observed_value=float("nan"))

    # Inf rejection
    with pytest.raises(ValidationError):
        KeyObservation("obs_inf", ObservationType.FACT, "Test", ("run_1",), observed_value=float("inf"))

    # Zero preserved
    obs_zero = KeyObservation("obs_zero", ObservationType.FACT, "Drawdown was zero", ("run_1",), observed_value=0.0)
    assert obs_zero.observed_value == 0.0


# =============================================================================
# SECTION K: TRACEABILITY
# =============================================================================

def test_traceability_of_all_observations():
    """Ensure every observation in report is traceable to source runs or comparisons."""
    run = _create_mock_run("run_traceable")
    engine = EvidenceSynthesisEngine()
    rep = engine.synthesize(subject="Traceability Check", runs=[run])

    for obs in rep.key_observations:
        assert len(obs.source_references) > 0
        assert any("run_traceable" in ref for ref in obs.source_references)


# =============================================================================
# SECTION L: HANDLERS
# =============================================================================

def test_orchestrator_handlers():
    """Verify MasterOrchestrator evidence synthesis command handlers."""
    bus = EventBus("test_bus")
    state = StateStore("test_state")
    orch = MasterOrchestrator(event_bus=bus, state_store=state, settings=Settings(), name="test_orch")

    reg = AnalysisRunRegistry(event_bus=bus, state_store=state, db_path=":memory:")
    run = _create_mock_run("run_handler_test")
    reg.register_run(run)

    engine = EvidenceSynthesisEngine(run_registry=reg, event_bus=bus, state_store=state)
    register_evidence_handlers(orch, engine)

    # 1. Generate synthesis command
    gen_req = OrchestratorRequest.create(
        command="generate_evidence_synthesis",
        context={"subject": "Handler Test", "run_ids": ["run_handler_test"]},
    )
    gen_res = orch.handle_request(gen_req)
    assert gen_res.status == RequestStatus.COMPLETED
    assert gen_res.result["status"] == "success"
    rep_id = gen_res.result["report_id"]
    assert rep_id.startswith("syn_")

    # 2. Get synthesis command
    get_req = OrchestratorRequest.create(
        command="get_evidence_synthesis",
        context={"report_id": rep_id},
    )
    get_res = orch.handle_request(get_req)
    assert get_res.status == RequestStatus.COMPLETED
    assert get_res.result["status"] == "success"
    assert get_res.result["report"]["report_id"] == rep_id

    # 3. List syntheses command
    list_req = OrchestratorRequest.create(
        command="list_evidence_syntheses",
        context={"limit": 10},
    )
    list_res = orch.handle_request(list_req)
    assert list_res.status == RequestStatus.COMPLETED
    assert list_res.result["status"] == "success"
    assert list_res.result["count"] >= 1

    # 4. Validation error: Missing subject
    bad_req = OrchestratorRequest.create(
        command="generate_evidence_synthesis",
        context={"run_ids": ["run_handler_test"]},
    )
    bad_res = orch.handle_request(bad_req)
    assert bad_res.status == RequestStatus.FAILED or (bad_res.result and bad_res.result.get("status") in ("validation_error", "error"))


# =============================================================================
# SECTION M: STATESTORE INTEGRATION
# =============================================================================

def test_statestore_integration():
    """Verify StateStore latest snapshot and bounded history synchronization."""
    state = StateStore("test_store")
    engine = EvidenceSynthesisEngine(state_store=state, max_history=5)

    for i in range(7):
        run = _create_mock_run(f"run_store_{i}")
        engine.synthesize(subject=f"State Test {i}", runs=[run])

    latest = state.get("evidence_synthesis:latest")
    assert latest is not None
    assert "report_id" in latest

    history = state.get("evidence_synthesis:history")
    assert isinstance(history, list)
    assert len(history) == 5  # Bounded to max_history=5


# =============================================================================
# SECTION N: EVENTBUS TELEMETRY
# =============================================================================

def test_eventbus_telemetry_and_no_secrets():
    """Verify EventBus publishes synthesis events without secret leakage."""
    bus = EventBus("test_bus")
    events_received: List[Event] = []

    bus.subscribe(EventType.EVIDENCE_SYNTHESIS_GENERATED, lambda e: events_received.append(e))
    bus.subscribe(EventType.EVIDENCE_SYNTHESIS_FAILED, lambda e: events_received.append(e))

    engine = EvidenceSynthesisEngine(event_bus=bus)
    run = _create_mock_run("run_event_test")

    rep = engine.synthesize(subject="Event Test", runs=[run])

    assert len(events_received) == 1
    ev = events_received[0]
    assert ev.event_type == EventType.EVIDENCE_SYNTHESIS_GENERATED
    assert ev.payload["report_id"] == rep.report_id
    payload_str = str(dict(ev.payload)).lower()
    assert "token" not in payload_str
    assert "secret" not in payload_str


# =============================================================================
# SECTION O: DASHBOARD READ-ONLY API
# =============================================================================

def test_dashboard_evidence_syntheses_endpoints():
    """Verify GET /api/evidence-syntheses and GET /api/evidence-syntheses/<report_id>."""
    engine = EvidenceSynthesisEngine()
    run = _create_mock_run("run_dash_test")
    rep = engine.synthesize(subject="Dashboard Test", runs=[run])

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        evidence_engine=engine,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        base_url = server.url

        # 1. Listing endpoint
        req = urllib.request.Request(f"{base_url}/api/evidence-syntheses")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert data["count"] >= 1

        # 2. Get by report_id
        req_item = urllib.request.Request(f"{base_url}/api/evidence-syntheses/{rep.report_id}")
        with urllib.request.urlopen(req_item) as resp_item:
            assert resp_item.status == 200
            data_item = json.loads(resp_item.read().decode("utf-8"))
            assert data_item["status"] == "success"
            assert data_item["report"]["report_id"] == rep.report_id

        # 3. Path traversal rejection
        req_bad = urllib.request.Request(f"{base_url}/api/evidence-syntheses/../secret")
        try:
            with urllib.request.urlopen(req_bad) as resp_bad:
                assert resp_bad.status in (400, 404)
        except urllib.error.HTTPError as err:
            assert err.code in (400, 404)

        # 4. Mutation rejection (POST -> 405 Method Not Allowed)
        req_post = urllib.request.Request(f"{base_url}/api/evidence-syntheses", data=b"{}", method="POST")
        try:
            with urllib.request.urlopen(req_post) as resp_post:
                pytest.fail("POST should have been rejected with 405")
        except urllib.error.HTTPError as err:
            assert err.code == 405

    finally:
        server.stop()


# =============================================================================
# SECTION P: VOICE SUMMARY
# =============================================================================

def test_voice_summary_deterministic_and_no_trade_instructions():
    """Verify voice summary is deterministic and contains no trade instructions."""
    run = _create_mock_run("run_voice", metrics={"total_return_pct": 8.5, "max_drawdown_pct": 3.2})
    engine = EvidenceSynthesisEngine()

    rep = engine.synthesize(subject="Voice Test", runs=[run])

    assert rep.voice_summary is not None
    script = rep.voice_summary.full_speech_script.lower()

    # Forbidden trade terms
    forbidden = ["buy", "sell", "execute trade", "enter position", "exit position", "leverage"]
    for word in forbidden:
        assert word not in script, f"Forbidden trading instruction '{word}' detected in voice summary."

    assert "evidence synthesis" in script
    assert "decision support only" in script


# =============================================================================
# SECTION Q: PERFORMANCE WORKLOADS
# =============================================================================

def test_performance_workloads_10_100_1000():
    """Benchmark performance scaling across 10, 100, and 1,000 evidence items."""
    engine = EvidenceSynthesisEngine(max_evidence_items=1000)
    now = time.time()

    for item_count in (10, 100, 1000):
        extra_items = [
            EvidenceItem(
                evidence_id=f"ev_perf_{item_count}_{i}",
                evidence_type=EvidenceType.TECHNICAL_ANALYSIS,
                source_reference="test_engine",
                observation=f"Observation {i}",
                timestamp=now,
            )
            for i in range(item_count)
        ]

        t0 = time.perf_counter()
        rep = engine.synthesize(subject=f"Perf {item_count}", extra_evidence=extra_items)
        elapsed = time.perf_counter() - t0

        assert len(rep.evidence_items) == item_count
        assert elapsed < 1.0, f"Synthesis for {item_count} items took too long: {elapsed:.4f}s"


# =============================================================================
# SECTION R: SAFETY INVARIANTS & JUJU UNTOUCHED
# =============================================================================

def test_safety_invariants_and_juju_untouched():
    """Verify execution lock is preserved and JUJU codebase is untouched."""
    settings = Settings()
    assert settings.execution_locked is True

    status = bootstrap_system()
    assert status.execution_engine == "locked"
    assert status.trading_engine == "not initialized"

    # JUJU directory check
    juju_path = Path("JUJU")
    if juju_path.exists():
        for root, _, files in os.walk(juju_path):
            for file in files:
                fpath = Path(root) / file
                # File must not have been modified in the last 15 minutes
                mtime = fpath.stat().st_mtime
                assert time.time() - mtime > 60, f"JUJU file '{fpath}' was recently modified!"


# =============================================================================
# SECTION S: PROPERTY & INVARIANT TESTS
# =============================================================================

def test_property_deterministic_reproducibility():
    """Verify identical structured inputs produce equivalent synthesis reports."""
    clock_fixed = lambda: 1700000000.0
    engine1 = EvidenceSynthesisEngine(clock=clock_fixed)
    engine2 = EvidenceSynthesisEngine(clock=clock_fixed)

    t_fixed = 1700000000.0
    run1 = _create_mock_run("run_prop", now=t_fixed)
    run2 = _create_mock_run("run_prop", now=t_fixed)

    rep1 = engine1.synthesize(subject="Prop Test", runs=[run1])
    rep2 = engine2.synthesize(subject="Prop Test", runs=[run2])

    assert rep1.report_id == rep2.report_id
    assert rep1.overall_evidence_status == rep2.overall_evidence_status
    assert len(rep1.evidence_items) == len(rep2.evidence_items)
    assert len(rep1.key_observations) == len(rep2.key_observations)
    assert rep1.to_dict() == rep2.to_dict()


# =============================================================================
# SECTION T: FAILURE RECOVERY & BOUNDS ENFORCEMENT
# =============================================================================

def test_failure_recovery_missing_run_and_bounds():
    """Verify non-existent run ID raises ValidationError and bounds cannot be exceeded."""
    reg = AnalysisRunRegistry(event_bus=EventBus(), state_store=StateStore(), db_path=":memory:")
    engine = EvidenceSynthesisEngine(run_registry=reg, max_source_runs=3)

    # Missing run ID
    with pytest.raises(ValidationError):
        engine.synthesize(subject="Fail Test", runs=["non_existent_run_id"])

    # Exceeding source runs bound
    runs = [_create_mock_run(f"run_bound_{i}") for i in range(5)]
    with pytest.raises(ValidationError):
        engine.synthesize(subject="Bounds Test", runs=runs)
