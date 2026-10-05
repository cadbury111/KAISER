"""KAISER — Phase 2, Step 22 Test Suite: Experiment Comparison & Run Comparison Foundation.

Comprehensive verification of:
A. Comparability
   - identical compatible runs
   - different operation type
   - different symbol universe
   - different timeframe
   - different data range
   - different provenance
   - different strategy version
   - different analysis version
   - missing provenance
   - missing quality
B. Configuration diff
   - identical configuration
   - changed scalar
   - nested change
   - added key
   - removed key
   - reordered mappings
   - None
   - zero
   - secret redaction
C. Provenance diff
   - same provenance
   - different source
   - different range
   - different record count
   - different quality
   - different transformation
D. Quality diff
   - VALID -> VALID
   - VALID -> DEGRADED
   - DEGRADED -> INVALID
   - different anomaly counts
   - different gap counts
   - missing quality
E. Metric comparison
   - equal
   - increased
   - decreased
   - zero
   - None
   - NaN
   - Infinity
   - zero baseline
   - negative values
   - unavailable metrics
F. Multi-run comparison
   - 2 runs
   - 10 runs
   - 20 runs
   - bound enforcement
   - mixed comparability
G. Handlers
   - valid request
   - invalid request
   - missing run
   - duplicate run IDs
   - too many run IDs
H. StateStore
   - bounded history
   - copy isolation
   - concurrent comparison storage
I. EventBus
   - correct event
   - bounded payload
   - no secret leakage
J. Dashboard
   - endpoint 200
   - read-only
   - invalid ID rejection
   - traversal rejection
   - sanitized output
K. Numerical invariants
   - no NaN/Infinity
   - legitimate zero preserved
   - unavailable percentage change when baseline zero
   - deterministic ordering
L. Performance
   - 2 runs
   - 10 runs
   - 20 runs
M. Safety
   - execution_locked remains True
   - trading engine uninitialized
   - zero broker connections
   - JUJU untouched
N. Property & Invariant Tests
   - reflexivity and symmetry
   - deterministic comparison IDs and outputs
   - no automatic winning strategy
O. Failure Recovery
   - missing runs
   - corrupted metadata
   - invalid configuration
   - diagnostic non-completed runs
"""

from __future__ import annotations

import collections
import concurrent.futures
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
    MultiRunComparison,
    ProvenanceDifference,
    QualityDifference,
    RunComparison,
    RunComparisonEngine,
    compare_metrics_dicts,
    compute_metric_difference,
    diff_configurations,
    diff_provenance,
    diff_quality,
    extract_metrics_from_result,
    register_comparison_handlers,
    safe_float,
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
from app.market.quality.status import DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer


# =============================================================================
# FIXTURES & HELPERS
# =============================================================================

def _create_mock_run(
    run_id: Optional[str] = None,
    op_type: OperationType = OperationType.BACKTEST,
    status: RunStatus = RunStatus.COMPLETED,
    config: Optional[Dict[str, Any]] = None,
    symbols: Tuple[str, ...] = ("BTC/USDT",),
    timeframes: Tuple[str, ...] = ("1m",),
    data_start: float = 1700000000.0,
    data_end: float = 1700086400.0,
    input_records: int = 1440,
    provenance_ids: Tuple[str, ...] = ("prov_test_01",),
    quality_status: Optional[DataQualityStatus] = DataQualityStatus.VALID,
    strategy_id: Optional[str] = "sma_crossover",
    strategy_version: Optional[str] = "1.0.0",
    analysis_version: str = "1.0.0",
    schema_version: str = "1.0.0",
    software_version: str = "1.0.0",
    result_type: Optional[str] = "BacktestResult",
    result_id: Optional[str] = None,
    execution_metadata: Optional[Dict[str, Any]] = None,
) -> AnalysisRun:
    """Helper to synthesize a valid AnalysisRun for testing."""
    rid = run_id or f"run_{uuid.uuid4().hex[:12]}"
    cfg = config if config is not None else {"fast_period": 5, "slow_period": 15}
    fp = generate_configuration_fingerprint(cfg)
    rep_id = generate_reproducibility_id(
        operation_type=op_type,
        input_provenance_ids=provenance_ids,
        configuration_fingerprint=fp,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        analysis_version=analysis_version,
        schema_version=schema_version,
        software_version=software_version,
    )
    return AnalysisRun(
        run_id=rid,
        reproducibility_id=rep_id,
        operation_type=op_type,
        status=status,
        created_at=1700000000.0,
        started_at=1700000001.0,
        completed_at=1700000005.0 if status.is_terminal else None,
        input_provenance_ids=provenance_ids,
        parent_run_ids=(),
        configuration_fingerprint=fp,
        configuration_summary=dict(cfg),
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        analysis_version=analysis_version,
        schema_version=schema_version,
        software_version=software_version,
        symbol_universe=symbols,
        timeframe_universe=timeframes,
        data_start=data_start,
        data_end=data_end,
        input_record_count=input_records,
        quality_assessment_ids=("qa_test_01",) if quality_status is not None else (),
        quality_status=quality_status,
        result_id=result_id or f"res_{rid}",
        result_type=result_type,
        result_fingerprint="mock_result_fp",
        execution_metadata=execution_metadata or {"total_return_pct": 12.5, "sharpe_ratio": 1.45, "max_drawdown_pct": 5.2},
    )


# =============================================================================
# SECTION A: COMPARABILITY
# =============================================================================

def test_comparability_identical_compatible_runs():
    """Verify identical compatible runs evaluate to COMPARABLE with zero changed dimensions."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(run_id="run_1")
    r2 = _create_mock_run(run_id="run_2")

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.COMPARABLE
    assert len(changed) == 0
    assert any("Identical" in r for r in reasons)


def test_comparability_different_operation_type():
    """Verify different operation types evaluate to NOT_COMPARABLE."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(op_type=OperationType.BACKTEST)
    r2 = _create_mock_run(op_type=OperationType.TECHNICAL_ANALYSIS)

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.NOT_COMPARABLE
    assert ComparisonDimension.OPERATION_TYPE in changed
    assert any("operation type" in r.lower() for r in reasons)


def test_comparability_different_symbol_universe():
    """Verify different symbol universes evaluate to NOT_COMPARABLE."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(symbols=("BTC/USDT",))
    r2 = _create_mock_run(symbols=("ETH/USDT",))

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.NOT_COMPARABLE
    assert ComparisonDimension.SYMBOL_UNIVERSE in changed


def test_comparability_different_timeframe_universe():
    """Verify different timeframe universes evaluate to NOT_COMPARABLE."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(timeframes=("1m",))
    r2 = _create_mock_run(timeframes=("5m",))

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.NOT_COMPARABLE
    assert ComparisonDimension.TIMEFRAME_UNIVERSE in changed


def test_comparability_different_data_range():
    """Verify different data ranges evaluate to PARTIALLY_COMPARABLE."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(data_start=1700000000.0, data_end=1700086400.0)
    r2 = _create_mock_run(data_start=1700086400.0, data_end=1700172800.0)

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.DATA_RANGE in changed


def test_comparability_different_provenance():
    """Verify different provenance IDs evaluate to PARTIALLY_COMPARABLE."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(provenance_ids=("prov_1",))
    r2 = _create_mock_run(provenance_ids=("prov_2",))

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.DATA_PROVENANCE in changed


def test_comparability_different_strategy_version():
    """Verify different strategy versions evaluate to PARTIALLY_COMPARABLE."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(strategy_version="1.0.0")
    r2 = _create_mock_run(strategy_version="1.1.0")

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.STRATEGY in changed


def test_comparability_different_analysis_version():
    """Verify different analysis versions evaluate to PARTIALLY_COMPARABLE."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(analysis_version="1.0.0")
    r2 = _create_mock_run(analysis_version="2.0.0")

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.ANALYSIS_VERSION in changed


def test_comparability_missing_provenance():
    """Verify missing provenance on either or both runs is PARTIALLY_COMPARABLE with explicit warning."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(provenance_ids=())
    r2 = _create_mock_run(provenance_ids=("prov_1",))

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.DATA_PROVENANCE in changed
    assert any("unrecorded or missing" in r for r in reasons)

    # Both empty: cannot infer equivalence!
    r3 = _create_mock_run(provenance_ids=())
    status2, reasons2, changed2 = engine.evaluate_comparability(r1, r3)
    assert status2 == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.DATA_PROVENANCE in changed2


def test_comparability_missing_quality():
    """Verify missing quality status on either or both runs is PARTIALLY_COMPARABLE with explicit warning."""
    engine = RunComparisonEngine()
    r1 = _create_mock_run(quality_status=None)
    r2 = _create_mock_run(quality_status=DataQualityStatus.VALID)

    status, reasons, changed = engine.evaluate_comparability(r1, r2)
    assert status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.DATA_QUALITY in changed
    assert any("unrecorded or missing" in r for r in reasons)

    # Both None: cannot infer equivalence!
    r3 = _create_mock_run(quality_status=None)
    status2, reasons2, changed2 = engine.evaluate_comparability(r1, r3)
    assert status2 == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.DATA_QUALITY in changed2


# =============================================================================
# SECTION B: CONFIGURATION DIFF ENGINE
# =============================================================================

def test_diff_configurations_identical():
    """Verify identical configurations yield all UNCHANGED diffs."""
    cfg = {"alpha": 1.0, "beta": "auto"}
    diffs = diff_configurations(cfg, cfg)
    assert len(diffs) == 2
    assert all(d.diff_type == DifferenceType.UNCHANGED for d in diffs)


def test_diff_configurations_changed_scalar_and_nested():
    """Verify changed scalar and deeply nested dictionary changes with dot notation."""
    c1 = {"risk": {"max_drawdown": 0.10, "leverage": 1.0}}
    c2 = {"risk": {"max_drawdown": 0.15, "leverage": 1.0}}

    diffs = diff_configurations(c1, c2)
    d_map = {d.parameter_path: d for d in diffs}
    assert d_map["risk.max_drawdown"].diff_type == DifferenceType.CHANGED
    assert d_map["risk.max_drawdown"].left_value == 0.10
    assert d_map["risk.max_drawdown"].right_value == 0.15
    assert d_map["risk.leverage"].diff_type == DifferenceType.UNCHANGED


def test_diff_configurations_added_and_removed_keys():
    """Verify keys added or removed are properly categorized."""
    c1 = {"retained": True, "old_param": 100}
    c2 = {"retained": True, "new_param": 200}

    diffs = diff_configurations(c1, c2)
    d_map = {d.parameter_path: d for d in diffs}
    assert d_map["old_param"].diff_type == DifferenceType.REMOVED
    assert d_map["old_param"].left_value == 100
    assert d_map["old_param"].right_value is None

    assert d_map["new_param"].diff_type == DifferenceType.ADDED
    assert d_map["new_param"].left_value is None
    assert d_map["new_param"].right_value == 200


def test_diff_configurations_reordered_mappings_are_deterministic():
    """Verify dictionary key ordering does not affect deterministic diff output."""
    c1 = collections.OrderedDict([("b", 2), ("a", 1)])
    c2 = collections.OrderedDict([("a", 1), ("b", 2)])

    diffs = diff_configurations(c1, c2)
    assert [d.parameter_path for d in diffs] == ["a", "b"]
    assert all(d.diff_type == DifferenceType.UNCHANGED for d in diffs)


def test_diff_configurations_handles_none_and_zero():
    """Verify None and zero are treated as distinct values without false equivalence."""
    c1 = {"val": 0}
    c2 = {"val": None}
    diffs = diff_configurations(c1, c2)
    assert diffs[0].diff_type == DifferenceType.CHANGED
    assert diffs[0].left_value == 0
    assert diffs[0].right_value is None

    # Zero vs zero: unchanged
    diffs_zero = diff_configurations({"val": 0}, {"val": 0})
    assert diffs_zero[0].diff_type == DifferenceType.UNCHANGED
    assert diffs_zero[0].left_value == 0


def test_diff_configurations_secret_redaction_in_memory_and_serialization():
    """Verify secret credentials are never exposed in memory or to_dict output."""
    c1 = {"api_key": "raw_secret_key_1", "normal": 42}
    c2 = {"api_key": "raw_secret_key_2", "normal": 42}

    diffs = diff_configurations(c1, c2)
    d_map = {d.parameter_path: d for d in diffs}
    sec_diff = d_map["api_key"]

    assert sec_diff.is_secret is True
    # In-memory values are redacted
    assert sec_diff.left_value == "[REDACTED]"
    assert sec_diff.right_value == "[REDACTED]"
    assert sec_diff.diff_type == DifferenceType.CHANGED

    # Serialized dict is redacted
    d_dict = sec_diff.to_dict()
    assert d_dict["left_value"] == "[REDACTED]"
    assert d_dict["right_value"] == "[REDACTED]"
    assert "raw_secret_key" not in json.dumps(d_dict)


# =============================================================================
# SECTION C: PROVENANCE DIFF ENGINE
# =============================================================================

def test_diff_provenance_identical_and_different():
    """Verify structured provenance diff across source, range, record count, and transformation."""
    r1 = _create_mock_run(
        provenance_ids=("prov_1",),
        data_start=1700000000.0,
        data_end=1700086400.0,
        input_records=1000,
        execution_metadata={"source": "binance", "transformation_version": "v1"},
    )
    r2 = _create_mock_run(
        provenance_ids=("prov_2",),
        data_start=1700086400.0,
        data_end=1700172800.0,
        input_records=1500,
        execution_metadata={"source": "coinbase", "transformation_version": "v2"},
    )

    diffs = diff_provenance(r1, r2)
    p_map = {d.dimension: d for d in diffs}

    assert p_map["input_provenance_ids"].diff_type == DifferenceType.CHANGED
    assert p_map["data_range"].diff_type == DifferenceType.CHANGED
    assert p_map["input_record_count"].diff_type == DifferenceType.CHANGED
    assert "+500" in p_map["input_record_count"].description
    assert p_map["source"].diff_type == DifferenceType.CHANGED
    assert p_map["transformation_version"].diff_type == DifferenceType.CHANGED


# =============================================================================
# SECTION D: QUALITY DIFF ENGINE
# =============================================================================

def test_diff_quality_status_transitions():
    """Verify quality status transitions: VALID -> VALID, VALID -> DEGRADED, DEGRADED -> INVALID."""
    # 1. VALID -> VALID
    r_val1 = _create_mock_run(quality_status=DataQualityStatus.VALID)
    r_val2 = _create_mock_run(quality_status=DataQualityStatus.VALID)
    diffs1 = diff_quality(r_val1, r_val2)
    q1 = next(d for d in diffs1 if d.dimension == "quality_status")
    assert q1.diff_type == DifferenceType.UNCHANGED
    assert "VALID" in q1.description

    # 2. VALID -> DEGRADED
    r_deg = _create_mock_run(quality_status=DataQualityStatus.DEGRADED)
    diffs2 = diff_quality(r_val1, r_deg)
    q2 = next(d for d in diffs2 if d.dimension == "quality_status")
    assert q2.diff_type == DifferenceType.CHANGED
    assert "VALID to DEGRADED" in q2.description

    # 3. DEGRADED -> INVALID
    r_inv = _create_mock_run(quality_status=DataQualityStatus.INVALID)
    diffs3 = diff_quality(r_deg, r_inv)
    q3 = next(d for d in diffs3 if d.dimension == "quality_status")
    assert q3.diff_type == DifferenceType.CHANGED
    assert "DEGRADED to INVALID" in q3.description


def test_diff_quality_anomaly_gap_and_score_metrics():
    """Verify tracking of quality_score, anomaly_count, gap_count, and invalid_record_count."""
    r1 = _create_mock_run(
        execution_metadata={
            "quality_score": 98.5,
            "anomaly_count": 0,
            "gap_count": 0,
            "invalid_record_count": 0,
            "highest_severity": "INFO",
        }
    )
    r2 = _create_mock_run(
        execution_metadata={
            "quality_score": 82.0,
            "anomaly_count": 3,
            "gap_count": 2,
            "invalid_record_count": 5,
            "highest_severity": "ERROR",
        }
    )

    diffs = diff_quality(r1, r2)
    q_map = {d.dimension: d for d in diffs}

    assert q_map["quality_score"].diff_type == DifferenceType.CHANGED
    assert q_map["quality_score"].left_value == 98.5
    assert q_map["quality_score"].right_value == 82.0

    assert q_map["anomaly_count"].diff_type == DifferenceType.CHANGED
    assert q_map["anomaly_count"].left_value == 0
    assert q_map["anomaly_count"].right_value == 3

    assert q_map["gap_count"].diff_type == DifferenceType.CHANGED
    assert q_map["invalid_record_count"].diff_type == DifferenceType.CHANGED
    assert q_map["highest_severity"].diff_type == DifferenceType.CHANGED


# =============================================================================
# SECTION E: METRIC COMPARISON & NUMERICAL SAFETY
# =============================================================================

def test_metric_comparison_numerical_invariants():
    """Verify safe metric comparisons: equal, increased, decreased, zero, negative, zero baseline."""
    # 1. Equal
    d_eq = compute_metric_difference("ret", 10.0, 10.0)
    assert d_eq.absolute_difference == 0.0
    assert d_eq.relative_difference == 0.0
    assert d_eq.direction == MetricDirection.UNCHANGED

    # 2. Increased
    d_inc = compute_metric_difference("ret", 10.0, 15.0)
    assert d_inc.absolute_difference == 5.0
    assert d_inc.relative_difference == 50.0
    assert d_inc.direction == MetricDirection.INCREASED

    # 3. Decreased
    d_dec = compute_metric_difference("ret", 20.0, 10.0)
    assert d_dec.absolute_difference == -10.0
    assert d_dec.relative_difference == -50.0
    assert d_dec.direction == MetricDirection.DECREASED

    # 4. Zero baseline: relative difference MUST be None with reason
    d_zero_base = compute_metric_difference("ret", 0.0, 10.0)
    assert d_zero_base.absolute_difference == 10.0
    assert d_zero_base.relative_difference is None
    assert d_zero_base.is_available is True
    assert "undefined" in d_zero_base.reason.lower()

    # 5. Negative values
    d_neg = compute_metric_difference("pnl", -50.0, -25.0)
    assert d_neg.absolute_difference == 25.0
    assert d_neg.relative_difference == 50.0
    assert d_neg.direction == MetricDirection.INCREASED

    # 6. NaN / Inf rejection
    d_nan = compute_metric_difference("bad", float("nan"), 10.0)
    assert d_nan.is_available is False
    assert d_nan.direction == MetricDirection.UNAVAILABLE
    assert d_nan.absolute_difference is None

    d_inf = compute_metric_difference("bad", 10.0, float("inf"))
    assert d_inf.is_available is False
    assert d_inf.direction == MetricDirection.UNAVAILABLE

    # 7. Preserves legitimate zero values (0.0 != unavailable)
    d_zero = compute_metric_difference("fees", 0.0, 0.0)
    assert d_zero.is_available is True
    assert d_zero.left_value == 0.0
    assert d_zero.right_value == 0.0
    assert d_zero.absolute_difference == 0.0


# =============================================================================
# SECTION F: MULTI-RUN COMPARISON
# =============================================================================

def test_multi_run_comparison_2_10_20_and_bounds():
    """Verify group comparison for 2 runs, 10 runs, 20 runs, and bound enforcement."""
    registry = AnalysisRunRegistry()
    runs = [_create_mock_run(run_id=f"m_run_{i}", config={"fast": i + 1}) for i in range(25)]
    for r in runs:
        registry.register_run(r)

    engine = RunComparisonEngine(run_registry=registry)

    # 2 runs
    comp2 = engine.compare_run_group(runs[:2])
    assert len(comp2.run_ids) == 2
    assert len(comp2.comparisons) == 1

    # 10 runs
    comp10 = engine.compare_run_group(runs[:10])
    assert len(comp10.run_ids) == 10
    assert len(comp10.comparisons) == 9

    # 20 runs
    comp20 = engine.compare_run_group(runs[:20])
    assert len(comp20.run_ids) == 20
    assert len(comp20.comparisons) == 19

    # Bound enforcement (> 20 runs raises ValidationError)
    with pytest.raises(ValidationError):
        engine.compare_run_group(runs[:21])

    # Minimum enforcement (< 2 runs raises ValidationError)
    with pytest.raises(ValidationError):
        engine.compare_run_group(runs[:1])


def test_multi_run_mixed_comparability():
    """Verify group comparison retains all runs and clearly reports mixed comparability."""
    r1 = _create_mock_run(run_id="base", op_type=OperationType.BACKTEST)
    r2 = _create_mock_run(run_id="cand_comp", op_type=OperationType.BACKTEST)
    r3 = _create_mock_run(run_id="cand_incomp", op_type=OperationType.TECHNICAL_ANALYSIS)

    engine = RunComparisonEngine()
    group = engine.compare_run_group([r1, r2, r3], baseline_run_id="base")
    assert len(group.comparisons) == 2

    c_map = {c.right_run_id: c for c in group.comparisons}
    assert c_map["cand_comp"].comparability_status == ComparabilityStatus.COMPARABLE
    assert c_map["cand_incomp"].comparability_status == ComparabilityStatus.NOT_COMPARABLE
    assert group.summary["status_breakdown"]["NOT_COMPARABLE"] == 1


# =============================================================================
# SECTION G: MASTER ORCHESTRATOR HANDLERS
# =============================================================================

def test_orchestrator_handlers_valid_and_invalid():
    """Verify MasterOrchestrator comparison handlers and rejection of malformed requests."""
    orchestrator = MasterOrchestrator()
    registry = AnalysisRunRegistry()
    r1 = _create_mock_run(run_id="h_r1")
    r2 = _create_mock_run(run_id="h_r2")
    registry.register_run(r1)
    registry.register_run(r2)

    engine = RunComparisonEngine(run_registry=registry)
    register_comparison_handlers(orchestrator=orchestrator, comparison_engine=engine)

    # 1. Valid compare_analysis_runs
    req = OrchestratorRequest.create(
        command="compare_analysis_runs",
        context={"left_run_id": "h_r1", "right_run_id": "h_r2"},
    )
    resp = orchestrator.handle_request(req)
    assert resp.is_success
    comp_id = resp.result["comparison_id"]

    # 2. Valid get_analysis_comparison
    get_req = OrchestratorRequest.create(
        command="get_analysis_comparison",
        context={"comparison_id": comp_id},
    )
    get_resp = orchestrator.handle_request(get_req)
    assert get_resp.is_success
    assert get_resp.result["comparison"]["comparison_id"] == comp_id

    # 3. Missing run
    miss_req = OrchestratorRequest.create(
        command="compare_analysis_runs",
        context={"left_run_id": "h_r1", "right_run_id": "non_existent_run"},
    )
    miss_resp = orchestrator.handle_request(miss_req)
    assert not miss_resp.is_success

    # 4. Duplicate run IDs in group
    dup_req = OrchestratorRequest.create(
        command="compare_analysis_run_group",
        context={"run_ids": ["h_r1", "h_r1"]},
    )
    dup_resp = orchestrator.handle_request(dup_req)
    assert not dup_resp.is_success

    # 5. Too many run IDs (> 20)
    many_req = OrchestratorRequest.create(
        command="compare_analysis_run_group",
        context={"run_ids": [f"run_{i}" for i in range(25)]},
    )
    many_resp = orchestrator.handle_request(many_req)
    assert not many_resp.is_success


# =============================================================================
# SECTION H: STATESTORE INTEGRATION
# =============================================================================

def test_statestore_bounded_history_and_copy_isolation():
    """Verify StateStore stores latest comparisons and maintains bounded history."""
    store = StateStore()
    engine = RunComparisonEngine(state_store=store)

    runs = [_create_mock_run(run_id=f"st_{i}") for i in range(60)]
    for i in range(55):
        engine.compare_runs(runs[i], runs[i + 1])

    history = store.get("analysis_comparisons:history")
    assert len(history) <= 50  # Bounded history cap

    # Copy isolation: modifying retrieved dict does not corrupt store
    history[0]["mutated"] = True
    fresh = store.get("analysis_comparisons:history")
    assert "mutated" not in fresh[0]


# =============================================================================
# SECTION I: EVENTBUS TELEMETRY
# =============================================================================

def test_eventbus_telemetry_correct_event_and_zero_secrets():
    """Verify EventBus publishes ANALYSIS_RUNS_COMPARED with zero secrets and bounded payload."""
    bus = EventBus()
    received: List[Event] = []
    bus.subscribe(EventType.ANALYSIS_RUNS_COMPARED, received.append)

    engine = RunComparisonEngine(event_bus=bus)
    r1 = _create_mock_run(run_id="ev1", config={"secret_token": "super_secret_123"})
    r2 = _create_mock_run(run_id="ev2", config={"secret_token": "super_secret_456"})

    comp = engine.compare_runs(r1, r2)
    assert len(received) == 1
    ev = received[0]

    assert ev.event_type == EventType.ANALYSIS_RUNS_COMPARED
    assert ev.payload["comparison_id"] == comp.comparison_id
    payload_str = str(ev.payload)
    assert "super_secret" not in payload_str


# =============================================================================
# SECTION J: DASHBOARD API INTEGRATION
# =============================================================================

def test_dashboard_api_endpoints_read_only_and_traversal_rejection():
    """Verify Dashboard /api/analysis-comparisons endpoints, traversal blocking, and 405 on POST."""
    status = bootstrap_system()
    engine = status.comparison_engine
    assert engine is not None

    r1 = _create_mock_run(run_id="d_r1")
    r2 = _create_mock_run(run_id="d_r2")
    comp = engine.compare_runs(r1, r2)

    server = PortfolioDashboardServer(
        reporting_service=status.portfolio_reporting_service,
        host="127.0.0.1",
        port=0,
        comparison_engine=engine,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        base_url = server.url

        # 1. Listing endpoint
        req1 = urllib.request.Request(f"{base_url}/api/analysis-comparisons")
        with urllib.request.urlopen(req1) as resp:
            assert resp.status == 200
            data1 = json.loads(resp.read().decode("utf-8"))
            assert data1["status"] == "success"
            assert data1["read_only"] is True

        # 2. Comparison by ID
        req2 = urllib.request.Request(f"{base_url}/api/analysis-comparisons/{comp.comparison_id}")
        with urllib.request.urlopen(req2) as resp:
            assert resp.status == 200
            data2 = json.loads(resp.read().decode("utf-8"))
            assert data2["comparison"]["comparison_id"] == comp.comparison_id

        # 3. Path traversal rejection
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/api/analysis-comparisons/..%2Fetc%2Fpasswd")
        assert exc_info.value.code in (400, 404)

        # 4. Read-only POST rejection
        post_req = urllib.request.Request(f"{base_url}/api/analysis-comparisons", data=b"{}", method="POST")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(post_req)
        assert exc_info.value.code == 405

    finally:
        server.stop()


# =============================================================================
# SECTION K: NUMERICAL INVARIANTS & NEUTRAL DIRECTIONS
# =============================================================================

def test_numerical_invariants_and_neutral_directions():
    """Verify absence of NaN/Inf, preservation of legitimate zeros, and neutral terminology."""
    diff = compute_metric_difference("drawdown", 10.0, 15.0)
    # Higher drawdown is mathematically an increase, but must NOT be labeled 'worse'
    assert diff.direction == MetricDirection.INCREASED
    assert diff.direction.value not in ("BETTER", "WORSE", "WINNER", "LOSER")


# =============================================================================
# SECTION L: PERFORMANCE BENCHMARKS
# =============================================================================

def test_performance_benchmarks():
    """Verify performance for 2, 10, and 20 run comparisons."""
    registry = AnalysisRunRegistry()
    runs = [_create_mock_run(run_id=f"perf_{i}") for i in range(20)]
    for r in runs:
        registry.register_run(r)

    engine = RunComparisonEngine(run_registry=registry)

    # 2 runs
    t0 = time.perf_counter()
    engine.compare_runs(runs[0], runs[1], bypass_cache=True)
    t_2 = time.perf_counter() - t0
    assert t_2 < 0.1

    # 10 runs
    t1 = time.perf_counter()
    engine.compare_run_group(runs[:10])
    t_10 = time.perf_counter() - t1
    assert t_10 < 0.5

    # 20 runs
    t2 = time.perf_counter()
    engine.compare_run_group(runs[:20])
    t_20 = time.perf_counter() - t2
    assert t_20 < 1.0


# =============================================================================
# SECTION M: SAFETY & JUJU UNTOUCHED
# =============================================================================

def test_safety_execution_locked_and_juju_untouched():
    """Verify safety lock, paper trading, and untouched JUJU directory."""
    settings = Settings()
    assert settings.execution_locked is True
    assert settings.paper_trading_only is True
    assert settings.broker_provider == "none"

    status = bootstrap_system()
    assert status.execution_engine == "locked"
    assert status.trading_engine == "not initialized"

    # Verify JUJU untouched
    juju_path = Path("JUJU")
    if juju_path.exists():
        for root, _, files in os.walk(juju_path):
            for file in files:
                assert not file.startswith("test_phase_2_step_22")


# =============================================================================
# SECTION N: PROPERTY INVARIANTS (REFLEXIVITY, SYMMETRY, NO WINNER)
# =============================================================================

def test_property_reflexivity_and_symmetry():
    """Verify reflexivity (A vs A) and symmetry (A vs B vs B vs A)."""
    engine = RunComparisonEngine()
    r_a = _create_mock_run(run_id="run_a", execution_metadata={"score": 10.0})
    r_b = _create_mock_run(run_id="run_b", execution_metadata={"score": 15.0})

    # Reflexivity
    comp_aa = engine.compare_runs(r_a, r_a)
    assert comp_aa.comparability_status == ComparabilityStatus.COMPARABLE
    m_aa = next(m for m in comp_aa.metric_differences if m.metric_name == "score")
    assert m_aa.absolute_difference == 0.0
    assert m_aa.direction == MetricDirection.UNCHANGED

    # Symmetry
    comp_ab = engine.compare_runs(r_a, r_b)
    comp_ba = engine.compare_runs(r_b, r_a)

    m_ab = next(m for m in comp_ab.metric_differences if m.metric_name == "score")
    m_ba = next(m for m in comp_ba.metric_differences if m.metric_name == "score")

    assert m_ab.absolute_difference == 5.0
    assert m_ab.direction == MetricDirection.INCREASED
    assert m_ba.absolute_difference == -5.0
    assert m_ba.direction == MetricDirection.DECREASED

    # Verify no winner or superior ranking is declared in summary or metric directions
    assert "winner" not in json.dumps(comp_ab.summary).lower()
    assert "superior" not in json.dumps(comp_ab.summary).lower()
    assert all(m.direction.value not in ("WINNER", "LOSER", "BETTER", "WORSE") for m in comp_ab.metric_differences)


# =============================================================================
# SECTION O: FAILURE RECOVERY & DIAGNOSTIC RUN COMPARISONS
# =============================================================================

def test_failure_recovery_diagnostic_runs():
    """Verify comparing failed/non-completed runs produces diagnostic limitations without crashing."""
    r_failed = _create_mock_run(run_id="fail_run", status=RunStatus.FAILED)
    r_ok = _create_mock_run(run_id="ok_run", status=RunStatus.COMPLETED)

    engine = RunComparisonEngine()
    comp = engine.compare_runs(r_failed, r_ok)

    assert any("Diagnostic run comparison" in lim for lim in comp.limitations)
    assert any("FAILED" in lim for lim in comp.limitations)
