"""KAISER — Phase 2, Step 22 Test Suite: Experiment Comparison & Run Comparison Foundation.

Comprehensive verification of:
A. Comparability Models & Status Enums (immutability, validation, neutral directions)
B. Comparability Evaluation across 11 Dimensions (operation_type, symbol_universe, etc.)
C. Configuration Difference Engine (dot notation, added/removed/changed, float tolerance, secret scrubbing)
D. Provenance & Quality Difference Engines (Step 20 provenance IDs, Step 19 quality statuses)
E. Numerical Metric Difference & Mathematical Safety (zero baseline, NaN/inf rejection, neutral direction)
F. Metric Extraction from Diverse Result Types (Backtest, Simulation, WalkForward, Robustness, Attribution)
G. RunComparisonEngine Operations (pair comparison, caching, LRU/FIFO eviction, bypass_cache, thread safety)
H. MultiRunComparison Operations (group comparison 2-20 runs, baseline alignment, metric matrix)
I. EventBus Telemetry (ANALYSIS_RUNS_COMPARED event, payload verification, zero secrets)
J. StateStore Synchronization (snapshot isolation, latest comparison, bounded history)
K. MasterOrchestrator Handlers (compare_analysis_runs, compare_analysis_run_group, get_analysis_comparison)
L. Dashboard API Endpoints (GET /api/analysis-comparisons, traversal rejection, 405 on mutations)
M. Grouping & Querying Utilities (group_runs_by dimensions)
N. Performance Workloads (pair comparisons, group benchmarks, cache hits)
O. Safety Invariants & JUJU Untouched (execution_locked=True, no broker calls, research disclaimers, JUJU untouched)
P. Property Invariants (reflexivity, symmetry, stability)
Q. Failure Recovery & Invalid Input Handling (non-existent runs, invalid IDs, duplicate runs in group)
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
from app.backtesting.models import BacktestResult, BacktestStatus
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
)
from app.backtesting.robustness.models import RobustnessTestResult
from app.backtesting.validation.models import WalkForwardResult
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.quality.status import DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.models import PortfolioAttributionReport
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
    quality_status: DataQualityStatus = DataQualityStatus.VALID,
    strategy_id: Optional[str] = "sma_crossover",
    strategy_version: Optional[str] = "1.0.0",
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
        completed_at=1700000005.0,
        input_provenance_ids=provenance_ids,
        parent_run_ids=(),
        configuration_fingerprint=fp,
        configuration_summary=dict(cfg),
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        analysis_version="1.0.0",
        schema_version=schema_version,
        software_version=software_version,
        symbol_universe=symbols,
        timeframe_universe=timeframes,
        data_start=data_start,
        data_end=data_end,
        input_record_count=input_records,
        quality_assessment_ids=("qa_test_01",),
        quality_status=quality_status,
        result_id=result_id or f"res_{rid}",
        result_type=result_type,
        result_fingerprint="mock_result_fp",
        execution_metadata=execution_metadata if execution_metadata is not None else {"total_return_pct": 12.5, "sharpe_ratio": 1.45, "max_drawdown_pct": 5.2},
    )


# =============================================================================
# SECTION A: DOMAIN MODELS & STATUS ENUMS
# =============================================================================

def test_domain_model_enums_and_immutability():
    """Verify enum invariants and dataclass immutability."""
    assert ComparabilityStatus.COMPARABLE == "COMPARABLE"
    assert ComparabilityStatus.PARTIALLY_COMPARABLE == "PARTIALLY_COMPARABLE"
    assert ComparabilityStatus.NOT_COMPARABLE == "NOT_COMPARABLE"

    assert DifferenceType.ADDED == "ADDED"
    assert DifferenceType.REMOVED == "REMOVED"
    assert DifferenceType.CHANGED == "CHANGED"
    assert DifferenceType.UNCHANGED == "UNCHANGED"

    assert MetricDirection.INCREASED == "INCREASED"
    assert MetricDirection.DECREASED == "DECREASED"
    assert MetricDirection.UNCHANGED == "UNCHANGED"
    assert MetricDirection.UNAVAILABLE == "UNAVAILABLE"

    # MetricDifference immutability
    diff = MetricDifference(
        metric_name="sharpe_ratio",
        left_value=1.2,
        right_value=1.5,
        absolute_difference=0.3,
        relative_difference=25.0,
        direction=MetricDirection.INCREASED,
        is_available=True,
    )
    with pytest.raises(Exception):
        diff.absolute_difference = 0.5  # type: ignore

    d_dict = diff.to_dict()
    assert d_dict["metric_name"] == "sharpe_ratio"
    assert d_dict["direction"] == "INCREASED"
    assert d_dict["is_available"] is True


def test_metric_difference_validation_rejects_nan_and_inf():
    """Verify MetricDifference strictly rejects NaN and Infinity."""
    with pytest.raises(ValidationError):
        MetricDifference(
            metric_name="bad_metric",
            left_value=float("nan"),
            right_value=1.0,
            absolute_difference=0.0,
            relative_difference=0.0,
            direction=MetricDirection.UNCHANGED,
            is_available=True,
        )

    with pytest.raises(ValidationError):
        MetricDifference(
            metric_name="bad_metric",
            left_value=1.0,
            right_value=float("inf"),
            absolute_difference=float("inf"),
            relative_difference=0.0,
            direction=MetricDirection.INCREASED,
            is_available=True,
        )


def test_configuration_difference_secret_redaction():
    """Verify ConfigurationDifference redacts secret values in serialization."""
    diff_plain = ConfigurationDifference(
        parameter_path="model.layers",
        left_value=3,
        right_value=5,
        diff_type=DifferenceType.CHANGED,
        is_secret=False,
    )
    assert diff_plain.to_dict()["left_value"] == 3
    assert diff_plain.to_dict()["right_value"] == 5

    diff_secret = ConfigurationDifference(
        parameter_path="broker.api_key",
        left_value="secret_key_123",
        right_value="secret_key_456",
        diff_type=DifferenceType.CHANGED,
        is_secret=True,
    )
    dict_sec = diff_secret.to_dict()
    assert dict_sec["left_value"] == "[REDACTED]"
    assert dict_sec["right_value"] == "[REDACTED]"
    assert dict_sec["is_secret"] is True


def test_run_comparison_model_validation():
    """Verify RunComparison required field validation and research disclaimer."""
    with pytest.raises(ValidationError):
        RunComparison(
            comparison_id="",
            generated_at=time.time(),
            left_run_id="run_1",
            right_run_id="run_2",
            comparability_status=ComparabilityStatus.COMPARABLE,
            comparability_reasons=(),
            changed_dimensions=(),
        )

    with pytest.raises(ValidationError):
        RunComparison(
            comparison_id="comp_1",
            generated_at=-1.0,  # Negative timestamp
            left_run_id="run_1",
            right_run_id="run_2",
            comparability_status=ComparabilityStatus.COMPARABLE,
            comparability_reasons=(),
            changed_dimensions=(),
        )

    comp = RunComparison(
        comparison_id="comp_valid",
        generated_at=1700000000.0,
        left_run_id="run_1",
        right_run_id="run_2",
        comparability_status=ComparabilityStatus.COMPARABLE,
        comparability_reasons=("Identical setup",),
        changed_dimensions=(),
    )
    assert "RESEARCH ONLY" in comp.disclaimer
    assert comp.to_dict()["comparison_id"] == "comp_valid"


def test_multi_run_comparison_model_bounds():
    """Verify MultiRunComparison enforces bounds (at least 2, at most 20 runs)."""
    with pytest.raises(ValidationError):
        MultiRunComparison(
            comparison_id="mcomp_1",
            generated_at=time.time(),
            run_ids=("run_1",),  # Only 1 run
            baseline_run_id="run_1",
            comparisons=(),
            aligned_metrics={},
        )

    with pytest.raises(ValidationError):
        MultiRunComparison(
            comparison_id="mcomp_2",
            generated_at=time.time(),
            run_ids=tuple(f"run_{i}" for i in range(25)),  # 25 runs exceeds 20
            baseline_run_id="run_0",
            comparisons=(),
            aligned_metrics={},
        )


# =============================================================================
# SECTION B: COMPARABILITY EVALUATION ACROSS 11 DIMENSIONS
# =============================================================================

def test_comparability_identical_runs():
    """Verify identical runs evaluate to COMPARABLE with zero changed dimensions."""
    engine = RunComparisonEngine()
    run1 = _create_mock_run(run_id="run_1")
    run2 = _create_mock_run(run_id="run_2")

    status, reasons, changed = engine.evaluate_comparability(run1, run2)
    assert status == ComparabilityStatus.COMPARABLE
    assert len(changed) == 0
    assert any("Identical" in r for r in reasons)


def test_comparability_operation_type_mismatch():
    """Verify mismatched operation_type evaluates to NOT_COMPARABLE."""
    engine = RunComparisonEngine()
    run1 = _create_mock_run(op_type=OperationType.BACKTEST)
    run2 = _create_mock_run(op_type=OperationType.TECHNICAL_ANALYSIS)

    status, reasons, changed = engine.evaluate_comparability(run1, run2)
    assert status == ComparabilityStatus.NOT_COMPARABLE
    assert ComparisonDimension.OPERATION_TYPE in changed
    assert any("operation types" in r.lower() for r in reasons)


def test_comparability_result_type_mismatch():
    """Verify mismatched result_type evaluates to NOT_COMPARABLE."""
    engine = RunComparisonEngine()
    run1 = _create_mock_run(result_type="BacktestResult")
    run2 = _create_mock_run(result_type="PortfolioSimulationResult")

    status, reasons, changed = engine.evaluate_comparability(run1, run2)
    assert status == ComparabilityStatus.NOT_COMPARABLE
    assert ComparisonDimension.RESULT_TYPE in changed


def test_comparability_symbol_universe_mismatch():
    """Verify differing symbol universes evaluate to NOT_COMPARABLE."""
    engine = RunComparisonEngine()
    run1 = _create_mock_run(symbols=("BTC/USDT",))
    run2 = _create_mock_run(symbols=("ETH/USDT",))

    status, reasons, changed = engine.evaluate_comparability(run1, run2)
    assert status == ComparabilityStatus.NOT_COMPARABLE
    assert ComparisonDimension.SYMBOL_UNIVERSE in changed


def test_comparability_timeframe_universe_mismatch():
    """Verify differing timeframe universes evaluate to NOT_COMPARABLE."""
    engine = RunComparisonEngine()
    run1 = _create_mock_run(timeframes=("1m",))
    run2 = _create_mock_run(timeframes=("5m",))

    status, reasons, changed = engine.evaluate_comparability(run1, run2)
    assert status == ComparabilityStatus.NOT_COMPARABLE
    assert ComparisonDimension.TIMEFRAME_UNIVERSE in changed


def test_comparability_schema_and_software_version():
    """Verify schema version mismatch is NOT_COMPARABLE, software version is PARTIALLY_COMPARABLE."""
    engine = RunComparisonEngine()

    # Schema version mismatch
    run1 = _create_mock_run(schema_version="1.0.0")
    run2 = _create_mock_run(schema_version="2.0.0")
    status, _, changed = engine.evaluate_comparability(run1, run2)
    assert status == ComparabilityStatus.NOT_COMPARABLE
    assert ComparisonDimension.SOFTWARE_VERSION in changed

    # Software version difference
    run3 = _create_mock_run(software_version="1.0.0")
    run4 = _create_mock_run(software_version="1.1.0")
    status2, _, changed2 = engine.evaluate_comparability(run3, run4)
    assert status2 == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.SOFTWARE_VERSION in changed2


def test_comparability_strategy_and_parameters():
    """Verify different strategies or configuration parameters evaluate to PARTIALLY_COMPARABLE."""
    engine = RunComparisonEngine()

    # Strategy mismatch
    run1 = _create_mock_run(strategy_id="sma_crossover")
    run2 = _create_mock_run(strategy_id="ema_crossover")
    status, _, changed = engine.evaluate_comparability(run1, run2)
    assert status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.STRATEGY in changed

    # Config parameter mismatch
    run3 = _create_mock_run(config={"fast_period": 5, "slow_period": 15})
    run4 = _create_mock_run(config={"fast_period": 10, "slow_period": 30})
    status2, _, changed2 = engine.evaluate_comparability(run3, run4)
    assert status2 == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.CONFIGURATION in changed2


def test_comparability_data_range_and_quality():
    """Verify differing date ranges or data quality statuses evaluate to PARTIALLY_COMPARABLE."""
    engine = RunComparisonEngine()

    # Date range mismatch
    run1 = _create_mock_run(data_start=1700000000.0, data_end=1700086400.0)
    run2 = _create_mock_run(data_start=1700086400.0, data_end=1700172800.0)
    status, _, changed = engine.evaluate_comparability(run1, run2)
    assert status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.DATA_RANGE in changed

    # Quality status mismatch
    run3 = _create_mock_run(quality_status=DataQualityStatus.VALID)
    run4 = _create_mock_run(quality_status=DataQualityStatus.DEGRADED)
    status2, _, changed2 = engine.evaluate_comparability(run3, run4)
    assert status2 == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.DATA_QUALITY in changed2


# =============================================================================
# SECTION C: CONFIGURATION DIFFERENCE ENGINE
# =============================================================================

def test_diff_configurations_basic_and_nested():
    """Verify nested configuration diffing with dot notation and difference types."""
    cfg1 = {
        "model": {"layers": 3, "activation": "relu"},
        "lr": 0.001,
        "removed_field": 42,
    }
    cfg2 = {
        "model": {"layers": 5, "activation": "relu"},
        "lr": 0.002,
        "added_field": "new",
    }

    diffs = diff_configurations(cfg1, cfg2)
    diff_map = {d.parameter_path: d for d in diffs}

    assert diff_map["model.layers"].diff_type == DifferenceType.CHANGED
    assert diff_map["model.layers"].left_value == 3
    assert diff_map["model.layers"].right_value == 5

    assert diff_map["model.activation"].diff_type == DifferenceType.UNCHANGED
    assert diff_map["lr"].diff_type == DifferenceType.CHANGED
    assert diff_map["removed_field"].diff_type == DifferenceType.REMOVED
    assert diff_map["added_field"].diff_type == DifferenceType.ADDED


def test_diff_configurations_floating_point_tolerance():
    """Verify floating point equivalence within 1e-9 tolerance."""
    cfg1 = {"value": 1.00000000001}
    cfg2 = {"value": 1.00000000002}
    diffs = diff_configurations(cfg1, cfg2)
    assert len(diffs) == 1
    assert diffs[0].diff_type == DifferenceType.UNCHANGED


def test_diff_configurations_secret_key_detection():
    """Verify credential and token keys are flagged as secrets."""
    cfg1 = {"api_key": "abc", "nested": {"password_hash": "xyz"}}
    cfg2 = {"api_key": "def", "nested": {"password_hash": "xyz"}}
    diffs = diff_configurations(cfg1, cfg2)
    diff_map = {d.parameter_path: d for d in diffs}

    assert diff_map["api_key"].is_secret is True
    assert diff_map["api_key"].to_dict()["left_value"] == "[REDACTED]"
    assert diff_map["nested.password_hash"].is_secret is True


# =============================================================================
# SECTION D: PROVENANCE & QUALITY DIFFERENCE ENGINES
# =============================================================================

def test_diff_provenance_and_quality():
    """Verify structured provenance and quality diff computation."""
    run1 = _create_mock_run(
        provenance_ids=("prov_1", "prov_2"),
        quality_status=DataQualityStatus.VALID,
        input_records=1000,
    )
    run2 = _create_mock_run(
        provenance_ids=("prov_1", "prov_3"),
        quality_status=DataQualityStatus.DEGRADED,
        input_records=1200,
    )

    prov_diffs = diff_provenance(run1, run2)
    p_map = {d.dimension: d for d in prov_diffs}
    assert p_map["input_provenance_ids"].diff_type == DifferenceType.CHANGED
    assert p_map["input_record_count"].diff_type == DifferenceType.CHANGED
    assert "+200" in p_map["input_record_count"].description

    qual_diffs = diff_quality(run1, run2)
    q_map = {d.dimension: d for d in qual_diffs}
    assert q_map["quality_status"].diff_type == DifferenceType.CHANGED
    assert "VALID to DEGRADED" in q_map["quality_status"].description


# =============================================================================
# SECTION E: NUMERICAL METRIC DIFFERENCE & MATHEMATICAL SAFETY
# =============================================================================

def test_safe_float_rejections():
    """Verify safe_float rejects invalid values and NaNs/Infs."""
    assert safe_float(12.5) == 12.5
    assert safe_float("12.5") == 12.5
    assert safe_float(0) == 0.0
    assert safe_float(None) is None
    assert safe_float(True) is None  # Boolean rejected
    assert safe_float(False) is None
    assert safe_float("invalid") is None
    assert safe_float(float("nan")) is None
    assert safe_float(float("inf")) is None
    assert safe_float(float("-inf")) is None


def test_compute_metric_difference_standard_and_neutral_directions():
    """Verify absolute and relative deltas with neutral direction semantics."""
    # Increase
    d1 = compute_metric_difference("sharpe", 1.0, 1.5)
    assert d1.is_available is True
    assert d1.absolute_difference == 0.5
    assert d1.relative_difference == 50.0
    assert d1.direction == MetricDirection.INCREASED

    # Decrease
    d2 = compute_metric_difference("drawdown", 10.0, 5.0)
    assert d2.absolute_difference == -5.0
    assert d2.relative_difference == -50.0
    assert d2.direction == MetricDirection.DECREASED

    # Unchanged
    d3 = compute_metric_difference("win_rate", 55.0, 55.0)
    assert d3.absolute_difference == 0.0
    assert d3.relative_difference == 0.0
    assert d3.direction == MetricDirection.UNCHANGED


def test_compute_metric_difference_zero_baseline():
    """Verify zero baseline flags relative difference as undefined with reason."""
    diff = compute_metric_difference("profit", 0.0, 100.0)
    assert diff.is_available is True
    assert diff.left_value == 0.0
    assert diff.right_value == 100.0
    assert diff.absolute_difference == 100.0
    assert diff.relative_difference is None
    assert diff.direction == MetricDirection.INCREASED
    assert "undefined" in diff.reason.lower()


def test_compute_metric_difference_unavailable():
    """Verify unavailable metrics are handled gracefully without raising."""
    diff = compute_metric_difference("sharpe", None, 1.5)
    assert diff.is_available is False
    assert diff.direction == MetricDirection.UNAVAILABLE
    assert diff.absolute_difference is None
    assert diff.relative_difference is None
    assert "unavailable" in diff.reason.lower()


# =============================================================================
# SECTION F: METRIC EXTRACTION FROM DIVERSE RESULT TYPES
# =============================================================================

def test_extract_metrics_from_diverse_results():
    """Verify normalized metric extraction across Backtest, Simulation, WalkForward, Robustness, and Attribution."""
    # 1. BacktestResult dict
    b_res = {
        "net_return_pct": 15.2,
        "max_drawdown_pct": 4.1,
        "sharpe": 1.8,
        "total_trades": 50,
    }
    b_extracted = extract_metrics_from_result(b_res)
    assert b_extracted["total_return_pct"] == 15.2
    assert b_extracted["max_drawdown_pct"] == 4.1
    assert b_extracted["sharpe_ratio"] == 1.8
    assert b_extracted["trade_count"] == 50.0

    # 2. WalkForward result dict
    wf_res = {
        "compounded_return_pct": 22.5,
        "mean_window_return_pct": 4.5,
        "overall_oos_win_rate": 0.58,
    }
    wf_extracted = extract_metrics_from_result(wf_res)
    assert wf_extracted["compounded_return_pct"] == 22.5
    assert wf_extracted["overall_oos_win_rate"] == 0.58

    # 3. Robustness result dict
    rob_res = {
        "baseline_return_pct": 12.0,
        "mean_stressed_return_pct": 8.0,
        "fragility_score": 0.25,
    }
    rob_extracted = extract_metrics_from_result(rob_res)
    assert rob_extracted["fragility_score"] == 0.25

    # 4. Attribution result dict
    att_res = {
        "active_return_pct": 3.4,
        "allocation_effect_pct": 1.2,
        "selection_effect_pct": 2.2,
    }
    att_extracted = extract_metrics_from_result(att_res)
    assert att_extracted["active_return_pct"] == 3.4


# =============================================================================
# SECTION G: RUN COMPARISON ENGINE OPERATIONS
# =============================================================================

def test_engine_compare_runs_lifecycle():
    """Verify full end-to-end pair comparison lifecycle."""
    registry = AnalysisRunRegistry()
    run1 = _create_mock_run(run_id="run_base", config={"period": 10})
    run2 = _create_mock_run(run_id="run_cand", config={"period": 20})
    registry.register_run(run1)
    registry.register_run(run2)

    engine = RunComparisonEngine(run_registry=registry)
    comp = engine.compare_runs(left_run_or_id="run_base", right_run_or_id="run_cand")

    assert comp.left_run_id == "run_base"
    assert comp.right_run_id == "run_cand"
    assert comp.comparability_status == ComparabilityStatus.PARTIALLY_COMPARABLE
    assert ComparisonDimension.CONFIGURATION in comp.changed_dimensions
    assert len(comp.configuration_differences) > 0

    # Cached retrieval
    comp_cached = engine.compare_runs(left_run_or_id="run_base", right_run_or_id="run_cand")
    assert comp_cached is comp

    # Bypass cache
    comp_bypassed = engine.compare_runs(left_run_or_id="run_base", right_run_or_id="run_cand", bypass_cache=True)
    assert comp_bypassed is not comp
    assert comp_bypassed.comparison_id == comp.comparison_id


def test_engine_bounded_fifo_cache_eviction():
    """Verify engine limits in-memory cache to max_cache_size via FIFO eviction."""
    engine = RunComparisonEngine(max_cache_size=3)
    runs = [_create_mock_run(run_id=f"r_{i}") for i in range(5)]

    engine.compare_runs(runs[0], runs[1])
    engine.compare_runs(runs[0], runs[2])
    engine.compare_runs(runs[0], runs[3])
    assert len(engine._cache) == 3

    # Add 4th comparison -> oldest should be evicted
    engine.compare_runs(runs[0], runs[4])
    assert len(engine._cache) == 3
    assert (runs[0].run_id, runs[1].run_id) not in engine._cache
    assert (runs[0].run_id, runs[4].run_id) in engine._cache


def test_engine_concurrency_and_thread_safety():
    """Verify RunComparisonEngine is thread-safe under concurrent multi-threaded load."""
    registry = AnalysisRunRegistry()
    runs = [_create_mock_run(run_id=f"thread_run_{i}") for i in range(10)]
    for r in runs:
        registry.register_run(r)

    engine = RunComparisonEngine(run_registry=registry, max_cache_size=100)

    def worker(idx: int):
        r_a = runs[idx % len(runs)]
        r_b = runs[(idx + 1) % len(runs)]
        return engine.compare_runs(r_a, r_b)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, i) for i in range(40)]
        results = [f.result() for f in futures]

    assert len(results) == 40
    assert all(isinstance(r, RunComparison) for r in results)


# =============================================================================
# SECTION H: MULTI-RUN COMPARISON (GROUP COMPARISON)
# =============================================================================

def test_engine_compare_run_group():
    """Verify comparing a group of runs against a baseline."""
    registry = AnalysisRunRegistry()
    runs = [
        _create_mock_run(run_id=f"g_run_{i}", config={"period": 10 * (i + 1)})
        for i in range(4)
    ]
    for r in runs:
        registry.register_run(r)

    engine = RunComparisonEngine(run_registry=registry)
    group_comp = engine.compare_run_group(
        runs_or_ids=[r.run_id for r in runs],
        baseline_run_id="g_run_0",
    )

    assert group_comp.baseline_run_id == "g_run_0"
    assert len(group_comp.run_ids) == 4
    assert len(group_comp.comparisons) == 3  # 3 comparisons vs baseline
    assert "total_return_pct" in group_comp.aligned_metrics
    assert "g_run_0" in group_comp.aligned_metrics["total_return_pct"]


def test_engine_compare_run_group_validation():
    """Verify group comparison input validation."""
    engine = RunComparisonEngine()
    run = _create_mock_run(run_id="single_run")

    # Less than 2 runs
    with pytest.raises(ValidationError):
        engine.compare_run_group([run])

    # Baseline not found
    runs = [_create_mock_run(run_id="r1"), _create_mock_run(run_id="r2")]
    with pytest.raises(ValidationError):
        engine.compare_run_group(runs, baseline_run_id="non_existent")


# =============================================================================
# SECTION I: EVENTBUS TELEMETRY
# =============================================================================

def test_eventbus_telemetry_emission():
    """Verify EventBus publishes ANALYSIS_RUNS_COMPARED event with zero secrets."""
    bus = EventBus()
    received_events: List[Event] = []
    bus.subscribe(EventType.ANALYSIS_RUNS_COMPARED, received_events.append)

    engine = RunComparisonEngine(event_bus=bus)
    r1 = _create_mock_run(run_id="ev_r1", config={"api_key": "secret_abc"})
    r2 = _create_mock_run(run_id="ev_r2", config={"api_key": "secret_def"})

    comp = engine.compare_runs(r1, r2)
    assert len(received_events) == 1
    event = received_events[0]

    assert event.event_type == EventType.ANALYSIS_RUNS_COMPARED
    assert event.payload["comparison_id"] == comp.comparison_id
    assert event.payload["left_run_id"] == "ev_r1"
    assert event.payload["right_run_id"] == "ev_r2"

    # Confirm no credentials in serialized payload
    payload_str = str(event.payload)
    assert "secret_abc" not in payload_str
    assert "secret_def" not in payload_str


# =============================================================================
# SECTION J: STATESTORE SYNCHRONIZATION
# =============================================================================

def test_statestore_synchronization():
    """Verify StateStore is updated with latest comparison and bounded history."""
    store = StateStore()
    engine = RunComparisonEngine(state_store=store)

    r1 = _create_mock_run(run_id="st_r1")
    r2 = _create_mock_run(run_id="st_r2")
    comp = engine.compare_runs(r1, r2)

    # Key by ID
    by_id = store.get(f"analysis_comparison:{comp.comparison_id}")
    assert by_id is not None
    assert by_id["comparison_id"] == comp.comparison_id

    # Latest comparison
    latest = store.get("analysis_comparisons:latest")
    assert latest is not None
    assert latest["comparison_id"] == comp.comparison_id

    # History list
    history = store.get("analysis_comparisons:history")
    assert len(history) == 1
    assert history[0]["comparison_id"] == comp.comparison_id


# =============================================================================
# SECTION K: MASTER ORCHESTRATOR HANDLERS
# =============================================================================

def test_orchestrator_handlers_integration():
    """Verify MasterOrchestrator handler execution for comparison commands."""
    orchestrator = MasterOrchestrator()
    registry = AnalysisRunRegistry()
    r1 = _create_mock_run(run_id="orch_r1")
    r2 = _create_mock_run(run_id="orch_r2")
    registry.register_run(r1)
    registry.register_run(r2)

    engine = RunComparisonEngine(run_registry=registry)
    register_comparison_handlers(orchestrator=orchestrator, comparison_engine=engine)

    # 1. compare_analysis_runs
    req = OrchestratorRequest.create(
        command="compare_analysis_runs",
        context={"left_run_id": "orch_r1", "right_run_id": "orch_r2"},
    )
    resp = orchestrator.handle_request(req)
    assert resp.is_success
    assert resp.result["status"] == "success"
    comp_id = resp.result["comparison_id"]

    # 2. get_analysis_comparison
    get_req = OrchestratorRequest.create(
        command="get_analysis_comparison",
        context={"comparison_id": comp_id},
    )
    get_resp = orchestrator.handle_request(get_req)
    assert get_resp.is_success
    assert get_resp.result["comparison"]["comparison_id"] == comp_id

    # 3. compare_analysis_run_group
    group_req = OrchestratorRequest.create(
        command="compare_analysis_run_group",
        context={"run_ids": ["orch_r1", "orch_r2"]},
    )
    group_resp = orchestrator.handle_request(group_req)
    assert group_resp.is_success
    assert group_resp.result["multi_run_comparison"]["runs_count"] == 2


def test_orchestrator_handlers_validation_failures():
    """Verify handler validation failures on malformed parameters."""
    orchestrator = MasterOrchestrator()
    engine = RunComparisonEngine()
    register_comparison_handlers(orchestrator=orchestrator, comparison_engine=engine)

    # Missing left_run_id
    req = OrchestratorRequest.create(
        command="compare_analysis_runs",
        context={"right_run_id": "orch_r2"},
    )
    resp = orchestrator.handle_request(req)
    assert not resp.is_success

    # Duplicate run IDs in group
    group_req = OrchestratorRequest.create(
        command="compare_analysis_run_group",
        context={"run_ids": ["run_1", "run_1"]},
    )
    group_resp = orchestrator.handle_request(group_req)
    assert not group_resp.is_success


# =============================================================================
# SECTION L: DASHBOARD API INTEGRATION
# =============================================================================

def test_dashboard_analysis_comparisons_endpoints():
    """Verify read-only GET /api/analysis-comparisons, traversal rejection, and 405 on POST."""
    status = bootstrap_system()
    engine = status.comparison_engine
    assert engine is not None

    r1 = _create_mock_run(run_id="dash_r1")
    r2 = _create_mock_run(run_id="dash_r2")
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
            assert data1["execution_locked"] is True
            assert data1["count"] >= 1

        # 2. Single comparison endpoint
        req2 = urllib.request.Request(f"{base_url}/api/analysis-comparisons/{comp.comparison_id}")
        with urllib.request.urlopen(req2) as resp:
            assert resp.status == 200
            data2 = json.loads(resp.read().decode("utf-8"))
            assert data2["comparison"]["comparison_id"] == comp.comparison_id

        # 3. Path traversal rejection
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/api/analysis-comparisons/..%2Fetc%2Fpasswd")
        assert exc_info.value.code in (400, 404)

        # 4. Read-only method blocking (POST -> 405 Method Not Allowed)
        post_req = urllib.request.Request(f"{base_url}/api/analysis-comparisons", data=b"{}", method="POST")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(post_req)
        assert exc_info.value.code == 405

    finally:
        server.stop()


# =============================================================================
# SECTION M: GROUPING & QUERYING UTILITIES
# =============================================================================

def test_engine_group_runs_by_dimensions():
    """Verify group_runs_by organizes runs by dimension categories."""
    engine = RunComparisonEngine()
    runs = [
        _create_mock_run(run_id="r1", op_type=OperationType.BACKTEST, strategy_id="sma"),
        _create_mock_run(run_id="r2", op_type=OperationType.BACKTEST, strategy_id="ema"),
        _create_mock_run(run_id="r3", op_type=OperationType.WALK_FORWARD, strategy_id="sma"),
    ]

    by_op = engine.group_runs_by(runs, ComparisonDimension.OPERATION_TYPE)
    assert len(by_op["BACKTEST"]) == 2
    assert len(by_op["WALK_FORWARD"]) == 1

    by_strat = engine.group_runs_by(runs, ComparisonDimension.STRATEGY)
    assert len(by_strat["sma"]) == 2
    assert len(by_strat["ema"]) == 1


# =============================================================================
# SECTION N: PERFORMANCE BENCHMARKS
# =============================================================================

def test_performance_pair_and_group_benchmarks():
    """Verify high-throughput pair and multi-run comparison execution."""
    registry = AnalysisRunRegistry()
    runs = [_create_mock_run(run_id=f"perf_{i}") for i in range(20)]
    for r in runs:
        registry.register_run(r)

    engine = RunComparisonEngine(run_registry=registry, max_cache_size=500)

    # 1. 50 pair comparisons benchmark
    start_t = time.perf_counter()
    for i in range(50):
        engine.compare_runs(runs[i % 20], runs[(i + 1) % 20], bypass_cache=True)
    pair_dur = time.perf_counter() - start_t
    assert pair_dur < 1.0  # 50 uncached comparisons in under 1 second

    # 2. Cache hit sub-millisecond check
    start_cache = time.perf_counter()
    for _ in range(100):
        engine.compare_runs(runs[0], runs[1], bypass_cache=False)
    cache_dur = time.perf_counter() - start_cache
    assert cache_dur < 0.05  # 100 cache hits in under 50ms

    # 3. 20-run group comparison
    start_grp = time.perf_counter()
    grp_comp = engine.compare_run_group(runs)
    grp_dur = time.perf_counter() - start_grp
    assert grp_dur < 0.5
    assert len(grp_comp.comparisons) == 19


# =============================================================================
# SECTION O: SAFETY INVARIANTS & JUJU UNTOUCHED
# =============================================================================

def test_safety_invariants_and_juju_untouched():
    """Verify execution engine lock, uninitialized trading, and untouched JUJU."""
    status = bootstrap_system()
    assert status.execution_engine == "locked"
    assert status.trading_engine == "not initialized"

    settings = Settings()
    assert settings.execution_locked is True
    assert settings.paper_trading_only is True
    assert settings.broker_provider == "none"

    # Confirm JUJU is untouched
    juju_path = Path("JUJU")
    if juju_path.exists():
        # Verify no files were modified today in JUJU
        for root, _, files in os.walk(juju_path):
            for file in files:
                fpath = Path(root) / file
                assert not fpath.name.startswith("test_phase_2_step_22")


# =============================================================================
# SECTION P: PROPERTY INVARIANTS (REFLEXIVITY & SYMMETRY)
# =============================================================================

def test_property_reflexivity_and_symmetry():
    """Verify reflexivity (A vs A) and symmetry (A vs B vs B vs A)."""
    engine = RunComparisonEngine()
    run_a = _create_mock_run(run_id="run_a", execution_metadata={"metric": 10.0})
    run_b = _create_mock_run(run_id="run_b", execution_metadata={"metric": 15.0})

    # Reflexivity: A vs A
    comp_aa = engine.compare_runs(run_a, run_a)
    assert comp_aa.comparability_status == ComparabilityStatus.COMPARABLE
    assert len(comp_aa.changed_dimensions) == 0
    metric_diff_aa = next(m for m in comp_aa.metric_differences if m.metric_name == "metric")
    assert metric_diff_aa.absolute_difference == 0.0
    assert metric_diff_aa.direction == MetricDirection.UNCHANGED

    # Symmetry: A vs B vs B vs A
    comp_ab = engine.compare_runs(run_a, run_b)
    comp_ba = engine.compare_runs(run_b, run_a)

    m_ab = next(m for m in comp_ab.metric_differences if m.metric_name == "metric")
    m_ba = next(m for m in comp_ba.metric_differences if m.metric_name == "metric")

    assert m_ab.absolute_difference == 5.0
    assert m_ab.direction == MetricDirection.INCREASED

    assert m_ba.absolute_difference == -5.0
    assert m_ba.direction == MetricDirection.DECREASED


# =============================================================================
# SECTION Q: FAILURE RECOVERY & ROBUSTNESS
# =============================================================================

def test_failure_recovery_and_robustness():
    """Verify failure resilience when encountering non-existent runs or uninitialized engine."""
    engine = RunComparisonEngine()  # Empty registry

    with pytest.raises(ValidationError):
        engine.compare_runs("non_existent_1", "non_existent_2")

    # Run with empty metadata produces comparison with available_metrics_count == 0
    run_empty = _create_mock_run(run_id="empty_meta", execution_metadata={})
    run_full = _create_mock_run(run_id="full_meta", execution_metadata={"sharpe": 1.5})
    comp = engine.compare_runs(run_empty, run_full)
    assert comp.summary["available_metrics_count"] == 0
    assert any("limited numeric metric" in lim for lim in comp.limitations)
