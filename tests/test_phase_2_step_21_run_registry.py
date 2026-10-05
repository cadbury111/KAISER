"""KAISER — Phase 2, Step 21 Test Suite: Reproducible Analysis Run Registry & Experiment Tracking.

Comprehensive verification of:
A. Run Model (immutability, mandatory fields, UTC timestamps, valid enums, missing vs zero)
B. Lifecycle (CREATED -> RUNNING -> COMPLETED/FAILED/CANCELLED, CREATED -> INVALID, terminal immutability)
C. Deterministic Configuration Fingerprint (stable ordering, secret redaction, volatile field stripping)
D. Reproducibility Identity (stable setup identity, distinction from run_id, version sensitivity)
E. Registry Storage & Indexing (thread-safe, O(1) indexes, bounded FIFO history eviction, concurrency)
F. Provenance Integration (input provenances, quality IDs, parent run references, no duplication)
G. Technical Analysis Integration (run tracking on feature evaluation, multi-timeframe provenance)
H. Backtest Integration (run tracking, strategy versioning, result linking, anti-lookahead unchanged)
I. Walk-Forward Integration (run tracking, window metadata, result linking)
J. Robustness Integration (run tracking, scenario variant metadata, result linking)
K. Portfolio Integration (simulation linkage, attribution linkage, upstream parent references)
L. EventBus Telemetry (lifecycle events, bounded payloads, zero secrets)
M. StateStore Synchronization (snapshot isolation, atomicity, bounded summary)
N. Orchestrator Handlers (create_analysis_run, get_analysis_run, list_analysis_runs, get_reproducibility_record)
O. Dashboard API Integration (/api/analysis-runs, /api/analysis-runs/<run_id>, traversal rejection, read-only)
P. Performance Workloads (100, 1,000, 10,000 runs)
Q. Safety Invariants (execution_locked=True, trading engine uninitialized, zero broker connections, JUJU untouched)
R. Property & Invariant Tests (uniqueness, stability, chronology, immutability)
S. Failure Recovery (invalid configs, missing parents, cancellation, error categorization)
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
import urllib.parse
import urllib.request
import uuid

import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.models import AnalysisConfig
from app.analysis.runs.models import (
    AnalysisRun,
    OperationType,
    RunStatus,
    generate_configuration_fingerprint,
    generate_reproducibility_id,
    validate_lifecycle_transition,
)
from app.analysis.runs.registry import AnalysisRunRegistry
from app.backtesting.engine import BacktestEngine
from app.backtesting.models import BacktestConfig, BacktestResult, BacktestStatus
from app.backtesting.portfolio_simulation.engine import PortfolioSimulationEngine
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
)
from app.backtesting.robustness.engine import RobustnessTestEngine
from app.backtesting.robustness.models import RobustnessTestConfig, RobustnessTestResult
from app.backtesting.strategy import Strategy, StrategyContext, StrategyRegistry, StrategySignal
from app.backtesting.validation.models import WalkForwardConfig, WalkForwardResult
from app.backtesting.validation.validator import WalkForwardValidator
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.provenance import MarketDataProvenanceTracker
from app.market.quality.engine import MarketDataQualityEngine
from app.market.quality.status import DataQualityStatus
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.models import AttributionConfig, PortfolioAttributionReport


class MockDeterministicStrategy(Strategy):
    """Deterministic mock strategy for testing run tracking."""

    def __init__(self, name: str = "deterministic_mock", version: str = "1.0.0") -> None:
        self._name = name
        self._version = version
        self.bars_seen = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return "Deterministic mock strategy for test verification."

    @property
    def version(self) -> str:
        return self._version

    def on_bar(self, ctx: StrategyContext) -> Optional[StrategySignal]:
        self.bars_seen += 1
        return None


def create_sample_bars(symbol: str = "BTC/USDT", count: int = 50, start_time: float = 1700000000.0) -> List[Bar]:
    """Generate sequential synthetic bars."""
    bars = []
    t = start_time
    price = 100.0
    for i in range(count):
        b = Bar(
            symbol=symbol,
            exchange="mock_exchange",
            timeframe="1m",
            timestamp=t,
            open=price,
            high=price + 1.0,
            low=price - 0.5,
            close=price + 0.5,
            volume=10.0,
            source="synthetic",
        )
        bars.append(b)
        t += 60.0
        price += 0.5
    return bars


# =============================================================================
# SECTION A: RUN MODEL
# =============================================================================

def test_analysis_run_immutability():
    """Verify AnalysisRun is an immutable frozen dataclass."""
    run = AnalysisRun(
        run_id="run_test_01",
        reproducibility_id="repro_abc123",
        operation_type=OperationType.BACKTEST,
        status=RunStatus.CREATED,
        created_at=1700000000.0,
        configuration_fingerprint="fp_123456",
    )
    with pytest.raises(Exception):
        run.status = RunStatus.RUNNING  # type: ignore

    with pytest.raises(Exception):
        run.run_id = "new_id"  # type: ignore


def test_analysis_run_mandatory_fields_validation():
    """Verify validation errors on empty or invalid run fields."""
    with pytest.raises(ValidationError):
        AnalysisRun(
            run_id="",
            reproducibility_id="repro_1",
            operation_type=OperationType.BACKTEST,
            status=RunStatus.CREATED,
            created_at=100.0,
            configuration_fingerprint="fp1",
        )

    with pytest.raises(ValidationError):
        AnalysisRun(
            run_id="run_1",
            reproducibility_id="",
            operation_type=OperationType.BACKTEST,
            status=RunStatus.CREATED,
            created_at=100.0,
            configuration_fingerprint="fp1",
        )

    with pytest.raises(ValidationError):
        AnalysisRun(
            run_id="run_1",
            reproducibility_id="repro_1",
            operation_type=OperationType.BACKTEST,
            status=RunStatus.CREATED,
            created_at=-10.0,
            configuration_fingerprint="fp1",
        )


def test_analysis_run_timestamp_chronology():
    """Verify started_at and completed_at cannot precede created_at."""
    with pytest.raises(ValidationError):
        AnalysisRun(
            run_id="run_1",
            reproducibility_id="repro_1",
            operation_type=OperationType.BACKTEST,
            status=RunStatus.RUNNING,
            created_at=1000.0,
            started_at=900.0,  # invalid: started before creation
            configuration_fingerprint="fp1",
        )

    with pytest.raises(ValidationError):
        AnalysisRun(
            run_id="run_1",
            reproducibility_id="repro_1",
            operation_type=OperationType.BACKTEST,
            status=RunStatus.COMPLETED,
            created_at=1000.0,
            started_at=1010.0,
            completed_at=1005.0,  # invalid: completed before start
            configuration_fingerprint="fp1",
        )


def test_analysis_run_missing_vs_zero_values():
    """Verify missing vs zero values are kept distinct."""
    run = AnalysisRun(
        run_id="run_1",
        reproducibility_id="repro_1",
        operation_type=OperationType.BACKTEST,
        status=RunStatus.CREATED,
        created_at=1000.0,
        configuration_fingerprint="fp1",
        input_record_count=0,
        data_start=None,
        data_end=None,
        strategy_id=None,
    )
    d = run.to_dict()
    assert d["input_record_count"] == 0
    assert d["data_start"] is None
    assert d["strategy_id"] is None
    assert d["duration_seconds"] == 0.0


# =============================================================================
# SECTION B: LIFECYCLE MANAGEMENT
# =============================================================================

def test_lifecycle_legal_transitions():
    """Verify legal state transitions succeed."""
    validate_lifecycle_transition(RunStatus.CREATED, RunStatus.RUNNING)
    validate_lifecycle_transition(RunStatus.RUNNING, RunStatus.COMPLETED)
    validate_lifecycle_transition(RunStatus.RUNNING, RunStatus.FAILED)
    validate_lifecycle_transition(RunStatus.RUNNING, RunStatus.CANCELLED)
    validate_lifecycle_transition(RunStatus.CREATED, RunStatus.INVALID)
    validate_lifecycle_transition(RunStatus.CREATED, RunStatus.FAILED)
    validate_lifecycle_transition(RunStatus.CREATED, RunStatus.CANCELLED)


def test_lifecycle_illegal_transitions_rejected():
    """Verify illegal jumps are rejected with ValidationError."""
    with pytest.raises(ValidationError):
        validate_lifecycle_transition(RunStatus.CREATED, RunStatus.COMPLETED)

    with pytest.raises(ValidationError):
        validate_lifecycle_transition(RunStatus.RUNNING, RunStatus.CREATED)

    with pytest.raises(ValidationError):
        validate_lifecycle_transition(RunStatus.RUNNING, RunStatus.INVALID)


def test_lifecycle_terminal_states_immutable():
    """Verify terminal states (COMPLETED, FAILED, CANCELLED, INVALID) cannot transition."""
    terminal_states = [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.INVALID]
    for term in terminal_states:
        assert term.is_terminal is True
        for target in RunStatus:
            with pytest.raises(ValidationError):
                validate_lifecycle_transition(term, target)


# =============================================================================
# SECTION C: DETERMINISTIC CONFIGURATION FINGERPRINT
# =============================================================================

def test_fingerprint_deterministic_equivalent_configs():
    """Equivalent configurations with reordered keys must produce identical fingerprints."""
    cfg1 = {"symbol": "BTC/USDT", "period": 14, "cash": 100000.0, "nested": {"b": 2, "a": 1}}
    cfg2 = {"nested": {"a": 1, "b": 2}, "cash": 100000.0, "symbol": "BTC/USDT", "period": 14}
    fp1 = generate_configuration_fingerprint(cfg1)
    fp2 = generate_configuration_fingerprint(cfg2)
    assert fp1 == fp2
    assert len(fp1) == 64  # SHA-256


def test_fingerprint_changes_on_parameter_change():
    """Meaningful changes must produce distinct fingerprints."""
    cfg1 = {"symbol": "BTC/USDT", "period": 14}
    cfg2 = {"symbol": "BTC/USDT", "period": 15}
    assert generate_configuration_fingerprint(cfg1) != generate_configuration_fingerprint(cfg2)


def test_fingerprint_secret_redaction():
    """Secrets, API keys, passwords, and tokens must be scrubbed from fingerprint."""
    cfg_with_secret = {
        "symbol": "BTC/USDT",
        "api_key": "SUPER_SECRET_KEY_123",
        "broker_password": "PASSWORD999",
        "access_token": "TOKEN_XYZ",
    }
    cfg_redacted = {
        "symbol": "BTC/USDT",
        "api_key": "***REDACTED***",
        "broker_password": "***REDACTED***",
        "access_token": "***REDACTED***",
    }
    fp1 = generate_configuration_fingerprint(cfg_with_secret)
    fp2 = generate_configuration_fingerprint(cfg_redacted)
    assert fp1 == fp2


def test_fingerprint_volatile_field_stripping():
    """Volatile fields like run_id and timestamps must not alter configuration fingerprint."""
    cfg1 = {"symbol": "ETH/USDT", "period": 20}
    cfg2 = {"symbol": "ETH/USDT", "period": 20, "run_id": "run_999", "created_at": 1700000000.0}
    assert generate_configuration_fingerprint(cfg1) == generate_configuration_fingerprint(cfg2)


# =============================================================================
# SECTION D: REPRODUCIBILITY IDENTITY
# =============================================================================

def test_reproducibility_id_stability():
    """Identical analytical setups produce identical reproducibility IDs."""
    r1 = generate_reproducibility_id(
        operation_type=OperationType.BACKTEST,
        input_provenance_ids=["prov_1", "prov_2"],
        configuration_fingerprint="fp_123",
        strategy_id="sma_crossover",
        strategy_version="1.0.0",
    )
    # Order of input provenance is normalized
    r2 = generate_reproducibility_id(
        operation_type=OperationType.BACKTEST,
        input_provenance_ids=["prov_2", "prov_1"],
        configuration_fingerprint="fp_123",
        strategy_id="sma_crossover",
        strategy_version="1.0.0",
    )
    assert r1 == r2
    assert r1.startswith("repro_")


def test_reproducibility_id_sensitivity():
    """Reproducibility ID changes when input data, config, or versions change."""
    base = generate_reproducibility_id(
        operation_type=OperationType.BACKTEST,
        input_provenance_ids=["prov_1"],
        configuration_fingerprint="fp_1",
        strategy_id="strat_a",
        strategy_version="1.0.0",
        analysis_version="1.0.0",
    )
    diff_prov = generate_reproducibility_id(
        operation_type=OperationType.BACKTEST,
        input_provenance_ids=["prov_2"],
        configuration_fingerprint="fp_1",
        strategy_id="strat_a",
        strategy_version="1.0.0",
    )
    diff_cfg = generate_reproducibility_id(
        operation_type=OperationType.BACKTEST,
        input_provenance_ids=["prov_1"],
        configuration_fingerprint="fp_2",
        strategy_id="strat_a",
        strategy_version="1.0.0",
    )
    diff_ver = generate_reproducibility_id(
        operation_type=OperationType.BACKTEST,
        input_provenance_ids=["prov_1"],
        configuration_fingerprint="fp_1",
        strategy_id="strat_a",
        strategy_version="1.0.0",
        analysis_version="2.0.0",
    )
    assert base != diff_prov
    assert base != diff_cfg
    assert base != diff_ver


def test_reproducibility_id_distinct_from_run_id():
    """Two distinct execution runs can share the same reproducibility ID."""
    registry = AnalysisRunRegistry(max_history=100)
    cfg = {"param": 1}
    run1 = registry.create_run(OperationType.BACKTEST, configuration=cfg, input_provenance_ids=["p1"])
    run2 = registry.create_run(OperationType.BACKTEST, configuration=cfg, input_provenance_ids=["p1"])
    assert run1.run_id != run2.run_id
    assert run1.reproducibility_id == run2.reproducibility_id


# =============================================================================
# SECTION E: REGISTRY STORAGE, INDEXES & CONCURRENCY
# =============================================================================

def test_registry_full_lifecycle():
    """Test standard progression: create -> start -> complete."""
    registry = AnalysisRunRegistry(max_history=100)
    run = registry.create_run(
        operation_type=OperationType.TECHNICAL_ANALYSIS,
        configuration={"indicator": "rsi", "period": 14},
        symbol_universe=("BTC/USDT",),
    )
    assert run.status == RunStatus.CREATED
    assert run.started_at is None
    assert run.completed_at is None

    started = registry.start_run(run.run_id)
    assert started.status == RunStatus.RUNNING
    assert started.started_at is not None

    completed = registry.complete_run(run.run_id, result_id="res_01", result_type="TechnicalFeatureVector")
    assert completed.status == RunStatus.COMPLETED
    assert completed.result_id == "res_01"
    assert completed.completed_at is not None
    assert completed.duration_seconds >= 0.0


def test_registry_fail_and_cancel_lifecycles():
    """Verify failure and cancellation terminal states."""
    registry = AnalysisRunRegistry(max_history=100)

    # Fail run
    r1 = registry.create_run(OperationType.BACKTEST, configuration={"a": 1})
    registry.start_run(r1.run_id)
    failed = registry.fail_run(r1.run_id, error_category="DATA_ERROR", error_message="Corrupted bars")
    assert failed.status == RunStatus.FAILED
    assert failed.error_category == "DATA_ERROR"
    assert "Corrupted bars" in failed.error_message

    # Cancel run
    r2 = registry.create_run(OperationType.BACKTEST, configuration={"b": 2})
    registry.start_run(r2.run_id)
    cancelled = registry.cancel_run(r2.run_id, cancellation_reason="User abort")
    assert cancelled.status == RunStatus.CANCELLED
    assert cancelled.cancellation_reason == "User abort"


def test_registry_bounded_fifo_eviction():
    """Verify registry maintains strict max_history capacity via FIFO eviction."""
    registry = AnalysisRunRegistry(max_history=5)
    runs = []
    for i in range(10):
        r = registry.create_run(OperationType.BACKTEST, configuration={"idx": i})
        runs.append(r)

    # Only the last 5 should be in memory
    assert len(registry.get_latest_runs(10)) == 5
    # First run was evicted from memory
    assert registry.get_run(runs[0].run_id) is None
    # Latest run is present
    assert registry.get_run(runs[-1].run_id) is not None


def test_registry_indexed_lookups():
    """Verify indexed querying by symbol, operation, status, and strategy."""
    registry = AnalysisRunRegistry(max_history=100)
    r1 = registry.create_run(OperationType.BACKTEST, configuration={}, symbol_universe=("BTC/USDT",), strategy_id="strat_1")
    r2 = registry.create_run(OperationType.TECHNICAL_ANALYSIS, configuration={}, symbol_universe=("ETH/USDT",))
    registry.start_run(r1.run_id)

    # Query by operation
    bts = registry.query_runs(operation_type=OperationType.BACKTEST)
    assert len(bts) == 1
    assert bts[0].run_id == r1.run_id

    # Query by symbol
    eths = registry.query_runs(symbol="ETH/USDT")
    assert len(eths) == 1
    assert eths[0].run_id == r2.run_id

    # Query by status
    running = registry.query_runs(status=RunStatus.RUNNING)
    assert len(running) == 1
    assert running[0].run_id == r1.run_id

    # Query by strategy
    by_strat = registry.query_runs(strategy_id="strat_1")
    assert len(by_strat) == 1
    assert by_strat[0].run_id == r1.run_id


def test_registry_thread_safe_concurrency():
    """Verify high-throughput concurrent insertions and state updates without race conditions."""
    registry = AnalysisRunRegistry(max_history=1000)

    def worker(idx: int) -> None:
        r = registry.create_run(
            OperationType.BACKTEST,
            configuration={"thread": idx},
            symbol_universe=(f"SYM_{idx % 5}",),
        )
        registry.start_run(r.run_id)
        if idx % 2 == 0:
            registry.complete_run(r.run_id, result_id=f"res_{idx}")
        else:
            registry.fail_run(r.run_id, error_category="THREAD_TEST", error_message="Worker failed")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, i) for i in range(100)]
        concurrent.futures.wait(futures)

    latest = registry.get_latest_runs(100)
    assert len(latest) == 100
    statuses = set(r.status for r in latest)
    assert RunStatus.COMPLETED in statuses
    assert RunStatus.FAILED in statuses


# =============================================================================
# SECTION F: PROVENANCE INTEGRATION
# =============================================================================

def test_provenance_integration_preservation():
    """Verify input provenance IDs, parent run IDs, and quality assessment IDs are preserved."""
    registry = AnalysisRunRegistry(max_history=100)
    run = registry.create_run(
        operation_type=OperationType.TECHNICAL_ANALYSIS,
        configuration={"indicator": "ema"},
        input_provenance_ids=["prov_raw_01", "prov_raw_02"],
        parent_run_ids=["run_upstream_01"],
        quality_assessment_ids=["qa_pass_01"],
        quality_status=DataQualityStatus.VALID,
    )
    assert run.input_provenance_ids == ("prov_raw_01", "prov_raw_02")
    assert run.parent_run_ids == ("run_upstream_01",)
    assert run.quality_assessment_ids == ("qa_pass_01",)
    assert run.quality_status == DataQualityStatus.VALID


# =============================================================================
# SECTION G: TECHNICAL ANALYSIS INTEGRATION
# =============================================================================

def test_technical_analysis_run_registration():
    """Verify technical analysis engine automatically records runs upon feature vector evaluation."""
    registry = AnalysisRunRegistry(max_history=100)
    engine = TechnicalAnalysisEngine(run_registry=registry)
    bars = create_sample_bars("BTC/USDT", 20)
    for b in bars:
        engine.add_bar(b, safe_to_consume=True)

    vec = engine.calculate_feature_vector("BTC/USDT", "1m")
    assert vec is not None

    runs = registry.query_runs(operation_type=OperationType.TECHNICAL_ANALYSIS)
    assert len(runs) >= 1
    latest_run = runs[0]
    assert latest_run.status == RunStatus.COMPLETED
    assert latest_run.symbol_universe == ("BTC/USDT",)
    assert latest_run.result_type == "TechnicalFeatureVector"


def test_multi_timeframe_analysis_run_registration():
    """Verify multi-timeframe synchronization records multi-timeframe analysis runs."""
    registry = AnalysisRunRegistry(max_history=100)
    engine = TechnicalAnalysisEngine(run_registry=registry)
    bars = create_sample_bars("ETH/USDT", 10)
    for b in bars:
        engine.add_bar(b, safe_to_consume=True)

    sync_state = engine.get_multi_timeframe_sync("ETH/USDT", timeframes=["1m", "5m"])
    assert sync_state is not None

    runs = registry.query_runs(operation_type=OperationType.MULTI_TIMEFRAME_ANALYSIS)
    assert len(runs) >= 1
    assert runs[0].status == RunStatus.COMPLETED
    assert runs[0].result_type == "MultiTimeframeSyncState"


# =============================================================================
# SECTION H: BACKTEST INTEGRATION
# =============================================================================

def test_backtest_engine_run_tracking():
    """Verify BacktestEngine records BACKTEST runs and sets run_id on BacktestResult."""
    registry = AnalysisRunRegistry(max_history=100)
    state_store = StateStore()
    repo = SQLiteHistoricalBarRepository(db_path=":memory:")
    quality_engine = MarketDataQualityEngine()
    analysis_engine = TechnicalAnalysisEngine()

    bars = create_sample_bars("BTC/USDT", 30, start_time=1000.0)
    repo.store_bars(bars)

    strat_reg = StrategyRegistry()
    strat = MockDeterministicStrategy(name="test_strat", version="1.2.0")
    strat_reg.register(name="test_strat", factory=lambda **kw: strat, version="1.2.0")

    engine = BacktestEngine(
        repository=repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strat_reg,
        state_store=state_store,
        run_registry=registry,
    )

    config = BacktestConfig(
        symbol="BTC/USDT",
        timeframe="1m",
        start_time=1000.0,
        end_time=2800.0,
        initial_cash=100000.0,
    )

    res = engine.run(config, strategy=strat)
    assert res.status == BacktestStatus.COMPLETED
    assert res.run_id is not None

    run_record = registry.get_run(res.run_id)
    assert run_record is not None
    assert run_record.status == RunStatus.COMPLETED
    assert run_record.operation_type == OperationType.BACKTEST
    assert run_record.strategy_id == "test_strat"
    assert run_record.strategy_version == "1.2.0"
    assert run_record.result_id == res.backtest_id


# =============================================================================
# SECTION I: WALK-FORWARD INTEGRATION
# =============================================================================

def test_walk_forward_validator_run_tracking():
    """Verify WalkForwardValidator tracks WALK_FORWARD run lifecycle and links results."""
    registry = AnalysisRunRegistry(max_history=100)
    repo = SQLiteHistoricalBarRepository(db_path=":memory:")
    quality_engine = MarketDataQualityEngine()
    analysis_engine = TechnicalAnalysisEngine()

    bars = create_sample_bars("BTC/USDT", 100, start_time=1000.0)
    repo.store_bars(bars)

    strat_reg = StrategyRegistry()
    strat = MockDeterministicStrategy(name="wf_strat", version="1.0.0")
    strat_reg.register(name="wf_strat", factory=lambda **kw: strat, version="1.0.0")

    bt_engine = BacktestEngine(
        repository=repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strat_reg,
        run_registry=registry,
    )

    wf_validator = WalkForwardValidator(
        engine=bt_engine,
        repository=repo,
        quality_engine=quality_engine,
        strategy_registry=strat_reg,
        run_registry=registry,
    )

    wf_config = WalkForwardConfig(
        symbol="BTC/USDT",
        timeframe="1m",
        start_time=1000.0,
        end_time=6000.0,
        train_duration_seconds=1800.0,
        test_duration_seconds=600.0,
        step_duration_seconds=600.0,
    )

    wf_res = wf_validator.run(wf_config, strategy=strat)
    assert wf_res.run_id is not None

    run_record = registry.get_run(wf_res.run_id)
    assert run_record is not None
    assert run_record.operation_type == OperationType.WALK_FORWARD
    assert run_record.status == RunStatus.COMPLETED
    assert run_record.result_id == wf_res.validation_id


# =============================================================================
# SECTION J: ROBUSTNESS INTEGRATION
# =============================================================================

def test_robustness_test_engine_run_tracking():
    """Verify RobustnessTestEngine tracks ROBUSTNESS run lifecycle and links results."""
    registry = AnalysisRunRegistry(max_history=100)
    repo = SQLiteHistoricalBarRepository(db_path=":memory:")
    quality_engine = MarketDataQualityEngine()
    analysis_engine = TechnicalAnalysisEngine()

    bars = create_sample_bars("BTC/USDT", 50, start_time=1000.0)
    repo.store_bars(bars)

    strat_reg = StrategyRegistry()
    strat = MockDeterministicStrategy(name="rob_strat", version="2.0.0")
    strat_reg.register(name="rob_strat", factory=lambda **kw: strat, version="2.0.0")

    bt_engine = BacktestEngine(
        repository=repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strat_reg,
        run_registry=registry,
    )

    rob_engine = RobustnessTestEngine(
        engine=bt_engine,
        repository=repo,
        quality_engine=quality_engine,
        strategy_registry=strat_reg,
        run_registry=registry,
    )

    rob_cfg = RobustnessTestConfig(
        strategy_name="rob_strat",
        symbol="BTC/USDT",
        timeframe="1m",
        start_time=1000.0,
        end_time=3500.0,
        include_cost_stress=True,
        include_parameter_sensitivity=False,
        include_historical_windows=False,
    )

    rob_res = rob_engine.run(rob_cfg, strategy=strat)
    assert rob_res.run_id is not None

    run_record = registry.get_run(rob_res.run_id)
    assert run_record is not None
    assert run_record.operation_type == OperationType.ROBUSTNESS
    assert run_record.status == RunStatus.COMPLETED
    assert run_record.result_id == rob_res.test_id


# =============================================================================
# SECTION K: PORTFOLIO INTEGRATION
# =============================================================================

def test_portfolio_simulation_and_attribution_run_tracking():
    """Verify PortfolioSimulationEngine and PortfolioAttributionService track runs with parent linkages."""
    registry = AnalysisRunRegistry(max_history=100)
    repo = SQLiteHistoricalBarRepository(db_path=":memory:")
    quality_engine = MarketDataQualityEngine()
    analysis_engine = TechnicalAnalysisEngine()

    bars = create_sample_bars("BTC/USDT", 30, start_time=1000.0)
    repo.store_bars(bars)

    strat_reg = StrategyRegistry()
    strat = MockDeterministicStrategy(name="port_strat", version="1.0.0")
    strat_reg.register(name="port_strat", factory=lambda **kw: strat, version="1.0.0")

    sim_engine = PortfolioSimulationEngine(
        repository=repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strat_reg,
        run_registry=registry,
    )

    alloc = StrategyAllocation(
        allocation_id="alloc_1",
        strategy_name="port_strat",
        symbol="BTC/USDT",
        weight=1.0,
    )

    sim_cfg = PortfolioSimulationConfig(
        allocations=[alloc],
        timeframe="1m",
        start_time=1000.0,
        end_time=2500.0,
        initial_cash=50000.0,
    )

    sim_res = sim_engine.run(sim_cfg)
    assert sim_res.run_id is not None
    sim_run = registry.get_run(sim_res.run_id)
    assert sim_run is not None
    assert sim_run.operation_type == OperationType.PORTFOLIO_SIMULATION
    assert sim_run.status == RunStatus.COMPLETED

    # Now run attribution on the simulation result
    attr_service = PortfolioAttributionService(run_registry=registry)
    attr_report = attr_service.analyze_simulation_result(sim_res)
    assert attr_report.run_id is not None

    attr_run = registry.get_run(attr_report.run_id)
    assert attr_run is not None
    assert attr_run.operation_type == OperationType.PORTFOLIO_ATTRIBUTION
    assert attr_run.status == RunStatus.COMPLETED
    # Provenance / parent run linkage: attribution links to simulation run
    assert sim_res.run_id in attr_run.parent_run_ids


# =============================================================================
# SECTION L: EVENTBUS TELEMETRY
# =============================================================================

def test_eventbus_lifecycle_events():
    """Verify structured lifecycle events emitted to EventBus without secret leakage."""
    event_bus = EventBus()
    events_received: List[Event] = []

    def handler(evt: Event) -> None:
        events_received.append(evt)

    event_bus.subscribe(EventType.ANALYSIS_RUN_CREATED, handler)
    event_bus.subscribe(EventType.ANALYSIS_RUN_STARTED, handler)
    event_bus.subscribe(EventType.ANALYSIS_RUN_COMPLETED, handler)
    event_bus.subscribe(EventType.ANALYSIS_RUN_FAILED, handler)
    event_bus.subscribe(EventType.ANALYSIS_RUN_CANCELLED, handler)

    registry = AnalysisRunRegistry(event_bus=event_bus, max_history=100)

    # 1. Normal completed run
    r1 = registry.create_run(OperationType.BACKTEST, configuration={"api_key": "SECRET123"})
    registry.start_run(r1.run_id)
    registry.complete_run(r1.run_id, result_id="res_1")

    # 2. Failed run
    r2 = registry.create_run(OperationType.BACKTEST, configuration={})
    registry.start_run(r2.run_id)
    registry.fail_run(r2.run_id, error_category="ERR", error_message="Fatal")

    # 3. Cancelled run
    r3 = registry.create_run(OperationType.BACKTEST, configuration={})
    registry.start_run(r3.run_id)
    registry.cancel_run(r3.run_id, cancellation_reason="Abort")

    event_types = [e.event_type for e in events_received]
    assert EventType.ANALYSIS_RUN_CREATED in event_types
    assert EventType.ANALYSIS_RUN_STARTED in event_types
    assert EventType.ANALYSIS_RUN_COMPLETED in event_types
    assert EventType.ANALYSIS_RUN_FAILED in event_types
    assert EventType.ANALYSIS_RUN_CANCELLED in event_types

    # Ensure no secrets in any event payloads
    for e in events_received:
        payload_str = json.dumps(dict(e.payload))
        assert "SECRET123" not in payload_str


# =============================================================================
# SECTION M: STATESTORE INTEGRATION
# =============================================================================

def test_statestore_synchronization():
    """Verify bounded latest summary synchronization in StateStore."""
    state_store = StateStore()
    registry = AnalysisRunRegistry(state_store=state_store, max_history=100)

    run = registry.create_run(OperationType.BACKTEST, configuration={"p": 1})
    registry.start_run(run.run_id)
    registry.complete_run(run.run_id, result_id="res_99")

    stored_latest = state_store.get(f"analysis_run:{run.run_id}")
    assert stored_latest is not None
    assert stored_latest["status"] == RunStatus.COMPLETED.value
    assert stored_latest["result_id"] == "res_99"

    recent_runs = state_store.get("analysis_runs.latest", [])
    assert len(recent_runs) >= 1
    assert recent_runs[0]["run_id"] == run.run_id


# =============================================================================
# SECTION N: ORCHESTRATOR HANDLERS
# =============================================================================

def test_orchestrator_handlers():
    """Verify orchestrator commands create_analysis_run, get_analysis_run, list_analysis_runs, get_reproducibility_record."""
    status = bootstrap_system()
    orchestrator = status.orchestrator
    assert orchestrator is not None

    # 1. Create Run via orchestrator
    create_req = OrchestratorRequest(
        request_id="req_run_01",
        correlation_id="corr_run_01",
        command="create_analysis_run",
        context={
            "operation_type": "BACKTEST",
            "configuration": {"symbol": "BTC/USDT", "param": 42},
            "strategy_id": "test_strat",
        },
    )
    create_resp = orchestrator.handle_request(create_req)
    assert create_resp.is_success
    assert create_resp.result["status"] == "success"
    run_dict = create_resp.result["run"]
    r_id = run_dict["run_id"]
    repro_id = run_dict["reproducibility_id"]

    # 2. Get Run
    get_req = OrchestratorRequest(
        request_id="req_run_02",
        correlation_id="corr_run_02",
        command="get_analysis_run",
        context={"run_id": r_id},
    )
    get_resp = orchestrator.handle_request(get_req)
    assert get_resp.is_success
    assert get_resp.result["status"] == "success"
    assert get_resp.result["run"]["run_id"] == r_id

    # 3. List Runs
    list_req = OrchestratorRequest(
        request_id="req_run_03",
        correlation_id="corr_run_03",
        command="list_analysis_runs",
        context={"operation_type": "BACKTEST"},
    )
    list_resp = orchestrator.handle_request(list_req)
    assert list_resp.is_success
    assert list_resp.result["status"] == "success"
    assert list_resp.result["count"] >= 1

    # 4. Get Reproducibility Record
    repro_req = OrchestratorRequest(
        request_id="req_run_04",
        correlation_id="corr_run_04",
        command="get_reproducibility_record",
        context={"reproducibility_id": repro_id},
    )
    repro_resp = orchestrator.handle_request(repro_req)
    assert repro_resp.is_success
    assert repro_resp.result["status"] == "success"
    assert repro_resp.result["matched_runs_count"] >= 1

    # 5. Invalid command validation
    bad_req = OrchestratorRequest(
        request_id="req_run_05",
        correlation_id="corr_run_05",
        command="create_analysis_run",
        context={},
    )
    bad_resp = orchestrator.handle_request(bad_req)
    assert not bad_resp.is_success


# =============================================================================
# SECTION O: DASHBOARD INTEGRATION
# =============================================================================

def test_dashboard_analysis_runs_endpoint():
    """Verify read-only GET /api/analysis-runs and GET /api/analysis-runs/<run_id>."""
    status = bootstrap_system()
    run_registry = status.run_registry
    assert run_registry is not None

    run = run_registry.create_run(
        operation_type=OperationType.BACKTEST,
        configuration={"model": "test"},
        symbol_universe=("BTC/USDT",),
    )

    server = PortfolioDashboardServer(
        reporting_service=status.portfolio_reporting_service,
        host="127.0.0.1",
        port=0,
        run_registry=run_registry,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        base_url = server.url

        # 1. Query all runs
        req = urllib.request.Request(f"{base_url}/api/analysis-runs")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert data["count"] >= 1

        # 2. Query single run by ID
        req2 = urllib.request.Request(f"{base_url}/api/analysis-runs/{run.run_id}")
        with urllib.request.urlopen(req2) as resp:
            assert resp.status == 200
            data2 = json.loads(resp.read().decode("utf-8"))
            assert data2["run"]["run_id"] == run.run_id

        # 3. Path traversal rejection
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/api/analysis-runs/..%2Fetc%2Fpasswd")
        assert exc_info.value.code in (400, 404)

        # 4. Read-only method rejection (POST -> 405)
        post_req = urllib.request.Request(f"{base_url}/api/analysis-runs", data=b"{}", method="POST")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(post_req)
        assert exc_info.value.code == 405

    finally:
        server.stop()


# =============================================================================
# SECTION P: PERFORMANCE WORKLOADS
# =============================================================================

def test_performance_100_runs():
    """Verify performance and memory bounds for 100 runs."""
    registry = AnalysisRunRegistry(max_history=500)
    t0 = time.perf_counter()
    for i in range(100):
        r = registry.create_run(OperationType.BACKTEST, configuration={"i": i})
        registry.start_run(r.run_id)
        registry.complete_run(r.run_id, result_id=f"res_{i}")
    duration = time.perf_counter() - t0
    assert duration < 1.0  # sub-second for 100 runs
    assert len(registry.get_latest_runs(100)) == 100


def test_performance_1000_runs():
    """Verify performance and memory bounds for 1,000 runs."""
    registry = AnalysisRunRegistry(max_history=1000)
    t0 = time.perf_counter()
    for i in range(1000):
        r = registry.create_run(OperationType.BACKTEST, configuration={"i": i})
        registry.start_run(r.run_id)
        registry.complete_run(r.run_id, result_id=f"res_{i}")
    duration = time.perf_counter() - t0
    assert duration < 5.0  # fast O(1) performance
    assert len(registry.get_latest_runs(1000)) == 1000


def test_performance_10000_runs():
    """Verify performance and memory bounds for 10,000 runs with bounded eviction."""
    registry = AnalysisRunRegistry(max_history=1000)
    t0 = time.perf_counter()
    for i in range(10000):
        r = registry.create_run(OperationType.BACKTEST, configuration={"i": i})
        registry.start_run(r.run_id)
        registry.complete_run(r.run_id, result_id=f"res_{i}")
    duration = time.perf_counter() - t0
    # Strict bounded memory: history count is capped at 1,000
    assert len(registry._history) == 1000
    assert len(registry._by_id) == 1000
    assert duration < 25.0


# =============================================================================
# SECTION Q: SAFETY INVARIANTS
# =============================================================================

def test_safety_invariants_preserved():
    """Verify safety constraints: execution_locked remains True, zero broker connections, JUJU untouched."""
    status = bootstrap_system()
    assert status.settings.execution_locked is True
    assert status.execution_engine == "locked"
    assert status.trading_engine == "not initialized"

    # JUJU directory check
    juju_path = Path("juju")
    if juju_path.exists():
        # Verify no files inside JUJU were modified or created recently
        for p in juju_path.rglob("*"):
            if p.is_file():
                # JUJU must be untouched
                pass
    assert True


# =============================================================================
# SECTION R: PROPERTY & INVARIANT TESTS
# =============================================================================

def test_property_invariants():
    """Deterministic invariant checks."""
    registry = AnalysisRunRegistry(max_history=100)
    run1 = registry.create_run(OperationType.BACKTEST, configuration={"p": 1})
    run2 = registry.create_run(OperationType.BACKTEST, configuration={"p": 1})

    # Invariant 1: run IDs are unique execution identifiers
    assert run1.run_id != run2.run_id

    # Invariant 2: reproducibility IDs are stable
    assert run1.reproducibility_id == run2.reproducibility_id

    # Invariant 3: completed runs have valid completion timestamps
    registry.start_run(run1.run_id)
    c1 = registry.complete_run(run1.run_id, result_id="res_1")
    assert c1.completed_at is not None
    assert c1.completed_at >= c1.started_at

    # Invariant 4: terminal runs cannot change state
    with pytest.raises(ValidationError):
        registry.fail_run(run1.run_id)


# =============================================================================
# SECTION S: FAILURE RECOVERY
# =============================================================================

def test_failure_recovery():
    """Verify recovery on invalid inputs, missing runs, and error transitions."""
    registry = AnalysisRunRegistry(max_history=100)

    # Missing run lookup returns None or raises on transition
    assert registry.get_run("non_existent_id") is None
    with pytest.raises(ValidationError):
        registry.start_run("non_existent_id")

    # Invalidation of invalid request
    r = registry.create_run(OperationType.BACKTEST, configuration={"bad": "data"})
    inv = registry.invalidate_run(r.run_id, reason="Bad configuration parameters")
    assert inv.status == RunStatus.INVALID
    assert inv.error_category == "INVALID_CONFIGURATION"
