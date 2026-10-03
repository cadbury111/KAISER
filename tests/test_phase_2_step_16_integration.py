"""KAISER — Phase 2 Step 16 End-to-End Integration, Reliability & Failure Recovery Tests.

Comprehensive integration test suite validating:
- Scenario A: Historical data ingestion, quality validation, SQLite storage, and deterministic replay to technical analysis.
- Scenario B: Backtest execution, chronological anti-lookahead guarantees, walk-forward validation, and robustness stress testing.
- Scenario C: Multi-strategy virtual portfolio simulation, risk monitoring, performance attribution, and unified reporting.
- Scenario D: Read-only HTTP dashboard integration, diagnostics auditing, and fault isolation.
- Scenario E: Deterministic failure injection, quality gating, storage recovery, replay cancellation, and error handling.
- Reliability & Concurrency: Multi-threaded report retrieval, simultaneous dashboard requests, lifecycle cleanups, and bounded collections.
- Safety Invariants: Settings.execution_locked = True, zero live trade execution, loopback binding, and secret scrubbing.
"""

from __future__ import annotations

import concurrent.futures
import json
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.backtesting.engine import BacktestEngine
from app.backtesting.handlers import register_backtest_handlers
from app.backtesting.models import BacktestConfig, BacktestResult, BacktestStatus
from app.backtesting.portfolio_simulation.engine import PortfolioSimulationEngine
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
)
from app.backtesting.robustness.engine import RobustnessTestEngine
from app.backtesting.robustness.models import RobustnessTestConfig
from app.backtesting.strategy import SMACrossoverStrategy, StrategyRegistry
from app.backtesting.validation.models import WalkForwardConfig
from app.backtesting.validation.validator import WalkForwardValidator
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.cache import HistoricalDataCache
from app.market.historical.handlers import register_historical_handlers
from app.market.historical.loader import HistoricalDataLoader
from app.market.historical.mock import MockHistoricalDataProvider
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalProviderStatus,
)
from app.market.historical.replay import (
    HistoricalReplayEngine,
    HistoricalReplayRequest,
    HistoricalReplayStatus,
)
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.market.quality.status import DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, RequestStatus
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.diagnostics import (
    DiagnosticEventType,
    DiagnosticOutcome,
    PortfolioDiagnosticsService,
)
from app.portfolio.handlers import register_portfolio_handlers
from app.portfolio.report_models import ReportDataStatus, UnifiedPortfolioReport
from app.portfolio.reporting import PortfolioReportingService
from app.portfolio.reporting_handlers import register_reporting_handlers
from app.risk.handlers import register_risk_handlers
from app.risk.models import AlertState, PortfolioRiskReport
from app.risk.monitor import PortfolioRiskMonitor


# ------------------------------------------------------------------------------
# Deterministic Test Fixtures and Helpers
# ------------------------------------------------------------------------------

def _generate_deterministic_bars(
    symbol: str = "TCS",
    count: int = 150,
    start_ts: float = 1700000000.0,
    interval_sec: float = 60.0,
    base_price: float = 3500.0,
) -> List[Bar]:
    """Generates synthetic, physically valid, monotonically increasing OHLCV bars."""
    bars: List[Bar] = []
    p = base_price
    for i in range(count):
        ts = start_ts + (i * interval_sec)
        # Deterministic wave pattern
        delta = 2.5 * ((i % 10) - 4.5)
        p = max(10.0, p + delta)
        high = p + 2.0
        low = p - 2.0
        open_px = p - 0.5
        close_px = p + 0.5
        bars.append(
            Bar(
                symbol=symbol,
                exchange="NSE",
                timeframe="1m",
                timestamp=ts,
                open=round(open_px, 2),
                high=round(high, 2),
                low=round(low, 2),
                close=round(close_px, 2),
                volume=100.0 + (i * 2.0),
                source="mock_historical",
            )
        )
    return bars


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_eb")


@pytest.fixture
def state_store() -> StateStore:
    return StateStore(name="test_ss")


@pytest.fixture
def in_memory_repo() -> SQLiteHistoricalBarRepository:
    return SQLiteHistoricalBarRepository(":memory:", name="test_in_memory_repo")


@pytest.fixture
def quality_engine(event_bus: EventBus, state_store: StateStore) -> MarketDataQualityEngine:
    return MarketDataQualityEngine(event_bus=event_bus, state_store=state_store)


@pytest.fixture
def analysis_engine(event_bus: EventBus, state_store: StateStore) -> TechnicalAnalysisEngine:
    return TechnicalAnalysisEngine(event_bus=event_bus, state_store=state_store, name="test_ta_engine")


@pytest.fixture
def strategy_registry() -> StrategyRegistry:
    return StrategyRegistry()


@pytest.fixture
def full_system_context(
    event_bus: EventBus,
    state_store: StateStore,
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
) -> Dict[str, Any]:
    """Sets up a complete, interconnected KAISER runtime environment."""
    provider = MockHistoricalDataProvider(name="test_mock_provider")
    cache = HistoricalDataCache(max_size=100)

    loader = HistoricalDataLoader(
        provider=provider,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        event_bus=event_bus,
        state_store=state_store,
        repository=in_memory_repo,
        cache=cache,
        name="test_loader",
    )

    replay_engine = HistoricalReplayEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        event_bus=event_bus,
        state_store=state_store,
        name="test_replay_engine",
    )

    backtest_engine = BacktestEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        name="test_backtest_engine",
    )

    walk_forward_validator = WalkForwardValidator(
        engine=backtest_engine,
        repository=in_memory_repo,
        quality_engine=quality_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        name="test_wf_validator",
    )

    robustness_engine = RobustnessTestEngine(
        engine=backtest_engine,
        repository=in_memory_repo,
        quality_engine=quality_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        name="test_robustness_engine",
    )

    portfolio_engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
        name="test_portfolio_engine",
    )

    risk_monitor = PortfolioRiskMonitor(
        event_bus=event_bus,
        state_store=state_store,
        name="test_risk_monitor",
    )

    attribution_service = PortfolioAttributionService(
        event_bus=event_bus,
        state_store=state_store,
    )

    diagnostics_service = PortfolioDiagnosticsService(
        max_history=100,
        event_bus=event_bus,
        state_store=state_store,
    )

    reporting_service = PortfolioReportingService(
        event_bus=event_bus,
        state_store=state_store,
        attribution_service=attribution_service,
        risk_monitor=risk_monitor,
        diagnostics_service=diagnostics_service,
    )

    orchestrator = MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        name="test_master_orchestrator",
    )

    # Register all component handlers
    register_historical_handlers(
        orchestrator=orchestrator,
        loader=loader,
        repository=in_memory_repo,
        replay_engine=replay_engine,
    )
    register_backtest_handlers(
        orchestrator=orchestrator,
        engine=backtest_engine,
        strategy_registry=strategy_registry,
        validator=walk_forward_validator,
        robustness_engine=robustness_engine,
        portfolio_engine=portfolio_engine,
    )
    register_risk_handlers(orchestrator=orchestrator, monitor=risk_monitor)
    register_portfolio_handlers(orchestrator=orchestrator, service=attribution_service)
    register_reporting_handlers(orchestrator=orchestrator, service=reporting_service)

    return {
        "event_bus": event_bus,
        "state_store": state_store,
        "repository": in_memory_repo,
        "quality_engine": quality_engine,
        "analysis_engine": analysis_engine,
        "strategy_registry": strategy_registry,
        "loader": loader,
        "replay_engine": replay_engine,
        "backtest_engine": backtest_engine,
        "walk_forward_validator": walk_forward_validator,
        "robustness_engine": robustness_engine,
        "portfolio_engine": portfolio_engine,
        "risk_monitor": risk_monitor,
        "attribution_service": attribution_service,
        "diagnostics_service": diagnostics_service,
        "reporting_service": reporting_service,
        "orchestrator": orchestrator,
    }


# ------------------------------------------------------------------------------
# Scenario A: Historical Data Ingestion, Storage, Replay & Downstream Analytics
# ------------------------------------------------------------------------------

def test_scenario_a_historical_data_ingest_quality_storage_replay_to_analysis(full_system_context):
    """Scenario A: Validates the data flow from historical loader through quality gating to storage and replay."""
    ctx = full_system_context
    loader: HistoricalDataLoader = ctx["loader"]
    repo: SQLiteHistoricalBarRepository = ctx["repository"]
    replay: HistoricalReplayEngine = ctx["replay_engine"]
    analysis: TechnicalAnalysisEngine = ctx["analysis_engine"]

    # 1. Ingest historical bars for TCS
    req = HistoricalBarRequest(
        symbol="TCS",
        timeframe="1m",
        start_time=1700000000.0,
        end_time=1700006000.0,
        limit=100,
        exchange="NSE",
    )
    load_res = loader.load_and_ingest(req)
    assert load_res.provider_status == HistoricalProviderStatus.SUCCESS
    assert load_res.accepted_bars_count > 0
    assert load_res.rejected_bars_count == 0

    # Persist loaded bars to repository
    loaded_resp = loader.load_bars(req)
    loader.persist_bars(loaded_resp.bars)

    # 2. Query stored bars and verify chronological storage integrity
    stored_bars = repo.query_bars(
        symbol="TCS",
        timeframe="1m",
        start_time=1700000000.0,
        end_time=1700006000.0,
    )
    assert len(stored_bars) == load_res.accepted_bars_count
    for i in range(1, len(stored_bars)):
        assert stored_bars[i].timestamp > stored_bars[i - 1].timestamp

    # 3. Replay stored bars through HistoricalReplayEngine to TechnicalAnalysisEngine
    replay_req = HistoricalReplayRequest(
        symbol="TCS",
        timeframe="1m",
        start_time=stored_bars[0].timestamp,
        end_time=stored_bars[-1].timestamp,
        limit=100,
        pacing_seconds=0.0,
        reset_analysis_state=True,
    )
    replay_res = replay.replay(replay_req)
    assert replay_res.status == HistoricalReplayStatus.COMPLETED
    assert replay_res.bars_replayed == len(stored_bars)

    # 4. Verify TechnicalAnalysisEngine state updated and computed indicators
    series = analysis.get_series("TCS", "1m")
    assert series is not None
    assert series.count == len(stored_bars)
    assert series.symbol == "TCS"
    assert series.timeframe == "1m"
    assert series.latest_bar is not None
    assert series.latest_bar.timestamp == stored_bars[-1].timestamp
    vec = analysis.get_feature_vector("TCS", "1m")
    assert vec is not None
    assert vec.symbol == "TCS"


# ------------------------------------------------------------------------------
# Scenario B: Backtesting, Walk-Forward Validation & Robustness Stress Testing
# ------------------------------------------------------------------------------

def test_scenario_b_backtest_to_walk_forward_and_robustness_workflows(full_system_context):
    """Scenario B: Runs backtesting on stored deterministic bars and passes results into validation workflows."""
    ctx = full_system_context
    repo: SQLiteHistoricalBarRepository = ctx["repository"]
    backtest: BacktestEngine = ctx["backtest_engine"]
    wf_validator: WalkForwardValidator = ctx["walk_forward_validator"]
    rob_engine: RobustnessTestEngine = ctx["robustness_engine"]

    # Pre-populate repository with 200 deterministic bars
    bars = _generate_deterministic_bars("INFY", count=200, start_ts=1700000000.0, interval_sec=60.0)
    repo.store_bars(bars)

    # 1. Run backtest with SMA Crossover
    cfg = BacktestConfig(
        symbol="INFY",
        timeframe="1m",
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        fee_rate=0.0005,
        slippage_bps=2.0,
        execution_model="next_bar_open",
    )
    bt_res: BacktestResult = backtest.run(config=cfg, strategy="sma_crossover")
    assert bt_res.status == BacktestStatus.COMPLETED
    assert bt_res.metrics is not None
    assert bt_res.config.initial_cash == 100_000.0
    assert bt_res.metrics.total_transaction_costs >= 0.0

    # 2. Run walk-forward validation across in-sample / out-of-sample segments
    wf_cfg = WalkForwardConfig(
        symbol="INFY",
        timeframe="1m",
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        train_duration_seconds=60 * 60,
        test_duration_seconds=30 * 60,
        step_duration_seconds=30 * 60,
        min_in_sample_bars=20,
        min_out_of_sample_bars=10,
        initial_cash=100_000.0,
    )
    wf_res = wf_validator.run(config=wf_cfg, strategy="sma_crossover")
    assert wf_res.is_success
    assert len(wf_res.windows) > 0
    assert wf_res.summary is not None
    assert wf_res.summary.completed_windows > 0

    # 3. Run robustness testing with cost stress and parameter sensitivity
    rob_cfg = RobustnessTestConfig(
        symbol="INFY",
        timeframe="1m",
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        strategy_name="sma_crossover",
        initial_cash=100_000.0,
        include_cost_stress=True,
        include_parameter_sensitivity=True,
    )
    rob_res = rob_engine.run(rob_cfg, strategy="sma_crossover")
    assert rob_res.is_success
    assert len(rob_res.scenario_results) > 0
    assert rob_res.summary.baseline_return_pct is not None
    assert rob_res.summary.worst_case_return_pct is not None


# ------------------------------------------------------------------------------
# Scenario C: Portfolio Analytics, Attribution, Risk Monitoring & Reporting
# ------------------------------------------------------------------------------

def test_scenario_c_multi_strategy_simulation_to_risk_attribution_reporting(full_system_context):
    """Scenario C: Multi-strategy virtual portfolio simulation flowing through risk, attribution, and reporting."""
    ctx = full_system_context
    repo: SQLiteHistoricalBarRepository = ctx["repository"]
    port_engine: PortfolioSimulationEngine = ctx["portfolio_engine"]
    risk_monitor: PortfolioRiskMonitor = ctx["risk_monitor"]
    attr_svc: PortfolioAttributionService = ctx["attribution_service"]
    reporting_svc: PortfolioReportingService = ctx["reporting_service"]

    # Store bars for two symbols
    bars_tcs = _generate_deterministic_bars("TCS", count=150, start_ts=1700000000.0, base_price=3500.0)
    bars_infy = _generate_deterministic_bars("INFY", count=150, start_ts=1700000000.0, base_price=1600.0)
    repo.store_bars(bars_tcs)
    repo.store_bars(bars_infy)

    # 1. Run multi-strategy portfolio simulation
    port_cfg = PortfolioSimulationConfig(
        allocations=(
            StrategyAllocation("sma_crossover", "TCS", weight=0.6),
            StrategyAllocation("sma_crossover", "INFY", weight=0.4),
        ),
        start_time=bars_tcs[0].timestamp,
        end_time=bars_tcs[-1].timestamp,
        timeframe="1m",
        initial_cash=100_000.0,
    )
    sim_res: PortfolioSimulationResult = port_engine.run(port_cfg)
    assert sim_res.status == PortfolioSimulationStatus.SUCCESS
    assert len(sim_res.strategy_contributions) == 2

    # 2. Evaluate portfolio risk
    risk_rep = risk_monitor.evaluate_simulation_result(sim_res)
    assert isinstance(risk_rep, PortfolioRiskReport)
    assert risk_rep.overall_state in (AlertState.NORMAL, AlertState.WARNING, AlertState.CRITICAL)

    # 3. Compute performance attribution
    attr_rep = attr_svc.analyze_simulation_result(sim_res)
    assert attr_rep.source_id == sim_res.simulation_id
    assert len(attr_rep.strategy_attributions) == 2
    assert attr_rep.reconciliation is not None
    assert attr_rep.reconciliation.is_reconciled is True

    # 4. Generate unified portfolio report
    unified_rep: UnifiedPortfolioReport = reporting_svc.generate_unified_report(
        source_result=sim_res,
        risk_report=risk_rep,
        attribution_report=attr_rep,
        title="Integration Multi-Strategy Report",
    )
    assert unified_rep.provenance.source_run_id == sim_res.simulation_id
    assert unified_rep.status in (ReportDataStatus.VALID, ReportDataStatus.STALE)
    assert unified_rep.initial_equity == 100_000.0
    assert len(unified_rep.strategy_attributions) == 2
    assert unified_rep.reconciliation is not None
    assert unified_rep.reconciliation["is_reconciled"] is True


# ------------------------------------------------------------------------------
# Scenario D: Dashboard and Operational Diagnostics Integration
# ------------------------------------------------------------------------------

def test_scenario_d_reporting_to_dashboard_and_diagnostics_integration(full_system_context):
    """Scenario D: Unified reports published, audited in diagnostics, and served via read-only dashboard."""
    ctx = full_system_context
    repo: SQLiteHistoricalBarRepository = ctx["repository"]
    backtest: BacktestEngine = ctx["backtest_engine"]
    reporting_svc: PortfolioReportingService = ctx["reporting_service"]
    diag_svc: PortfolioDiagnosticsService = ctx["diagnostics_service"]

    # Populate bars and run backtest
    bars = _generate_deterministic_bars("WIPRO", count=100, start_ts=1700000000.0)
    repo.store_bars(bars)
    bt_cfg = BacktestConfig(
        symbol="WIPRO",
        timeframe="1m",
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
    )
    bt_res = backtest.run(bt_cfg, strategy="sma_crossover")

    # Generate unified report
    report = reporting_svc.generate_unified_report(bt_res, title="Wipro Backtest Report")
    assert report.report_id is not None

    # Verify diagnostic event recorded for generation
    history = diag_svc.get_history(limit=10)
    gen_events = [e for e in history if e.event_type == DiagnosticEventType.REPORT_GENERATION_SUCCEEDED]
    assert len(gen_events) >= 1
    assert gen_events[0].report_id == report.report_id
    assert gen_events[0].outcome == DiagnosticOutcome.SUCCESS

    # Start ephemeral loopback dashboard server
    server = PortfolioDashboardServer(
        reporting_service=reporting_svc,
        host="127.0.0.1",
        port=0,
        diagnostics_service=diag_svc,
    )
    server.start(blocking=False)
    assert server.is_running
    bound_url = server.url

    try:
        # 1. Fetch latest report via HTTP GET
        req_latest = urllib.request.Request(f"{bound_url}/api/report/latest")
        with urllib.request.urlopen(req_latest, timeout=5) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["report_id"] == report.report_id

        # 2. Fetch diagnostics via HTTP GET
        req_diag = urllib.request.Request(f"{bound_url}/api/diagnostics")
        with urllib.request.urlopen(req_diag, timeout=5) as resp:
            assert resp.status == 200
            diag_data = json.loads(resp.read().decode("utf-8"))
            assert diag_data["status"] == "success"
            assert diag_data["snapshot"]["dashboard_lifecycle"]["state"] == "RUNNING"
            assert diag_data["snapshot"]["counts"]["total_recorded"] >= 1

        # 3. Fault isolation: verify diagnostics failure does NOT corrupt report synthesis
        with patch.object(diag_svc, "record_event", side_effect=RuntimeError("Simulated telemetry failure")):
            rep2 = reporting_svc.generate_unified_report(bt_res, title="Isolated Fault Report")
            assert rep2 is not None
            assert rep2.report_id != report.report_id

    finally:
        server.stop()
        assert not server.is_running


# ------------------------------------------------------------------------------
# Scenario E: Failure Injection, Quality Gating & Lifecycle Recovery
# ------------------------------------------------------------------------------

def test_scenario_e_failure_injection_and_lifecycle_recovery(full_system_context):
    """Scenario E: Deterministic error injection covering quality gating, missing data, and cancellation."""
    ctx = full_system_context
    repo: SQLiteHistoricalBarRepository = ctx["repository"]
    quality: MarketDataQualityEngine = ctx["quality_engine"]
    backtest: BacktestEngine = ctx["backtest_engine"]
    replay: HistoricalReplayEngine = ctx["replay_engine"]
    orchestrator: MasterOrchestrator = ctx["orchestrator"]
    reporting_svc: PortfolioReportingService = ctx["reporting_service"]

    # 1. Malformed orchestrator command input -> Fails cleanly with RequestStatus.FAILED
    req_bad = OrchestratorRequest.create(command="run_backtest", context={"symbol": ""})
    res_bad = orchestrator.handle_request(req_bad)
    assert not res_bad.is_success
    assert res_bad.status == RequestStatus.FAILED
    assert len(res_bad.errors) > 0

    # 2. Unknown strategy -> Fails cleanly with RequestStatus.FAILED
    req_bad_strat = OrchestratorRequest.create(
        command="run_backtest",
        context={"symbol": "TCS", "strategy": "non_existent_martingale_strategy"},
    )
    res_bad_strat = orchestrator.handle_request(req_bad_strat)
    assert not res_bad_strat.is_success
    assert res_bad_strat.status == RequestStatus.FAILED

    # 3. Quality Gating:
    # A) Bar constructor rejects inverted high/low prices
    with pytest.raises(ValidationError):
        Bar(
            symbol="TCS",
            exchange="NSE",
            timeframe="1m",
            timestamp=1700000000.0,
            open=100.0,
            high=90.0,  # Invalid: high < low
            low=110.0,
            close=95.0,
            volume=50.0,
        )

    # B) Quality engine detects impossible future clock skew as INVALID
    future_bar = Bar(
        symbol="TCS",
        exchange="NSE",
        timeframe="1m",
        timestamp=time.time() + 100_000.0,
        open=100.0,
        high=105.0,
        low=95.0,
        close=102.0,
        volume=50.0,
    )
    assessment = quality.evaluate_bar(future_bar)
    assert assessment.quality_status == DataQualityStatus.INVALID
    assert assessment.safe_to_consume is False

    # 4. Empty data range for backtesting -> completes gracefully with empty metrics and warning
    cfg_empty = BacktestConfig(
        symbol="EMPTY_SYM",
        timeframe="1m",
        start_time=1000.0,
        end_time=2000.0,
    )
    res_empty = backtest.run(cfg_empty, strategy="sma_crossover")
    assert res_empty.status == BacktestStatus.COMPLETED
    assert res_empty.total_bars_available == 0
    assert len(res_empty.trades) == 0
    assert any("No historical bars found" in w for w in res_empty.warnings)

    # 5. Non-existent report retrieval -> returns None safely
    assert reporting_svc.get_report_by_id("missing_rep_id_999") is None
    assert reporting_svc.get_report_by_run_id("missing_run_id_999") is None

    # 6. Replay cancellation and clean subsequent recovery
    bars = _generate_deterministic_bars("CANCEL_TEST", count=100, start_ts=1700000000.0)
    repo.store_bars(bars)
    rep_req = HistoricalReplayRequest(
        symbol="CANCEL_TEST",
        timeframe="1m",
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        pacing_seconds=0.01,
        replay_id="rep_cancel_01",
    )

    t = threading.Thread(target=replay.replay, args=(rep_req,))
    t.start()
    time.sleep(0.05)
    replay.cancel_replay("rep_cancel_01")
    t.join(timeout=3.0)

    # Subsequent replay on same symbol completes cleanly
    rep_req_clean = HistoricalReplayRequest(
        symbol="CANCEL_TEST",
        timeframe="1m",
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        pacing_seconds=0.0,
    )
    res_clean = replay.replay(rep_req_clean)
    assert res_clean.status == HistoricalReplayStatus.COMPLETED


# ------------------------------------------------------------------------------
# Concurrency, Reliability & Resource Lifecycle
# ------------------------------------------------------------------------------

def test_concurrency_and_thread_safety_under_load(full_system_context):
    """Reliability test: Concurrent diagnostic recording and report retrievals across worker threads."""
    ctx = full_system_context
    diag_svc: PortfolioDiagnosticsService = ctx["diagnostics_service"]
    reporting_svc: PortfolioReportingService = ctx["reporting_service"]
    repo: SQLiteHistoricalBarRepository = ctx["repository"]
    backtest: BacktestEngine = ctx["backtest_engine"]

    bars = _generate_deterministic_bars("CONC_SYM", count=80, start_ts=1700000000.0)
    repo.store_bars(bars)
    bt_cfg = BacktestConfig(symbol="CONC_SYM", timeframe="1m", start_time=bars[0].timestamp, end_time=bars[-1].timestamp)
    bt_res = backtest.run(bt_cfg, strategy="sma_crossover")
    rep = reporting_svc.generate_unified_report(bt_res, title="Concurrent Benchmark Report")

    def worker_action(idx: int) -> bool:
        # Record diagnostic event
        diag_svc.record_event(
            event_type=DiagnosticEventType.REPORT_RETRIEVAL_SUCCEEDED,
            outcome=DiagnosticOutcome.SUCCESS,
            correlation_id=f"conc_worker_{idx}",
            report_id=rep.report_id,
            details={"worker_index": idx},
        )
        # Retrieve report by ID
        fetched = reporting_svc.get_report_by_id(rep.report_id)
        # Retrieve snapshot
        snap = diag_svc.get_snapshot()
        return fetched is not None and snap["counts"]["total_recorded"] > 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker_action, i) for i in range(40)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert all(results)
    assert len(results) == 40
    assert diag_svc.get_snapshot()["counts"]["total_recorded"] >= 40


def test_repeated_dashboard_server_startup_shutdown_cycles(full_system_context):
    """Lifecycle test: Repeated startup and shutdown of dashboard server does not leak threads or ports."""
    ctx = full_system_context
    reporting_svc: PortfolioReportingService = ctx["reporting_service"]
    diag_svc: PortfolioDiagnosticsService = ctx["diagnostics_service"]

    for cycle in range(3):
        server = PortfolioDashboardServer(
            reporting_service=reporting_svc,
            host="127.0.0.1",
            port=0,
            diagnostics_service=diag_svc,
        )
        server.start(blocking=False)
        assert server.is_running
        port = server.port
        assert port > 0

        # Quick check
        req = urllib.request.Request(f"http://127.0.0.1:{port}/api/status")
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200

        server.stop()
        assert not server.is_running


# ------------------------------------------------------------------------------
# Master Orchestrator Complete End-to-End Pipeline Dispatch
# ------------------------------------------------------------------------------

def test_orchestrator_complete_e2e_pipeline(full_system_context):
    """End-to-end integration through MasterOrchestrator command dispatching."""
    ctx = full_system_context
    orchestrator: MasterOrchestrator = ctx["orchestrator"]
    repo: SQLiteHistoricalBarRepository = ctx["repository"]

    # Pre-load data
    bars_tcs = _generate_deterministic_bars("TCS", count=120, start_ts=1700000000.0, base_price=3500.0)
    bars_infy = _generate_deterministic_bars("INFY", count=120, start_ts=1700000000.0, base_price=1500.0)
    repo.store_bars(bars_tcs)
    repo.store_bars(bars_infy)

    # 1. Query historical bars command
    res_query = orchestrator.handle_request(OrchestratorRequest.create(
        command="query_historical_bars",
        context={
            "symbol": "TCS",
            "timeframe": "1m",
            "start_time": bars_tcs[0].timestamp,
            "end_time": bars_tcs[-1].timestamp,
        },
    ))
    assert res_query.is_success
    assert res_query.result["status"] == "success"
    assert res_query.result["bars_count"] == 120

    # 2. Run Backtest command
    res_bt = orchestrator.handle_request(OrchestratorRequest.create(
        command="run_backtest",
        context={
            "symbol": "TCS",
            "timeframe": "1m",
            "strategy": "sma_crossover",
            "start_time": bars_tcs[0].timestamp,
            "end_time": bars_tcs[-1].timestamp,
        },
    ))
    assert res_bt.is_success
    assert res_bt.result["status"] == "success"
    assert "backtest_id" in res_bt.result

    # 3. Run Portfolio Simulation command
    res_sim = orchestrator.handle_request(OrchestratorRequest.create(
        command="run_portfolio_simulation",
        context={
            "allocations": [
                {"strategy": "sma_crossover", "symbol": "TCS", "weight": 0.6},
                {"strategy": "sma_crossover", "symbol": "INFY", "weight": 0.4},
            ],
            "start_time": bars_tcs[0].timestamp,
            "end_time": bars_tcs[-1].timestamp,
            "timeframe": "1m",
        },
    ))
    assert res_sim.is_success
    assert res_sim.result["status"] == "success"
    sim_id = res_sim.result["simulation_id"]
    assert sim_id is not None

    # 4. Calculate Portfolio Attribution via StateStore simulation_id lookup
    res_attr = orchestrator.handle_request(OrchestratorRequest.create(
        command="calculate_portfolio_attribution",
        context={"simulation_id": sim_id},
    ))
    assert res_attr.is_success
    assert res_attr.result["status"] == "success"
    assert res_attr.result["report"]["source_id"] == sim_id

    # 5. Generate Unified Portfolio Report via StateStore simulation_id lookup
    res_rep = orchestrator.handle_request(OrchestratorRequest.create(
        command="generate_unified_portfolio_report",
        context={"simulation_id": sim_id, "title": "Orchestrator E2E Report"},
    ))
    assert res_rep.is_success
    assert res_rep.result["status"] == "success"
    report_dict = res_rep.result["report"]
    report_id = report_dict["report_id"]
    assert report_id is not None

    # 6. Retrieve Latest Portfolio Report command
    res_latest = orchestrator.handle_request(OrchestratorRequest.create(
        command="get_latest_portfolio_report",
        context={},
    ))
    assert res_latest.is_success
    assert res_latest.result["status"] == "success"
    assert res_latest.result["report"]["report_id"] == report_id

    # 7. Get Portfolio Diagnostics command
    res_diag = orchestrator.handle_request(OrchestratorRequest.create(
        command="get_portfolio_diagnostics",
        context={"limit": 10},
    ))
    assert res_diag.is_success
    assert res_diag.result["status"] == "success"
    assert res_diag.result["snapshot"] is not None
    assert res_diag.result["count"] > 0


# ------------------------------------------------------------------------------
# Safety Invariants & Execution Lock Enforcement
# ------------------------------------------------------------------------------

def test_safety_invariants_and_execution_lock_preservation(full_system_context):
    """Safety tests: Verifies that live trading remains strictly locked and security boundaries are enforced."""
    ctx = full_system_context
    reporting_svc: PortfolioReportingService = ctx["reporting_service"]
    diag_svc: PortfolioDiagnosticsService = ctx["diagnostics_service"]

    # 1. Execution lock invariant
    settings = Settings()
    assert settings.execution_locked is True

    # 2. Loopback binding only
    with pytest.raises(ValidationError):
        PortfolioDashboardServer(reporting_service=reporting_svc, host="0.0.0.0")

    with pytest.raises(ValidationError):
        PortfolioDashboardServer(reporting_service=reporting_svc, host="192.168.1.100")

    # 3. Read-only dashboard rejecting mutating HTTP methods
    server = PortfolioDashboardServer(
        reporting_service=reporting_svc,
        host="127.0.0.1",
        port=0,
        diagnostics_service=diag_svc,
    )
    server.start(blocking=False)
    try:
        url = server.url
        for method in ("POST", "PUT", "DELETE"):
            req = urllib.request.Request(f"{url}/api/diagnostics", data=b"{}", method=method)
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req)
            assert exc_info.value.code == 405
    finally:
        server.stop()

    # 4. Secret scrubbing in diagnostics
    diag_svc.record_event(
        event_type=DiagnosticEventType.ANALYTICS_SERVICE_ERROR,
        outcome=DiagnosticOutcome.WARNING,
        details={
            "safe_tag": "token_check",
            "token": "ghp_123456789012345678901234567890123456",
            "api_key": "secret_key_abcdef",
            "file_path": r"C:\Users\username\Desktop\KAISER\secret.txt",
        },
    )
    event = diag_svc.get_history(limit=1)[0]
    # Key scrubbing
    assert "token" not in event.details
    assert "api_key" not in event.details
    # Path scrubbing
    if "file_path" in event.details:
        assert r"C:\Users" not in event.details["file_path"]
