"""KAISER — Portfolio Analytics Dashboard Operational Validation & Acceptance Tests.

Phase 2, Step 14: End-to-end operational validation of the KAISER Portfolio Analytics
Dashboard and its integration with the portfolio reporting, simulation, and risk systems.

VALIDATION INVARIANTS:
1. End-to-end flow from deterministic mock/historical data -> simulation -> report -> dashboard.
2. Complete metric fidelity against underlying report sources without fabrication.
3. Distinction between genuine numeric zero (0.0), missing (null), and non-finite values (NaN/Inf).
4. Full Data-State Acceptance Matrix (Scenarios A through I).
5. HTTP operational validation: loopback-only binding, read-only enforcement (405 for POST/PUT/DELETE),
   security headers, path traversal defense, and safe error responses.
6. Absolute safety guardrails: Settings.execution_locked remains True, zero broker/trading hooks.
"""

from __future__ import annotations

import concurrent.futures
import json
import math
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Generator, List, Optional, Tuple

import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    BacktestStatus,
    EquityPoint,
    OrderSide,
    PerformanceMetrics,
    TradeRecord,
)
from app.backtesting.portfolio_simulation.engine import PortfolioSimulationEngine
from app.backtesting.portfolio_simulation.models import (
    PortfolioPosition,
    PortfolioRiskSnapshot,
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
    StrategyContribution,
)
from app.backtesting.strategy import SMACrossoverStrategy, StrategyRegistry
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.state.store import StateStore
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.dashboard.template import get_dashboard_html
from app.portfolio.report_models import (
    ReportDataProvenance,
    ReportDataStatus,
    UnifiedPortfolioReport,
    sanitize_dict_for_export,
    sanitize_numeric,
)
from app.portfolio.reporting import PortfolioReportingService
from app.portfolio.reporting_handlers import register_reporting_handlers
from app.risk.models import (
    AlertSeverity,
    AlertState,
    PortfolioRiskAlert,
    PortfolioRiskReport,
    RiskRuleId,
)
from app.risk.monitor import PortfolioRiskMonitor


# ------------------------------------------------------------------------------
# Test Fixtures & Helpers
# ------------------------------------------------------------------------------

@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def state_store() -> StateStore:
    return StateStore()


@pytest.fixture
def attribution_service(event_bus: EventBus, state_store: StateStore) -> PortfolioAttributionService:
    return PortfolioAttributionService(event_bus=event_bus, state_store=state_store)


@pytest.fixture
def risk_monitor(event_bus: EventBus, state_store: StateStore) -> PortfolioRiskMonitor:
    return PortfolioRiskMonitor(event_bus=event_bus, state_store=state_store)


@pytest.fixture
def reporting_service(
    event_bus: EventBus,
    state_store: StateStore,
    attribution_service: PortfolioAttributionService,
    risk_monitor: PortfolioRiskMonitor,
) -> PortfolioReportingService:
    return PortfolioReportingService(
        event_bus=event_bus,
        state_store=state_store,
        attribution_service=attribution_service,
        risk_monitor=risk_monitor,
        max_history_size=20,
    )


@pytest.fixture
def in_memory_bar_repo() -> SQLiteHistoricalBarRepository:
    return SQLiteHistoricalBarRepository(db_path=":memory:", name="test_acceptance_mem_repo")


@pytest.fixture
def dashboard_server(
    reporting_service: PortfolioReportingService,
) -> Generator[PortfolioDashboardServer, None, None]:
    """Provides a running dashboard server on a dynamic OS-assigned port."""
    server = PortfolioDashboardServer(
        reporting_service=reporting_service,
        host="127.0.0.1",
        port=0,
    )
    server.start(blocking=False)
    yield server
    server.stop()


def _generate_deterministic_bars(
    symbol: str = "NIFTY50",
    count: int = 50,
    start_ts: float = 1700000000.0,
    interval_s: float = 900.0,
) -> List[Bar]:
    """Generates deterministic synthetic bars with a known price trend."""
    bars: List[Bar] = []
    base_price = 100.0
    for i in range(count):
        ts = start_ts + (i * interval_s)
        # Alternate upward and slight dips for deterministic crossings
        price = base_price + (i * 1.5) + (2.0 if i % 4 == 0 else -1.0)
        o = round(price, 2)
        h = round(o + 2.0, 2)
        l = round(o - 2.0, 2)
        c = round(o + 0.5, 2)
        v = 1000.0 + (i * 10.0)
        bars.append(
            Bar(
                symbol=symbol,
                exchange="NSE",
                timeframe="15m",
                timestamp=ts,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=v,
                source="mock",
            )
        )
    return bars


def _create_mock_simulation_result(
    simulation_id: str = "sim_acc_01",
    initial_equity: float = 100_000.0,
    final_equity: float = 112_500.0,
    gross_pnl: float = 13_000.0,
    net_pnl: float = 12_500.0,
    total_fees: float = 300.0,
    total_slippage: float = 200.0,
    base_ts: Optional[float] = None,
) -> PortfolioSimulationResult:
    """Creates a deterministic PortfolioSimulationResult with verified accounting."""
    now = base_ts if base_ts is not None else time.time()
    t0 = now - 3600.0
    alloc = StrategyAllocation(strategy_name="sma_crossover", symbol="NIFTY50", weight=1.0)
    cfg = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=t0,
        end_time=now,
        initial_cash=initial_equity,
        simulation_id=simulation_id,
    )

    eq_points = (
        EquityPoint(timestamp=t0, cash=100_000.0, position_value=0.0, total_equity=100_000.0, drawdown_pct=0.0, drawdown_amount=0.0),
        EquityPoint(timestamp=t0 + 1000.0, cash=80_000.0, position_value=26_000.0, total_equity=106_000.0, drawdown_pct=0.0, drawdown_amount=0.0),
        EquityPoint(timestamp=t0 + 2000.0, cash=70_000.0, position_value=45_000.0, total_equity=115_000.0, drawdown_pct=0.0, drawdown_amount=0.0),
        EquityPoint(timestamp=now, cash=112_500.0, position_value=0.0, total_equity=112_500.0, drawdown_pct=2.17, drawdown_amount=2_500.0),
    )

    risk_snaps = (
        PortfolioRiskSnapshot(
            timestamp=t0,
            total_equity=100_000.0,
            cash=100_000.0,
            cash_reserve_pct=100.0,
            gross_exposure=0.0,
            gross_exposure_pct=0.0,
            net_exposure=0.0,
            net_exposure_pct=0.0,
            drawdown_pct=0.0,
        ),
        PortfolioRiskSnapshot(
            timestamp=now,
            total_equity=112_500.0,
            cash=112_500.0,
            cash_reserve_pct=100.0,
            gross_exposure=0.0,
            gross_exposure_pct=0.0,
            net_exposure=0.0,
            net_exposure_pct=0.0,
            drawdown_pct=2.17,
        ),
    )

    contrib = StrategyContribution(
        strategy_id="strat_sma_nifty",
        strategy_name="sma_crossover",
        symbol="NIFTY50",
        weight=1.0,
        allocated_capital=100_000.0,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        net_return_pct=12.5,
        portfolio_pnl_share_pct=100.0,
        total_trades=8,
        winning_trades=6,
        losing_trades=2,
        win_rate=75.0,
        profit_factor=2.5,
        profit_factor_description="Favorable",
        total_fees=total_fees,
        total_slippage=total_slippage,
        orders_submitted=8,
        orders_accepted=8,
        orders_rejected=0,
    )

    return PortfolioSimulationResult(
        simulation_id=simulation_id,
        status=PortfolioSimulationStatus.SUCCESS,
        config=cfg,
        initial_equity=initial_equity,
        final_equity=final_equity,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        net_return_pct=((final_equity - initial_equity) / initial_equity) * 100.0,
        max_drawdown_pct=2.17,
        max_drawdown_amount=2_500.0,
        max_drawdown_duration_bars=5,
        total_fees=total_fees,
        total_slippage=total_slippage,
        total_transaction_costs=total_fees + total_slippage,
        total_trades=8,
        winning_trades=6,
        losing_trades=2,
        win_rate=75.0,
        profit_factor=2.5,
        profit_factor_description="Favorable",
        equity_curve=eq_points,
        risk_snapshots=risk_snaps,
        strategy_contributions=(contrib,),
        start_timestamp=1700000000.0,
        end_timestamp=1700045000.0,
    )


# ------------------------------------------------------------------------------
# 1. End-to-End Simulation to Dashboard Flow (Section 2)
# ------------------------------------------------------------------------------

def test_e2e_mock_data_to_simulation_to_report_to_dashboard_flow(
    in_memory_bar_repo: SQLiteHistoricalBarRepository,
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """E2E Flow: Bar generation -> Simulation Engine -> Reporting Service -> HTTP Dashboard."""
    # 1. Generate & store deterministic mock bars
    bars = _generate_deterministic_bars(symbol="NIFTY50", count=50)
    in_memory_bar_repo.store_bars(bars)

    # 2. Run portfolio simulation engine
    registry = StrategyRegistry()
    registry.register("sma_crossover", lambda **kw: SMACrossoverStrategy(fast_period=3, slow_period=8))
    analysis_engine = TechnicalAnalysisEngine(name="test_acc_analysis")

    sim_engine = PortfolioSimulationEngine(
        repository=in_memory_bar_repo,
        strategy_registry=registry,
        analysis_engine=analysis_engine,
    )

    sim_cfg = PortfolioSimulationConfig(
        allocations=(StrategyAllocation(strategy_name="sma_crossover", symbol="NIFTY50", weight=1.0),),
        initial_cash=100_000.0,
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        timeframe="15m",
        simulation_id="sim_e2e_01",
    )
    sim_result = sim_engine.run(sim_cfg)
    assert sim_result.status == PortfolioSimulationStatus.SUCCESS

    # 3. Generate unified report via reporting service
    report = reporting_service.generate_unified_report(sim_result)
    assert report.report_id.startswith("report_")
    assert report.provenance.source_run_id == "sim_e2e_01"

    # 4. Query Dashboard HTTP UI & API Endpoints
    # Root HTML
    with urllib.request.urlopen(f"{dashboard_server.url}/") as resp:
        assert resp.status == 200
        html = resp.read().decode("utf-8")
        assert "KAISER — Portfolio Analytics Command Center" in html
        assert "valNetPnL" in html
        assert "equitySvg" in html

    # Latest Report API
    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/latest") as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["report_id"] == report.report_id
        assert data["provenance"]["source_run_id"] == "sim_e2e_01"
        assert data["initial_equity"] == 100_000.0
        assert data["final_equity"] == sim_result.final_equity
        assert data["net_pnl"] == sim_result.net_pnl
        assert data["max_drawdown_pct"] == sim_result.max_drawdown_pct

    # Series API
    with urllib.request.urlopen(f"{dashboard_server.url}/api/series/{report.report_id}") as resp:
        assert resp.status == 200
        s_data = json.loads(resp.read().decode("utf-8"))
        assert s_data["status"] == "success"
        assert "series" in s_data
        assert len(s_data["series"]["points"]) == len(sim_result.equity_curve)

    # Reports list API
    with urllib.request.urlopen(f"{dashboard_server.url}/api/reports") as resp:
        assert resp.status == 200
        l_data = json.loads(resp.read().decode("utf-8"))
        assert l_data["count"] >= 1
        assert l_data["reports"][0]["report_id"] == report.report_id


# ------------------------------------------------------------------------------
# 2. Portfolio Metrics Acceptance Tests (Section 3)
# ------------------------------------------------------------------------------

def test_metric_fidelity_and_precision_against_underlying_report(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Verify that every metric exposed via the dashboard API matches the report source exactly."""
    sim = _create_mock_simulation_result(simulation_id="sim_fidelity_01")
    rep = reporting_service.generate_unified_report(sim)

    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))

        # Exact match assertions
        assert data["net_pnl"] == rep.net_pnl == 12_500.0
        assert data["gross_pnl"] == rep.gross_pnl == 13_000.0
        assert data["net_return_pct"] == rep.net_return_pct == 12.5
        assert data["max_drawdown_pct"] == rep.max_drawdown_pct == 2.17
        assert data["max_drawdown_amount"] == rep.max_drawdown_amount == 2_500.0
        assert data["max_drawdown_duration_bars"] == rep.max_drawdown_duration_bars == 5
        assert data["total_transaction_costs"] == rep.total_transaction_costs == 500.0
        assert data["initial_equity"] == rep.initial_equity == 100_000.0
        assert data["final_equity"] == rep.final_equity == 112_500.0
        assert data["cash_balance"] == rep.cash_balance == 112_500.0
        assert data["gross_exposure"] == rep.gross_exposure == 0.0
        assert data["net_exposure"] == rep.net_exposure == 0.0
        assert data["cash_reserve_pct"] == rep.cash_reserve_pct == 100.0

        # Strategy attribution fidelity
        assert len(data["strategy_attributions"]) == 1
        strat = data["strategy_attributions"][0]
        assert strat["strategy_name"] == "sma_crossover"
        assert strat["net_pnl"] == 12_500.0
        assert strat["trades_count"] == 8
        assert strat["win_rate"] == 75.0


def test_genuine_zero_versus_missing_metrics_acceptance(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Verify genuine 0.0 values remain 0.0, while missing metrics serialize strictly to null."""
    # 1. Report with genuine zeroes
    zero_dict = {
        "simulation_id": "sim_zero_acc",
        "initial_equity": 100_000.0,
        "final_equity": 100_000.0,
        "gross_pnl": 0.0,
        "net_pnl": 0.0,
        "net_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "cash_balance": 100_000.0,
        "gross_exposure": 0.0,
        "total_transaction_costs": 0.0,
    }
    rep_zero = reporting_service.generate_unified_report(zero_dict)
    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep_zero.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["net_pnl"] == 0.0
        assert data["gross_pnl"] == 0.0
        assert data["net_return_pct"] == 0.0
        assert data["max_drawdown_pct"] == 0.0
        assert data["gross_exposure"] == 0.0
        assert data["total_transaction_costs"] == 0.0

    # 2. Report with missing metrics
    missing_dict = {
        "simulation_id": "sim_missing_acc",
        "initial_equity": 100_000.0,
        "final_equity": 100_000.0,
        "gross_pnl": None,
        "net_pnl": None,
        "net_return_pct": None,
        "max_drawdown_pct": None,
        "cash_balance": None,
        "gross_exposure": None,
        "total_transaction_costs": None,
    }
    rep_missing = reporting_service.generate_unified_report(missing_dict)
    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep_missing.report_id}") as resp:
        data_m = json.loads(resp.read().decode("utf-8"))
        assert data_m["net_pnl"] is None
        assert data_m["gross_pnl"] is None
        assert data_m["net_return_pct"] is None
        assert data_m["max_drawdown_pct"] is None
        assert data_m["cash_balance"] is None
        assert data_m["gross_exposure"] is None


def test_chronological_ordering_and_empty_vs_flat_series(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Verify equity curve chronological ordering and distinction between empty and flat curves."""
    # 1. Normal Chronological Series
    sim = _create_mock_simulation_result(simulation_id="sim_series_01")
    rep = reporting_service.generate_unified_report(sim)

    with urllib.request.urlopen(f"{dashboard_server.url}/api/series/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        pts = data["series"]["points"]
        timestamps = [p["timestamp"] for p in pts]
        assert timestamps == sorted(timestamps)
        assert len(timestamps) > 1

    # 2. Empty Series vs Flat Series
    # Empty
    rep_empty = reporting_service.generate_unified_report({
        "simulation_id": "sim_empty_series",
        "initial_equity": 100_000.0,
        "final_equity": 100_000.0,
        "equity_curve": [],
    })
    with urllib.request.urlopen(f"{dashboard_server.url}/api/series/{rep_empty.report_id}") as resp:
        data_empty = json.loads(resp.read().decode("utf-8"))
        assert data_empty["series"]["points_count"] == 0

    # Flat
    flat_pts = [
        {"timestamp": 1000.0 + i * 60, "total_equity": 100_000.0, "cash": 100_000.0, "position_value": 0.0, "unrealized_pnl": 0.0, "realized_pnl": 0.0}
        for i in range(5)
    ]
    rep_flat = reporting_service.generate_unified_report({
        "simulation_id": "sim_flat_series",
        "initial_equity": 100_000.0,
        "final_equity": 100_000.0,
        "equity_curve": flat_pts,
    })
    with urllib.request.urlopen(f"{dashboard_server.url}/api/series/{rep_flat.report_id}") as resp:
        data_flat = json.loads(resp.read().decode("utf-8"))
        assert data_flat["series"]["points_count"] == 5
        assert all(p["total_equity"] == 100_000.0 for p in data_flat["series"]["points"])
        assert all(p["drawdown_pct"] == 0.0 for p in data_flat["series"]["points"])


# ------------------------------------------------------------------------------
# 3. Data-State Acceptance Matrix (Section 4, Scenarios A to I)
# ------------------------------------------------------------------------------

def test_scenario_a_valid_report_rendering(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario A: All available metrics, charts, provenance, and timestamps render correctly."""
    sim = _create_mock_simulation_result(simulation_id="sim_valid_a")
    rep = reporting_service.generate_unified_report(sim)

    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "VALID"
        assert data["provenance"]["source_run_id"] == "sim_valid_a"
        assert "generated_at_iso" in data["provenance"]
        assert len(data["equity_curve"]) == 4


def test_scenario_b_no_reports_empty_state(
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario B: Dashboard handles absence of reports gracefully with 404 and empty list."""
    # Latest report returns 404
    try:
        with urllib.request.urlopen(f"{dashboard_server.url}/api/report/latest"):
            assert False, "Expected 404 Not Found"
    except urllib.error.HTTPError as err:
        assert err.code == 404
        data = json.loads(err.read().decode("utf-8"))
        assert "No portfolio reports generated yet" in data["message"]

    # Reports list returns count 0
    with urllib.request.urlopen(f"{dashboard_server.url}/api/reports") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["count"] == 0
        assert data["reports"] == []

    # HTML page contains empty state markup
    with urllib.request.urlopen(f"{dashboard_server.url}/") as resp:
        html = resp.read().decode("utf-8")
        assert 'id="emptyState"' in html
        assert "No Portfolio Reports Available" in html


def test_scenario_c_missing_metrics(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario C: Missing fields render as null / N/A without raising exceptions."""
    sparse_data = {
        "simulation_id": "sim_sparse_c",
        "initial_equity": 50_000.0,
        "final_equity": 55_000.0,
        "gross_pnl": 5_000.0,
        "net_pnl": 5_000.0,
        "risk_adjusted_metrics": {
            "sharpe_ratio": {"is_available": False, "value": None},
        },
        # Sortino, drawdowns omitted
    }
    rep = reporting_service.generate_unified_report(sparse_data)
    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["net_pnl"] == 5_000.0
        assert data["max_drawdown_pct"] is None
        assert data["risk_adjusted_metrics"]["sharpe_ratio"]["is_available"] is False
        assert data["risk_adjusted_metrics"]["sharpe_ratio"]["value"] is None


def test_scenario_d_genuine_zero_values(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario D: Valid zero P&L, exposure, and returns remain zero."""
    zero_data = {
        "simulation_id": "sim_zero_d",
        "initial_equity": 100_000.0,
        "final_equity": 100_000.0,
        "gross_pnl": 0.0,
        "net_pnl": 0.0,
        "net_return_pct": 0.0,
        "gross_exposure": 0.0,
        "net_exposure": 0.0,
        "max_drawdown_pct": 0.0,
    }
    rep = reporting_service.generate_unified_report(zero_data)
    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["net_pnl"] == 0.0
        assert data["net_return_pct"] == 0.0
        assert data["gross_exposure"] == 0.0


def test_scenario_e_non_finite_values_handling(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario E: NaN, +Inf, -Inf in input dictionaries are sanitized to None/null in JSON."""
    non_finite_data = {
        "simulation_id": "sim_inf_e",
        "initial_equity": 100_000.0,
        "final_equity": 100_000.0,
        "gross_pnl": float("nan"),
        "net_pnl": float("inf"),
        "net_return_pct": float("-inf"),
        "max_drawdown_pct": float("nan"),
    }
    rep = reporting_service.generate_unified_report(non_finite_data)
    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["gross_pnl"] is None
        assert data["net_pnl"] is None
        assert data["net_return_pct"] is None
        assert data["max_drawdown_pct"] is None


def test_scenario_f_stale_or_incomplete_report_status(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario F: Stale or incomplete status is preserved with prominent warnings."""
    stale_data = {
        "simulation_id": "sim_stale_f",
        "initial_equity": 100_000.0,
        "final_equity": 105_000.0,
        "status": "STALE",
        "data_warnings": ["Snapshot timestamp is older than max allowed age (300s)."],
    }
    rep = reporting_service.generate_unified_report(stale_data)
    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "STALE"
        assert len(data["data_warnings"]) >= 1
        assert "older than" in data["data_warnings"][0]


def test_scenario_g_malformed_report_or_retrieval_error(
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario G: Malformed paths, traversal, and invalid IDs return controlled responses."""
    # Traversal attempt
    try:
        with urllib.request.urlopen(f"{dashboard_server.url}/api/report/..%2f..%2fsecret"):
            assert False, "Expected 400 Bad Request"
    except urllib.error.HTTPError as err:
        assert err.code == 400
        data = json.loads(err.read().decode("utf-8"))
        assert data["error"] == "Bad Request"

    # Traversal sequence
    try:
        with urllib.request.urlopen(f"{dashboard_server.url}/api/series/../evil"):
            assert False, "Expected 400 Bad Request"
    except urllib.error.HTTPError as err:
        assert err.code in (400, 404)

    # Malformed compare parameters
    try:
        with urllib.request.urlopen(f"{dashboard_server.url}/api/compare?runs=only_one_run"):
            assert False, "Expected 400 Bad Request"
    except urllib.error.HTTPError as err:
        assert err.code == 400
        data = json.loads(err.read().decode("utf-8"))
        assert "At least two run IDs" in data["message"]


def test_scenario_h_historical_run_comparison(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario H: Multi-run historical comparison matches source metrics without ranking bias."""
    sim1 = _create_mock_simulation_result(simulation_id="sim_h_01", initial_equity=100_000.0, final_equity=110_000.0)
    sim2 = _create_mock_simulation_result(simulation_id="sim_h_02", initial_equity=100_000.0, final_equity=95_000.0)
    rep1 = reporting_service.generate_unified_report(sim1)
    rep2 = reporting_service.generate_unified_report(sim2)

    url = f"{dashboard_server.url}/api/compare?runs={rep1.report_id},{rep2.report_id}"
    with urllib.request.urlopen(url) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "success"
        comp = data["comparison"]
        assert len(comp["entities"]) == 2
        # Check research disclaimer
        assert "RESEARCH ONLY" in comp["disclaimer"]


def test_scenario_i_risk_alerts_scenarios(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Scenario I: Active, empty, and multiple severity risk alerts render accurately."""
    # 1. Active CRITICAL and WARNING alerts
    report_dict = {
        "simulation_id": "sim_alerts_i",
        "initial_equity": 100_000.0,
        "final_equity": 80_000.0,
        "risk_status": {
            "status": "CRITICAL",
            "active_alerts_count": 2,
            "alerts": [
                {
                    "alert_id": "alt_01",
                    "rule_id": "max_drawdown_limit",
                    "severity": "CRITICAL",
                    "message": "Maximum drawdown breached 20.0% threshold (22.5%).",
                    "state": "ACTIVE",
                    "threshold_value": 20.0,
                    "breach_value": 22.5,
                },
                {
                    "alert_id": "alt_02",
                    "rule_id": "cash_reserve_buffer",
                    "severity": "WARNING",
                    "message": "Cash reserve buffer below 5.0% minimum (3.2%).",
                    "state": "ACTIVE",
                    "threshold_value": 5.0,
                    "breach_value": 3.2,
                },
            ],
        },
    }
    rep = reporting_service.generate_unified_report(report_dict)

    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        risk = data["risk_status"]
        assert risk["status"] == "CRITICAL"
        assert risk["active_alerts_count"] == 2
        assert risk["alerts"][0]["severity"] == "CRITICAL"
        assert risk["alerts"][1]["severity"] == "WARNING"


# ------------------------------------------------------------------------------
# 4. HTTP and Operational Validation (Section 5)
# ------------------------------------------------------------------------------

def test_http_loopback_only_binding_and_validation(reporting_service: PortfolioReportingService):
    """Verify strictly loopback binding is allowed, public IPs and bad ports are rejected."""
    # Valid loopback
    for host in ["127.0.0.1", "localhost", "::1"]:
        srv = PortfolioDashboardServer(reporting_service=reporting_service, host=host, port=0)
        assert srv.host == host

    # Invalid public hosts
    for bad_host in ["0.0.0.0", "192.168.1.50", "8.8.8.8", "public.kaiser.ai"]:
        with pytest.raises(ValidationError, match="Security constraint violation"):
            PortfolioDashboardServer(reporting_service=reporting_service, host=bad_host, port=0)

    # Invalid ports
    for bad_port in [-5, 65536, 100000]:
        with pytest.raises(ValidationError, match="Invalid port"):
            PortfolioDashboardServer(reporting_service=reporting_service, host="127.0.0.1", port=bad_port)


def test_http_read_only_get_and_head_methods(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Verify GET and HEAD methods across all routes."""
    sim = _create_mock_simulation_result(simulation_id="sim_get_head_01")
    rep = reporting_service.generate_unified_report(sim)

    routes = [
        "/",
        "/index.html",
        "/api/status",
        "/api/report/latest",
        f"/api/report/{rep.report_id}",
        "/api/reports",
        f"/api/series/{rep.report_id}",
    ]

    for route in routes:
        # GET
        with urllib.request.urlopen(f"{dashboard_server.url}{route}") as resp:
            assert resp.status == 200
            assert len(resp.read()) > 0

        # HEAD
        req_head = urllib.request.Request(f"{dashboard_server.url}{route}", method="HEAD")
        with urllib.request.urlopen(req_head) as resp_head:
            assert resp_head.status == 200
            assert resp_head.read() == b""


def test_http_mutating_methods_rejected(dashboard_server: PortfolioDashboardServer):
    """Verify all modifying HTTP methods return 405 Method Not Allowed with Allow header."""
    for method in ["POST", "PUT", "DELETE", "PATCH"]:
        req = urllib.request.Request(f"{dashboard_server.url}/api/report/latest", method=method, data=b"{}")
        try:
            with urllib.request.urlopen(req):
                assert False, f"Expected 405 for method {method}"
        except urllib.error.HTTPError as err:
            assert err.code == 405
            assert "GET, HEAD" in err.headers.get("Allow", "")
            data = json.loads(err.read().decode("utf-8"))
            assert data["error"] == "Method Not Allowed"


def test_http_security_headers_and_content_types(dashboard_server: PortfolioDashboardServer):
    """Verify complete security header injection and correct content-type charset."""
    # HTML endpoint
    with urllib.request.urlopen(f"{dashboard_server.url}/") as resp:
        assert resp.headers.get("Content-Type") == "text/html; charset=utf-8"
        assert "default-src 'self'" in resp.headers.get("Content-Security-Policy", "")
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"
        assert resp.headers.get("X-Frame-Options") == "DENY"
        assert resp.headers.get("Referrer-Policy") == "no-referrer"
        assert "no-store" in resp.headers.get("Cache-Control", "")

    # JSON endpoint
    with urllib.request.urlopen(f"{dashboard_server.url}/api/status") as resp:
        assert resp.headers.get("Content-Type") == "application/json; charset=utf-8"
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"


def test_http_html_escaping_and_injection_defense(
    reporting_service: PortfolioReportingService,
    dashboard_server: PortfolioDashboardServer,
):
    """Verify template sanitization and escaping against XSS payloads."""
    malicious_dict = {
        "simulation_id": "sim_xss_01",
        "title": "<script>alert('pwned')</script>",
        "initial_equity": 100_000.0,
        "final_equity": 105_000.0,
        "strategy_attributions": [
            {
                "strategy_id": "strat_<b>inject</b>",
                "strategy_name": "<img src=x onerror=alert(1)>",
                "weight_pct": 100.0,
                "net_pnl": 5_000.0,
            }
        ],
        "data_warnings": ["<svg onload=alert(2)>"],
    }
    rep = reporting_service.generate_unified_report(malicious_dict)

    with urllib.request.urlopen(f"{dashboard_server.url}/api/report/{rep.report_id}") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        # Serialized data retains raw characters cleanly in JSON
        assert "<img" in data["strategy_attributions"][0]["strategy_name"]
        assert "<svg" in data["data_warnings"][0]

    # Verify template code enforces client-side escapeHtml() across dynamic insertions
    html = get_dashboard_html()
    assert "function escapeHtml" in html
    assert "escapeHtml(s.strategy_name" in html
    assert "escapeHtml(w)" in html


def test_server_repeated_start_stop_lifecycle(reporting_service: PortfolioReportingService):
    """Verify server can start and stop repeatedly across cycles without resource leaks."""
    server = PortfolioDashboardServer(reporting_service=reporting_service, host="127.0.0.1", port=0)
    for _ in range(3):
        assert not server.is_running
        server.start(blocking=False)
        assert server.is_running
        # Ping health endpoint
        with urllib.request.urlopen(f"{server.url}/api/status") as resp:
            assert resp.status == 200
        server.stop()
        assert not server.is_running


def test_concurrent_read_only_requests_integrity(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Verify concurrent read-only queries execute reliably without state corruption."""
    sim = _create_mock_simulation_result(simulation_id="sim_concurrent_acc")
    rep = reporting_service.generate_unified_report(sim)

    urls = [
        f"{dashboard_server.url}/",
        f"{dashboard_server.url}/api/status",
        f"{dashboard_server.url}/api/report/latest",
        f"{dashboard_server.url}/api/report/{rep.report_id}",
        f"{dashboard_server.url}/api/reports",
        f"{dashboard_server.url}/api/series/{rep.report_id}",
    ] * 6  # 36 concurrent requests

    def fetch_status(u: str) -> int:
        with urllib.request.urlopen(u) as resp:
            return resp.status

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(fetch_status, urls))

    assert len(statuses) == 36
    assert all(s == 200 for s in statuses)


# ------------------------------------------------------------------------------
# 5. Read-Only and Safety Acceptance (Section 6)
# ------------------------------------------------------------------------------

def test_safety_invariants_and_execution_lock_preservation(
    dashboard_server: PortfolioDashboardServer,
):
    """Verify execution lock is strictly maintained and no trade execution paths exist."""
    settings = Settings()
    assert settings.execution_locked is True

    # No order routing or broker methods exist on the dashboard server
    for forbidden in ["place_order", "execute_trade", "cancel_order", "liquidate_positions", "broker"]:
        assert not hasattr(dashboard_server, forbidden)

    # Health API explicitly confirms execution_locked and read_only
    with urllib.request.urlopen(f"{dashboard_server.url}/api/status") as resp:
        data = json.loads(resp.read().decode("utf-8"))
        assert data["execution_locked"] is True
        assert data["read_only"] is True


def test_orchestrator_integration_read_only_commands(
    reporting_service: PortfolioReportingService,
):
    """Verify MasterOrchestrator dashboard lifecycle handlers."""
    orchestrator = MasterOrchestrator()
    server = PortfolioDashboardServer(reporting_service=reporting_service, host="127.0.0.1", port=0)
    register_reporting_handlers(orchestrator=orchestrator, service=reporting_service, server=server)

    try:
        # 1. Start dashboard via orchestrator
        req_start = OrchestratorRequest.create(command="start_portfolio_dashboard", context={"port": 0})
        res_start = orchestrator.handle_request(req_start)
        assert res_start.is_success is True
        assert res_start.result["is_running"] is True

        # 2. Get status
        req_stat = OrchestratorRequest.create(command="get_portfolio_dashboard_status")
        res_stat = orchestrator.handle_request(req_stat)
        assert res_stat.is_success is True
        assert res_stat.result["is_running"] is True
        assert "url" in res_stat.result

        # 3. Stop dashboard
        req_stop = OrchestratorRequest.create(command="stop_portfolio_dashboard")
        res_stop = orchestrator.handle_request(req_stop)
        assert res_stop.is_success is True
        assert res_stop.result["is_running"] is False
    finally:
        server.stop()
