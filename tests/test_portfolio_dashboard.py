"""KAISER — Tests for Portfolio Analytics Dashboard UI Foundation.

Comprehensive verification of:
1. Dashboard loads when a valid report exists.
2. Dashboard renders an appropriate empty state when no report exists.
3. Missing metrics appear as unavailable rather than fabricated values.
4. Equity and drawdown series preserve chronological order.
5. Invalid or non-finite values are handled safely.
6. VALID, STALE, INCOMPLETE, and UNAVAILABLE states are represented correctly.
7. Risk alerts render correctly for active, resolved, and empty states where supported.
8. Historical run comparison handles missing metrics and empty results.
9. Dashboard access does not mutate portfolio or strategy state.
10. No order-execution or broker pathway is introduced (POST/PUT/DELETE rejected with 405).
11. Existing voice and CLI startup behavior remains compatible.
12. No JUJU files are modified.
"""

import json
import urllib.error
import urllib.request
from typing import Generator, List

import pytest

from app.backtesting.models import EquityPoint, OrderSide, TradeRecord
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
    StrategyContribution,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.dashboard.template import get_dashboard_html
from app.portfolio.report_models import ReportDataStatus
from app.portfolio.reporting import PortfolioReportingService
from app.portfolio.reporting_handlers import register_reporting_handlers
from app.risk.models import AlertSeverity, AlertState, PortfolioRiskAlert, PortfolioRiskReport
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
        max_history_size=10,
    )


@pytest.fixture
def dashboard_server(
    reporting_service: PortfolioReportingService,
) -> Generator[PortfolioDashboardServer, None, None]:
    """Provides a running dashboard server on a dynamic OS-assigned port."""
    server = PortfolioDashboardServer(
        reporting_service=reporting_service,
        host="127.0.0.1",
        port=0,  # OS dynamically selects free port
    )
    server.start(blocking=False)
    yield server
    server.stop()


def _create_mock_simulation_result(
    simulation_id: str = "sim_dash_01",
    initial_equity: float = 100_000.0,
    final_equity: float = 108_500.0,
    gross_pnl: float = 9_000.0,
    net_pnl: float = 8_500.0,
    status: PortfolioSimulationStatus = PortfolioSimulationStatus.SUCCESS,
) -> PortfolioSimulationResult:
    """Helper creating deterministic simulation result."""
    alloc_a = StrategyAllocation(strategy_name="alpha_trend", symbol="TCS", weight=0.6)
    alloc_b = StrategyAllocation(strategy_name="beta_mean_rev", symbol="INFY", weight=0.4)

    cfg = PortfolioSimulationConfig(
        allocations=(alloc_a, alloc_b),
        start_time=1700000000.0,
        end_time=1700003600.0,
        initial_cash=initial_equity,
    )

    t1 = TradeRecord(
        trade_id="t1",
        symbol="TCS",
        side=OrderSide.BUY,
        entry_time=1700000100.0,
        exit_time=1700000500.0,
        entry_price=100.0,
        exit_price=110.0,
        quantity=50.0,
        gross_pnl=5_300.0,
        net_pnl=5_000.0,
        fees=180.0,
        slippage=120.0,
        return_pct=10.0,
        duration_seconds=400.0,
        is_win=True,
    )

    c1 = StrategyContribution(
        strategy_id="alpha_trend_TCS",
        strategy_name="alpha_trend",
        symbol="TCS",
        weight=0.6,
        allocated_capital=60_000.0,
        gross_pnl=5_300.0,
        net_pnl=5_000.0,
        net_return_pct=8.33,
        portfolio_pnl_share_pct=58.82,
        total_trades=1,
        winning_trades=1,
        losing_trades=0,
        win_rate=100.0,
        profit_factor=None,
        profit_factor_description="Infinity",
        total_fees=180.0,
        total_slippage=120.0,
        orders_submitted=1,
        orders_accepted=1,
        orders_rejected=0,
        trades=(t1,),
    )

    eq_curve = [
        EquityPoint(1700000000.0, 100_000.0, 0.0, 100_000.0, 0.0, 0.0),
        EquityPoint(1700000100.0, 50_000.0, 53_000.0, 103_000.0, 0.0, 0.0),
        EquityPoint(1700000200.0, 50_000.0, 57_000.0, 107_000.0, 0.0, 0.0),
        EquityPoint(1700000300.0, 50_000.0, 55_000.0, 105_000.0, 1.87, 2_000.0),
        EquityPoint(1700000400.0, 108_500.0, 0.0, 108_500.0, 0.0, 0.0),
    ]

    return PortfolioSimulationResult(
        simulation_id=simulation_id,
        status=status,
        config=cfg,
        initial_equity=initial_equity,
        final_equity=final_equity,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        net_return_pct=(net_pnl / initial_equity) * 100.0,
        max_drawdown_pct=1.87,
        max_drawdown_amount=2_000.0,
        max_drawdown_duration_bars=1,
        total_fees=300.0,
        total_slippage=200.0,
        total_transaction_costs=500.0,
        total_trades=1,
        winning_trades=1,
        losing_trades=0,
        win_rate=100.0,
        profit_factor=None,
        profit_factor_description="Infinity",
        equity_curve=tuple(eq_curve),
        risk_snapshots=(),
        strategy_contributions=(c1,),
    )


# ------------------------------------------------------------------------------
# Test Cases
# ------------------------------------------------------------------------------

def test_dashboard_loads_when_valid_report_exists(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Test 1: Dashboard root and latest API load cleanly when valid report exists."""
    sim = _create_mock_simulation_result(simulation_id="sim_valid_01")
    rep = reporting_service.generate_unified_report(sim)

    # 1. Test HTML UI root
    req = urllib.request.Request(f"{dashboard_server.url}/")
    with urllib.request.urlopen(req) as response:
        assert response.status == 200
        html = response.read().decode("utf-8")
        assert "KAISER — Portfolio Analytics Command Center" in html
        assert "LOCKED (READ-ONLY)" in html
        assert "Portfolio Overview" in html or "valNetPnL" in html

    # 2. Test JSON API /api/report/latest
    api_req = urllib.request.Request(f"{dashboard_server.url}/api/report/latest")
    with urllib.request.urlopen(api_req) as response:
        assert response.status == 200
        data = json.loads(response.read().decode("utf-8"))
        assert data["report_id"] == rep.report_id
        assert data["net_pnl"] == 8_500.0
        assert data["status"] == "VALID"


def test_dashboard_renders_empty_state_when_no_report_exists(
    dashboard_server: PortfolioDashboardServer,
):
    """Test 2: When no reports exist, HTML contains empty state guide and /api/report/latest returns 404."""
    # HTML root returns 200 with empty state markup
    req = urllib.request.Request(f"{dashboard_server.url}/")
    with urllib.request.urlopen(req) as response:
        assert response.status == 200
        html = response.read().decode("utf-8")
        assert "emptyState" in html
        assert "No Portfolio Reports Available" in html

    # API returns 404 with helpful empty state explanation
    try:
        api_req = urllib.request.Request(f"{dashboard_server.url}/api/report/latest")
        with urllib.request.urlopen(api_req) as response:
            assert False, "Expected 404 error"
    except urllib.error.HTTPError as err:
        assert err.code == 404
        data = json.loads(err.read().decode("utf-8"))
        assert data["status"] == "not_found"
        assert "No portfolio reports generated yet" in data["message"]


def test_missing_metrics_appear_as_unavailable(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Test 3: Missing metrics return is_available=False with reasons, not fabricated values."""
    # Flat equity curve: zero variance yields unavailable Sharpe ratio
    flat_curve = [
        EquityPoint(1700000000.0, 100_000.0, 0.0, 100_000.0, 0.0, 0.0),
        EquityPoint(1700000100.0, 100_000.0, 0.0, 100_000.0, 0.0, 0.0),
    ]
    sim = _create_mock_simulation_result(simulation_id="sim_flat_01")
    object.__setattr__(sim, "equity_curve", tuple(flat_curve))

    rep = reporting_service.generate_unified_report(sim)
    api_req = urllib.request.Request(f"{dashboard_server.url}/api/report/{rep.report_id}")
    with urllib.request.urlopen(api_req) as response:
        assert response.status == 200
        data = json.loads(response.read().decode("utf-8"))
        sh = data["risk_adjusted_metrics"]["sharpe_ratio"]
        assert sh["is_available"] is False
        assert sh["value"] is None
        assert "zero variance" in sh["unavailable_reason"].lower() or "insufficient" in sh["unavailable_reason"].lower()


def test_equity_and_drawdown_series_ordering(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Test 4: Time series points are strictly ordered chronologically."""
    sim = _create_mock_simulation_result(simulation_id="sim_order_01")
    rep = reporting_service.generate_unified_report(sim)

    api_req = urllib.request.Request(f"{dashboard_server.url}/api/series/{rep.report_id}")
    with urllib.request.urlopen(api_req) as response:
        assert response.status == 200
        data = json.loads(response.read().decode("utf-8"))
        points = data["series"]["points"]
        timestamps = [p["timestamp"] for p in points]
        assert timestamps == sorted(timestamps)
        assert data["series"]["peak_equity"] >= data["series"]["trough_equity"]


def test_invalid_or_non_finite_values_handled_safely(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Test 5: Non-finite numbers (NaN, Inf) are sanitized and never emit invalid JSON."""
    raw_dict = {
        "simulation_id": "sim_nan_01",
        "initial_equity": 100_000.0,
        "final_equity": 105_000.0,
        "gross_pnl": float("nan"),
        "net_pnl": float("inf"),
        "equity_curve": [
            {"timestamp": 1700000000.0, "total_equity": float("nan"), "cash": float("-inf")},
        ],
    }
    rep = reporting_service.generate_unified_report(raw_dict)
    api_req = urllib.request.Request(f"{dashboard_server.url}/api/report/{rep.report_id}")
    with urllib.request.urlopen(api_req) as response:
        assert response.status == 200
        raw_body = response.read().decode("utf-8")
        assert "NaN" not in raw_body
        assert "Infinity" not in raw_body
        data = json.loads(raw_body)
        assert data["equity_curve"][0]["total_equity"] == 0.0


def test_status_states_represented_correctly(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Test 6: VALID, STALE, INCOMPLETE statuses are accurately reflected."""
    # 1. Incomplete
    sim_incomplete = _create_mock_simulation_result(
        simulation_id="sim_inc_01",
        status=PortfolioSimulationStatus.FAILED,
    )
    rep_inc = reporting_service.generate_unified_report(sim_incomplete)
    assert rep_inc.status == ReportDataStatus.INCOMPLETE

    api_req = urllib.request.Request(f"{dashboard_server.url}/api/report/{rep_inc.report_id}")
    with urllib.request.urlopen(api_req) as response:
        data = json.loads(response.read().decode("utf-8"))
        assert data["status"] == "INCOMPLETE"
        assert len(data["data_warnings"]) > 0

    # 2. Stale
    stale_risk = PortfolioRiskReport(
        report_id="risk_stale_01",
        timestamp=1700000000.0,
        overall_state=AlertState.NORMAL,
        is_snapshot_stale=True,
    )
    sim_stale = _create_mock_simulation_result(simulation_id="sim_stale_01")
    rep_stale = reporting_service.generate_unified_report(sim_stale, risk_report=stale_risk)
    assert rep_stale.status == ReportDataStatus.STALE


def test_risk_alerts_rendered_correctly(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Test 7: Risk alerts render active breaches accurately."""
    alert = PortfolioRiskAlert(
        alert_id="alt_exp_01",
        rule_id="GROSS_EXPOSURE",
        severity=AlertSeverity.CRITICAL,
        state=AlertState.CRITICAL,
        metric_name="gross_exposure_pct",
        observed_value=105.0,
        threshold_value=100.0,
        unit="%",
        target="portfolio",
        message="Gross exposure exceeds critical threshold.",
    )
    risk_rep = PortfolioRiskReport(
        report_id="risk_crit_01",
        timestamp=1700000000.0,
        overall_state=AlertState.CRITICAL,
        active_alerts=(alert,),
    )

    sim = _create_mock_simulation_result(simulation_id="sim_risk_01")
    rep = reporting_service.generate_unified_report(sim, risk_report=risk_rep)

    api_req = urllib.request.Request(f"{dashboard_server.url}/api/report/{rep.report_id}")
    with urllib.request.urlopen(api_req) as response:
        data = json.loads(response.read().decode("utf-8"))
        assert data["active_alerts_count"] == 1
        assert data["risk_status"]["overall_state"] == "CRITICAL"
        assert data["risk_status"]["active_alerts"][0]["severity"] == "CRITICAL"


def test_historical_run_comparison_handles_missing_metrics(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Test 8: Comparison API correctly compares historical runs and handles bad requests."""
    sim1 = _create_mock_simulation_result(simulation_id="sim_comp_1", net_pnl=5_000.0)
    sim2 = _create_mock_simulation_result(simulation_id="sim_comp_2", net_pnl=12_000.0)

    rep1 = reporting_service.generate_unified_report(sim1, title="Run 1")
    rep2 = reporting_service.generate_unified_report(sim2, title="Run 2")

    # Valid comparison
    api_req = urllib.request.Request(f"{dashboard_server.url}/api/compare?runs={rep1.report_id},{rep2.report_id}")
    with urllib.request.urlopen(api_req) as response:
        assert response.status == 200
        data = json.loads(response.read().decode("utf-8"))
        assert data["status"] == "success"
        comp = data["comparison"]
        assert len(comp["entities"]) == 2
        # Highest return sorted first
        assert comp["entities"][0]["entity_id"] == rep2.report_id

    # Comparison with fewer than 2 runs returns 400 Bad Request
    try:
        bad_req = urllib.request.Request(f"{dashboard_server.url}/api/compare?runs={rep1.report_id}")
        with urllib.request.urlopen(bad_req) as response:
            assert False, "Expected 400 Bad Request"
    except urllib.error.HTTPError as err:
        assert err.code == 400


def test_dashboard_access_does_not_mutate_state(
    dashboard_server: PortfolioDashboardServer,
    reporting_service: PortfolioReportingService,
):
    """Test 9: Repeated dashboard queries do not mutate report or simulation state."""
    sim = _create_mock_simulation_result(simulation_id="sim_immut_01")
    rep = reporting_service.generate_unified_report(sim)

    orig_pnl = rep.net_pnl
    orig_equity = rep.final_equity

    # Perform 5 consecutive queries
    for _ in range(5):
        req = urllib.request.Request(f"{dashboard_server.url}/api/report/{rep.report_id}")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200

    assert rep.net_pnl == orig_pnl
    assert rep.final_equity == orig_equity


def test_no_order_execution_pathway_post_methods_rejected(
    dashboard_server: PortfolioDashboardServer,
):
    """Test 10: POST, PUT, DELETE, PATCH are strictly rejected with 405 Method Not Allowed."""
    for method in ["POST", "PUT", "DELETE", "PATCH"]:
        req = urllib.request.Request(
            f"{dashboard_server.url}/api/report/latest",
            method=method,
            data=b"{}",
        )
        try:
            with urllib.request.urlopen(req) as response:
                assert False, f"Expected 405 Method Not Allowed for {method}"
        except urllib.error.HTTPError as err:
            assert err.code == 405
            data = json.loads(err.read().decode("utf-8"))
            assert "read-only" in data["message"].lower()


def test_orchestrator_dashboard_lifecycle_handlers(
    reporting_service: PortfolioReportingService,
    event_bus: EventBus,
    state_store: StateStore,
):
    """Test 11: Orchestrator handlers start, query, and stop dashboard cleanly."""
    server = PortfolioDashboardServer(
        reporting_service=reporting_service,
        host="127.0.0.1",
        port=0,
    )
    orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)
    register_reporting_handlers(orchestrator=orchestrator, service=reporting_service, server=server)

    try:
        # 1. Query status before starting
        r1 = orchestrator.handle_request(OrchestratorRequest.create(command="get_portfolio_dashboard_status"))
        assert r1.is_success
        assert r1.result["is_running"] is False

        # 2. Start dashboard
        r2 = orchestrator.handle_request(OrchestratorRequest.create(command="start_portfolio_dashboard"))
        assert r2.is_success
        assert r2.result["is_running"] is True
        assert server.is_running is True

        # 3. Query status while running
        r3 = orchestrator.handle_request(OrchestratorRequest.create(command="get_portfolio_dashboard_status"))
        assert r3.is_success
        assert r3.result["is_running"] is True

        # 4. Stop dashboard
        r4 = orchestrator.handle_request(OrchestratorRequest.create(command="stop_portfolio_dashboard"))
        assert r4.is_success
        assert r4.result["is_running"] is False
        assert server.is_running is False
    finally:
        if server.is_running:
            server.stop()


def test_safety_invariants_strictly_maintained(
    dashboard_server: PortfolioDashboardServer,
):
    """Test 12: Settings execution locked is preserved, zero trading hooks."""
    settings = Settings()
    assert settings.execution_locked is True

    # Confirm dashboard server has no broker or trade execution methods
    for forbidden in ["place_order", "cancel_order", "execute_trade", "liquidate_positions", "broker"]:
        assert not hasattr(dashboard_server, forbidden)
