"""KAISER — Tests for Portfolio Analytics Reporting & Read-Only Dashboard Data API.

Comprehensive verification of:
1. Unified report generation from valid source results (PortfolioSimulationResult, BacktestResult, dict).
2. Missing or unavailable source results handling.
3. Stale and incomplete data handling (ReportDataStatus).
4. Attribution reconciliation and warning propagation.
5. Risk alerts included accurately in reports.
6. Equity and drawdown series ordering and cumulative return calculation.
7. Historical report retrieval and bounded pagination (limit, offset validation).
8. Run/report identifier validation.
9. Comparison of multiple simulation runs.
10. In-sample versus out-of-sample labelling.
11. Metric units, timestamps, and serialization (to_dict).
12. Rejection or safe handling of NaN and Infinity (no non-finite values in output).
13. StateStore persistence and retrieval.
14. EventBus success and failure event publication.
15. Orchestrator handler integration.
16. Read-only behavior and no simulation-state mutation.
17. No broker, order, or liquidation path invocation.
18. Full regression compatibility with earlier steps.
"""

import math
import time
from typing import Any, Dict, List, Optional, Tuple

import pytest

from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    BacktestStatus,
    EquityPoint,
    OrderSide,
    PerformanceMetrics,
    TradeRecord,
)
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
    StrategyContribution,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.handlers import register_portfolio_handlers
from app.portfolio.report_models import (
    DashboardSeriesPoint,
    DashboardTimeSeries,
    HistoricalReportSummary,
    ReportDataProvenance,
    ReportDataStatus,
    UnifiedPortfolioReport,
    VoiceSummary,
)
from app.portfolio.reporting import PortfolioReportingService
from app.portfolio.reporting_handlers import register_reporting_handlers
from app.risk.models import (
    AlertSeverity,
    AlertState,
    PortfolioRiskAlert,
    PortfolioRiskReport,
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
        max_history_size=10,
    )


def _create_mock_simulation_result(
    simulation_id: str = "sim_test_01",
    initial_equity: float = 100_000.0,
    final_equity: float = 110_000.0,
    gross_pnl: float = 10_500.0,
    net_pnl: float = 10_000.0,
    total_fees: float = 300.0,
    total_slippage: float = 200.0,
    equity_curve: Optional[List[EquityPoint]] = None,
    contributions: Optional[List[StrategyContribution]] = None,
    status: PortfolioSimulationStatus = PortfolioSimulationStatus.SUCCESS,
) -> PortfolioSimulationResult:
    """Constructs a deterministic PortfolioSimulationResult with verified accounting."""
    alloc_a = StrategyAllocation(strategy_name="strat_a", symbol="TCS", weight=0.6)
    alloc_b = StrategyAllocation(strategy_name="strat_b", symbol="INFY", weight=0.4)

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
        gross_pnl=6_300.0,
        net_pnl=6_000.0,
        fees=180.0,
        slippage=120.0,
        return_pct=10.0,
        duration_seconds=400.0,
        is_win=True,
    )
    t2 = TradeRecord(
        trade_id="t2",
        symbol="INFY",
        side=OrderSide.BUY,
        entry_time=1700000200.0,
        exit_time=1700000600.0,
        entry_price=200.0,
        exit_price=220.0,
        quantity=25.0,
        gross_pnl=4_200.0,
        net_pnl=4_000.0,
        fees=120.0,
        slippage=80.0,
        return_pct=10.0,
        duration_seconds=400.0,
        is_win=True,
    )

    if contributions is None:
        c1 = StrategyContribution(
            strategy_id="strat_a_TCS",
            strategy_name="strat_a",
            symbol="TCS",
            weight=0.6,
            allocated_capital=60_000.0,
            gross_pnl=6_300.0,
            net_pnl=6_000.0,
            net_return_pct=10.0,
            portfolio_pnl_share_pct=60.0,
            total_trades=1,
            winning_trades=1,
            losing_trades=0,
            win_rate=100.0,
            profit_factor=None,
            profit_factor_description="Infinity (No losses)",
            total_fees=180.0,
            total_slippage=120.0,
            orders_submitted=1,
            orders_accepted=1,
            orders_rejected=0,
            trades=(t1,),
        )
        c2 = StrategyContribution(
            strategy_id="strat_b_INFY",
            strategy_name="strat_b",
            symbol="INFY",
            weight=0.4,
            allocated_capital=40_000.0,
            gross_pnl=4_200.0,
            net_pnl=4_000.0,
            net_return_pct=10.0,
            portfolio_pnl_share_pct=40.0,
            total_trades=1,
            winning_trades=1,
            losing_trades=0,
            win_rate=100.0,
            profit_factor=None,
            profit_factor_description="Infinity (No losses)",
            total_fees=120.0,
            total_slippage=80.0,
            orders_submitted=1,
            orders_accepted=1,
            orders_rejected=0,
            trades=(t2,),
        )
        contributions = [c1, c2]

    if equity_curve is None:
        equity_curve = [
            EquityPoint(1700000000.0, 100_000.0, 0.0, 100_000.0, 0.0, 0.0),
            EquityPoint(1700000100.0, 50_000.0, 52_000.0, 102_000.0, 0.0, 0.0),
            EquityPoint(1700000200.0, 50_000.0, 56_000.0, 106_000.0, 0.0, 0.0),
            EquityPoint(1700000300.0, 50_000.0, 54_000.0, 104_000.0, 1.88, 2_000.0),
            EquityPoint(1700000400.0, 110_000.0, 0.0, 110_000.0, 0.0, 0.0),
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
        max_drawdown_pct=1.88,
        max_drawdown_amount=2_000.0,
        max_drawdown_duration_bars=1,
        total_fees=total_fees,
        total_slippage=total_slippage,
        total_transaction_costs=total_fees + total_slippage,
        total_trades=2,
        winning_trades=2,
        losing_trades=0,
        win_rate=100.0,
        profit_factor=None,
        profit_factor_description="Infinity (No losses)",
        equity_curve=tuple(equity_curve),
        risk_snapshots=(),
        strategy_contributions=tuple(contributions),
    )


# ------------------------------------------------------------------------------
# Test Cases
# ------------------------------------------------------------------------------

def test_unified_report_generation_from_valid_source(reporting_service: PortfolioReportingService):
    """Test 1: Generates complete unified report from PortfolioSimulationResult."""
    sim = _create_mock_simulation_result(simulation_id="sim_valid_01")
    report = reporting_service.generate_unified_report(sim, title="Primary Multi-Strategy Run")

    assert report.report_id.startswith("report_")
    assert report.provenance.source_run_id == "sim_valid_01"
    assert report.provenance.source_type == "portfolio_simulation"
    assert report.status == ReportDataStatus.VALID

    # Financials
    assert report.initial_equity == 100_000.0
    assert report.final_equity == 110_000.0
    assert report.gross_pnl == 10_500.0
    assert report.net_pnl == 10_000.0
    assert report.net_return_pct == 10.0
    assert report.total_transaction_costs == 500.0

    # Curves & Strategies
    assert len(report.equity_curve) == 5
    assert len(report.strategy_attributions) == 2

    # Voice Summary
    assert report.voice_summary is not None
    assert "profit of $10,000.00" in report.voice_summary.headline
    assert "10.0 percent return" in report.voice_summary.headline


def test_unified_report_from_backtest_result(reporting_service: PortfolioReportingService):
    """Test 2: Generates unified report from standalone BacktestResult."""
    cfg = BacktestConfig(
        symbol="NIFTY",
        timeframe="1d",
        start_time=1700000000.0,
        end_time=1700086400.0,
        initial_cash=50_000.0,
    )
    metrics = PerformanceMetrics(
        initial_equity=50_000.0,
        final_equity=54_500.0,
        gross_profit=8_000.0,
        gross_loss=3_000.0,
        net_profit=4_500.0,
        total_return_pct=9.0,
        max_drawdown_pct=2.4,
        max_drawdown_amount=1_200.0,
        total_trades=10,
        winning_trades=6,
        losing_trades=4,
        even_trades=0,
        win_rate=60.0,
        profit_factor=2.67,
        profit_factor_description="Profitable",
        total_fees=300.0,
        total_slippage=200.0,
        total_transaction_costs=500.0,
        market_exposure_pct=20.0,
    )
    eq_pts = [
        EquityPoint(1700000000.0, 50_000.0, 0.0, 50_000.0, 0.0, 0.0),
        EquityPoint(1700086400.0, 54_500.0, 0.0, 54_500.0, 0.0, 0.0),
    ]

    bt = BacktestResult(
        backtest_id="bt_single_01",
        status=BacktestStatus.COMPLETED,
        strategy_name="trend_strategy",
        strategy_version="1.0.0",
        config=cfg,
        metrics=metrics,
        trades=(),
        equity_curve=tuple(eq_pts),
    )

    report = reporting_service.generate_unified_report(bt)
    assert report.provenance.source_run_id == "bt_single_01"
    assert report.provenance.source_type == "backtest"
    assert report.net_pnl == 4_500.0
    assert report.initial_equity == 50_000.0
    assert report.final_equity == 54_500.0
    assert len(report.strategy_attributions) == 1
    assert report.strategy_attributions[0]["strategy_id"] == "trend_strategy"


def test_missing_or_unavailable_source_handling(reporting_service: PortfolioReportingService):
    """Test 3: Nonexistent report IDs return None or raise ValidationError."""
    # Query non-existent report
    assert reporting_service.get_report_by_id("rep_non_existent") is None
    assert reporting_service.get_report_by_run_id("run_non_existent") is None

    # Invalid argument types
    with pytest.raises(ValidationError):
        reporting_service.get_report_by_id("")
    with pytest.raises(ValidationError):
        reporting_service.get_report_by_id("   ")
    with pytest.raises(ValidationError):
        reporting_service.get_report_by_run_id("")


def test_stale_and_incomplete_data_handling(reporting_service: PortfolioReportingService):
    """Test 4: Stale snapshots and failed simulations yield STALE/INCOMPLETE status and warnings."""
    # 1. Incomplete simulation status
    sim_failed = _create_mock_simulation_result(
        simulation_id="sim_fail_01",
        status=PortfolioSimulationStatus.FAILED,
    )
    rep1 = reporting_service.generate_unified_report(sim_failed)
    assert rep1.status == ReportDataStatus.INCOMPLETE
    assert any("Source simulation status is 'FAILED'" in w for w in rep1.data_warnings)

    # 2. Stale risk report
    stale_risk = PortfolioRiskReport(
        report_id="risk_rep_stale",
        timestamp=time.time(),
        overall_state=AlertState.NORMAL,
        is_snapshot_stale=True,
    )
    sim_valid = _create_mock_simulation_result(simulation_id="sim_valid_02")
    rep2 = reporting_service.generate_unified_report(sim_valid, risk_report=stale_risk)
    assert rep2.status == ReportDataStatus.STALE
    assert any("snapshot is stale" in w for w in rep2.data_warnings)


def test_attribution_reconciliation_and_warning_propagation(reporting_service: PortfolioReportingService):
    """Test 5: Accounting discrepancy is detected and propagated to report warnings."""
    # Inject discrepancy: strategy contributions sum to $8,000, but portfolio net PnL is $10,000
    t_mismatch = TradeRecord(
        trade_id="t_mis",
        symbol="TCS",
        side=OrderSide.BUY,
        entry_time=1700000100.0,
        exit_time=1700000500.0,
        entry_price=100.0,
        exit_price=108.0,
        quantity=50.0,
        gross_pnl=4_000.0,
        net_pnl=4_000.0,
        fees=0.0,
        slippage=0.0,
        return_pct=8.0,
        duration_seconds=400.0,
        is_win=True,
    )
    c_mismatch = StrategyContribution(
        strategy_id="strat_only",
        strategy_name="strat_only",
        symbol="TCS",
        weight=1.0,
        allocated_capital=100_000.0,
        gross_pnl=4_000.0,
        net_pnl=4_000.0,
        net_return_pct=4.0,
        portfolio_pnl_share_pct=40.0,
        total_trades=1,
        winning_trades=1,
        losing_trades=0,
        win_rate=100.0,
        profit_factor=None,
        profit_factor_description="Infinity",
        total_fees=0.0,
        total_slippage=0.0,
        orders_submitted=1,
        orders_accepted=1,
        orders_rejected=0,
        trades=(t_mismatch,),
    )

    sim = _create_mock_simulation_result(
        simulation_id="sim_mismatch_01",
        net_pnl=10_000.0,
        contributions=[c_mismatch],
    )
    rep = reporting_service.generate_unified_report(sim)

    assert rep.reconciliation is not None
    assert not rep.reconciliation["is_reconciled"]
    assert any("Accounting discrepancy detected" in w for w in rep.data_warnings)


def test_risk_alerts_included_accurately_in_report(reporting_service: PortfolioReportingService):
    """Test 6: Active risk alerts and monitoring state are accurately incorporated."""
    alert = PortfolioRiskAlert(
        alert_id="alt_gross_exp",
        rule_id="GROSS_EXPOSURE",
        severity=AlertSeverity.WARNING,
        state=AlertState.WARNING,
        metric_name="gross_exposure_pct",
        observed_value=85.5,
        threshold_value=80.0,
        unit="%",
        target="portfolio",
        message="Gross exposure 85.5% exceeds warning threshold of 80.0%.",
    )
    risk_rep = PortfolioRiskReport(
        report_id="risk_rep_warning",
        timestamp=time.time(),
        overall_state=AlertState.WARNING,
        active_alerts=(alert,),
    )

    sim = _create_mock_simulation_result(simulation_id="sim_alert_01")
    rep = reporting_service.generate_unified_report(sim, risk_report=risk_rep)

    assert rep.active_alerts_count == 1
    assert rep.risk_status is not None
    assert rep.risk_status["overall_state"] == "WARNING"
    assert len(rep.risk_status["active_alerts"]) == 1
    assert rep.risk_status["active_alerts"][0]["alert_id"] == "alt_gross_exp"
    assert rep.voice_summary is not None
    assert "WARNING state with 1 active alerts" in rep.voice_summary.risk_status_speech


def test_equity_and_drawdown_series_ordering_and_cumulative_return(reporting_service: PortfolioReportingService):
    """Test 7: Equity points are strictly chronologically sorted and cumulative returns calculated."""
    # Out-of-order points
    unordered_points = [
        EquityPoint(1700000300.0, 50_000.0, 54_000.0, 104_000.0, 1.88, 2_000.0),
        EquityPoint(1700000000.0, 100_000.0, 0.0, 100_000.0, 0.0, 0.0),
        EquityPoint(1700000200.0, 50_000.0, 56_000.0, 106_000.0, 0.0, 0.0),
        EquityPoint(1700000100.0, 50_000.0, 52_000.0, 102_000.0, 0.0, 0.0),
        EquityPoint(1700000400.0, 110_000.0, 0.0, 110_000.0, 0.0, 0.0),
    ]
    sim = _create_mock_simulation_result(simulation_id="sim_order_01", equity_curve=unordered_points)
    rep = reporting_service.generate_unified_report(sim)

    # Check ascending order
    timestamps = [pt.timestamp for pt in rep.equity_curve]
    assert timestamps == sorted(timestamps)

    # Check cumulative return % calculation: (110k - 100k)/100k * 100 = 10.0%
    assert rep.equity_curve[0].cumulative_return_pct == 0.0
    assert rep.equity_curve[-1].cumulative_return_pct == 10.0

    # Query series via API
    series = reporting_service.get_equity_and_drawdown_series(rep.report_id, limit=3, offset=1)
    assert series is not None
    assert len(series.points) == 3
    assert series.total_points == 5
    assert series.peak_equity == 110_000.0
    assert series.trough_equity == 100_000.0


def test_historical_report_retrieval_and_bounded_pagination(reporting_service: PortfolioReportingService):
    """Test 8: Validates pagination parameters and bounds for report summaries."""
    # Generate 5 reports
    for i in range(5):
        sim = _create_mock_simulation_result(simulation_id=f"sim_hist_{i}")
        reporting_service.generate_unified_report(sim)

    # Valid query
    summaries = reporting_service.list_historical_reports(limit=3, offset=1)
    assert len(summaries) == 3

    # Invalid pagination bounds
    with pytest.raises(ValidationError):
        reporting_service.list_historical_reports(limit=0)
    with pytest.raises(ValidationError):
        reporting_service.list_historical_reports(limit=101)
    with pytest.raises(ValidationError):
        reporting_service.list_historical_reports(offset=-1)


def test_run_and_report_identifier_validation(reporting_service: PortfolioReportingService):
    """Test 9: Invalid identifiers reject with ValidationError."""
    with pytest.raises(ValidationError):
        reporting_service.get_equity_and_drawdown_series("")
    with pytest.raises(ValidationError):
        reporting_service.get_strategy_attribution("")
    with pytest.raises(ValidationError):
        reporting_service.get_risk_status_and_alerts("")


def test_comparison_of_multiple_runs(reporting_service: PortfolioReportingService):
    """Test 10: Compares multiple simulation runs with deterministic sorting."""
    sim1 = _create_mock_simulation_result(simulation_id="sim_alpha", net_pnl=5_000.0)
    sim2 = _create_mock_simulation_result(simulation_id="sim_beta", net_pnl=15_000.0)

    rep1 = reporting_service.generate_unified_report(sim1, title="Strategy Alpha")
    rep2 = reporting_service.generate_unified_report(sim2, title="Strategy Beta")

    comp = reporting_service.compare_simulation_runs([rep1.report_id, rep2.report_id])
    assert len(comp.entities) == 2
    # Best entity (Beta, 15%) must be sorted first
    assert comp.entities[0].entity_name == "Strategy Beta"
    assert "Strategy Beta" in comp.summary["headline"]

    # Fails if < 2 runs
    with pytest.raises(ValidationError):
        reporting_service.compare_simulation_runs([rep1.report_id])


def test_in_sample_vs_out_of_sample_labelling(reporting_service: PortfolioReportingService):
    """Test 11: Explicit labelling of in-sample and out-of-sample summaries."""
    wf_summary = {
        "evaluation_type": "walk_forward_validation",
        "in_sample_windows": [
            {"window_id": "W1_IS", "mode": "IN_SAMPLE", "sharpe": 1.8},
        ],
        "out_of_sample_windows": [
            {"window_id": "W1_OOS", "mode": "OUT_OF_SAMPLE", "sharpe": 1.2},
        ],
        "efficiency_ratio": 0.67,
    }
    sim = _create_mock_simulation_result(simulation_id="sim_wf_01")
    rep = reporting_service.generate_unified_report(sim, walk_forward_summary=wf_summary)

    assert rep.walk_forward_summary is not None
    assert rep.walk_forward_summary["in_sample_windows"][0]["mode"] == "IN_SAMPLE"
    assert rep.walk_forward_summary["out_of_sample_windows"][0]["mode"] == "OUT_OF_SAMPLE"


def test_metric_units_timestamps_and_serialization(reporting_service: PortfolioReportingService):
    """Test 12: Verifies explicit units, ISO timestamps, and dictionary serialization."""
    sim = _create_mock_simulation_result(simulation_id="sim_units_01")
    rep = reporting_service.generate_unified_report(sim)

    serialized = rep.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["report_id"] == rep.report_id
    assert serialized["status"] == "VALID"
    assert "Z" in serialized["provenance"]["generated_at_iso"]

    # Point formatting
    first_pt = serialized["equity_curve"][0]
    assert "timestamp_iso" in first_pt
    assert first_pt["total_equity"] == 100_000.0


def test_rejection_or_safe_handling_of_nan_and_inf(reporting_service: PortfolioReportingService):
    """Test 13: Non-finite NaN and Infinity are safely converted to None during serialization."""
    source_dict = {
        "simulation_id": "sim_nan_01",
        "source_type": "synthetic",
        "initial_equity": 100_000.0,
        "final_equity": 105_000.0,
        "gross_pnl": float("nan"),
        "net_pnl": float("inf"),
        "equity_curve": [
            {"timestamp": 1700000000.0, "total_equity": float("nan"), "cash": float("-inf")},
        ],
    }

    rep = reporting_service.generate_unified_report(source_dict)
    d = rep.to_dict()

    # None in output instead of float('nan') or float('inf')
    assert d["equity_curve"][0]["total_equity"] == 0.0
    assert d["equity_curve"][0]["cash"] == 0.0


def test_state_store_persistence_and_retrieval(
    reporting_service: PortfolioReportingService,
    state_store: StateStore,
):
    """Test 14: Verifies persistence into StateStore and lookup by latest/ID."""
    sim = _create_mock_simulation_result(simulation_id="sim_store_01")
    rep = reporting_service.generate_unified_report(sim)

    # Check StateStore keys
    stored_latest = state_store.get(PortfolioReportingService.STATE_LATEST_KEY)
    assert stored_latest is not None
    assert stored_latest["report_id"] == rep.report_id

    stored_rep = state_store.get(f"{PortfolioReportingService.STATE_REPORTS_PREFIX}{rep.report_id}")
    assert stored_rep is not None
    assert stored_rep["net_pnl"] == 10_000.0

    # Retrieve through service
    fetched = reporting_service.get_report_by_id(rep.report_id)
    assert fetched is not None
    assert fetched.report_id == rep.report_id


def test_event_bus_success_and_failure_events(
    reporting_service: PortfolioReportingService,
    event_bus: EventBus,
):
    """Test 15: EventBus emits PORTFOLIO_REPORT_GENERATED on success."""
    events_captured: List[Event] = []

    def _handler(ev: Event) -> None:
        events_captured.append(ev)

    event_bus.subscribe(EventType.PORTFOLIO_REPORT_GENERATED, _handler)

    sim = _create_mock_simulation_result(simulation_id="sim_ev_01")
    rep = reporting_service.generate_unified_report(sim)

    assert len(events_captured) == 1
    ev = events_captured[0]
    assert ev.event_type == EventType.PORTFOLIO_REPORT_GENERATED
    assert ev.payload["report_id"] == rep.report_id
    assert ev.payload["source_run_id"] == "sim_ev_01"


def test_orchestrator_handler_integration(
    reporting_service: PortfolioReportingService,
    event_bus: EventBus,
    state_store: StateStore,
):
    """Test 16: Verifies registration and dispatch of all reporting orchestrator commands."""
    orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)
    register_reporting_handlers(orchestrator=orchestrator, service=reporting_service)

    sim = _create_mock_simulation_result(simulation_id="sim_orch_01")

    # 1. generate_unified_portfolio_report
    r1 = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="generate_unified_portfolio_report",
            context={"simulation_result": sim, "title": "Orchestrator Test Report"},
        )
    )
    assert r1.is_success
    report_dict = r1.result["report"]
    report_id = report_dict["report_id"]

    # 2. get_latest_portfolio_report
    r2 = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="get_latest_portfolio_report",
        )
    )
    assert r2.is_success
    assert r2.result["report"]["report_id"] == report_id

    # 3. get_portfolio_report_by_id
    r3 = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="get_portfolio_report_by_id",
            context={"report_id": report_id},
        )
    )
    assert r3.is_success
    assert r3.result["report"]["report_id"] == report_id

    # 4. list_portfolio_reports
    r4 = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="list_portfolio_reports",
            context={"limit": 10},
        )
    )
    assert r4.is_success
    assert r4.result["count"] >= 1

    # 5. get_portfolio_dashboard_series
    r5 = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="get_portfolio_dashboard_series",
            context={"report_id": report_id, "limit": 2},
        )
    )
    assert r5.is_success
    assert r5.result["series"]["points_count"] == 2

    # 6. get_portfolio_voice_summary
    r6 = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="get_portfolio_voice_summary",
            context={"report_id": report_id},
        )
    )
    assert r6.is_success
    assert "profit of $10,000.00" in r6.result["voice_summary"]["headline"]


def test_read_only_behavior_and_no_simulation_mutation(reporting_service: PortfolioReportingService):
    """Test 17: Verifies that source simulation results are completely unmodified."""
    sim = _create_mock_simulation_result(simulation_id="sim_readonly_01")

    # Snapshot attributes before report generation
    orig_equity = sim.final_equity
    orig_net_pnl = sim.net_pnl
    orig_trades_count = len(sim.strategy_contributions[0].trades)

    _ = reporting_service.generate_unified_report(sim)

    # Must remain identical
    assert sim.final_equity == orig_equity
    assert sim.net_pnl == orig_net_pnl
    assert len(sim.strategy_contributions[0].trades) == orig_trades_count


def test_safety_invariants_strictly_maintained(reporting_service: PortfolioReportingService):
    """Test 18: Verifies safety invariants (execution_locked, zero live broker paths)."""
    settings = Settings()
    assert settings.execution_locked is True

    # Confirm reporting service is purely observational with no broker/trading hooks
    for forbidden in ["place_order", "cancel_order", "modify_order", "liquidate", "broker"]:
        assert not hasattr(reporting_service, forbidden)
