"""KAISER — Tests for Portfolio Performance Attribution & Risk-Adjusted Metrics.

Comprehensive verification of:
1. Known gross and net P&L calculations.
2. Strategy contribution reconciliation (exact and tolerance checks).
3. Instrument attribution aggregation.
4. Transaction-cost reconciliation.
5. Sharpe and Sortino calculations with known return series.
6. Annualization assumptions and configuration validation.
7. Volatility and maximum drawdown (percentage, amount, duration).
8. Profit factor, win rate, and average win/loss.
9. Zero variance and insufficient observations (< 2 periods).
10. Missing, non-finite (NaN, Inf), and inconsistent data handling.
11. Zero denominators and undefined metrics (safe None returns with reasons).
12. Unrealized versus realized P&L accounting.
13. In-sample versus out-of-sample comparison separation.
14. Deterministic results and numerical stability.
15. StateStore and EventBus integration.
16. Orchestrator handler integration.
17. Safety invariant verification (Settings.execution_locked = True, zero execution paths).
"""

import math
from typing import List, Optional, Tuple

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
from app.backtesting.robustness.models import (
    RobustnessStatus,
    RobustnessSummary,
    RobustnessTestConfig,
    RobustnessTestResult,
    ScenarioType,
    StressScenario,
    StressScenarioResult,
)
from app.backtesting.validation.models import (
    OutOfSampleSummary,
    ValidationWindow,
    ValidationWindowResult,
    WalkForwardConfig,
    WalkForwardResult,
    WalkForwardStatus,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.handlers import register_portfolio_handlers
from app.portfolio.metrics import (
    calculate_annualized_volatility,
    calculate_calmar_ratio,
    calculate_equity_returns,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
    calculate_sortino_ratio,
    calculate_trade_metrics,
    compute_risk_adjusted_metrics,
)
from app.portfolio.models import (
    AttributionConfig,
    ComparisonType,
    MetricValue,
    PortfolioAttributionReport,
    ReconciliationStatus,
)


# ------------------------------------------------------------------------------
# Test Fixtures & Helpers
# ------------------------------------------------------------------------------

@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def state_store() -> StateStore:
    return StateStore()


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
        status=PortfolioSimulationStatus.SUCCESS,
        config=cfg,
        initial_equity=initial_equity,
        final_equity=final_equity,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        net_return_pct=((final_equity - initial_equity) / initial_equity) * 100.0,
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
        strategy_contributions=tuple(contributions),
        bars_processed=4,
    )


# ------------------------------------------------------------------------------
# 1. P&L & Strategy Contribution Reconciliation Tests
# ------------------------------------------------------------------------------

def test_gross_and_net_pnl_reconciliation():
    """Verify exact reconciliation between gross P&L, transaction costs, and net P&L."""
    res = _create_mock_simulation_result(
        gross_pnl=10_500.0,
        total_fees=300.0,
        total_slippage=200.0,
        net_pnl=10_000.0,
    )
    service = PortfolioAttributionService()
    report = service.analyze_simulation_result(res)

    assert report.gross_pnl == 10_500.0
    assert report.total_costs.total_costs == 500.0
    assert report.net_pnl == 10_000.0
    assert report.reconciliation is not None
    assert report.reconciliation.status == ReconciliationStatus.EXACT
    assert report.reconciliation.is_reconciled is True
    assert report.reconciliation.strategy_discrepancy == 0.0
    assert report.reconciliation.gross_to_net_discrepancy == 0.0


def test_strategy_contribution_reconciliation():
    """Verify sum of strategy net PnLs matches portfolio net PnL."""
    res = _create_mock_simulation_result()
    service = PortfolioAttributionService()
    report = service.analyze_simulation_result(res)

    assert len(report.strategy_attributions) == 2
    strat_a = next(s for s in report.strategy_attributions if s.strategy_name == "strat_a")
    strat_b = next(s for s in report.strategy_attributions if s.strategy_name == "strat_b")

    assert strat_a.net_pnl == 6_000.0
    assert strat_b.net_pnl == 4_000.0
    assert strat_a.portfolio_pnl_share_pct == 60.0
    assert strat_b.portfolio_pnl_share_pct == 40.0
    assert (strat_a.net_pnl + strat_b.net_pnl) == report.net_pnl


def test_reconciliation_discrepancy_detected():
    """Verify DISCREPANCY_DETECTED when strategy PnL sums diverge from portfolio net PnL."""
    # Force an inconsistent portfolio simulation result
    res = _create_mock_simulation_result(net_pnl=12_000.0)  # Strategy sum is 10,000, mismatch!
    service = PortfolioAttributionService()
    report = service.analyze_simulation_result(res)

    assert report.reconciliation is not None
    assert report.reconciliation.status == ReconciliationStatus.DISCREPANCY_DETECTED
    assert report.reconciliation.is_reconciled is False
    assert report.reconciliation.strategy_discrepancy == 2_000.0


# ------------------------------------------------------------------------------
# 2. Instrument-Level Attribution Tests
# ------------------------------------------------------------------------------

def test_instrument_attribution_aggregation():
    """Verify proper aggregation of metrics across traded instruments."""
    res = _create_mock_simulation_result()
    service = PortfolioAttributionService()
    report = service.analyze_simulation_result(res)

    assert len(report.instrument_attributions) == 2
    tcs = next(i for i in report.instrument_attributions if i.symbol == "TCS")
    infy = next(i for i in report.instrument_attributions if i.symbol == "INFY")

    assert tcs.net_pnl == 6_000.0
    assert tcs.total_costs == 300.0
    assert tcs.trades_count == 1
    assert tcs.is_available is True

    assert infy.net_pnl == 4_000.0
    assert infy.total_costs == 200.0
    assert infy.trades_count == 1
    assert infy.is_available is True


def test_instrument_attribution_unavailable_handling():
    """Verify explicit unavailable status when strategy contributions are missing."""
    res = _create_mock_simulation_result(contributions=[])
    service = PortfolioAttributionService()
    report = service.analyze_simulation_result(res)

    assert len(report.instrument_attributions) == 1
    inst = report.instrument_attributions[0]
    assert inst.is_available is False
    assert inst.unavailable_reason is not None
    assert "does not contain" in inst.unavailable_reason


# ------------------------------------------------------------------------------
# 3. Sharpe & Sortino Calculations with Known Series
# ------------------------------------------------------------------------------

def test_sharpe_ratio_known_returns():
    """Verify Sharpe ratio calculation against known analytical solution.

    Returns: [0.02, 0.04, -0.01, 0.03, 0.02]
    Mean = 0.02
    N = 5
    Sum squared diffs = (0)^2 + (0.02)^2 + (-0.03)^2 + (0.01)^2 + (0)^2
                      = 0 + 0.0004 + 0.0009 + 0.0001 + 0 = 0.0014
    Sample Variance = 0.0014 / 4 = 0.00035
    Sample Std Dev = sqrt(0.00035) ≈ 0.018708287
    Annualization factor = 252 (daily)
    Excess Return = 0.02 - 0 = 0.02
    Sharpe = (0.02 / 0.018708287) * sqrt(252) ≈ 1.069045 * 15.8745079 ≈ 16.9706
    """
    returns = [0.02, 0.04, -0.01, 0.03, 0.02]
    sharpe_mv = calculate_sharpe_ratio(returns, risk_free_rate=0.0, annualization_factor=252.0)

    assert sharpe_mv.is_available is True
    assert sharpe_mv.value is not None
    expected_sharpe = (0.02 / math.sqrt(0.0014 / 4)) * math.sqrt(252)
    assert pytest.approx(sharpe_mv.value, rel=1e-3) == expected_sharpe


def test_sortino_ratio_known_returns():
    """Verify Sortino ratio calculation using downside risk deviation below zero.

    Returns: [0.02, 0.04, -0.01, 0.03, 0.02]
    Mean = 0.02
    Downside differences (min(0, r)): [0, 0, -0.01, 0, 0]
    Downside variance = (-0.01)^2 / 5 = 0.0001 / 5 = 0.00002
    Downside Std = sqrt(0.00002) ≈ 0.004472136
    Sortino = (0.02 / 0.004472136) * sqrt(252) ≈ 4.472136 * 15.8745079 ≈ 70.99
    """
    returns = [0.02, 0.04, -0.01, 0.03, 0.02]
    sortino_mv = calculate_sortino_ratio(returns, risk_free_rate=0.0, annualization_factor=252.0)

    assert sortino_mv.is_available is True
    assert sortino_mv.value is not None
    expected_downside_std = math.sqrt(((-0.01) ** 2) / 5)
    expected_sortino = (0.02 / expected_downside_std) * math.sqrt(252)
    assert pytest.approx(sortino_mv.value, rel=1e-3) == expected_sortino


def test_sortino_no_negative_returns():
    """Verify Sortino returns None safely when there are no negative returns (downside std is zero)."""
    returns = [0.01, 0.02, 0.03, 0.01]  # All positive!
    sortino_mv = calculate_sortino_ratio(returns, risk_free_rate=0.0)

    assert sortino_mv.is_available is False
    assert sortino_mv.value is None
    assert "Zero downside deviation" in str(sortino_mv.unavailable_reason)


# ------------------------------------------------------------------------------
# 4. Volatility, Annualization & Max Drawdown
# ------------------------------------------------------------------------------

def test_volatility_and_annualization_config():
    """Verify annualized volatility computation and invalid annualization config rejection."""
    # Config validation
    with pytest.raises(ValidationError, match="annualization_factor must be strictly positive"):
        AttributionConfig(annualization_factor=0.0)

    with pytest.raises(ValidationError, match="reconciliation_tolerance cannot be negative"):
        AttributionConfig(reconciliation_tolerance=-0.1)

    # Volatility on known series
    returns = [0.01, -0.01, 0.01, -0.01]  # Mean = 0.0
    vol_mv = calculate_annualized_volatility(returns, annualization_factor=252.0)
    assert vol_mv.is_available is True
    assert vol_mv.value is not None
    # Sample variance: (0.01^2 * 4) / 3 = 0.0004 / 3 = 0.00013333...
    expected_vol_pct = math.sqrt(0.0004 / 3) * math.sqrt(252.0) * 100.0
    assert pytest.approx(vol_mv.value, rel=1e-3) == expected_vol_pct


def test_max_drawdown_calculation():
    """Verify peak-to-trough max drawdown amount, percentage, and duration in bars."""
    eq_pts = [
        EquityPoint(1.0, 100.0, 0.0, 100.0, 0.0, 0.0),    # Peak = 100
        EquityPoint(2.0, 110.0, 0.0, 110.0, 0.0, 0.0),    # Peak = 110
        EquityPoint(3.0, 99.0, 0.0, 99.0, 10.0, 11.0),    # DD = 11 (10.0%), dur = 1
        EquityPoint(4.0, 88.0, 0.0, 88.0, 20.0, 22.0),    # DD = 22 (20.0%), dur = 2 (Max)
        EquityPoint(5.0, 105.0, 0.0, 105.0, 4.55, 5.0),   # DD = 5, dur = 3
        EquityPoint(6.0, 120.0, 0.0, 120.0, 0.0, 0.0),    # New peak, dur resets
    ]
    pct_mv, amt_mv, dur_mv = calculate_max_drawdown(eq_pts)

    assert pct_mv.value == 20.0
    assert amt_mv.value == 22.0
    assert dur_mv.value == 3.0  # Max duration bars while underwater


# ------------------------------------------------------------------------------
# 5. Trade Metrics & Undefined Values
# ------------------------------------------------------------------------------

def test_profit_factor_and_trade_metrics():
    """Verify win rate, profit factor, win/loss ratio, and average win/loss."""
    t1 = TradeRecord("1", "TCS", OrderSide.BUY, 1, 2, 100, 110, 1, 100, 100, 0, 0, 10, 1, True)
    t2 = TradeRecord("2", "TCS", OrderSide.BUY, 2, 3, 100, 120, 1, 200, 200, 0, 0, 20, 1, True)
    t3 = TradeRecord("3", "TCS", OrderSide.BUY, 3, 4, 100, 50, 1, -50, -50, 0, 0, -50, 1, False)

    tm = calculate_trade_metrics([t1, t2, t3])
    assert tm["total_trades"].value == 3.0
    assert pytest.approx(tm["win_rate"].value, rel=1e-3) == (2.0 / 3.0) * 100.0
    assert tm["profit_factor"].value == (300.0 / 50.0)  # 6.0
    assert tm["average_win"].value == 150.0  # (100 + 200) / 2
    assert tm["average_loss"].value == 50.0
    assert tm["win_loss_ratio"].value == (150.0 / 50.0)  # 3.0


def test_zero_variance_and_insufficient_observations():
    """Verify zero variance and insufficient observation (< 2) edge cases."""
    # 1. Zero observations
    ret_empty = calculate_equity_returns([])
    assert len(ret_empty) == 0

    # 2. Insufficient observations (< 2 periods)
    vol_insufficient = calculate_annualized_volatility([0.01], 252.0)
    assert vol_insufficient.is_available is False
    assert "Insufficient observations" in str(vol_insufficient.unavailable_reason)

    sharpe_insufficient = calculate_sharpe_ratio([0.01], 0.0, 252.0)
    assert sharpe_insufficient.is_available is False
    assert "Insufficient observations" in str(sharpe_insufficient.unavailable_reason)

    # 3. Zero variance (all returns identical)
    zero_var_returns = [0.01, 0.01, 0.01, 0.01]
    vol_zero = calculate_annualized_volatility(zero_var_returns, 252.0)
    assert vol_zero.value == 0.0

    sharpe_zero = calculate_sharpe_ratio(zero_var_returns, 0.0, 252.0)
    assert sharpe_zero.is_available is False
    assert "Zero return volatility" in str(sharpe_zero.unavailable_reason)


def test_calmar_ratio_zero_drawdown():
    """Verify Calmar ratio returns unavailable safely when max drawdown is zero."""
    calmar = calculate_calmar_ratio(annualized_return_pct=15.0, max_drawdown_pct=0.0)
    assert calmar.is_available is False
    assert calmar.value is None
    assert "Zero maximum drawdown" in str(calmar.unavailable_reason)


# ------------------------------------------------------------------------------
# 6. Non-Finite Data Handling
# ------------------------------------------------------------------------------

def test_non_finite_nan_and_inf_handling():
    """Verify that NaN and Inf are intercepted and marked unavailable in MetricValue."""
    mv_nan = MetricValue(value=float("nan"), unit="%")
    assert mv_nan.is_available is False
    assert mv_nan.value is None
    assert "non-finite" in str(mv_nan.unavailable_reason)

    mv_inf = MetricValue(value=float("inf"), unit="ratio")
    assert mv_inf.is_available is False
    assert mv_inf.value is None
    assert "non-finite" in str(mv_inf.unavailable_reason)


# ------------------------------------------------------------------------------
# 7. Realized vs Unrealized Accounting
# ------------------------------------------------------------------------------

def test_realized_vs_unrealized_accounting():
    """Verify proper segregation of realized trades vs open position unrealized P&L."""
    res = _create_mock_simulation_result(
        net_pnl=10_000.0,
    )
    service = PortfolioAttributionService()
    report = service.analyze_simulation_result(res)

    # In clean completion, all positions liquidated, so all 10,000 realized
    assert report.realized_pnl == 10_000.0
    assert report.unrealized_pnl == 0.0


# ------------------------------------------------------------------------------
# 8. Standalone Backtest Attribution Tests
# ------------------------------------------------------------------------------

def test_analyze_backtest_result():
    """Verify attribution generation for a standalone BacktestResult."""
    cfg = BacktestConfig(
        symbol="RELIANCE",
        timeframe="15m",
        start_time=1700000000.0,
        end_time=1700003600.0,
        initial_cash=100_000.0,
    )
    t = TradeRecord("t_bt", "RELIANCE", OrderSide.BUY, 1, 2, 100, 105, 100, 500, 480, 10, 10, 5.0, 1, True)
    metrics = PerformanceMetrics(
        initial_equity=100_000.0,
        final_equity=100_480.0,
        gross_profit=500.0,
        gross_loss=0.0,
        net_profit=480.0,
        total_return_pct=0.48,
        max_drawdown_pct=0.0,
        max_drawdown_amount=0.0,
        total_trades=1,
        winning_trades=1,
        losing_trades=0,
        even_trades=0,
        win_rate=100.0,
        profit_factor=None,
        profit_factor_description="Infinity",
        total_fees=10.0,
        total_slippage=10.0,
        total_transaction_costs=20.0,
        market_exposure_pct=10.0,
    )
    bt_res = BacktestResult(
        backtest_id="bt_123",
        status=BacktestStatus.COMPLETED,
        strategy_name="sma_crossover",
        strategy_version="1.0.0",
        config=cfg,
        metrics=metrics,
        equity_curve=(
            EquityPoint(1.0, 100_000.0, 0.0, 100_000.0, 0.0, 0.0),
            EquityPoint(2.0, 100_480.0, 0.0, 100_480.0, 0.0, 0.0),
        ),
        trades=(t,),
        bars_processed=2,
    )

    service = PortfolioAttributionService()
    report = service.analyze_backtest_result(bt_res)

    assert report.source_type == "backtest"
    assert report.source_id == "bt_123"
    assert report.net_pnl == 480.0
    assert report.total_costs.total_costs == 20.0
    assert len(report.strategy_attributions) == 1
    assert report.strategy_attributions[0].strategy_name == "sma_crossover"
    assert len(report.instrument_attributions) == 1
    assert report.instrument_attributions[0].symbol == "RELIANCE"


# ------------------------------------------------------------------------------
# 9. Comparison Reports (Simulations, Walk-Forward, Robustness)
# ------------------------------------------------------------------------------

def test_compare_simulation_runs():
    """Verify comparing multiple simulation runs side-by-side."""
    res1 = _create_mock_simulation_result(simulation_id="sim_A", final_equity=110_000.0)
    res2 = _create_mock_simulation_result(simulation_id="sim_B", final_equity=120_000.0)

    service = PortfolioAttributionService()
    comp = service.compare_simulation_runs([res1, res2])

    assert comp.comparison_type == ComparisonType.SIMULATION_RUNS
    assert len(comp.entities) == 2
    assert comp.summary["best_return_simulation_id"] == "sim_B"


def test_compare_walk_forward_windows():
    """Verify In-Sample vs Out-of-Sample window separation and efficiency calculation."""
    cfg = WalkForwardConfig(
        symbol="TCS",
        timeframe="15m",
        start_time=1700000000.0,
        end_time=1700003600.0,
        train_duration_seconds=1800.0,
        test_duration_seconds=1800.0,
    )
    pm_is = PerformanceMetrics(100_000, 110_000, 10_000, 0, 10_000, 10.0, 1.0, 100, 5, 4, 1, 0, 80.0, 5.0, "5.0", 50, 50, 100, 20)
    pm_oos = PerformanceMetrics(100_000, 106_000, 6_000, 0, 6_000, 6.0, 2.0, 200, 3, 2, 1, 0, 66.7, 3.0, "3.0", 30, 30, 60, 15)

    b_cfg = BacktestConfig(symbol="TCS", timeframe="15m", start_time=1700000000.0, end_time=1700003600.0)
    b_is = BacktestResult("b_is", BacktestStatus.COMPLETED, "sma_crossover", "1.0", b_cfg, pm_is, (), (), 100)
    b_oos = BacktestResult("b_oos", BacktestStatus.COMPLETED, "sma_crossover", "1.0", b_cfg, pm_oos, (), (), 50)
    win_def = ValidationWindow(
        window_id=0,
        train_start=1700000000.0,
        train_end=1700001800.0,
        test_start=1700001800.0,
        test_end=1700003600.0,
    )

    w1 = ValidationWindowResult(
        window=win_def,
        status="COMPLETED",
        in_sample_bars=100,
        out_of_sample_bars=50,
        in_sample_result=b_is,
        out_of_sample_result=b_oos,
    )
    oos_summary = OutOfSampleSummary(
        total_windows_generated=1,
        eligible_windows=1,
        completed_windows=1,
        skipped_windows=0,
        failed_windows=0,
        mean_window_return_pct=6.0,
        median_window_return_pct=6.0,
        compounded_return_pct=6.0,
        total_oos_net_pnl=6000.0,
        total_oos_trades=3,
        total_oos_winning_trades=2,
        total_oos_losing_trades=1,
        overall_oos_win_rate=66.7,
        mean_window_win_rate=66.7,
        mean_profit_factor=3.0,
        overall_profit_factor=3.0,
        overall_profit_factor_description="3.0",
        max_window_drawdown_pct=2.0,
        worst_window_drawdown_amount=200.0,
        total_transaction_costs=60.0,
        profitable_windows_count=1,
        unprofitable_windows_count=0,
        window_win_rate_pct=100.0,
    )
    wf_res = WalkForwardResult(
        validation_id="wf_test_123",
        status=WalkForwardStatus.COMPLETED,
        strategy_name="sma_crossover",
        strategy_version="1.0.0",
        config=cfg,
        summary=oos_summary,
        windows=(w1,),
    )

    service = PortfolioAttributionService()
    comp = service.compare_walk_forward_windows(wf_res)

    assert comp.comparison_type == ComparisonType.WALK_FORWARD
    assert len(comp.entities) == 1
    # OOS Return = 6.0%, IS Return = 10.0% -> Efficiency = 0.6
    assert pytest.approx(comp.entities[0].metrics["oos_to_is_efficiency_ratio"], rel=1e-3) == 0.6


def test_compare_robustness_runs():
    """Verify comparing baseline strategy vs adverse stress scenarios."""
    b_cfg = BacktestConfig("TCS", "15m", 1, 2)
    b_metrics = PerformanceMetrics(100_000, 110_000, 10_000, 0, 10_000, 10.0, 1.0, 100, 5, 4, 1, 0, 80.0, 5.0, "5.0", 50, 50, 100, 20)
    base_res = BacktestResult("b1", BacktestStatus.COMPLETED, "sma_crossover", "1.0", b_cfg, b_metrics, (), (), 2)

    scen = StressScenario("2x_comm", "Double commissions", ScenarioType.COMMISSION_STRESS, fee_rate=0.001)
    s_metrics = PerformanceMetrics(100_000, 108_000, 10_000, 0, 8_000, 8.0, 1.2, 120, 5, 4, 1, 0, 80.0, 4.0, "4.0", 100, 50, 150, 20)
    scen_bt = BacktestResult("s1", BacktestStatus.COMPLETED, "sma_crossover", "1.0", b_cfg, s_metrics, (), (), 2)
    scen_res = StressScenarioResult(
        scenario=scen,
        status="COMPLETED",
        backtest_result=scen_bt,
        return_degradation_pct=20.0,
        drawdown_increase_pct=20.0,
    )

    r_summary = RobustnessSummary(
        total_scenarios_evaluated=1,
        completed_scenarios=1,
        failed_scenarios=0,
        skipped_scenarios=0,
        baseline_return_pct=10.0,
        baseline_max_drawdown_pct=1.0,
        baseline_net_pnl=10_000.0,
        baseline_trades=5,
        worst_case_return_pct=8.0,
        worst_case_scenario_name="2x_comm",
        worst_case_max_drawdown_pct=1.2,
        worst_case_drawdown_scenario_name="2x_comm",
        mean_scenario_return_pct=8.0,
        max_cost_impact_pnl=2_000.0,
        max_cost_scenario_name="2x_comm",
        profitable_scenarios_count=1,
        unprofitable_scenarios_count=0,
        scenario_pass_rate_pct=100.0,
    )
    r_cfg = RobustnessTestConfig(
        symbol="TCS",
        timeframe="15m",
        start_time=1700000000.0,
        end_time=1700003600.0,
        strategy_name="sma_crossover",
        custom_scenarios=(scen,),
    )
    rob_res = RobustnessTestResult(
        test_id="rob_123",
        status=RobustnessStatus.COMPLETED,
        strategy_name="sma_crossover",
        strategy_version="1.0.0",
        config=r_cfg,
        baseline_result=base_res,
        summary=r_summary,
        scenario_results=(scen_res,),
    )

    service = PortfolioAttributionService()
    comp = service.compare_robustness_runs(rob_res)

    assert comp.comparison_type == ComparisonType.ROBUSTNESS
    assert len(comp.entities) == 2  # Baseline + 1 scenario
    assert comp.entities[0].entity_id == "baseline"
    assert comp.entities[1].entity_id == "2x_comm"


# ------------------------------------------------------------------------------
# 10. EventBus & StateStore Integration Tests
# ------------------------------------------------------------------------------

def test_event_bus_and_state_store_integration(event_bus: EventBus, state_store: StateStore):
    """Verify EventBus publication and StateStore persistence upon attribution calculation."""
    events_published = []
    event_bus.subscribe(
        EventType.PORTFOLIO_ATTRIBUTION_COMPLETED,
        lambda ev: events_published.append(ev),
    )

    service = PortfolioAttributionService(
        event_bus=event_bus,
        state_store=state_store,
    )
    res = _create_mock_simulation_result()
    report = service.analyze_simulation_result(res)

    # 1. EventBus verified
    assert len(events_published) == 1
    assert events_published[0].payload["report_id"] == report.report_id
    assert events_published[0].payload["net_pnl"] == report.net_pnl

    # 2. StateStore verified
    latest = state_store.get("portfolio_attribution:latest")
    assert latest is not None
    assert latest["report_id"] == report.report_id
    assert latest["net_pnl"] == 10_000.0


# ------------------------------------------------------------------------------
# 11. Orchestrator Integration Tests
# ------------------------------------------------------------------------------

def test_orchestrator_handlers_integration(event_bus: EventBus, state_store: StateStore):
    """Verify calculate_portfolio_attribution, get_portfolio_attribution, and compare_performance_runs."""
    service = PortfolioAttributionService(
        event_bus=event_bus,
        state_store=state_store,
    )
    orchestrator = MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        name="test_orchestrator",
    )
    register_portfolio_handlers(orchestrator=orchestrator, service=service)

    res = _create_mock_simulation_result()
    state_store.set("portfolio_simulation:sim_test_01", res)

    # 1. Test calculate_portfolio_attribution via simulation_id in state_store
    req1 = OrchestratorRequest.create(
        command="calculate_portfolio_attribution",
        context={"simulation_id": "sim_test_01"},
    )
    resp1 = orchestrator.handle_request(req1)
    assert resp1.is_success
    assert resp1.result["status"] == "success"
    assert resp1.result["report"]["net_pnl"] == 10_000.0

    # 2. Test get_portfolio_attribution
    req2 = OrchestratorRequest.create(command="get_portfolio_attribution")
    resp2 = orchestrator.handle_request(req2)
    assert resp2.is_success
    assert resp2.result["status"] == "success"
    assert resp2.result["report"]["net_pnl"] == 10_000.0

    # 3. Test compare_performance_runs
    req3 = OrchestratorRequest.create(
        command="compare_performance_runs",
        context={"comparison_type": "simulation_runs", "simulation_results": [res]},
    )
    resp3 = orchestrator.handle_request(req3)
    assert resp3.is_success
    assert resp3.result["status"] == "success"
    assert "comparison" in resp3.result


# ------------------------------------------------------------------------------
# 12. Safety Invariants Confirmation
# ------------------------------------------------------------------------------

def test_safety_invariants_strictly_maintained():
    """Verify Settings.execution_locked is True and no trading/order methods exist on the attribution service."""
    settings = Settings()
    assert settings.execution_locked is True

    service = PortfolioAttributionService()
    assert not hasattr(service, "place_order")
    assert not hasattr(service, "liquidate")
    assert not hasattr(service, "broker")
    assert not hasattr(service, "modify_order")
