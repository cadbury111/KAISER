"""KAISER — Backtesting & Strategy Evaluation Foundation Tests.

Comprehensive, deterministic test suite verifying:
1. BacktestConfig, StrategySignal, SimulatedOrder, SimulatedFill, and BacktestResult models.
2. SimulatedPortfolio virtual accounting: cash, positions, fees, slippage, and balance integrity.
3. Pre-trade risk controls: insufficient cash, exposure limits, position limits, and circuit breakers.
4. Strategy contract, StrategyContext time-isolation, and SMACrossoverStrategy.
5. Anti-look-ahead protections: signal at bar t close executes at bar t+1 open.
6. PerformanceMetrics calculation and edge cases (zero trades, zero losses, division-by-zero safety).
7. BacktestEngine chronological execution, quality gating, and state isolation between runs.
8. Deterministic reproducibility across repeated runs.
9. Cooperative cancellation and empty data range handling.
10. HandlerRegistry and MasterOrchestrator commands (`run_backtest`, `list_backtest_strategies`).
11. Absolute trading safety: execution remains locked, virtual simulation only.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from typing import Any, List, Optional, Tuple, cast
import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.backtesting.engine import BacktestEngine
from app.backtesting.handlers import register_backtest_handlers
from app.backtesting.metrics import calculate_performance_metrics
from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    BacktestStatus,
    EquityPoint,
    ExecutionModel,
    FeeModel,
    OrderSide,
    OrderStatus,
    OrderType,
    PerformanceMetrics,
    PositionSizeType,
    SignalType,
    SimulatedFill,
    SimulatedOrder,
    StrategySignal,
    TradeRecord,
)
from app.backtesting.portfolio import Position, SimulatedPortfolio
from app.backtesting.strategy import (
    SMACrossoverStrategy,
    Strategy,
    StrategyContext,
    StrategyRegistry,
)
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest


# ------------------------------------------------------------------------------
# Test Fixtures and Helpers
# ------------------------------------------------------------------------------

def _create_sample_bars(
    symbol: str = "RELIANCE",
    timeframe: str = "1m",
    count: int = 30,
    start_ts: float = 1700000000.0,
    interval_sec: float = 60.0,
    base_price: float = 2500.0,
    trend: str = "flat",
) -> List[Bar]:
    """Helper to generate a deterministic list of valid Bar objects."""
    bars: List[Bar] = []
    p = base_price
    for i in range(count):
        ts = start_ts + (i * interval_sec)
        if trend == "up":
            p += 2.0
        elif trend == "down":
            p -= 2.0
        elif trend == "sine":
            import math
            p = base_price + math.sin(i / 3.0) * 15.0

        o = round(p, 2)
        h = round(o + 2.0, 2)
        l = round(o - 1.5, 2)
        c = round(o + 0.5, 2)
        v = 100.0 + (i * 5.0)

        bars.append(
            Bar(
                symbol=symbol,
                exchange="NSE",
                timeframe=timeframe,
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


@pytest.fixture
def in_memory_repo() -> SQLiteHistoricalBarRepository:
    """Fixture providing an isolated in-memory SQLite bar repository."""
    repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_mem_repo")
    return repo


@pytest.fixture
def quality_engine() -> MarketDataQualityEngine:
    """Fixture providing a fresh MarketDataQualityEngine."""
    return MarketDataQualityEngine()


@pytest.fixture
def analysis_engine() -> TechnicalAnalysisEngine:
    """Fixture providing a fresh TechnicalAnalysisEngine."""
    return TechnicalAnalysisEngine(name="test_analysis_engine")


# ------------------------------------------------------------------------------
# 1. Backtest Domain Models & Validation Tests
# ------------------------------------------------------------------------------

class TestBacktestDomainModels:
    """Tests verifying input validation and integrity of backtesting data models."""

    def test_backtest_config_validation(self) -> None:
        """Config must validate positive cash, valid timeframes, and ordered timestamps."""
        config = BacktestConfig(
            symbol="nifty",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700010000.0,
            initial_cash=50000.0,
            position_size_value=15.0,
        )
        assert config.symbol == "NIFTY"
        assert config.timeframe == "15m"
        assert config.initial_cash == 50000.0
        assert config.execution_model == ExecutionModel.NEXT_BAR_OPEN.value

        # Negative cash
        with pytest.raises(ValidationError):
            BacktestConfig(symbol="NIFTY", timeframe="15m", start_time=100.0, end_time=200.0, initial_cash=-50.0)

        # Inverted timestamps
        with pytest.raises(ValidationError):
            BacktestConfig(symbol="NIFTY", timeframe="15m", start_time=500.0, end_time=200.0)

        # Invalid position size type
        with pytest.raises(ValidationError):
            BacktestConfig(symbol="NIFTY", timeframe="15m", start_time=100.0, end_time=200.0, position_size_type="invalid")

    def test_strategy_signal_validation(self) -> None:
        """StrategySignal must enforce non-empty symbol and strength in [0, 1]."""
        sig = StrategySignal(
            symbol="tcs",
            timestamp=1700000000.0,
            signal_type=SignalType.BUY,
            strength=0.85,
            suggested_quantity=20.0,
            reason="Test breakout",
        )
        assert sig.symbol == "TCS"
        assert sig.signal_type == SignalType.BUY
        assert sig.strength == 0.85

        with pytest.raises(ValidationError):
            StrategySignal(symbol="TCS", timestamp=100.0, signal_type=SignalType.BUY, strength=1.5)

    def test_simulated_order_and_fill(self) -> None:
        """Orders and fills must correctly represent virtual transactions."""
        order = SimulatedOrder(
            order_id="ord_1",
            symbol="INFY",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.0,
            created_at=1700000000.0,
        )
        assert order.status == OrderStatus.PENDING

        fill = SimulatedFill(
            fill_id="fill_1",
            order_id="ord_1",
            symbol="INFY",
            side=OrderSide.BUY,
            quantity=10.0,
            price=1500.25,
            fee=7.50,
            slippage_cost=3.00,
            timestamp=1700000060.0,
        )
        d = fill.to_dict()
        assert d["price"] == 1500.25
        assert d["fee"] == 7.50


# ------------------------------------------------------------------------------
# 2. Simulated Portfolio & Virtual Accounting Tests
# ------------------------------------------------------------------------------

class TestSimulatedPortfolioAccounting:
    """Tests verifying virtual portfolio cash, positions, fees, slippage, and balance integrity."""

    def test_portfolio_initial_state(self) -> None:
        """Initial cash and positions must be correctly set."""
        pf = SimulatedPortfolio(initial_cash=100_000.0)
        assert pf.cash == 100_000.0
        assert pf.total_equity == 100_000.0
        assert len(pf.positions) == 0

    def test_portfolio_buy_and_sell_accounting(self) -> None:
        """Buying reduces cash and creates position; selling restores cash and records TradeRecord."""
        pf = SimulatedPortfolio(initial_cash=100_000.0)
        config = BacktestConfig(
            symbol="RELIANCE",
            timeframe="1m",
            start_time=100.0,
            end_time=500.0,
            fee_model="percentage",
            fee_rate=0.001,  # 0.1%
            slippage_bps=0.0,
            half_spread_bps=0.0,
        )

        # 1. Execute BUY
        buy_order = SimulatedOrder(
            order_id="ord_b1",
            symbol="RELIANCE",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.0,
            created_at=100.0,
        )
        fill = pf.execute_order(buy_order, base_price=2500.0, timestamp=100.0, config=config)
        assert fill is not None
        # Fee = 10 * 2500 * 0.001 = 25.0
        assert pf.cash == 100_000.0 - (25000.0 + 25.0)
        pos = pf.get_position("RELIANCE")
        assert pos is not None
        assert pos.quantity == 10.0
        assert pos.avg_cost == 2500.0
        assert pf.total_equity == pf.cash + (10.0 * 2500.0)

        # 2. Execute SELL at higher price (win)
        sell_order = SimulatedOrder(
            order_id="ord_s1",
            symbol="RELIANCE",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=10.0,
            created_at=200.0,
        )
        fill_sell = pf.execute_order(sell_order, base_price=2600.0, timestamp=200.0, config=config)
        assert fill_sell is not None
        # Fee = 10 * 2600 * 0.001 = 26.0
        # Cash in = 26000.0 - 26.0 = 25974.0
        assert pf.get_position("RELIANCE") is None
        assert len(pf.trades) == 1

        trade = pf.trades[0]
        assert trade.gross_pnl == 1000.0  # (2600 - 2500) * 10
        assert trade.fees == 25.0 + 26.0  # Entry fee + Exit fee
        assert trade.net_pnl == 1000.0 - 51.0
        assert trade.is_win is True
        assert pf.cash == 100_000.0 + trade.net_pnl
        assert pf.total_equity == pf.cash  # No positions open

    def test_slippage_and_spread_impact(self) -> None:
        """Buy executes higher and sell executes lower when slippage & spread are configured."""
        pf = SimulatedPortfolio(initial_cash=50_000.0)
        config = BacktestConfig(
            symbol="INFY",
            timeframe="1m",
            start_time=100.0,
            end_time=500.0,
            slippage_bps=10.0,      # 10 bps = 0.1%
            half_spread_bps=5.0,    # 5 bps = 0.05%
            fee_rate=0.0,
        )

        buy_price, slip, spread = pf.calculate_execution_price(
            side=OrderSide.BUY,
            base_price=1000.0,
            slippage_bps=10.0,
            half_spread_bps=5.0,
        )
        # Expected: 1000 * (1 + 0.0015) = 1001.5
        assert round(buy_price, 2) == 1001.50

        sell_price, _, _ = pf.calculate_execution_price(
            side=OrderSide.SELL,
            base_price=1000.0,
            slippage_bps=10.0,
            half_spread_bps=5.0,
        )
        # Expected: 1000 * (1 - 0.0015) = 998.5
        assert round(sell_price, 2) == 998.50

    def test_insufficient_cash_rejection(self) -> None:
        """Order must be rejected if virtual cash is insufficient to buy."""
        pf = SimulatedPortfolio(initial_cash=1000.0)
        config = BacktestConfig(
            symbol="MRF",
            timeframe="1m",
            start_time=100.0,
            end_time=200.0,
        )
        order = SimulatedOrder(
            order_id="ord_fail",
            symbol="MRF",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.0,  # 10 * 100,000 = 1,000,000
        )
        fill = pf.execute_order(order, base_price=100_000.0, timestamp=100.0, config=config)
        assert fill is None
        assert len(pf.rejected_orders) == 1
        assert pf.rejected_orders[0].rejection_reason is not None
        assert "INSUFFICIENT_CASH" in pf.rejected_orders[0].rejection_reason
        assert pf.cash == 1000.0  # Cash completely untouched

    def test_exposure_limit_rejection(self) -> None:
        """Order must be rejected if it would exceed maximum exposure percentage."""
        pf = SimulatedPortfolio(initial_cash=100_000.0)
        config = BacktestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=100.0,
            end_time=200.0,
            max_exposure_pct=0.30,  # Max 30% of equity
        )
        # Attempt to buy 50% of portfolio
        order = SimulatedOrder(
            order_id="ord_exp",
            symbol="TCS",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=50.0,  # 50 * 1000 = 50,000 > 30,000
        )
        fill = pf.execute_order(order, base_price=1000.0, timestamp=100.0, config=config)
        assert fill is None
        assert pf.rejected_orders[0].rejection_reason is not None
        assert "EXPOSURE_LIMIT_EXCEEDED" in pf.rejected_orders[0].rejection_reason

    def test_drawdown_circuit_breaker(self) -> None:
        """When drawdown exceeds max_drawdown_limit_pct, circuit breaker triggers."""
        pf = SimulatedPortfolio(initial_cash=100_000.0)
        config = BacktestConfig(
            symbol="XYZ",
            timeframe="1m",
            start_time=100.0,
            end_time=500.0,
            max_drawdown_limit_pct=10.0,  # 10% limit
            fee_rate=0.0,
            slippage_bps=0.0,
            half_spread_bps=0.0,
        )
        # Buy 50 shares at 1000
        buy_order = SimulatedOrder(order_id="b1", symbol="XYZ", side=OrderSide.BUY, order_type=OrderType.MARKET, quantity=50.0)
        pf.execute_order(buy_order, base_price=1000.0, timestamp=100.0, config=config)

        # Price drops from 1000 to 700 (position value drops from 50k to 35k -> equity drops from 100k to 85k = 15% drawdown)
        pt = pf.mark_to_market(timestamp=200.0, current_prices={"XYZ": 700.0}, config=config)
        assert pt.drawdown_pct >= 15.0
        assert pf.circuit_breaker_triggered is True

        # Next order should be rejected due to circuit breaker
        new_order = SimulatedOrder(order_id="b2", symbol="XYZ", side=OrderSide.BUY, order_type=OrderType.MARKET, quantity=1.0)
        fill = pf.execute_order(new_order, base_price=700.0, timestamp=200.0, config=config)
        assert fill is None
        assert pf.rejected_orders[-1].rejection_reason is not None
        assert "CIRCUIT_BREAKER_ACTIVE" in pf.rejected_orders[-1].rejection_reason


# ------------------------------------------------------------------------------
# 3. Strategy Contract and Anti-Look-Ahead Tests
# ------------------------------------------------------------------------------

class TestStrategyContractAndAntiLookAhead:
    """Tests verifying Strategy, StrategyContext, and look-ahead bias protections."""

    def test_strategy_context_time_isolation(self) -> None:
        """StrategyContext must strictly forbid future access."""
        bar = Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=1000.0, open=100.0, high=105.0, low=99.0, close=104.0, volume=10.0)
        past_bars = [bar]
        ctx = StrategyContext(
            symbol="NIFTY",
            timeframe="1m",
            current_bar=bar,
            timestamp=1000.0,
            feature_vector=None,
            portfolio_cash=10000.0,
            position_quantity=0.0,
            position_avg_cost=0.0,
            position_market_value=0.0,
            portfolio_equity=10000.0,
            bars_history=past_bars,
        )
        assert ctx.is_flat is True
        assert len(ctx.get_bars()) == 1

        # Mismatched timestamp must raise ValidationError
        with pytest.raises(ValidationError):
            StrategyContext(
                symbol="NIFTY",
                timeframe="1m",
                current_bar=bar,
                timestamp=2000.0,  # Invalid: doesn't match current_bar
                feature_vector=None,
                portfolio_cash=10000.0,
                position_quantity=0.0,
                position_avg_cost=0.0,
                position_market_value=0.0,
                portfolio_equity=10000.0,
                bars_history=past_bars,
            )

    def test_sma_crossover_strategy_logic(self) -> None:
        """SMACrossoverStrategy generates BUY on golden cross and SELL on death cross."""
        strat = SMACrossoverStrategy(fast_period=2, slow_period=4)
        assert "sma_crossover" in strat.name
        strat.reset()

        # Build sequence of bars
        prices = [10.0, 10.0, 10.0, 10.0, 12.0, 15.0, 18.0, 12.0, 8.0]
        bars = [
            Bar(symbol="TEST", exchange="NSE", timeframe="1m", timestamp=100.0 + i * 60, open=p, high=p+1, low=p-1, close=p, volume=100)
            for i, p in enumerate(prices)
        ]

        signals: List[StrategySignal] = []
        has_pos = False
        pos_qty = 0.0

        for i, bar in enumerate(bars):
            history = bars[: i + 1]
            ctx = StrategyContext(
                symbol="TEST",
                timeframe="1m",
                current_bar=bar,
                timestamp=bar.timestamp,
                feature_vector=None,
                portfolio_cash=10000.0,
                position_quantity=pos_qty,
                position_avg_cost=10.0 if has_pos else 0.0,
                position_market_value=pos_qty * bar.close,
                portfolio_equity=10000.0,
                bars_history=history,
            )
            sig = strat.on_bar(ctx)
            if sig:
                signals.append(sig)
                if sig.signal_type == SignalType.BUY:
                    has_pos = True
                    pos_qty = 10.0
                elif sig.signal_type == SignalType.SELL:
                    has_pos = False
                    pos_qty = 0.0

        # Must have generated BUY then SELL
        assert len(signals) >= 2
        assert signals[0].signal_type == SignalType.BUY
        assert signals[1].signal_type == SignalType.SELL

    def test_strategy_registry(self) -> None:
        """StrategyRegistry provides registration, lookup, and metadata listing."""
        reg = StrategyRegistry()
        assert reg.has_strategy("sma_crossover") is True

        strat = reg.get("sma_crossover", fast_period=3, slow_period=7)
        assert isinstance(strat, SMACrossoverStrategy)
        assert strat.fast_period == 3

        with pytest.raises(ValidationError):
            reg.get("non_existent_strategy")


# ------------------------------------------------------------------------------
# 4. Performance Metrics Calculation Tests
# ------------------------------------------------------------------------------

class TestPerformanceMetricsCalculations:
    """Tests verifying edge cases and mathematical safety of metrics calculation."""

    def test_zero_trades_metric_handling(self) -> None:
        """When 0 trades execute, metrics must avoid NaN, Inf, and division by zero."""
        metrics = calculate_performance_metrics(
            initial_equity=100_000.0,
            final_equity=100_000.0,
            trades=[],
            equity_curve=[
                EquityPoint(100.0, 100_000.0, 0.0, 100_000.0, 0.0, 0.0),
                EquityPoint(200.0, 100_000.0, 0.0, 100_000.0, 0.0, 0.0),
            ],
            total_bars=10,
        )
        assert metrics.total_trades == 0
        assert metrics.winning_trades == 0
        assert metrics.win_rate == 0.0
        assert metrics.profit_factor is None
        assert "No trades" in metrics.profit_factor_description
        assert metrics.max_drawdown_pct == 0.0
        assert metrics.net_profit == 0.0

        d = metrics.to_dict()
        assert d["profit_factor"] is None
        assert d["total_trades"] == 0

    def test_zero_losses_metric_handling(self) -> None:
        """When all trades win (gross_loss == 0), profit factor is represented safely."""
        trades = [
            TradeRecord("t1", "INFY", OrderSide.BUY, 100.0, 200.0, 100.0, 110.0, 10.0, 100.0, 95.0, 5.0, 0.0, 10.0, 100.0, True),
            TradeRecord("t2", "INFY", OrderSide.BUY, 300.0, 400.0, 110.0, 120.0, 10.0, 100.0, 95.0, 5.0, 0.0, 9.09, 100.0, True),
        ]
        metrics = calculate_performance_metrics(
            initial_equity=100_000.0,
            final_equity=100_190.0,
            trades=trades,
            equity_curve=[],
            total_bars=20,
        )
        assert metrics.total_trades == 2
        assert metrics.winning_trades == 2
        assert metrics.losing_trades == 0
        assert metrics.win_rate == 100.0
        assert metrics.profit_factor is None
        assert "Infinite" in metrics.profit_factor_description
        assert metrics.net_profit == 190.0

    def test_mixed_trades_and_drawdown(self) -> None:
        """Accurate calculation of win rate, profit factor, and max drawdown."""
        trades = [
            TradeRecord("t1", "SYM", OrderSide.BUY, 100.0, 200.0, 100.0, 120.0, 10.0, 200.0, 190.0, 10.0, 0.0, 20.0, 100.0, True),
            TradeRecord("t2", "SYM", OrderSide.BUY, 300.0, 400.0, 120.0, 110.0, 10.0, -100.0, -110.0, 10.0, 0.0, -8.33, 100.0, False),
        ]
        eq_curve = [
            EquityPoint(100.0, 100000.0, 0.0, 100000.0, 0.0, 0.0),
            EquityPoint(200.0, 100190.0, 0.0, 100190.0, 0.0, 0.0),  # Peak
            EquityPoint(300.0, 98000.0, 0.0, 98000.0, 2.19, 2190.0), # Trough
            EquityPoint(400.0, 100080.0, 0.0, 100080.0, 0.11, 110.0),
        ]
        metrics = calculate_performance_metrics(
            initial_equity=100_000.0,
            final_equity=100_080.0,
            trades=trades,
            equity_curve=eq_curve,
            total_bars=50,
            bars_with_position=20,
        )
        assert metrics.total_trades == 2
        assert metrics.winning_trades == 1
        assert metrics.losing_trades == 1
        assert metrics.win_rate == 50.0
        # Profit factor: gross_profit=200, gross_loss=100 => 2.0
        assert metrics.profit_factor == 2.0
        assert metrics.max_drawdown_amount == 2190.0
        assert round(metrics.max_drawdown_pct, 2) == 2.19
        assert metrics.market_exposure_pct == 40.0  # 20 / 50 * 100


# ------------------------------------------------------------------------------
# 5. BacktestEngine Integration & Replay Tests
# ------------------------------------------------------------------------------

class TestBacktestEngineIntegration:
    """Tests verifying BacktestEngine execution, look-ahead prevention, and reproducibility."""

    def test_backtest_next_bar_open_execution_timing(
        self,
        in_memory_repo: SQLiteHistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
    ) -> None:
        """CRITICAL: Signal generated at bar t close MUST execute at bar t+1 open."""
        # Generate alternating prices that force a single BUY on bar 4
        bars = _create_sample_bars(symbol="NIFTY", timeframe="1m", count=10, start_ts=1700000000.0, base_price=100.0)
        # Store in repo
        in_memory_repo.store_bars(bars)

        # Custom single-signal strategy
        class OneShotBuyStrategy(Strategy):
            def __init__(self) -> None:
                self.has_fired = False

            @property
            def name(self) -> str:
                return "oneshot_buy"

            @property
            def description(self) -> str:
                return "Fires buy on bar index 3"

            def on_bar(self, ctx: StrategyContext) -> Optional[StrategySignal]:
                if not self.has_fired and len(ctx.bars_history) == 3:
                    self.has_fired = True
                    return StrategySignal(
                        symbol=ctx.symbol,
                        timestamp=ctx.timestamp,
                        signal_type=SignalType.BUY,
                        suggested_quantity=10.0,
                    )
                return None

        engine = BacktestEngine(
            repository=in_memory_repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )

        config = BacktestConfig(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700001000.0,
            initial_cash=10000.0,
            slippage_bps=0.0,
            half_spread_bps=0.0,
            execution_model="next_bar_open",
        )

        result = engine.run(config=config, strategy=OneShotBuyStrategy())
        assert result.is_success is True
        assert result.signals_generated == 1

        # The signal fired at bar index 2 (timestamp = bars[2].timestamp)
        # The fill must have executed on bar index 3 (timestamp = bars[3].timestamp) at bars[3].OPEN!
        assert len(result.trades) >= 1
        trade = result.trades[0]
        assert trade.entry_time == bars[3].timestamp
        assert trade.entry_price == bars[3].open

    def test_deterministic_reproducibility(
        self,
        in_memory_repo: SQLiteHistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
    ) -> None:
        """Two independent runs of the same strategy and data must yield bit-exact identical results."""
        bars = _create_sample_bars(symbol="TCS", timeframe="15m", count=40, start_ts=1700000000.0, base_price=3000.0, trend="sine")
        in_memory_repo.store_bars(bars)

        engine = BacktestEngine(
            repository=in_memory_repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )

        config = BacktestConfig(
            symbol="TCS",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700050000.0,
            initial_cash=100000.0,
            position_size_value=10.0,
        )

        # Run 1
        res1 = engine.run(config=config, strategy="sma_crossover")
        # Run 2
        res2 = engine.run(config=config, strategy="sma_crossover")

        assert res1.is_success is True
        assert res2.is_success is True
        assert res1.metrics.net_profit == res2.metrics.net_profit
        assert res1.metrics.total_trades == res2.metrics.total_trades
        assert res1.metrics.max_drawdown_pct == res2.metrics.max_drawdown_pct
        assert len(res1.trades) == len(res2.trades)
        assert len(res1.equity_curve) == len(res2.equity_curve)

    def test_quality_gate_quarantined_bars_are_skipped(
        self,
        in_memory_repo: SQLiteHistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
    ) -> None:
        """Out-of-order or unhealthy bars quarantined by MarketDataQualityEngine are rejected."""
        valid_bars = _create_sample_bars(symbol="INFY", timeframe="1m", count=5, start_ts=1700000000.0)

        # Create an out-of-order bar whose timestamp is earlier than bar 1
        out_of_order_bar = Bar(
            symbol="INFY",
            exchange="NSE",
            timeframe="1m",
            timestamp=1700000000.0 + 30.0,  # 30s is earlier than bar 1 (60s)
            open=100.0,
            high=105.0,
            low=95.0,
            close=100.0,
            volume=50.0,
            source="replay",
        )

        class CorruptStreamRepository:
            def query_bars(self, *args, **kwargs):
                return [valid_bars[0], valid_bars[1], out_of_order_bar, valid_bars[2]]

        engine = BacktestEngine(
            repository=cast(Any, CorruptStreamRepository()),
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )

        config = BacktestConfig(
            symbol="INFY",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700002000.0,
        )

        result = engine.run(config=config, strategy="sma_crossover")
        assert result.is_success is True
        # At least one bar rejected by quality gate
        assert result.bars_rejected >= 1
        assert len(result.rejection_reasons) >= 1
        assert any("OUT_OF_ORDER" in r for r in result.rejection_reasons)

    def test_empty_range_handling(
        self,
        in_memory_repo: SQLiteHistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
    ) -> None:
        """Querying an empty range returns completed status with 0 bars and a clear warning."""
        engine = BacktestEngine(
            repository=in_memory_repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )
        config = BacktestConfig(
            symbol="NONEXISTENT",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700001000.0,
        )
        result = engine.run(config=config, strategy="sma_crossover")
        assert result.is_success is True
        assert result.total_bars_available == 0
        assert result.metrics.total_trades == 0
        assert len(result.warnings) >= 1

    def test_cooperative_cancellation(
        self,
        in_memory_repo: SQLiteHistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
    ) -> None:
        """Setting cancellation token aborts backtest and returns CANCELLED status."""
        bars = _create_sample_bars(symbol="HDFC", timeframe="1m", count=50, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        stop_token = threading.Event()
        stop_token.set()  # Cancel immediately

        engine = BacktestEngine(
            repository=in_memory_repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )
        config = BacktestConfig(
            symbol="HDFC",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700010000.0,
        )
        result = engine.run(config=config, strategy="sma_crossover", cancellation_token=stop_token)
        assert result.status == BacktestStatus.CANCELLED
        assert result.is_success is False


# ------------------------------------------------------------------------------
# 6. MasterOrchestrator Integration & Safety Tests
# ------------------------------------------------------------------------------

class TestBacktestOrchestratorIntegrationAndSafety:
    """Tests verifying MasterOrchestrator commands, Handlers, and strict trading safety."""

    def test_run_backtest_command_execution(
        self,
        in_memory_repo: SQLiteHistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
    ) -> None:
        """Command 'run_backtest' executes end-to-end via MasterOrchestrator."""
        bars = _create_sample_bars(symbol="SBIN", timeframe="15m", count=30, start_ts=1700000000.0, trend="sine")
        in_memory_repo.store_bars(bars)

        orchestrator = MasterOrchestrator(name="test_orchestrator")
        engine = BacktestEngine(
            repository=in_memory_repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )
        register_backtest_handlers(orchestrator=orchestrator, engine=engine)

        req = OrchestratorRequest.create(
            command="run_backtest",
            context={
                "symbol": "SBIN",
                "timeframe": "15m",
                "start_time": 1700000000.0,
                "end_time": 1700050000.0,
                "strategy": "sma_crossover",
                "initial_cash": 100000.0,
            },
        )
        resp = orchestrator.handle_request(req)
        assert resp.status.value == "COMPLETED"
        assert resp.result["status"] == "success"
        assert "data" in resp.result
        assert "metrics" in resp.result

    def test_list_backtest_strategies_command(self) -> None:
        """Command 'list_backtest_strategies' returns all registered strategies."""
        orchestrator = MasterOrchestrator(name="test_orchestrator")
        repo = SQLiteHistoricalBarRepository(db_path=":memory:")
        engine = BacktestEngine(
            repository=repo,
            quality_engine=MarketDataQualityEngine(),
            analysis_engine=TechnicalAnalysisEngine(),
        )
        register_backtest_handlers(orchestrator=orchestrator, engine=engine)

        req = OrchestratorRequest.create(command="list_backtest_strategies")
        resp = orchestrator.handle_request(req)
        assert resp.status.value == "COMPLETED"
        strategies = resp.result["strategies"]
        assert len(strategies) >= 1
        assert any(s["name"] == "sma_crossover" for s in strategies)

    def test_unknown_strategy_validation_error(self) -> None:
        """Command 'run_backtest' with unknown strategy fails with clear ValidationError."""
        orchestrator = MasterOrchestrator(name="test_orchestrator")
        repo = SQLiteHistoricalBarRepository(db_path=":memory:")
        engine = BacktestEngine(
            repository=repo,
            quality_engine=MarketDataQualityEngine(),
            analysis_engine=TechnicalAnalysisEngine(),
        )
        register_backtest_handlers(orchestrator=orchestrator, engine=engine)

        req = OrchestratorRequest.create(
            command="run_backtest",
            context={
                "symbol": "RELIANCE",
                "strategy": "quantum_money_printer_9000",
                "start_time": 100.0,
                "end_time": 200.0,
            },
        )
        resp = orchestrator.handle_request(req)
        assert resp.status.value == "FAILED"
        assert any("Unknown strategy" in err for err in resp.errors)

    def test_system_bootstrap_and_safety_invariants(self) -> None:
        """Bootstrap initializes BacktestEngine, while execution remains locked."""
        status = bootstrap_system()
        assert status.initialized is True
        assert status.backtest_engine is not None
        assert status.strategy_registry is not None
        assert status.strategy_registry.has_strategy("sma_crossover") is True

        # CRITICAL SAFETY INVARIANTS:
        assert status.settings.execution_locked is True
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
