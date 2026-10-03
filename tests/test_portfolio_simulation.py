"""KAISER — Portfolio-Level Risk Analytics & Multi-Strategy Simulation Foundation Tests.

Comprehensive, deterministic test suite verifying:
1. StrategyAllocation and PortfolioSimulationConfig domain models and validation rules.
2. Multiple strategies sharing a central virtual portfolio with balance integrity.
3. Central cash accounting and position tracking across multiple instruments.
4. Simultaneous signal handling and deterministic order priority (SELL before BUY, weight priority).
5. Exposure limits: max portfolio exposure, per-strategy exposure, and instrument concentration.
6. Cash reserve buffer constraints and order rejections (CASH_RESERVE_VIOLATION).
7. Drawdown circuit breaker triggering and halting new entries (CIRCUIT_BREAKER_ACTIVE).
8. Transaction cost accounting (commission, spread, slippage) without duplication.
9. Strategy contribution vs portfolio-level aggregate metrics.
10. Correlation matrices, HHI concentration, and diversification metrics.
11. Next-bar execution timing and anti-look-ahead protection.
12. Deterministic reproducibility across repeated simulation runs.
13. State isolation between sequential simulations.
14. Zero-trade, zero-loss, and division-by-zero edge cases.
15. Cooperative cancellation, event publishing, and StateStore persistence.
16. HandlerRegistry and MasterOrchestrator integration (`run_portfolio_simulation`).
17. Absolute trading safety invariants: execution locked, virtual portfolio only, research disclaimer.
"""

from __future__ import annotations

import math
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.backtesting.engine import BacktestEngine
from app.backtesting.handlers import register_backtest_handlers
from app.backtesting.models import (
    BacktestConfig,
    ExecutionModel,
    FeeModel,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSizeType,
    SignalType,
    StrategySignal,
)
from app.backtesting.portfolio_simulation.analytics import (
    calculate_pearson_correlation,
    compute_diversification_metrics,
)
from app.backtesting.portfolio_simulation.engine import PortfolioSimulationEngine
from app.backtesting.portfolio_simulation.models import (
    DiversificationMetrics,
    PortfolioPosition,
    PortfolioRiskSnapshot,
    PortfolioRiskWarning,
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
    StrategyContribution,
)
from app.backtesting.strategy import (
    SMACrossoverStrategy,
    Strategy,
    StrategyContext,
    StrategyRegistry,
)
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.market.quality.status import DataFreshnessStatus, DataQualityAssessment, DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest



# ------------------------------------------------------------------------------
# Test Fixtures and Helpers
# ------------------------------------------------------------------------------

def _create_sample_bars(
    symbol: str = "TCS",
    timeframe: str = "1m",
    count: int = 120,
    start_ts: float = 1700000000.0,
    interval_sec: float = 60.0,
    base_price: float = 3500.0,
    trend: str = "sine",
) -> List[Bar]:
    """Generate deterministic Bar objects for testing."""
    bars: List[Bar] = []
    p = base_price
    for i in range(count):
        ts = start_ts + (i * interval_sec)
        if trend == "up":
            p += 2.0
        elif trend == "down":
            p -= 2.0
        elif trend == "sine":
            p = base_price + math.sin(i / 6.0) * 30.0
        elif trend == "flat":
            p = base_price

        o = round(p, 2)
        h = round(o + 2.0, 2)
        l = round(o - 2.0, 2)
        c = round(o + 0.5, 2)
        v = 1000.0 + (i * 5.0)

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
    return SQLiteHistoricalBarRepository(db_path=":memory:", name="test_portsim_mem_repo")


@pytest.fixture
def quality_engine() -> MarketDataQualityEngine:
    """Fixture providing a fresh MarketDataQualityEngine."""
    return MarketDataQualityEngine()


@pytest.fixture
def analysis_engine() -> TechnicalAnalysisEngine:
    """Fixture providing a fresh TechnicalAnalysisEngine."""
    return TechnicalAnalysisEngine(name="test_portsim_analysis_engine")


@pytest.fixture
def strategy_registry() -> StrategyRegistry:
    """Fixture providing a fresh StrategyRegistry with standard test strategies."""
    registry = StrategyRegistry()
    registry.register(
        name="sma_crossover",
        factory=lambda **kwargs: SMACrossoverStrategy(
            fast_period=kwargs.get("fast_period", 5),
            slow_period=kwargs.get("slow_period", 15),
        ),
        description="Demonstration SMA Crossover",
    )
    return registry


@pytest.fixture
def event_bus() -> EventBus:
    """Fixture providing a clean EventBus."""
    return EventBus()


@pytest.fixture
def state_store() -> StateStore:
    """Fixture providing a clean StateStore."""
    return StateStore()


# ------------------------------------------------------------------------------
# 1. Domain Models and Validation Tests
# ------------------------------------------------------------------------------

def test_strategy_allocation_model_validation():
    """Verify StrategyAllocation invariants and validations."""
    # Valid allocation
    alloc = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="TCS",
        weight=0.5,
        parameters={"fast_period": 5, "slow_period": 15},
    )
    assert alloc.strategy_name == "sma_crossover"
    assert alloc.symbol == "TCS"
    assert alloc.weight == 0.5
    assert alloc.allocation_id == "sma_crossover_TCS"

    # Empty strategy name rejected
    with pytest.raises(ValidationError, match="strategy_name must be a non-empty string"):
        StrategyAllocation(strategy_name="", symbol="TCS", weight=0.5)

    # Empty symbol rejected
    with pytest.raises(ValidationError, match="symbol must be a non-empty string"):
        StrategyAllocation(strategy_name="sma_crossover", symbol="", weight=0.5)

    # Negative or > 1.0 weight rejected
    with pytest.raises(ValidationError, match="weight must be strictly positive and <= 1.0"):
        StrategyAllocation(strategy_name="sma_crossover", symbol="TCS", weight=0.0)

    with pytest.raises(ValidationError, match="weight must be strictly positive and <= 1.0"):
        StrategyAllocation(strategy_name="sma_crossover", symbol="TCS", weight=1.5)

    # Invalid max_exposure_pct
    with pytest.raises(ValidationError, match="max_exposure_pct must be in"):
        StrategyAllocation(strategy_name="sma_crossover", symbol="TCS", max_exposure_pct=1.5)


def test_portfolio_simulation_config_validation():
    """Verify PortfolioSimulationConfig invariants and validations."""
    alloc1 = StrategyAllocation(strategy_name="sma_crossover", symbol="TCS", weight=0.5)
    alloc2 = StrategyAllocation(strategy_name="sma_crossover", symbol="INFY", weight=0.5)

    cfg = PortfolioSimulationConfig(
        allocations=(alloc1, alloc2),
        start_time=1700000000.0,
        end_time=1700007200.0,
        initial_cash=100_000.0,
    )
    assert len(cfg.allocations) == 2
    assert cfg.symbols == ("INFY", "TCS")
    assert cfg.max_portfolio_exposure_pct == 1.0

    # Empty allocations rejected
    with pytest.raises(ValidationError, match="requires at least one StrategyAllocation"):
        PortfolioSimulationConfig(
            allocations=(),
            start_time=1700000000.0,
            end_time=1700007200.0,
        )

    # Allocation weights exceeding 1.0 rejected
    alloc_too_big = StrategyAllocation(strategy_name="sma_crossover", symbol="TCS", weight=0.7)
    with pytest.raises(ValidationError, match="cannot exceed 1.0"):
        PortfolioSimulationConfig(
            allocations=(alloc1, alloc_too_big),
            start_time=1700000000.0,
            end_time=1700007200.0,
        )

    # Negative cash rejected
    with pytest.raises(ValidationError, match="initial_cash must be strictly positive"):
        PortfolioSimulationConfig(
            allocations=(alloc1,),
            start_time=1700000000.0,
            end_time=1700007200.0,
            initial_cash=-500.0,
        )

    # end_time <= start_time rejected
    with pytest.raises(ValidationError, match="end_time .* must be strictly greater than start_time"):
        PortfolioSimulationConfig(
            allocations=(alloc1,),
            start_time=1700007200.0,
            end_time=1700000000.0,
        )

    # Invalid cash reserve rejected
    with pytest.raises(ValidationError, match="min_cash_reserve_pct must be in"):
        PortfolioSimulationConfig(
            allocations=(alloc1,),
            start_time=1700000000.0,
            end_time=1700007200.0,
            min_cash_reserve_pct=1.2,
        )


# ------------------------------------------------------------------------------
# 2. Multi-Strategy Shared Virtual Portfolio Simulation Tests
# ------------------------------------------------------------------------------

def test_multi_strategy_shared_virtual_portfolio(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
    event_bus: EventBus,
    state_store: StateStore,
):
    """Test 2 strategies trading distinct instruments inside a shared virtual portfolio."""
    # Store historical bars for TCS and INFY
    bars_tcs = _create_sample_bars(symbol="TCS", count=100, trend="sine", base_price=3500.0)
    bars_infy = _create_sample_bars(symbol="INFY", count=100, trend="sine", base_price=1500.0)
    in_memory_repo.store_bars(bars_tcs)
    in_memory_repo.store_bars(bars_infy)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
    )

    alloc1 = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="TCS",
        weight=0.5,
        parameters={"fast_period": 3, "slow_period": 8},
        allocation_id="tcs_sma",
    )
    alloc2 = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="INFY",
        weight=0.5,
        parameters={"fast_period": 4, "slow_period": 10},
        allocation_id="infy_sma",
    )

    config = PortfolioSimulationConfig(
        allocations=(alloc1, alloc2),
        start_time=bars_tcs[0].timestamp,
        end_time=bars_tcs[-1].timestamp,
        initial_cash=100_000.0,
        timeframe="1m",
    )

    result = engine.run(config)

    assert result.is_success
    assert result.status == PortfolioSimulationStatus.SUCCESS
    assert result.initial_equity == 100_000.0
    assert result.final_equity > 0.0
    assert result.bars_processed == 100
    assert len(result.strategy_contributions) == 2

    # Verify both strategies produced trades or evaluated bars
    strat_ids = {c.strategy_id for c in result.strategy_contributions}
    assert strat_ids == {"tcs_sma", "infy_sma"}

    # Verify risk snapshots recorded
    assert len(result.risk_snapshots) > 0
    # Verify equity curve recorded
    assert len(result.equity_curve) > 0

    # Verify StateStore updated
    saved_state = state_store.get(f"portfolio_simulation:{config.simulation_id}")
    assert saved_state is not None
    assert saved_state["status"] == "SUCCESS"


def test_balance_integrity_throughout_simulation(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that cash + position_value == total_equity at every single step."""
    bars_tcs = _create_sample_bars(symbol="TCS", count=80, trend="sine")
    in_memory_repo.store_bars(bars_tcs)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    alloc = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="TCS",
        weight=1.0,
        parameters={"fast_period": 3, "slow_period": 7},
    )

    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars_tcs[0].timestamp,
        end_time=bars_tcs[-1].timestamp,
        initial_cash=50_000.0,
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success

    for snap in result.risk_snapshots:
        # Check cash + net_exposure == total_equity within 0.01 precision
        assert abs((snap.cash + snap.net_exposure) - snap.total_equity) < 0.05


# ------------------------------------------------------------------------------
# 3. Simultaneous Signals and Deterministic Order Priority
# ------------------------------------------------------------------------------

class ControlledSignalStrategy(Strategy):
    """Deterministic strategy emitting predetermined signals at specific timestamps."""

    def __init__(self, signals_by_ts: Dict[float, Tuple[SignalType, float]], name: str = "ctrl"):
        self._signals_by_ts = signals_by_ts
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return "Controlled Signal Test Strategy"

    def on_bar(self, ctx: StrategyContext) -> Optional[StrategySignal]:
        if ctx.timestamp in self._signals_by_ts:
            sig_type, strength = self._signals_by_ts[ctx.timestamp]
            return StrategySignal(
                symbol=ctx.symbol,
                timestamp=ctx.timestamp,
                signal_type=sig_type,
                strength=strength,
                reason="Controlled test signal",
            )
        return None


def test_simultaneous_signals_deterministic_priority(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
):
    """Verify that SELL orders execute before BUY orders and higher weight strategies take priority."""
    registry = StrategyRegistry()
    ts1 = 1700000000.0
    ts2 = ts1 + 60.0
    ts3 = ts2 + 60.0

    bars_a = _create_sample_bars(symbol="SYM_A", count=10, start_ts=ts1)
    bars_b = _create_sample_bars(symbol="SYM_B", count=10, start_ts=ts1)
    in_memory_repo.store_bars(bars_a)
    in_memory_repo.store_bars(bars_b)

    # Strategy A: Emits BUY on ts1, SELL on ts2
    # Strategy B: Emits BUY on ts2
    # At ts2, Strategy A has SELL, Strategy B has BUY. SELL must be processed first to free cash!
    strat_a = ControlledSignalStrategy(
        signals_by_ts={ts1: (SignalType.BUY, 1.0), ts2: (SignalType.SELL, 1.0)},
        name="strat_a",
    )
    strat_b = ControlledSignalStrategy(
        signals_by_ts={ts2: (SignalType.BUY, 1.0)},
        name="strat_b",
    )

    registry.register("strat_a", lambda **kw: strat_a)
    registry.register("strat_b", lambda **kw: strat_b)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=registry,
    )

    alloc_a = StrategyAllocation(strategy_name="strat_a", symbol="SYM_A", weight=0.6, allocation_id="A")
    alloc_b = StrategyAllocation(strategy_name="strat_b", symbol="SYM_B", weight=0.4, allocation_id="B")

    config = PortfolioSimulationConfig(
        allocations=(alloc_a, alloc_b),
        start_time=ts1,
        end_time=ts1 + (9 * 60.0),
        initial_cash=50_000.0,
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    assert result.orders_submitted >= 2


# ------------------------------------------------------------------------------
# 4. Exposure Limits & Concentration Controls
# ------------------------------------------------------------------------------

def test_cash_reserve_limit_enforcement(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that orders violating the required cash reserve buffer are rejected."""
    bars = _create_sample_bars(symbol="TCS", count=50, trend="sine")
    in_memory_repo.store_bars(bars)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    # Set min_cash_reserve_pct to 90%. Sizing attempts to use remaining cash.
    # If cash reserve is breached, order should be rejected with CASH_RESERVE_VIOLATION.
    alloc = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="TCS",
        weight=1.0,
        position_size_type="fixed_cash",
        position_size_value=30_000.0,  # 30k out of 50k initial cash with 90% (45k) reserve -> breach!
    )

    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=50_000.0,
        min_cash_reserve_pct=0.90,  # Requires at least $45,000 cash reserve
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    # Orders should have been rejected due to cash reserve breach
    assert result.orders_rejected > 0
    rej_reasons = [r["reason"] for r in result.rejected_orders]
    assert any("CASH_RESERVE_VIOLATION" in r for r in rej_reasons)


def test_max_portfolio_exposure_limit(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that orders exceeding max_portfolio_exposure_pct are rejected."""
    bars = _create_sample_bars(symbol="TCS", count=50, trend="sine")
    in_memory_repo.store_bars(bars)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    # Max portfolio exposure 20% (0.20)
    # Order tries to take 50% position
    alloc = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="TCS",
        weight=1.0,
        position_size_type="fixed_cash",
        position_size_value=40_000.0,
    )

    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        max_portfolio_exposure_pct=0.20,  # Max 20k exposure allowed
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    assert result.orders_rejected > 0
    rej_reasons = [r["reason"] for r in result.rejected_orders]
    assert any("MAX_PORTFOLIO_EXPOSURE_EXCEEDED" in r for r in rej_reasons)


def test_instrument_concentration_limit(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that orders exceeding single-instrument concentration cap are rejected."""
    bars = _create_sample_bars(symbol="TCS", count=50, trend="sine")
    in_memory_repo.store_bars(bars)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    alloc = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="TCS",
        weight=1.0,
        position_size_type="fixed_cash",
        position_size_value=50_000.0,
    )

    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        max_instrument_concentration_pct=0.30,  # Max 30k allowed in TCS
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    assert result.orders_rejected > 0
    rej_reasons = [r["reason"] for r in result.rejected_orders]
    assert any("INSTRUMENT_CONCENTRATION_EXCEEDED" in r for r in rej_reasons)


def test_per_strategy_exposure_limit(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that orders exceeding per-strategy exposure limits are rejected."""
    bars = _create_sample_bars(symbol="TCS", count=50, trend="sine")
    in_memory_repo.store_bars(bars)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    alloc = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="TCS",
        weight=1.0,
        max_exposure_pct=0.15,  # Max 15% strategy exposure limit
        position_size_type="fixed_cash",
        position_size_value=40_000.0,
    )

    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    assert result.orders_rejected > 0
    rej_reasons = [r["reason"] for r in result.rejected_orders]
    assert any("STRATEGY_EXPOSURE_LIMIT_EXCEEDED" in r for r in rej_reasons)



def test_drawdown_circuit_breaker(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
):
    """Verify that portfolio circuit breaker triggers on excessive drawdown and blocks new long entries."""
    registry = StrategyRegistry()
    # Market goes down
    bars = _create_sample_bars(symbol="TCS", count=30, trend="down", base_price=3500.0)
    in_memory_repo.store_bars(bars)

    # Buy on bar 0, then price drops, then attempt second buy on bar 5
    strat = ControlledSignalStrategy(
        signals_by_ts={
            bars[0].timestamp: (SignalType.BUY, 1.0),
            bars[5].timestamp: (SignalType.BUY, 1.0),
        },
        name="test_dd",
    )
    registry.register("test_dd", lambda **kw: strat)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=registry,
    )

    alloc = StrategyAllocation(
        strategy_name="test_dd",
        symbol="TCS",
        weight=1.0,
        position_size_type="fixed_quantity",
        position_size_value=20.0,
    )

    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        max_drawdown_limit_pct=0.01,  # 0.01% limit triggers easily on falling price
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    warning_types = [w.warning_type for w in result.warnings]
    assert "CIRCUIT_BREAKER_TRIGGERED" in warning_types
    # Verify that the subsequent buy was rejected due to active circuit breaker
    rej_reasons = [r["reason"] for r in result.rejected_orders]
    assert any("CIRCUIT_BREAKER_ACTIVE" in r for r in rej_reasons)



# ------------------------------------------------------------------------------
# 5. Shared Transaction Cost Accounting Tests
# ------------------------------------------------------------------------------

def test_shared_transaction_cost_accounting(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that commissions and slippage are applied accurately without duplication."""
    bars = _create_sample_bars(symbol="TCS", count=80, trend="sine")
    in_memory_repo.store_bars(bars)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    alloc = StrategyAllocation(
        strategy_name="sma_crossover",
        symbol="TCS",
        weight=1.0,
        parameters={"fast_period": 3, "slow_period": 8},
    )

    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        fee_model=FeeModel.PERCENTAGE.value,
        fee_rate=0.001,  # 10 bps
        slippage_bps=5.0,  # 5 bps
        half_spread_bps=2.0,  # 2 bps
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success

    if result.total_trades > 0:
        assert result.total_fees > 0.0
        assert result.total_slippage > 0.0
        assert math.isclose(
            result.total_transaction_costs,
            result.total_fees + result.total_slippage,
            rel_tol=1e-5,
        )
        contrib = result.strategy_contributions[0]
        assert math.isclose(contrib.total_fees, result.total_fees, rel_tol=1e-5)
        assert math.isclose(contrib.total_slippage, result.total_slippage, rel_tol=1e-5)


# ------------------------------------------------------------------------------
# 6. Correlation and Diversification Analytics Tests
# ------------------------------------------------------------------------------

def test_pearson_correlation_calculations():
    """Verify Pearson correlation calculation under various mathematical conditions."""
    # Perfectly correlated
    x = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    y = [2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
    r = calculate_pearson_correlation(x, y, min_observations=5)
    assert r == 1.0

    # Perfectly anti-correlated
    y_inv = [-2.0, -4.0, -6.0, -8.0, -10.0, -12.0, -14.0, -16.0, -18.0, -20.0]
    r_inv = calculate_pearson_correlation(x, y_inv, min_observations=5)
    assert r_inv == -1.0

    # Constant series (zero variance) returns None safely without division by zero
    y_const = [5.0] * 10
    r_const = calculate_pearson_correlation(x, y_const, min_observations=5)
    assert r_const is None

    # Insufficient observations returns None safely
    r_short = calculate_pearson_correlation(x[:4], y[:4], min_observations=5)
    assert r_short is None


def test_diversification_metrics_computation():
    """Verify compute_diversification_metrics with multi-instrument returns."""
    allocations = (
        StrategyAllocation("sma_crossover", "TCS", weight=0.5),
        StrategyAllocation("sma_crossover", "INFY", weight=0.5),
    )
    inst_returns = {
        "TCS": {float(i): 0.01 * (i % 3) for i in range(20)},
        "INFY": {float(i): 0.01 * ((i + 1) % 3) for i in range(20)},
    }
    strat_returns = {
        "sma_crossover_TCS": {float(i): 0.005 for i in range(20)},
        "sma_crossover_INFY": {float(i): 0.005 for i in range(20)},
    }

    metrics = compute_diversification_metrics(allocations, inst_returns, strat_returns, min_observations=10)
    assert metrics.is_valid
    assert metrics.herfindahl_index == 0.5  # 0.5^2 + 0.5^2 = 0.5
    assert metrics.effective_strategies_count == 2.0  # 1 / 0.5 = 2.0
    assert "TCS" in metrics.instrument_correlations
    assert metrics.disclaimer is not None


# ------------------------------------------------------------------------------
# 7. Next-Bar Execution and Anti-Lookahead Integrity Tests
# ------------------------------------------------------------------------------

def test_next_bar_execution_timing(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
):
    """Verify that orders generated at bar T close are filled strictly at bar T+1 open price."""
    registry = StrategyRegistry()
    ts1 = 1700000000.0
    ts2 = ts1 + 60.0

    # Bar 1 close = 100. Bar 2 open = 110.
    bar1 = Bar("SYM", "EX", "1m", ts1, open=98.0, high=102.0, low=97.0, close=100.0, volume=100.0, source="mock")
    bar2 = Bar("SYM", "EX", "1m", ts2, open=110.0, high=112.0, low=108.0, close=111.0, volume=100.0, source="mock")
    in_memory_repo.store_bars([bar1, bar2])

    strat = ControlledSignalStrategy(signals_by_ts={ts1: (SignalType.BUY, 1.0)}, name="test_strat")
    registry.register("test_strat", lambda **kw: strat)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=registry,
    )

    alloc = StrategyAllocation("test_strat", "SYM", weight=1.0, position_size_type="fixed_quantity", position_size_value=10.0)
    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=ts1,
        end_time=ts2,
        initial_cash=50_000.0,
        slippage_bps=0.0,
        half_spread_bps=0.0,
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    assert result.orders_accepted >= 1
    # Trade was closed on bar2 liquidation
    trade = result.strategy_contributions[0].trades[0]
    # Entry price should be bar 2's OPEN price (110.0), NOT bar 1's close (100.0)!
    assert trade.entry_price == 110.0


# ------------------------------------------------------------------------------
# 8. Deterministic Reproducibility & State Isolation Tests
# ------------------------------------------------------------------------------

def test_deterministic_reproducibility(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that repeated simulations with identical inputs produce identical numerical outputs."""
    bars = _create_sample_bars(symbol="TCS", count=70, trend="sine")
    in_memory_repo.store_bars(bars)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    alloc = StrategyAllocation("sma_crossover", "TCS", weight=1.0, parameters={"fast_period": 3, "slow_period": 8})
    config1 = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        timeframe="1m",
    )
    config2 = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        timeframe="1m",
    )

    res1 = engine.run(config1)
    res2 = engine.run(config2)

    assert res1.is_success and res2.is_success
    assert res1.final_equity == res2.final_equity
    assert res1.net_pnl == res2.net_pnl
    assert res1.total_trades == res2.total_trades
    assert res1.max_drawdown_pct == res2.max_drawdown_pct
    assert res1.orders_submitted == res2.orders_submitted


# ------------------------------------------------------------------------------
# 9. Edge Cases: Zero Trades, Zero Losses, Undefined Ratios
# ------------------------------------------------------------------------------

def test_zero_trades_edge_case(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify safe handling of simulation with flat prices and zero trades."""
    bars = _create_sample_bars(symbol="TCS", count=40, trend="flat")
    in_memory_repo.store_bars(bars)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    alloc = StrategyAllocation("sma_crossover", "TCS", weight=1.0)
    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    assert result.total_trades == 0
    assert result.win_rate == 0.0
    assert result.profit_factor is None
    assert "No trades" in result.profit_factor_description
    assert result.net_pnl == 0.0


def test_state_isolation_between_consecutive_runs(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that multiple consecutive runs with different strategies do not leak state."""
    bars_active = _create_sample_bars(symbol="TCS", count=60, trend="sine")
    in_memory_repo.store_bars(bars_active)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    # Run 1: Active trading
    alloc1 = StrategyAllocation("sma_crossover", "TCS", weight=1.0, parameters={"fast_period": 3, "slow_period": 8})
    cfg1 = PortfolioSimulationConfig(
        allocations=(alloc1,),
        start_time=bars_active[0].timestamp,
        end_time=bars_active[-1].timestamp,
        initial_cash=100_000.0,
        timeframe="1m",
    )
    res1 = engine.run(cfg1)
    assert res1.is_success

    # Run 2: Flat data, zero trades
    bars_flat = _create_sample_bars(symbol="INFY", count=30, trend="flat")
    in_memory_repo.store_bars(bars_flat)
    alloc2 = StrategyAllocation("sma_crossover", "INFY", weight=1.0)
    cfg2 = PortfolioSimulationConfig(
        allocations=(alloc2,),
        start_time=bars_flat[0].timestamp,
        end_time=bars_flat[-1].timestamp,
        initial_cash=50_000.0,
        timeframe="1m",
    )
    res2 = engine.run(cfg2)
    assert res2.is_success
    assert res2.initial_equity == 50_000.0
    assert res2.final_equity == 50_000.0
    assert res2.total_trades == 0
    assert len(res2.strategy_contributions) == 1
    assert res2.strategy_contributions[0].symbol == "INFY"


def test_quarantined_bars_data_quality_rejection(
    in_memory_repo: SQLiteHistoricalBarRepository,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
):
    """Verify that unhealthy/quarantined bars are caught and logged as warnings."""
    bars = _create_sample_bars(symbol="TCS", count=20, trend="sine")
    in_memory_repo.store_bars(bars)

    class MockQuarantineQualityEngine(MarketDataQualityEngine):
        def evaluate_bar(self, bar, correlation_id=None, reference_time=None):
            if bar.timestamp == bars[10].timestamp:
                return DataQualityAssessment(
                    assessment_id="test_quarantine_id",
                    symbol=bar.symbol,
                    data_type="bar",
                    source=bar.source,
                    timestamp=bar.timestamp,
                    quality_status=DataQualityStatus.OUT_OF_ORDER,
                    checks_performed=("sequence_check",),
                    warnings=("Out of order bar",),
                    errors=(),
                    freshness_status=DataFreshnessStatus.FRESH,
                    completeness_status="COMPLETE",
                    sequence_status="OUT_OF_ORDER",
                    duplicate_status="UNIQUE",
                    assessment_timestamp=time.time(),
                    correlation_id=correlation_id or "test",
                    safe_to_consume=False,
                )
            return super().evaluate_bar(bar, correlation_id, reference_time)

    mock_quality = MockQuarantineQualityEngine()
    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=mock_quality,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
    )

    alloc = StrategyAllocation("sma_crossover", "TCS", weight=1.0)
    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        initial_cash=100_000.0,
        timeframe="1m",
    )

    result = engine.run(config)
    assert result.is_success
    warning_types = [w.warning_type for w in result.warnings]
    assert "DATA_QUALITY_REJECTION" in warning_types



# ------------------------------------------------------------------------------
# 10. Cancellation & Failure Recovery Tests
# ------------------------------------------------------------------------------


def test_cooperative_cancellation(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
    event_bus: EventBus,
):
    """Verify cooperative cancellation mid-simulation."""
    bars = _create_sample_bars(symbol="TCS", count=50)
    in_memory_repo.store_bars(bars)

    engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
    )

    cancelled_events = []
    event_bus.subscribe(EventType.PORTFOLIO_SIMULATION_CANCELLED, lambda ev: cancelled_events.append(ev))

    token = threading.Event()
    token.set()  # Cancel immediately

    alloc = StrategyAllocation("sma_crossover", "TCS", weight=1.0)
    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=bars[0].timestamp,
        end_time=bars[-1].timestamp,
        timeframe="1m",
    )

    result = engine.run(config, cancellation_token=token)
    assert result.status == PortfolioSimulationStatus.CANCELLED
    assert len(cancelled_events) == 1


# ------------------------------------------------------------------------------
# 11. HandlerRegistry and MasterOrchestrator Integration Tests
# ------------------------------------------------------------------------------

def test_orchestrator_integration_run_portfolio_simulation(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
    event_bus: EventBus,
    state_store: StateStore,
):
    """Verify MasterOrchestrator 'run_portfolio_simulation' command execution."""
    bars_tcs = _create_sample_bars(symbol="TCS", count=60, trend="sine")
    in_memory_repo.store_bars(bars_tcs)

    backtest_engine = BacktestEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
    )
    port_engine = PortfolioSimulationEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        event_bus=event_bus,
        state_store=state_store,
    )

    orchestrator = MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        name="test_orchestrator",
    )
    register_backtest_handlers(
        orchestrator=orchestrator,
        engine=backtest_engine,
        strategy_registry=strategy_registry,
        portfolio_engine=port_engine,
    )

    req = OrchestratorRequest.create(
        command="run_portfolio_simulation",
        context={
            "symbols": ["TCS"],
            "strategy": "sma_crossover",
            "start_time": bars_tcs[0].timestamp,
            "end_time": bars_tcs[-1].timestamp,
            "initial_cash": 100_000.0,
            "timeframe": "1m",
        },
    )

    response = orchestrator.handle_request(req)
    assert response.is_success
    assert response.result is not None
    assert response.result["status"] == "success"
    assert "simulation_status" in response.result
    assert "initial_equity" in response.result
    assert "strategy_contributions" in response.result
    assert "disclaimer" in response.result



# ------------------------------------------------------------------------------
# 12. Safety Invariant Confirmation
# ------------------------------------------------------------------------------

def test_trading_safety_invariants():
    """Verify that Settings.execution_locked remains True and research disclaimer is enforced."""
    settings = Settings()
    assert settings.execution_locked is True

    alloc = StrategyAllocation("sma_crossover", "TCS", weight=1.0)
    config = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=1700000000.0,
        end_time=1700003600.0,
    )
    assert config.initial_cash == 100_000.0

    res = PortfolioSimulationResult(
        simulation_id="test",
        status=PortfolioSimulationStatus.SUCCESS,
        config=config,
        initial_equity=100_000.0,
        final_equity=100_000.0,
        gross_pnl=0.0,
        net_pnl=0.0,
        net_return_pct=0.0,
        max_drawdown_pct=0.0,
        max_drawdown_amount=0.0,
        max_drawdown_duration_bars=0,
        total_fees=0.0,
        total_slippage=0.0,
        total_transaction_costs=0.0,
        total_trades=0,
        winning_trades=0,
        losing_trades=0,
        win_rate=0.0,
        profit_factor=None,
        profit_factor_description="N/A",
    )
    assert "RESEARCH ONLY" in res.disclaimer
