"""KAISER — Backtesting & Strategy Evaluation Package.

Provides a deterministic, research-only backtesting engine utilizing historical
data storage, replay capabilities, quality validation gates, and virtual portfolio accounting.

SAFETY INVARIANTS:
1. Research only: strictly simulates hypothetical fills inside a virtual portfolio.
2. Broker isolation: Settings.execution_locked = True is strictly maintained.
3. Anti-look-ahead: signals evaluated at bar close execute on the arrival of the next bar.
"""

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
from app.backtesting.validation import (
    OutOfSampleSummary,
    ValidationWarning,
    ValidationWindow,
    ValidationWindowResult,
    WalkForwardConfig,
    WalkForwardResult,
    WalkForwardStatus,
    WalkForwardValidator,
    WindowMode,
    generate_walk_forward_windows,
)

__all__ = [
    "BacktestEngine",
    "BacktestConfig",
    "BacktestResult",
    "BacktestStatus",
    "PerformanceMetrics",
    "Strategy",
    "StrategyContext",
    "StrategyRegistry",
    "SMACrossoverStrategy",
    "SimulatedPortfolio",
    "Position",
    "SimulatedOrder",
    "SimulatedFill",
    "TradeRecord",
    "EquityPoint",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "SignalType",
    "ExecutionModel",
    "PositionSizeType",
    "FeeModel",
    "calculate_performance_metrics",
    "register_backtest_handlers",
    "WalkForwardValidator",
    "WalkForwardConfig",
    "WalkForwardResult",
    "WalkForwardStatus",
    "WindowMode",
    "ValidationWindow",
    "ValidationWindowResult",
    "OutOfSampleSummary",
    "ValidationWarning",
    "generate_walk_forward_windows",
]
