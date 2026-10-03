"""KAISER — Strategy Interface, Context, and Registry.

Defines the core deterministic Strategy contract, the isolated StrategyContext,
and a demonstration SMA crossover strategy strictly for engine verification.

SAFETY INVARIANTS:
1. Strategy cannot look ahead: Context exposes only bars and indicators strictly
   up to and including the current bar's closing timestamp.
2. Strategy produces hypothetical signals only; it cannot execute trades or mutate portfolio.
3. Demonstration strategy is explicitly research-only and non-profitable.
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Type, Union

from app.analysis.models import TechnicalFeatureVector
from app.backtesting.models import BacktestConfig, SignalType, StrategySignal
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.market.models import Bar

logger = get_logger("backtesting.strategy")


@dataclass(frozen=True)
class StrategyContext:
    """Bounded, time-isolated context provided to a Strategy on each bar.

    CRITICAL ANTI-LOOK-AHEAD INVARIANT:
    Contains ONLY information that was physically available at or before
    `current_bar.timestamp`. No future bars, quotes, or future-derived indicators
    can be accessed through this context.

    Attributes:
        symbol: Ticker symbol.
        timeframe: Bar timeframe.
        current_bar: The most recently closed bar being evaluated.
        timestamp: Timestamp of the current bar (simulation clock).
        feature_vector: Technical indicators evaluated up to current_bar.
        portfolio_cash: Current available virtual cash.
        position_quantity: Currently held quantity (positive for long).
        position_avg_cost: Weighted average cost basis of current position.
        position_market_value: Current position marked to market.
        portfolio_equity: Total portfolio equity (cash + position_market_value).
        bars_history: Past bars up to and including current_bar.
    """

    symbol: str
    timeframe: str
    current_bar: Bar
    timestamp: float
    feature_vector: Optional[TechnicalFeatureVector]
    portfolio_cash: float
    position_quantity: float
    position_avg_cost: float
    position_market_value: float
    portfolio_equity: float
    bars_history: Sequence[Bar]

    def __post_init__(self) -> None:
        if self.current_bar.timestamp != self.timestamp:
            raise ValidationError(
                f"StrategyContext timestamp ({self.timestamp}) must match current_bar ({self.current_bar.timestamp})"
            )

    @property
    def has_position(self) -> bool:
        """True if the portfolio currently holds shares/contracts in this symbol."""
        return self.position_quantity > 0.000001

    @property
    def is_flat(self) -> bool:
        """True if no position is currently held."""
        return not self.has_position

    @property
    def unrealized_pnl(self) -> float:
        """Unrealized dollar profit/loss on open position."""
        if not self.has_position:
            return 0.0
        return self.position_market_value - (self.position_quantity * self.position_avg_cost)

    def get_indicator(self, name: str) -> Optional[float]:
        """Retrieve a specific indicator value from the current feature vector."""
        if not self.feature_vector:
            return None
        val_obj = self.feature_vector.indicators.get(name)
        if val_obj is not None and val_obj.is_valid:
            return val_obj.value
        return None

    def get_indicator_signal(self, name: str) -> Optional[str]:
        """Retrieve a categorical indicator signal (e.g. 'BULLISH', 'BEARISH')."""
        if not self.feature_vector:
            return None
        return self.feature_vector.signals.get(name)

    def get_bars(self, lookback: Optional[int] = None) -> Sequence[Bar]:
        """Retrieve historical bars up to current_bar, bounded by lookback.

        Guaranteed: all returned bars have timestamp <= self.timestamp.
        """
        if lookback is None or lookback >= len(self.bars_history):
            return self.bars_history
        return self.bars_history[-lookback:]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize context snapshot to dictionary."""
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "timestamp": self.timestamp,
            "current_price": self.current_bar.close,
            "portfolio_cash": round(self.portfolio_cash, 2),
            "position_quantity": self.position_quantity,
            "position_avg_cost": round(self.position_avg_cost, 4),
            "position_market_value": round(self.position_market_value, 2),
            "portfolio_equity": round(self.portfolio_equity, 2),
            "has_position": self.has_position,
            "bars_count": len(self.bars_history),
        }


class Strategy(ABC):
    """Abstract Base Class for all deterministic research strategies.

    Responsibilities:
    - Pure signal generation: evaluate market state in StrategyContext and emit StrategySignal.
    - Separation of concerns: never directly places trades, mutates portfolio, or accesses broker.
    - State resetting: must reset all internal indicators/counters between independent runs.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique strategy name."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable strategy description."""

    @property
    def version(self) -> str:
        """Strategy version string."""
        return "1.0.0"

    @abstractmethod
    def on_bar(self, ctx: StrategyContext) -> Optional[StrategySignal]:
        """Evaluate market state at candle close and optionally emit a StrategySignal.

        Args:
            ctx: Strictly time-isolated StrategyContext.

        Returns:
            Optional[StrategySignal]: Hypothetical trading signal or None.
        """

    def on_start(self, config: BacktestConfig) -> None:
        """Lifecycle hook invoked before backtesting begins."""
        pass

    def on_finish(self) -> None:
        """Lifecycle hook invoked after backtesting completes."""
        pass

    def reset(self) -> None:
        """Reset internal strategy state (counters, caches, crossover states)."""
        pass


class SMACrossoverStrategy(Strategy):
    """Demonstration SMA Crossover Research Strategy.

    DISCLAIMER:
    DEMONSTRATION RESEARCH STRATEGY ONLY.
    This strategy is designed purely to test and verify the backtesting engine,
    signal generation, order processing, and anti-look-ahead guarantees.
    It is NOT profitable, NOT optimized, and NOT intended for real financial trading.

    Logic:
    - Fast SMA period (e.g. 5) vs Slow SMA period (e.g. 15).
    - When Fast SMA crosses above Slow SMA and flat -> Emit BUY signal.
    - When Fast SMA crosses below Slow SMA and in position -> Emit SELL/CLOSE signal.
    """

    def __init__(
        self,
        fast_period: int = 5,
        slow_period: int = 15,
        name: str = "sma_crossover",
    ) -> None:
        if fast_period <= 0 or slow_period <= 0:
            raise ValidationError("SMA periods must be positive integers.")
        if fast_period >= slow_period:
            raise ValidationError(
                f"fast_period ({fast_period}) must be strictly less than slow_period ({slow_period})"
            )

        self._fast_period = fast_period
        self._slow_period = slow_period
        self._name = name
        self._prev_fast: Optional[float] = None
        self._prev_slow: Optional[float] = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return (
            f"Demonstration SMA Crossover Strategy (Fast={self._fast_period}, Slow={self._slow_period}). "
            "RESEARCH ONLY: Not profitable or intended for production use."
        )

    @property
    def fast_period(self) -> int:
        return self._fast_period

    @property
    def slow_period(self) -> int:
        return self._slow_period

    def reset(self) -> None:
        """Reset previous SMA values between backtest runs."""
        self._prev_fast = None
        self._prev_slow = None

    def on_bar(self, ctx: StrategyContext) -> Optional[StrategySignal]:
        """Evaluate SMA crossover at bar close."""
        # 1. Attempt to read SMAs from feature vector
        fast_val = ctx.get_indicator(f"SMA_{self._fast_period}")
        slow_val = ctx.get_indicator(f"SMA_{self._slow_period}")

        # Fallback calculation if not present in pre-configured feature vector
        if fast_val is None or slow_val is None:
            closes = [b.close for b in ctx.get_bars(self._slow_period)]
            if len(closes) >= self._slow_period:
                fast_val = sum(closes[-self._fast_period:]) / self._fast_period
                slow_val = sum(closes[-self._slow_period:]) / self._slow_period
            else:
                # Insufficient bars for SMA calculation
                return None

        signal: Optional[StrategySignal] = None

        if self._prev_fast is not None and self._prev_slow is not None:
            # Bullish Crossover: Fast crosses above Slow
            if self._prev_fast <= self._prev_slow and fast_val > slow_val:
                if ctx.is_flat:
                    signal = StrategySignal(
                        symbol=ctx.symbol,
                        timestamp=ctx.timestamp,
                        signal_type=SignalType.BUY,
                        strength=1.0,
                        reason=(
                            f"SMA Bullish Crossover: Fast({self._fast_period})={fast_val:.2f} "
                            f"crossed above Slow({self._slow_period})={slow_val:.2f}"
                        ),
                        metadata={
                            "fast_period": self._fast_period,
                            "slow_period": self._slow_period,
                            "fast_val": fast_val,
                            "slow_val": slow_val,
                        },
                    )

            # Bearish Crossover: Fast crosses below Slow
            elif self._prev_fast >= self._prev_slow and fast_val < slow_val:
                if ctx.has_position:
                    signal = StrategySignal(
                        symbol=ctx.symbol,
                        timestamp=ctx.timestamp,
                        signal_type=SignalType.SELL,
                        strength=1.0,
                        reason=(
                            f"SMA Bearish Crossover: Fast({self._fast_period})={fast_val:.2f} "
                            f"crossed below Slow({self._slow_period})={slow_val:.2f}"
                        ),
                        metadata={
                            "fast_period": self._fast_period,
                            "slow_period": self._slow_period,
                            "fast_val": fast_val,
                            "slow_val": slow_val,
                        },
                    )

        # Update previous state
        self._prev_fast = fast_val
        self._prev_slow = slow_val

        return signal


class StrategyRegistry:
    """Thread-safe catalog of registered backtesting strategies."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._factories: Dict[str, Callable[..., Strategy]] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}

        # Register default demonstration strategies
        self.register(
            name="sma_crossover",
            factory=lambda **kwargs: SMACrossoverStrategy(
                fast_period=kwargs.get("fast_period", 5),
                slow_period=kwargs.get("slow_period", 15),
            ),
            description="Demonstration SMA Crossover (Fast=5, Slow=15) - Research Only",
            version="1.0.0",
        )

    def register(
        self,
        name: str,
        factory: Callable[..., Strategy],
        description: str = "",
        version: str = "1.0.0",
        allow_override: bool = True,
    ) -> None:
        """Register a strategy factory."""
        key = name.strip().lower()
        with self._lock:
            if key in self._factories and not allow_override:
                raise ValidationError(f"Strategy '{name}' is already registered.")
            self._factories[key] = factory
            self._metadata[key] = {
                "name": key,
                "display_name": name,
                "description": description,
                "version": version,
            }
            logger.info("Registered backtest strategy: %s (%s)", name, description)

    def get(self, name: str, **kwargs: Any) -> Strategy:
        """Instantiate a strategy by name."""
        key = name.strip().lower()
        with self._lock:
            factory = self._factories.get(key)
            if not factory:
                raise ValidationError(
                    f"Unknown strategy '{name}'. Available: {list(self._factories.keys())}"
                )
            return factory(**kwargs)

    def has_strategy(self, name: str) -> bool:
        """Check if a strategy is registered."""
        with self._lock:
            return name.strip().lower() in self._factories

    def list_strategies(self) -> List[Dict[str, Any]]:
        """Return metadata for all registered strategies."""
        with self._lock:
            return list(self._metadata.values())
