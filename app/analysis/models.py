"""KAISER — Technical & Quantitative Analysis Domain Models.

Provides strongly-typed, immutable data structures for technical indicators,
feature vectors, signals, and engine configurations.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from app.core.exceptions import ValidationError


class IndicatorType(str, Enum):
    """Supported technical and quantitative indicator types."""

    SMA = "SMA"
    EMA = "EMA"
    RSI = "RSI"
    MACD = "MACD"
    BOLLINGER_BANDS = "BOLLINGER_BANDS"
    ATR = "ATR"
    CUSTOM = "CUSTOM"


class SignalDirection(str, Enum):
    """Categorical directional or boundary signal states."""

    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    OVERBOUGHT = "OVERBOUGHT"
    OVERSOLD = "OVERSOLD"


@dataclass(frozen=True)
class IndicatorValue:
    """Immutable point-in-time calculation outcome for a single technical indicator.

    Attributes:
        name: Unique indicator identifier (e.g. 'SMA_20', 'RSI_14', 'MACD_12_26_9').
        indicator_type: Category of the indicator.
        symbol: Ticker symbol the calculation applies to.
        timeframe: Bar timeframe (e.g. '1m', '5m', '1D').
        timestamp: POSIX UTC timestamp of the reference bar.
        value: Primary scalar value (e.g., 2500.50 for SMA, 62.30 for RSI).
        values: Dictionary for compound outputs (e.g., MACD line, signal, histogram).
        is_valid: False if calculation could not complete (insufficient data, division by zero).
        error: Diagnostic error message if calculation failed or was skipped.
        metadata: Additional context (e.g. smoothing parameter, period).
    """

    name: str
    indicator_type: IndicatorType
    symbol: str
    timeframe: str
    timestamp: float
    value: Optional[float] = None
    values: Dict[str, float] = field(default_factory=dict)
    is_valid: bool = True
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate fields upon initialization."""
        if not self.name or not isinstance(self.name, str) or not self.name.strip():
            raise ValidationError("IndicatorValue name must be a non-empty string.")
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("IndicatorValue symbol must be a non-empty string.")
        if not self.timeframe or not isinstance(self.timeframe, str) or not self.timeframe.strip():
            raise ValidationError("IndicatorValue timeframe must be a non-empty string.")
        if self.timestamp <= 0:
            raise ValidationError(f"IndicatorValue timestamp must be positive, got {self.timestamp}")

        # Normalization
        object.__setattr__(self, "name", self.name.strip())
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "timeframe", self.timeframe.strip())
        object.__setattr__(self, "values", copy.deepcopy(dict(self.values or {})))
        object.__setattr__(self, "metadata", copy.deepcopy(dict(self.metadata or {})))

    @property
    def iso_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a JSON-serializable dictionary."""
        return {
            "name": self.name,
            "indicator_type": self.indicator_type.value,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "value": self.value,
            "values": copy.deepcopy(self.values),
            "is_valid": self.is_valid,
            "error": self.error,
            "metadata": copy.deepcopy(self.metadata),
        }


@dataclass(frozen=True)
class TechnicalFeatureVector:
    """Immutable aggregate technical feature vector evaluated over a symbol's bar history.

    Attributes:
        symbol: Ticker symbol.
        timeframe: Bar timeframe.
        timestamp: POSIX UTC timestamp of the latest evaluated bar.
        bar_count: Total number of valid bars in the rolling window used.
        last_close: Most recent closing price.
        indicators: Mapping of indicator name -> IndicatorValue.
        signals: Mapping of signal name -> categorical signal string.
        metadata: Auxiliary calculation metadata.
    """

    symbol: str
    timeframe: str
    timestamp: float
    bar_count: int
    last_close: float
    indicators: Dict[str, IndicatorValue] = field(default_factory=dict)
    signals: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate invariants upon initialization."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("TechnicalFeatureVector symbol must be a non-empty string.")
        if not self.timeframe or not isinstance(self.timeframe, str) or not self.timeframe.strip():
            raise ValidationError("TechnicalFeatureVector timeframe must be a non-empty string.")
        if self.timestamp <= 0:
            raise ValidationError(f"TechnicalFeatureVector timestamp must be positive, got {self.timestamp}")
        if self.bar_count < 0:
            raise ValidationError(f"TechnicalFeatureVector bar_count cannot be negative, got {self.bar_count}")
        if self.last_close < 0:
            raise ValidationError(f"TechnicalFeatureVector last_close cannot be negative, got {self.last_close}")

        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "timeframe", self.timeframe.strip())
        object.__setattr__(self, "indicators", copy.deepcopy(dict(self.indicators or {})))
        object.__setattr__(self, "signals", copy.deepcopy(dict(self.signals or {})))
        object.__setattr__(self, "metadata", copy.deepcopy(dict(self.metadata or {})))

    @property
    def iso_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def get_indicator(self, name: str) -> Optional[IndicatorValue]:
        """Convenience accessor for a named indicator."""
        return self.indicators.get(name)

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a JSON-serializable dictionary."""
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "bar_count": self.bar_count,
            "last_close": self.last_close,
            "indicators": {k: v.to_dict() for k, v in self.indicators.items()},
            "signals": copy.deepcopy(self.signals),
            "metadata": copy.deepcopy(self.metadata),
        }


@dataclass(frozen=True)
class AnalysisConfig:
    """Immutable configuration for technical indicator computation and series capacity.

    Attributes:
        sma_periods: Tuple of SMA periods to calculate (default: 20, 50, 200).
        ema_periods: Tuple of EMA periods to calculate (default: 9, 21, 55).
        rsi_period: Period for RSI calculation (default: 14).
        macd_fast: Fast EMA period for MACD (default: 12).
        macd_slow: Slow EMA period for MACD (default: 26).
        macd_signal: Signal EMA period for MACD (default: 9).
        bollinger_period: Moving average period for Bollinger Bands (default: 20).
        bollinger_std_dev: Standard deviation multiplier for Bollinger Bands (default: 2.0).
        atr_period: Period for Average True Range calculation (default: 14).
        max_bar_history: Maximum number of bars retained in memory per stream (default: 500).
    """

    sma_periods: Tuple[int, ...] = (20, 50, 200)
    ema_periods: Tuple[int, ...] = (9, 21, 55)
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bollinger_period: int = 20
    bollinger_std_dev: float = 2.0
    atr_period: int = 14
    max_bar_history: int = 500

    def __post_init__(self) -> None:
        """Validate parameter boundaries."""
        if any(p <= 0 for p in self.sma_periods):
            raise ValidationError("All SMA periods must be strictly positive integers.")
        if any(p <= 0 for p in self.ema_periods):
            raise ValidationError("All EMA periods must be strictly positive integers.")
        if self.rsi_period <= 0:
            raise ValidationError("RSI period must be a strictly positive integer.")
        if self.macd_fast <= 0 or self.macd_slow <= 0 or self.macd_signal <= 0:
            raise ValidationError("All MACD periods must be strictly positive integers.")
        if self.macd_fast >= self.macd_slow:
            raise ValidationError(
                f"MACD fast period ({self.macd_fast}) must be strictly less than slow period ({self.macd_slow})."
            )
        if self.bollinger_period <= 0:
            raise ValidationError("Bollinger period must be a strictly positive integer.")
        if self.bollinger_std_dev <= 0:
            raise ValidationError("Bollinger standard deviation multiplier must be strictly positive.")
        if self.atr_period <= 0:
            raise ValidationError("ATR period must be a strictly positive integer.")
        if self.max_bar_history < 50:
            raise ValidationError(f"max_bar_history must be at least 50 bars, got {self.max_bar_history}")
