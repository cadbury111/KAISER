"""KAISER — Deterministic Technical Analysis Calculations.

Pure, numerical implementations of classical technical indicators:
- Simple Moving Average (SMA)
- Exponential Moving Average (EMA)
- Relative Strength Index (RSI - Wilder's Smoothing)
- Moving Average Convergence Divergence (MACD)
- Bollinger Bands (Middle, Upper, Lower, Bandwidth, %B)
- Average True Range (ATR - Wilder's Smoothing)

Design Guarantees:
- Zero external runtime dependencies (pure Python math).
- Deterministic, side-effect-free calculations.
- Robust boundary condition handling (zero division, empty series, flat lines).
- No trade execution logic.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence

from app.analysis.models import SignalDirection
from app.market.models import Bar


def calculate_sma(prices: Sequence[float], period: int) -> Optional[float]:
    """Calculate Simple Moving Average over the latest `period` prices.

    Args:
        prices: Ordered sequence of historical prices.
        period: Number of periods in the rolling window.

    Returns:
        Optional[float]: Rounded SMA value, or None if insufficient data.
    """
    if period <= 0 or len(prices) < period:
        return None

    window = prices[-period:]
    return round(sum(window) / float(period), 4)


def calculate_ema(prices: Sequence[float], period: int, smoothing: float = 2.0) -> Optional[float]:
    """Calculate Exponential Moving Average using standard smoothing multiplier.

    Args:
        prices: Ordered sequence of historical prices.
        period: Smoothing period.
        smoothing: Multiplier weight factor (standard is 2.0).

    Returns:
        Optional[float]: Latest EMA value, or None if insufficient data.
    """
    if period <= 0 or len(prices) < period:
        return None

    series = calculate_ema_series(prices, period=period, smoothing=smoothing)
    if not series:
        return None
    return series[-1]


def calculate_ema_series(
    prices: Sequence[float],
    period: int,
    smoothing: float = 2.0,
) -> List[float]:
    """Calculate the full sequence of EMA values aligned with the price series.

    The first `period - 1` entries are None or skipped, starting with the SMA seed
    at index `period - 1`.

    Args:
        prices: Sequence of historical prices.
        period: Window length.
        smoothing: Smoothing factor (default 2.0).

    Returns:
        List[float]: Sequence of calculated EMA values starting at index period - 1.
    """
    if period <= 0 or len(prices) < period:
        return []

    alpha = smoothing / (float(period) + 1.0)
    # Seed with SMA of the first `period` prices
    seed_sma = sum(prices[:period]) / float(period)

    ema_values: List[float] = [round(seed_sma, 4)]
    current_ema = seed_sma

    for price in prices[period:]:
        current_ema = (price - current_ema) * alpha + current_ema
        ema_values.append(round(current_ema, 4))

    return ema_values


def calculate_rsi(prices: Sequence[float], period: int = 14) -> Optional[float]:
    """Calculate Relative Strength Index (RSI) using Wilder's exponential smoothing.

    Args:
        prices: Ordered sequence of closing prices.
        period: RSI period (standard 14).

    Returns:
        Optional[float]: Bounded RSI value between [0.0, 100.0], or None if insufficient data.
    """
    if period <= 0 or len(prices) < period + 1:
        return None

    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

    # Initial average gain and loss over the first `period` deltas
    gains = [max(0.0, d) for d in deltas[:period]]
    losses = [max(0.0, -d) for d in deltas[:period]]

    avg_gain = sum(gains) / float(period)
    avg_loss = sum(losses) / float(period)

    # Wilder's smoothing for subsequent deltas
    for d in deltas[period:]:
        current_gain = max(0.0, d)
        current_loss = max(0.0, -d)
        avg_gain = (avg_gain * (period - 1) + current_gain) / float(period)
        avg_loss = (avg_loss * (period - 1) + current_loss) / float(period)

    # Boundary handling for zero loss or zero gain
    if avg_loss == 0.0:
        if avg_gain == 0.0:
            return 50.0  # Flat market: no upward or downward movement
        return 100.0  # Only positive movement
    if avg_gain == 0.0:
        return 0.0  # Only negative movement

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(min(100.0, max(0.0, rsi)), 4)


def calculate_macd(
    prices: Sequence[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> Optional[Dict[str, float]]:
    """Calculate MACD Line, Signal Line, and Histogram.

    Args:
        prices: Sequence of closing prices.
        fast_period: Fast EMA window (default 12).
        slow_period: Slow EMA window (default 26).
        signal_period: Signal line EMA window (default 9).

    Returns:
        Optional[Dict[str, float]]: Dictionary with 'macd', 'signal', 'histogram',
        or None if insufficient data.
    """
    if fast_period <= 0 or slow_period <= 0 or signal_period <= 0:
        return None
    if fast_period >= slow_period:
        return None

    # Required points: slow_period for MACD line, plus signal_period - 1 for signal line EMA
    min_required = slow_period + signal_period - 1
    if len(prices) < min_required:
        return None

    # Calculate full EMA series for slow and fast
    slow_series = calculate_ema_series(prices, period=slow_period)
    fast_series = calculate_ema_series(prices, period=fast_period)

    # Align series: fast_series has length len(prices) - fast_period + 1
    # slow_series has length len(prices) - slow_period + 1
    # We need the tail of fast_series that corresponds to slow_series
    offset = slow_period - fast_period
    aligned_fast = fast_series[offset:]

    if len(aligned_fast) != len(slow_series):
        return None

    # MACD line = Fast EMA - Slow EMA
    macd_line_series = [
        round(f - s, 4) for f, s in zip(aligned_fast, slow_series)
    ]

    if len(macd_line_series) < signal_period:
        return None

    # Signal line = EMA of MACD line series
    signal_series = calculate_ema_series(macd_line_series, period=signal_period)
    if not signal_series:
        return None

    latest_macd = macd_line_series[-1]
    latest_signal = signal_series[-1]
    latest_hist = round(latest_macd - latest_signal, 4)

    return {
        "macd": latest_macd,
        "signal": latest_signal,
        "histogram": latest_hist,
    }


def calculate_bollinger_bands(
    prices: Sequence[float],
    period: int = 20,
    num_std: float = 2.0,
) -> Optional[Dict[str, float]]:
    """Calculate Bollinger Bands (Middle, Upper, Lower, Bandwidth, %B).

    Args:
        prices: Sequence of closing prices.
        period: Window length for moving average and standard deviation (default 20).
        num_std: Number of standard deviations for bands (default 2.0).

    Returns:
        Optional[Dict[str, float]]: Dict with 'middle', 'upper', 'lower',
        'bandwidth', 'percent_b', or None if insufficient data.
    """
    if period <= 0 or num_std <= 0 or len(prices) < period:
        return None

    window = prices[-period:]
    middle = sum(window) / float(period)

    # Population variance
    variance = sum((p - middle) ** 2 for p in window) / float(period)
    std_dev = math.sqrt(variance)

    upper = middle + (num_std * std_dev)
    lower = middle - (num_std * std_dev)

    # Bandwidth = (Upper - Lower) / Middle
    bandwidth = 0.0
    if middle != 0.0:
        bandwidth = (upper - lower) / middle

    # %B = (Last Price - Lower) / (Upper - Lower)
    last_price = window[-1]
    band_width_abs = upper - lower
    percent_b = 0.5
    if band_width_abs != 0.0:
        percent_b = (last_price - lower) / band_width_abs

    return {
        "middle": round(middle, 4),
        "upper": round(upper, 4),
        "lower": round(lower, 4),
        "bandwidth": round(bandwidth, 4),
        "percent_b": round(percent_b, 4),
    }


def calculate_true_range(high: float, low: float, prev_close: Optional[float]) -> float:
    """Calculate True Range (TR) for a single bar.

    TR = max(High - Low, abs(High - PrevClose), abs(Low - PrevClose))
    """
    hl = high - low
    if prev_close is None:
        return round(hl, 4)

    h_pc = abs(high - prev_close)
    l_pc = abs(low - prev_close)
    return round(max(hl, h_pc, l_pc), 4)


def calculate_atr(bars: Sequence[Bar], period: int = 14) -> Optional[float]:
    """Calculate Average True Range (ATR) using Wilder's smoothing.

    Args:
        bars: Sequence of Bar objects in chronological order.
        period: ATR period (default 14).

    Returns:
        Optional[float]: ATR value, or None if insufficient data.
    """
    if period <= 0 or len(bars) < period + 1:
        return None

    # Calculate True Range sequence
    tr_list: List[float] = []
    for i in range(1, len(bars)):
        tr = calculate_true_range(
            high=bars[i].high,
            low=bars[i].low,
            prev_close=bars[i - 1].close,
        )
        tr_list.append(tr)

    if len(tr_list) < period:
        return None

    # Seed ATR with SMA of first `period` True Ranges
    atr = sum(tr_list[:period]) / float(period)

    # Wilder's smoothing for remaining TRs
    for tr in tr_list[period:]:
        atr = (atr * (period - 1) + tr) / float(period)

    return round(atr, 4)


# -----------------------------------------------------------------------------
# Signal Classification Helpers
# -----------------------------------------------------------------------------

def classify_rsi(
    rsi: Optional[float],
    overbought_threshold: float = 70.0,
    oversold_threshold: float = 30.0,
) -> SignalDirection:
    """Classify RSI into OVERBOUGHT, OVERSOLD, or NEUTRAL signal."""
    if rsi is None:
        return SignalDirection.NEUTRAL
    if rsi >= overbought_threshold:
        return SignalDirection.OVERBOUGHT
    if rsi <= oversold_threshold:
        return SignalDirection.OVERSOLD
    return SignalDirection.NEUTRAL


def classify_macd(macd_data: Optional[Dict[str, float]]) -> SignalDirection:
    """Classify MACD into BULLISH (histogram > 0 and macd > signal), BEARISH, or NEUTRAL."""
    if not macd_data:
        return SignalDirection.NEUTRAL

    macd_line = macd_data.get("macd", 0.0)
    signal_line = macd_data.get("signal", 0.0)
    hist = macd_data.get("histogram", 0.0)

    if hist > 0 and macd_line > signal_line:
        return SignalDirection.BULLISH
    if hist < 0 and macd_line < signal_line:
        return SignalDirection.BEARISH
    return SignalDirection.NEUTRAL


def classify_bollinger(price: float, bands: Optional[Dict[str, float]]) -> SignalDirection:
    """Classify Bollinger Band positioning relative to price."""
    if not bands:
        return SignalDirection.NEUTRAL

    upper = bands.get("upper", float("inf"))
    lower = bands.get("lower", float("-inf"))
    middle = bands.get("middle", price)

    if price >= upper:
        return SignalDirection.OVERBOUGHT
    if price <= lower:
        return SignalDirection.OVERSOLD
    if price > middle:
        return SignalDirection.BULLISH
    if price < middle:
        return SignalDirection.BEARISH
    return SignalDirection.NEUTRAL
