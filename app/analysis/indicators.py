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
    for p in window:
        if not math.isfinite(p):
            return None
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
    for p in prices:
        if not math.isfinite(p):
            return None

    alpha = smoothing / (float(period) + 1.0)
    current_ema = sum(prices[:period]) / float(period)

    for price in prices[period:]:
        current_ema = (price - current_ema) * alpha + current_ema

    return round(current_ema, 4)


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
    for p in prices:
        if not math.isfinite(p):
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
    for p in prices:
        if not math.isfinite(p):
            return None

    # Initial average gain and loss over the first `period` changes
    sum_gain = 0.0
    sum_loss = 0.0
    for i in range(1, period + 1):
        diff = prices[i] - prices[i - 1]
        if diff > 0.0:
            sum_gain += diff
        elif diff < 0.0:
            sum_loss -= diff

    avg_gain = sum_gain / float(period)
    avg_loss = sum_loss / float(period)

    # Wilder's smoothing for subsequent changes
    p_float = float(period)
    p_prev = float(period - 1)
    for i in range(period + 1, len(prices)):
        diff = prices[i] - prices[i - 1]
        if diff > 0.0:
            avg_gain = (avg_gain * p_prev + diff) / p_float
            avg_loss = (avg_loss * p_prev) / p_float
        elif diff < 0.0:
            avg_gain = (avg_gain * p_prev) / p_float
            avg_loss = (avg_loss * p_prev - diff) / p_float
        else:
            avg_gain = (avg_gain * p_prev) / p_float
            avg_loss = (avg_loss * p_prev) / p_float

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
    for p in prices:
        if not math.isfinite(p):
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
    latest_signal = calculate_ema(macd_line_series, period=signal_period)
    if latest_signal is None:
        return None

    latest_macd = macd_line_series[-1]
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
    for p in window:
        if not math.isfinite(p):
            return None

    middle = sum(window) / float(period)

    # Population variance
    variance = sum((p - middle) ** 2 for p in window) / float(period)
    std_dev = math.sqrt(max(0.0, variance))

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
    if not (math.isfinite(high) and math.isfinite(low) and (prev_close is None or math.isfinite(prev_close))):
        return 0.0

    hl = abs(high - low)
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

    # Guard against non-finite prices in bars
    for b in bars:
        if not (math.isfinite(b.high) and math.isfinite(b.low) and math.isfinite(b.close)):
            return None

    # Calculate True Range sequence using single pass without list allocation
    p_float = float(period)
    p_prev = float(period - 1)

    initial_tr_sum = 0.0
    for i in range(1, period + 1):
        b = bars[i]
        pc = bars[i - 1].close
        hl = b.high - b.low
        h_pc = abs(b.high - pc)
        l_pc = abs(b.low - pc)
        tr = round(max(hl, h_pc, l_pc), 4)
        initial_tr_sum += tr

    atr = initial_tr_sum / p_float

    for i in range(period + 1, len(bars)):
        b = bars[i]
        pc = bars[i - 1].close
        hl = b.high - b.low
        h_pc = abs(b.high - pc)
        l_pc = abs(b.low - pc)
        tr = round(max(hl, h_pc, l_pc), 4)
        atr = (atr * p_prev + tr) / p_float

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


def classify_order_book_imbalance(
    imbalance: Optional[float],
    threshold: float = 0.05,
) -> SignalDirection:
    """Classify Order Book Imbalance into BULLISH (buy pressure), BEARISH (sell pressure), or NEUTRAL.

    Args:
        imbalance: OBI value in [-1.0, 1.0] or None.
        threshold: Sensitivity threshold (default 0.05).

    Returns:
        SignalDirection: BULLISH if imbalance > threshold, BEARISH if < -threshold, else NEUTRAL.
    """
    if imbalance is None or not math.isfinite(imbalance):
        return SignalDirection.NEUTRAL
    if imbalance > threshold:
        return SignalDirection.BULLISH
    if imbalance < -threshold:
        return SignalDirection.BEARISH
    return SignalDirection.NEUTRAL


def extract_depth_features(analytics: Optional[Any]) -> Dict[str, Any]:
    """Extract standard read-only microstructure depth features for downstream specialist agents.

    Guarantees deterministic, clean serializable dictionary. If analytics is None or invalid,
    defaults safely to neutral / zero values without raising exceptions.

    Args:
        analytics: MarketDepthAnalytics instance or None.

    Returns:
        Dict[str, Any]: Dictionary of numerical and categorical depth features.
    """
    if analytics is None or not getattr(analytics, "is_valid", False):
        return {
            "depth_available": False,
            "order_book_imbalance": 0.0,
            "order_book_pressure": "NEUTRAL",
            "micro_price": None,
            "depth_slope_ratio": 0.0,
            "relative_spread_bps": None,
            "volume_weighted_spread": None,
            "cumulative_bid_quantity": 0.0,
            "cumulative_ask_quantity": 0.0,
        }

    imb = getattr(analytics, "imbalance", None)
    mp = getattr(analytics, "micro_price", None)
    ds = getattr(analytics, "depth_slope", None)
    ws = getattr(analytics, "weighted_spread", None)

    return {
        "depth_available": True,
        "order_book_imbalance": imb.value if imb else 0.0,
        "order_book_pressure": imb.interpretation.value if imb else "NEUTRAL",
        "micro_price": mp.price if mp else None,
        "depth_slope_ratio": ds.ratio if ds else 0.0,
        "relative_spread_bps": ws.relative_spread_bps if ws else None,
        "volume_weighted_spread": ws.volume_weighted_spread if ws else None,
        "cumulative_bid_quantity": getattr(analytics, "cumulative_bid_quantity", 0.0),
        "cumulative_ask_quantity": getattr(analytics, "cumulative_ask_quantity", 0.0),
    }


def classify_trade_flow_imbalance(
    tfi: Optional[float],
    threshold: float = 0.05,
) -> SignalDirection:
    """Classify Trade Flow Imbalance into BULLISH (buyer aggression), BEARISH (seller aggression), or NEUTRAL.

    Args:
        tfi: TFI value in [-1.0, 1.0] or None.
        threshold: Sensitivity threshold (default 0.05).

    Returns:
        SignalDirection: BULLISH if tfi > threshold, BEARISH if < -threshold, else NEUTRAL.
    """
    if tfi is None or not math.isfinite(tfi):
        return SignalDirection.NEUTRAL
    if tfi > threshold:
        return SignalDirection.BULLISH
    if tfi < -threshold:
        return SignalDirection.BEARISH
    return SignalDirection.NEUTRAL


def extract_trade_flow_features(analytics: Optional[Any]) -> Dict[str, Any]:
    """Extract standard read-only trade flow features for downstream specialist agents.

    Guarantees deterministic, clean serializable dictionary. If analytics is None or invalid,
    defaults safely to neutral / zero values without raising exceptions.

    Args:
        analytics: TradeFlowAnalytics instance or None.

    Returns:
        Dict[str, Any]: Dictionary of numerical and categorical trade flow features.
    """
    if analytics is None or not getattr(analytics, "is_valid", False):
        return {
            "trade_flow_available": False,
            "trade_flow_imbalance": 0.0,
            "tfi_ewma": 0.0,
            "rolling_tfi": 0.0,
            "total_traded_volume": 0.0,
            "buy_aggressor_volume": 0.0,
            "sell_aggressor_volume": 0.0,
            "unknown_volume": 0.0,
            "trade_count": 0,
            "trade_flow_pressure": "NEUTRAL",
            "combined_pressure": None,
            "vwap": None,
        }

    interp = getattr(analytics, "interpretation", None)
    comb = getattr(analytics, "combined_diagnostic", None)

    return {
        "trade_flow_available": True,
        "trade_flow_imbalance": getattr(analytics, "trade_flow_imbalance", 0.0),
        "tfi_ewma": getattr(analytics, "tfi_ewma", 0.0),
        "rolling_tfi": getattr(analytics, "rolling_tfi", 0.0),
        "total_traded_volume": getattr(analytics, "total_traded_volume", 0.0),
        "buy_aggressor_volume": getattr(analytics, "buy_aggressor_volume", 0.0),
        "sell_aggressor_volume": getattr(analytics, "sell_aggressor_volume", 0.0),
        "unknown_volume": getattr(analytics, "unknown_volume", 0.0),
        "trade_count": getattr(analytics, "trade_count", 0),
        "trade_flow_pressure": interp.value if hasattr(interp, "value") else str(interp or "NEUTRAL"),
        "combined_pressure": comb.value if hasattr(comb, "value") else (str(comb) if comb else None),
        "vwap": getattr(analytics, "vwap", None),
    }


def extract_synchronized_context_features(context: Optional[Any]) -> Dict[str, Any]:
    """Extract standard read-only synchronized multi-stream context features.

    Guarantees deterministic, clean serializable dictionary. If context is None or invalid,
    defaults safely to un-synchronized / default values without raising exceptions.
    Ensures no recommendation prohibited keys are used.

    Args:
        context: SynchronizedMarketContext instance or None.

    Returns:
        Dict[str, Any]: Dictionary of numerical and categorical synchronization features.
    """
    if context is None or not getattr(context, "is_valid", False):
        return {
            "context_available": False,
            "sync_quality": "INVALID" if (context and not getattr(context, "is_valid", False)) else "NO_CONTEXT",
            "is_synchronized": False,
            "max_skew_ms": 0.0,
            "missing_streams": ["QUOTE", "DEPTH", "TRADE"],
            "has_quote": False,
            "has_depth": False,
            "has_trade": False,
        }

    sq = getattr(context, "sync_quality", None)
    sq_val = sq.value if hasattr(sq, "value") else str(sq or "PARTIAL")

    return {
        "context_available": True,
        "sync_quality": sq_val,
        "is_synchronized": bool(getattr(context, "is_synchronized", False)),
        "max_skew_ms": float(getattr(context, "max_skew_ms", 0.0)),
        "missing_streams": list(getattr(context, "missing_streams", ())),
        "has_quote": getattr(context, "quote", None) is not None,
        "has_depth": getattr(context, "depth_snapshot", None) is not None,
        "has_trade": getattr(context, "last_trade", None) is not None,
    }
