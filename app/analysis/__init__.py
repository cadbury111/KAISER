"""KAISER Market Analysis Package.

Hosts deterministic technical and quantitative analysis engines:
- Moving Averages (SMA, EMA)
- Relative Strength Index (RSI)
- Moving Average Convergence Divergence (MACD)
- Bollinger Bands (Middle, Upper, Lower, Bandwidth, %B)
- Average True Range (ATR)
- Data Quality Gate & Rolling Bar Series
- Technical Analysis Engine & Orchestrator Handlers
"""

from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.handlers import register_analysis_handlers
from app.analysis.indicators import (
    calculate_atr,
    calculate_bollinger_bands,
    calculate_ema,
    calculate_ema_series,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
    calculate_true_range,
    classify_bollinger,
    classify_macd,
    classify_rsi,
)
from app.analysis.models import (
    SUPPORTED_TIMEFRAMES,
    TIMEFRAME_SECONDS,
    AnalysisConfig,
    IndicatorType,
    IndicatorValue,
    MultiTimeframeSyncState,
    SignalDirection,
    TechnicalFeatureVector,
    TimeframeHealth,
    TimeframeHealthStatus,
    normalize_timeframe,
)
from app.analysis.series import RollingBarSeries

__all__ = [
    # Domain Models & Timeframe Constants
    "IndicatorType",
    "SignalDirection",
    "IndicatorValue",
    "TechnicalFeatureVector",
    "AnalysisConfig",
    "SUPPORTED_TIMEFRAMES",
    "TIMEFRAME_SECONDS",
    "normalize_timeframe",
    "TimeframeHealth",
    "TimeframeHealthStatus",
    "MultiTimeframeSyncState",
    # Rolling Series Container
    "RollingBarSeries",
    # Pure Indicator Calculation Functions
    "calculate_sma",
    "calculate_ema",
    "calculate_ema_series",
    "calculate_rsi",
    "calculate_macd",
    "calculate_bollinger_bands",
    "calculate_true_range",
    "calculate_atr",
    "classify_rsi",
    "classify_macd",
    "classify_bollinger",
    # Technical Engine & Handlers
    "TechnicalAnalysisEngine",
    "register_analysis_handlers",
]
