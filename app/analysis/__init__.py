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
    AnalysisConfig,
    IndicatorType,
    IndicatorValue,
    SignalDirection,
    TechnicalFeatureVector,
)
from app.analysis.series import RollingBarSeries

__all__ = [
    # Domain Models
    "IndicatorType",
    "SignalDirection",
    "IndicatorValue",
    "TechnicalFeatureVector",
    "AnalysisConfig",
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
