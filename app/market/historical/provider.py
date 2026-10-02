"""KAISER — Historical Market Data Provider Interface.

Defines the abstract contract for external or synthetic historical market data
providers. Separates provider data access from indicator calculations and
agent interpretation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Tuple

from app.analysis.models import SUPPORTED_TIMEFRAMES, normalize_timeframe
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalProviderStatus,
)


class HistoricalDataProvider(ABC):
    """Abstract Base Class for historical market data providers.

    All historical data providers must adhere to the following conventions:
    1. Candle Timestamp Convention:
       Timestamps represent the POSIX UTC float of the candle OPEN time.
    2. OHLCV Invariants:
       Prices must be non-negative and satisfy:
       - high >= open, close, low
       - low <= open, close, high
       - volume >= 0.0
    3. Zero-Volume vs Missing Data:
       A period where trading occurred at 0 volume (e.g., illiquid intervals or auction pauses)
       must be returned as a valid Bar with volume=0.0. Periods with NO data must NOT be
       synthetically filled or fabricated by the provider.
    4. Isolation from Live Prices:
       Historical data queries must never trigger trade execution or overwrite real-time quotes.
    """

    def __init__(
        self,
        name: str,
        supported_timeframes: Optional[Tuple[str, ...]] = None,
        supported_symbols: Optional[Tuple[str, ...]] = None,
    ) -> None:
        self.name = name
        self.supported_timeframes = supported_timeframes or tuple(SUPPORTED_TIMEFRAMES)
        self.supported_symbols = (
            tuple(s.strip().upper() for s in supported_symbols) if supported_symbols is not None else None
        )

    def supports_timeframe(self, timeframe: str) -> bool:
        """Check if the specified timeframe is supported by this provider."""
        try:
            canonical = normalize_timeframe(timeframe)
            return canonical in self.supported_timeframes
        except Exception:
            return False

    def supports_symbol(self, symbol: str) -> bool:
        """Check if the specified symbol is supported by this provider."""
        if not symbol or not isinstance(symbol, str):
            return False
        if self.supported_symbols is None:
            return True
        return symbol.strip().upper() in self.supported_symbols

    @abstractmethod
    def get_historical_bars(self, request: HistoricalBarRequest) -> HistoricalBarResponse:
        """Fetch historical candlestick bars matching the request criteria.

        Args:
            request: Validated HistoricalBarRequest specifying symbol, timeframe,
                     bounded time range, and limit.

        Returns:
            HistoricalBarResponse: Structured response with provider status,
            typed Bar records, and optional pagination cursor.
        """
        pass
