"""KAISER — Historical Market Data Integration Package.

Provides provider-independent loading, normalization, quality evaluation, and
pipeline ingestion for historical OHLCV candlestick records.
"""

from __future__ import annotations

from app.market.historical.handlers import register_historical_handlers
from app.market.historical.loader import HistoricalDataLoader
from app.market.historical.mock import MockHistoricalDataProvider, get_base_price_for_symbol
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalIngestionResult,
    HistoricalProviderStatus,
)
from app.market.historical.provider import HistoricalDataProvider

__all__ = [
    "HistoricalBarRequest",
    "HistoricalBarResponse",
    "HistoricalIngestionResult",
    "HistoricalProviderStatus",
    "HistoricalDataProvider",
    "MockHistoricalDataProvider",
    "HistoricalDataLoader",
    "register_historical_handlers",
    "get_base_price_for_symbol",
]
