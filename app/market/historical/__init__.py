"""KAISER — Historical Market Data Integration Package.

Provides provider-independent loading, normalization, quality evaluation, local persistence,
bounded caching, pipeline ingestion, and deterministic replay for historical OHLCV candlestick records.
"""

from __future__ import annotations

from app.market.historical.cache import HistoricalDataCache, make_cache_key
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
from app.market.historical.replay import (
    HistoricalReplayEngine,
    HistoricalReplayRequest,
    HistoricalReplayResult,
    HistoricalReplayStatus,
)
from app.market.historical.storage import (
    HistoricalBarRepository,
    SQLiteHistoricalBarRepository,
)
from app.market.historical.twelve_data import TwelveDataHistoricalProvider

__all__ = [
    "HistoricalBarRequest",
    "HistoricalBarResponse",
    "HistoricalIngestionResult",
    "HistoricalProviderStatus",
    "HistoricalDataProvider",
    "MockHistoricalDataProvider",
    "TwelveDataHistoricalProvider",
    "HistoricalDataLoader",
    "register_historical_handlers",
    "get_base_price_for_symbol",
    "HistoricalBarRepository",
    "SQLiteHistoricalBarRepository",
    "HistoricalDataCache",
    "make_cache_key",
    "HistoricalReplayRequest",
    "HistoricalReplayResult",
    "HistoricalReplayStatus",
    "HistoricalReplayEngine",
]
