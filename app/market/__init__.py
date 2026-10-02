"""KAISER Market Data Package.

Manages market data ingestion, streaming, caching, provider abstraction,
local persistence, and quality reliability evaluation.
"""

from app.market.feeds import (
    MarketFeed,
    MarketRecord,
    MockMarketFeed,
    MockScenario,
    ReplayMarketFeed,
)
from app.market.historical import (
    HistoricalBarRepository,
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalDataCache,
    HistoricalDataLoader,
    HistoricalDataProvider,
    HistoricalIngestionResult,
    HistoricalProviderStatus,
    HistoricalReplayEngine,
    HistoricalReplayRequest,
    HistoricalReplayResult,
    HistoricalReplayStatus,
    MockHistoricalDataProvider,
    SQLiteHistoricalBarRepository,
    get_base_price_for_symbol,
    make_cache_key,
    register_historical_handlers,
)
from app.market.models import Bar, OrderBookLevel, OrderBookSnapshot, Quote
from app.market.normalizer import MarketDataNormalizer
from app.market.quality import (
    DataQualityAssessment,
    DataQualityStatus,
    MarketDataQualityEngine,
    MarketQualityConfig,
    SourceHealth,
    SourceHealthTracker,
    evaluate_bar_completeness,
    evaluate_order_book_completeness,
    evaluate_quote_completeness,
    is_safe_for_downstream,
    resolve_quality_status,
)
from app.market.service import MarketDataService
from app.market.validators import DataFreshnessStatus, check_freshness

__all__ = [
    "Quote",
    "Bar",
    "OrderBookLevel",
    "OrderBookSnapshot",
    "MarketDataNormalizer",
    "MarketFeed",
    "MarketRecord",
    "MockMarketFeed",
    "MockScenario",
    "ReplayMarketFeed",
    "MarketDataService",
    "DataFreshnessStatus",
    "check_freshness",
    "MarketDataQualityEngine",
    "DataQualityStatus",
    "DataQualityAssessment",
    "MarketQualityConfig",
    "SourceHealth",
    "SourceHealthTracker",
    "evaluate_quote_completeness",
    "evaluate_bar_completeness",
    "evaluate_order_book_completeness",
    "resolve_quality_status",
    "is_safe_for_downstream",
    "HistoricalBarRequest",
    "HistoricalBarResponse",
    "HistoricalIngestionResult",
    "HistoricalProviderStatus",
    "HistoricalDataProvider",
    "MockHistoricalDataProvider",
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
