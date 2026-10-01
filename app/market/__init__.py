"""KAISER Market Data Package.

Manages market data ingestion, streaming, caching, provider abstraction,
and quality reliability evaluation.
"""

from app.market.feeds import (
    MarketFeed,
    MarketRecord,
    MockMarketFeed,
    MockScenario,
    ReplayMarketFeed,
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
]
