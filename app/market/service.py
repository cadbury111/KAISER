"""KAISER — Market Data Ingestion Service.

Coordinates feed ingestion, schema normalization, domain validation,
EventBus distribution, and StateStore caching for market quotes, bars,
and depth snapshots.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, Optional, Union

from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.feeds.base import MarketFeed, MarketRecord
from app.market.models import Bar, OrderBookSnapshot, Quote
from app.market.normalizer import MarketDataNormalizer
from app.market.quality import MarketDataQualityEngine, MarketQualityConfig
from app.market.validators import DataFreshnessStatus, check_freshness

logger = get_logger("market.service")


class MarketDataService:
    """Central service managing ingestion, quality reliability, and distribution of market data.

    Enforces data quality rules, checks freshness thresholds, caches latest
    quotes in the StateStore, and dispatches immutable events across the EventBus.
    """

    def __init__(
        self,
        event_bus: EventBus,
        state_store: StateStore,
        freshness_threshold_seconds: float = 60.0,
        quality_engine: Optional[MarketDataQualityEngine] = None,
        name: str = "market_service",
    ) -> None:
        self.name = name
        self.event_bus = event_bus
        self.state_store = state_store
        self.freshness_threshold = freshness_threshold_seconds
        self.normalizer = MarketDataNormalizer()
        self.quality_engine = quality_engine or MarketDataQualityEngine(
            config=MarketQualityConfig(freshness_threshold_seconds=freshness_threshold_seconds),
            event_bus=event_bus,
            state_store=state_store,
        )
        self._feeds: Dict[str, MarketFeed] = {}

    def register_feed(self, feed: MarketFeed) -> None:
        """Register a market data feed provider."""
        self._feeds[feed.name] = feed
        logger.info("Registered market feed '%s' [source=%s]", feed.name, feed.source)

    def get_feed(self, name: str) -> Optional[MarketFeed]:
        """Retrieve a registered feed by name."""
        return self._feeds.get(name)

    def ingest_quote(
        self,
        raw_or_quote: Union[Dict[str, Any], Quote],
        correlation_id: Optional[str] = None,
        reference_time: Optional[float] = None,
    ) -> Quote:
        """Ingest, validate, normalize, store, and publish a quote record.

        Args:
            raw_or_quote: Raw dictionary or normalized Quote model.
            correlation_id: Tracing correlation ID.
            reference_time: Optional clock reference (useful for replay testing).

        Returns:
            Quote: The validated and ingested Quote model.

        Raises:
            ValidationError: If record is malformed or violates domain invariants.
        """
        cid = correlation_id or uuid.uuid4().hex

        try:
            if isinstance(raw_or_quote, Quote):
                quote = raw_or_quote
            elif isinstance(raw_or_quote, dict):
                quote = self.normalizer.normalize_quote(raw_or_quote)
            else:
                raise ValidationError(f"Expected dict or Quote, got {type(raw_or_quote).__name__}")
        except ValidationError as err:
            logger.error("Market quote validation failure [cid=%s]: %s", cid, err)
            # Record error in event bus and state store
            self.state_store.update("market_errors_count", lambda c: (c or 0) + 1, default=0)
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.MARKET_DATA_ERROR,
                    source="market_service",
                    correlation_id=cid,
                    payload={"error": str(err), "record_type": "quote"},
                )
            )
            raise

        # 1. Run Data Quality Assessment
        assessment = self.quality_engine.evaluate_quote(
            quote,
            correlation_id=cid,
            reference_time=reference_time,
        )

        # Check data freshness
        freshness = assessment.freshness_status

        # 2. Update StateStore (Latest Operational State only, not unbounded history)
        self.state_store.update(
            "latest_quotes",
            {quote.symbol: quote.to_dict()},
            default={},
        )
        self.state_store.update(
            "market_data_freshness",
            {quote.symbol: freshness.value},
            default={},
        )
        self.state_store.set("market_status", "active")

        # 2. Publish Standard MARKET_QUOTE_UPDATED Event
        self.event_bus.publish(
            Event.create(
                event_type=EventType.MARKET_QUOTE_UPDATED,
                source=quote.source,
                correlation_id=cid,
                payload=quote.to_dict(),
                metadata={"freshness": freshness.value},
            )
        )

        # 3. If data is stale, also emit MARKET_DATA_STALE notification event
        if freshness == DataFreshnessStatus.STALE:
            now_clock = time.time() if reference_time is None else reference_time
            age = max(0.0, round(now_clock - quote.timestamp, 2))
            logger.warning(
                "Stale quote received for %s [age=%.1fs, threshold=%.1fs]",
                quote.symbol,
                age,
                self.freshness_threshold,
            )
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.MARKET_DATA_STALE,
                    source=quote.source,
                    correlation_id=cid,
                    payload={"symbol": quote.symbol, "age_seconds": age, "timestamp": quote.timestamp},
                )
            )

        return quote

    def ingest_bar(
        self,
        raw_or_bar: Union[Dict[str, Any], Bar],
        correlation_id: Optional[str] = None,
    ) -> Bar:
        """Ingest, validate, store, and publish a candlestick Bar record."""
        cid = correlation_id or uuid.uuid4().hex

        try:
            if isinstance(raw_or_bar, Bar):
                bar = raw_or_bar
            elif isinstance(raw_or_bar, dict):
                bar = self.normalizer.normalize_bar(raw_or_bar)
            else:
                raise ValidationError(f"Expected dict or Bar, got {type(raw_or_bar).__name__}")
        except ValidationError as err:
            logger.error("Market bar validation failure [cid=%s]: %s", cid, err)
            self.state_store.update("market_errors_count", lambda c: (c or 0) + 1, default=0)
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.MARKET_DATA_ERROR,
                    source="market_service",
                    correlation_id=cid,
                    payload={"error": str(err), "record_type": "bar"},
                )
            )
            raise

        # 1. Run Data Quality Assessment
        assessment = self.quality_engine.evaluate_bar(
            bar,
            correlation_id=cid,
        )

        # Update StateStore latest bars cache
        cache_key = f"{bar.symbol}_{bar.timeframe}"
        self.state_store.update(
            "latest_bars",
            {cache_key: bar.to_dict()},
            default={},
        )

        # Publish MARKET_BAR_UPDATED Event
        self.event_bus.publish(
            Event.create(
                event_type=EventType.MARKET_BAR_UPDATED,
                source=bar.source,
                correlation_id=cid,
                payload=bar.to_dict(),
            )
        )
        return bar

    def ingest_order_book(
        self,
        raw_or_ob: Union[Dict[str, Any], OrderBookSnapshot],
        correlation_id: Optional[str] = None,
    ) -> OrderBookSnapshot:
        """Ingest, validate, store, and publish a market depth snapshot."""
        cid = correlation_id or uuid.uuid4().hex

        try:
            if isinstance(raw_or_ob, OrderBookSnapshot):
                ob = raw_or_ob
            elif isinstance(raw_or_ob, dict):
                ob = self.normalizer.normalize_order_book(raw_or_ob)
            else:
                raise ValidationError(f"Expected dict or OrderBookSnapshot, got {type(raw_or_ob).__name__}")
        except ValidationError as err:
            logger.error("Market order book validation failure [cid=%s]: %s", cid, err)
            self.state_store.update("market_errors_count", lambda c: (c or 0) + 1, default=0)
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.MARKET_DATA_ERROR,
                    source="market_service",
                    correlation_id=cid,
                    payload={"error": str(err), "record_type": "order_book"},
                )
            )
            raise

        # 1. Run Data Quality Assessment
        assessment = self.quality_engine.evaluate_order_book(
            ob,
            correlation_id=cid,
        )

        # Update StateStore latest order book
        self.state_store.update(
            "latest_order_books",
            {ob.symbol: ob.to_dict()},
            default={},
        )

        # Publish MARKET_DEPTH_UPDATED Event
        self.event_bus.publish(
            Event.create(
                event_type=EventType.MARKET_DEPTH_UPDATED,
                source=ob.source,
                correlation_id=cid,
                payload=ob.to_dict(),
            )
        )
        return ob

    def poll_feed(
        self,
        feed_name: Optional[str] = None,
        correlation_id: Optional[str] = None,
        reference_time: Optional[float] = None,
    ) -> Optional[MarketRecord]:
        """Poll a record from a registered feed and ingest it through the pipeline.

        Args:
            feed_name: Specific feed name, or first registered feed if None.
            correlation_id: Tracing ID.
            reference_time: Clock reference for replay feeds.

        Returns:
            Optional[MarketRecord]: Ingested Quote, Bar, or OrderBookSnapshot, or None.
        """
        if not self._feeds:
            return None

        target_feed = self._feeds.get(feed_name) if feed_name else next(iter(self._feeds.values()))
        if not target_feed or not target_feed.is_running:
            return None

        record = target_feed.next_record()
        if record is None:
            return None

        if isinstance(record, Quote):
            return self.ingest_quote(record, correlation_id=correlation_id, reference_time=reference_time)
        elif isinstance(record, Bar):
            return self.ingest_bar(record, correlation_id=correlation_id)
        elif isinstance(record, OrderBookSnapshot):
            return self.ingest_order_book(record, correlation_id=correlation_id)

        return None
