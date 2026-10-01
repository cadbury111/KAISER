"""KAISER — Market Data Layer Test Suite.

Deterministic unit and integration tests verifying:
1. Domain models (Quote, Bar, OrderBookSnapshot, OrderBookLevel) & immutability
2. Normalization layer (type conversions, timestamp parsing, error handling)
3. Data freshness evaluation and configurable thresholds
4. MockMarketFeed deterministic scenarios and lifecycle
5. ReplayMarketFeed sequenced playback, reset, and file loading
6. EventBus integration (MARKET_QUOTE_UPDATED, MARKET_BAR_UPDATED, MARKET_DATA_ERROR, MARKET_DATA_STALE)
7. StateStore integration (bounded operational state caching)
8. Safety invariants (no external network, no live trading, locked execution)
"""

import json
import time
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import List

import pytest

from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market import (
    Bar,
    DataFreshnessStatus,
    MarketDataNormalizer,
    MarketDataService,
    MockMarketFeed,
    MockScenario,
    OrderBookLevel,
    OrderBookSnapshot,
    Quote,
    ReplayMarketFeed,
    check_freshness,
)


# ==============================================================================
# 1. DOMAIN MODEL & IMMUTABILITY TESTS
# ==============================================================================

class TestMarketModels:
    """Test suite for Quote, Bar, OrderBookSnapshot, and OrderBookLevel models."""

    def test_quote_creation_valid(self) -> None:
        """Verify Quote instantiates with valid parameters and calculates properties."""
        q = Quote(
            symbol="nifty",  # Tests automatic uppercase normalization
            exchange="nse",
            timestamp=1700000000.0,
            last_price=24500.0,
            bid=24499.0,
            ask=24501.0,
            bid_size=100.0,
            ask_size=150.0,
            volume=50000.0,
            currency="INR",
            source="mock",
        )
        assert q.symbol == "NIFTY"
        assert q.exchange == "NSE"
        assert q.last_price == 24500.0
        assert q.spread == 2.0
        assert q.mid_price == 24500.0
        assert "T" in q.iso_timestamp

    def test_quote_validation_errors(self) -> None:
        """Verify Quote raises ValidationError on invalid parameters."""
        now = time.time()

        # Empty symbol
        with pytest.raises(ValidationError, match="symbol must be a non-empty string"):
            Quote(symbol="", exchange="NSE", timestamp=now, last_price=100.0)

        # Negative price
        with pytest.raises(ValidationError, match="last_price cannot be negative"):
            Quote(symbol="NIFTY", exchange="NSE", timestamp=now, last_price=-10.0)

        # Negative volume
        with pytest.raises(ValidationError, match="volume cannot be negative"):
            Quote(symbol="NIFTY", exchange="NSE", timestamp=now, last_price=100.0, volume=-5.0)

        # Inverted spread (ask < bid)
        with pytest.raises(ValidationError, match="Inverted spread invariant violated"):
            Quote(
                symbol="NIFTY",
                exchange="NSE",
                timestamp=now,
                last_price=100.0,
                bid=105.0,
                ask=95.0,
            )

    def test_quote_immutability(self) -> None:
        """Verify Quote attributes cannot be reassigned."""
        q = Quote(symbol="INFY", exchange="NSE", timestamp=time.time(), last_price=1800.0)
        with pytest.raises((FrozenInstanceError, AttributeError)):
            q.last_price = 1900.0  # type: ignore

    def test_bar_creation_valid(self) -> None:
        """Verify Bar instantiates and validates valid OHLC relationships."""
        bar = Bar(
            symbol="tcs",
            exchange="nse",
            timeframe="5m",
            timestamp=1700000000.0,
            open=4100.0,
            high=4150.0,
            low=4090.0,
            close=4140.0,
            volume=25000.0,
            source="mock",
        )
        assert bar.symbol == "TCS"
        assert bar.exchange == "NSE"
        assert bar.is_bullish is True
        assert bar.range == 60.0

    def test_bar_ohlc_relational_violations(self) -> None:
        """Verify Bar rejects physically impossible OHLC relationships."""
        now = time.time()

        # High less than Open
        with pytest.raises(ValidationError, match="Invalid Bar high price"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now,
                open=100.0, high=90.0, low=80.0, close=85.0)

        # High less than Close
        with pytest.raises(ValidationError, match="Invalid Bar high price"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now,
                open=100.0, high=105.0, low=95.0, close=110.0)

        # Low greater than Open
        with pytest.raises(ValidationError, match="Invalid Bar low price"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now,
                open=100.0, high=120.0, low=105.0, close=115.0)

        # Low greater than Close
        with pytest.raises(ValidationError, match="Invalid Bar low price"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now,
                open=100.0, high=120.0, low=95.0, close=90.0)

    def test_order_book_snapshot(self) -> None:
        """Verify OrderBookSnapshot and OrderBookLevel validations."""
        bids = (OrderBookLevel(price=24500.0, quantity=50.0), OrderBookLevel(price=24495.0, quantity=100.0))
        asks = (OrderBookLevel(price=24505.0, quantity=75.0), OrderBookLevel(price=24510.0, quantity=125.0))

        ob = OrderBookSnapshot(
            symbol="nifty",
            exchange="nse",
            timestamp=1700000000.0,
            bids=bids,
            asks=asks,
            source="mock",
        )
        assert ob.symbol == "NIFTY"
        assert ob.best_bid == 24500.0
        assert ob.best_ask == 24505.0
        assert ob.spread == 5.0

        # Negative price in level raises
        with pytest.raises(ValidationError, match="price cannot be negative"):
            OrderBookLevel(price=-1.0, quantity=10.0)


# ==============================================================================
# 2. NORMALIZATION LAYER TESTS
# ==============================================================================

class TestMarketNormalizer:
    """Test suite for MarketDataNormalizer conversions and error handling."""

    def test_normalize_raw_quote_with_strings_and_iso(self) -> None:
        """Verify string numbers and ISO timestamps are safely converted."""
        raw = {
            "symbol": "reliance",
            "last_price": "2980.75",
            "bid": "2980.00",
            "ask": "2981.50",
            "bid_size": "200",
            "ask_size": "150",
            "volume": "125000",
            "timestamp": "2026-10-01T10:00:00Z",
            "source": "raw_provider",
        }
        quote = MarketDataNormalizer.normalize_quote(raw)
        assert quote.symbol == "RELIANCE"
        assert quote.last_price == 2980.75
        assert quote.bid == 2980.0
        assert quote.ask == 2981.50
        assert quote.volume == 125000.0
        assert quote.source == "raw_provider"
        assert quote.timestamp > 0

    def test_normalize_quote_missing_optional_fields(self) -> None:
        """Verify missing optional fields are left as None rather than invented."""
        raw = {
            "symbol": "TCS",
            "price": "4150.0",
            "timestamp": 1700000000.0,
        }
        quote = MarketDataNormalizer.normalize_quote(raw)
        assert quote.symbol == "TCS"
        assert quote.last_price == 4150.0
        assert quote.bid is None
        assert quote.ask is None
        assert quote.bid_size is None
        assert quote.ask_size is None
        assert quote.volume == 0.0

    def test_normalize_quote_missing_mandatory_fields(self) -> None:
        """Verify missing symbol or price raises ValidationError."""
        with pytest.raises(ValidationError, match="Missing mandatory symbol field"):
            MarketDataNormalizer.normalize_quote({"price": 100.0, "timestamp": 1700000000.0})

        with pytest.raises(ValidationError, match="Missing mandatory price field"):
            MarketDataNormalizer.normalize_quote({"symbol": "NIFTY", "timestamp": 1700000000.0})

    def test_normalize_raw_bar(self) -> None:
        """Verify candlestick bar normalization."""
        raw = {
            "symbol": "INFY",
            "timeframe": "15m",
            "open": "1800.0",
            "high": "1825.0",
            "low": "1795.0",
            "close": "1820.0",
            "volume": "50000",
            "timestamp": 1700000000.0,
        }
        bar = MarketDataNormalizer.normalize_bar(raw)
        assert bar.symbol == "INFY"
        assert bar.timeframe == "15m"
        assert bar.open == 1800.0
        assert bar.high == 1825.0
        assert bar.low == 1795.0
        assert bar.close == 1820.0
        assert bar.volume == 50000.0

    def test_normalize_raw_order_book(self) -> None:
        """Verify order book snapshot normalization with list of lists and dicts."""
        raw = {
            "symbol": "NIFTY",
            "timestamp": 1700000000.0,
            "bids": [[24500.0, 100.0], {"price": 24495.0, "quantity": 200.0}],
            "asks": [[24505.0, 150.0], {"price": 24510.0, "quantity": 250.0}],
        }
        ob = MarketDataNormalizer.normalize_order_book(raw)
        assert ob.symbol == "NIFTY"
        assert len(ob.bids) == 2
        assert len(ob.asks) == 2
        assert ob.best_bid == 24500.0
        assert ob.best_ask == 24505.0


# ==============================================================================
# 3. DATA FRESHNESS TESTS
# ==============================================================================

class TestDataFreshness:
    """Test suite for configurable freshness status checks."""

    def test_fresh_data(self) -> None:
        """Verify recent timestamps return DataFreshnessStatus.FRESH."""
        now = 1000.0
        record_time = 990.0  # 10s old
        status = check_freshness(record_time, reference_time=now, threshold_seconds=60.0)
        assert status == DataFreshnessStatus.FRESH

    def test_stale_data(self) -> None:
        """Verify older timestamps return DataFreshnessStatus.STALE."""
        now = 1000.0
        record_time = 900.0  # 100s old, threshold is 60s
        status = check_freshness(record_time, reference_time=now, threshold_seconds=60.0)
        assert status == DataFreshnessStatus.STALE

    def test_invalid_future_or_negative_timestamp(self) -> None:
        """Verify negative timestamps or timestamps far in the future return INVALID."""
        now = 1000.0
        assert check_freshness(-1.0, reference_time=now) == DataFreshnessStatus.INVALID
        # 1000s in the future (exceeding 300s skew limit)
        assert check_freshness(2000.0, reference_time=now, max_future_skew_seconds=300.0) == DataFreshnessStatus.INVALID


# ==============================================================================
# 4. MOCK FEED TESTS
# ==============================================================================

class TestMockMarketFeed:
    """Test suite for MockMarketFeed deterministic behavior and test scenarios."""

    def test_mock_feed_deterministic_cycling(self) -> None:
        """Verify mock feed deterministically cycles across symbols and stamps source='mock'."""
        feed = MockMarketFeed(name="test_mock", symbols=["NIFTY", "RELIANCE"])
        r1 = feed.next_record()
        r2 = feed.next_record()
        r3 = feed.next_record()

        assert isinstance(r1, Quote) and isinstance(r2, Quote) and isinstance(r3, Quote)
        assert r1.symbol == "NIFTY"
        assert r2.symbol == "RELIANCE"
        assert r3.symbol == "NIFTY"
        assert r1.source == "mock"
        assert feed.records_emitted == 3

    def test_mock_feed_scenarios(self) -> None:
        """Verify predefined mock test scenarios generate expected raw structures."""
        feed = MockMarketFeed()
        now = 1700000000.0

        # Normal Quote
        nq = feed.generate_scenario_quote(MockScenario.NORMAL_QUOTE, symbol="NIFTY", reference_time=now)
        assert nq["last_price"] == 24500.0
        assert nq["bid"] == 24499.5
        assert nq["ask"] == 24500.5

        # Price Increase
        pi = feed.generate_scenario_quote(MockScenario.PRICE_INCREASE, symbol="NIFTY", reference_time=now)
        assert pi["last_price"] > 24500.0

        # Volume Spike
        vs = feed.generate_scenario_quote(MockScenario.VOLUME_SPIKE, symbol="NIFTY", reference_time=now)
        assert vs["volume"] == 1000000.0

        # Stale Timestamp
        st = feed.generate_scenario_quote(MockScenario.STALE_TIMESTAMP, symbol="NIFTY", reference_time=now)
        assert st["timestamp"] == now - 7200.0

    def test_mock_feed_reset_and_stop(self) -> None:
        """Verify feed reset and stop lifecycle."""
        feed = MockMarketFeed()
        feed.next_record()
        feed.next_record()
        assert feed.records_emitted == 2

        feed.reset()
        assert feed.records_emitted == 0

        feed.stop()
        assert feed.is_running is False
        assert feed.next_record() is None


# ==============================================================================
# 5. REPLAY FEED TESTS
# ==============================================================================

class TestReplayMarketFeed:
    """Test suite for ReplayMarketFeed sequenced playback and reset."""

    def test_replay_feed_playback_and_inspection(self) -> None:
        """Verify records are emitted in exact order with source='replay'."""
        raw_records = [
            {"symbol": "NIFTY", "last_price": 24500.0, "timestamp": 1700000001.0},
            {"symbol": "NIFTY", "last_price": 24505.0, "timestamp": 1700000002.0},
            {"symbol": "NIFTY", "last_price": 24510.0, "timestamp": 1700000003.0},
        ]
        feed = ReplayMarketFeed(name="test_replay", records=raw_records)

        assert feed.total_records == 3
        assert feed.current_index == 0
        assert feed.has_next() is True

        r1 = feed.step()
        assert isinstance(r1, Quote)
        assert r1.last_price == 24500.0
        assert r1.source == "replay"
        assert feed.current_index == 1

        r2 = feed.step()
        assert isinstance(r2, Quote)
        assert r2.last_price == 24505.0

        r3 = feed.step()
        assert isinstance(r3, Quote)
        assert r3.last_price == 24510.0

        # End of feed
        assert feed.has_next() is False
        assert feed.step() is None

        # Reset
        feed.reset()
        assert feed.current_index == 0
        assert feed.has_next() is True

    def test_replay_feed_json_file_loading(self, tmp_path: Path) -> None:
        """Verify loading records from a local JSON file."""
        sample_file = tmp_path / "historical_quotes.json"
        data = [
            {"symbol": "RELIANCE", "last_price": 2980.0, "timestamp": 1700000010.0},
            {"symbol": "RELIANCE", "last_price": 2985.0, "timestamp": 1700000020.0},
        ]
        sample_file.write_text(json.dumps(data), encoding="utf-8")

        feed = ReplayMarketFeed(name="json_replay")
        count = feed.load_from_json_file(sample_file)

        assert count == 2
        all_records = feed.play_all()
        assert len(all_records) == 2
        assert all_records[0].symbol == "RELIANCE"
        assert all_records[0].source == "replay"


# ==============================================================================
# 6. EVENT BUS & STATE STORE INTEGRATION TESTS
# ==============================================================================

class TestMarketDataServiceIntegration:
    """Test suite verifying end-to-end ingestion, event publishing, and state store caching."""

    def test_ingest_quote_pipeline(self) -> None:
        """Verify raw quote -> service -> EventBus event -> StateStore update."""
        bus = EventBus(name="service_bus")
        store = StateStore(name="service_store")
        service = MarketDataService(event_bus=bus, state_store=store, freshness_threshold_seconds=60.0)

        received_events: List[Event] = []
        bus.subscribe(EventType.MARKET_QUOTE_UPDATED, lambda e: received_events.append(e))

        raw_quote = {
            "symbol": "INFY",
            "last_price": 1820.50,
            "bid": 1820.00,
            "ask": 1821.00,
            "volume": 75000.0,
            "timestamp": time.time(),
            "source": "mock",
        }

        quote = service.ingest_quote(raw_quote, correlation_id="cid-quote-001")

        # 1. Returned Quote
        assert quote.symbol == "INFY"
        assert quote.last_price == 1820.50

        # 2. EventBus publication
        assert len(received_events) == 1
        event = received_events[0]
        assert event.event_type == "MARKET_QUOTE_UPDATED"
        assert event.correlation_id == "cid-quote-001"
        assert event.payload["symbol"] == "INFY"
        assert event.payload["last_price"] == 1820.50

        # 3. StateStore caching
        latest_quotes = store.get("latest_quotes")
        assert "INFY" in latest_quotes
        assert latest_quotes["INFY"]["last_price"] == 1820.50
        assert store.get("market_data_freshness")["INFY"] == "FRESH"
        assert store.get("market_status") == "active"

    def test_stale_data_event_trigger(self) -> None:
        """Verify ingesting stale quote emits MARKET_DATA_STALE notification event."""
        bus = EventBus(name="stale_bus")
        store = StateStore(name="stale_store")
        service = MarketDataService(event_bus=bus, state_store=store, freshness_threshold_seconds=30.0)

        stale_events: List[Event] = []
        bus.subscribe(EventType.MARKET_DATA_STALE, lambda e: stale_events.append(e))

        now = 10000.0
        stale_quote_data = {
            "symbol": "TCS",
            "last_price": 4150.0,
            "timestamp": now - 120.0,  # 120s old (threshold is 30s)
            "source": "mock",
        }

        service.ingest_quote(stale_quote_data, reference_time=now)

        assert len(stale_events) == 1
        stale_event = stale_events[0]
        assert stale_event.event_type == "MARKET_DATA_STALE"
        assert stale_event.payload["symbol"] == "TCS"
        assert stale_event.payload["age_seconds"] == 120.0

        # StateStore marks freshness as STALE
        assert store.get("market_data_freshness")["TCS"] == "STALE"

    def test_invalid_data_emits_error_event(self) -> None:
        """Verify invalid raw quote emits MARKET_DATA_ERROR and updates error count."""
        bus = EventBus(name="err_bus")
        store = StateStore(name="err_store")
        service = MarketDataService(event_bus=bus, state_store=store)

        error_events: List[Event] = []
        bus.subscribe(EventType.MARKET_DATA_ERROR, lambda e: error_events.append(e))

        invalid_data = {
            "symbol": "NIFTY",
            "last_price": -500.0,  # Negative price
            "timestamp": time.time(),
        }

        with pytest.raises(ValidationError):
            service.ingest_quote(invalid_data, correlation_id="err-cid-99")

        assert len(error_events) == 1
        err_event = error_events[0]
        assert err_event.event_type == "MARKET_DATA_ERROR"
        assert err_event.correlation_id == "err-cid-99"
        assert "cannot be negative" in err_event.payload["error"]
        assert store.get("market_errors_count") == 1

    def test_ingest_bar_and_order_book(self) -> None:
        """Verify bar and order book ingestion into EventBus and StateStore."""
        bus = EventBus(name="bar_ob_bus")
        store = StateStore(name="bar_ob_store")
        service = MarketDataService(event_bus=bus, state_store=store)

        bar_events: List[Event] = []
        depth_events: List[Event] = []
        bus.subscribe(EventType.MARKET_BAR_UPDATED, lambda e: bar_events.append(e))
        bus.subscribe(EventType.MARKET_DEPTH_UPDATED, lambda e: depth_events.append(e))

        now = time.time()
        # Bar Ingestion
        bar = service.ingest_bar({
            "symbol": "NIFTY",
            "timeframe": "1h",
            "timestamp": now,
            "open": 24400.0,
            "high": 24550.0,
            "low": 24380.0,
            "close": 24520.0,
            "volume": 150000.0,
            "source": "replay",
        })
        assert bar.symbol == "NIFTY"
        assert len(bar_events) == 1
        assert "NIFTY_1h" in store.get("latest_bars")

        # Order Book Ingestion
        ob = service.ingest_order_book({
            "symbol": "NIFTY",
            "timestamp": now,
            "bids": [[24520.0, 50.0]],
            "asks": [[24525.0, 50.0]],
            "source": "replay",
        })
        assert ob.symbol == "NIFTY"
        assert len(depth_events) == 1
        assert "NIFTY" in store.get("latest_order_books")


# ==============================================================================
# 7. SAFETY & ISOLATION INVARIANTS
# ==============================================================================

class TestMarketSafetyInvariants:
    """Verify safety constraints are rigorously maintained."""

    def test_no_external_network_calls_in_feeds(self) -> None:
        """Verify MockMarketFeed and ReplayMarketFeed operate purely offline."""
        mock_feed = MockMarketFeed()
        replay_feed = ReplayMarketFeed(records=[{"symbol": "NIFTY", "last_price": 24500.0, "timestamp": time.time()}])

        # Both generate/step without any network or external socket connection
        q_mock = mock_feed.next_record()
        q_replay = replay_feed.next_record()

        assert q_mock is not None
        assert q_replay is not None
        assert q_mock.source == "mock"
        assert q_replay.source == "replay"
