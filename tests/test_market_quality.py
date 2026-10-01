"""KAISER — Market Data Quality Engine Test Suite.

Comprehensive tests verifying:
1. Valid data assessments (Quote, Bar, OrderBookSnapshot)
2. Degraded data handling (missing optional fields, non-fatal warnings)
3. Stale data detection
4. Invalid data rejection
5. Missing data / heartbeat timeout detection
6. Duplicate record detection & price-repeat distinction
7. Out-of-order sequence detection
8. Multi-symbol and multi-source isolation
9. Deterministic quality precedence resolution
10. EventBus quality event publishing
11. StateStore operational caching
12. Thread safety under concurrent evaluations
13. Determinism across repeated executions
"""

import threading
import time
from typing import List

import pytest

from app.core.events import Event, EventBus, EventType
from app.core.state import StateStore
from app.market.models import Bar, OrderBookLevel, OrderBookSnapshot, Quote
from app.market.quality import (
    DataQualityAssessment,
    DataQualityStatus,
    MarketDataQualityEngine,
    MarketQualityConfig,
    SourceHealthTracker,
    is_safe_for_downstream,
    resolve_quality_status,
)
from app.market.validators import DataFreshnessStatus


# ==============================================================================
# 1. VALID DATA ASSESSMENTS
# ==============================================================================

class TestValidDataAssessments:
    """Verify clean, complete market records evaluate to VALID and safe_to_consume=True."""

    def test_valid_quote_evaluation(self) -> None:
        """Verify full quote evaluates to DataQualityStatus.VALID."""
        engine = MarketDataQualityEngine()
        now = time.time()
        quote = Quote(
            symbol="NIFTY",
            exchange="NSE",
            timestamp=now,
            last_price=24500.0,
            bid=24499.0,
            ask=24501.0,
            bid_size=100.0,
            ask_size=150.0,
            volume=50000.0,
            source="mock",
        )
        assessment = engine.evaluate_quote(quote, reference_time=now)

        assert assessment.quality_status == DataQualityStatus.VALID
        assert assessment.safe_to_consume is True
        assert assessment.completeness_status == "COMPLETE"
        assert assessment.sequence_status == "IN_ORDER"
        assert assessment.duplicate_status == "UNIQUE"
        assert len(assessment.errors) == 0
        assert len(assessment.warnings) == 0
        assert "T" in assessment.iso_assessment_timestamp

    def test_valid_bar_evaluation(self) -> None:
        """Verify full bar evaluates to DataQualityStatus.VALID."""
        engine = MarketDataQualityEngine()
        now = time.time()
        bar = Bar(
            symbol="RELIANCE",
            exchange="NSE",
            timeframe="5m",
            timestamp=now,
            open=2980.0,
            high=2995.0,
            low=2975.0,
            close=2990.0,
            volume=15000.0,
            source="replay",
        )
        assessment = engine.evaluate_bar(bar, reference_time=now)

        assert assessment.quality_status == DataQualityStatus.VALID
        assert assessment.safe_to_consume is True
        assert assessment.completeness_status == "COMPLETE"

    def test_valid_order_book_evaluation(self) -> None:
        """Verify full order book evaluates to DataQualityStatus.VALID."""
        engine = MarketDataQualityEngine()
        now = time.time()
        ob = OrderBookSnapshot(
            symbol="TCS",
            exchange="NSE",
            timestamp=now,
            bids=(OrderBookLevel(price=4150.0, quantity=50.0),),
            asks=(OrderBookLevel(price=4152.0, quantity=50.0),),
            source="mock",
        )
        assessment = engine.evaluate_order_book(ob, reference_time=now)

        assert assessment.quality_status == DataQualityStatus.VALID
        assert assessment.safe_to_consume is True


# ==============================================================================
# 2. DEGRADED DATA ASSESSMENTS
# ==============================================================================

class TestDegradedDataAssessments:
    """Verify records missing optional fields or carrying non-fatal warnings are DEGRADED."""

    def test_quote_missing_optional_bid_ask(self) -> None:
        """Verify quote missing bid/ask is marked DEGRADED with warnings but safe to consume."""
        engine = MarketDataQualityEngine()
        now = time.time()
        quote = Quote(
            symbol="INFY",
            exchange="NSE",
            timestamp=now,
            last_price=1820.0,
            bid=None,
            ask=None,
            volume=10000.0,
            source="mock",
        )
        assessment = engine.evaluate_quote(quote, reference_time=now)

        assert assessment.quality_status == DataQualityStatus.DEGRADED
        assert assessment.safe_to_consume is True
        assert assessment.completeness_status == "PARTIAL"
        assert any("Missing optional quote fields" in w for w in assessment.warnings)

    def test_bar_zero_volume_degraded(self) -> None:
        """Verify candlestick bar with zero volume produces DEGRADED status."""
        engine = MarketDataQualityEngine()
        now = time.time()
        bar = Bar(
            symbol="INFY",
            exchange="NSE",
            timeframe="1m",
            timestamp=now,
            open=1820.0,
            high=1820.0,
            low=1820.0,
            close=1820.0,
            volume=0.0,
            source="mock",
        )
        assessment = engine.evaluate_bar(bar, reference_time=now)

        assert assessment.quality_status == DataQualityStatus.DEGRADED
        assert assessment.safe_to_consume is True
        assert any("zero volume" in w for w in assessment.warnings)

    def test_order_book_empty_side_degraded(self) -> None:
        """Verify order book with empty ask side is DEGRADED."""
        engine = MarketDataQualityEngine()
        now = time.time()
        ob = OrderBookSnapshot(
            symbol="NIFTY",
            exchange="NSE",
            timestamp=now,
            bids=(OrderBookLevel(price=24500.0, quantity=100.0),),
            asks=(),  # Empty asks
            source="mock",
        )
        assessment = engine.evaluate_order_book(ob, reference_time=now)

        assert assessment.quality_status == DataQualityStatus.DEGRADED
        assert any("empty asks" in w for w in assessment.warnings)


# ==============================================================================
# 3. STALE & INVALID DATA ASSESSMENTS
# ==============================================================================

class TestStaleAndInvalidData:
    """Verify stale and invalid records are appropriately categorized and unsafe."""

    def test_stale_quote_evaluation(self) -> None:
        """Verify quote older than freshness threshold evaluates to STALE and unsafe."""
        config = MarketQualityConfig(freshness_threshold_seconds=30.0)
        engine = MarketDataQualityEngine(config=config)
        now = 10000.0
        stale_time = now - 90.0  # 90s old vs 30s threshold

        quote = Quote(
            symbol="NIFTY",
            exchange="NSE",
            timestamp=stale_time,
            last_price=24500.0,
            source="mock",
        )
        assessment = engine.evaluate_quote(quote, reference_time=now)

        assert assessment.quality_status == DataQualityStatus.STALE
        assert assessment.safe_to_consume is False
        assert assessment.freshness_status == DataFreshnessStatus.STALE
        assert any("Data is stale" in w for w in assessment.warnings)

    def test_invalid_clock_skew_timestamp(self) -> None:
        """Verify quote timestamp from distant future evaluates to INVALID."""
        engine = MarketDataQualityEngine()
        now = 10000.0
        future_time = now + 10000.0  # Impossible future timestamp

        quote = Quote(
            symbol="NIFTY",
            exchange="NSE",
            timestamp=future_time,
            last_price=24500.0,
            source="mock",
        )
        assessment = engine.evaluate_quote(quote, reference_time=now)

        assert assessment.quality_status == DataQualityStatus.INVALID
        assert assessment.safe_to_consume is False
        assert len(assessment.errors) > 0


# ==============================================================================
# 4. MISSING DATA DETECTION
# ==============================================================================

class TestMissingDataDetection:
    """Verify heartbeat timeout generates MISSING status assessments."""

    def test_missing_data_heartbeat_timeout(self) -> None:
        """Verify check_missing flags stream as MISSING when exceeding timeout."""
        config = MarketQualityConfig(missing_data_timeout_seconds=60.0)
        engine = MarketDataQualityEngine(config=config)

        # Ingest first quote at t=1000
        quote = Quote(symbol="NIFTY", exchange="NSE", timestamp=1000.0, last_price=24500.0, source="mock")
        engine.evaluate_quote(quote, reference_time=1000.0)

        # At t=1030 (30s later), not missing yet
        assert engine.check_missing("NIFTY", source="mock", current_time=1030.0) is None

        # At t=1070 (70s later > 60s timeout), stream is missing
        missing_assessment = engine.check_missing("NIFTY", source="mock", current_time=1070.0)
        assert missing_assessment is not None
        assert missing_assessment.quality_status == DataQualityStatus.MISSING
        assert missing_assessment.safe_to_consume is False
        assert missing_assessment.completeness_status == "INCOMPLETE"


# ==============================================================================
# 5. DUPLICATE DETECTION & PRICE-REPEAT DISTINCTION
# ==============================================================================

class TestDuplicateDetection:
    """Verify duplicate detection and distinction between duplicates and price repeats."""

    def test_exact_duplicate_detection(self) -> None:
        """Verify exact same quote submitted twice triggers DUPLICATE on second call."""
        engine = MarketDataQualityEngine()
        now = time.time()
        quote = Quote(symbol="TCS", exchange="NSE", timestamp=now, last_price=4150.0, volume=100.0, source="mock")

        first = engine.evaluate_quote(quote, reference_time=now)
        second = engine.evaluate_quote(quote, reference_time=now)

        assert first.quality_status == DataQualityStatus.DEGRADED or first.quality_status == DataQualityStatus.VALID
        assert first.duplicate_status == "UNIQUE"

        assert second.quality_status == DataQualityStatus.DUPLICATE
        assert second.duplicate_status == "DUPLICATE"
        assert second.safe_to_consume is False

    def test_repeated_price_different_timestamp_is_not_duplicate(self) -> None:
        """Verify separate observations at identical prices are NOT marked duplicate."""
        engine = MarketDataQualityEngine()
        t1 = 1000.0
        t2 = 1001.0

        q1 = Quote(symbol="RELIANCE", exchange="NSE", timestamp=t1, last_price=2980.0, volume=100.0, source="mock")
        q2 = Quote(symbol="RELIANCE", exchange="NSE", timestamp=t2, last_price=2980.0, volume=200.0, source="mock")

        first = engine.evaluate_quote(q1, reference_time=t1)
        second = engine.evaluate_quote(q2, reference_time=t2)

        assert first.duplicate_status == "UNIQUE"
        assert second.duplicate_status == "UNIQUE"
        assert second.quality_status != DataQualityStatus.DUPLICATE


# ==============================================================================
# 6. OUT-OF-ORDER SEQUENCE DETECTION
# ==============================================================================

class TestOutOfOrderDetection:
    """Verify out-of-order sequence arrivals are flagged and quarantined."""

    def test_out_of_order_detection(self) -> None:
        """Verify older timestamp arriving after newer timestamp is OUT_OF_ORDER."""
        engine = MarketDataQualityEngine()

        q1 = Quote(symbol="NIFTY", exchange="NSE", timestamp=1000.0, last_price=24500.0, source="mock")
        q2 = Quote(symbol="NIFTY", exchange="NSE", timestamp=1005.0, last_price=24510.0, source="mock")
        q3 = Quote(symbol="NIFTY", exchange="NSE", timestamp=1002.0, last_price=24505.0, source="mock")  # Arrives late

        r1 = engine.evaluate_quote(q1, reference_time=1000.0)
        r2 = engine.evaluate_quote(q2, reference_time=1005.0)
        r3 = engine.evaluate_quote(q3, reference_time=1005.0)

        assert r1.sequence_status == "IN_ORDER"
        assert r2.sequence_status == "IN_ORDER"

        assert r3.sequence_status == "OUT_OF_ORDER"
        assert r3.quality_status == DataQualityStatus.OUT_OF_ORDER
        assert r3.safe_to_consume is False

    def test_independent_symbols_ordering(self) -> None:
        """Verify out-of-order sequence on Symbol A does not affect Symbol B."""
        engine = MarketDataQualityEngine()

        # Symbol A moves to t=2000
        engine.evaluate_quote(Quote(symbol="INFY", exchange="NSE", timestamp=2000.0, last_price=1800.0, source="mock"), reference_time=2000.0)

        # Symbol B at t=1500 is completely in order for Symbol B
        res_b = engine.evaluate_quote(Quote(symbol="TCS", exchange="NSE", timestamp=1500.0, last_price=4100.0, source="mock"), reference_time=1500.0)

        assert res_b.sequence_status == "IN_ORDER"
        assert res_b.quality_status != DataQualityStatus.OUT_OF_ORDER


# ==============================================================================
# 7. SOURCE HEALTH TRACKING
# ==============================================================================

class TestSourceHealthTracking:
    """Verify operational telemetry per data source."""

    def test_source_health_metrics_accumulation(self) -> None:
        """Verify source health tracks accepted, duplicate, and stale records independently."""
        engine = MarketDataQualityEngine()

        now = 10000.0
        # 1. Valid quote
        engine.evaluate_quote(Quote(symbol="NIFTY", exchange="NSE", timestamp=now, last_price=24500.0, source="feed_A"), reference_time=now)
        # 2. Duplicate quote
        engine.evaluate_quote(Quote(symbol="NIFTY", exchange="NSE", timestamp=now, last_price=24500.0, source="feed_A"), reference_time=now)
        # 3. Quote from feed_B
        engine.evaluate_quote(Quote(symbol="TCS", exchange="NSE", timestamp=now, last_price=4150.0, source="feed_B"), reference_time=now)

        health_a = engine.source_tracker.get_health("feed_A")
        health_b = engine.source_tracker.get_health("feed_B")

        assert health_a.records_received == 2
        assert health_a.duplicate_count == 1

        assert health_b.records_received == 1
        assert health_b.duplicate_count == 0


# ==============================================================================
# 8. DETERMINISTIC PRECEDENCE RESOLUTION
# ==============================================================================

class TestQualityPrecedence:
    """Verify deterministic precedence: INVALID > MISSING > OUT_OF_ORDER > DUPLICATE > STALE > DEGRADED > VALID."""

    def test_precedence_invalid_overrides_all(self) -> None:
        """Verify INVALID supersedes out-of-order, duplicate, stale, degraded."""
        res = resolve_quality_status(
            has_errors=True,
            freshness=DataFreshnessStatus.STALE,
            is_out_of_order=True,
            is_duplicate=True,
            is_degraded=True,
        )
        assert res == DataQualityStatus.INVALID

    def test_precedence_out_of_order_overrides_duplicate_and_stale(self) -> None:
        """Verify OUT_OF_ORDER supersedes duplicate and stale."""
        res = resolve_quality_status(
            has_errors=False,
            freshness=DataFreshnessStatus.STALE,
            is_out_of_order=True,
            is_duplicate=True,
            is_degraded=True,
        )
        assert res == DataQualityStatus.OUT_OF_ORDER

    def test_precedence_duplicate_overrides_stale(self) -> None:
        """Verify DUPLICATE supersedes stale."""
        res = resolve_quality_status(
            has_errors=False,
            freshness=DataFreshnessStatus.STALE,
            is_duplicate=True,
            is_degraded=True,
        )
        assert res == DataQualityStatus.DUPLICATE

    def test_precedence_stale_overrides_degraded(self) -> None:
        """Verify STALE supersedes degraded."""
        res = resolve_quality_status(
            has_errors=False,
            freshness=DataFreshnessStatus.STALE,
            is_degraded=True,
        )
        assert res == DataQualityStatus.STALE


# ==============================================================================
# 9. EVENT BUS & STATE STORE INTEGRATION
# ==============================================================================

class TestQualityEventAndStateIntegration:
    """Verify EventBus event publishing and StateStore persistence."""

    def test_quality_events_and_state_caching(self) -> None:
        """Verify quality assessments are broadcast on EventBus and cached in StateStore."""
        bus = EventBus(name="q_bus")
        store = StateStore(name="q_store")
        engine = MarketDataQualityEngine(event_bus=bus, state_store=store)

        quality_events: List[Event] = []
        degraded_events: List[Event] = []
        bus.subscribe(EventType.MARKET_DATA_QUALITY_UPDATED, lambda e: quality_events.append(e))
        bus.subscribe(EventType.MARKET_DATA_DEGRADED, lambda e: degraded_events.append(e))

        now = time.time()
        # Ingest quote missing bid/ask (degraded)
        quote = Quote(symbol="NIFTY", exchange="NSE", timestamp=now, last_price=24500.0, bid=None, ask=None, source="mock")
        assessment = engine.evaluate_quote(quote, correlation_id="cid-quality-100", reference_time=now)

        # 1. EventBus events received
        assert len(quality_events) == 1
        assert quality_events[0].event_type == "MARKET_DATA_QUALITY_UPDATED"
        assert quality_events[0].correlation_id == "cid-quality-100"

        assert len(degraded_events) == 1
        assert degraded_events[0].event_type == "MARKET_DATA_DEGRADED"

        # 2. StateStore cached
        stored_quality = store.get("market_data_quality")
        assert "NIFTY_quote" in stored_quality
        assert stored_quality["NIFTY_quote"]["quality_status"] == "DEGRADED"

        source_health = store.get("market_source_health")
        assert "mock" in source_health
        assert source_health["mock"]["records_accepted"] == 1


# ==============================================================================
# 10. THREAD SAFETY & CONCURRENCY
# ==============================================================================

class TestQualityConcurrency:
    """Verify multi-threaded evaluation does not corrupt state or cause race conditions."""

    def test_concurrent_quality_evaluations(self) -> None:
        """Run concurrent evaluations across multiple threads."""
        engine = MarketDataQualityEngine()
        thread_count = 8
        evals_per_thread = 25
        errors: List[Exception] = []

        def worker(thread_id: int) -> None:
            try:
                base_time = 1000.0 + (thread_id * 100)
                for i in range(evals_per_thread):
                    q = Quote(
                        symbol=f"SYM_{thread_id}",
                        exchange="NSE",
                        timestamp=base_time + i,
                        last_price=100.0 + i,
                        source="mock",
                    )
                    assessment = engine.evaluate_quote(q, reference_time=base_time + i)
                    assert assessment is not None
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(tid,)) for tid in range(thread_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        total_records = engine.source_tracker.get_health("mock").records_received
        assert total_records == thread_count * evals_per_thread


# ==============================================================================
# 11. DETERMINISM TEST
# ==============================================================================

class TestQualityDeterminism:
    """Verify exact same input sequence produces identical outputs across executions."""

    def test_deterministic_output(self) -> None:
        """Run two independent engines on same sequence and assert identical outputs."""
        seq = [
            Quote(symbol="NIFTY", exchange="NSE", timestamp=1000.0, last_price=24500.0, bid=24499.0, ask=24501.0, bid_size=100.0, ask_size=100.0, source="mock"),
            Quote(symbol="NIFTY", exchange="NSE", timestamp=1000.0, last_price=24500.0, bid=24499.0, ask=24501.0, bid_size=100.0, ask_size=100.0, source="mock"),  # Duplicate
            Quote(symbol="NIFTY", exchange="NSE", timestamp=995.0, last_price=24490.0, source="mock"),  # Out of order
        ]

        def run_seq() -> List[DataQualityStatus]:
            eng = MarketDataQualityEngine()
            results = []
            for item in seq:
                res = eng.evaluate_quote(item, reference_time=1000.0)
                results.append(res.quality_status)
            return results

        run1 = run_seq()
        run2 = run_seq()

        assert run1 == run2
        assert run1 == [DataQualityStatus.VALID, DataQualityStatus.DUPLICATE, DataQualityStatus.OUT_OF_ORDER]
