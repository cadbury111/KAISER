"""KAISER — Historical Data Storage, Cache & Replay Engine Tests.

Deterministic, offline tests verifying:
1. SQLiteHistoricalBarRepository persistence, schema integrity, and query round trips.
2. Uniqueness constraints, duplicate prevention, and conflict resolution (replace vs ignore).
3. Symbol, timeframe, and timestamp range isolation.
4. Strictly ascending timestamp ordering and UTC conventions.
5. HistoricalDataCache deterministic keys, LRU bounded eviction, TTL expiry, and invalidation.
6. Error handling, corrupted record detection, and SQLite integrity checks.
7. HistoricalReplayEngine chronological playback, configurable pacing, and observable events.
8. Replay reproducibility and deterministic indicator outputs.
9. Analysis state isolation (reset_stream) between replay runs.
10. Quality-gate enforcement during replay.
11. Replay cancellation and failure recovery.
12. MasterOrchestrator historical command handlers (store, query, replay).
13. Offline startup and trading safety invariants (Settings.execution_locked = True).
"""

from __future__ import annotations

import sqlite3
import threading
import time
from typing import List, Tuple
import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.models import SUPPORTED_TIMEFRAMES, normalize_timeframe
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.cache import HistoricalDataCache, make_cache_key
from app.market.historical.handlers import register_historical_handlers
from app.market.historical.loader import HistoricalDataLoader
from app.market.historical.mock import MockHistoricalDataProvider, get_base_price_for_symbol
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalProviderStatus,
)
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
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.market.quality.status import DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, RequestStatus


def _create_sample_bars(
    symbol: str = "RELIANCE",
    timeframe: str = "1m",
    count: int = 10,
    start_ts: float = 1700000000.0,
    interval_sec: float = 60.0,
    base_price: float = 2500.0,
    source: str = "mock",
) -> List[Bar]:
    """Helper to generate a deterministic list of valid Bar objects."""
    bars: List[Bar] = []
    p = base_price
    for i in range(count):
        ts = start_ts + (i * interval_sec)
        o = p
        h = o + 2.0
        l = o - 1.5
        c = o + 0.5
        v = 100.0 + (i * 10.0)
        bars.append(
            Bar(
                symbol=symbol,
                exchange="NSE",
                timeframe=timeframe,
                timestamp=ts,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=v,
                source=source,
            )
        )
        p = c
    return bars


# ==============================================================================
# 1. Local Historical Storage (SQLite) Tests
# ==============================================================================

class TestSQLiteHistoricalBarRepository:
    """Tests for SQLiteHistoricalBarRepository."""

    def test_schema_init_and_integrity_check(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        assert repo.verify_integrity() is True
        assert repo.get_bar_count() == 0
        repo.close()

    def test_store_and_query_round_trip(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        bars = _create_sample_bars(symbol="TCS", timeframe="5m", count=5, start_ts=1700000000.0, interval_sec=300.0)

        inserted = repo.store_bars(bars, source_provider="unit_test")
        assert inserted == 5
        assert repo.get_bar_count(symbol="TCS", timeframe="5m") == 5

        # Query all
        queried = repo.query_bars(
            symbol="tcs",
            timeframe="5m",
            start_time=1700000000.0,
            end_time=1700001500.0,
        )
        assert len(queried) == 5
        for original, retrieved in zip(bars, queried):
            assert retrieved.symbol == "TCS"
            assert retrieved.timeframe == "5m"
            assert retrieved.timestamp == original.timestamp
            assert retrieved.open == pytest.approx(original.open)
            assert retrieved.high == pytest.approx(original.high)
            assert retrieved.low == pytest.approx(original.low)
            assert retrieved.close == pytest.approx(original.close)
            assert retrieved.volume == pytest.approx(original.volume)
            assert retrieved.source == "unit_test"
        repo.close()

    def test_duplicate_prevention_on_conflict_replace(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        bars = _create_sample_bars(symbol="INFY", timeframe="15m", count=3, start_ts=1700000000.0, interval_sec=900.0)
        repo.store_bars(bars)
        assert repo.get_bar_count(symbol="INFY", timeframe="15m") == 3

        # Update the second bar with a revised close
        revised_bar = Bar(
            symbol="INFY",
            exchange="NSE",
            timeframe="15m",
            timestamp=bars[1].timestamp,
            open=bars[1].open,
            high=bars[1].high + 5.0,
            low=bars[1].low,
            close=bars[1].close + 4.0,
            volume=bars[1].volume + 50.0,
            source="revised_feed",
        )
        repo.store_bars([revised_bar], on_conflict="replace")

        # Total count must still be 3 (no duplicate inserted)
        assert repo.get_bar_count(symbol="INFY", timeframe="15m") == 3

        queried = repo.query_bars(
            symbol="INFY",
            timeframe="15m",
            start_time=bars[0].timestamp,
            end_time=bars[2].timestamp,
        )
        assert len(queried) == 3
        assert queried[1].high == pytest.approx(revised_bar.high)
        assert queried[1].close == pytest.approx(revised_bar.close)
        assert queried[1].source == "revised_feed"
        repo.close()

    def test_duplicate_prevention_on_conflict_ignore(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        bars = _create_sample_bars(symbol="INFY", timeframe="15m", count=2, start_ts=1700000000.0, interval_sec=900.0)
        repo.store_bars(bars)

        # Attempt to insert identical bar with on_conflict="ignore"
        ignored_bar = Bar(
            symbol="INFY",
            exchange="NSE",
            timeframe="15m",
            timestamp=bars[0].timestamp,
            open=999.0,
            high=999.0,
            low=999.0,
            close=999.0,
            volume=999.0,
            source="ignored_feed",
        )
        repo.store_bars([ignored_bar], on_conflict="ignore")

        queried = repo.query_bars("INFY", "15m", 1700000000.0, 1700001000.0)
        assert len(queried) == 2
        # Original price preserved
        assert queried[0].close == pytest.approx(bars[0].close)
        assert queried[0].close != 999.0
        repo.close()

    def test_symbol_timeframe_and_range_isolation(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        rel_1m = _create_sample_bars("RELIANCE", "1m", 10, start_ts=1000.0, interval_sec=60.0)
        rel_5m = _create_sample_bars("RELIANCE", "5m", 10, start_ts=1000.0, interval_sec=300.0)
        tcs_1m = _create_sample_bars("TCS", "1m", 10, start_ts=1000.0, interval_sec=60.0)

        repo.store_bars(rel_1m)
        repo.store_bars(rel_5m)
        repo.store_bars(tcs_1m)

        assert repo.get_bar_count() == 30
        assert repo.get_bar_count(symbol="RELIANCE") == 20
        assert repo.get_bar_count(symbol="RELIANCE", timeframe="1m") == 10
        assert repo.get_bar_count(symbol="TCS", timeframe="1m") == 10

        # Query bounded range for RELIANCE 1m
        bounded = repo.query_bars("RELIANCE", "1m", start_time=1120.0, end_time=1300.0)
        # Timestamps: 1000, 1060, 1120, 1180, 1240, 1300, 1360...
        # 1120, 1180, 1240, 1300 -> exactly 4 bars
        assert len(bounded) == 4
        assert bounded[0].timestamp == 1120.0
        assert bounded[-1].timestamp == 1300.0
        for b in bounded:
            assert b.symbol == "RELIANCE"
            assert b.timeframe == "1m"
        repo.close()

    def test_timestamp_ordering_strictly_ascending(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        bars = _create_sample_bars("NIFTY", "1h", 5, start_ts=1000.0, interval_sec=3600.0)
        # Store in shuffled/reverse order
        shuffled = [bars[4], bars[1], bars[3], bars[0], bars[2]]
        repo.store_bars(shuffled)

        queried = repo.query_bars("NIFTY", "1h", start_time=500.0, end_time=20000.0)
        assert len(queried) == 5
        timestamps = [b.timestamp for b in queried]
        assert timestamps == sorted(timestamps)
        assert timestamps == [bars[i].timestamp for i in range(5)]
        repo.close()

    def test_get_stored_ranges_and_delete(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        bars = _create_sample_bars("SBIN", "1D", 4, start_ts=10000.0, interval_sec=86400.0)
        repo.store_bars(bars)

        ranges = repo.get_stored_ranges()
        assert len(ranges) == 1
        r = ranges[0]
        assert r["symbol"] == "SBIN"
        assert r["timeframe"] == "1D"
        assert r["bar_count"] == 4
        assert r["min_timestamp"] == 10000.0
        assert r["max_timestamp"] == 10000.0 + (3 * 86400.0)
        assert r["min_iso"] is not None
        assert r["max_iso"] is not None

        # Delete specific range
        deleted = repo.delete_bars("SBIN", "1D", start_time=10000.0, end_time=10000.0 + 86400.0)
        assert deleted == 2
        assert repo.get_bar_count(symbol="SBIN", timeframe="1D") == 2
        repo.close()

    def test_corrupted_record_detection_raises_validation_error(self):
        """Verify that directly inserted corrupt records are caught and raise ValidationError."""
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        # Direct raw SQL insert of corrupt bar (high < low)
        conn = repo._get_connection()
        conn.execute("""
            INSERT INTO historical_bars (
                symbol, exchange, timeframe, timestamp, open, high, low, close, volume, source, created_at
            ) VALUES ('CORRUPT', 'NSE', '1m', 1000.0, 100.0, 50.0, 150.0, 100.0, 10.0, 'corrupt', 1000.0);
        """)

        with pytest.raises(ValidationError, match="Corrupted bar record in database"):
            repo.query_bars("CORRUPT", "1m", start_time=500.0, end_time=2000.0)
        repo.close()

    def test_provider_interface_conformance(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        bars = _create_sample_bars("WIPRO", "1m", 3, start_ts=1000.0, interval_sec=60.0)
        repo.store_bars(bars)

        req_found = HistoricalBarRequest(symbol="WIPRO", timeframe="1m", start_time=1000.0, end_time=1200.0)
        resp_found = repo.get_historical_bars(req_found)
        assert resp_found.status == HistoricalProviderStatus.SUCCESS
        assert len(resp_found.bars) == 3

        req_empty = HistoricalBarRequest(symbol="WIPRO", timeframe="15m", start_time=1000.0, end_time=1200.0)
        resp_empty = repo.get_historical_bars(req_empty)
        assert resp_empty.status == HistoricalProviderStatus.EMPTY
        assert len(resp_empty.bars) == 0
        repo.close()

    def test_invalid_query_parameters(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="test_repo")
        with pytest.raises(ValidationError, match="symbol must be a non-empty string"):
            repo.query_bars("", "1m", 100.0, 200.0)

        with pytest.raises(ValidationError, match="Invalid query time range"):
            repo.query_bars("RELIANCE", "1m", 200.0, 100.0)

        with pytest.raises(ValidationError, match="Invalid query time range"):
            repo.query_bars("RELIANCE", "1m", -10.0, 100.0)
        repo.close()


# ==============================================================================
# 2. Historical Data Cache Tests
# ==============================================================================

class TestHistoricalDataCache:
    """Tests for HistoricalDataCache bounded LRU cache."""

    def test_make_cache_key_deterministic(self):
        k1 = make_cache_key("reliance ", "1M", 1700000000.0001, 1700003600.0, 500)
        k2 = make_cache_key("RELIANCE", "1m", 1700000000.0, 1700003600.0, 500)
        assert k1 == k2
        assert k1 == ("RELIANCE", "1m", 1700000000.0, 1700003600.0, 500)

    def test_cache_hit_and_miss(self):
        cache = HistoricalDataCache(max_size=10, ttl_seconds=60.0)
        bars = tuple(_create_sample_bars("TCS", "1m", 2, 1000.0))
        key = make_cache_key("TCS", "1m", 1000.0, 1100.0, 10)

        # Cache miss
        assert cache.get(key) is None
        assert cache.hits == 0
        assert cache.misses == 1

        # Cache put & hit
        cache.put(key, bars, is_complete=True)
        cached = cache.get(key)
        assert cached is not None
        assert len(cached) == 2
        assert cache.hits == 1
        assert cache.hit_rate == 0.5

    def test_bounded_capacity_lru_eviction(self):
        cache = HistoricalDataCache(max_size=3, ttl_seconds=60.0)
        k1 = make_cache_key("S1", "1m", 100.0, 200.0)
        k2 = make_cache_key("S2", "1m", 100.0, 200.0)
        k3 = make_cache_key("S3", "1m", 100.0, 200.0)
        k4 = make_cache_key("S4", "1m", 100.0, 200.0)

        b = tuple(_create_sample_bars("S", "1m", 1, 100.0))
        cache.put(k1, b)
        cache.put(k2, b)
        cache.put(k3, b)
        assert cache.size == 3
        assert cache.evictions == 0

        # Access k1 to make k2 the least recently used
        assert cache.get(k1) is not None

        # Put 4th entry -> k2 should be evicted
        cache.put(k4, b)
        assert cache.size == 3
        assert cache.evictions == 1
        assert cache.get(k2) is None
        assert cache.get(k1) is not None
        assert cache.get(k3) is not None
        assert cache.get(k4) is not None

    def test_ttl_expiry(self):
        cache = HistoricalDataCache(max_size=10, ttl_seconds=0.05)
        key = make_cache_key("EXP", "1m", 100.0, 200.0)
        cache.put(key, tuple(_create_sample_bars("EXP", "1m", 1, 100.0)))

        assert cache.get(key) is not None
        time.sleep(0.06)
        # Should be expired and treated as miss
        assert cache.get(key) is None

    def test_stream_invalidation(self):
        cache = HistoricalDataCache(max_size=10, ttl_seconds=60.0)
        k_rel_1m = make_cache_key("RELIANCE", "1m", 100.0, 200.0)
        k_rel_5m = make_cache_key("RELIANCE", "5m", 100.0, 500.0)
        k_tcs_1m = make_cache_key("TCS", "1m", 100.0, 200.0)

        b = tuple(_create_sample_bars("S", "1m", 1, 100.0))
        cache.put(k_rel_1m, b)
        cache.put(k_rel_5m, b)
        cache.put(k_tcs_1m, b)
        assert cache.size == 3

        # Invalidate only RELIANCE 1m
        removed = cache.invalidate_stream("RELIANCE", "1m")
        assert removed == 1
        assert cache.get(k_rel_1m) is None
        assert cache.get(k_rel_5m) is not None
        assert cache.get(k_tcs_1m) is not None

        # Invalidate all RELIANCE
        removed_all_rel = cache.invalidate_stream("RELIANCE")
        assert removed_all_rel == 1
        assert cache.get(k_rel_5m) is None
        assert cache.get(k_tcs_1m) is not None

    def test_incomplete_results_not_cached(self):
        cache = HistoricalDataCache(max_size=10)
        key = make_cache_key("INC", "1m", 100.0, 200.0)
        cache.put(key, tuple(_create_sample_bars("INC", "1m", 1, 100.0)), is_complete=False)
        assert cache.get(key) is None
        assert cache.size == 0


# ==============================================================================
# 3. Loader with Repository and Cache Integration
# ==============================================================================

class TestHistoricalDataLoaderWithStorageAndCache:
    """Tests verifying cache and repository interaction in HistoricalDataLoader."""

    def test_loader_populates_cache_and_serves_hit(self):
        provider = MockHistoricalDataProvider(name="mock")
        quality_engine = MarketDataQualityEngine()
        analysis_engine = TechnicalAnalysisEngine()
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")
        cache = HistoricalDataCache(max_size=50)

        loader = HistoricalDataLoader(
            provider=provider,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
            repository=repo,
            cache=cache,
        )

        req = HistoricalBarRequest(symbol="RELIANCE", timeframe="1m", start_time=1700000000.0, end_time=1700000300.0)
        # First call: cache miss, provider loaded
        resp1 = loader.load_bars(req)
        assert resp1.status == HistoricalProviderStatus.SUCCESS
        assert cache.hits == 0
        assert cache.size == 1

        # Second call: cache hit!
        resp2 = loader.load_bars(req)
        assert resp2.status == HistoricalProviderStatus.SUCCESS
        assert resp2.provider_name.endswith("_cached")
        assert cache.hits == 1

    def test_persist_bars_invalidates_cache_and_emits_event(self):
        event_bus = EventBus()
        events_captured: List[Event] = []
        event_bus.subscribe(EventType.HISTORICAL_DATA_STORED, lambda ev: events_captured.append(ev))

        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")
        cache = HistoricalDataCache(max_size=50)
        loader = HistoricalDataLoader(
            provider=MockHistoricalDataProvider(),
            quality_engine=MarketDataQualityEngine(),
            analysis_engine=TechnicalAnalysisEngine(),
            event_bus=event_bus,
            repository=repo,
            cache=cache,
        )

        # Pre-seed cache
        req = HistoricalBarRequest(symbol="TCS", timeframe="1m", start_time=1000.0, end_time=1200.0)
        loader.load_bars(req)
        assert cache.size == 1

        # Persist new bars for TCS 1m
        new_bars = _create_sample_bars("TCS", "1m", 2, 2000.0)
        stored_count = loader.persist_bars(new_bars)
        assert stored_count == 2
        # Cache for TCS 1m must be invalidated
        assert cache.size == 0
        assert len(events_captured) == 1
        assert events_captured[0].payload["symbol"] == "TCS"
        assert events_captured[0].payload["count"] == 2


# ==============================================================================
# 4. Deterministic Replay Engine Tests
# ==============================================================================

class TestHistoricalReplayEngine:
    """Tests for HistoricalReplayEngine."""

    def test_chronological_replay_and_indicator_computation(self):
        event_bus = EventBus()
        state_store = StateStore()
        quality_engine = MarketDataQualityEngine()
        analysis_engine = TechnicalAnalysisEngine(event_bus=event_bus, state_store=state_store)
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")

        events_captured: List[Event] = []
        for ev_type in (
            EventType.HISTORICAL_REPLAY_STARTED,
            EventType.HISTORICAL_REPLAY_PROGRESS,
            EventType.HISTORICAL_REPLAY_COMPLETED,
        ):
            event_bus.subscribe(ev_type, lambda ev: events_captured.append(ev))

        # Store 60 sequential 1m bars
        stored = repo.store_bars(_create_sample_bars("RELIANCE", "1m", count=60, start_ts=1700000000.0))
        assert stored == 60

        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
            event_bus=event_bus,
            state_store=state_store,
        )

        req = HistoricalReplayRequest(
            symbol="RELIANCE",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            pacing_seconds=0.0,
            reset_analysis_state=True,
        )

        result = replay_engine.replay(req)

        assert result.status == HistoricalReplayStatus.COMPLETED
        assert result.total_bars_available == 60
        assert result.bars_replayed == 60
        assert result.bars_accepted == 60
        assert result.bars_rejected == 0
        assert result.vectors_computed == 60
        assert result.duration_ms >= 0.0

        # Verify analysis engine has 60 bars in series
        series = analysis_engine.get_series("RELIANCE", "1m")
        assert series is not None
        assert series.count == 60

        # Verify lifecycle events were published
        event_types = [ev.event_type for ev in events_captured]
        assert EventType.HISTORICAL_REPLAY_STARTED in event_types
        assert EventType.HISTORICAL_REPLAY_COMPLETED in event_types

        # Verify StateStore recorded result
        last_replay = state_store.get("last_replay_result")
        assert last_replay is not None
        assert last_replay["status"] == "COMPLETED"
        assert state_store.get("replays_completed_count") == 1

    def test_replay_reproducibility(self):
        """Verifies running replay twice with reset produces identical indicators."""
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")
        bars = _create_sample_bars("INFY", "5m", count=40, start_ts=1700000000.0, interval_sec=300.0)
        repo.store_bars(bars)

        quality_engine = MarketDataQualityEngine()
        analysis_engine = TechnicalAnalysisEngine()
        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )

        req = HistoricalReplayRequest(
            symbol="INFY",
            timeframe="5m",
            start_time=1700000000.0,
            end_time=1700012000.0,
            pacing_seconds=0.0,
            reset_analysis_state=True,
        )

        # Run 1
        res1 = replay_engine.replay(req)
        vec1 = analysis_engine.get_latest_vector("INFY", "5m")

        # Run 2
        res2 = replay_engine.replay(req)
        vec2 = analysis_engine.get_latest_vector("INFY", "5m")

        assert res1.bars_accepted == res2.bars_accepted == 40
        assert vec1 is not None and vec2 is not None
        assert vec1.timestamp == vec2.timestamp
        assert vec1.last_close == pytest.approx(vec2.last_close)
        assert vec1.bar_count == vec2.bar_count == 40
        if "rsi_14" in vec1.indicators and "rsi_14" in vec2.indicators:
            assert vec1.indicators["rsi_14"].value == pytest.approx(vec2.indicators["rsi_14"].value)
        if "sma_20" in vec1.indicators and "sma_20" in vec2.indicators:
            assert vec1.indicators["sma_20"].value == pytest.approx(vec2.indicators["sma_20"].value)

    def test_state_isolation_between_replay_runs(self):
        """Verify reset_analysis_state=True isolates engine state from prior runs."""
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")
        repo.store_bars(_create_sample_bars("TCS", "1m", count=15, start_ts=1000.0))

        analysis_engine = TechnicalAnalysisEngine()
        # Seed engine with older bars
        prior_bar = Bar(
            symbol="TCS", exchange="NSE", timeframe="1m", timestamp=500.0,
            open=3000.0, high=3010.0, low=2990.0, close=3005.0, volume=100.0
        )
        analysis_engine.add_bar(prior_bar, safe_to_consume=True)
        series0 = analysis_engine.get_series("TCS", "1m")
        assert series0 is not None and series0.count == 1

        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=MarketDataQualityEngine(),
            analysis_engine=analysis_engine,
        )

        # Replay with reset
        req = HistoricalReplayRequest(
            symbol="TCS", timeframe="1m", start_time=1000.0, end_time=2000.0,
            reset_analysis_state=True,
        )
        res = replay_engine.replay(req)
        assert res.bars_accepted == 15

        # Series count must be 15, NOT 16 (the ts=500 bar was cleared)
        series = analysis_engine.get_series("TCS", "1m")
        assert series is not None
        assert series.count == 15
        assert series.get_bars()[0].timestamp == 1000.0

    def test_replay_cancellation(self):
        event_bus = EventBus()
        cancelled_events: List[Event] = []
        event_bus.subscribe(EventType.HISTORICAL_REPLAY_CANCELLED, lambda ev: cancelled_events.append(ev))

        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")
        repo.store_bars(_create_sample_bars("SBIN", "1m", count=50, start_ts=1000.0))

        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=MarketDataQualityEngine(),
            analysis_engine=TechnicalAnalysisEngine(),
            event_bus=event_bus,
        )

        stop_token = threading.Event()
        stop_token.set()  # Cancel immediately

        req = HistoricalReplayRequest(
            symbol="SBIN", timeframe="1m", start_time=1000.0, end_time=5000.0,
        )
        result = replay_engine.replay(req, cancellation_token=stop_token)

        assert result.status == HistoricalReplayStatus.CANCELLED
        assert result.bars_replayed == 0
        assert len(cancelled_events) == 1

    def test_quality_gate_enforcement_during_replay(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")
        bars = _create_sample_bars("RELIANCE", "1m", count=5, start_ts=1000.0)
        repo.store_bars(bars)

        # Force insert a degraded zero-volume bar and check quality gate handling
        zero_vol_bar = Bar(
            symbol="RELIANCE", exchange="NSE", timeframe="1m", timestamp=1000.0 + 300.0,
            open=2500.0, high=2505.0, low=2495.0, close=2501.0, volume=0.0, source="mock"
        )
        repo.store_bars([zero_vol_bar])

        analysis_engine = TechnicalAnalysisEngine()
        quality_engine = MarketDataQualityEngine()
        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )

        req = HistoricalReplayRequest("RELIANCE", "1m", start_time=1000.0, end_time=2000.0)
        result = replay_engine.replay(req)

        assert result.total_bars_available == 6
        assert result.zero_volume_bars == 1
        # Zero-volume bar is degraded but safe_to_consume=True, so accepted
        assert result.bars_accepted == 6

    def test_configurable_pacing(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")
        repo.store_bars(_create_sample_bars("FAST", "1m", count=4, start_ts=1000.0))

        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=MarketDataQualityEngine(),
            analysis_engine=TechnicalAnalysisEngine(),
        )

        # Immediate replay (0 latency)
        t0 = time.perf_counter()
        res_fast = replay_engine.replay(
            HistoricalReplayRequest("FAST", "1m", 1000.0, 2000.0, pacing_seconds=0.0)
        )
        elapsed_fast = time.perf_counter() - t0
        assert res_fast.bars_replayed == 4
        assert elapsed_fast < 0.1

        # Paced replay (0.01s per bar -> ~40ms)
        t1 = time.perf_counter()
        res_paced = replay_engine.replay(
            HistoricalReplayRequest("FAST", "1m", 1000.0, 2000.0, pacing_seconds=0.01)
        )
        elapsed_paced = time.perf_counter() - t1
        assert res_paced.bars_replayed == 4
        assert elapsed_paced >= 0.035

    def test_empty_dataset_replay(self):
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="repo")
        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=MarketDataQualityEngine(),
            analysis_engine=TechnicalAnalysisEngine(),
        )

        req = HistoricalReplayRequest("EMPTY", "1m", 1000.0, 2000.0)
        res = replay_engine.replay(req)
        assert res.status == HistoricalReplayStatus.COMPLETED
        assert res.total_bars_available == 0
        assert res.bars_replayed == 0


# ==============================================================================
# 5. Master Orchestrator Command Handlers Tests
# ==============================================================================

class TestHistoricalOrchestratorCommands:
    """Tests for MasterOrchestrator historical data commands."""

    @pytest.fixture
    def setup_orchestrator(self):
        event_bus = EventBus()
        state_store = StateStore()
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)
        repo = SQLiteHistoricalBarRepository(db_path=":memory:", name="orch_repo")
        cache = HistoricalDataCache(max_size=50)
        quality_engine = MarketDataQualityEngine()
        analysis_engine = TechnicalAnalysisEngine(event_bus=event_bus, state_store=state_store)
        provider = MockHistoricalDataProvider()
        loader = HistoricalDataLoader(
            provider=provider,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
            repository=repo,
            cache=cache,
        )
        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
            event_bus=event_bus,
            state_store=state_store,
        )

        register_historical_handlers(
            orchestrator=orchestrator,
            loader=loader,
            repository=repo,
            replay_engine=replay_engine,
        )
        return orchestrator, repo, replay_engine

    def test_store_and_query_via_orchestrator(self, setup_orchestrator):
        orchestrator, repo, _ = setup_orchestrator

        bars_data = [
            {
                "symbol": "RELIANCE",
                "timeframe": "1m",
                "timestamp": 1700000000.0 + (i * 60),
                "open": 2500.0 + i,
                "high": 2505.0 + i,
                "low": 2495.0 + i,
                "close": 2502.0 + i,
                "volume": 100.0,
            }
            for i in range(3)
        ]

        store_req = OrchestratorRequest.create(
            command="store_historical_bars",
            context={"bars": bars_data, "source": "orch_test"},
        )
        store_res = orchestrator.handle_request(store_req)
        assert store_res.status == RequestStatus.COMPLETED
        assert store_res.result["stored_count"] == 3

        query_req = OrchestratorRequest.create(
            command="query_historical_bars",
            context={
                "symbol": "RELIANCE",
                "timeframe": "1m",
                "start_time": 1700000000.0,
                "end_time": 1700000200.0,
            },
        )
        query_res = orchestrator.handle_request(query_req)
        assert query_res.status == RequestStatus.COMPLETED
        assert query_res.result["count"] == 3
        assert len(query_res.result["bars"]) == 3

    def test_replay_via_orchestrator(self, setup_orchestrator):
        orchestrator, repo, _ = setup_orchestrator
        repo.store_bars(_create_sample_bars("INFY", "1m", count=10, start_ts=1700000000.0))

        replay_req = OrchestratorRequest.create(
            command="replay_historical_bars",
            context={
                "symbol": "INFY",
                "timeframe": "1m",
                "start_time": 1700000000.0,
                "end_time": 1700001000.0,
                "pacing_seconds": 0.0,
                "reset_analysis_state": True,
            },
        )
        replay_res = orchestrator.handle_request(replay_req)
        assert replay_res.status == RequestStatus.COMPLETED
        res_data = replay_res.result
        assert res_data["status"] == "success"
        assert res_data["replay_status"] == "COMPLETED"
        assert res_data["bars_replayed"] == 10
        assert res_data["bars_accepted"] == 10


# ==============================================================================
# 6. Safety & Bootstrap Invariants
# ==============================================================================

class TestOfflineSafetyInvariants:
    """Verifies that system initialization adheres strictly to safety rules."""

    def test_bootstrap_offline_and_execution_locked(self):
        status = bootstrap_system()
        assert status.settings.execution_locked is True
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.historical_repository is not None
        assert status.historical_cache is not None
        assert status.historical_replay_engine is not None

        # Verify no automatic replay is running
        assert status.historical_replay_engine.active_replay_count == 0
