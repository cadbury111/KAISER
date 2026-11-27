"""KAISER — Tests for Multi-Stream Synchronization & Unified Market Data Coordinator (Phase 3 Step 89).

Verifies:
1. UnifiedMarketDataEvent validation, timestamp unit normalization (s vs ms vs ns), and immutability.
2. Quote-only, Depth-only, and Trade-only isolated stream ingestion.
3. Full three-stream synchronization and alignment (SyncQuality.FULL).
4. Partial synchronization and explicit missing-stream reporting without data fabrication.
5. Cross-stream skew calculation and max_skew boundary enforcement.
6. Staleness detection (SyncQuality.STALE) for aged streams.
7. Out-of-order stream events and independent per-stream sequence gap tracking.
8. Malformed and invalid timestamp rejection fail-closed.
9. Multiple-symbol isolation without cross-contamination.
10. Bounded ring buffer eviction and overflow resilience.
11. Multi-threaded ingestion and thread safety.
12. Deterministic replay reproducibility.
13. EventBus event publication and StateStore caching.
14. Read-only analytical integration with TechnicalAnalysisEngine and AgentConsensusReport.
15. Non-negotiable execution safety invariants (execution_locked=True, broker_provider="none").
"""

from __future__ import annotations

import concurrent.futures
import copy
from dataclasses import dataclass, FrozenInstanceError
import json
import math
import threading
import time
from types import MappingProxyType
from typing import List

import pytest

from app.agents.consensus.models import AgentConsensusReport, AgentFinding, ConsensusStatus
from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.indicators import extract_synchronized_context_features
from app.core.config import get_settings
from app.core.events import EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.coordinator import (
    MarketStreamType,
    MultiStreamMarketDataCoordinator,
    SyncQuality,
    SynchronizedMarketContext,
    UnifiedMarketDataEvent,
    normalize_timestamp,
)
from app.market.depth_analytics import L2DepthAnalyticsEngine
from app.market.models import AggressorSide, OrderBookLevel, OrderBookSnapshot, Quote, TradeTick
from app.market.trade_flow import TradeFlowEngine


def _make_quote(
    symbol: str = "RELIANCE",
    last_price: float = 2500.0,
    bid: float = 2499.5,
    ask: float = 2500.5,
    timestamp: Optional[float] = None,
) -> Quote:
    ts = timestamp if timestamp is not None else time.time()
    return Quote(
        symbol=symbol,
        exchange="NSE",
        timestamp=ts,
        last_price=last_price,
        bid=bid,
        ask=ask,
        bid_size=100.0,
        ask_size=100.0,
        volume=50000.0,
        source="quote_feed",
    )


def _make_depth(
    symbol: str = "RELIANCE",
    bids: List[tuple[float, float]] = None,
    asks: List[tuple[float, float]] = None,
    timestamp: Optional[float] = None,
    sequence_number: int = 1,
    is_valid: bool = True,
) -> OrderBookSnapshot:
    ts = timestamp if timestamp is not None else time.time()
    bid_levels = tuple(OrderBookLevel(p, q) for p, q in (bids or [(2499.5, 100.0)]))
    ask_levels = tuple(OrderBookLevel(p, q) for p, q in (asks or [(2500.5, 100.0)]))
    return OrderBookSnapshot(
        symbol=symbol,
        exchange="NSE",
        timestamp=ts,
        bids=bid_levels,
        asks=ask_levels,
        sequence_number=sequence_number,
        source="depth_feed",
        is_valid=is_valid,
    )


def _make_trade(
    symbol: str = "RELIANCE",
    price: float = 2500.0,
    volume: float = 50.0,
    aggressor_side: AggressorSide = AggressorSide.BUY_INITIATED,
    trade_id: str = "t1",
    timestamp: Optional[float] = None,
    sequence_number: int = 1,
) -> TradeTick:
    ts = timestamp if timestamp is not None else time.time()
    return TradeTick(
        symbol=symbol,
        price=price,
        volume=volume,
        timestamp=ts,
        received_timestamp=ts + 0.005,
        trade_id=trade_id,
        sequence_number=sequence_number,
        aggressor_side=aggressor_side,
        exchange="NSE",
        source="trade_feed",
    )


class TestUnifiedMarketDataEvent:
    """Tests for UnifiedMarketDataEvent and timestamp normalization."""

    def test_timestamp_normalization_all_four_units_and_fractions(self) -> None:
        """Correctly normalizes seconds, milliseconds, microseconds, and nanoseconds to POSIX seconds."""
        # 1. Seconds (10 digits: ~1.7e9)
        ts_sec = 1700000000.123456
        assert normalize_timestamp(ts_sec) == 1700000000.123456

        # 2. Milliseconds (13 digits: ~1.7e12)
        ts_ms = 1700000000123.456
        assert normalize_timestamp(ts_ms) == 1700000000.123456

        # 3. Microseconds (16 digits: ~1.7e15)
        ts_us = 1700000000123456.0
        assert normalize_timestamp(ts_us) == 1700000000.123456

        # 4. Nanoseconds (19 digits: ~1.7e18)
        ts_ns = 1700000000123456000.0
        assert normalize_timestamp(ts_ns) == 1700000000.123456

    def test_timestamp_normalization_boundaries_and_rejections(self) -> None:
        """Verifies boundary thresholds (1e11, 1e14, 1e17) and invalid input rejection."""
        # Boundary around 1e11 (seconds vs milliseconds)
        assert normalize_timestamp(9.9999e10) == round(9.9999e10, 6)  # Treated as seconds
        assert normalize_timestamp(1.00001e11) == round(1.00001e11 / 1e3, 6)  # Treated as milliseconds

        # Boundary around 1e14 (milliseconds vs microseconds)
        assert normalize_timestamp(9.9999e13) == round(9.9999e13 / 1e3, 6)  # Treated as milliseconds
        assert normalize_timestamp(1.00001e14) == round(1.00001e14 / 1e6, 6)  # Treated as microseconds

        # Boundary around 1e17 (microseconds vs nanoseconds)
        assert normalize_timestamp(9.9999e16) == round(9.9999e16 / 1e6, 6)  # Treated as microseconds
        assert normalize_timestamp(1.00001e17) == round(1.00001e17 / 1e9, 6)  # Treated as nanoseconds

        # Rejection of zero, negative, NaN, infinity, and invalid strings
        with pytest.raises(ValidationError):
            normalize_timestamp(0.0)
        with pytest.raises(ValidationError):
            normalize_timestamp(-100.0)
        with pytest.raises(ValidationError):
            normalize_timestamp(float("nan"))
        with pytest.raises(ValidationError):
            normalize_timestamp(float("inf"))
        with pytest.raises(ValidationError):
            normalize_timestamp(float("-inf"))
        with pytest.raises(ValidationError):
            normalize_timestamp("invalid_timestamp")
        with pytest.raises(ValidationError):
            normalize_timestamp("")

    def test_unified_event_creation_and_latency(self) -> None:
        """Creates unified event, measures receipt latency, and serializes."""
        event = UnifiedMarketDataEvent(
            symbol="infy",
            stream_type=MarketStreamType.QUOTE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.025,
            sequence_number=10,
        )
        assert event.symbol == "INFY"
        assert event.stream_type == MarketStreamType.QUOTE
        assert event.receipt_latency_ms == 25.0
        assert "iso_timestamp" in event.to_dict()

    def test_event_payload_defensive_isolation(self) -> None:
        """Mutating a caller-owned dictionary after event creation cannot alter event payload."""
        mutable_dict = {"side": "BUY", "volume": 100.0, "nested": {"counter": 1}}
        event = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.TRADE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.005,
            payload=mutable_dict,
        )

        # Mutate the caller's original dictionary
        mutable_dict["side"] = "MUTATED"
        mutable_dict["nested"]["counter"] = 999
        mutable_dict["new_injected"] = "HACK"

        # Assert event payload remained isolated
        assert event.payload["side"] == "BUY"
        assert event.payload["nested"]["counter"] == 1
        assert "new_injected" not in event.payload


class TestSingleAndMultiStreamIngestion:
    """Tests for isolated, partial, and full multi-stream synchronization."""

    def test_quote_only_ingestion_is_partial(self) -> None:
        """Quote-only ingestion generates context with PARTIAL quality and missing depth/trade."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        q = _make_quote(timestamp=now)
        ctx = coord.ingest_quote(q, received_timestamp=now + 0.010)

        assert ctx.symbol == "RELIANCE"
        assert ctx.quote is not None
        assert ctx.depth_snapshot is None
        assert ctx.last_trade is None
        assert ctx.missing_streams == ("DEPTH", "TRADE")
        assert ctx.sync_quality == SyncQuality.PARTIAL
        assert ctx.is_synchronized is False
        assert ctx.is_valid is True

    def test_depth_only_ingestion_is_partial(self) -> None:
        """Depth-only ingestion generates context with depth analytics and missing quote/trade."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        d = _make_depth(timestamp=now)
        ctx = coord.ingest_depth(d, received_timestamp=now + 0.010)

        assert ctx.depth_snapshot is not None
        assert ctx.depth_analytics is not None
        assert ctx.quote is None
        assert ctx.last_trade is None
        assert ctx.missing_streams == ("QUOTE", "TRADE")
        assert ctx.sync_quality == SyncQuality.PARTIAL
        assert ctx.is_synchronized is False

    def test_trade_only_ingestion_is_partial(self) -> None:
        """Trade-only ingestion generates context with trade flow and missing quote/depth."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        t = _make_trade(timestamp=now)
        ctx = coord.ingest_trade(t)

        assert ctx.last_trade is not None
        assert ctx.trade_flow is not None
        assert ctx.quote is None
        assert ctx.depth_snapshot is None
        assert ctx.missing_streams == ("QUOTE", "DEPTH")
        assert ctx.sync_quality == SyncQuality.PARTIAL
        assert ctx.is_synchronized is False

    def test_full_three_stream_synchronization(self) -> None:
        """When all three streams are ingested within max_skew, sync_quality is FULL."""
        coord = MultiStreamMarketDataCoordinator(max_cross_stream_skew_seconds=0.5)

        base_ts = time.time()
        q = _make_quote(timestamp=base_ts)
        d = _make_depth(timestamp=base_ts + 0.05)  # 50ms skew
        t = _make_trade(timestamp=base_ts + 0.10)  # 100ms skew

        coord.ingest_quote(q, received_timestamp=base_ts + 0.01)
        coord.ingest_depth(d, received_timestamp=base_ts + 0.06)
        ctx = coord.ingest_trade(t)

        assert ctx.quote is not None
        assert ctx.depth_snapshot is not None
        assert ctx.last_trade is not None
        assert ctx.missing_streams == ()
        assert ctx.sync_quality == SyncQuality.FULL
        assert ctx.is_synchronized is True
        assert ctx.is_valid is True
        # Skews: quote_vs_depth=50ms, quote_vs_trade=100ms, depth_vs_trade=50ms
        assert ctx.max_skew_ms == 100.0
        assert ctx.cross_stream_skew_ms["quote_vs_trade"] == 100.0


class TestSkewStalenessAndDiscontinuity:
    """Tests for skew boundaries, staleness detection, sequence gaps, and out-of-order handling."""

    def test_max_skew_boundary_exceeded_falls_back_to_partial(self) -> None:
        """When cross-stream skew exceeds max_cross_stream_skew_seconds, quality is PARTIAL."""
        # Max skew allowed: 200ms
        coord = MultiStreamMarketDataCoordinator(max_cross_stream_skew_seconds=0.200)

        base_ts = time.time()
        q = _make_quote(timestamp=base_ts)
        d = _make_depth(timestamp=base_ts + 0.05)
        t = _make_trade(timestamp=base_ts + 0.50)  # 500ms skew > 200ms threshold

        coord.ingest_quote(q, received_timestamp=base_ts + 0.01)
        coord.ingest_depth(d, received_timestamp=base_ts + 0.06)
        ctx = coord.ingest_trade(t)

        assert ctx.sync_quality == SyncQuality.PARTIAL
        assert ctx.is_synchronized is False
        assert ctx.max_skew_ms == 500.0

    def test_stale_stream_detection(self) -> None:
        """If stream received timestamp exceeds max_staleness_seconds, quality is STALE."""
        coord = MultiStreamMarketDataCoordinator(max_staleness_seconds=2.0)
        old_time = time.time() - 10.0  # 10 seconds ago

        q = _make_quote(timestamp=old_time)
        ctx = coord.ingest_quote(q, received_timestamp=old_time)

        assert ctx.sync_quality == SyncQuality.STALE
        assert ctx.is_synchronized is False

    def test_crossed_order_book_marks_context_invalid(self) -> None:
        """If incoming L2 book is crossed, context is marked INVALID fail-closed."""
        coord = MultiStreamMarketDataCoordinator()
        # Crossed book: best_bid (2510) > best_ask (2500)
        d = _make_depth(
            bids=[(2510.0, 100.0)],
            asks=[(2500.0, 100.0)],
            is_valid=False,
        )
        ctx = coord.ingest_depth(d)

        assert ctx.sync_quality == SyncQuality.INVALID
        assert ctx.is_valid is False
        assert ctx.error is not None
        assert "crossed" in ctx.error.lower() or "invalid" in ctx.error.lower()

    def test_independent_sequence_gap_and_out_of_order_tracking(self) -> None:
        """Sequence gaps and out-of-order events are tracked independently per stream channel."""
        coord = MultiStreamMarketDataCoordinator()

        # Depth sequence 1, then jump to 5 (gap = 1)
        d1 = _make_depth(sequence_number=1, timestamp=100.0)
        coord.ingest_depth(d1, received_timestamp=100.0)
        d2 = _make_depth(sequence_number=5, timestamp=105.0)
        coord.ingest_depth(d2, received_timestamp=105.0)

        # Trade sequence 1, then out-of-order sequence 0
        t1 = _make_trade(sequence_number=10, timestamp=100.0, trade_id="tr1")
        coord.ingest_trade(t1)
        t2 = _make_trade(sequence_number=8, timestamp=95.0, trade_id="tr2")
        ctx = coord.ingest_trade(t2)

        status = ctx.sequence_status
        assert status["sequence_gaps"]["DEPTH"] == 1
        assert status["out_of_order_counts"]["TRADE"] >= 1

    def test_deterministic_replay_with_custom_clock(self) -> None:
        """Custom clock enables deterministic replay of historical timestamps without false staleness."""
        replay_time = 1700000000.0
        coord = MultiStreamMarketDataCoordinator(
            clock=lambda: replay_time,
            max_staleness_seconds=5.0,
            max_cross_stream_skew_seconds=0.5,
        )

        q = _make_quote(timestamp=replay_time)
        d = _make_depth(timestamp=replay_time + 0.05)
        t = _make_trade(timestamp=replay_time + 0.10)

        coord.ingest_quote(q, received_timestamp=replay_time + 0.01)
        coord.ingest_depth(d, received_timestamp=replay_time + 0.06)
        ctx = coord.ingest_trade(t)

        assert ctx.sync_quality == SyncQuality.FULL
        assert ctx.is_synchronized is True
        assert ctx.max_skew_ms == 100.0


class TestMultiSymbolAndBufferResilience:
    """Tests for multi-symbol isolation, buffer eviction, thread safety, and replay."""

    def test_multi_symbol_isolation(self) -> None:
        """Multiple symbols maintain strictly isolated stream states and contexts."""
        coord = MultiStreamMarketDataCoordinator()

        q_rel = _make_quote(symbol="RELIANCE", last_price=2500.0)
        q_tcs = _make_quote(symbol="TCS", last_price=3500.0)

        coord.ingest_quote(q_rel)
        coord.ingest_quote(q_tcs)

        ctx_rel = coord.get_latest_context("RELIANCE")
        ctx_tcs = coord.get_latest_context("TCS")

        assert ctx_rel is not None
        assert ctx_tcs is not None
        assert ctx_rel.symbol == "RELIANCE"
        assert ctx_rel.quote.last_price == 2500.0
        assert ctx_tcs.symbol == "TCS"
        assert ctx_tcs.quote.last_price == 3500.0

    def test_ring_buffer_eviction_on_overflow(self) -> None:
        """Ring buffers evict oldest items when exceeding max_buffer_size."""
        coord = MultiStreamMarketDataCoordinator(max_buffer_size=5)

        for i in range(10):
            q = _make_quote(timestamp=100.0 + i)
            coord.ingest_quote(q, received_timestamp=100.0 + i)

        status = coord.get_stream_status("RELIANCE")
        assert status["quotes_buffered"] == 5
        assert coord.total_events_processed == 10

    def test_concurrent_ingestion_thread_safety(self) -> None:
        """Concurrent multi-stream ingestion across threads does not corrupt state."""
        coord = MultiStreamMarketDataCoordinator(max_buffer_size=100)

        def ingest_quotes() -> None:
            for i in range(25):
                q = _make_quote(symbol="CONCUR_TEST", timestamp=100.0 + i)
                coord.ingest_quote(q, received_timestamp=100.0 + i)

        def ingest_trades() -> None:
            for i in range(25):
                t = _make_trade(symbol="CONCUR_TEST", trade_id=f"th_{i}", timestamp=100.0 + i)
                coord.ingest_trade(t)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            f1 = executor.submit(ingest_quotes)
            f2 = executor.submit(ingest_trades)
            concurrent.futures.wait([f1, f2])

        assert coord.total_events_processed == 50
        ctx = coord.get_latest_context("CONCUR_TEST")
        assert ctx is not None
        assert ctx.quote is not None
        assert ctx.last_trade is not None

    def test_deterministic_replay(self) -> None:
        """Identical event streams produce identical synchronized contexts."""
        coord1 = MultiStreamMarketDataCoordinator()
        coord2 = MultiStreamMarketDataCoordinator()

        events = [
            _make_quote(timestamp=100.0),
            _make_depth(timestamp=100.05),
            _make_trade(timestamp=100.10, trade_id="r1"),
        ]

        ctx1 = None
        for ev in events:
            if isinstance(ev, Quote):
                ctx1 = coord1.ingest_quote(ev, received_timestamp=ev.timestamp + 0.005)
            elif isinstance(ev, OrderBookSnapshot):
                ctx1 = coord1.ingest_depth(ev, received_timestamp=ev.timestamp + 0.005)
            elif isinstance(ev, TradeTick):
                ctx1 = coord1.ingest_trade(ev)

        ctx2 = None
        for ev in events:
            if isinstance(ev, Quote):
                ctx2 = coord2.ingest_quote(ev, received_timestamp=ev.timestamp + 0.005)
            elif isinstance(ev, OrderBookSnapshot):
                ctx2 = coord2.ingest_depth(ev, received_timestamp=ev.timestamp + 0.005)
            elif isinstance(ev, TradeTick):
                ctx2 = coord2.ingest_trade(ev)

        assert ctx1.sync_quality == ctx2.sync_quality
        assert ctx1.max_skew_ms == ctx2.max_skew_ms
        assert ctx1.is_synchronized == ctx2.is_synchronized


class TestEventBusAndAnalyticalIntegration:
    """Tests for EventBus publication, StateStore caching, and specialist consensus visibility."""

    def test_event_bus_and_statestore_integration(self) -> None:
        """Dispatches MARKET_DATA_SYNCHRONIZED event and updates StateStore."""
        bus = EventBus("test_bus")
        state_store = StateStore()
        published_events = []
        bus.subscribe(
            EventType.MARKET_DATA_SYNCHRONIZED,
            lambda e: published_events.append(e),
        )

        coord = MultiStreamMarketDataCoordinator(event_bus=bus, state_store=state_store)
        q = _make_quote(symbol="BUS_TEST")
        coord.ingest_quote(q)

        assert len(published_events) == 1
        assert published_events[0].event_type == EventType.MARKET_DATA_SYNCHRONIZED
        assert published_events[0].payload["symbol"] == "BUS_TEST"

        stored = state_store.get("synchronized_market_contexts")
        assert stored is not None
        assert "BUS_TEST" in stored

    def test_technical_analysis_engine_and_consensus_visibility(self) -> None:
        """TechnicalAnalysisEngine stores context and AgentConsensusReport exposes it cleanly."""
        engine = TechnicalAnalysisEngine()
        coord = MultiStreamMarketDataCoordinator()

        q = _make_quote(symbol="SYNC_FEATS")
        ctx = coord.ingest_quote(q)
        engine.set_synchronized_context(ctx)

        feats = engine.get_synchronized_context_features("SYNC_FEATS")
        assert feats["context_available"] is True
        assert feats["has_quote"] is True
        assert feats["has_depth"] is False
        assert feats["has_trade"] is False
        assert "is_synchronized" in feats

        # Consensus report visibility
        finding = AgentFinding(
            agent_name="technical_analysis_specialist",
            agent_type="technical_analysis",
            posture="BULLISH",
            facts={
                "synchronized_context": feats,
            },
            is_valid=True,
        )

        report = AgentConsensusReport(
            consensus_id="cons_sync_test",
            symbol="SYNC_FEATS",
            timestamp=1700000000.0,
            iso_timestamp="2023-11-14T22:13:20+00:00",
            status=ConsensusStatus.AGREEMENT,
            synthesized_posture="BULLISH",
            participating_agents=["technical_analysis_specialist"],
            agent_findings={"technical_analysis_specialist": finding},
            agreement_points=["Consensus reached."],
            disagreement_points=[],
            limitations=[],
            voice_summary="Specialist consensus reached.",
        )

        assert report.synchronized_market_context is not None
        assert report.synchronized_market_context["has_quote"] is True
        assert "synchronized_context" in report.microstructure_context


class TestStep90ImmutabilityAndDefensiveAccess:
    """Verifies Step 90 hardening for deep immutability and defensive isolation."""

    def test_context_mappings_are_deeply_immutable_mappingproxy(self) -> None:
        """Context mapping attributes are wrapped in MappingProxyType recursively."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        coord.ingest_quote(_make_quote(timestamp=now), received_timestamp=now)
        coord.ingest_depth(_make_depth(timestamp=now), received_timestamp=now)
        ctx = coord.ingest_trade(_make_trade(timestamp=now))

        assert isinstance(ctx.stream_timestamps, MappingProxyType)
        assert isinstance(ctx.cross_stream_skew_ms, MappingProxyType)
        assert isinstance(ctx.sequence_status, MappingProxyType)
        assert isinstance(ctx.sequence_status["sequence_gaps"], MappingProxyType)
        assert isinstance(ctx.sequence_status["last_sequences"], MappingProxyType)
        assert isinstance(ctx.sequence_status["out_of_order_counts"], MappingProxyType)
        assert isinstance(ctx.missing_streams, tuple)

    def test_top_level_mapping_mutation_rejected(self) -> None:
        """Top-level mapping modification or deletion raises TypeError."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        ctx = coord.ingest_quote(_make_quote(timestamp=now), received_timestamp=now)

        with pytest.raises(TypeError):
            ctx.stream_timestamps["QUOTE"] = 999999.0

        with pytest.raises(TypeError):
            del ctx.stream_timestamps["QUOTE"]

        with pytest.raises(TypeError):
            ctx.cross_stream_skew_ms["quote_vs_depth"] = 0.0

    def test_nested_mapping_mutation_rejected(self) -> None:
        """Nested mapping mutation within sequence_status raises TypeError."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        ctx = coord.ingest_depth(_make_depth(timestamp=now), received_timestamp=now)

        with pytest.raises(TypeError):
            ctx.sequence_status["sequence_gaps"]["DEPTH"] = 999

        with pytest.raises(TypeError):
            del ctx.sequence_status["sequence_gaps"]["DEPTH"]

        with pytest.raises(TypeError):
            ctx.sequence_status["last_sequences"]["QUOTE"] = 999

    def test_frozen_context_attribute_mutation_rejected(self) -> None:
        """Direct attribute reassignment on SynchronizedMarketContext raises FrozenInstanceError."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        ctx = coord.ingest_quote(_make_quote(timestamp=now), received_timestamp=now)

        with pytest.raises(FrozenInstanceError):
            ctx.symbol = "MUTATED"

        with pytest.raises(FrozenInstanceError):
            ctx.max_skew_ms = 999.0

        with pytest.raises(FrozenInstanceError):
            ctx.is_synchronized = True

    def test_cached_context_and_history_isolated_from_caller(self) -> None:
        """Caller mutating query results cannot alter internal coordinator state or history."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        coord.ingest_depth(_make_depth(timestamp=now, sequence_number=1), received_timestamp=now)
        coord.ingest_depth(_make_depth(timestamp=now, sequence_number=5), received_timestamp=now)

        ctx1 = coord.get_latest_context("RELIANCE")
        hist = coord.get_context_history("RELIANCE")
        status_before = coord.get_stream_status("RELIANCE")

        # Mutate the dictionary returned by get_stream_status
        status_before["sequence_status"]["sequence_gaps"]["DEPTH"] = 999

        # Subsequent query must reflect unmodified state
        status_after = coord.get_stream_status("RELIANCE")
        assert status_after["sequence_status"]["sequence_gaps"]["DEPTH"] == 1
        assert ctx1.sequence_status["sequence_gaps"]["DEPTH"] == 1
        assert hist[-1].sequence_status["sequence_gaps"]["DEPTH"] == 1

    def test_agent_consensus_report_defensive_isolation(self) -> None:
        """Mutating feature dictionaries returned by AgentConsensusReport does not alter report state."""
        finding = AgentFinding(
            agent_name="technical_analysis_specialist",
            agent_type="technical_analysis",
            posture="BULLISH",
            facts={
                "depth": {"imbalance_ratio": 0.45, "micro_price": 2500.5},
                "trade_flow": {"tfi": 0.30, "volume": 1000.0},
                "synchronized_context": {"has_quote": True, "sync_quality": "FULL"},
            },
            is_valid=True,
        )

        report = AgentConsensusReport(
            consensus_id="cons_defensive_test",
            symbol="ISOLATION_TEST",
            timestamp=1700000000.0,
            iso_timestamp="2023-11-14T22:13:20+00:00",
            status=ConsensusStatus.AGREEMENT,
            synthesized_posture="BULLISH",
            participating_agents=["technical_analysis_specialist"],
            agent_findings={"technical_analysis_specialist": finding},
            agreement_points=["Consensus reached."],
            disagreement_points=[],
            limitations=[],
            voice_summary="Specialist consensus reached.",
        )

        # Mutate synchronized_market_context copy
        sync_feats = report.synchronized_market_context
        sync_feats["has_quote"] = False
        sync_feats["injected_key"] = "MALICIOUS"
        assert report.synchronized_market_context["has_quote"] is True
        assert "injected_key" not in report.synchronized_market_context

        # Mutate depth_features copy
        depth_feats = report.depth_features
        depth_feats["imbalance_ratio"] = 999.0
        assert report.depth_features["imbalance_ratio"] == 0.45

        # Mutate trade_flow_features copy
        tf_feats = report.trade_flow_features
        tf_feats["tfi"] = -999.0
        assert report.trade_flow_features["tfi"] == 0.30

        # Assert microstructure_context is also isolated
        assert report.microstructure_context["depth"]["imbalance_ratio"] == 0.45


class TestStep90BHardenedIsolationAndReconstruction:
    """Verifies Step 90B MappingProxyType deepcopy compatibility and advanced payload isolation."""

    def test_frozen_context_reconstruction_and_nested_mappingproxy_input(self) -> None:
        """Context can be reconstructed from an existing frozen context without MappingProxyType pickling errors."""
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        coord.ingest_depth(_make_depth(timestamp=now, sequence_number=1), received_timestamp=now)
        ctx1 = coord.get_latest_context("RELIANCE")
        assert ctx1 is not None
        assert isinstance(ctx1.sequence_status, MappingProxyType)
        assert isinstance(ctx1.sequence_status["sequence_gaps"], MappingProxyType)

        # Reconstruct new context using ctx1's sequence_status and stream_timestamps
        ctx2 = SynchronizedMarketContext(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=now + 1.0,
            sequence_status=ctx1.sequence_status,
            stream_timestamps=ctx1.stream_timestamps,
        )

        assert ctx2.symbol == "RELIANCE"
        assert isinstance(ctx2.sequence_status, MappingProxyType)
        assert isinstance(ctx2.sequence_status["sequence_gaps"], MappingProxyType)
        assert ctx2.sequence_status["sequence_gaps"]["DEPTH"] == 0

        # Verify mutation still fails on reconstructed context
        with pytest.raises(TypeError):
            ctx2.sequence_status["sequence_gaps"]["DEPTH"] = 999

    def test_tuple_wrapped_mutable_payload_isolated(self) -> None:
        """Tuples containing mutable collections are defensively isolated from caller mutation."""
        t_dict = (1, {"key": "original", "list": [10, 20]})
        ev = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.QUOTE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.01,
            payload=t_dict,
        )

        # Mutate the caller's original dictionary inside tuple
        t_dict[1]["key"] = "MUTATED"
        t_dict[1]["list"].append(30)

        # Assert event's stored payload remained isolated
        assert ev.payload[1]["key"] == "original"
        assert ev.payload[1]["list"] == [10, 20]

    def test_nested_tuple_list_dict_combination_payload_isolated(self) -> None:
        """Complex combinations of tuples, lists, and dicts are defensively isolated."""
        combo = [10, (20, {"nested": [30, {"deep": "initial"}]})]
        ev = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.TRADE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.01,
            payload=combo,
        )

        # Mutate deeply nested dictionary
        combo[1][1]["nested"][1]["deep"] = "MUTATED"
        combo[0] = 999

        assert ev.payload[0] == 10
        assert ev.payload[1][1]["nested"][1]["deep"] == "initial"

    def test_custom_mutable_object_payload_isolated(self) -> None:
        """Custom mutable objects are defensively deep-copied upon event creation."""
        class CustomOrderData:
            def __init__(self, val: int) -> None:
                self.val = val

        obj = CustomOrderData(42)
        ev = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.TRADE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.01,
            payload=obj,
        )

        # Mutate original instance
        obj.val = 999
        assert ev.payload.val == 42

    def test_frozen_dataclass_with_mutable_collection_isolated(self) -> None:
        """Frozen dataclasses holding mutable collections are defensively deep-copied."""
        @dataclass(frozen=True)
        class FrozenContainer:
            meta: dict

        fwd = FrozenContainer(meta={"tags": ["fast", "valid"]})
        ev = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.DEPTH,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.01,
            payload=fwd,
        )

        # Mutate dictionary inside frozen dataclass
        fwd.meta["tags"].append("HACKED")
        assert ev.payload.meta["tags"] == ["fast", "valid"]

    def test_sets_and_frozensets_payload_isolated(self) -> None:
        """Sets and frozensets are defensively isolated and preserved."""
        s = {1, 2, 3}
        fs = frozenset(["a", "b"])

        ev1 = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.QUOTE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.01,
            payload=s,
        )
        ev2 = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.QUOTE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.01,
            payload=fs,
        )

        s.add(4)
        assert ev1.payload == {1, 2, 3}
        assert ev2.payload == frozenset(["a", "b"])

    def test_uncopyable_unsupported_payload_rejected_fail_closed(self) -> None:
        """Uncopyable objects (e.g. locks or raw thread objects) raise ValidationError."""
        lock = threading.Lock()
        with pytest.raises(ValidationError):
            UnifiedMarketDataEvent(
                symbol="RELIANCE",
                stream_type=MarketStreamType.QUOTE,
                exchange_timestamp=1700000000.0,
                received_timestamp=1700000000.01,
                payload=lock,
            )

    def test_safe_serialization_and_typed_models_compatibility(self) -> None:
        """Events and contexts with various payloads serialize safely to valid JSON."""
        # 1. Typed model event
        q = _make_quote()
        ev_q = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.QUOTE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.01,
            payload=q,
        )
        json_q = json.dumps(ev_q.to_dict())
        assert "last_price" in json_q

        # 2. Complex collection payload event
        complex_payload = {"set_data": {1, 2}, "tuple_data": (3, 4)}
        ev_c = UnifiedMarketDataEvent(
            symbol="RELIANCE",
            stream_type=MarketStreamType.TRADE,
            exchange_timestamp=1700000000.0,
            received_timestamp=1700000000.01,
            payload=complex_payload,
        )
        json_c = json.dumps(ev_c.to_dict())
        assert "set_data" in json_c

        # 3. SynchronizedMarketContext serialization
        coord = MultiStreamMarketDataCoordinator()
        now = time.time()
        coord.ingest_quote(_make_quote(timestamp=now), received_timestamp=now)
        ctx = coord.get_latest_context("RELIANCE")
        json_ctx = json.dumps(ctx.to_dict())
        assert "RELIANCE" in json_ctx


class TestExecutionSafetyInvariants:
    """Verifies that Step 89 strictly preserves execution locks and broker isolation."""

    def test_safety_invariants_strictly_enforced(self) -> None:
        """Confirm execution_locked=True, broker_provider='none', paper_execution_enabled=False."""
        settings = get_settings()
        assert settings.execution_locked is True, "execution_locked must remain strictly True"
        assert settings.broker_provider == "none", "broker_provider must remain strictly 'none'"
        assert settings.paper_execution_enabled is False, "paper_execution_enabled must remain strictly False"
