"""KAISER — Multi-Stream Synchronization & Unified Market Data Coordinator.

Correlates:
1. Level 1 Quotes (`Quote`).
2. Level 2 Order Book Depth Snapshots (`OrderBookSnapshot`).
3. Executed Trade Ticks (`TradeTick`).

Features:
- UnifiedMarketDataEvent: Normalized event container preserving source timestamps,
  normalizing seconds vs. milliseconds units, and guarding against malformed inputs.
- Cross-Stream Time Alignment: Evaluates cross-stream skew (quote vs. depth vs. trade),
  detects stale streams, tracks sequence gaps per stream, and handles missing streams
  without fabricating dummy data.
- SynchronizedMarketContext: Immutable point-in-time multi-stream context with
  synchronization quality tiers (FULL, PARTIAL, STALE, INVALID).
- MultiStreamMarketDataCoordinator: Thread-safe, non-blocking per-symbol coordinator
  supporting deterministic replay, EventBus dispatch, and StateStore caching.

STRICT INVARIANTS:
- Analytical only: Zero trade recommendations, order placement, or execution fields.
- Non-fabricating: Missing streams are flagged as missing, never synthesized.
- Mathematical safety: Cross-stream skews bounded and finite, zero division protected.
"""

from __future__ import annotations

import collections
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple, Union
import uuid

from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.market.depth_analytics import L2DepthAnalyticsEngine, MarketDepthAnalytics, compute_depth_analytics
from app.market.models import OrderBookSnapshot, Quote, TradeTick
from app.market.trade_flow import TradeFlowAnalytics, TradeFlowEngine

logger = get_logger("market.coordinator")


class MarketStreamType(str, Enum):
    """Supported market data streaming channel types."""

    QUOTE = "QUOTE"
    DEPTH = "DEPTH"
    TRADE = "TRADE"


class SyncQuality(str, Enum):
    """Categorical classification of cross-stream synchronization quality."""

    FULL = "FULL"          # All 3 streams present, valid, and skew <= max_skew
    PARTIAL = "PARTIAL"    # 1 or 2 streams present, or skew > max_skew but not stale
    STALE = "STALE"        # At least one active stream exceeds max staleness
    INVALID = "INVALID"    # Crossed order book or malformed stream data


def _freeze_mapping(obj: Any) -> Any:
    """Recursively freeze dicts to MappingProxyType and lists/sets to tuples/frozensets."""
    if isinstance(obj, (dict, MappingProxyType)):
        return MappingProxyType({k: _freeze_mapping(v) for k, v in obj.items()})
    elif isinstance(obj, (list, tuple)):
        return tuple(_freeze_mapping(v) for v in obj)
    elif isinstance(obj, (set, frozenset)):
        return frozenset(_freeze_mapping(v) for v in obj)
    return obj


def _unfreeze_mapping(obj: Any) -> Any:
    """Recursively convert MappingProxyType back to standard dicts and tuples/sets to lists for JSON serialization."""
    if isinstance(obj, (dict, MappingProxyType)):
        return {k: _unfreeze_mapping(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_unfreeze_mapping(v) for v in obj]
    elif isinstance(obj, (set, frozenset)):
        return [_unfreeze_mapping(v) for v in obj]
    return obj


IMMUTABLE_DOMAIN_MODELS = (
    Quote,
    OrderBookSnapshot,
    TradeTick,
    MarketDepthAnalytics,
    TradeFlowAnalytics,
)


def _isolate_payload(payload: Any) -> Any:
    """Defensively isolate event payload from caller-owned mutable references."""
    if payload is None or isinstance(payload, (int, float, str, bool, bytes)):
        return payload
    if isinstance(payload, IMMUTABLE_DOMAIN_MODELS):
        return payload

    try:
        if isinstance(payload, (dict, MappingProxyType, list, tuple, set, frozenset)):
            def _clean(obj: Any) -> Any:
                if isinstance(obj, (dict, MappingProxyType)):
                    return {k: _clean(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [_clean(v) for v in obj]
                elif isinstance(obj, tuple):
                    return tuple(_clean(v) for v in obj)
                elif isinstance(obj, set):
                    return {_clean(v) for v in obj}
                elif isinstance(obj, frozenset):
                    return frozenset(_clean(v) for v in obj)
                return copy.deepcopy(obj)
            return _clean(payload)
        else:
            return copy.deepcopy(payload)
    except Exception as err:
        raise ValidationError(f"Payload cannot be safely isolated: {err}") from err


def normalize_timestamp(ts: Union[float, int, str]) -> float:
    """Safely parse and normalize timestamp to POSIX UTC seconds.

    Handles seconds, milliseconds, microseconds, and nanoseconds consistently:
    - If ts > 1e17: interpreted as nanoseconds (~1.7e18) -> ts / 1e9
    - If ts > 1e14: interpreted as microseconds (~1.7e15) -> ts / 1e6
    - If ts > 1e11: interpreted as milliseconds (~1.7e12) -> ts / 1e3
    - Otherwise: interpreted as seconds (~1.7e9)

    Preserves fractional seconds up to microsecond precision (6 decimal places).

    Raises:
        ValidationError: If timestamp is non-finite, negative, zero, or unparseable.
    """
    try:
        val = float(ts)
    except (ValueError, TypeError) as err:
        raise ValidationError(f"Invalid timestamp value: {ts}") from err

    if not math.isfinite(val) or val <= 0:
        raise ValidationError(f"Timestamp must be a positive finite float, got {val}")

    if val > 1e17:
        norm_val = val / 1e9
    elif val > 1e14:
        norm_val = val / 1e6
    elif val > 1e11:
        norm_val = val / 1000.0
    else:
        norm_val = val

    return round(norm_val, 6)


@dataclass(frozen=True)
class UnifiedMarketDataEvent:
    """Immutable normalized market data event container across all stream channels.

    Attributes:
        symbol: Ticker symbol.
        stream_type: QUOTE, DEPTH, or TRADE.
        exchange_timestamp: Normalized exchange execution POSIX UTC timestamp in seconds.
        received_timestamp: Normalized local receipt POSIX UTC timestamp in seconds.
        sequence_number: Optional monotonic sequence number for gap detection.
        source: Upstream feed identifier.
        event_id: Unique event identifier.
        payload: Underlying domain model (Quote, OrderBookSnapshot, TradeTick, or dict).
        raw_exchange_timestamp: Original unparsed exchange timestamp.
        raw_received_timestamp: Original unparsed receipt timestamp.
    """

    symbol: str
    stream_type: MarketStreamType
    exchange_timestamp: float
    received_timestamp: float
    sequence_number: Optional[int] = None
    source: str = "market_feed"
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    payload: Any = None
    raw_exchange_timestamp: Optional[Any] = None
    raw_received_timestamp: Optional[Any] = None

    def __post_init__(self) -> None:
        """Validate event invariants fail-closed."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("UnifiedMarketDataEvent symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if isinstance(self.stream_type, str):
            clean_type = self.stream_type.strip().upper()
            if clean_type not in ("QUOTE", "DEPTH", "TRADE"):
                raise ValidationError(f"Invalid stream_type: '{self.stream_type}'. Must be QUOTE, DEPTH, or TRADE.")
            object.__setattr__(self, "stream_type", MarketStreamType(clean_type))
        elif not isinstance(self.stream_type, MarketStreamType):
            raise ValidationError(f"stream_type must be MarketStreamType, got {type(self.stream_type)}")

        norm_exch = normalize_timestamp(self.exchange_timestamp)
        norm_recv = normalize_timestamp(self.received_timestamp)
        object.__setattr__(self, "exchange_timestamp", norm_exch)
        object.__setattr__(self, "received_timestamp", norm_recv)

        if self.sequence_number is not None:
            if not isinstance(self.sequence_number, int) or self.sequence_number < 0:
                raise ValidationError(
                    f"sequence_number must be a non-negative integer, got {self.sequence_number}"
                )

        if self.payload is not None:
            object.__setattr__(self, "payload", _isolate_payload(self.payload))

    @property
    def receipt_latency_ms(self) -> float:
        """Measurement of transmission latency in milliseconds."""
        return round((self.received_timestamp - self.exchange_timestamp) * 1000.0, 2)

    @property
    def iso_timestamp(self) -> str:
        """ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.exchange_timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a JSON-serializable dictionary."""
        if hasattr(self.payload, "to_dict"):
            payload_dict = self.payload.to_dict()
        elif isinstance(self.payload, (dict, MappingProxyType, list, tuple, set, frozenset)):
            payload_dict = _unfreeze_mapping(self.payload)
        else:
            payload_dict = self.payload
        return {
            "symbol": self.symbol,
            "stream_type": self.stream_type.value,
            "event_id": self.event_id,
            "exchange_timestamp": self.exchange_timestamp,
            "received_timestamp": self.received_timestamp,
            "receipt_latency_ms": self.receipt_latency_ms,
            "iso_timestamp": self.iso_timestamp,
            "sequence_number": self.sequence_number,
            "source": self.source,
            "payload": payload_dict,
        }


@dataclass(frozen=True)
class SynchronizedMarketContext:
    """Immutable point-in-time synchronized multi-stream market context for a symbol.

    Attributes:
        symbol: Ticker symbol.
        exchange: Primary exchange identifier.
        timestamp: Context reference timestamp (latest stream timestamp).
        quote: Latest valid Level 1 Quote if present.
        depth_snapshot: Latest valid Level 2 OrderBookSnapshot if present.
        depth_analytics: Evaluated Level 2 depth analytics if available.
        trade_flow: Evaluated trade flow analytics if available.
        last_trade: Latest executed TradeTick if present.
        stream_timestamps: Mapping of stream_type -> latest exchange timestamp.
        cross_stream_skew_ms: Pairwise timestamp skews in milliseconds.
        max_skew_ms: Maximum pairwise skew across present streams in milliseconds.
        sequence_status: Sequence numbers and gap counts per stream.
        missing_streams: Streams missing from this context.
        sync_quality: FULL, PARTIAL, STALE, or INVALID.
        is_synchronized: True if all streams are present and within max skew.
        is_valid: False if crossed book or corrupt data detected.
        generation_timestamp: POSIX UTC generation timestamp.
        error: Diagnostic error message if any.
    """

    symbol: str
    exchange: str
    timestamp: float
    quote: Optional[Quote] = None
    depth_snapshot: Optional[OrderBookSnapshot] = None
    depth_analytics: Optional[MarketDepthAnalytics] = None
    trade_flow: Optional[TradeFlowAnalytics] = None
    last_trade: Optional[TradeTick] = None
    stream_timestamps: Mapping[str, float] = field(default_factory=dict)
    cross_stream_skew_ms: Mapping[str, float] = field(default_factory=dict)
    max_skew_ms: float = 0.0
    sequence_status: Mapping[str, Any] = field(default_factory=dict)
    missing_streams: Tuple[str, ...] = ()
    sync_quality: SyncQuality = SyncQuality.PARTIAL
    is_synchronized: bool = False
    is_valid: bool = True
    generation_timestamp: float = field(default_factory=time.time)
    error: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate context fields and freeze mappings."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("SynchronizedMarketContext symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        object.__setattr__(
            self,
            "stream_timestamps",
            _freeze_mapping(_unfreeze_mapping(self.stream_timestamps or {})),
        )
        object.__setattr__(
            self,
            "cross_stream_skew_ms",
            _freeze_mapping(_unfreeze_mapping(self.cross_stream_skew_ms or {})),
        )
        object.__setattr__(
            self,
            "sequence_status",
            _freeze_mapping(_unfreeze_mapping(self.sequence_status or {})),
        )
        object.__setattr__(self, "missing_streams", tuple(self.missing_streams or ()))

    @property
    def iso_timestamp(self) -> str:
        """ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a JSON-serializable dictionary with safe keys (no prohibited recommendation words)."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "quote": self.quote.to_dict() if self.quote else None,
            "depth_snapshot": self.depth_snapshot.to_dict() if self.depth_snapshot else None,
            "depth_analytics": self.depth_analytics.to_dict() if self.depth_analytics else None,
            "trade_flow": self.trade_flow.to_dict() if self.trade_flow else None,
            "last_trade": self.last_trade.to_dict() if self.last_trade else None,
            "stream_timestamps": _unfreeze_mapping(self.stream_timestamps),
            "cross_stream_skew_ms": _unfreeze_mapping(self.cross_stream_skew_ms),
            "max_skew_ms": self.max_skew_ms,
            "sequence_status": _unfreeze_mapping(self.sequence_status),
            "missing_streams": list(self.missing_streams),
            "sync_quality": self.sync_quality.value,
            "is_synchronized": self.is_synchronized,
            "is_valid": self.is_valid,
            "generation_timestamp": self.generation_timestamp,
            "error": self.error,
        }


# =============================================================================
# Stateful Multi-Stream Coordinator
# =============================================================================

class _SymbolStreamState:
    """Internal mutable stream state container for a single symbol under coordinator lock."""

    def __init__(self, symbol: str, buffer_size: int = 100) -> None:
        self.symbol = symbol
        self.buffer_size = max(1, buffer_size)
        self.exchange = "NSE"

        # Latest domain models
        self.latest_quote: Optional[Quote] = None
        self.latest_depth_snapshot: Optional[OrderBookSnapshot] = None
        self.latest_depth_analytics: Optional[MarketDepthAnalytics] = None
        self.latest_trade: Optional[TradeTick] = None
        self.latest_trade_flow: Optional[TradeFlowAnalytics] = None

        # Timestamps
        self.stream_timestamps: Dict[str, float] = {}
        self.stream_received_timestamps: Dict[str, float] = {}

        # Sequences & gaps
        self.last_sequences: Dict[str, int] = {}
        self.sequence_gaps: Dict[str, int] = {"QUOTE": 0, "DEPTH": 0, "TRADE": 0}
        self.out_of_order_counts: Dict[str, int] = {"QUOTE": 0, "DEPTH": 0, "TRADE": 0}

        # Bounded event ring buffers
        self.quote_buffer: Deque[UnifiedMarketDataEvent] = collections.deque(maxlen=self.buffer_size)
        self.depth_buffer: Deque[UnifiedMarketDataEvent] = collections.deque(maxlen=self.buffer_size)
        self.trade_buffer: Deque[UnifiedMarketDataEvent] = collections.deque(maxlen=self.buffer_size)

        # Context history
        self.context_history: Deque[SynchronizedMarketContext] = collections.deque(maxlen=self.buffer_size)


class MultiStreamMarketDataCoordinator:
    """Thread-safe, deterministic, read-only multi-stream market data coordinator.

    Correlates Quote, Depth, and Trade channels per symbol, evaluates cross-stream
    skew, detects sequence gaps and staleness, and emits immutable SynchronizedMarketContext
    models to analytical layers and the EventBus.
    """

    def __init__(
        self,
        max_cross_stream_skew_seconds: float = 1.0,
        max_staleness_seconds: float = 10.0,
        max_buffer_size: int = 100,
        depth_engine: Optional[L2DepthAnalyticsEngine] = None,
        trade_flow_engine: Optional[TradeFlowEngine] = None,
        event_bus: Optional[Any] = None,
        state_store: Optional[Any] = None,
        clock: Optional[Callable[[], float]] = None,
        name: str = "multi_stream_coordinator",
    ) -> None:
        self.name = name
        if max_cross_stream_skew_seconds <= 0:
            raise ValidationError(f"max_cross_stream_skew_seconds must be positive, got {max_cross_stream_skew_seconds}")
        if max_staleness_seconds <= 0:
            raise ValidationError(f"max_staleness_seconds must be positive, got {max_staleness_seconds}")
        if max_buffer_size < 1:
            raise ValidationError(f"max_buffer_size must be at least 1, got {max_buffer_size}")

        self.max_cross_stream_skew_seconds = max_cross_stream_skew_seconds
        self.max_staleness_seconds = max_staleness_seconds
        self.max_buffer_size = max_buffer_size
        self.clock = clock or time.time

        self.depth_engine = depth_engine or L2DepthAnalyticsEngine()
        self.trade_flow_engine = trade_flow_engine or TradeFlowEngine()
        self.event_bus = event_bus
        self.state_store = state_store

        self._lock = threading.RLock()
        self._states: Dict[str, _SymbolStreamState] = {}
        self._latest_contexts: Dict[str, SynchronizedMarketContext] = {}
        self._total_events_processed = 0

    @property
    def total_events_processed(self) -> int:
        """Total valid events processed across all streams."""
        with self._lock:
            return self._total_events_processed

    def _get_or_create_state(self, symbol: str) -> _SymbolStreamState:
        """Retrieve or allocate per-symbol state under lock."""
        sym = symbol.strip().upper()
        if sym not in self._states:
            self._states[sym] = _SymbolStreamState(symbol=sym, buffer_size=self.max_buffer_size)
        return self._states[sym]

    def ingest_quote(
        self,
        quote: Quote,
        received_timestamp: Optional[float] = None,
        sequence_number: Optional[int] = None,
        source: str = "quote_feed",
    ) -> SynchronizedMarketContext:
        """Ingest a Level 1 Quote record."""
        now = self.clock()
        recv_ts = received_timestamp if (received_timestamp is not None and received_timestamp > 0) else now
        event = UnifiedMarketDataEvent(
            symbol=quote.symbol,
            stream_type=MarketStreamType.QUOTE,
            exchange_timestamp=quote.timestamp,
            received_timestamp=recv_ts,
            sequence_number=sequence_number,
            source=source or quote.source,
            payload=quote,
            raw_exchange_timestamp=quote.timestamp,
            raw_received_timestamp=recv_ts,
        )
        return self.ingest_event(event)

    def ingest_depth(
        self,
        snapshot: OrderBookSnapshot,
        received_timestamp: Optional[float] = None,
        source: str = "depth_feed",
    ) -> SynchronizedMarketContext:
        """Ingest a Level 2 OrderBookSnapshot."""
        now = self.clock()
        recv_ts = received_timestamp if (received_timestamp is not None and received_timestamp > 0) else now
        event = UnifiedMarketDataEvent(
            symbol=snapshot.symbol,
            stream_type=MarketStreamType.DEPTH,
            exchange_timestamp=snapshot.timestamp,
            received_timestamp=recv_ts,
            sequence_number=snapshot.sequence_number,
            source=source or snapshot.source,
            payload=snapshot,
            raw_exchange_timestamp=snapshot.timestamp,
            raw_received_timestamp=recv_ts,
        )
        return self.ingest_event(event)

    def ingest_trade(
        self,
        tick: TradeTick,
        source: str = "trade_feed",
    ) -> SynchronizedMarketContext:
        """Ingest an executed TradeTick."""
        event = UnifiedMarketDataEvent(
            symbol=tick.symbol,
            stream_type=MarketStreamType.TRADE,
            exchange_timestamp=tick.timestamp,
            received_timestamp=tick.received_timestamp,
            sequence_number=tick.sequence_number,
            source=source or tick.source,
            payload=tick,
            raw_exchange_timestamp=tick.timestamp,
            raw_received_timestamp=tick.received_timestamp,
        )
        return self.ingest_event(event)

    def ingest_event(self, event: UnifiedMarketDataEvent) -> SynchronizedMarketContext:
        """Ingest a UnifiedMarketDataEvent deterministically and update synchronized context.

        Non-blocking: Never blocks waiting for other streams.

        Args:
            event: Validated UnifiedMarketDataEvent.

        Returns:
            SynchronizedMarketContext: Newly evaluated synchronized market context.
        """
        with self._lock:
            state = self._get_or_create_state(event.symbol)
            st_type = event.stream_type.value

            # 1. Update Sequence & Out-of-Order Tracking
            if event.sequence_number is not None:
                last_seq = state.last_sequences.get(st_type)
                if last_seq is not None:
                    expected_seq = last_seq + 1
                    if event.sequence_number > expected_seq:
                        state.sequence_gaps[st_type] += 1
                        logger.warning(
                            "Sequence gap on %s stream for %s: expected %d, got %d",
                            st_type,
                            event.symbol,
                            expected_seq,
                            event.sequence_number,
                        )
                    elif event.sequence_number < last_seq:
                        state.out_of_order_counts[st_type] += 1
                state.last_sequences[st_type] = event.sequence_number

            # Check timestamp regression
            last_ts = state.stream_timestamps.get(st_type, 0.0)
            if last_ts > 0 and event.exchange_timestamp < last_ts:
                state.out_of_order_counts[st_type] += 1

            state.stream_timestamps[st_type] = max(last_ts, event.exchange_timestamp)
            state.stream_received_timestamps[st_type] = event.received_timestamp

            # 2. Update Channel-Specific Models
            if event.stream_type == MarketStreamType.QUOTE:
                state.quote_buffer.append(event)
                if isinstance(event.payload, Quote):
                    state.latest_quote = event.payload
                    state.exchange = event.payload.exchange

            elif event.stream_type == MarketStreamType.DEPTH:
                state.depth_buffer.append(event)
                if isinstance(event.payload, OrderBookSnapshot):
                    state.latest_depth_snapshot = event.payload
                    state.exchange = event.payload.exchange
                    # Compute depth analytics via attached depth engine
                    state.latest_depth_analytics = self.depth_engine.evaluate_snapshot(event.payload)

            elif event.stream_type == MarketStreamType.TRADE:
                state.trade_buffer.append(event)
                if isinstance(event.payload, TradeTick):
                    state.latest_trade = event.payload
                    state.exchange = event.payload.exchange
                    # Process tick through attached trade flow engine
                    state.latest_trade_flow = self.trade_flow_engine.process_tick(event.payload)

            # 3. Synchronize Cross-Stream Context
            context = self._build_synchronized_context(state)
            self._latest_contexts[event.symbol] = context
            state.context_history.append(context)
            self._total_events_processed += 1

            # 4. Dispatch EventBus & StateStore
            if self.event_bus is not None:
                try:
                    from app.core.events import Event, EventType
                    evt_type = getattr(EventType, "MARKET_DATA_SYNCHRONIZED", "MARKET_DATA_SYNCHRONIZED")
                    ev = Event.create(
                        event_type=evt_type,
                        payload=context.to_dict(),
                        source=self.name,
                    )
                    self.event_bus.publish(ev)
                except Exception as err:
                    logger.error("Failed to publish synchronized market context event: %s", err)

            if self.state_store is not None:
                try:
                    self.state_store.update(
                        "synchronized_market_contexts",
                        {event.symbol: context.to_dict()},
                        default={},
                    )
                except Exception as err:
                    logger.error("Failed to update StateStore with synchronized context: %s", err)

            return context

    def _build_synchronized_context(self, state: _SymbolStreamState) -> SynchronizedMarketContext:
        """Internal helper to calculate cross-stream skew and synchronization quality."""
        # Detect missing streams
        missing: List[str] = []
        if state.latest_quote is None:
            missing.append("QUOTE")
        if state.latest_depth_snapshot is None:
            missing.append("DEPTH")
        if state.latest_trade is None:
            missing.append("TRADE")

        # Context reference timestamp (maximum available exchange timestamp)
        present_ts = list(state.stream_timestamps.values())
        context_ts = max(present_ts) if present_ts else time.time()

        # Pairwise cross-stream skew calculations
        skews_ms: Dict[str, float] = {}
        max_skew_ms = 0.0

        ts_q = state.stream_timestamps.get("QUOTE")
        ts_d = state.stream_timestamps.get("DEPTH")
        ts_t = state.stream_timestamps.get("TRADE")

        if ts_q is not None and ts_d is not None:
            skew_qd = round(abs(ts_q - ts_d) * 1000.0, 2)
            skews_ms["quote_vs_depth"] = skew_qd
            max_skew_ms = max(max_skew_ms, skew_qd)

        if ts_q is not None and ts_t is not None:
            skew_qt = round(abs(ts_q - ts_t) * 1000.0, 2)
            skews_ms["quote_vs_trade"] = skew_qt
            max_skew_ms = max(max_skew_ms, skew_qt)

        if ts_d is not None and ts_t is not None:
            skew_dt = round(abs(ts_d - ts_t) * 1000.0, 2)
            skews_ms["depth_vs_trade"] = skew_dt
            max_skew_ms = max(max_skew_ms, skew_dt)

        # Invariant & Validity Check
        is_valid = True
        error_msg = None
        if state.latest_depth_snapshot is not None and not state.latest_depth_snapshot.is_valid:
            is_valid = False
            error_msg = "Latest OrderBookSnapshot is marked invalid or crossed"

        # Staleness check
        is_stale = False
        now = self.clock()
        for s_type, s_ts in state.stream_timestamps.items():
            # If received timestamp is available, compare receipt age to avoid mock timestamp skew
            recv_ts = state.stream_received_timestamps.get(s_type, s_ts)
            if (now - recv_ts) > self.max_staleness_seconds:
                is_stale = True

        # Synchronization Quality Resolution
        max_skew_threshold_ms = self.max_cross_stream_skew_seconds * 1000.0

        if not is_valid:
            quality = SyncQuality.INVALID
            is_synced = False
        elif is_stale:
            quality = SyncQuality.STALE
            is_synced = False
        elif len(missing) == 0 and max_skew_ms <= max_skew_threshold_ms:
            quality = SyncQuality.FULL
            is_synced = True
        else:
            quality = SyncQuality.PARTIAL
            is_synced = False

        sequence_status = {
            "last_sequences": dict(state.last_sequences),
            "sequence_gaps": dict(state.sequence_gaps),
            "out_of_order_counts": dict(state.out_of_order_counts),
        }

        return SynchronizedMarketContext(
            symbol=state.symbol,
            exchange=state.exchange,
            timestamp=context_ts,
            quote=state.latest_quote,
            depth_snapshot=state.latest_depth_snapshot,
            depth_analytics=state.latest_depth_analytics,
            trade_flow=state.latest_trade_flow,
            last_trade=state.latest_trade,
            stream_timestamps=dict(state.stream_timestamps),
            cross_stream_skew_ms=skews_ms,
            max_skew_ms=max_skew_ms,
            sequence_status=sequence_status,
            missing_streams=tuple(missing),
            sync_quality=quality,
            is_synchronized=is_synced,
            is_valid=is_valid,
            generation_timestamp=now,
            error=error_msg,
        )

    def get_latest_context(self, symbol: str) -> Optional[SynchronizedMarketContext]:
        """Retrieve latest SynchronizedMarketContext for symbol."""
        with self._lock:
            return self._latest_contexts.get(symbol.strip().upper())

    def get_context_history(self, symbol: str, limit: int = 20) -> List[SynchronizedMarketContext]:
        """Retrieve recent context history for symbol."""
        with self._lock:
            sym = symbol.strip().upper()
            state = self._states.get(sym)
            if not state:
                return []
            return list(state.context_history)[-limit:]

    def get_stream_status(self, symbol: str) -> Dict[str, Any]:
        """Retrieve operational telemetry and stream health for symbol."""
        with self._lock:
            sym = symbol.strip().upper()
            state = self._states.get(sym)
            if not state:
                return {"symbol": sym, "status": "UNKNOWN"}
            ctx = self._latest_contexts.get(sym)
            return {
                "symbol": sym,
                "exchange": state.exchange,
                "sync_quality": ctx.sync_quality.value if ctx else "NO_CONTEXT",
                "missing_streams": list(ctx.missing_streams) if ctx else ["QUOTE", "DEPTH", "TRADE"],
                "sequence_status": _unfreeze_mapping(ctx.sequence_status) if ctx else {},
                "max_skew_ms": ctx.max_skew_ms if ctx else 0.0,
                "quotes_buffered": len(state.quote_buffer),
                "depth_buffered": len(state.depth_buffer),
                "trades_buffered": len(state.trade_buffer),
            }

    def clear(self) -> None:
        """Clear all coordinator in-memory state."""
        with self._lock:
            self._states.clear()
            self._latest_contexts.clear()
            self._total_events_processed = 0
