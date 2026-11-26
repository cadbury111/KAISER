"""KAISER — Read-Only Asynchronous WebSocket Transport & Market Feed Foundation.

Provides a decoupled, resilient, non-mutating streaming market data foundation:
1. AsyncWebSocketTransport protocol defining connect, receive, send, close lifecycle.
2. Deterministic MockWebSocketTransport for in-memory and simulated network testing.
3. WebSocketMarketFeed implementing MarketFeed with explicit lifecycle states:
   DISCONNECTED, CONNECTING, CONNECTED, RECONNECTING, STALE, STOPPED.
4. Bounded ring buffering with explicit overflow tracking (drop oldest).
5. Deterministic exponential backoff with jitter and configurable maximum retry policies.
6. Fail-closed stale and invalid data protection without touching execution/broker layers.
"""

from __future__ import annotations

import asyncio
import collections
from dataclasses import dataclass, field
from enum import Enum
import json
import logging
import math
import random
import threading
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Protocol, Sequence, Tuple, Union

from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.depth import L2OrderBookManager
from app.market.feeds.base import MarketFeed, MarketRecord
from app.market.models import Bar, OrderBookDelta, OrderBookSnapshot, Quote
from app.market.normalizer import MarketDataNormalizer
from app.market.validators import DataFreshnessStatus, check_freshness

logger = get_logger("market.feeds.websocket")


class WebSocketFeedState(str, Enum):
    """Categorical lifecycle states for WebSocket market data feeds."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    STALE = "STALE"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class ReconnectPolicy:
    """Configurable exponential backoff policy for WebSocket reconnection."""

    max_reconnect_attempts: int = 5
    initial_backoff_seconds: float = 1.0
    backoff_factor: float = 2.0
    max_backoff_seconds: float = 30.0
    enable_jitter: bool = True

    def __post_init__(self) -> None:
        if self.max_reconnect_attempts < 0:
            raise ConfigurationError("max_reconnect_attempts must be non-negative.")
        if self.initial_backoff_seconds <= 0:
            raise ConfigurationError("initial_backoff_seconds must be positive.")
        if self.backoff_factor < 1.0:
            raise ConfigurationError("backoff_factor must be >= 1.0.")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ConfigurationError("max_backoff_seconds must be >= initial_backoff_seconds.")

    def calculate_backoff(self, attempt: int, jitter_ratio: Optional[float] = None) -> float:
        """Calculate the backoff duration in seconds for a given 0-indexed attempt count.

        Args:
            attempt: Current retry attempt index (0, 1, 2, ...).
            jitter_ratio: Optional deterministic jitter float in [0.0, 1.0] for testing.

        Returns:
            float: Duration in seconds to back off before retrying.
        """
        raw_backoff = min(
            self.max_backoff_seconds,
            self.initial_backoff_seconds * (self.backoff_factor ** attempt),
        )
        if not self.enable_jitter:
            return raw_backoff

        # Jitter applies scaling between 50% and 100% of raw_backoff
        factor = 0.5 + (0.5 * (jitter_ratio if jitter_ratio is not None else random.random()))
        return round(raw_backoff * factor, 4)


@dataclass(frozen=True)
class WebSocketFeedConfig:
    """Configuration container for WebSocket market data feed."""

    uri: str = "ws://127.0.0.1:8765/ws/market"
    symbols: Tuple[str, ...] = ("RELIANCE", "TCS", "INFY")
    exchange: str = "NSE"
    reconnect_policy: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    buffer_size: int = 1000
    connect_timeout_seconds: float = 10.0
    receive_timeout_seconds: float = 30.0
    freshness_threshold_seconds: float = 60.0
    source: str = "websocket"

    def __post_init__(self) -> None:
        if not self.uri or not isinstance(self.uri, str) or not self.uri.strip():
            raise ConfigurationError("WebSocketFeedConfig requires a non-empty uri.")
        clean_uri = self.uri.strip()
        if not (clean_uri.startswith("ws://") or clean_uri.startswith("wss://")):
            raise ConfigurationError(f"WebSocketFeedConfig uri must start with 'ws://' or 'wss://', got '{clean_uri}'.")
        object.__setattr__(self, "uri", clean_uri)

        if not self.symbols:
            raise ConfigurationError("WebSocketFeedConfig requires at least one symbol.")
        cleaned_symbols = tuple(s.strip().upper() for s in self.symbols if s.strip())
        object.__setattr__(self, "symbols", cleaned_symbols)

        norm_exchange = self.exchange.strip().upper()
        if not norm_exchange:
            raise ConfigurationError("WebSocketFeedConfig requires a non-empty exchange.")
        object.__setattr__(self, "exchange", norm_exchange)

        if self.buffer_size <= 0:
            raise ConfigurationError("buffer_size must be a positive integer.")
        if self.connect_timeout_seconds <= 0:
            raise ConfigurationError("connect_timeout_seconds must be positive.")
        if self.freshness_threshold_seconds <= 0:
            raise ConfigurationError("freshness_threshold_seconds must be positive.")


class AsyncWebSocketTransport(Protocol):
    """Abstract asynchronous transport protocol for WebSocket market data streams."""

    async def connect(self, uri: str, timeout: float = 10.0) -> None:
        """Establish asynchronous WebSocket connection to the specified URI."""
        ...

    async def receive(self) -> Union[str, bytes]:
        """Await and return the next raw text or binary message from the socket."""
        ...

    async def send(self, data: Union[str, bytes]) -> None:
        """Send a control or subscription message over the socket."""
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Gracefully close the WebSocket connection."""
        ...

    @property
    def is_connected(self) -> bool:
        """Return whether the transport is currently connected."""
        ...


class MockWebSocketTransport:
    """Deterministic in-memory mock WebSocket transport for testing and simulation.

    Requires zero external network access, operates entirely in-memory, supports
    queueing messages, simulating network drops, injected failures, and timeouts.
    """

    def __init__(self) -> None:
        self._is_connected: bool = False
        self._queue: asyncio.Queue[Union[str, bytes, Exception]] = asyncio.Queue()
        self._sent_messages: List[Union[str, bytes]] = []
        self._connect_count: int = 0
        self._close_count: int = 0
        self._pending_failure: Optional[Exception] = None
        self._failure_is_persistent: bool = False
        self._latency_seconds: float = 0.0

    @property
    def is_connected(self) -> bool:
        """Return connection status."""
        return self._is_connected

    @property
    def connect_count(self) -> int:
        """Return total number of connect calls made."""
        return self._connect_count

    @property
    def close_count(self) -> int:
        """Return total number of close calls made."""
        return self._close_count

    @property
    def sent_messages(self) -> List[Union[str, bytes]]:
        """Return copy of all messages sent over this transport."""
        return list(self._sent_messages)

    def enqueue_message(self, message: Union[str, bytes, Dict[str, Any]]) -> None:
        """Enqueue a mock raw message to be returned by receive()."""
        if isinstance(message, dict):
            payload: Union[str, bytes] = json.dumps(message)
        else:
            payload = message
        self._queue.put_nowait(payload)

    def enqueue_messages(self, messages: Sequence[Union[str, bytes, Dict[str, Any]]]) -> None:
        """Enqueue multiple messages sequentially."""
        for msg in messages:
            self.enqueue_message(msg)

    def simulate_failure(self, exc: Exception, persistent: bool = False) -> None:
        """Set an exception to be raised on the next connect, receive, or send."""
        self._pending_failure = exc
        self._failure_is_persistent = persistent

    def simulate_disconnect(self, exc: Optional[Exception] = None) -> None:
        """Simulate an abrupt transport disconnect, waking up any waiting receive call."""
        self._is_connected = False
        self._queue.put_nowait(exc or ConnectionResetError("Mock transport connection dropped unexpectedly."))

    def simulate_latency(self, seconds: float) -> None:
        """Configure artificial latency on operations."""
        self._latency_seconds = max(0.0, seconds)

    async def connect(self, uri: str, timeout: float = 10.0) -> None:
        """Connect to mock URI."""
        if self._latency_seconds > 0:
            await asyncio.sleep(self._latency_seconds)

        if self._pending_failure:
            err = self._pending_failure
            if not self._failure_is_persistent:
                self._pending_failure = None
            raise err

        self._is_connected = True
        self._connect_count += 1

    async def receive(self) -> Union[str, bytes]:
        """Receive the next enqueued mock message."""
        if self._latency_seconds > 0:
            await asyncio.sleep(self._latency_seconds)

        if not self._is_connected and self._queue.empty():
            raise ConnectionResetError("Cannot receive from disconnected mock WebSocket transport.")

        item = await self._queue.get()
        if isinstance(item, Exception):
            self._is_connected = False
            raise item
        return item

    async def send(self, data: Union[str, bytes]) -> None:
        """Record outbound message in mock sent buffer."""
        if self._latency_seconds > 0:
            await asyncio.sleep(self._latency_seconds)

        if not self._is_connected:
            raise ConnectionResetError("Cannot send on disconnected mock WebSocket transport.")

        if self._pending_failure:
            err = self._pending_failure
            self._pending_failure = None
            raise err

        self._sent_messages.append(data)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close mock connection."""
        self._is_connected = False
        self._close_count += 1


class WebSocketMarketFeed(MarketFeed):
    """Read-only asynchronous WebSocket market data feed adapter.

    Adheres strictly to the MarketFeed protocol while providing asynchronous ingestion,
    bounded buffering with explicit overflow drops, deterministic backoff reconnection,
    freshness monitoring, and EventBus distribution.
    """

    def __init__(
        self,
        config: WebSocketFeedConfig,
        transport: AsyncWebSocketTransport,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        normalizer: Optional[MarketDataNormalizer] = None,
        order_book_manager: Optional[L2OrderBookManager] = None,
        name: str = "websocket_feed",
        clock: Optional[Callable[[], float]] = None,
        sleep_func: Optional[Callable[[float], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        super().__init__(name=name, source=config.source)
        self.config = config
        self.transport = transport
        self.event_bus = event_bus
        self.state_store = state_store
        self.normalizer = normalizer or MarketDataNormalizer()
        self.order_book_manager = order_book_manager or L2OrderBookManager(
            exchange=self.config.exchange,
            source=self.config.source,
        )
        self._clock = clock or time.time
        self._sleep = sleep_func or asyncio.sleep

        # Categorical lifecycle state
        self._state: WebSocketFeedState = WebSocketFeedState.DISCONNECTED
        self._lock = threading.RLock()

        # Bounded ring buffer for normalized records (drop oldest on overflow)
        self._buffer: collections.deque[MarketRecord] = collections.deque(maxlen=self.config.buffer_size)

        # Operational metrics
        self._records_received: int = 0
        self._records_validated: int = 0
        self._records_rejected: int = 0
        self._records_dropped: int = 0
        self._reconnect_attempts: int = 0
        self._last_received_timestamp: Optional[float] = None
        self._last_error: Optional[str] = None

        # Asynchronous supervisor handles
        self._running: bool = False
        self._consumer_task: Optional[asyncio.Task[None]] = None

    @property
    def state(self) -> WebSocketFeedState:
        """Return the current categorical lifecycle state."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Return whether the feed is currently active."""
        return self._running and self._is_running

    @property
    def records_received(self) -> int:
        """Total raw messages received from the transport."""
        return self._records_received

    @property
    def records_validated(self) -> int:
        """Total messages successfully validated and normalized."""
        return self._records_validated

    @property
    def records_rejected(self) -> int:
        """Total messages rejected due to malformed schema, negative prices, or staleness."""
        return self._records_rejected

    @property
    def records_dropped(self) -> int:
        """Total records discarded due to bounded buffer overflow (backpressure)."""
        return self._records_dropped

    @property
    def buffer_depth(self) -> int:
        """Current number of pending normalized records in the ring buffer."""
        return len(self._buffer)

    @property
    def reconnect_attempts(self) -> int:
        """Current consecutive reconnection attempts."""
        return self._reconnect_attempts

    def reset(self) -> None:
        """Reset feed buffer, cursor metrics, and backoff counters."""
        with self._lock:
            self._buffer.clear()
            self._records_emitted = 0
            self._records_received = 0
            self._records_validated = 0
            self._records_rejected = 0
            self._records_dropped = 0
            self._reconnect_attempts = 0
            self._last_received_timestamp = None
            self._last_error = None
            self.order_book_manager.reset()
            if not self._running:
                self._state = WebSocketFeedState.DISCONNECTED
            logger.debug("WebSocket feed '%s' reset.", self.name)

    def get_order_book(self, symbol: str) -> Optional[OrderBookSnapshot]:
        """Return the current reconstructed L2 order book snapshot for the symbol."""
        return self.order_book_manager.get_snapshot(symbol)

    def next_record(self) -> Optional[MarketRecord]:
        """Fetch the next available normalized record from the bounded buffer (synchronous pull).

        Returns:
            Optional[MarketRecord]: Normalized Quote or Bar, or None if buffer is empty.
        """
        if not self._is_running:
            return None

        with self._lock:
            if not self._buffer:
                return None
            record = self._buffer.popleft()
            self._records_emitted += 1
            return record

    async def get_record_async(self, timeout: Optional[float] = None) -> Optional[MarketRecord]:
        """Asynchronously await the next record from the buffer."""
        if not self._is_running:
            return None

        start_t = time.time()
        while self._is_running:
            rec = self.next_record()
            if rec is not None:
                return rec
            if timeout is not None and (time.time() - start_t) >= timeout:
                return None
            await asyncio.sleep(0.01)
        return None

    def start(self) -> None:
        """Synchronously start the feed marker (for compatibility with MarketFeed interface)."""
        super().start()
        self._running = True
        self._state = WebSocketFeedState.CONNECTING

    def stop(self) -> None:
        """Synchronously request feed termination."""
        super().stop()
        self._running = False
        self._state = WebSocketFeedState.STOPPED

    async def start_async(self) -> asyncio.Task[None]:
        """Start the asynchronous supervisor loop in the current event loop.

        Returns:
            asyncio.Task: Handle to the background supervisor task.
        """
        self.start()
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self._run_supervisor())
        return self._consumer_task

    async def stop_async(self) -> None:
        """Gracefully stop the asynchronous supervisor and close the transport."""
        self.stop()
        if self._consumer_task and not self._consumer_task.done():
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            except Exception as err:
                logger.debug("Consumer task completed with exception during cancellation: %s", err)
            self._consumer_task = None

        try:
            await self.transport.close(code=1000, reason="Feed stopped gracefully")
        except Exception as close_err:
            logger.warning("Error closing transport during stop_async: %s", close_err)

        self._state = WebSocketFeedState.STOPPED

    async def _run_supervisor(self) -> None:
        """Main supervisor loop managing connection, message ingestion, and reconnection."""
        while self._running:
            try:
                self._state = WebSocketFeedState.CONNECTING
                logger.debug("Connecting to WebSocket URI: %s", self.config.uri)
                await self.transport.connect(self.config.uri, timeout=self.config.connect_timeout_seconds)
                self._state = WebSocketFeedState.CONNECTED
                self._reconnect_attempts = 0
                logger.info("WebSocket feed '%s' connected to %s", self.name, self.config.uri)

                # Send subscription message for configured symbols
                sub_msg = json.dumps({"action": "subscribe", "symbols": list(self.config.symbols), "exchange": self.config.exchange})
                try:
                    await self.transport.send(sub_msg)
                except Exception as sub_err:
                    logger.debug("Subscription payload send failed (may be unneeded by mock): %s", sub_err)

                # Active streaming loop
                while self._running and self.transport.is_connected:
                    try:
                        raw = await self.transport.receive()
                        self._process_raw_message(raw)
                    except asyncio.CancelledError:
                        raise
                    except Exception as rx_err:
                        if not self._running:
                            break
                        logger.warning("WebSocket receive error on '%s': %s", self.name, rx_err)
                        self._last_error = str(rx_err)
                        break

            except asyncio.CancelledError:
                break
            except Exception as conn_err:
                if not self._running:
                    break
                self._last_error = str(conn_err)
                logger.warning("WebSocket connection failed on '%s': %s", self.name, conn_err)

            if not self._running:
                break

            # Handle reconnection backoff
            policy = self.config.reconnect_policy
            if self._reconnect_attempts >= policy.max_reconnect_attempts:
                logger.error(
                    "WebSocket feed '%s' reached max reconnect attempts (%d). Halting feed.",
                    self.name,
                    policy.max_reconnect_attempts,
                )
                self._state = WebSocketFeedState.DISCONNECTED
                self._running = False
                self._is_running = False
                break

            self._state = WebSocketFeedState.RECONNECTING
            backoff_delay = policy.calculate_backoff(self._reconnect_attempts)
            self._reconnect_attempts += 1
            logger.info(
                "WebSocket feed '%s' reconnecting in %.2fs (attempt %d/%d)...",
                self.name,
                backoff_delay,
                self._reconnect_attempts,
                policy.max_reconnect_attempts,
            )
            try:
                await self._sleep(backoff_delay)
            except asyncio.CancelledError:
                break

        if not self._running and self._state != WebSocketFeedState.DISCONNECTED:
            self._state = WebSocketFeedState.STOPPED

    def _process_raw_message(self, raw: Union[str, bytes]) -> Optional[MarketRecord]:
        """Decode, validate, normalize, buffer, and publish incoming market data."""
        self._records_received += 1

        # 1. Decode text/JSON payload
        if isinstance(raw, bytes):
            try:
                raw_text = raw.decode("utf-8")
            except Exception as dec_err:
                self._records_rejected += 1
                self._last_error = f"Binary decode failure: {dec_err}"
                return None
        else:
            raw_text = raw

        try:
            payload = json.loads(raw_text)
        except Exception as json_err:
            self._records_rejected += 1
            self._last_error = f"JSON parse error: {json_err}"
            return None

        if isinstance(payload, list):
            last_record = None
            for item in payload:
                if isinstance(item, dict):
                    res = self._normalize_and_buffer_item(item)
                    if res:
                        last_record = res
            return last_record
        elif isinstance(payload, dict):
            return self._normalize_and_buffer_item(payload)

        self._records_rejected += 1
        return None

    def _normalize_and_buffer_item(self, data: Dict[str, Any]) -> Optional[MarketRecord]:
        """Validate, normalize, buffer, and distribute a single raw dictionary record."""
        # 2. Normalize via MarketDataNormalizer
        try:
            if isinstance(data, OrderBookDelta):
                record: MarketRecord = self.order_book_manager.apply_delta(data)
            elif (
                "action" in data
                or "operation" in data
                or data.get("type") in ("delta", "ADD", "UPDATE", "DELETE", "add", "update", "delete")
                or ("side" in data and ("price" in data or "level" in data) and "open" not in data and "bids" not in data)
            ):
                delta = self.normalizer.normalize_order_book_delta(data, default_source=self.config.source)
                record = self.order_book_manager.apply_delta(delta)
            elif "open" in data and "close" in data:
                record = self.normalizer.normalize_bar(data, default_source=self.config.source)
            elif "bids" in data or "asks" in data:
                record = self.normalizer.normalize_order_book(data, default_source=self.config.source)
            else:
                record = self.normalizer.normalize_quote(data, default_source=self.config.source)
        except Exception as norm_err:
            self._records_rejected += 1
            self._last_error = f"Normalization failure: {norm_err}"
            return None

        # 3. Freshness Assessment
        now = self._clock()
        freshness = check_freshness(
            record.timestamp,
            reference_time=now,
            threshold_seconds=self.config.freshness_threshold_seconds,
        )

        if freshness == DataFreshnessStatus.INVALID:
            self._records_rejected += 1
            self._last_error = f"Invalid timestamp for {record.symbol}: {record.timestamp}"
            if self.event_bus:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.MARKET_DATA_INVALID,
                        source=self.source,
                        payload={"symbol": record.symbol, "timestamp": record.timestamp},
                    )
                )
            return None

        if freshness == DataFreshnessStatus.STALE:
            self._state = WebSocketFeedState.STALE
            self._records_rejected += 1
            self._last_error = f"Stale market record for {record.symbol}: age={max(0.0, now - record.timestamp):.1f}s"
            if self.event_bus:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.MARKET_DATA_STALE,
                        source=self.source,
                        payload={"symbol": record.symbol, "timestamp": record.timestamp, "age": max(0.0, now - record.timestamp)},
                    )
                )
            return None

        # Record is valid and fresh
        self._records_validated += 1
        self._last_received_timestamp = record.timestamp

        with self._lock:
            # Check for buffer overflow
            if len(self._buffer) >= self.config.buffer_size:
                self._records_dropped += 1
            self._buffer.append(record)

        # Update StateStore cache (latest quotes & order books)
        if self.state_store:
            if isinstance(record, Quote):
                self.state_store.update(
                    "latest_quotes",
                    {record.symbol: record.to_dict()},
                    default={},
                )
            elif isinstance(record, OrderBookSnapshot):
                self.state_store.update(
                    "latest_order_books",
                    {record.symbol: record.to_dict()},
                    default={},
                )
            self.state_store.set("websocket_feed_state", self._state.value)

        # Distribute immutable event across EventBus
        if self.event_bus:
            if isinstance(record, Quote):
                etype = EventType.MARKET_QUOTE_UPDATED
            elif isinstance(record, Bar):
                etype = EventType.MARKET_BAR_UPDATED
            else:
                etype = EventType.MARKET_DEPTH_UPDATED

            self.event_bus.publish(
                Event.create(
                    event_type=etype,
                    source=self.source,
                    payload=record.to_dict(),
                    metadata={"freshness": freshness.value, "transport": "websocket"},
                )
            )

        return record

    def get_status(self) -> Dict[str, Any]:
        """Return diagnostic status metadata for the WebSocket feed."""
        base_status = super().get_status()
        base_status.update({
            "feed_state": self._state.value,
            "uri": self.config.uri,
            "symbols": list(self.config.symbols),
            "exchange": self.config.exchange,
            "records_received": self._records_received,
            "records_validated": self._records_validated,
            "records_rejected": self._records_rejected,
            "records_dropped": self._records_dropped,
            "buffer_depth": len(self._buffer),
            "buffer_capacity": self.config.buffer_size,
            "reconnect_attempts": self._reconnect_attempts,
            "last_received_timestamp": self._last_received_timestamp,
            "last_error": self._last_error,
            "is_transport_connected": self.transport.is_connected,
        })
        return base_status
