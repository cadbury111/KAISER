"""KAISER — Tests for Read-Only Asynchronous WebSocket Transport & Feed Foundation.

Validates Phase 3 Step 85 implementation:
1. AsyncWebSocketTransport protocol contract and MockWebSocketTransport.
2. ReconnectPolicy backoff progression, jitter, and bounds.
3. WebSocketFeedConfig validation.
4. WebSocketMarketFeed streaming, normalization, validation, and buffer operations.
5. Fail-closed stale and invalid message rejection.
6. Bounded ring buffer overflow and backpressure telemetry.
7. Reconnection transitions and max-attempt exhaustion.
8. Clean cancellation, task shutdown, and event silence after termination.
9. Execution boundary and safety invariant preservation.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Coroutine, Dict, List, TypeVar

import pytest

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.state import StateStore
from app.market.feeds.websocket import (
    MockWebSocketTransport,
    ReconnectPolicy,
    WebSocketFeedConfig,
    WebSocketFeedState,
    WebSocketMarketFeed,
)
from app.market.models import Bar, Quote

T = TypeVar("T")


def run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Helper to run coroutines synchronously in pytest without third-party plugins."""
    return asyncio.run(coro)


# -----------------------------------------------------------------------------
# 1. Transport & Protocol Unit Tests
# -----------------------------------------------------------------------------


def test_mock_websocket_transport_lifecycle() -> None:
    """Test MockWebSocketTransport connect, receive, send, and close lifecycle."""

    async def _test() -> None:
        transport = MockWebSocketTransport()
        assert not transport.is_connected
        assert transport.connect_count == 0
        assert transport.close_count == 0

        await transport.connect("ws://127.0.0.1:8765/ws")
        assert transport.is_connected
        assert transport.connect_count == 1

        # Send outbound message
        await transport.send(json.dumps({"action": "ping"}))
        assert len(transport.sent_messages) == 1
        assert "ping" in transport.sent_messages[0]

        # Enqueue and receive inbound message
        transport.enqueue_message({"symbol": "RELIANCE", "last_price": 2500.0})
        received = await transport.receive()
        data = json.loads(received)
        assert data["symbol"] == "RELIANCE"

        # Close connection
        await transport.close()
        assert not transport.is_connected
        assert transport.close_count == 1

    run_async(_test())


def test_mock_websocket_transport_failures() -> None:
    """Test MockWebSocketTransport failure simulation and abrupt disconnection."""

    async def _test() -> None:
        transport = MockWebSocketTransport()

        # Injected connection failure
        transport.simulate_failure(ConnectionRefusedError("Connection refused by test mock"))
        with pytest.raises(ConnectionRefusedError, match="Connection refused"):
            await transport.connect("ws://127.0.0.1:8765/ws")

        # Connect successfully
        await transport.connect("ws://127.0.0.1:8765/ws")
        assert transport.is_connected

        # Simulate abrupt disconnect
        transport.simulate_disconnect()
        assert not transport.is_connected

        with pytest.raises(ConnectionResetError):
            await transport.receive()

    run_async(_test())


# -----------------------------------------------------------------------------
# 2. Reconnect Policy & Config Tests
# -----------------------------------------------------------------------------


def test_reconnect_policy_backoff_calculation() -> None:
    """Test deterministic backoff calculation without and with jitter."""
    policy = ReconnectPolicy(
        max_reconnect_attempts=5,
        initial_backoff_seconds=1.0,
        backoff_factor=2.0,
        max_backoff_seconds=10.0,
        enable_jitter=False,
    )

    # Attempt 0: 1.0s
    assert policy.calculate_backoff(0) == 1.0
    # Attempt 1: 2.0s
    assert policy.calculate_backoff(1) == 2.0
    # Attempt 2: 4.0s
    assert policy.calculate_backoff(2) == 4.0
    # Attempt 3: 8.0s
    assert policy.calculate_backoff(3) == 8.0
    # Attempt 4: capped at 10.0s
    assert policy.calculate_backoff(4) == 10.0

    # Test with jitter bounds
    policy_jitter = ReconnectPolicy(
        max_reconnect_attempts=5,
        initial_backoff_seconds=2.0,
        backoff_factor=2.0,
        max_backoff_seconds=20.0,
        enable_jitter=True,
    )
    # Jitter factor ranges from 0.5 to 1.0 of raw backoff
    min_backoff = policy_jitter.calculate_backoff(1, jitter_ratio=0.0)
    max_backoff = policy_jitter.calculate_backoff(1, jitter_ratio=1.0)
    assert min_backoff == 2.0  # 4.0 * 0.5
    assert max_backoff == 4.0  # 4.0 * 1.0


def test_reconnect_policy_validation_errors() -> None:
    """Test ReconnectPolicy rejects invalid configuration parameters."""
    with pytest.raises(ConfigurationError, match="max_reconnect_attempts"):
        ReconnectPolicy(max_reconnect_attempts=-1)

    with pytest.raises(ConfigurationError, match="initial_backoff_seconds"):
        ReconnectPolicy(initial_backoff_seconds=0.0)

    with pytest.raises(ConfigurationError, match="backoff_factor"):
        ReconnectPolicy(backoff_factor=0.5)

    with pytest.raises(ConfigurationError, match="max_backoff_seconds"):
        ReconnectPolicy(initial_backoff_seconds=10.0, max_backoff_seconds=5.0)


def test_websocket_feed_config_validation() -> None:
    """Test WebSocketFeedConfig parameter validation and normalization."""
    # Valid config
    cfg = WebSocketFeedConfig(uri="ws://127.0.0.1:8765/stream", symbols=("RELIANCE", "tcs "))
    assert cfg.symbols == ("RELIANCE", "TCS")
    assert cfg.uri == "ws://127.0.0.1:8765/stream"

    # Invalid URI scheme
    with pytest.raises(ConfigurationError, match="must start with 'ws://' or 'wss://'"):
        WebSocketFeedConfig(uri="http://127.0.0.1:8765/stream")

    # Empty symbols
    with pytest.raises(ConfigurationError, match="at least one symbol"):
        WebSocketFeedConfig(symbols=())

    # Invalid buffer size
    with pytest.raises(ConfigurationError, match="buffer_size"):
        WebSocketFeedConfig(buffer_size=0)


# -----------------------------------------------------------------------------
# 3. WebSocketMarketFeed Streaming & Ingestion Tests
# -----------------------------------------------------------------------------


def test_websocket_feed_connect_and_streaming() -> None:
    """Test end-to-end asynchronous streaming, normalization, and synchronous pull."""

    async def _test() -> None:
        now = time.time()
        transport = MockWebSocketTransport()
        config = WebSocketFeedConfig(buffer_size=50)
        feed = WebSocketMarketFeed(config=config, transport=transport, clock=lambda: now)

        # Enqueue valid mock quotes
        transport.enqueue_message({
            "symbol": "RELIANCE",
            "last_price": 2520.5,
            "bid": 2520.0,
            "ask": 2521.0,
            "volume": 50000.0,
            "timestamp": now,
            "exchange": "NSE",
        })
        transport.enqueue_message({
            "symbol": "TCS",
            "last_price": 4150.0,
            "bid": 4149.0,
            "ask": 4151.0,
            "volume": 25000.0,
            "timestamp": now,
            "exchange": "NSE",
        })

        # Start feed supervisor
        task = await feed.start_async()
        assert feed.is_running
        assert feed.state in (WebSocketFeedState.CONNECTING, WebSocketFeedState.CONNECTED)

        # Allow event loop to process messages
        for _ in range(30):
            if feed.records_validated >= 2:
                break
            await asyncio.sleep(0.01)

        assert feed.records_received == 2
        assert feed.records_validated == 2
        assert feed.records_rejected == 0
        assert feed.buffer_depth == 2

        # Pull records synchronously via next_record()
        rec1 = feed.next_record()
        assert isinstance(rec1, Quote)
        assert rec1.symbol == "RELIANCE"
        assert rec1.last_price == 2520.5
        assert feed.records_emitted == 1

        rec2 = feed.next_record()
        assert isinstance(rec2, Quote)
        assert rec2.symbol == "TCS"
        assert rec2.last_price == 4150.0
        assert feed.records_emitted == 2

        assert feed.next_record() is None

        # Gracefully stop feed
        await feed.stop_async()
        assert feed.state == WebSocketFeedState.STOPPED
        assert not feed.is_running

    run_async(_test())


def test_websocket_feed_event_bus_and_state_store_integration() -> None:
    """Test EventBus event dispatching and StateStore quote caching."""

    async def _test() -> None:
        now = time.time()
        transport = MockWebSocketTransport()
        event_bus = EventBus()
        state_store = StateStore()

        events_captured: List[Event] = []
        event_bus.subscribe(EventType.MARKET_QUOTE_UPDATED, lambda e: events_captured.append(e))
        event_bus.subscribe(EventType.MARKET_BAR_UPDATED, lambda e: events_captured.append(e))

        config = WebSocketFeedConfig()
        feed = WebSocketMarketFeed(
            config=config,
            transport=transport,
            event_bus=event_bus,
            state_store=state_store,
            clock=lambda: now,
        )

        # 1. Enqueue Quote
        transport.enqueue_message({
            "symbol": "INFY",
            "last_price": 1820.0,
            "timestamp": now,
            "volume": 10000.0,
        })

        # 2. Enqueue Bar
        transport.enqueue_message({
            "symbol": "INFY",
            "timeframe": "1m",
            "open": 1818.0,
            "high": 1822.0,
            "low": 1817.0,
            "close": 1820.0,
            "volume": 5000.0,
            "timestamp": now,
        })

        await feed.start_async()

        for _ in range(30):
            if len(events_captured) >= 2:
                break
            await asyncio.sleep(0.01)

        assert len(events_captured) == 2
        assert events_captured[0].event_type == EventType.MARKET_QUOTE_UPDATED
        assert events_captured[0].payload["symbol"] == "INFY"
        assert events_captured[1].event_type == EventType.MARKET_BAR_UPDATED
        assert events_captured[1].payload["close"] == 1820.0

        # Verify StateStore cache updated
        cached_quotes = state_store.get("latest_quotes", {})
        assert "INFY" in cached_quotes
        assert cached_quotes["INFY"]["last_price"] == 1820.0

        await feed.stop_async()

    run_async(_test())


# -----------------------------------------------------------------------------
# 4. Fail-Closed Validation & Freshness Tests
# -----------------------------------------------------------------------------


def test_websocket_feed_stale_and_invalid_rejection() -> None:
    """Test that stale and structurally invalid records are rejected without entering buffer."""

    async def _test() -> None:
        now = 1700000000.0
        transport = MockWebSocketTransport()
        event_bus = EventBus()
        stale_events: List[Event] = []
        event_bus.subscribe(EventType.MARKET_DATA_STALE, lambda e: stale_events.append(e))

        config = WebSocketFeedConfig(freshness_threshold_seconds=60.0)
        feed = WebSocketMarketFeed(
            config=config,
            transport=transport,
            event_bus=event_bus,
            clock=lambda: now,
        )

        # 1. Stale record (age = 120s > 60s threshold)
        transport.enqueue_message({
            "symbol": "RELIANCE",
            "last_price": 2500.0,
            "timestamp": now - 120.0,
        })

        # 2. Invalid price (negative)
        transport.enqueue_message({
            "symbol": "RELIANCE",
            "last_price": -50.0,
            "timestamp": now,
        })

        # 3. Invalid JSON
        transport.enqueue_message("MALFORMED_NON_JSON_RECORD")

        # 4. Valid record for comparison
        transport.enqueue_message({
            "symbol": "RELIANCE",
            "last_price": 2505.0,
            "timestamp": now,
        })

        await feed.start_async()

        for _ in range(35):
            if feed.records_validated >= 1:
                break
            await asyncio.sleep(0.01)

        assert feed.records_received == 4
        assert feed.records_validated == 1  # Only the valid record passed
        assert feed.records_rejected == 3   # Stale, negative price, and malformed json rejected
        assert len(stale_events) >= 1

        # Buffer contains only the single valid record
        assert feed.buffer_depth == 1
        valid_rec = feed.next_record()
        assert valid_rec is not None
        assert valid_rec.last_price == 2505.0
        assert feed.next_record() is None

        await feed.stop_async()

    run_async(_test())


# -----------------------------------------------------------------------------
# 5. Bounded Buffer Overflow & Backpressure Tests
# -----------------------------------------------------------------------------


def test_websocket_feed_bounded_buffer_overflow() -> None:
    """Test ring buffer overflow handling: drops oldest and records drop count."""

    async def _test() -> None:
        now = time.time()
        transport = MockWebSocketTransport()
        config = WebSocketFeedConfig(buffer_size=3)  # Small capacity to test drop behavior
        feed = WebSocketMarketFeed(config=config, transport=transport, clock=lambda: now)

        # Enqueue 5 valid quotes
        for i in range(1, 6):
            transport.enqueue_message({
                "symbol": "RELIANCE",
                "last_price": 2500.0 + i,
                "timestamp": now,
            })

        await feed.start_async()

        for _ in range(30):
            if feed.records_validated >= 5:
                break
            await asyncio.sleep(0.01)

        assert feed.records_validated == 5
        assert feed.buffer_depth == 3       # Buffer size remains capped at 3
        assert feed.records_dropped == 2     # Exactly 2 oldest records dropped

        # Drain buffer: should contain items 3, 4, 5 (prices 2503, 2504, 2505)
        r1 = feed.next_record()
        assert r1 is not None and r1.last_price == 2503.0
        r2 = feed.next_record()
        assert r2 is not None and r2.last_price == 2504.0
        r3 = feed.next_record()
        assert r3 is not None and r3.last_price == 2505.0
        assert feed.next_record() is None

        # Check status telemetry reflects drops
        status = feed.get_status()
        assert status["records_dropped"] == 2
        assert status["buffer_capacity"] == 3

        await feed.stop_async()

    run_async(_test())


# -----------------------------------------------------------------------------
# 6. Reconnection & Recovery Tests
# -----------------------------------------------------------------------------


def test_websocket_feed_reconnection_transitions() -> None:
    """Test automatic reconnect transitions upon simulated connection drops."""

    async def _test() -> None:
        now = time.time()
        transport = MockWebSocketTransport()

        slept_durations: List[float] = []

        async def fast_mock_sleep(seconds: float) -> None:
            slept_durations.append(seconds)
            await asyncio.sleep(0.001)

        policy = ReconnectPolicy(
            max_reconnect_attempts=3,
            initial_backoff_seconds=1.0,
            backoff_factor=2.0,
            enable_jitter=False,
        )
        config = WebSocketFeedConfig(reconnect_policy=policy)
        feed = WebSocketMarketFeed(
            config=config,
            transport=transport,
            clock=lambda: now,
            sleep_func=fast_mock_sleep,
        )

        await feed.start_async()
        for _ in range(15):
            if feed.state == WebSocketFeedState.CONNECTED:
                break
            await asyncio.sleep(0.01)
        assert feed.state == WebSocketFeedState.CONNECTED

        # Simulate transport drop
        transport.simulate_disconnect()

        # Allow reconnect loop to execute
        for _ in range(30):
            if feed.state == WebSocketFeedState.CONNECTED and transport.connect_count >= 2:
                break
            await asyncio.sleep(0.01)

        assert transport.connect_count >= 2
        assert feed.state == WebSocketFeedState.CONNECTED
        assert len(slept_durations) >= 1
        assert slept_durations[0] == 1.0  # Initial backoff

        await feed.stop_async()

    run_async(_test())


def test_websocket_feed_max_reconnect_exhaustion() -> None:
    """Test feed halts safely when maximum reconnect attempts are exceeded."""

    async def _test() -> None:
        transport = MockWebSocketTransport()

        async def fast_mock_sleep(seconds: float) -> None:
            await asyncio.sleep(0.001)

        # Only 2 attempts allowed
        policy = ReconnectPolicy(max_reconnect_attempts=2, initial_backoff_seconds=0.1, enable_jitter=False)
        config = WebSocketFeedConfig(reconnect_policy=policy)
        feed = WebSocketMarketFeed(
            config=config,
            transport=transport,
            sleep_func=fast_mock_sleep,
        )

        # First connect succeeds
        await feed.start_async()
        for _ in range(15):
            if feed.state == WebSocketFeedState.CONNECTED:
                break
            await asyncio.sleep(0.01)

        # Now make all subsequent connect attempts fail
        transport.simulate_failure(ConnectionRefusedError("Host permanently down"), persistent=True)
        transport.simulate_disconnect()

        # Wait for exhaustion
        for _ in range(40):
            if feed.state == WebSocketFeedState.DISCONNECTED and not feed.is_running:
                break
            await asyncio.sleep(0.01)

        assert feed.state == WebSocketFeedState.DISCONNECTED
        assert not feed.is_running
        assert feed.reconnect_attempts == 2

        await feed.stop_async()

    run_async(_test())


# -----------------------------------------------------------------------------
# 7. Cancellation & Shutdown Tests
# -----------------------------------------------------------------------------


def test_websocket_feed_cancellation_and_graceful_shutdown() -> None:
    """Test clean cancellation of consumer task and transport closure on stop."""

    async def _test() -> None:
        transport = MockWebSocketTransport()
        config = WebSocketFeedConfig()
        feed = WebSocketMarketFeed(config=config, transport=transport)

        task = await feed.start_async()
        assert feed.is_running

        await feed.stop_async()
        assert not feed.is_running
        assert feed.state == WebSocketFeedState.STOPPED
        assert task.done()
        assert transport.close_count >= 1

        # Ensure no further messages processed
        transport.enqueue_message({"symbol": "RELIANCE", "last_price": 2600.0, "timestamp": time.time()})
        await asyncio.sleep(0.05)
        assert feed.next_record() is None

    run_async(_test())


# -----------------------------------------------------------------------------
# 8. Execution Safety Invariant Assertions
# -----------------------------------------------------------------------------


def test_websocket_feed_safety_invariants() -> None:
    """Verify that WebSocketMarketFeed adheres strictly to Phase 3 read-only invariants."""
    transport = MockWebSocketTransport()
    feed = WebSocketMarketFeed(config=WebSocketFeedConfig(), transport=transport)

    # 1. No execution methods exist on WebSocketMarketFeed
    for forbidden in ("submit_order", "place_order", "cancel_order", "execute_order", "create_order"):
        assert not hasattr(feed, forbidden), f"Forbidden execution method found: {forbidden}"

    # 2. Source is stamped as read-only websocket
    assert feed.source == "websocket"

    # 3. Global settings execution invariants remain locked
    settings = Settings()
    assert settings.execution_locked is True
    assert settings.broker_provider == "none"
    assert settings.paper_execution_enabled is False
    assert settings.paper_trading_only is True
