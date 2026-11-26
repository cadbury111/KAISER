"""KAISER — Tests for Level 2 Order Book Depth Delta Reconstruction & Streaming Engine.

Validates Phase 3 Step 86 implementation:
1. OrderBookDelta model validation (types, sides, actions, price/quantity limits).
2. L2OrderBookBuilder ADD, UPDATE, DELETE, and zero-quantity level removals.
3. Strict bid (descending) and ask (ascending) sorting invariants.
4. Crossed-book detection and fail-closed rollback.
5. Monotonic sequence validation (duplicate and out-of-order rejection).
6. Sequence gap detection and threshold handling.
7. Immutable OrderBookSnapshot guarantees.
8. Multi-instrument L2OrderBookManager coordination.
9. Read-only WebSocketMarketFeed streaming L2 delta integration.
10. Execution boundary and safety invariant preservation.
"""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
import json
import time
from typing import Any, Coroutine, Dict, List, TypeVar

import pytest

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.depth import L2OrderBookBuilder, L2OrderBookManager
from app.market.feeds.websocket import (
    MockWebSocketTransport,
    WebSocketFeedConfig,
    WebSocketMarketFeed,
)
from app.market.models import (
    BookSide,
    DeltaAction,
    OrderBookDelta,
    OrderBookLevel,
    OrderBookSnapshot,
)

T = TypeVar("T")


def run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Helper to run coroutines synchronously in standard pytest."""
    return asyncio.run(coro)


# -----------------------------------------------------------------------------
# 1. OrderBookDelta Domain Model Tests
# -----------------------------------------------------------------------------


def test_order_book_delta_creation_and_normalization() -> None:
    """Test valid OrderBookDelta creation with case coercion and serialization."""
    now = time.time()
    delta = OrderBookDelta(
        symbol=" reliance ",
        side="bid",
        price=2500.5,
        quantity=100.0,
        action="add",
        timestamp=now,
        sequence_number=1,
        orders_count=5,
        exchange="nse",
    )

    assert delta.symbol == "RELIANCE"
    assert delta.side == BookSide.BID
    assert delta.action == DeltaAction.ADD
    assert delta.exchange == "NSE"
    assert delta.price == 2500.5
    assert delta.quantity == 100.0
    assert delta.sequence_number == 1
    assert delta.orders_count == 5

    d = delta.to_dict()
    assert d["symbol"] == "RELIANCE"
    assert d["side"] == "BID"
    assert d["action"] == "ADD"
    assert d["price"] == 2500.5
    assert d["quantity"] == 100.0


def test_order_book_delta_validation_failures() -> None:
    """Test fail-closed validation on invalid delta parameters."""
    now = time.time()

    # Empty symbol
    with pytest.raises(ValidationError, match="symbol"):
        OrderBookDelta(symbol="", side=BookSide.BID, price=100.0, quantity=10.0, action=DeltaAction.ADD, timestamp=now)

    # Invalid side
    with pytest.raises(ValidationError, match="side"):
        OrderBookDelta(symbol="NIFTY", side="INVALID", price=100.0, quantity=10.0, action=DeltaAction.ADD, timestamp=now)

    # Invalid action
    with pytest.raises(ValidationError, match="action"):
        OrderBookDelta(symbol="NIFTY", side=BookSide.BID, price=100.0, quantity=10.0, action="CANCEL", timestamp=now)

    # Non-positive price
    with pytest.raises(ValidationError, match="price"):
        OrderBookDelta(symbol="NIFTY", side=BookSide.BID, price=0.0, quantity=10.0, action=DeltaAction.ADD, timestamp=now)

    with pytest.raises(ValidationError, match="price"):
        OrderBookDelta(symbol="NIFTY", side=BookSide.BID, price=-50.0, quantity=10.0, action=DeltaAction.ADD, timestamp=now)

    with pytest.raises(ValidationError, match="price"):
        OrderBookDelta(symbol="NIFTY", side=BookSide.BID, price=float("nan"), quantity=10.0, action=DeltaAction.ADD, timestamp=now)

    # Negative quantity
    with pytest.raises(ValidationError, match="quantity"):
        OrderBookDelta(symbol="NIFTY", side=BookSide.BID, price=100.0, quantity=-1.0, action=DeltaAction.ADD, timestamp=now)

    # Negative sequence number
    with pytest.raises(ValidationError, match="sequence_number"):
        OrderBookDelta(symbol="NIFTY", side=BookSide.BID, price=100.0, quantity=10.0, action=DeltaAction.ADD, timestamp=now, sequence_number=-5)


# -----------------------------------------------------------------------------
# 2. L2OrderBookBuilder Mutation & Sorting Tests
# -----------------------------------------------------------------------------


def test_l2_builder_add_bid_and_ask_levels() -> None:
    """Test ADD operations insert discrete levels and compute BBO/spread correctly."""
    builder = L2OrderBookBuilder(symbol="RELIANCE")
    now = time.time()

    # Add 3 bids
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.BID, price=100.0, quantity=50, action=DeltaAction.ADD, timestamp=now, sequence_number=1))
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.BID, price=99.5, quantity=75, action=DeltaAction.ADD, timestamp=now, sequence_number=2))
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.BID, price=99.0, quantity=100, action=DeltaAction.ADD, timestamp=now, sequence_number=3))

    # Add 3 asks
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.ASK, price=100.5, quantity=40, action=DeltaAction.ADD, timestamp=now, sequence_number=4))
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.ASK, price=101.0, quantity=60, action=DeltaAction.ADD, timestamp=now, sequence_number=5))
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.ASK, price=101.5, quantity=80, action=DeltaAction.ADD, timestamp=now, sequence_number=6))

    assert builder.bids_count == 3
    assert builder.asks_count == 3
    assert builder.best_bid == 100.0
    assert builder.best_ask == 100.5
    assert builder.spread == 0.5
    assert not builder.is_crossed
    assert builder.deltas_processed == 6
    assert builder.deltas_rejected == 0


def test_l2_builder_sorting_invariants_and_depth_limit() -> None:
    """Verify bids sort descending, asks sort ascending, and depth_limit slices correctly."""
    builder = L2OrderBookBuilder(symbol="TCS", depth_limit=2)
    now = time.time()

    # Insert bids in scrambled price order
    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3000.0, quantity=10, action=DeltaAction.ADD, timestamp=now, sequence_number=1))
    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3020.0, quantity=20, action=DeltaAction.ADD, timestamp=now, sequence_number=2))
    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3010.0, quantity=30, action=DeltaAction.ADD, timestamp=now, sequence_number=3))

    # Insert asks in scrambled price order
    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.ASK, price=3050.0, quantity=15, action=DeltaAction.ADD, timestamp=now, sequence_number=4))
    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.ASK, price=3030.0, quantity=25, action=DeltaAction.ADD, timestamp=now, sequence_number=5))
    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.ASK, price=3040.0, quantity=35, action=DeltaAction.ADD, timestamp=now, sequence_number=6))

    snapshot = builder.get_snapshot()

    # Bids sorted descending: 3020, 3010 (top 2 because depth_limit=2)
    assert len(snapshot.bids) == 2
    assert snapshot.bids[0].price == 3020.0
    assert snapshot.bids[0].quantity == 20
    assert snapshot.bids[1].price == 3010.0

    # Asks sorted ascending: 3030, 3040 (top 2 because depth_limit=2)
    assert len(snapshot.asks) == 2
    assert snapshot.asks[0].price == 3030.0
    assert snapshot.asks[0].quantity == 25
    assert snapshot.asks[1].price == 3040.0


def test_l2_builder_update_existing_level() -> None:
    """Test UPDATE changes level quantity without duplicating the price tier."""
    builder = L2OrderBookBuilder(symbol="INFY")
    now = time.time()

    builder.apply_delta(OrderBookDelta(symbol="INFY", side=BookSide.BID, price=1800.0, quantity=50, action=DeltaAction.ADD, timestamp=now, sequence_number=1))
    assert builder.bids_count == 1
    assert builder.get_snapshot().bids[0].quantity == 50

    # Update quantity to 150
    builder.apply_delta(OrderBookDelta(symbol="INFY", side=BookSide.BID, price=1800.0, quantity=150, action=DeltaAction.UPDATE, timestamp=now, sequence_number=2))
    assert builder.bids_count == 1
    assert builder.get_snapshot().bids[0].quantity == 150


def test_l2_builder_delete_level_and_zero_quantity_removal() -> None:
    """Test explicit DELETE action and zero-quantity UPDATE/ADD remove the price level."""
    builder = L2OrderBookBuilder(symbol="INFY")
    now = time.time()

    builder.apply_delta(OrderBookDelta(symbol="INFY", side=BookSide.BID, price=1800.0, quantity=50, action=DeltaAction.ADD, timestamp=now, sequence_number=1))
    builder.apply_delta(OrderBookDelta(symbol="INFY", side=BookSide.BID, price=1790.0, quantity=80, action=DeltaAction.ADD, timestamp=now, sequence_number=2))
    assert builder.bids_count == 2

    # 1. Explicit DELETE
    builder.apply_delta(OrderBookDelta(symbol="INFY", side=BookSide.BID, price=1800.0, quantity=0, action=DeltaAction.DELETE, timestamp=now, sequence_number=3))
    assert builder.bids_count == 1
    assert builder.best_bid == 1790.0

    # 2. Zero-quantity UPDATE removes remaining level
    builder.apply_delta(OrderBookDelta(symbol="INFY", side=BookSide.BID, price=1790.0, quantity=0, action=DeltaAction.UPDATE, timestamp=now, sequence_number=4))
    assert builder.bids_count == 0
    assert builder.best_bid is None


# -----------------------------------------------------------------------------
# 3. Crossed-Book Detection & Fail-Closed Safety Tests
# -----------------------------------------------------------------------------


def test_l2_builder_crossed_book_rejection_and_state_rollback() -> None:
    """Test crossed order book (best_bid >= best_ask) is rejected and rolled back."""
    builder = L2OrderBookBuilder(symbol="RELIANCE")
    now = time.time()

    # Establish clean spread: Bid=100.0, Ask=102.0
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.BID, price=100.0, quantity=50, action=DeltaAction.ADD, timestamp=now, sequence_number=1))
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.ASK, price=102.0, quantity=50, action=DeltaAction.ADD, timestamp=now, sequence_number=2))

    assert builder.best_bid == 100.0
    assert builder.best_ask == 102.0
    assert builder.spread == 2.0

    # 1. Attempt to add Bid >= Ask (102.0 >= 102.0)
    with pytest.raises(ValidationError, match="Crossed order book invariant violated"):
        builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.BID, price=102.0, quantity=25, action=DeltaAction.ADD, timestamp=now, sequence_number=3))

    # Verify rollback: Bid 102 was NOT persisted; book remains Bid=100, Ask=102
    assert builder.best_bid == 100.0
    assert builder.best_ask == 102.0
    assert not builder.is_crossed
    assert builder.crossed_book_count == 1
    assert builder.deltas_rejected == 1

    # 2. Attempt to add Ask <= Bid (99.0 <= 100.0)
    with pytest.raises(ValidationError, match="Crossed order book invariant violated"):
        builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.ASK, price=99.0, quantity=25, action=DeltaAction.ADD, timestamp=now, sequence_number=4))

    # Verify rollback: Ask 99 was NOT persisted
    assert builder.best_bid == 100.0
    assert builder.best_ask == 102.0
    assert not builder.is_crossed
    assert builder.crossed_book_count == 2
    assert builder.deltas_rejected == 2


# -----------------------------------------------------------------------------
# 4. Sequence Monotonicity & Gap Detection Tests
# -----------------------------------------------------------------------------


def test_l2_builder_duplicate_and_out_of_order_sequence_rejection() -> None:
    """Test duplicate and lower sequence numbers fail closed."""
    builder = L2OrderBookBuilder(symbol="TCS")
    now = time.time()

    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3500.0, quantity=10, action=DeltaAction.ADD, timestamp=now, sequence_number=10))
    assert builder.last_sequence_number == 10

    # Duplicate sequence number 10
    with pytest.raises(ValidationError, match="Out-of-order or duplicate sequence"):
        builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3490.0, quantity=10, action=DeltaAction.ADD, timestamp=now, sequence_number=10))

    # Regressive sequence number 9
    with pytest.raises(ValidationError, match="Out-of-order or duplicate sequence"):
        builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3490.0, quantity=10, action=DeltaAction.ADD, timestamp=now, sequence_number=9))

    assert builder.deltas_rejected == 2
    assert builder.last_sequence_number == 10


def test_l2_builder_sequence_gap_detection_and_threshold() -> None:
    """Test sequence gaps increment anomaly counters and reject when exceeding threshold."""
    builder = L2OrderBookBuilder(symbol="TCS", max_allowed_gap=5)
    now = time.time()

    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3500.0, quantity=10, action=DeltaAction.ADD, timestamp=now, sequence_number=10))

    # Moderate gap: from 10 to 14 (missing 3: 11, 12, 13) -> gap <= 5 passes with warning
    builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3490.0, quantity=10, action=DeltaAction.ADD, timestamp=now, sequence_number=14))
    assert builder.sequence_gap_count == 1
    assert builder.last_sequence_number == 14

    # Excessive gap: from 14 to 30 (gap = 15 > max_allowed_gap=5) -> raises ValidationError
    with pytest.raises(ValidationError, match="Sequence gap .* exceeded max allowed"):
        builder.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.BID, price=3480.0, quantity=10, action=DeltaAction.ADD, timestamp=now, sequence_number=30))

    assert builder.sequence_gap_count == 2
    assert builder.deltas_rejected == 1


# -----------------------------------------------------------------------------
# 5. Immutable Snapshot Guarantees
# -----------------------------------------------------------------------------


def test_l2_builder_immutable_snapshot_guarantee() -> None:
    """Verify OrderBookSnapshot is truly immutable and unaffected by subsequent builder edits."""
    builder = L2OrderBookBuilder(symbol="RELIANCE")
    now = time.time()

    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.BID, price=2500.0, quantity=50, action=DeltaAction.ADD, timestamp=now, sequence_number=1))
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.ASK, price=2505.0, quantity=50, action=DeltaAction.ADD, timestamp=now, sequence_number=2))

    snapshot1 = builder.get_snapshot()
    assert snapshot1.best_bid == 2500.0
    assert snapshot1.best_ask == 2505.0

    # Attempt to mutate snapshot directly -> FrozenInstanceError
    with pytest.raises(FrozenInstanceError):
        snapshot1.symbol = "MUTATED"  # type: ignore

    # Mutate builder with new delta
    builder.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.BID, price=2501.0, quantity=100, action=DeltaAction.ADD, timestamp=now, sequence_number=3))

    # Verify snapshot1 was NOT altered by subsequent builder mutations
    assert snapshot1.best_bid == 2500.0
    assert len(snapshot1.bids) == 1

    # New snapshot reflects mutation
    snapshot2 = builder.get_snapshot()
    assert snapshot2.best_bid == 2501.0
    assert len(snapshot2.bids) == 2


# -----------------------------------------------------------------------------
# 6. Multi-Instrument L2OrderBookManager Tests
# -----------------------------------------------------------------------------


def test_l2_order_book_manager_multi_instrument() -> None:
    """Test L2OrderBookManager tracks multiple symbols independently."""
    manager = L2OrderBookManager(exchange="NSE")
    now = time.time()

    # Route delta to RELIANCE
    manager.apply_delta(OrderBookDelta(symbol="RELIANCE", side=BookSide.BID, price=2500.0, quantity=50, action=DeltaAction.ADD, timestamp=now, sequence_number=1))

    # Route delta to TCS
    manager.apply_delta(OrderBookDelta(symbol="TCS", side=BookSide.ASK, price=4100.0, quantity=25, action=DeltaAction.ADD, timestamp=now, sequence_number=1))

    rel_snap = manager.get_snapshot("RELIANCE")
    tcs_snap = manager.get_snapshot("TCS")

    assert rel_snap is not None
    assert rel_snap.symbol == "RELIANCE"
    assert rel_snap.best_bid == 2500.0
    assert rel_snap.best_ask is None

    assert tcs_snap is not None
    assert tcs_snap.symbol == "TCS"
    assert tcs_snap.best_ask == 4100.0
    assert tcs_snap.best_bid is None

    # Reset RELIANCE only
    manager.reset("RELIANCE")
    assert manager.get_snapshot("RELIANCE") is None
    assert manager.get_snapshot("TCS") is not None


# -----------------------------------------------------------------------------
# 7. WebSocket Streaming L2 Delta Integration Tests
# -----------------------------------------------------------------------------


def test_websocket_feed_l2_delta_streaming_integration() -> None:
    """Test WebSocketMarketFeed consumes L2 deltas, reconstructs depth, and publishes MARKET_DEPTH_UPDATED."""

    async def _test() -> None:
        now = time.time()
        transport = MockWebSocketTransport()
        event_bus = EventBus()
        state_store = StateStore()

        depth_events: List[Event] = []
        event_bus.subscribe(EventType.MARKET_DEPTH_UPDATED, lambda e: depth_events.append(e))

        config = WebSocketFeedConfig()
        feed = WebSocketMarketFeed(
            config=config,
            transport=transport,
            event_bus=event_bus,
            state_store=state_store,
            clock=lambda: now,
        )

        # Enqueue 3 L2 deltas
        transport.enqueue_message({
            "type": "delta",
            "action": "ADD",
            "side": "BID",
            "symbol": "RELIANCE",
            "price": 2500.0,
            "quantity": 100.0,
            "timestamp": now,
            "sequence": 1,
        })
        transport.enqueue_message({
            "type": "delta",
            "action": "ADD",
            "side": "ASK",
            "symbol": "RELIANCE",
            "price": 2502.0,
            "quantity": 80.0,
            "timestamp": now,
            "sequence": 2,
        })
        transport.enqueue_message({
            "type": "delta",
            "action": "UPDATE",
            "side": "BID",
            "symbol": "RELIANCE",
            "price": 2500.0,
            "quantity": 250.0,
            "timestamp": now,
            "sequence": 3,
        })

        await feed.start_async()

        for _ in range(30):
            if len(depth_events) >= 3:
                break
            await asyncio.sleep(0.01)

        assert len(depth_events) == 3
        assert depth_events[0].event_type == EventType.MARKET_DEPTH_UPDATED
        assert depth_events[0].payload["symbol"] == "RELIANCE"

        # Verify feed reconstructed L2 book
        book = feed.get_order_book("RELIANCE")
        assert book is not None
        assert book.best_bid == 2500.0
        assert book.best_ask == 2502.0
        assert book.spread == 2.0
        assert book.bids[0].quantity == 250.0  # Updated quantity

        # Verify next_record() pulls the OrderBookSnapshot from buffer
        rec = feed.next_record()
        assert isinstance(rec, OrderBookSnapshot)
        assert rec.symbol == "RELIANCE"

        # Verify StateStore cache updated
        latest_books = state_store.get("latest_order_books", {})
        assert "RELIANCE" in latest_books
        assert latest_books["RELIANCE"]["best_bid"] == 2500.0

        await feed.stop_async()

    run_async(_test())


# -----------------------------------------------------------------------------
# 8. Execution Safety Invariant Assertions
# -----------------------------------------------------------------------------


def test_l2_depth_safety_invariants() -> None:
    """Verify that L2 depth engines have zero execution capabilities and preserve invariants."""
    builder = L2OrderBookBuilder(symbol="RELIANCE")
    manager = L2OrderBookManager()

    for target in (builder, manager):
        for forbidden in ("submit_order", "place_order", "cancel_order", "execute_order", "create_order", "broker"):
            assert not hasattr(target, forbidden), f"Forbidden method/attr found: {forbidden}"

    # Global settings execution invariants remain locked
    settings = Settings()
    assert settings.execution_locked is True
    assert settings.broker_provider == "none"
    assert settings.paper_execution_enabled is False
    assert settings.paper_trading_only is True
