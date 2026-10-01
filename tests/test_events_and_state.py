"""KAISER — Event Bus & State Store Test Suite.

Comprehensive tests verifying:
1. Event creation, validation, and strict immutability
2. Event Bus routing, filtering, wildcard support, error isolation, ordering, and shutdown
3. State Store thread-safety, mutation protection, atomic updates, and security constraints
4. End-to-end integration flows across events and state
"""

import copy
import threading
import time
from dataclasses import FrozenInstanceError
from typing import List

import pytest

from app.core.events import (
    Event,
    EventBus,
    EventType,
    HandlerFailure,
    WILDCARD_EVENT_TYPE,
)
from app.core.exceptions import SecurityError, ServiceError
from app.core.state import StateStore


# ==============================================================================
# 1. EVENT IMMUTABILITY & MODEL TESTS
# ==============================================================================

class TestEventModel:
    """Test suite verifying Event creation, validation, and immutability invariants."""

    def test_event_create_defaults(self) -> None:
        """Verify default event creation populates required fields."""
        event = Event.create(
            event_type=EventType.VOICE_COMMAND_RECEIVED,
            source="voice",
            payload={"transcript": "market summary", "confidence": 0.95},
        )
        assert event.event_type == "VOICE_COMMAND_RECEIVED"
        assert event.source == "voice"
        assert len(event.event_id) > 0
        assert len(event.correlation_id) > 0
        assert event.timestamp > 0
        assert event.payload["transcript"] == "market summary"
        assert event.payload["confidence"] == 0.95
        assert isinstance(event.iso_timestamp, str)
        assert "T" in event.iso_timestamp

    def test_event_required_fields_validation(self) -> None:
        """Verify empty required fields raise ValueError."""
        with pytest.raises(ValueError, match="event_id must not be empty"):
            Event(
                event_id="",
                event_type="TEST",
                timestamp=time.time(),
                source="test",
                correlation_id="corr-1",
                payload={},
                metadata={},
            )

        with pytest.raises(ValueError, match="event_type must not be empty"):
            Event(
                event_id="id-1",
                event_type="",
                timestamp=time.time(),
                source="test",
                correlation_id="corr-1",
                payload={},
                metadata={},
            )

        with pytest.raises(ValueError, match="source must not be empty"):
            Event(
                event_id="id-1",
                event_type="TEST",
                timestamp=time.time(),
                source="",
                correlation_id="corr-1",
                payload={},
                metadata={},
            )

        with pytest.raises(ValueError, match="correlation_id must not be empty"):
            Event(
                event_id="id-1",
                event_type="TEST",
                timestamp=time.time(),
                source="test",
                correlation_id="",
                payload={},
                metadata={},
            )

    def test_event_attribute_immutability(self) -> None:
        """Verify top-level event attributes cannot be reassigned or modified."""
        event = Event.create(
            event_type=EventType.MARKET_QUOTE_UPDATED,
            source="market_data",
            payload={"symbol": "NIFTY", "price": 24500.0},
        )

        with pytest.raises((FrozenInstanceError, AttributeError)):
            event.source = "rogue_source"  # type: ignore

        with pytest.raises((FrozenInstanceError, AttributeError)):
            event.event_type = "ROGUE_TYPE"  # type: ignore

        with pytest.raises((FrozenInstanceError, AttributeError)):
            event.payload = {"symbol": "BANKNIFTY"}  # type: ignore

    def test_event_payload_mutation_protection(self) -> None:
        """Verify event payload and nested structures cannot be modified in-place."""
        original_dict = {
            "symbol": "NIFTY",
            "nested": {"level": 1, "tags": ["index", "fno"]},
        }
        event = Event.create(
            event_type=EventType.MARKET_QUOTE_UPDATED,
            source="market_data",
            payload=original_dict,
        )

        # 1. Mutating original dict after creation must not affect event
        original_dict["symbol"] = "MUTATED"
        original_dict["nested"]["level"] = 99
        assert event.payload["symbol"] == "NIFTY"
        assert event.payload["nested"]["level"] == 1

        # 2. Directly attempting to mutate event.payload raises TypeError
        with pytest.raises(TypeError):
            event.payload["symbol"] = "HACKED"  # type: ignore

        with pytest.raises(TypeError):
            event.payload["nested"]["level"] = 99  # type: ignore

    def test_event_custom_string_and_enums(self) -> None:
        """Verify events accept both EventType enum and arbitrary custom strings."""
        e1 = Event.create(event_type=EventType.RISK_APPROVAL_REQUIRED, source="risk")
        assert e1.event_type == "RISK_APPROVAL_REQUIRED"

        e2 = Event.create(event_type="CUSTOM_SIGNAL_DETECTED", source="quant")
        assert e2.event_type == "CUSTOM_SIGNAL_DETECTED"

    def test_event_to_dict_isolation(self) -> None:
        """Verify to_dict returns a deep-copied, serializable dictionary."""
        event = Event.create(
            event_type=EventType.ANALYSIS_REQUESTED,
            source="orchestrator",
            payload={"symbol": "RELIANCE", "timeframe": "1D"},
        )
        data = event.to_dict()
        assert data["event_type"] == "ANALYSIS_REQUESTED"
        assert data["payload"]["symbol"] == "RELIANCE"

        # Mutating data dict must not alter the event
        data["payload"]["symbol"] = "CHANGED"
        assert event.payload["symbol"] == "RELIANCE"


# ==============================================================================
# 2. EVENT BUS ROUTING & SAFETY TESTS
# ==============================================================================

class TestEventBus:
    """Test suite verifying Event Bus publish/subscribe, filtering, and error isolation."""

    def test_subscribe_and_publish_single_handler(self) -> None:
        """Verify subscriber receives published event."""
        bus = EventBus(name="test_bus")
        received: List[Event] = []

        def handler(event: Event) -> None:
            received.append(event)

        bus.subscribe(EventType.VOICE_COMMAND_RECEIVED, handler)
        event = Event.create(
            event_type=EventType.VOICE_COMMAND_RECEIVED,
            source="voice",
            payload={"text": "hello"},
        )
        dispatched = bus.publish(event)

        assert dispatched == 1
        assert len(received) == 1
        assert received[0].event_id == event.event_id

    def test_multiple_subscribers_same_event(self) -> None:
        """Verify multiple handlers registered for the same event type are all invoked."""
        bus = EventBus(name="multi_bus")
        results = []

        bus.subscribe("TEST_EVENT", lambda e: results.append("handler_1"))
        bus.subscribe("TEST_EVENT", lambda e: results.append("handler_2"))

        event = Event.create(event_type="TEST_EVENT", source="test")
        dispatched = bus.publish(event)

        assert dispatched == 2
        assert results == ["handler_1", "handler_2"]

    def test_unsubscribe(self) -> None:
        """Verify unregistering a handler stops event delivery."""
        bus = EventBus(name="unsub_bus")
        received = []

        def handler(event: Event) -> None:
            received.append(event)

        bus.subscribe("PING", handler)
        assert bus.get_subscriber_count("PING") == 1

        bus.publish(Event.create(event_type="PING", source="test"))
        assert len(received) == 1

        unsub_result = bus.unsubscribe("PING", handler)
        assert unsub_result is True
        assert bus.get_subscriber_count("PING") == 0

        bus.publish(Event.create(event_type="PING", source="test"))
        assert len(received) == 1  # No new events received

        # Unsubscribing a non-existent handler returns False
        assert bus.unsubscribe("PING", handler) is False

    def test_event_filtering_by_type(self) -> None:
        """Verify subscribers only receive events matching their registered type."""
        bus = EventBus(name="filter_bus")
        voice_events = []
        market_events = []

        bus.subscribe(EventType.VOICE_COMMAND_RECEIVED, lambda e: voice_events.append(e))
        bus.subscribe(EventType.MARKET_QUOTE_UPDATED, lambda e: market_events.append(e))

        bus.publish(Event.create(event_type=EventType.VOICE_COMMAND_RECEIVED, source="voice"))
        bus.publish(Event.create(event_type=EventType.MARKET_QUOTE_UPDATED, source="market"))

        assert len(voice_events) == 1
        assert voice_events[0].event_type == "VOICE_COMMAND_RECEIVED"
        assert len(market_events) == 1
        assert market_events[0].event_type == "MARKET_QUOTE_UPDATED"

    def test_wildcard_subscription(self) -> None:
        """Verify wildcard '*' subscriber receives all published events."""
        bus = EventBus(name="wildcard_bus")
        all_events = []

        bus.subscribe(WILDCARD_EVENT_TYPE, lambda e: all_events.append(e))

        bus.publish(Event.create(event_type=EventType.SYSTEM_INITIALIZED, source="system"))
        bus.publish(Event.create(event_type=EventType.MARKET_QUOTE_UPDATED, source="market"))
        bus.publish(Event.create(event_type=EventType.USER_APPROVED, source="human"))

        assert len(all_events) == 3
        types = [e.event_type for e in all_events]
        assert types == ["SYSTEM_INITIALIZED", "MARKET_QUOTE_UPDATED", "USER_APPROVED"]

    def test_subscriber_failure_isolation(self) -> None:
        """Verify an exception in one subscriber does NOT crash bus or stop others."""
        bus = EventBus(name="isolation_bus")
        execution_order = []

        def failing_handler(event: Event) -> None:
            execution_order.append("failing_start")
            raise RuntimeError("Catastrophic subscriber exception!")

        def healthy_handler(event: Event) -> None:
            execution_order.append("healthy_executed")

        bus.subscribe("TEST_FAULT", failing_handler)
        bus.subscribe("TEST_FAULT", healthy_handler)

        event = Event.create(event_type="TEST_FAULT", source="test")
        dispatched = bus.publish(event)

        # Healthy handler ran despite failing handler crashing
        assert "healthy_executed" in execution_order
        assert dispatched == 1

        # Check failure audit record
        failures = bus.get_recent_failures()
        assert len(failures) == 1
        failure = failures[0]
        assert failure.event_id == event.event_id
        assert failure.event_type == "TEST_FAULT"
        assert "failing_handler" in failure.handler_name
        assert isinstance(failure.error, RuntimeError)

    def test_correlation_id_preserved_across_bus(self) -> None:
        """Verify correlation_id is faithfully propagated to all handlers."""
        bus = EventBus(name="trace_bus")
        received_correlations = []

        bus.subscribe("TRACE_EVENT", lambda e: received_correlations.append(e.correlation_id))
        event = Event.create(
            event_type="TRACE_EVENT",
            source="test",
            correlation_id="corr-trace-9999",
        )
        bus.publish(event)

        assert received_correlations == ["corr-trace-9999"]

    def test_shutdown_lifecycle(self) -> None:
        """Verify shutdown clears subscriptions and blocks subsequent publish/subscribe."""
        bus = EventBus(name="shutdown_bus")
        bus.subscribe("EVENT", lambda e: None)
        assert bus.is_shutdown is False

        bus.shutdown()
        assert bus.is_shutdown is True
        assert bus.get_subscriber_count() == 0

        with pytest.raises(ServiceError, match="bus is shut down"):
            bus.publish(Event.create(event_type="EVENT", source="test"))

        with pytest.raises(ServiceError, match="bus is shut down"):
            bus.subscribe("EVENT", lambda e: None)

    def test_invalid_arguments_raise_error(self) -> None:
        """Verify type checking for publish and subscribe arguments."""
        bus = EventBus()
        with pytest.raises(ValueError, match="Handler must be callable"):
            bus.subscribe("EVENT", "not_a_callable")  # type: ignore

        with pytest.raises(TypeError, match="Expected Event instance"):
            bus.publish({"not": "an_event"})  # type: ignore


# ==============================================================================
# 3. STATE STORE TESTS
# ==============================================================================

class TestStateStore:
    """Test suite verifying StateStore methods, thread-safety, and mutation protection."""

    def test_set_and_get(self) -> None:
        """Verify basic set and get operations."""
        store = StateStore(name="test_store")
        store.set("market_status", "open")
        assert store.get("market_status") == "open"
        assert store.version == 1

    def test_get_missing_key_default(self) -> None:
        """Verify get on missing key returns provided default or None."""
        store = StateStore()
        assert store.get("non_existent") is None
        assert store.get("non_existent", default="fallback") == "fallback"

    def test_exists_and_delete(self) -> None:
        """Verify key existence check and deletion."""
        store = StateStore()
        store.set("active_symbol", "TATAMOTORS")
        assert store.exists("active_symbol") is True

        deleted = store.delete("active_symbol")
        assert deleted is True
        assert store.exists("active_symbol") is False

        # Deleting already deleted key returns False
        assert store.delete("active_symbol") is False

    def test_atomic_update_with_callable(self) -> None:
        """Verify atomic update using a transformation callable."""
        store = StateStore()
        store.set("quote_count", 10)

        new_val = store.update("quote_count", lambda current: current + 1)
        assert new_val == 11
        assert store.get("quote_count") == 11

    def test_atomic_update_with_dict_merge(self) -> None:
        """Verify atomic update with dictionary merge."""
        store = StateStore()
        store.set("position", {"symbol": "INFY", "quantity": 100, "pnl": 500.0})

        store.update("position", {"pnl": 750.0, "status": "holding"})
        updated = store.get("position")

        assert updated["symbol"] == "INFY"
        assert updated["quantity"] == 100
        assert updated["pnl"] == 750.0
        assert updated["status"] == "holding"

    def test_mutation_protection_on_get(self) -> None:
        """Verify mutating returned value does NOT mutate internal state."""
        store = StateStore()
        initial_data = {"symbols": ["NIFTY", "BANKNIFTY"], "meta": {"active": True}}
        store.set("watchlist", initial_data)

        retrieved = store.get("watchlist")
        retrieved["symbols"].append("FINNIFTY")
        retrieved["meta"]["active"] = False

        stored_state = store.get("watchlist")
        assert stored_state["symbols"] == ["NIFTY", "BANKNIFTY"]
        assert stored_state["meta"]["active"] is True

    def test_mutation_protection_on_set(self) -> None:
        """Verify mutating original object passed to set() does NOT mutate stored state."""
        store = StateStore()
        source_data = {"key": "initial_value"}
        store.set("data", source_data)

        source_data["key"] = "tampered_value"
        assert store.get("data")["key"] == "initial_value"

    def test_snapshot_consistency_and_isolation(self) -> None:
        """Verify snapshot returns consistent copy unaffected by caller mutation."""
        store = StateStore()
        store.set("risk_level", "low")
        store.set("max_drawdown", 0.05)

        snap = store.snapshot()
        assert snap == {"risk_level": "low", "max_drawdown": 0.05}

        # Modify snapshot dictionary
        snap["risk_level"] = "extreme"
        snap["new_key"] = "injected"

        # Verify internal store was completely unaffected
        assert store.get("risk_level") == "low"
        assert store.exists("new_key") is False

    def test_keys_and_clear(self) -> None:
        """Verify keys listing and clearing store."""
        store = StateStore()
        store.set("k1", 1)
        store.set("k2", 2)
        assert sorted(store.keys()) == ["k1", "k2"]

        store.clear()
        assert len(store.keys()) == 0
        assert store.snapshot() == {}

    def test_security_credential_rejection(self) -> None:
        """Verify StateStore actively refuses to store API keys or secrets."""
        store = StateStore()

        prohibited_keys = [
            "BROKER_API_KEY",
            "binance_secret_key",
            "user_password",
            "private_key_pem",
            "auth_token",
        ]

        for bad_key in prohibited_keys:
            with pytest.raises(SecurityError, match="Security violation"):
                store.set(bad_key, "secret_value_123")

            with pytest.raises(SecurityError, match="Security violation"):
                store.update(bad_key, {"token": "secret"})

    def test_thread_safety_concurrent_access(self) -> None:
        """Verify concurrent read, write, update, and snapshot operations under multi-threading."""
        store = StateStore()
        store.set("counter", 0)
        thread_count = 10
        increments_per_thread = 50
        errors: List[Exception] = []

        def worker() -> None:
            try:
                for _ in range(increments_per_thread):
                    store.update("counter", lambda c: c + 1)
                    _ = store.get("counter")
                    _ = store.snapshot()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(thread_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        expected_total = thread_count * increments_per_thread
        assert store.get("counter") == expected_total


# ==============================================================================
# 4. INTEGRATION FLOW TESTS
# ==============================================================================

class TestEventAndStateIntegration:
    """Test suite verifying end-to-end event and state coordination."""

    def test_voice_command_to_state_pipeline(self) -> None:
        """Test full pipeline: Voice command event -> State Store update -> Snapshot verification."""
        bus = EventBus(name="integration_bus")
        store = StateStore(name="integration_store")

        def on_voice_command(event: Event) -> None:
            store.set("active_voice_session", {
                "transcript": event.payload.get("transcript"),
                "language": event.payload.get("language"),
                "correlation_id": event.correlation_id,
                "timestamp": event.timestamp,
            })
            store.set("voice_status", "listening_processed")

        bus.subscribe(EventType.VOICE_COMMAND_RECEIVED, on_voice_command)

        # Trigger event
        corr_id = "trace-voice-session-101"
        bus.publish(
            Event.create(
                event_type=EventType.VOICE_COMMAND_RECEIVED,
                source="voice",
                payload={"transcript": "Show NIFTY chart", "language": "tanglish"},
                correlation_id=corr_id,
            )
        )

        snapshot = store.snapshot()
        assert snapshot["voice_status"] == "listening_processed"
        assert snapshot["active_voice_session"]["transcript"] == "Show NIFTY chart"
        assert snapshot["active_voice_session"]["language"] == "tanglish"
        assert snapshot["active_voice_session"]["correlation_id"] == corr_id

    def test_pipeline_continues_despite_failing_observer(self) -> None:
        """Verify that an audit observer failing does not disrupt state updates by other subscribers."""
        bus = EventBus(name="resilient_bus")
        store = StateStore(name="resilient_store")

        def buggy_logger(event: Event) -> None:
            raise ValueError("Buggy monitoring plugin failure")

        def quote_processor(event: Event) -> None:
            store.update(
                "quotes",
                {event.payload["symbol"]: event.payload["price"]},
                default={},
            )

        bus.subscribe(EventType.MARKET_QUOTE_UPDATED, buggy_logger)
        bus.subscribe(EventType.MARKET_QUOTE_UPDATED, quote_processor)

        bus.publish(
            Event.create(
                event_type=EventType.MARKET_QUOTE_UPDATED,
                source="market_data",
                payload={"symbol": "RELIANCE", "price": 2980.50},
            )
        )

        # State updated despite buggy_logger crashing
        assert store.get("quotes")["RELIANCE"] == 2980.50
        assert len(bus.get_recent_failures()) == 1
