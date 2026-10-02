"""KAISER — Master Orchestrator & Request Lifecycle Test Suite.

Comprehensive tests covering:
1. Request & Response validation, immutability, and container freezing
2. Handler Registry registration, deterministic lookup, duplicate rejection, and overrides
3. Request lifecycle execution (Received -> Validating -> Routing -> Processing -> Completed/Failed)
4. Correlation ID propagation across events and responses
5. Handler exceptions and malformed results handling
6. Event Bus lifecycle event emission and subscriber failure isolation
7. Bounded State Store operational metric tracking
8. Multi-threaded concurrency stress testing
9. Clean shutdown behavior and active request termination
10. Language metadata handling (English, Tamil, Tanglish, Mixed)
11. Secret-safe logging and persistence guardrails
12. Bootstrap subsystem integration and safety invariants
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from app.bootstrap import SystemStatus, bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator import (
    DuplicateHandlerError,
    HandlerRegistration,
    HandlerRegistry,
    HandlerResult,
    InputLanguage,
    MasterOrchestrator,
    OrchestratorRequest,
    OrchestratorResponse,
    RequestStatus,
    UnknownCommandError,
    create_system_status_handler,
    mask_sensitive_text,
    normalize_language,
    sanitize_container,
    system_status_handler,
)


# ==============================================================================
# 1. REQUEST & RESPONSE MODEL TESTS
# ==============================================================================

class TestRequestResponseModels:
    """Tests for typed request and response data models and immutability invariants."""

    def test_request_create_valid(self) -> None:
        """Verify OrchestratorRequest creation with valid parameters."""
        req = OrchestratorRequest.create(
            command="market_summary",
            context={"timeframe": "1D", "symbol": "NIFTY"},
            source="voice",
            input_language="en",
        )
        assert req.command == "market_summary"
        assert req.source == "voice"
        assert req.input_language == "en"
        assert req.context["symbol"] == "NIFTY"
        assert len(req.request_id) > 0
        assert len(req.correlation_id) > 0
        assert req.created_at > 0
        assert isinstance(req.iso_created_at, str)
        assert "T" in req.iso_created_at

    def test_request_validation_errors(self) -> None:
        """Verify invalid request fields raise ValidationError."""
        # Empty command
        with pytest.raises(ValidationError, match="command must be a non-empty string"):
            OrchestratorRequest(request_id="req-1", correlation_id="c-1", command="")

        # Empty request_id
        with pytest.raises(ValidationError, match="request_id must be a non-empty string"):
            OrchestratorRequest(request_id="", correlation_id="c-1", command="status")

        # Empty source
        with pytest.raises(ValidationError, match="source must be a non-empty string"):
            OrchestratorRequest(request_id="req-1", correlation_id="c-1", command="status", source="")

        # Negative created_at
        with pytest.raises(ValidationError, match="created_at must be a positive float"):
            OrchestratorRequest(request_id="req-1", correlation_id="c-1", command="status", created_at=-5.0)

    def test_request_immutability(self) -> None:
        """Verify top-level attributes and nested context cannot be mutated."""
        req = OrchestratorRequest.create(
            command="status",
            context={"params": {"level": 1}, "tags": ["prod", "live"]},
        )
        # Attribute reassignment blocked
        with pytest.raises(FrozenInstanceError):
            req.command = "new_command"  # type: ignore

        # Dict mutation blocked
        with pytest.raises(TypeError):
            req.context["new_key"] = "val"  # type: ignore

        # Nested dict mutation blocked
        with pytest.raises(TypeError):
            req.context["params"]["level"] = 2  # type: ignore

        # Nested list mutation blocked (converted to tuple)
        assert isinstance(req.context["tags"], tuple)

    def test_request_to_dict(self) -> None:
        """Verify request serialization to dictionary."""
        req = OrchestratorRequest.create(command="ping", context={"seq": 1})
        data = req.to_dict()
        assert data["command"] == "ping"
        assert data["context"]["seq"] == 1
        # Mutating exported dict does not mutate original request
        data["context"]["seq"] = 999
        assert req.context["seq"] == 1

    def test_response_create_success(self) -> None:
        """Verify OrchestratorResponse success factory and duration calculation."""
        t0 = 1000.0
        t1 = 1000.050  # 50 ms later
        req = OrchestratorRequest.create(command="ping", created_at=t0)
        resp = OrchestratorResponse.success(
            request=req,
            response_text="Pong",
            result={"status": "ok"},
            completed_at=t1,
        )
        assert resp.status == RequestStatus.COMPLETED
        assert resp.is_success is True
        assert resp.response_text == "Pong"
        assert resp.result["status"] == "ok"
        assert resp.duration_ms == 50.0
        assert resp.request_id == req.request_id
        assert resp.correlation_id == req.correlation_id

    def test_response_create_failure(self) -> None:
        """Verify OrchestratorResponse failure factory and properties."""
        req = OrchestratorRequest.create(command="query")
        resp = OrchestratorResponse.failure(
            request=req,
            errors=("Database connection timed out",),
            response_text="Query failed due to timeout.",
        )
        assert resp.status == RequestStatus.FAILED
        assert resp.is_success is False
        assert "Database connection timed out" in resp.errors
        assert resp.response_text == "Query failed due to timeout."

    def test_response_completed_with_errors_rejected(self) -> None:
        """Verify response cannot be marked COMPLETED when errors are present."""
        with pytest.raises(ValidationError, match="cannot have status COMPLETED when errors are present"):
            OrchestratorResponse(
                request_id="r1",
                correlation_id="c1",
                status=RequestStatus.COMPLETED,
                response_text="Done",
                errors=("Unexpected error",),
            )

    def test_response_immutability(self) -> None:
        """Verify top-level and nested response payload immutability."""
        resp = OrchestratorResponse(
            request_id="r1",
            correlation_id="c1",
            status=RequestStatus.COMPLETED,
            response_text="Done",
            result={"nested": {"data": 42}},
        )
        with pytest.raises(FrozenInstanceError):
            resp.status = RequestStatus.FAILED  # type: ignore

        with pytest.raises(TypeError):
            resp.result["nested"]["data"] = 100  # type: ignore

    def test_handler_result_immutability_and_defaults(self) -> None:
        """Verify HandlerResult immutability and success logic."""
        hr = HandlerResult(response_text="OK", result={"count": 10})
        assert hr.is_success is True
        assert hr.status == RequestStatus.COMPLETED
        with pytest.raises(FrozenInstanceError):
            hr.response_text = "Mutated"  # type: ignore


# ==============================================================================
# 2. HANDLER REGISTRY TESTS
# ==============================================================================

class TestHandlerRegistry:
    """Tests for command handler registration, deterministic lookup, and overrides."""

    def test_register_and_lookup_deterministic(self) -> None:
        """Verify registration and lookup normalizes commands deterministically."""
        registry = HandlerRegistry(name="test_registry")

        def dummy_handler(req: OrchestratorRequest) -> HandlerResult:
            return HandlerResult(response_text="Executed")

        reg = registry.register(
            command="GET_MARKET_SUMMARY",
            handler=dummy_handler,
            description="Returns market summary",
            capability="analytics",
        )
        assert reg.command == "get_market_summary"
        assert reg.capability == "analytics"

        # Case-insensitive and trimmed lookup
        handler1 = registry.get_handler("get_market_summary")
        handler2 = registry.get_handler("  GET_MARKET_SUMMARY  ")
        assert handler1 is dummy_handler
        assert handler2 is dummy_handler

    def test_duplicate_registration_rejected_without_override(self) -> None:
        """Verify registering existing command without allow_override raises DuplicateHandlerError."""
        registry = HandlerRegistry()

        def handler_a(req: OrchestratorRequest) -> HandlerResult:
            return HandlerResult(response_text="A")

        def handler_b(req: OrchestratorRequest) -> HandlerResult:
            return HandlerResult(response_text="B")

        registry.register("status", handler_a)

        with pytest.raises(DuplicateHandlerError, match="Command 'status' is already registered"):
            registry.register("status", handler_b, allow_override=False)

    def test_duplicate_registration_allowed_with_override(self) -> None:
        """Verify registering existing command with allow_override=True succeeds."""
        registry = HandlerRegistry()

        def handler_a(req: OrchestratorRequest) -> HandlerResult:
            return HandlerResult(response_text="A")

        def handler_b(req: OrchestratorRequest) -> HandlerResult:
            return HandlerResult(response_text="B")

        registry.register("status", handler_a)
        registry.register("status", handler_b, allow_override=True)

        resolved = registry.get_handler("status")
        assert resolved is handler_b

    def test_unknown_command_raises_error(self) -> None:
        """Verify resolving unregistered command raises UnknownCommandError with available commands."""
        registry = HandlerRegistry()
        registry.register("system_status", lambda req: HandlerResult(response_text="OK"))

        with pytest.raises(UnknownCommandError, match="Unknown command: 'nonexistent'"):
            registry.get_handler("nonexistent")

    def test_unregister_command(self) -> None:
        """Verify unregistering a command removes it from registry."""
        registry = HandlerRegistry()
        registry.register("ping", lambda req: HandlerResult(response_text="pong"), capability="diagnostics")
        assert registry.has_command("ping") is True

        assert registry.unregister("ping") is True
        assert registry.has_command("ping") is False
        assert registry.unregister("ping") is False

    def test_list_commands_and_capabilities(self) -> None:
        """Verify listing commands and indexing by capability."""
        registry = HandlerRegistry()
        registry.register("quote", lambda req: None, capability="market")
        registry.register("depth", lambda req: None, capability="market")
        registry.register("status", lambda req: None, capability="system")

        assert registry.list_commands() == ["depth", "quote", "status"]
        assert registry.get_commands_by_capability("market") == ["depth", "quote"]
        assert registry.get_commands_by_capability("system") == ["status"]

    def test_system_status_placeholder_handler(self) -> None:
        """Verify standalone system_status_handler returns safe read-only status."""
        req = OrchestratorRequest.create(command="system_status")
        result = system_status_handler(req)
        assert result.is_success is True
        assert result.result["trading_engine"] == "not initialized"
        assert result.result["execution_engine"] == "locked"

    def test_create_system_status_handler_reads_state_store(self) -> None:
        """Verify create_system_status_handler pulls active state from StateStore."""
        state_store = StateStore()
        state_store.set("system_status", "ready")
        state_store.set("trading_engine", "not initialized")
        state_store.set("execution_engine", "locked")
        state_store.set("environment", "test")

        handler = create_system_status_handler(state_store=state_store)
        req = OrchestratorRequest.create(command="system_status")
        result = handler(req)

        assert result.is_success is True
        assert result.result["system_status"] == "ready"
        assert result.result["environment"] == "test"


# ==============================================================================
# 3. MASTER ORCHESTRATOR REQUEST LIFECYCLE TESTS
# ==============================================================================

class TestMasterOrchestratorLifecycle:
    """Tests covering complete end-to-end request lifecycle execution."""

    def test_successful_request_lifecycle(self) -> None:
        """Verify complete successful request lifecycle with event emission and state tracking."""
        event_bus = EventBus(name="test_bus")
        state_store = StateStore(name="test_store")
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)

        events_received: List[Event] = []
        event_bus.subscribe("*", lambda ev: events_received.append(ev))

        def echo_handler(req: OrchestratorRequest) -> HandlerResult:
            return HandlerResult(
                response_text=f"Echo: {req.context.get('message', '')}",
                result={"ack": True},
                status=RequestStatus.COMPLETED,
            )

        orchestrator.register_handler("echo", echo_handler)

        req = OrchestratorRequest.create(
            command="echo",
            context={"message": "hello world"},
            source="test_client",
        )
        response = orchestrator.handle_request(req)

        # Assert response structure
        assert response.is_success is True
        assert response.status == RequestStatus.COMPLETED
        assert response.response_text == "Echo: hello world"
        assert response.result["ack"] is True
        assert response.request_id == req.request_id
        assert response.correlation_id == req.correlation_id
        assert response.duration_ms is not None
        assert response.duration_ms >= 0.0

        # Assert lifecycle events published in order
        event_types = [ev.event_type for ev in events_received]
        assert EventType.REQUEST_RECEIVED in event_types
        assert EventType.REQUEST_STARTED in event_types
        assert EventType.REQUEST_COMPLETED in event_types

        # Verify correlation ID is threaded through all events
        for ev in events_received:
            assert ev.correlation_id == req.correlation_id

        # Assert StateStore operational metrics
        counters = state_store.get("orchestrator_request_counters")
        assert counters["total"] == 1
        assert counters["completed"] == 1
        assert counters["failed"] == 0
        assert counters["active"] == 0

        last_completed = state_store.get("orchestrator_last_completed")
        assert last_completed["request_id"] == req.request_id
        assert last_completed["command"] == "echo"
        assert last_completed["status"] == RequestStatus.COMPLETED.value

    def test_dict_input_conversion(self) -> None:
        """Verify orchestrator accepts plain dict input and validates it into response."""
        orchestrator = MasterOrchestrator()
        orchestrator.register_handler(
            "ping",
            lambda req: HandlerResult(response_text="pong"),
        )
        response = orchestrator.handle_request({"command": "ping", "context": {"seq": 10}})
        assert response.is_success is True
        assert response.response_text == "pong"

    def test_unknown_command_returns_structured_error(self) -> None:
        """Verify unknown command emits failure event and returns structured FAILED response."""
        event_bus = EventBus()
        state_store = StateStore()
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)

        events: List[Event] = []
        event_bus.subscribe("*", lambda e: events.append(e))

        req = OrchestratorRequest.create(command="unregistered_action")
        response = orchestrator.handle_request(req)

        assert response.is_success is False
        assert response.status == RequestStatus.FAILED
        assert "Unknown command" in response.errors[0]

        event_types = [e.event_type for e in events]
        assert EventType.REQUEST_RECEIVED in event_types
        assert EventType.REQUEST_FAILED in event_types

        counters = state_store.get("orchestrator_request_counters")
        assert counters["total"] == 1
        assert counters["failed"] == 1
        assert counters["completed"] == 0

        last_error = state_store.get("orchestrator_last_error")
        assert last_error["request_id"] == req.request_id
        assert "Unknown command" in last_error["error"]

    def test_empty_command_rejected(self) -> None:
        """Verify submitting empty command returns structured failure."""
        orchestrator = MasterOrchestrator()
        response = orchestrator.handle_request({"command": "   "})
        assert response.status == RequestStatus.FAILED
        assert "Malformed request" in response.errors[0] or "Empty command" in response.errors[0]

    def test_malformed_input_type_rejected(self) -> None:
        """Verify non-dict, non-request input returns structured error without crashing."""
        orchestrator = MasterOrchestrator()
        response = orchestrator.handle_request(12345)  # type: ignore
        assert response.status == RequestStatus.FAILED
        assert "Malformed request" in response.errors[0]

    def test_handler_exception_isolation(self) -> None:
        """Verify exceptions thrown by a handler are safely trapped and returned as failure."""
        event_bus = EventBus()
        state_store = StateStore()
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)

        def exploding_handler(req: OrchestratorRequest) -> HandlerResult:
            raise RuntimeError("Unexpected subsystem crash!")

        orchestrator.register_handler("explode", exploding_handler)

        req = OrchestratorRequest.create(command="explode")
        response = orchestrator.handle_request(req)

        assert response.is_success is False
        assert response.status == RequestStatus.FAILED
        assert "Unexpected subsystem crash!" in response.errors[0]

        counters = state_store.get("orchestrator_request_counters")
        assert counters["failed"] == 1

    def test_handler_returning_failure_status(self) -> None:
        """Verify handler returning HandlerResult with FAILED status is normalized properly."""
        orchestrator = MasterOrchestrator()

        def failed_handler(req: OrchestratorRequest) -> HandlerResult:
            return HandlerResult(
                response_text="Risk limit exceeded",
                errors=("Max drawdown reached",),
                status=RequestStatus.FAILED,
            )

        orchestrator.register_handler("check_risk", failed_handler)

        req = OrchestratorRequest.create(command="check_risk")
        response = orchestrator.handle_request(req)

        assert response.is_success is False
        assert response.status == RequestStatus.FAILED
        assert "Max drawdown reached" in response.errors

    def test_handler_returning_string_or_dict(self) -> None:
        """Verify handlers returning raw string or raw dictionary are handled gracefully."""
        orchestrator = MasterOrchestrator()

        orchestrator.register_handler("say_hello", lambda req: "Hello there!")
        orchestrator.register_handler("get_data", lambda req: {"price": 100.5, "symbol": "INFY"})

        resp1 = orchestrator.handle_request(OrchestratorRequest.create(command="say_hello"))
        assert resp1.is_success is True
        assert resp1.response_text == "Hello there!"

        resp2 = orchestrator.handle_request(OrchestratorRequest.create(command="get_data"))
        assert resp2.is_success is True
        assert resp2.result["symbol"] == "INFY"


# ==============================================================================
# 4. EVENT BUS & STATE STORE INTEGRATION TESTS
# ==============================================================================

class TestEventAndStateIntegration:
    """Tests for EventBus subscriber failure isolation and StateStore bounded storage."""

    def test_subscriber_failure_isolation(self) -> None:
        """Verify a failing EventBus subscriber does NOT fail the request response."""
        event_bus = EventBus(name="fail_bus")
        state_store = StateStore()
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)

        # Flaky subscriber that raises on REQUEST_COMPLETED
        def crashing_subscriber(ev: Event) -> None:
            if ev.event_type == EventType.REQUEST_COMPLETED:
                raise ValueError("Subscriber crashed during audit log!")

        event_bus.subscribe(EventType.REQUEST_COMPLETED, crashing_subscriber)

        orchestrator.register_handler("test", lambda req: HandlerResult(response_text="Success!"))

        req = OrchestratorRequest.create(command="test")
        response = orchestrator.handle_request(req)

        # Invariant: Subscriber failure must not mark handler as failed
        assert response.is_success is True
        assert response.status == RequestStatus.COMPLETED
        assert response.response_text == "Success!"

        # Failure was captured in EventBus audit log
        failures = event_bus.get_recent_failures()
        assert len(failures) == 1
        assert failures[0].event_type == EventType.REQUEST_COMPLETED

    def test_state_store_bounded_memory(self) -> None:
        """Verify multiple requests do NOT cause unbounded history accumulation in StateStore."""
        state_store = StateStore()
        orchestrator = MasterOrchestrator(state_store=state_store)
        orchestrator.register_handler("ping", lambda req: HandlerResult(response_text="pong"))

        # Process 50 requests
        for i in range(50):
            orchestrator.handle_request(OrchestratorRequest.create(command="ping"))

        # Verify only bounded keys exist
        keys = state_store.keys()
        expected_keys = {
            "orchestrator_request_counters",
            "orchestrator_current_status",
            "orchestrator_active_requests",
            "orchestrator_last_completed",
        }
        assert set(keys).issubset(expected_keys)

        counters = state_store.get("orchestrator_request_counters")
        assert counters["total"] == 50
        assert counters["completed"] == 50
        assert counters["active"] == 0

        # No list of 50 requests exists
        active = state_store.get("orchestrator_active_requests")
        assert len(active) == 0

    def test_duplicate_active_request_id_rejected(self) -> None:
        """Verify submitting the same request_id while it is actively processing is rejected."""
        orchestrator = MasterOrchestrator()

        started_event = threading.Event()
        continue_event = threading.Event()

        def slow_handler(req: OrchestratorRequest) -> HandlerResult:
            started_event.set()
            continue_event.wait(timeout=2.0)
            return HandlerResult(response_text="Done")

        orchestrator.register_handler("slow", slow_handler)

        req1 = OrchestratorRequest.create(command="slow", request_id="fixed_req_id")
        req2 = OrchestratorRequest.create(command="slow", request_id="fixed_req_id")

        resp1_holder: List[OrchestratorResponse] = []

        def run_first() -> None:
            resp1 = orchestrator.handle_request(req1)
            resp1_holder.append(resp1)

        t1 = threading.Thread(target=run_first)
        t1.start()

        # Wait until req1 has entered handler and is actively tracked
        assert started_event.wait(timeout=2.0) is True

        # Submit req2 with identical request_id while req1 is actively processing
        resp2 = orchestrator.handle_request(req2)

        # Allow req1 to complete
        continue_event.set()
        t1.join(timeout=2.0)

        assert len(resp1_holder) == 1
        assert resp1_holder[0].is_success is True
        assert resp2.is_success is False
        assert resp2.status == RequestStatus.FAILED
        assert "Duplicate request_id" in resp2.errors[0]


# ==============================================================================
# 5. CONCURRENCY & LIFECYCLE TESTS
# ==============================================================================

class TestConcurrencyAndLifecycle:
    """Tests for multi-threaded concurrency and clean orchestrator shutdown."""

    def test_concurrent_multi_threaded_requests(self) -> None:
        """Verify multiple threads can concurrently process requests without race conditions."""
        event_bus = EventBus(name="concurrent_bus")
        state_store = StateStore(name="concurrent_store")
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)

        def math_handler(req: OrchestratorRequest) -> HandlerResult:
            val = int(req.context.get("value", 0))
            time.sleep(0.005)  # Simulate small compute
            return HandlerResult(
                response_text=f"Computed: {val * 2}",
                result={"result": val * 2},
            )

        orchestrator.register_handler("double", math_handler)

        num_threads = 16
        requests_per_thread = 5
        total_requests = num_threads * requests_per_thread

        def worker(worker_id: int) -> List[OrchestratorResponse]:
            responses: List[OrchestratorResponse] = []
            for i in range(requests_per_thread):
                req = OrchestratorRequest.create(
                    command="double",
                    context={"value": worker_id * 100 + i},
                )
                resp = orchestrator.handle_request(req)
                responses.append(resp)
            return responses

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, w) for w in range(num_threads)]
            all_responses = [resp for f in futures for resp in f.result()]

        assert len(all_responses) == total_requests
        assert all(r.is_success for r in all_responses)

        counters = state_store.get("orchestrator_request_counters")
        assert counters["total"] == total_requests
        assert counters["completed"] == total_requests
        assert counters["failed"] == 0
        assert counters["active"] == 0

    def test_shutdown_rejects_new_requests(self) -> None:
        """Verify shutting down the orchestrator rejects any new incoming requests."""
        orchestrator = MasterOrchestrator()
        orchestrator.register_handler("ping", lambda req: HandlerResult(response_text="pong"))

        orchestrator.shutdown()
        assert orchestrator.is_shutdown is True

        req = OrchestratorRequest.create(command="ping")
        resp = orchestrator.handle_request(req)

        assert resp.is_success is False
        assert resp.status == RequestStatus.FAILED
        assert "shut down" in resp.errors[0]

    def test_cancellation_of_active_request(self) -> None:
        """Verify cancel_request cancels an active request and triggers cancellation event."""
        event_bus = EventBus()
        orchestrator = MasterOrchestrator(event_bus=event_bus)

        events: List[Event] = []
        event_bus.subscribe("*", lambda e: events.append(e))

        barrier = threading.Barrier(2)
        cancelled_flag = threading.Event()

        def wait_handler(req: OrchestratorRequest) -> HandlerResult:
            barrier.wait(timeout=2.0)
            cancelled_flag.wait(timeout=2.0)
            return HandlerResult(response_text="Finished")

        orchestrator.register_handler("wait", wait_handler)

        target_req_id = "request_to_cancel_123"
        req = OrchestratorRequest.create(command="wait", request_id=target_req_id)

        def runner() -> None:
            orchestrator.handle_request(req)

        t = threading.Thread(target=runner)
        t.start()

        # Wait until request is actively running
        barrier.wait(timeout=2.0)
        assert orchestrator.active_request_count == 1

        # Cancel the request
        cancelled = orchestrator.cancel_request(target_req_id, reason="User requested cancel")
        assert cancelled is True

        cancelled_flag.set()
        t.join(timeout=2.0)

        # Check cancellation event
        event_types = [e.event_type for e in events]
        assert EventType.REQUEST_CANCELLED in event_types


# ==============================================================================
# 6. LANGUAGE METADATA TESTS
# ==============================================================================

class TestLanguageMetadata:
    """Tests verifying acceptance of multilingual metadata without speech/translation dependencies."""

    def test_english_language_request(self) -> None:
        """Verify standard English metadata acceptance."""
        req = OrchestratorRequest.create(command="status", input_language="en")
        assert req.input_language == "en"

    def test_tamil_language_request(self) -> None:
        """Verify Tamil script and metadata normalization."""
        # Command with Tamil script text
        req = OrchestratorRequest.create(
            command="system_status",
            input_language="ta",
            context={"transcript": "கணினி நிலை என்ன"},
        )
        assert req.input_language == "ta"
        assert req.context["transcript"] == "கணினி நிலை என்ன"

    def test_tanglish_language_request(self) -> None:
        """Verify Tanglish (Tamil in Roman script) metadata acceptance."""
        req = OrchestratorRequest.create(
            command="system_status",
            input_language="tanglish",
            context={"transcript": "system eppadi irukku"},
        )
        assert req.input_language == "tanglish"
        assert req.context["transcript"] == "system eppadi irukku"

    def test_mixed_language_request(self) -> None:
        """Verify mixed English/Tamil language metadata acceptance."""
        req = OrchestratorRequest.create(
            command="system_status",
            input_language="mixed",
            context={"transcript": "status என்ன"},
        )
        assert req.input_language == "mixed"

    def test_language_normalization_helper(self) -> None:
        """Verify normalize_language normalizes variations to canonical forms."""
        assert normalize_language("ENGLISH") == "en"
        assert normalize_language("Tamil") == "ta"
        assert normalize_language("tanglish") == "tanglish"
        assert normalize_language("mixed_en_ta") == "mixed"
        assert normalize_language("") == "en"
        assert normalize_language(None) == "en"


# ==============================================================================
# 7. SECURITY & SECRET-SAFE LOGGING TESTS
# ==============================================================================

class TestSecurityAndSecretSafeLogging:
    """Tests for credential masking in logs and StateStore mutation protection."""

    def test_mask_sensitive_text(self) -> None:
        """Verify regex replaces tokens, passwords, and API keys with [REDACTED]."""
        raw_msg = "Error using api_key='sk-123456789' and Bearer eyJhbGciOi"
        masked = mask_sensitive_text(raw_msg)
        assert "sk-123456789" not in masked
        assert "[REDACTED]" in masked

    def test_sanitize_container(self) -> None:
        """Verify dictionary keys matching secret patterns are scrubbed."""
        data = {
            "symbol": "NIFTY",
            "api_key": "secret_key_123",
            "nested": {
                "auth_token": "token_abc",
                "normal": "value",
            },
        }
        clean = sanitize_container(data)
        assert clean["symbol"] == "NIFTY"
        assert clean["api_key"] == "[REDACTED]"
        assert clean["nested"]["auth_token"] == "[REDACTED]"
        assert clean["nested"]["normal"] == "value"

    def test_sensitive_context_does_not_crash_state_store(self) -> None:
        """Verify request containing sensitive context keys is sanitized before state persistence."""
        state_store = StateStore()
        orchestrator = MasterOrchestrator(state_store=state_store)

        orchestrator.register_handler("test", lambda req: HandlerResult(response_text="ok"))

        # Context containing a prohibited key
        req = OrchestratorRequest.create(
            command="test",
            context={"api_key": "super_secret_value", "normal": "ok"},
        )

        # Should execute safely without raising SecurityError from StateStore
        resp = orchestrator.handle_request(req)
        assert resp.is_success is True


# ==============================================================================
# 8. BOOTSTRAP SUBSYSTEM INTEGRATION TESTS
# ==============================================================================

class TestBootstrapIntegration:
    """Tests verifying orchestrator integration into bootstrap without breaking safety guarantees."""

    def test_bootstrap_orchestrator_initialization(self) -> None:
        """Verify bootstrap_system initializes MasterOrchestrator with system_status handler."""
        status = bootstrap_system(log_to_file=False)

        assert status.orchestrator is not None
        assert isinstance(status.orchestrator, MasterOrchestrator)
        assert status.orchestrator.registry.has_command("system_status") is True

        # Safety guarantees preserved
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"

    def test_bootstrap_execute_system_status_request(self) -> None:
        """Verify executing system_status request via bootstrapped orchestrator."""
        status = bootstrap_system(log_to_file=False)
        assert status.orchestrator is not None

        req = OrchestratorRequest.create(command="system_status")
        response = status.orchestrator.handle_request(req)

        assert response.is_success is True
        assert response.result["trading_engine"] == "not initialized"
        assert response.result["execution_engine"] == "locked"
        assert response.result["environment"] == "development"

    def test_bootstrap_no_duplicate_handler_registration(self, caplog: pytest.LogCaptureFixture) -> None:
        """Regression test: verify system_status handler is registered exactly once without duplicate warning."""
        with caplog.at_level(logging.WARNING):
            status = bootstrap_system(log_to_file=False)

        assert status.orchestrator is not None
        assert status.orchestrator.registry.has_command("system_status") is True
        # Check that no warning was logged about duplicate handler or overwrite
        duplicate_warnings = [
            record.message
            for record in caplog.records
            if "duplicate" in record.message.lower() or "overwriting handler" in record.message.lower()
        ]
        assert len(duplicate_warnings) == 0, f"Found unexpected duplicate registration warnings: {duplicate_warnings}"
