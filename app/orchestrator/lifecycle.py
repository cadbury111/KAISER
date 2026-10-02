"""KAISER — Request Lifecycle & Operational State Manager.

Coordinates request lifecycle transitions, Event Bus publication, and bounded
State Store operational telemetry with strict credential sanitization and
subscriber failure isolation.
"""

from __future__ import annotations

import copy
import re
import threading
import time
from typing import Any, Dict, List, Mapping, Optional, Set

from app.core.events import Event, EventBus, EventType
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.orchestrator.models import OrchestratorRequest, OrchestratorResponse, RequestStatus

logger = get_logger("orchestrator.lifecycle")

# Sensitive keywords and token patterns to redact from logs and state
SENSITIVE_KEY_PATTERN = re.compile(
    r".*(api[_-]?key|secret|password|private[_-]?key|auth[_-]?token|credential).*",
    re.IGNORECASE,
)
SENSITIVE_VALUE_REGEX = re.compile(
    r"(?i)(bearer\s+[a-z0-9_\-\.]+|(?:api[_-]?key|secret|password)\s*[:=]\s*['\"]?[^\s,'\"]+['\"]?)",
)


def mask_sensitive_text(text: str) -> str:
    """Redact sensitive patterns (passwords, tokens, keys) in strings for safe logging."""
    if not isinstance(text, str):
        return str(text)
    return SENSITIVE_VALUE_REGEX.sub("[REDACTED]", text)


def sanitize_container(data: Any) -> Any:
    """Recursively scrub sensitive keys and text from dictionaries and lists."""
    if isinstance(data, (dict, Mapping)):
        clean: Dict[str, Any] = {}
        for k, v in data.items():
            if SENSITIVE_KEY_PATTERN.match(str(k)):
                clean[str(k)] = "[REDACTED]"
            else:
                clean[str(k)] = sanitize_container(v)
        return clean
    if isinstance(data, (list, tuple)):
        return [sanitize_container(item) for item in data]
    if isinstance(data, str):
        return mask_sensitive_text(data)
    return data


class RequestLifecycleManager:
    """Manages operational lifecycle transitions, event publishing, and state metrics.

    Ensures:
    1. Bounded operational state tracking in StateStore (never unbounded lists).
    2. Event Bus publishing with subscriber failure isolation.
    3. Complete correlation ID, request ID, and timestamp traceability.
    4. Secret-safe logging and persistence.
    """

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
    ) -> None:
        self.event_bus = event_bus
        self.state_store = state_store
        self._lock = threading.RLock()
        self._active_request_ids: Set[str] = set()

        # In-memory bounded counters
        self._counters = {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "cancelled": 0,
        }

        self._sync_state_counters()

    @property
    def active_request_count(self) -> int:
        """Number of requests currently undergoing active execution."""
        with self._lock:
            return len(self._active_request_ids)

    def _sync_state_counters(self) -> None:
        """Synchronize in-memory bounded metrics into StateStore safely."""
        if self.state_store is None:
            return
        with self._lock:
            active_count = len(self._active_request_ids)
            counters_payload = {
                "total": self._counters["total"],
                "completed": self._counters["completed"],
                "failed": self._counters["failed"],
                "cancelled": self._counters["cancelled"],
                "active": active_count,
            }
            status_desc = "idle" if active_count == 0 else f"processing_{active_count}"

        try:
            self.state_store.set("orchestrator_request_counters", counters_payload)
            self.state_store.set("orchestrator_current_status", status_desc)
            self.state_store.set("orchestrator_active_requests", sorted(list(self._active_request_ids)))
        except Exception as exc:
            logger.warning("Failed to update StateStore counters: %s", exc)

    def on_request_received(self, request: OrchestratorRequest) -> None:
        """Track incoming request arrival and broadcast REQUEST_RECEIVED event."""
        with self._lock:
            self._counters["total"] += 1
            self._active_request_ids.add(request.request_id)

        self._sync_state_counters()

        safe_context = sanitize_container(dict(request.context))
        logger.info(
            "Request received [id=%s, correlation_id=%s, cmd=%s, lang=%s, source=%s]",
            request.request_id,
            request.correlation_id,
            request.command,
            request.input_language,
            request.source,
        )

        if self.event_bus is not None:
            try:
                event = Event.create(
                    event_type=EventType.REQUEST_RECEIVED,
                    source="orchestrator",
                    correlation_id=request.correlation_id,
                    payload={
                        "request_id": request.request_id,
                        "command": request.command,
                        "input_language": request.input_language,
                        "source": request.source,
                        "created_at": request.created_at,
                        "context": safe_context,
                    },
                    metadata={
                        "request_id": request.request_id,
                        "correlation_id": request.correlation_id,
                        "status": RequestStatus.RECEIVED.value,
                    },
                )
                self.event_bus.publish(event)
            except Exception as exc:
                # Subscriber failure isolation: event bus issues must never disrupt request lifecycle
                logger.warning("EventBus dispatch failed for REQUEST_RECEIVED: %s", exc)

    def on_request_started(self, request: OrchestratorRequest) -> None:
        """Broadcast REQUEST_STARTED event when handler begins execution."""
        logger.debug(
            "Request processing started [id=%s, correlation_id=%s, cmd=%s]",
            request.request_id,
            request.correlation_id,
            request.command,
        )

        if self.event_bus is not None:
            try:
                event = Event.create(
                    event_type=EventType.REQUEST_STARTED,
                    source="orchestrator",
                    correlation_id=request.correlation_id,
                    payload={
                        "request_id": request.request_id,
                        "command": request.command,
                        "input_language": request.input_language,
                        "timestamp": time.time(),
                    },
                    metadata={
                        "request_id": request.request_id,
                        "correlation_id": request.correlation_id,
                        "status": RequestStatus.PROCESSING.value,
                    },
                )
                self.event_bus.publish(event)
            except Exception as exc:
                logger.warning("EventBus dispatch failed for REQUEST_STARTED: %s", exc)

    def on_request_completed(
        self,
        request: OrchestratorRequest,
        response: OrchestratorResponse,
    ) -> None:
        """Record successful completion, update bounded state, and publish event."""
        with self._lock:
            self._counters["completed"] += 1
            self._active_request_ids.discard(request.request_id)

        # Bounded last completed summary in StateStore
        if self.state_store is not None:
            try:
                completed_summary = {
                    "request_id": response.request_id,
                    "correlation_id": response.correlation_id,
                    "command": request.command,
                    "status": response.status.value,
                    "duration_ms": response.duration_ms,
                    "completed_at": response.completed_at,
                }
                self.state_store.set("orchestrator_last_completed", completed_summary)
            except Exception as exc:
                logger.warning("Failed to store last completed request in StateStore: %s", exc)

        self._sync_state_counters()

        logger.info(
            "Request completed successfully [id=%s, correlation_id=%s, cmd=%s, duration_ms=%s]",
            request.request_id,
            request.correlation_id,
            request.command,
            response.duration_ms,
        )

        if self.event_bus is not None:
            try:
                safe_result = sanitize_container(response.result)
                event = Event.create(
                    event_type=EventType.REQUEST_COMPLETED,
                    source="orchestrator",
                    correlation_id=response.correlation_id,
                    payload={
                        "request_id": response.request_id,
                        "command": request.command,
                        "status": response.status.value,
                        "response_text": mask_sensitive_text(response.response_text),
                        "result": safe_result,
                        "duration_ms": response.duration_ms,
                        "completed_at": response.completed_at,
                    },
                    metadata={
                        "request_id": response.request_id,
                        "correlation_id": response.correlation_id,
                        "status": response.status.value,
                    },
                )
                self.event_bus.publish(event)
            except Exception as exc:
                logger.warning("EventBus dispatch failed for REQUEST_COMPLETED: %s", exc)

    def on_request_failed(
        self,
        request: Optional[OrchestratorRequest],
        response: OrchestratorResponse,
    ) -> None:
        """Record request failure, update bounded error state, and publish failure event."""
        with self._lock:
            self._counters["failed"] += 1
            if request:
                self._active_request_ids.discard(request.request_id)
            else:
                self._active_request_ids.discard(response.request_id)

        safe_errors = [mask_sensitive_text(err) for err in response.errors]
        command_name = request.command if request else "unknown"

        # Bounded last error summary in StateStore
        if self.state_store is not None:
            try:
                error_summary = {
                    "request_id": response.request_id,
                    "correlation_id": response.correlation_id,
                    "command": command_name,
                    "status": response.status.value,
                    "error": safe_errors[0] if safe_errors else "Unknown error",
                    "errors": safe_errors,
                    "timestamp": response.completed_at or time.time(),
                }
                self.state_store.set("orchestrator_last_error", error_summary)
            except Exception as exc:
                logger.warning("Failed to store last error summary in StateStore: %s", exc)

        self._sync_state_counters()

        logger.error(
            "Request failed [id=%s, correlation_id=%s, cmd=%s, errors=%s]",
            response.request_id,
            response.correlation_id,
            command_name,
            safe_errors,
        )

        if self.event_bus is not None:
            try:
                event = Event.create(
                    event_type=EventType.REQUEST_FAILED,
                    source="orchestrator",
                    correlation_id=response.correlation_id,
                    payload={
                        "request_id": response.request_id,
                        "command": command_name,
                        "status": response.status.value,
                        "response_text": mask_sensitive_text(response.response_text),
                        "errors": safe_errors,
                        "completed_at": response.completed_at,
                    },
                    metadata={
                        "request_id": response.request_id,
                        "correlation_id": response.correlation_id,
                        "status": response.status.value,
                    },
                )
                self.event_bus.publish(event)
            except Exception as exc:
                logger.warning("EventBus dispatch failed for REQUEST_FAILED: %s", exc)

    def on_request_cancelled(
        self,
        request: OrchestratorRequest,
        response: OrchestratorResponse,
    ) -> None:
        """Record request cancellation and publish cancellation event."""
        with self._lock:
            self._counters["cancelled"] += 1
            self._active_request_ids.discard(request.request_id)

        self._sync_state_counters()

        logger.info(
            "Request cancelled [id=%s, correlation_id=%s, cmd=%s, reason=%s]",
            request.request_id,
            request.correlation_id,
            request.command,
            response.response_text,
        )

        if self.event_bus is not None:
            try:
                event = Event.create(
                    event_type=EventType.REQUEST_CANCELLED,
                    source="orchestrator",
                    correlation_id=response.correlation_id,
                    payload={
                        "request_id": response.request_id,
                        "command": request.command,
                        "status": RequestStatus.CANCELLED.value,
                        "reason": mask_sensitive_text(response.response_text),
                        "completed_at": response.completed_at,
                    },
                    metadata={
                        "request_id": response.request_id,
                        "correlation_id": response.correlation_id,
                        "status": RequestStatus.CANCELLED.value,
                    },
                )
                self.event_bus.publish(event)
            except Exception as exc:
                logger.warning("EventBus dispatch failed for REQUEST_CANCELLED: %s", exc)

    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """Return a point-in-time snapshot of request counters and active volume."""
        with self._lock:
            return {
                "total": self._counters["total"],
                "completed": self._counters["completed"],
                "failed": self._counters["failed"],
                "cancelled": self._counters["cancelled"],
                "active": len(self._active_request_ids),
                "active_ids": sorted(list(self._active_request_ids)),
            }
