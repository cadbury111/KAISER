"""KAISER — Thread-Safe Event Bus.

Provides a publish-subscribe event routing infrastructure with:
- Error isolation across subscribers
- Correlation ID tracing and structured logging
- Event type filtering and wildcard subscriptions
- Graceful shutdown lifecycle management
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set

from app.core.events.event import Event, EventType
from app.core.exceptions import ServiceError
from app.core.logging_config import get_logger

logger = get_logger("events.bus")

EventHandler = Callable[[Event], Any]
WILDCARD_EVENT_TYPE = "*"


@dataclass(frozen=True)
class HandlerFailure:
    """Audit record capturing a subscriber handler execution failure."""

    event_id: str
    event_type: str
    correlation_id: str
    handler_name: str
    error: Exception


class EventBus:
    """Thread-safe, decoupled event broker for KAISER subsystems."""

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._lock = threading.RLock()
        self._subscribers: DefaultDict[str, List[EventHandler]] = defaultdict(list)
        self._is_shutdown: bool = False
        self._recent_failures: List[HandlerFailure] = []
        self._event_counter: int = 0

    @property
    def is_shutdown(self) -> bool:
        """Check whether the EventBus has been shut down."""
        with self._lock:
            return self._is_shutdown

    def subscribe(
        self,
        event_type: EventType | str,
        handler: EventHandler,
    ) -> EventHandler:
        """Register a subscriber callback for a specific event type or wildcard '*'.

        Args:
            event_type: EventType, string event name, or '*' for all events.
            handler: Callable taking a single immutable Event argument.

        Returns:
            EventHandler: The registered handler (allows use as a decorator).

        Raises:
            ServiceError: If the bus is shut down.
            ValueError: If handler is not callable.
        """
        if not callable(handler):
            raise ValueError(f"Handler must be callable: {handler}")

        resolved_type = (
            event_type.value
            if isinstance(event_type, EventType)
            else str(event_type).strip()
        )

        with self._lock:
            if self._is_shutdown:
                raise ServiceError(f"Cannot subscribe to EventBus '{self.name}': bus is shut down.")

            if handler not in self._subscribers[resolved_type]:
                self._subscribers[resolved_type].append(handler)
                logger.debug(
                    "Subscribed '%s' to event '%s' on bus '%s'",
                    getattr(handler, "__name__", repr(handler)),
                    resolved_type,
                    self.name,
                )

        return handler

    def unsubscribe(
        self,
        event_type: EventType | str,
        handler: EventHandler,
    ) -> bool:
        """Unregister a subscriber callback for a specific event type.

        Args:
            event_type: EventType, string event name, or '*'.
            handler: The callback previously registered.

        Returns:
            bool: True if handler was found and removed, False otherwise.
        """
        resolved_type = (
            event_type.value
            if isinstance(event_type, EventType)
            else str(event_type).strip()
        )

        with self._lock:
            handlers = self._subscribers.get(resolved_type, [])
            if handler in handlers:
                handlers.remove(handler)
                if not handlers:
                    del self._subscribers[resolved_type]
                logger.debug(
                    "Unsubscribed '%s' from event '%s' on bus '%s'",
                    getattr(handler, "__name__", repr(handler)),
                    resolved_type,
                    self.name,
                )
                return True
            return False

    def publish(self, event: Event) -> int:
        """Publish an event to all matching subscribers with error isolation.

        Args:
            event: The immutable Event to dispatch.

        Returns:
            int: Total number of handlers invoked.

        Raises:
            ServiceError: If the bus has been shut down.
            TypeError: If event is not an instance of Event.
        """
        if not isinstance(event, Event):
            raise TypeError(f"Expected Event instance, got: {type(event).__name__}")

        with self._lock:
            if self._is_shutdown:
                raise ServiceError(f"Cannot publish on EventBus '{self.name}': bus is shut down.")

            self._event_counter += 1
            # Retrieve handlers matching specific type and wildcard handlers
            specific_handlers = list(self._subscribers.get(event.event_type, []))
            wildcard_handlers = list(self._subscribers.get(WILDCARD_EVENT_TYPE, []))
            target_handlers = specific_handlers + [
                h for h in wildcard_handlers if h not in specific_handlers
            ]

        logger.debug(
            "Publishing event '%s' [id=%s, correlation_id=%s, source=%s] to %d subscriber(s)",
            event.event_type,
            event.event_id,
            event.correlation_id,
            event.source,
            len(target_handlers),
        )

        dispatched_count = 0
        for handler in target_handlers:
            handler_name = getattr(handler, "__name__", repr(handler))
            try:
                handler(event)
                dispatched_count += 1
            except Exception as exc:
                # Isolate subscriber failure — never crash the event bus or disrupt other subscribers
                failure = HandlerFailure(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    correlation_id=event.correlation_id,
                    handler_name=handler_name,
                    error=exc,
                )
                with self._lock:
                    self._recent_failures.append(failure)
                    # Limit size of retained audit failures to prevent memory leak
                    if len(self._recent_failures) > 1000:
                        self._recent_failures.pop(0)

                logger.error(
                    "Subscriber failure in handler '%s' during event '%s' [id=%s, correlation_id=%s]: %s",
                    handler_name,
                    event.event_type,
                    event.event_id,
                    event.correlation_id,
                    exc,
                    exc_info=True,
                )

        return dispatched_count

    def get_subscriber_count(self, event_type: EventType | str | None = None) -> int:
        """Count active subscribers for a given type or across the entire bus."""
        with self._lock:
            if event_type is not None:
                resolved_type = (
                    event_type.value
                    if isinstance(event_type, EventType)
                    else str(event_type).strip()
                )
                return len(self._subscribers.get(resolved_type, []))
            # Unique total subscribers across all event types
            all_handlers: Set[EventHandler] = set()
            for handlers in self._subscribers.values():
                all_handlers.update(handlers)
            return len(all_handlers)

    def get_recent_failures(self) -> List[HandlerFailure]:
        """Return a copy of recent handler failures for audit/diagnostics."""
        with self._lock:
            return list(self._recent_failures)

    def clear_failures(self) -> None:
        """Clear the failure audit log."""
        with self._lock:
            self._recent_failures.clear()

    def shutdown(self) -> None:
        """Gracefully shut down the event bus and clear all subscriptions."""
        with self._lock:
            self._is_shutdown = True
            self._subscribers.clear()
            logger.info("EventBus '%s' successfully shut down.", self.name)
