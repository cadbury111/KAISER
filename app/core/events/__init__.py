"""KAISER — Events Core Package.

Exports the immutable Event model, standard EventTypes, and the EventBus.
"""

from app.core.events.bus import EventBus, EventHandler, HandlerFailure, WILDCARD_EVENT_TYPE
from app.core.events.event import Event, EventType

__all__ = [
    "Event",
    "EventType",
    "EventBus",
    "EventHandler",
    "HandlerFailure",
    "WILDCARD_EVENT_TYPE",
]
