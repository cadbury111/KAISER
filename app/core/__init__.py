"""KAISER — Core Foundation Package.

Provides centralized configuration, logging, and error handling.
"""

from app.core.config import Settings, get_settings
from app.core.exceptions import (
    ConfigurationError,
    ExecutionLockedError,
    KaiserError,
    RiskGateError,
    SecurityError,
    ServiceError,
    ValidationError,
)
from app.core.events import Event, EventBus, EventType, HandlerFailure
from app.core.logging_config import get_logger, setup_logging
from app.core.state import StateStore

__all__ = [
    "Settings",
    "get_settings",
    "setup_logging",
    "get_logger",
    "KaiserError",
    "ConfigurationError",
    "ValidationError",
    "ServiceError",
    "SecurityError",
    "ExecutionLockedError",
    "RiskGateError",
    "Event",
    "EventType",
    "EventBus",
    "HandlerFailure",
    "StateStore",
]
