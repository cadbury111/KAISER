"""KAISER Orchestrator Package.

Coordinates specialist trading and intelligence agents, request validation,
deterministic command routing, lifecycle tracing, and task orchestration
across the KAISER platform.
"""

from app.orchestrator.lifecycle import (
    RequestLifecycleManager,
    mask_sensitive_text,
    sanitize_container,
)
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import (
    HandlerResult,
    InputLanguage,
    OrchestratorRequest,
    OrchestratorResponse,
    RequestStatus,
    normalize_language,
)
from app.orchestrator.registry import (
    DuplicateHandlerError,
    HandlerCallable,
    HandlerRegistration,
    HandlerRegistry,
    OrchestratorError,
    UnknownCommandError,
    create_system_status_handler,
    system_status_handler,
)

__all__ = [
    "MasterOrchestrator",
    "HandlerRegistry",
    "HandlerRegistration",
    "HandlerCallable",
    "OrchestratorError",
    "UnknownCommandError",
    "DuplicateHandlerError",
    "system_status_handler",
    "create_system_status_handler",
    "OrchestratorRequest",
    "OrchestratorResponse",
    "HandlerResult",
    "RequestStatus",
    "InputLanguage",
    "normalize_language",
    "RequestLifecycleManager",
    "mask_sensitive_text",
    "sanitize_container",
]
