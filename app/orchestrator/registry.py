"""KAISER — Application Command Handler Registry.

Provides deterministic handler registration, lookup, capability mapping,
and execution safety without dynamic code execution.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from app.core.exceptions import KaiserError, ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.orchestrator.models import HandlerResult, OrchestratorRequest, RequestStatus

logger = get_logger("orchestrator.registry")

HandlerCallable = Callable[[OrchestratorRequest], Any]


class OrchestratorError(KaiserError):
    """Base exception for orchestrator subsystem errors."""


class UnknownCommandError(OrchestratorError):
    """Raised when an incoming request targets an unregistered command."""


class DuplicateHandlerError(OrchestratorError):
    """Raised when registering a handler for a command that already exists without allow_override=True."""


@dataclass(frozen=True)
class HandlerRegistration:
    """Immutable metadata descriptor for a registered command handler.

    Attributes:
        command: Normalized command keyword.
        handler: Callable handling the request.
        description: Functional description of the command.
        capability: Subsystem capability grouping (e.g. 'diagnostics', 'analysis').
        allow_override: Whether replacement is explicitly allowed.
    """

    command: str
    handler: HandlerCallable
    description: str = ""
    capability: str = ""
    allow_override: bool = False


class HandlerRegistry:
    """Thread-safe registry for application command handlers.

    Enforces:
    1. Deterministic command normalization (case-insensitive, whitespace-trimmed).
    2. Strict duplicate registration rejection unless allow_override=True.
    3. Zero dynamic code execution (only pre-registered callables).
    4. Comprehensive command and capability introspection.
    """

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._lock = threading.RLock()
        self._handlers: Dict[str, HandlerRegistration] = {}
        self._capability_index: Dict[str, List[str]] = defaultdict(list)

    @staticmethod
    def normalize_command(command: str) -> str:
        """Deterministically normalize command string.

        Args:
            command: Raw command string.

        Returns:
            str: Trimmed, lowercased command key.

        Raises:
            ValidationError: If command is not a non-empty string.
        """
        if not command or not isinstance(command, str) or not command.strip():
            raise ValidationError("Command must be a non-empty string.")
        return command.strip().lower()

    def register(
        self,
        command: str,
        handler: HandlerCallable,
        description: str = "",
        capability: str = "",
        allow_override: bool = False,
    ) -> HandlerRegistration:
        """Register a command handler.

        Args:
            command: Explicit command name.
            handler: Callable taking an OrchestratorRequest and returning a result.
            description: Human-readable description.
            capability: Optional capability tag for grouping.
            allow_override: If True, replaces existing registration without error.

        Returns:
            HandlerRegistration: The created immutable registration descriptor.

        Raises:
            ValidationError: If command or handler is invalid.
            DuplicateHandlerError: If command is already registered and allow_override is False.
        """
        norm_cmd = self.normalize_command(command)

        if not callable(handler):
            raise ValidationError(f"Handler for command '{norm_cmd}' must be callable.")

        with self._lock:
            existing = self._handlers.get(norm_cmd)
            if existing and not allow_override and not existing.allow_override:
                raise DuplicateHandlerError(
                    f"Command '{norm_cmd}' is already registered with handler "
                    f"'{getattr(existing.handler, '__name__', repr(existing.handler))}'. "
                    f"Set allow_override=True to permit replacement."
                )

            registration = HandlerRegistration(
                command=norm_cmd,
                handler=handler,
                description=description.strip(),
                capability=capability.strip().lower(),
                allow_override=allow_override,
            )
            self._handlers[norm_cmd] = registration

            if capability:
                cap_key = capability.strip().lower()
                if norm_cmd not in self._capability_index[cap_key]:
                    self._capability_index[cap_key].append(norm_cmd)

            logger.info(
                "Registered handler '%s' for command '%s' [capability='%s', override=%s]",
                getattr(handler, "__name__", repr(handler)),
                norm_cmd,
                registration.capability,
                allow_override,
            )
            return registration

    def unregister(self, command: str) -> bool:
        """Unregister a handler by command name.

        Args:
            command: Command name to unregister.

        Returns:
            bool: True if handler was found and removed, False otherwise.
        """
        norm_cmd = self.normalize_command(command)
        with self._lock:
            if norm_cmd in self._handlers:
                reg = self._handlers.pop(norm_cmd)
                if reg.capability and reg.capability in self._capability_index:
                    if norm_cmd in self._capability_index[reg.capability]:
                        self._capability_index[reg.capability].remove(norm_cmd)
                logger.info("Unregistered handler for command '%s'", norm_cmd)
                return True
            return False

    def has_command(self, command: str) -> bool:
        """Check whether a command is registered.

        Args:
            command: Command name to query.

        Returns:
            bool: True if registered, False otherwise.
        """
        try:
            norm_cmd = self.normalize_command(command)
        except ValidationError:
            return False
        with self._lock:
            return norm_cmd in self._handlers

    def get_handler(self, command: str) -> HandlerCallable:
        """Retrieve the registered handler for a command deterministically.

        Args:
            command: Command name to resolve.

        Returns:
            HandlerCallable: The callable associated with the command.

        Raises:
            UnknownCommandError: If command is not registered.
        """
        norm_cmd = self.normalize_command(command)
        with self._lock:
            registration = self._handlers.get(norm_cmd)
            if not registration:
                raise UnknownCommandError(
                    f"Unknown command: '{norm_cmd}'. "
                    f"Registered commands: {sorted(self._handlers.keys())}"
                )
            return registration.handler

    def get_registration(self, command: str) -> Optional[HandlerRegistration]:
        """Retrieve the registration descriptor for a command if it exists."""
        try:
            norm_cmd = self.normalize_command(command)
        except ValidationError:
            return None
        with self._lock:
            return self._handlers.get(norm_cmd)

    def list_commands(self) -> List[str]:
        """Return a sorted list of all currently registered command names."""
        with self._lock:
            return sorted(self._handlers.keys())

    def list_registrations(self) -> List[HandlerRegistration]:
        """Return a copy of all current registrations sorted by command."""
        with self._lock:
            return [self._handlers[cmd] for cmd in sorted(self._handlers.keys())]

    def get_commands_by_capability(self, capability: str) -> List[str]:
        """Return all commands mapped to a specific capability tag."""
        cap_key = capability.strip().lower()
        with self._lock:
            return sorted(self._capability_index.get(cap_key, []))

    @property
    def count(self) -> int:
        """Total number of registered commands."""
        with self._lock:
            return len(self._handlers)

    def clear(self) -> None:
        """Clear all registered handlers and capability mappings."""
        with self._lock:
            self._handlers.clear()
            self._capability_index.clear()
            logger.info("Cleared all registrations in HandlerRegistry '%s'", self.name)


# ==============================================================================
# SAFE PLACEHOLDER SYSTEM STATUS HANDLER
# ==============================================================================

def system_status_handler(request: OrchestratorRequest) -> HandlerResult:
    """Safe, read-only placeholder handler returning system operational status.

    Does not trigger live trading or broker actions.
    """
    return HandlerResult(
        response_text="KAISER system is operational. Trading engine: not initialized. Execution engine: locked.",
        result={
            "app_name": "KAISER",
            "system_status": "operational",
            "trading_engine": "not initialized",
            "execution_engine": "locked",
            "environment": "development",
        },
        status=RequestStatus.COMPLETED,
    )


def create_system_status_handler(
    state_store: Optional[StateStore] = None,
    settings: Optional[Any] = None,
) -> HandlerCallable:
    """Factory creating a system status handler wired to real StateStore/Settings.

    Args:
        state_store: Optional StateStore to inspect operational keys.
        settings: Optional Settings instance to inspect configuration.

    Returns:
        HandlerCallable: Safe status inspection callable.
    """
    def _status_handler(request: OrchestratorRequest) -> HandlerResult:
        app_name = getattr(settings, "app_name", "KAISER")
        environment = getattr(settings, "app_env", "development")

        system_status = "operational"
        trading_engine = "not initialized"
        execution_engine = "locked"

        if state_store is not None:
            try:
                system_status = state_store.get("system_status", system_status)
                trading_engine = state_store.get("trading_engine", trading_engine)
                execution_engine = state_store.get("execution_engine", execution_engine)
                environment = state_store.get("environment", environment)
            except Exception as exc:
                logger.warning("Error reading state_store during system_status: %s", exc)

        response_text = (
            f"{app_name} status is {system_status}. "
            f"Trading engine: {trading_engine}. "
            f"Execution engine: {execution_engine}."
        )

        return HandlerResult(
            response_text=response_text,
            result={
                "app_name": app_name,
                "environment": environment,
                "system_status": system_status,
                "trading_engine": trading_engine,
                "execution_engine": execution_engine,
            },
            status=RequestStatus.COMPLETED,
        )

    return _status_handler
