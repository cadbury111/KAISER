"""KAISER — Master Orchestrator.

Implements the centralized, thread-safe application request lifecycle:
Request received
→ validate request
→ publish lifecycle event (REQUEST_RECEIVED)
→ resolve registered handler
→ execute handler (REQUEST_STARTED)
→ validate handler result
→ update operational state
→ publish completion/failure event (REQUEST_COMPLETED / REQUEST_FAILED)
→ return structured response (OrchestratorResponse)
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Union

from app.core.events import EventBus
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.orchestrator.lifecycle import RequestLifecycleManager
from app.orchestrator.models import (
    HandlerResult,
    OrchestratorRequest,
    OrchestratorResponse,
    RequestStatus,
)
from app.orchestrator.registry import (
    HandlerCallable,
    HandlerRegistry,
    UnknownCommandError,
    create_system_status_handler,
)

logger = get_logger("orchestrator.master")


class MasterOrchestrator:
    """Central application controller orchestrating request processing and subsystem routing.

    Key Architectural Invariants:
    1. Zero live trading or broker connectivity (execution engine remains locked).
    2. Strict lifecycle traceability with immutable Request and Response models.
    3. Error isolation: Handler or subscriber exceptions never corrupt application state.
    4. Deterministic handler resolution with explicit unknown-command reporting.
    5. Complete concurrency protection across multi-threaded request processing.
    """

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        registry: Optional[HandlerRegistry] = None,
        settings: Optional[Any] = None,
        name: str = "master_orchestrator",
    ) -> None:
        self.name = name
        self.event_bus = event_bus
        self.state_store = state_store
        self.settings = settings
        self.registry = registry or HandlerRegistry(name=f"{name}_registry")
        if not self.registry.has_command("system_status"):
            self.registry.register(
                command="system_status",
                handler=create_system_status_handler(state_store=state_store, settings=settings),
                description="Inspect KAISER foundational system readiness and safety invariants",
                capability="diagnostics",
                allow_override=True,
            )
        self._lock = threading.RLock()
        self._is_shutdown = False
        self._active_requests: Dict[str, OrchestratorRequest] = {}
        self._lifecycle = RequestLifecycleManager(
            event_bus=event_bus,
            state_store=state_store,
        )

        logger.info(
            "MasterOrchestrator '%s' initialized (event_bus=%s, state_store=%s)",
            self.name,
            "connected" if event_bus else "none",
            "connected" if state_store else "none",
        )

    @property
    def is_shutdown(self) -> bool:
        """Check whether the orchestrator has been shut down."""
        with self._lock:
            return self._is_shutdown

    @property
    def active_request_count(self) -> int:
        """Return the number of requests currently executing."""
        with self._lock:
            return len(self._active_requests)

    def register_handler(
        self,
        command: str,
        handler: HandlerCallable,
        description: str = "",
        capability: str = "",
        allow_override: bool = False,
    ) -> None:
        """Register a command handler in the orchestrator registry.

        Args:
            command: Explicit command or capability keyword.
            handler: Callable taking an OrchestratorRequest.
            description: Human-readable command description.
            capability: Functional capability grouping.
            allow_override: Allow replacement of an existing handler if True.
        """
        self.registry.register(
            command=command,
            handler=handler,
            description=description,
            capability=capability,
            allow_override=allow_override,
        )

    def handle_request(
        self,
        request: Union[OrchestratorRequest, Dict[str, Any]],
    ) -> OrchestratorResponse:
        """Execute the complete KAISER request lifecycle pipeline.

        Pipeline Stages:
        1. Check shutdown state.
        2. Normalize and validate input into OrchestratorRequest.
        3. Check duplicate active request IDs.
        4. Track active request and publish REQUEST_RECEIVED event.
        5. Resolve command handler deterministically from registry.
        6. Publish REQUEST_STARTED event.
        7. Execute handler with exception isolation.
        8. Validate and normalize handler output into OrchestratorResponse.
        9. Update StateStore telemetry metrics and publish completion event.
        10. Return immutable OrchestratorResponse.

        Args:
            request: OrchestratorRequest instance or dictionary of request fields.

        Returns:
            OrchestratorResponse: Immutable structured outcome.
        """
        # 1. Check shutdown state
        with self._lock:
            if self._is_shutdown:
                logger.warning("Rejected incoming request: Orchestrator '%s' is shut down.", self.name)
                return OrchestratorResponse.failure(
                    request=None,
                    errors=("Orchestrator is shut down. All new requests are rejected.",),
                    response_text="Orchestrator is currently shut down.",
                    status=RequestStatus.FAILED,
                )

        # 2. Normalize and validate request
        req: OrchestratorRequest
        if isinstance(request, OrchestratorRequest):
            req = request
        elif isinstance(request, dict):
            try:
                req = OrchestratorRequest.create(**request)
            except (ValidationError, TypeError, ValueError) as val_err:
                logger.error("Request validation failed for dictionary input: %s", val_err)
                return OrchestratorResponse.failure(
                    request=None,
                    errors=(f"Malformed request: {val_err}",),
                    response_text=f"Request validation error: {val_err}",
                    status=RequestStatus.FAILED,
                    request_id=str(request.get("request_id", "")),
                    correlation_id=str(request.get("correlation_id", "")),
                )
        else:
            return OrchestratorResponse.failure(
                request=None,
                errors=(f"Malformed request: Expected OrchestratorRequest or dict, got {type(request).__name__}",),
                response_text="Invalid request type submitted.",
                status=RequestStatus.FAILED,
            )

        # 3. Check for empty command string
        if not req.command or not req.command.strip():
            return OrchestratorResponse.failure(
                request=req,
                errors=("Empty command: command string cannot be blank.",),
                response_text="Command cannot be empty.",
                status=RequestStatus.FAILED,
            )

        # 4. Check duplicate active request IDs
        with self._lock:
            if req.request_id in self._active_requests:
                err_msg = f"Duplicate request_id '{req.request_id}' is actively executing."
                logger.warning(err_msg)
                return OrchestratorResponse.failure(
                    request=req,
                    errors=(err_msg,),
                    response_text="Duplicate active request detected.",
                    status=RequestStatus.FAILED,
                )
            self._active_requests[req.request_id] = req

        # 5. Track request received and publish event
        self._lifecycle.on_request_received(req)

        # 6. Resolve command handler deterministically
        try:
            handler = self.registry.get_handler(req.command)
        except UnknownCommandError as unk_err:
            with self._lock:
                self._active_requests.pop(req.request_id, None)
            response = OrchestratorResponse.failure(
                request=req,
                errors=(str(unk_err),),
                response_text=f"Unknown command: '{req.command}'. No handler registered.",
                status=RequestStatus.FAILED,
            )
            self._lifecycle.on_request_failed(req, response)
            return response

        # 7. Publish REQUEST_STARTED event
        self._lifecycle.on_request_started(req)

        # 8. Execute handler with exception isolation
        try:
            raw_result = handler(req)
        except Exception as exc:
            logger.error(
                "Handler exception during execution of command '%s' [id=%s]: %s",
                req.command,
                req.request_id,
                exc,
                exc_info=True,
            )
            with self._lock:
                self._active_requests.pop(req.request_id, None)
            response = OrchestratorResponse.failure(
                request=req,
                errors=(f"Handler exception: {exc}",),
                response_text=f"Error occurred while processing '{req.command}'.",
                status=RequestStatus.FAILED,
            )
            self._lifecycle.on_request_failed(req, response)
            return response

        # 9. Validate and normalize handler output
        response = self._normalize_handler_output(req, raw_result)

        # 10. Update operational state and dispatch completion/failure events
        with self._lock:
            self._active_requests.pop(req.request_id, None)

        if response.is_success:
            self._lifecycle.on_request_completed(req, response)
        else:
            self._lifecycle.on_request_failed(req, response)

        return response

    def _normalize_handler_output(
        self,
        request: OrchestratorRequest,
        raw_result: Any,
    ) -> OrchestratorResponse:
        """Validate and normalize diverse handler return types into OrchestratorResponse."""
        now = time.time()

        if isinstance(raw_result, HandlerResult):
            if not raw_result.is_success:
                return OrchestratorResponse.failure(
                    request=request,
                    errors=raw_result.errors or ("Handler reported failure.",),
                    response_text=raw_result.response_text or "Handler failed.",
                    status=raw_result.status if raw_result.status != RequestStatus.COMPLETED else RequestStatus.FAILED,
                    completed_at=now,
                )
            return OrchestratorResponse.success(
                request=request,
                response_text=raw_result.response_text or "Request completed successfully.",
                result=raw_result.result,
                completed_at=now,
            )

        if isinstance(raw_result, dict):
            # If the dictionary explicitly denotes an error
            if raw_result.get("status") == RequestStatus.FAILED.value or "error" in raw_result:
                error_msg = str(raw_result.get("error", "Handler returned error status."))
                return OrchestratorResponse.failure(
                    request=request,
                    errors=(error_msg,),
                    response_text=str(raw_result.get("response_text", error_msg)),
                    status=RequestStatus.FAILED,
                    completed_at=now,
                )
            response_text = str(raw_result.get("response_text", "Request completed successfully."))
            return OrchestratorResponse.success(
                request=request,
                response_text=response_text,
                result=raw_result,
                completed_at=now,
            )

        if isinstance(raw_result, str):
            return OrchestratorResponse.success(
                request=request,
                response_text=raw_result,
                result=None,
                completed_at=now,
            )

        if raw_result is None:
            return OrchestratorResponse.success(
                request=request,
                response_text="Request completed successfully.",
                result=None,
                completed_at=now,
            )

        # Fallback for structured objects / dataclasses
        try:
            return OrchestratorResponse.success(
                request=request,
                response_text="Request completed successfully.",
                result=raw_result,
                completed_at=now,
            )
        except Exception as exc:
            return OrchestratorResponse.failure(
                request=request,
                errors=(f"Malformed handler result: {exc}",),
                response_text="Could not construct valid response from handler output.",
                status=RequestStatus.FAILED,
                completed_at=now,
            )

    def cancel_request(self, request_id: str, reason: str = "Request cancelled by user") -> bool:
        """Cancel an actively executing request.

        Args:
            request_id: Request identifier to cancel.
            reason: Cancellation explanation.

        Returns:
            bool: True if request was actively running and cancelled, False otherwise.
        """
        with self._lock:
            req = self._active_requests.pop(request_id, None)

        if req is not None:
            response = OrchestratorResponse.cancelled(request=req, reason=reason)
            self._lifecycle.on_request_cancelled(req, response)
            logger.info("Cancelled request '%s' [reason='%s']", request_id, reason)
            return True

        return False

    def get_metrics(self) -> Dict[str, Any]:
        """Return operational telemetry snapshot."""
        return self._lifecycle.get_metrics_snapshot()

    def shutdown(self) -> None:
        """Gracefully shut down the orchestrator and terminate active requests."""
        with self._lock:
            self._is_shutdown = True
            active_ids = list(self._active_requests.keys())
            for rid in active_ids:
                req = self._active_requests.pop(rid, None)
                if req:
                    response = OrchestratorResponse.cancelled(
                        request=req,
                        reason="Master Orchestrator shutdown initiated.",
                    )
                    self._lifecycle.on_request_cancelled(req, response)

        logger.info("MasterOrchestrator '%s' shut down successfully.", self.name)
