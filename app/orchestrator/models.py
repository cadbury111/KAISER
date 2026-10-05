"""KAISER — Orchestrator Request & Response Domain Models.

Provides strongly-typed, immutable data structures for application request lifecycle:
- RequestStatus & InputLanguage enums
- OrchestratorRequest (validated, immutable incoming command)
- HandlerResult (standardized handler execution result)
- OrchestratorResponse (immutable structured response with audit traceability)
"""

from __future__ import annotations

import copy
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from app.core.events.event import _freeze_container
from app.core.exceptions import ValidationError


class RequestStatus(str, Enum):
    """Standardized lifecycle statuses for orchestrator requests."""

    RECEIVED = "RECEIVED"
    VALIDATING = "VALIDATING"
    ROUTING = "ROUTING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class InputLanguage(str, Enum):
    """Supported input languages/scripts accepted as request metadata.

    Note: At this stage language is metadata only. No translation or speech
    recognition is performed yet.
    """

    ENGLISH = "en"
    TAMIL = "ta"
    TANGLISH = "tanglish"
    MIXED = "mixed"


def normalize_language(lang: Optional[str]) -> str:
    """Normalize input language string to standard code or clean lower-case descriptor.

    Args:
        lang: Raw language identifier (e.g. 'en', 'ta', 'Tamil', 'tanglish', 'mixed').

    Returns:
        str: Normalized language identifier.
    """
    if not lang or not isinstance(lang, str) or not lang.strip():
        return InputLanguage.ENGLISH.value

    clean = lang.strip().lower()
    mapping = {
        "en": InputLanguage.ENGLISH.value,
        "english": InputLanguage.ENGLISH.value,
        "ta": InputLanguage.TAMIL.value,
        "tamil": InputLanguage.TAMIL.value,
        "tanglish": InputLanguage.TANGLISH.value,
        "tamil_english": InputLanguage.TANGLISH.value,
        "ta-en": InputLanguage.TANGLISH.value,
        "mixed": InputLanguage.MIXED.value,
        "mixed_en_ta": InputLanguage.MIXED.value,
        "mixed_ta_en": InputLanguage.MIXED.value,
    }
    return mapping.get(clean, clean)


@dataclass(frozen=True)
class OrchestratorRequest:
    """Immutable, validated request submitted to the KAISER Master Orchestrator.

    Attributes:
        request_id: Unique identifier for this request.
        correlation_id: Tracing identifier threading across lifecycle events.
        command: Target command or capability keyword to execute.
        input_language: Language metadata tag (e.g. 'en', 'ta', 'tanglish', 'mixed').
        source: Originating subsystem or channel (e.g. 'voice', 'cli', 'system').
        created_at: POSIX UTC timestamp when the request was initiated.
        context: Immutable metadata and contextual parameters.
    """

    request_id: str
    correlation_id: str
    command: str
    input_language: str = "en"
    source: str = "system"
    created_at: float = 0.0
    context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate core invariants and recursively freeze nested context."""
        if not self.request_id or not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValidationError("request_id must be a non-empty string.")
        object.__setattr__(self, "request_id", self.request_id.strip())

        if not self.command or not isinstance(self.command, str) or not self.command.strip():
            raise ValidationError("command must be a non-empty string.")
        object.__setattr__(self, "command", self.command.strip())

        if not self.correlation_id or not isinstance(self.correlation_id, str) or not self.correlation_id.strip():
            object.__setattr__(self, "correlation_id", uuid.uuid4().hex)
        else:
            object.__setattr__(self, "correlation_id", self.correlation_id.strip())

        if not self.source or not isinstance(self.source, str) or not self.source.strip():
            raise ValidationError("source must be a non-empty string.")
        object.__setattr__(self, "source", self.source.strip())

        norm_lang = normalize_language(self.input_language)
        object.__setattr__(self, "input_language", norm_lang)

        if self.created_at < 0:
            raise ValidationError(f"created_at must be a positive float, got {self.created_at}")
        resolved_created_at = time.time() if self.created_at == 0.0 else self.created_at
        object.__setattr__(self, "created_at", resolved_created_at)

        # Protect nested context against unintended mutations
        clean_ctx = dict(self.context or {})
        object.__setattr__(self, "context", _freeze_container(clean_ctx))

    @classmethod
    def create(
        cls,
        command: str,
        request_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        input_language: str = "en",
        source: str = "system",
        created_at: Optional[float] = None,
        context: Optional[Mapping[str, Any]] = None,
        parameters: Optional[Mapping[str, Any]] = None,
    ) -> OrchestratorRequest:
        """Factory method to construct an OrchestratorRequest with safe defaults.

        Args:
            command: Command name or capability identifier.
            request_id: Optional request identifier (generated if omitted).
            correlation_id: Optional correlation identifier (generated if omitted).
            input_language: Input language metadata (default 'en').
            source: Source channel tag (default 'system').
            created_at: Optional creation timestamp (defaults to current time).
            context: Optional contextual parameters dictionary.
            parameters: Optional alias for context dictionary.

        Returns:
            OrchestratorRequest: The validated, immutable request instance.
        """
        resolved_request_id = (request_id or uuid.uuid4().hex).strip()
        resolved_correlation_id = (correlation_id or uuid.uuid4().hex).strip()
        resolved_created_at = time.time() if created_at is None else float(created_at)
        raw_ctx = context if context is not None else (parameters or {})
        clean_context = copy.deepcopy(dict(raw_ctx))

        return cls(
            request_id=resolved_request_id,
            correlation_id=resolved_correlation_id,
            command=command,
            input_language=input_language,
            source=source,
            created_at=resolved_created_at,
            context=clean_context,
        )

    @property
    def iso_created_at(self) -> str:
        """Return the creation timestamp formatted as ISO-8601 UTC string."""
        return datetime.fromtimestamp(self.created_at, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert OrchestratorRequest into a serializable dictionary."""
        return {
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "command": self.command,
            "input_language": self.input_language,
            "source": self.source,
            "created_at": self.created_at,
            "iso_created_at": self.iso_created_at,
            "context": copy.deepcopy(dict(self.context)),
        }


@dataclass(frozen=True)
class HandlerResult:
    """Standardized result returned by a registered application command handler.

    Attributes:
        response_text: Human/voice readable response text.
        result: Primary structured result payload (dictionary, list, or primitive).
        errors: Tuple of error messages if handler encountered issues.
        status: Target completion status (default COMPLETED).
    """

    response_text: str = ""
    result: Any = None
    errors: Tuple[str, ...] = ()
    status: RequestStatus = RequestStatus.COMPLETED

    def __post_init__(self) -> None:
        """Ensure result and error collections are strictly frozen."""
        object.__setattr__(self, "result", _freeze_container(self.result))

        if isinstance(self.errors, (list, tuple)):
            clean_errors = tuple(str(e) for e in self.errors)
        elif isinstance(self.errors, str):
            clean_errors = (self.errors,)
        else:
            clean_errors = ()
        object.__setattr__(self, "errors", clean_errors)

        if not isinstance(self.status, RequestStatus):
            try:
                object.__setattr__(self, "status", RequestStatus(str(self.status)))
            except ValueError:
                object.__setattr__(self, "status", RequestStatus.FAILED)

    @property
    def is_success(self) -> bool:
        """True if the handler succeeded without errors."""
        return self.status == RequestStatus.COMPLETED and len(self.errors) == 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert HandlerResult into a serializable dictionary."""
        return {
            "response_text": self.response_text,
            "result": copy.deepcopy(self.result) if isinstance(self.result, (dict, list)) else self.result,
            "errors": list(self.errors),
            "status": self.status.value,
            "is_success": self.is_success,
        }


@dataclass(frozen=True)
class OrchestratorResponse:
    """Immutable structured response returned by the Master Orchestrator.

    Attributes:
        request_id: Request identifier matching the originating request.
        correlation_id: Tracing identifier matching the originating request.
        status: Final RequestStatus outcome.
        response_text: Human/voice readable response text.
        result: Structured output data or payload.
        errors: Tuple of descriptive error strings if the request failed.
        created_at: POSIX UTC timestamp when the request started.
        completed_at: POSIX UTC timestamp when the request completed.
    """

    request_id: str
    correlation_id: str
    status: RequestStatus
    response_text: str
    result: Any = None
    errors: Tuple[str, ...] = ()
    created_at: float = 0.0
    completed_at: Optional[float] = None

    def __post_init__(self) -> None:
        """Validate response invariants and freeze all nested payload data."""
        if not self.request_id or not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValidationError("request_id must be a non-empty string.")
        object.__setattr__(self, "request_id", self.request_id.strip())

        if not self.correlation_id or not isinstance(self.correlation_id, str) or not self.correlation_id.strip():
            raise ValidationError("correlation_id must be a non-empty string.")
        object.__setattr__(self, "correlation_id", self.correlation_id.strip())

        if not isinstance(self.status, RequestStatus):
            object.__setattr__(self, "status", RequestStatus(str(self.status)))

        # Freeze result container
        object.__setattr__(self, "result", _freeze_container(self.result))

        # Ensure errors is a tuple of strings
        if isinstance(self.errors, (list, tuple)):
            clean_errors = tuple(str(e) for e in self.errors)
        elif isinstance(self.errors, str):
            clean_errors = (self.errors,)
        else:
            clean_errors = ()
        object.__setattr__(self, "errors", clean_errors)

        # Invariant: Never fabricate successful status when errors are present
        if self.status == RequestStatus.COMPLETED and len(clean_errors) > 0:
            raise ValidationError(
                "Invariant violated: OrchestratorResponse cannot have status COMPLETED when errors are present."
            )

    @property
    def is_success(self) -> bool:
        """Check whether the request completed successfully without errors."""
        return self.status == RequestStatus.COMPLETED and len(self.errors) == 0

    @property
    def duration_ms(self) -> Optional[float]:
        """Total execution latency in milliseconds, if completed_at is recorded."""
        if self.completed_at is not None and self.created_at > 0:
            return round(max(0.0, (self.completed_at - self.created_at) * 1000.0), 2)
        return None

    @property
    def iso_created_at(self) -> str:
        """Return the creation timestamp formatted as ISO-8601 UTC string."""
        return datetime.fromtimestamp(self.created_at, tz=timezone.utc).isoformat()

    @property
    def iso_completed_at(self) -> Optional[str]:
        """Return the completion timestamp formatted as ISO-8601 UTC string."""
        if self.completed_at is not None:
            return datetime.fromtimestamp(self.completed_at, tz=timezone.utc).isoformat()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert OrchestratorResponse into a serializable dictionary."""
        return {
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "status": self.status.value,
            "response_text": self.response_text,
            "result": copy.deepcopy(self.result) if isinstance(self.result, (dict, list)) else self.result,
            "errors": list(self.errors),
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
            "iso_created_at": self.iso_created_at,
            "iso_completed_at": self.iso_completed_at,
            "is_success": self.is_success,
        }

    @classmethod
    def success(
        cls,
        request: OrchestratorRequest,
        response_text: str,
        result: Any = None,
        completed_at: Optional[float] = None,
    ) -> OrchestratorResponse:
        """Construct a successful OrchestratorResponse from a completed request.

        Args:
            request: The originating request.
            response_text: Human/voice feedback text.
            result: Structured result payload.
            completed_at: Optional completion timestamp (defaults to current time).

        Returns:
            OrchestratorResponse: Immutable response marked COMPLETED.
        """
        now = time.time() if completed_at is None else completed_at
        return cls(
            request_id=request.request_id,
            correlation_id=request.correlation_id,
            status=RequestStatus.COMPLETED,
            response_text=response_text,
            result=result,
            errors=(),
            created_at=request.created_at,
            completed_at=now,
        )

    @classmethod
    def failure(
        cls,
        request: Optional[OrchestratorRequest] = None,
        errors: Union[Sequence[str], str] = (),
        response_text: str = "",
        status: RequestStatus = RequestStatus.FAILED,
        completed_at: Optional[float] = None,
        request_id: str = "",
        correlation_id: str = "",
        created_at: Optional[float] = None,
    ) -> OrchestratorResponse:
        """Construct an error OrchestratorResponse.

        Args:
            request: Optional originating request instance.
            errors: Single error message or sequence of error strings.
            response_text: Optional error summary text.
            status: Target failure status (default FAILED).
            completed_at: Optional completion timestamp.
            request_id: Explicit request_id fallback if request is None.
            correlation_id: Explicit correlation_id fallback if request is None.
            created_at: Explicit creation timestamp fallback if request is None.

        Returns:
            OrchestratorResponse: Immutable response with error details.
        """
        now = time.time() if completed_at is None else completed_at
        resolved_req_id = request.request_id if request else (request_id or uuid.uuid4().hex)
        resolved_corr_id = request.correlation_id if request else (correlation_id or uuid.uuid4().hex)
        resolved_created_at = request.created_at if request else (created_at or now)

        if isinstance(errors, str):
            err_tuple = (errors,)
        elif isinstance(errors, (list, tuple)):
            err_tuple = tuple(str(e) for e in errors)
        else:
            err_tuple = (str(errors),)

        resolved_text = response_text or (err_tuple[0] if err_tuple else "Request failed.")

        return cls(
            request_id=resolved_req_id,
            correlation_id=resolved_corr_id,
            status=status,
            response_text=resolved_text,
            result=None,
            errors=err_tuple,
            created_at=resolved_created_at,
            completed_at=now,
        )

    @classmethod
    def cancelled(
        cls,
        request: OrchestratorRequest,
        reason: str = "Request cancelled",
        completed_at: Optional[float] = None,
    ) -> OrchestratorResponse:
        """Construct a cancelled OrchestratorResponse.

        Args:
            request: The originating request.
            reason: Cancellation reason.
            completed_at: Optional completion timestamp.

        Returns:
            OrchestratorResponse: Immutable response marked CANCELLED.
        """
        now = time.time() if completed_at is None else completed_at
        return cls(
            request_id=request.request_id,
            correlation_id=request.correlation_id,
            status=RequestStatus.CANCELLED,
            response_text=reason,
            result=None,
            errors=(reason,),
            created_at=request.created_at,
            completed_at=now,
        )
