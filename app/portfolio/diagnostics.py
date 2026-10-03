"""KAISER — Portfolio Analytics Observability, Auditability & Operational Diagnostics.

Provides a lightweight, secret-safe, bounded operational diagnostics and audit system:
- Structured operational event logging across report generation, retrieval, and server lifecycle.
- Bounded in-memory audit history with deterministic oldest-first eviction.
- Stable sequence ordering for events with identical timestamps.
- Sensitive credential, token, transcript, and local filesystem path scrubbing.
- Strict fault isolation: diagnostic recording errors never crash trading or analytics workflows.
- Thread-safe access with read-only query interfaces.

SAFETY & RESEARCH INVARIANTS:
1. Purely informational, research-only diagnostics.
2. In-memory bounded audit history; does NOT claim durable disk persistence.
3. Completely secret-free: credentials, tokens, and local paths are scrubbed.
4. Preserves Settings.execution_locked = True; zero trade execution paths exist.
"""

from __future__ import annotations

import collections
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import logging
import re
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from app.core.exceptions import ValidationError

logger = logging.getLogger("kaiser.portfolio.diagnostics")

# Default bounded retention limit
DEFAULT_DIAGNOSTICS_MAX_HISTORY = 100
MAX_FIELD_LENGTH = 256
MAX_DETAIL_ITEMS = 20

# Safe identifier pattern for run IDs and report IDs
SAFE_ID_REGEX = re.compile(r"^[A-Za-z0-9_\-\.]+$")

# Path sanitization regexes (Windows and Unix absolute paths)
WINDOWS_PATH_REGEX = re.compile(r"[A-Za-z]:\\[^\s\"'<>]+", re.IGNORECASE)
UNIX_PATH_REGEX = re.compile(r"/(?:Users|home|root|var|etc|tmp|opt|usr)/[^\s\"'<>]+", re.IGNORECASE)

# Secret sanitization patterns
CREDENTIAL_KEY_REGEX = re.compile(
    r"(?i)(api[_-]?key|secret|password|private[_-]?key|auth[_-]?token|bearer|credential|token)"
)
CREDENTIAL_VALUE_REGEX = re.compile(
    r"(?i)(api[_-]?key|secret|password|token|bearer|credential)\s*[:=]\s*['\"]?([A-Za-z0-9_\-\.\$\/+=]{6,})['\"]?"
)
TOKEN_PATTERNS = re.compile(
    r"(?:ghp_[A-Za-z0-9]{36}|ey[A-Za-z0-9_-]{15,}\.ey[A-Za-z0-9_-]{15,}\.[A-Za-z0-9_-]{15,}|[A-Fa-f0-9]{32,64})"
)


class DiagnosticEventType(str, Enum):
    """Categorized operational diagnostic event types."""

    # Report Generation Lifecycle
    REPORT_GENERATION_STARTED = "REPORT_GENERATION_STARTED"
    REPORT_GENERATION_SUCCEEDED = "REPORT_GENERATION_SUCCEEDED"
    REPORT_GENERATION_FAILED = "REPORT_GENERATION_FAILED"

    # Report Retrieval Lifecycle
    REPORT_RETRIEVAL_SUCCEEDED = "REPORT_RETRIEVAL_SUCCEEDED"
    REPORT_RETRIEVAL_UNAVAILABLE = "REPORT_RETRIEVAL_UNAVAILABLE"
    REPORT_RETRIEVAL_INVALID = "REPORT_RETRIEVAL_INVALID"
    REPORT_RETRIEVAL_FAILED = "REPORT_RETRIEVAL_FAILED"

    # Dashboard Server Lifecycle
    DASHBOARD_SERVER_STARTED = "DASHBOARD_SERVER_STARTED"
    DASHBOARD_SERVER_STOPPED = "DASHBOARD_SERVER_STOPPED"
    DASHBOARD_SERVER_START_FAILED = "DASHBOARD_SERVER_START_FAILED"

    # Data Quality & Provenance
    DATA_PROVENANCE_EVALUATED = "DATA_PROVENANCE_EVALUATED"

    # Analytics Service Errors
    ANALYTICS_SERVICE_ERROR = "ANALYTICS_SERVICE_ERROR"


class DiagnosticOutcome(str, Enum):
    """Standardized operational outcome classifications."""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    WARNING = "WARNING"
    INFO = "INFO"
    INVALID = "INVALID"
    UNAVAILABLE = "UNAVAILABLE"


def format_utc_iso(epoch_seconds: Optional[float]) -> str:
    """Format an epoch timestamp to a standardized UTC ISO-8601 string."""
    if epoch_seconds is None or epoch_seconds <= 0.0:
        return "1970-01-01T00:00:00Z"
    try:
        dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return "1970-01-01T00:00:00Z"


def sanitize_diagnostic_text(text: Any, max_length: int = MAX_FIELD_LENGTH) -> str:
    """Sanitize strings by scrubbing paths, credentials, and control characters."""
    if text is None:
        return ""
    s = str(text).replace("\x00", " ")

    # Scrub local file system paths
    s = WINDOWS_PATH_REGEX.sub("[REDACTED_PATH]", s)
    s = UNIX_PATH_REGEX.sub("[REDACTED_PATH]", s)

    # Scrub explicit credential assignments (e.g. key=xyz123)
    s = CREDENTIAL_VALUE_REGEX.sub(r"\1=[REDACTED]", s)

    # Scrub potential tokens or secrets
    s = TOKEN_PATTERNS.sub("[REDACTED_SECRET]", s)

    # Cap length
    if len(s) > max_length:
        s = s[: max_length - 3] + "..."
    return s


def sanitize_diagnostic_error_category(exc: Any) -> str:
    """Extract a clean, sanitized error category without revealing stack traces or paths."""
    if exc is None:
        return "UnknownError"
    if isinstance(exc, type):
        return exc.__name__
    if hasattr(exc, "__class__"):
        return exc.__class__.__name__
    return "Error"


def sanitize_diagnostic_details(
    raw_details: Optional[Dict[str, Any]],
    max_items: int = MAX_DETAIL_ITEMS,
    max_length: int = MAX_FIELD_LENGTH,
) -> Dict[str, Any]:
    """Sanitize and boundedly cap details dictionary."""
    if not raw_details or not isinstance(raw_details, dict):
        return {}

    sanitized: Dict[str, Any] = {}
    items_count = 0

    # Prohibited keys that may leak voice audio, transcripts, raw market ticks, or credentials
    prohibited_keys = {
        "audio",
        "audio_bytes",
        "transcript",
        "raw_transcript",
        "prompt",
        "raw_prompt",
        "user_voice",
        "market_bars",
        "market_data",
        "order_book",
        "password",
        "secret",
        "token",
        "key",
        "api_key",
        "credential",
    }

    for k, v in raw_details.items():
        if items_count >= max_items:
            break
        k_str = str(k).lower().strip()
        if any(p in k_str for p in prohibited_keys) or CREDENTIAL_KEY_REGEX.search(k_str):
            continue

        safe_key = sanitize_diagnostic_text(str(k), max_length=64)

        if isinstance(v, (int, bool)):
            sanitized[safe_key] = v
        elif isinstance(v, float):
            sanitized[safe_key] = round(v, 4)
        elif isinstance(v, Enum):
            sanitized[safe_key] = v.value
        elif isinstance(v, (list, tuple)):
            # Cap list length
            safe_list = [
                sanitize_diagnostic_text(item, max_length=64)
                for item in v[:5]
            ]
            sanitized[safe_key] = safe_list
        elif isinstance(v, dict):
            # Shallow bounded sub-dict
            safe_sub = {
                sanitize_diagnostic_text(sub_k, max_length=32): sanitize_diagnostic_text(sub_v, max_length=64)
                for sub_k, sub_v in list(v.items())[:5]
                if not any(p in str(sub_k).lower() for p in prohibited_keys)
            }
            sanitized[safe_key] = safe_sub
        else:
            sanitized[safe_key] = sanitize_diagnostic_text(v, max_length=max_length)

        items_count += 1

    return sanitized


@dataclass(frozen=True)
class OperationalDiagnosticEvent:
    """Strongly typed, immutable operational diagnostic event record."""

    event_id: str
    sequence_number: int
    timestamp: float
    timestamp_iso: str
    event_type: DiagnosticEventType
    outcome: DiagnosticOutcome
    correlation_id: str
    report_id: Optional[str] = None
    run_id: Optional[str] = None
    data_status: Optional[str] = None
    error_category: Optional[str] = None
    duration_ms: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to a clean dictionary for JSON rendering."""
        return {
            "event_id": self.event_id,
            "sequence_number": self.sequence_number,
            "timestamp": self.timestamp,
            "timestamp_iso": self.timestamp_iso,
            "event_type": self.event_type.value if isinstance(self.event_type, Enum) else str(self.event_type),
            "outcome": self.outcome.value if isinstance(self.outcome, Enum) else str(self.outcome),
            "correlation_id": self.correlation_id,
            "report_id": self.report_id,
            "run_id": self.run_id,
            "data_status": self.data_status,
            "error_category": self.error_category,
            "duration_ms": self.duration_ms,
            "details": copy.deepcopy(self.details),
        }


class PortfolioDiagnosticsService:
    """Bounded, thread-safe operational diagnostics and auditing service.

    Maintains a deterministic in-memory history of operational events,
    tracks event counts, manages bounded oldest-first eviction, and
    provides grounded status snapshots for dashboard and orchestrator consumption.
    """

    STATE_SNAPSHOT_KEY = "portfolio_diagnostics:snapshot"

    def __init__(
        self,
        max_history: int = DEFAULT_DIAGNOSTICS_MAX_HISTORY,
        event_bus: Optional[Any] = None,
        state_store: Optional[Any] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        """Initialize the diagnostics service.

        Args:
            max_history: Maximum number of events retained before deterministic oldest-first eviction.
            event_bus: Optional EventBus for publishing diagnostic notifications.
            state_store: Optional StateStore for maintaining centralized status snapshot.
            clock: Optional injected clock function for deterministic testing.
        """
        if max_history <= 0:
            raise ValidationError(f"max_history must be positive, got {max_history}")

        self.max_history = max_history
        self.event_bus = event_bus
        self.state_store = state_store
        self._clock = clock or time.time

        self._lock = threading.RLock()
        self._history: collections.deque[OperationalDiagnosticEvent] = collections.deque(maxlen=max_history)
        self._sequence_counter: int = 0
        self._total_recorded: int = 0
        self._total_evicted: int = 0

        self._counts_by_type: Dict[str, int] = collections.defaultdict(int)
        self._counts_by_outcome: Dict[str, int] = collections.defaultdict(int)

        self._latest_report_generation: Optional[Dict[str, Any]] = None
        self._latest_data_status: Optional[str] = None
        self._dashboard_lifecycle: Dict[str, Any] = {
            "state": "OFFLINE",
            "is_running": False,
            "host": None,
            "port": None,
            "url": None,
            "last_transition_iso": None,
        }

    def record_event(
        self,
        event_type: Union[DiagnosticEventType, str],
        outcome: Union[DiagnosticOutcome, str],
        correlation_id: Optional[str] = None,
        report_id: Optional[str] = None,
        run_id: Optional[str] = None,
        data_status: Optional[str] = None,
        error_category: Optional[str] = None,
        duration_ms: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> OperationalDiagnosticEvent:
        """Record a structured operational diagnostic event with strict fault isolation.

        Guarantees:
        1. Thread-safe execution under RLock.
        2. Deterministic oldest-first eviction when max_history is reached.
        3. Scrubbed secrets, sanitized local paths, and bounded string lengths.
        4. Stable sequence ordering.
        5. Fault isolation: if an unexpected error occurs during recording, it logs a warning
           and returns a fallback event rather than raising to caller.
        """
        try:
            now = self._clock()
            now_iso = format_utc_iso(now)

            # Resolve Enums
            if isinstance(event_type, DiagnosticEventType):
                resolved_type = event_type
            else:
                try:
                    resolved_type = DiagnosticEventType(str(event_type).upper())
                except ValueError:
                    resolved_type = DiagnosticEventType.ANALYTICS_SERVICE_ERROR

            if isinstance(outcome, DiagnosticOutcome):
                resolved_outcome = outcome
            else:
                try:
                    resolved_outcome = DiagnosticOutcome(str(outcome).upper())
                except ValueError:
                    resolved_outcome = DiagnosticOutcome.INFO

            # Sanitize identifiers
            safe_corr = sanitize_diagnostic_text(correlation_id or f"corr_{uuid.uuid4().hex[:8]}", max_length=64)
            safe_rep = sanitize_diagnostic_text(report_id, max_length=64) if report_id else None
            safe_run = sanitize_diagnostic_text(run_id, max_length=64) if run_id else None
            safe_status = sanitize_diagnostic_text(data_status, max_length=32) if data_status else None
            safe_err = sanitize_diagnostic_text(error_category, max_length=64) if error_category else None
            safe_dur = round(float(duration_ms), 2) if duration_ms is not None and duration_ms >= 0 else None
            safe_details = sanitize_diagnostic_details(details)

            with self._lock:
                self._sequence_counter += 1
                seq = self._sequence_counter

                # Track eviction if queue is at capacity
                if len(self._history) >= self.max_history:
                    self._total_evicted += 1

                event_id = f"diag_{now_iso[:10].replace('-', '')}_{seq:06d}"

                event = OperationalDiagnosticEvent(
                    event_id=event_id,
                    sequence_number=seq,
                    timestamp=now,
                    timestamp_iso=now_iso,
                    event_type=resolved_type,
                    outcome=resolved_outcome,
                    correlation_id=safe_corr,
                    report_id=safe_rep,
                    run_id=safe_run,
                    data_status=safe_status,
                    error_category=safe_err,
                    duration_ms=safe_dur,
                    details=safe_details,
                )

                self._history.append(event)
                self._total_recorded += 1
                self._counts_by_type[resolved_type.value] += 1
                self._counts_by_outcome[resolved_outcome.value] += 1

                # Update latest operational pointers
                if resolved_type in (
                    DiagnosticEventType.REPORT_GENERATION_STARTED,
                    DiagnosticEventType.REPORT_GENERATION_SUCCEEDED,
                    DiagnosticEventType.REPORT_GENERATION_FAILED,
                ):
                    self._latest_report_generation = {
                        "event_type": resolved_type.value,
                        "outcome": resolved_outcome.value,
                        "report_id": safe_rep,
                        "run_id": safe_run,
                        "timestamp_iso": now_iso,
                        "duration_ms": safe_dur,
                        "error_category": safe_err,
                    }

                if safe_status:
                    self._latest_data_status = safe_status

                if resolved_type == DiagnosticEventType.DASHBOARD_SERVER_STARTED:
                    self._dashboard_lifecycle = {
                        "state": "RUNNING",
                        "is_running": True,
                        "host": safe_details.get("host", "127.0.0.1"),
                        "port": safe_details.get("port", 8080),
                        "url": safe_details.get("url", "http://127.0.0.1:8080"),
                        "last_transition_iso": now_iso,
                    }
                elif resolved_type == DiagnosticEventType.DASHBOARD_SERVER_STOPPED:
                    self._dashboard_lifecycle = {
                        "state": "STOPPED",
                        "is_running": False,
                        "host": None,
                        "port": None,
                        "url": None,
                        "last_transition_iso": now_iso,
                    }
                elif resolved_type == DiagnosticEventType.DASHBOARD_SERVER_START_FAILED:
                    self._dashboard_lifecycle = {
                        "state": "FAILED",
                        "is_running": False,
                        "host": safe_details.get("host"),
                        "port": safe_details.get("port"),
                        "url": None,
                        "last_transition_iso": now_iso,
                        "error_category": safe_err,
                    }

            # Optional StateStore snapshot update
            if self.state_store is not None:
                try:
                    self.state_store.set(self.STATE_SNAPSHOT_KEY, self.get_snapshot())
                except Exception as ss_exc:
                    logger.debug("StateStore update failed in diagnostics (ignored): %s", ss_exc)

            # Optional EventBus publishing
            if self.event_bus is not None:
                try:
                    from app.core.events.event import Event, EventType
                    self.event_bus.publish(
                        Event.create(
                            event_type=getattr(EventType, "PORTFOLIO_DIAGNOSTIC_RECORDED", "PORTFOLIO_DIAGNOSTIC_RECORDED"),
                            source="portfolio_diagnostics",
                            correlation_id=safe_corr,
                            payload=event.to_dict(),
                        )
                    )
                except Exception as eb_exc:
                    logger.debug("EventBus publishing failed in diagnostics (ignored): %s", eb_exc)

            return event

        except Exception as exc:
            logger.warning("PortfolioDiagnosticsService encountered error recording event (fault isolated): %s", exc)
            fallback_now = time.time()
            return OperationalDiagnosticEvent(
                event_id=f"fallback_{int(fallback_now)}",
                sequence_number=-1,
                timestamp=fallback_now,
                timestamp_iso=format_utc_iso(fallback_now),
                event_type=DiagnosticEventType.ANALYTICS_SERVICE_ERROR,
                outcome=DiagnosticOutcome.FAILURE,
                correlation_id="fallback",
                error_category="DiagnosticsInternalFailure",
                details={"error": "Fallback diagnostic event due to recording failure."},
            )

    def get_history(
        self,
        limit: int = 50,
        offset: int = 0,
        event_type: Optional[str] = None,
        outcome: Optional[str] = None,
        newest_first: bool = True,
    ) -> List[OperationalDiagnosticEvent]:
        """Retrieve a bounded, queryable history of operational events.

        Guarantees:
        - Thread-safe read under lock.
        - Deterministic, stable ordering (ordered by (timestamp, sequence_number)).
        - Clamped pagination limits (1 <= limit <= 100, offset >= 0).
        """
        limit_clamped = max(1, min(100, int(limit)))
        offset_clamped = max(0, int(offset))

        with self._lock:
            # Sort with stable sequence_number tie-breaker
            sorted_events = sorted(
                self._history,
                key=lambda e: (e.timestamp, e.sequence_number),
                reverse=newest_first,
            )

            # Filter if requested
            filtered: List[OperationalDiagnosticEvent] = []
            for ev in sorted_events:
                if event_type and ev.event_type.value != str(event_type).upper():
                    continue
                if outcome and ev.outcome.value != str(outcome).upper():
                    continue
                filtered.append(ev)

            # Bounded slice
            return filtered[offset_clamped : offset_clamped + limit_clamped]

    def get_snapshot(self) -> Dict[str, Any]:
        """Expose a grounded, read-only operational diagnostics snapshot.

        Includes only verifiable state, explicit counts, and clearly documents
        the in-memory bounded retention policy.
        """
        with self._lock:
            now = self._clock()
            return {
                "dashboard_lifecycle": copy.deepcopy(self._dashboard_lifecycle),
                "counts": {
                    "total_recorded": self._total_recorded,
                    "total_evicted": self._total_evicted,
                    "current_history_size": len(self._history),
                    "max_history_limit": self.max_history,
                    "has_evicted": self._total_evicted > 0,
                    "by_type": dict(self._counts_by_type),
                    "by_outcome": dict(self._counts_by_outcome),
                },
                "latest_report_generation": copy.deepcopy(self._latest_report_generation),
                "latest_data_status": self._latest_data_status,
                "audit_retention": "bounded_in_memory",
                "retention_policy": f"in-memory FIFO bounded to {self.max_history} events (deterministic oldest-first eviction)",
                "generated_at_iso": format_utc_iso(now),
            }

    def clear_history(self) -> None:
        """Clear all in-memory audit history and reset counters (intended for test fixtures)."""
        with self._lock:
            self._history.clear()
            self._sequence_counter = 0
            self._total_recorded = 0
            self._total_evicted = 0
            self._counts_by_type.clear()
            self._counts_by_outcome.clear()
            self._latest_report_generation = None
            self._latest_data_status = None
