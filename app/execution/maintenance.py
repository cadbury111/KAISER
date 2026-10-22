"""Automated Maintenance Scheduler for Safe Outbox Pruning and Periodic Health Digest Snapshots.

Phase 2, Step 66:
- Controlled lifecycle (explicit start, stop, restart, recover).
- Safe outbox retention and pruning hooks with active-session safety gating.
- Periodic immutable health digest snapshots with deterministic SHA-256 digests.
- Coordinated with PaperTradingWorkflowCoordinator session state.
- Bounded SQLite persistence for diagnostic runs and health digests.
- Error isolation and anti-overlapping execution guarantees.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union
from dataclasses import dataclass, field, asdict
from enum import Enum

from app.core.config import Settings, get_settings
from app.core.exceptions import ValidationError, ConfigurationError
from app.ledger.outbox import TransactionalOutbox, OutboxStatus, OutboxPruneResult

logger = logging.getLogger(__name__)

SENSITIVE_KEY_PATTERN = re.compile(
    r"(?:api_?key|access_?token|token|secret|password|bearer|auth|credential|private_?key)",
    re.IGNORECASE,
)

SENSITIVE_TEXT_PATTERN = re.compile(
    r"((?:token|auth_token|access_token|api_key|apikey|key|secret|bearer|password)\s*[:=]\s*['\"]?)([^'\"\s&]+)(['\"]?)",
    re.IGNORECASE,
)


def sanitize_sensitive_text(text: str) -> str:
    """Redact sensitive keys/tokens from error messages and text logs."""
    if not text or not isinstance(text, str):
        return str(text) if text is not None else ""
    return SENSITIVE_TEXT_PATTERN.sub(r"\1[REDACTED]\3", text)


def sanitize_dict_secrets(obj: Any) -> Any:
    """Recursively redact sensitive key-values from dictionaries/lists."""
    if isinstance(obj, dict):
        clean = {}
        for k, v in obj.items():
            if isinstance(k, str) and SENSITIVE_KEY_PATTERN.search(k):
                clean[k] = "[REDACTED]"
            else:
                clean[k] = sanitize_dict_secrets(v)
        return clean
    elif isinstance(obj, list):
        return [sanitize_dict_secrets(item) for item in obj]
    elif isinstance(obj, str):
        return sanitize_sensitive_text(obj)
    return obj


class MaintenanceHealthStatus(str, Enum):
    """Deterministic system health statuses."""

    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class WorkerStatus(str, Enum):
    """Operational lifecycle status for the maintenance background worker thread."""

    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    SHUTDOWN_TIMED_OUT = "SHUTDOWN_TIMED_OUT"
    DISABLED = "DISABLED"


class MaintenanceTaskStatus(str, Enum):
    """Structured outcomes for scheduled and manual maintenance tasks."""

    SUCCESS = "SUCCESS"
    SKIPPED_DISABLED = "SKIPPED_DISABLED"
    SKIPPED_ACTIVE_SESSION = "SKIPPED_ACTIVE_SESSION"
    SKIPPED_RECOVERY_INCOMPLETE = "SKIPPED_RECOVERY_INCOMPLETE"
    SKIPPED_OVERLAPPING = "SKIPPED_OVERLAPPING"
    FAILED = "FAILED"
    RUNNING = "RUNNING"
    INTERRUPTED = "INTERRUPTED"
    TIMED_OUT = "TIMED_OUT"
    UNKNOWN = "UNKNOWN"
    RECOVERED = "RECOVERED"


@dataclass(frozen=True)
class DurationSummary:
    """Statistical latency and duration summary."""

    count: int
    total_seconds: float
    min_seconds: Optional[float]
    max_seconds: Optional[float]
    mean_seconds: Optional[float]
    median_seconds: Optional[float]
    p90_seconds: Optional[float]
    p95_seconds: Optional[float]
    p99_seconds: Optional[float]
    calculation_method: str = "nearest_rank"
    sample_size: int = 0
    is_sufficient_sample: bool = False

    @property
    def sample_count(self) -> int:
        return self.count

    def __getitem__(self, item: str) -> Any:
        return self.to_dict()[item]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "count": self.count,
            "sample_count": self.count,
            "total_seconds": self.total_seconds,
            "min_seconds": self.min_seconds,
            "max_seconds": self.max_seconds,
            "mean_seconds": self.mean_seconds,
            "median_seconds": self.median_seconds,
            "p90_seconds": self.p90_seconds,
            "p95_seconds": self.p95_seconds,
            "p99_seconds": self.p99_seconds,
            "calculation_method": self.calculation_method,
            "sample_size": self.sample_size,
            "is_sufficient_sample": self.is_sufficient_sample,
        }


def compute_duration_summary(
    samples: Sequence[float],
    calculation_method: str = "nearest_rank",
) -> DurationSummary:
    """Compute deterministic latency and duration statistical summary.

    Calculation Method:
    - min_seconds / max_seconds: exact minimum and maximum of finite non-negative values.
    - mean_seconds: arithmetic average rounded to 6 decimal places.
    - median_seconds: exact 50th percentile (middle value for odd N, average of two middle values for even N).
    - p90_seconds, p95_seconds, p99_seconds: nearest rank percentile on sorted samples:
        rank = ceil((P / 100) * N)
        index = max(0, min(N - 1, rank - 1))
      To prevent unreliable extrapolation on small samples:
        - p90 requires sample size >= 5
        - p95 requires sample size >= 10
        - p99 requires sample size >= 20
      If sample size is below these thresholds, the percentile is reported as None.
      is_sufficient_sample is True when sample size >= 10.
    """
    clean_samples = [float(s) for s in samples if s is not None and math.isfinite(s) and s >= 0.0]
    n = len(clean_samples)
    if n == 0:
        return DurationSummary(
            count=0,
            total_seconds=0.0,
            min_seconds=None,
            max_seconds=None,
            mean_seconds=0.0,
            median_seconds=None,
            p90_seconds=None,
            p95_seconds=None,
            p99_seconds=None,
            calculation_method=calculation_method,
            sample_size=0,
            is_sufficient_sample=False,
        )

    tot = sum(clean_samples)
    sorted_s = sorted(clean_samples)
    min_val = sorted_s[0]
    max_val = sorted_s[-1]
    mean_val = tot / n

    mid = n // 2
    if n % 2 == 1:
        med_val = sorted_s[mid]
    else:
        med_val = (sorted_s[mid - 1] + sorted_s[mid]) / 2.0

    def _percentile(p: float) -> float:
        rank = math.ceil((p / 100.0) * n)
        idx = max(0, min(n - 1, rank - 1))
        return sorted_s[idx]

    p90 = round(_percentile(90.0), 6) if n >= 5 else None
    p95 = round(_percentile(95.0), 6) if n >= 10 else None
    p99 = round(_percentile(99.0), 6) if n >= 20 else None

    return DurationSummary(
        count=n,
        total_seconds=round(tot, 6),
        min_seconds=round(min_val, 6),
        max_seconds=round(max_val, 6),
        mean_seconds=round(mean_val, 6),
        median_seconds=round(med_val, 6),
        p90_seconds=p90,
        p95_seconds=p95,
        p99_seconds=p99,
        calculation_method=calculation_method,
        sample_size=n,
        is_sufficient_sample=(n >= 10),
    )


@dataclass(frozen=True)
class DiagnosticFailureSummary:
    """Structured, alert-ready summary of failures, timeouts, and recovery actions."""

    summary_id: str
    failure_type: str
    task_name: Optional[str]
    first_seen_at: float
    last_seen_at: float
    occurrence_count: int
    last_error_message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "summary_id": self.summary_id,
            "failure_type": self.failure_type,
            "task_name": self.task_name,
            "first_seen_at": self.first_seen_at,
            "last_seen_at": self.last_seen_at,
            "occurrence_count": self.occurrence_count,
            "last_error_message": sanitize_sensitive_text(self.last_error_message),
            "details": sanitize_dict_secrets(dict(self.details)),
        }


class TaskMetricsTracker:
    """Thread-safe bounded in-memory metrics tracker for a scheduled maintenance task."""

    def __init__(self, task_name: str, max_samples: int = 100) -> None:
        self.task_name = task_name
        self.max_samples = max(10, max_samples)
        self._lock = threading.Lock()
        self.attempts_total: int = 0
        self.completions_total: int = 0
        self.outcomes: Dict[str, int] = {
            MaintenanceTaskStatus.SUCCESS.value: 0,
            MaintenanceTaskStatus.FAILED.value: 0,
            "SKIPPED": 0,
            MaintenanceTaskStatus.SKIPPED_DISABLED.value: 0,
            MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION.value: 0,
            MaintenanceTaskStatus.SKIPPED_RECOVERY_INCOMPLETE.value: 0,
            MaintenanceTaskStatus.SKIPPED_OVERLAPPING.value: 0,
            MaintenanceTaskStatus.INTERRUPTED.value: 0,
            MaintenanceTaskStatus.TIMED_OUT.value: 0,
            MaintenanceTaskStatus.UNKNOWN.value: 0,
            MaintenanceTaskStatus.RECOVERED.value: 0,
        }
        self.consecutive_failures: int = 0
        self.last_successful_run: Optional[Dict[str, Any]] = None
        self.last_failed_run: Optional[Dict[str, Any]] = None
        self.duration_samples: List[float] = []
        self.last_duration_seconds: Optional[float] = None

    def record_attempt(self) -> None:
        with self._lock:
            self.attempts_total += 1

    def record_completion(self, result: MaintenanceTaskResult) -> None:
        with self._lock:
            self.completions_total += 1
            st_val = result.status.value if isinstance(result.status, MaintenanceTaskStatus) else str(result.status)
            self.outcomes[st_val] = self.outcomes.get(st_val, 0) + 1
            if st_val.startswith("SKIPPED"):
                self.outcomes["SKIPPED"] = self.outcomes.get("SKIPPED", 0) + 1

            dur = float(result.duration_seconds)
            if math.isfinite(dur) and dur >= 0.0:
                self.duration_samples.append(dur)
                if len(self.duration_samples) > self.max_samples:
                    self.duration_samples = self.duration_samples[-self.max_samples:]
                self.last_duration_seconds = round(dur, 6)

            if result.status == MaintenanceTaskStatus.SUCCESS:
                self.consecutive_failures = 0
                self.last_successful_run = result.to_dict()
            elif result.status in (MaintenanceTaskStatus.FAILED, MaintenanceTaskStatus.TIMED_OUT, MaintenanceTaskStatus.INTERRUPTED):
                self.consecutive_failures += 1
                self.last_failed_run = result.to_dict()

    @property
    def attempts(self) -> int:
        with self._lock:
            return self.attempts_total

    @property
    def completions(self) -> int:
        with self._lock:
            return self.completions_total

    @property
    def counts_by_outcome(self) -> Dict[str, int]:
        with self._lock:
            return dict(self.outcomes)

    def get_duration_samples(self) -> List[float]:
        with self._lock:
            return list(self.duration_samples)

    def to_dict(self) -> Dict[str, Any]:
        return self.get_summary()

    def reset(self) -> None:
        with self._lock:
            self.attempts_total = 0
            self.completions_total = 0
            for k in self.outcomes:
                self.outcomes[k] = 0
            self.consecutive_failures = 0
            self.duration_samples.clear()
            self.last_duration_seconds = None
            self.last_successful_run = None
            self.last_failed_run = None

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            dur_summary = compute_duration_summary(self.duration_samples).to_dict()
            return {
                "task_name": self.task_name,
                "attempts": self.attempts_total,
                "attempts_total": self.attempts_total,
                "completions": self.completions_total,
                "completions_total": self.completions_total,
                "outcomes": dict(self.outcomes),
                "counts_by_outcome": dict(self.outcomes),
                "consecutive_failures": self.consecutive_failures,
                "last_duration_seconds": self.last_duration_seconds,
                "last_successful_run": dict(self.last_successful_run) if self.last_successful_run else None,
                "last_failed_run": dict(self.last_failed_run) if self.last_failed_run else None,
                "duration_summary": dur_summary,
            }


@dataclass(frozen=True)
class MaintenanceTaskResult:
    """Immutable audit record representing the outcome of a single maintenance task run."""

    run_id: str
    task_name: str
    status: MaintenanceTaskStatus
    started_at: float
    completed_at: float
    duration_seconds: float
    records_inspected: int = 0
    records_processed: int = 0
    records_skipped: int = 0
    has_more: bool = False
    skip_reason: Optional[str] = None
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "task_name": self.task_name,
            "status": self.status.value if isinstance(self.status, MaintenanceTaskStatus) else str(self.status),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": round(self.duration_seconds, 6),
            "records_inspected": self.records_inspected,
            "records_processed": self.records_processed,
            "records_skipped": self.records_skipped,
            "has_more": self.has_more,
            "skip_reason": sanitize_sensitive_text(self.skip_reason) if self.skip_reason else None,
            "error_message": sanitize_sensitive_text(self.error_message) if self.error_message else None,
            "details": sanitize_dict_secrets(dict(self.details)),
        }


@dataclass(frozen=True)
class HealthDigestSnapshot:
    """Immutable, versioned health digest snapshot summarizing system diagnostics."""

    digest_id: str
    timestamp: float
    schema_version: int = 1
    correlation_id: Optional[str] = None
    runtime_status: str = "HEALTHY"
    scheduler_enabled: bool = False
    scheduler_running: bool = False
    execution_locked: bool = True
    paper_execution_enabled: bool = False
    broker_provider: str = "none"
    tasks_status: Dict[str, Any] = field(default_factory=dict)
    outbox_counts: Dict[str, int] = field(default_factory=dict)
    outbox_retention: Dict[str, Any] = field(default_factory=dict)
    outbox_dispatcher_running: Optional[bool] = None
    sessions_total: int = 0
    sessions_active: int = 0
    portfolio_reconciliation: Optional[Dict[str, Any]] = None
    latest_equity_snapshot_id: Optional[str] = None
    latest_equity_snapshot_timestamp: Optional[float] = None
    latest_equity_total: Optional[float] = None
    data_quality: Optional[Dict[str, Any]] = None
    recent_errors: List[str] = field(default_factory=list)
    health_reasons: List[str] = field(default_factory=list)
    worker_status: str = "STOPPED"
    scheduler_uptime_seconds: float = 0.0
    task_metrics: Dict[str, Any] = field(default_factory=dict)
    duration_summaries: Dict[str, Any] = field(default_factory=dict)
    failure_summaries: List[Dict[str, Any]] = field(default_factory=list)
    telemetry_sources: Dict[str, bool] = field(default_factory=dict)
    deterministic_digest: str = ""

    def __post_init__(self) -> None:
        if not self.digest_id or not isinstance(self.digest_id, str) or not self.digest_id.strip():
            raise ValidationError("HealthDigestSnapshot digest_id must be a non-empty string.")

        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"Invalid timestamp: {self.timestamp}. Must be positive finite float.")

        if self.schema_version <= 0:
            raise ValidationError(f"Invalid schema_version: {self.schema_version}. Must be positive integer.")

        if self.latest_equity_total is not None:
            if not math.isfinite(self.latest_equity_total) or self.latest_equity_total < 0:
                raise ValidationError(f"Invalid latest_equity_total: {self.latest_equity_total}. Must be non-negative finite float.")

        if not self.deterministic_digest:
            digest_str = self._compute_digest()
            object.__setattr__(self, "deterministic_digest", digest_str)

    def _compute_digest(self) -> str:
        """Compute SHA-256 fingerprint over core diagnostic health indicators."""
        core_state = {
            "digest_id": self.digest_id,
            "timestamp": round(self.timestamp, 4),
            "schema_version": self.schema_version,
            "runtime_status": self.runtime_status,
            "scheduler_running": self.scheduler_running,
            "outbox_counts": dict(sorted(self.outbox_counts.items())),
            "sessions_active": self.sessions_active,
            "execution_locked": self.execution_locked,
            "latest_equity_snapshot_id": self.latest_equity_snapshot_id,
            "latest_equity_total": round(self.latest_equity_total, 4) if self.latest_equity_total is not None else None,
        }
        encoded = json.dumps(core_state, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @property
    def digest_hash(self) -> str:
        """Alias for deterministic_digest SHA-256 fingerprint."""
        return self.deterministic_digest

    def to_dict(self) -> Dict[str, Any]:
        return {
            "digest_id": self.digest_id,
            "timestamp": self.timestamp,
            "schema_version": self.schema_version,
            "correlation_id": self.correlation_id,
            "runtime_status": self.runtime_status,
            "scheduler_enabled": self.scheduler_enabled,
            "scheduler_running": self.scheduler_running,
            "execution_locked": self.execution_locked,
            "paper_execution_enabled": self.paper_execution_enabled,
            "broker_provider": self.broker_provider,
            "tasks_status": dict(self.tasks_status),
            "outbox_counts": dict(self.outbox_counts),
            "outbox_retention": dict(self.outbox_retention),
            "outbox_dispatcher_running": self.outbox_dispatcher_running,
            "sessions_total": self.sessions_total,
            "sessions_active": self.sessions_active,
            "portfolio_reconciliation": dict(self.portfolio_reconciliation) if self.portfolio_reconciliation else None,
            "latest_equity_snapshot_id": self.latest_equity_snapshot_id,
            "latest_equity_snapshot_timestamp": self.latest_equity_snapshot_timestamp,
            "latest_equity_total": self.latest_equity_total,
            "data_quality": dict(self.data_quality) if self.data_quality else None,
            "recent_errors": list(self.recent_errors),
            "health_reasons": list(self.health_reasons),
            "worker_status": self.worker_status,
            "scheduler_uptime_seconds": self.scheduler_uptime_seconds,
            "task_metrics": dict(self.task_metrics),
            "duration_summaries": dict(self.duration_summaries),
            "failure_summaries": list(self.failure_summaries),
            "telemetry_sources": dict(self.telemetry_sources),
            "deterministic_digest": self.deterministic_digest,
        }


class MaintenanceScheduler:
    """Automated maintenance scheduler for bounded outbox pruning and health digest snapshots."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        outbox: Optional[TransactionalOutbox] = None,
        coordinator: Optional[Any] = None,
        dispatcher: Optional[Any] = None,
        db_path: Optional[str] = None,
        clock: Callable[[], float] = time.time,
        mono_clock: Callable[[], float] = time.monotonic,
        enabled: Optional[bool] = None,
        pruning_enabled: Optional[bool] = None,
        pruning_interval_seconds: Optional[float] = None,
        prune_interval_seconds: Optional[float] = None,
        health_digest_enabled: Optional[bool] = None,
        health_digest_interval_seconds: Optional[float] = None,
        active_session_policy: Optional[str] = None,
        shutdown_timeout_seconds: Optional[float] = None,
        history_limit: Optional[int] = None,
        consecutive_failure_threshold: Optional[int] = None,
        duration_sample_limit: Optional[int] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.outbox = outbox
        self.coordinator = coordinator
        self.dispatcher = dispatcher
        self.db_path = db_path or (getattr(coordinator, "db_path", None) if coordinator else None)
        self._clock = clock
        self._mono_clock = mono_clock

        # Enabled switches
        self._enabled = enabled if enabled is not None else getattr(self.settings, "maintenance_scheduler_enabled", False)
        self._pruning_enabled = pruning_enabled if pruning_enabled is not None else getattr(self.settings, "maintenance_pruning_enabled", False)
        self._health_digest_enabled = health_digest_enabled if health_digest_enabled is not None else getattr(self.settings, "maintenance_health_digest_enabled", False)

        # Intervals & policy
        chosen_prune_interval = prune_interval_seconds if prune_interval_seconds is not None else pruning_interval_seconds
        raw_prune_interval = chosen_prune_interval if chosen_prune_interval is not None else getattr(self.settings, "maintenance_pruning_interval_seconds", 3600.0)
        raw_digest_interval = health_digest_interval_seconds if health_digest_interval_seconds is not None else getattr(self.settings, "maintenance_health_digest_interval_seconds", 300.0)
        raw_shutdown_timeout = shutdown_timeout_seconds if shutdown_timeout_seconds is not None else getattr(self.settings, "maintenance_shutdown_timeout_seconds", 5.0)
        raw_policy = active_session_policy if active_session_policy is not None else getattr(self.settings, "maintenance_active_session_policy", "skip")
        raw_history_limit = history_limit if history_limit is not None else getattr(self.settings, "maintenance_history_limit", 1000)
        raw_fail_threshold = consecutive_failure_threshold if consecutive_failure_threshold is not None else getattr(self.settings, "maintenance_consecutive_failure_threshold", 3)
        raw_sample_limit = duration_sample_limit if duration_sample_limit is not None else getattr(self.settings, "maintenance_duration_sample_limit", 100)

        # Validation
        if not math.isfinite(raw_prune_interval) or raw_prune_interval <= 0:
            raise ValidationError(f"pruning_interval_seconds must be a positive finite float, got {raw_prune_interval}")
        if not math.isfinite(raw_digest_interval) or raw_digest_interval <= 0:
            raise ValidationError(f"health_digest_interval_seconds must be a positive finite float, got {raw_digest_interval}")
        if not math.isfinite(raw_shutdown_timeout) or raw_shutdown_timeout <= 0 or raw_shutdown_timeout > 60.0:
            raise ValidationError(f"shutdown_timeout_seconds must be positive float <= 60.0, got {raw_shutdown_timeout}")
        if raw_history_limit < 10:
            raise ValidationError(f"history_limit must be at least 10, got {raw_history_limit}")
        if int(raw_fail_threshold) < 1:
            raise ValidationError(f"consecutive_failure_threshold must be >= 1, got {raw_fail_threshold}")
        if int(raw_sample_limit) < 10:
            raise ValidationError(f"duration_sample_limit must be >= 10, got {raw_sample_limit}")

        norm_policy = str(raw_policy).strip().lower()
        if norm_policy not in ("skip", "allow"):
            raise ValidationError(f"active_session_policy must be 'skip' or 'allow', got '{raw_policy}'")

        self.pruning_interval_seconds = float(raw_prune_interval)
        self.health_digest_interval_seconds = float(raw_digest_interval)
        self.shutdown_timeout_seconds = float(raw_shutdown_timeout)
        self.active_session_policy = norm_policy
        self.history_limit = int(raw_history_limit)
        self.consecutive_failure_threshold = int(raw_fail_threshold)
        self.duration_sample_limit = int(raw_sample_limit)

        # Concurrency & lifecycle
        self._lock = threading.RLock()
        self._shutdown_lock = threading.Lock()
        self._prune_lock = threading.Lock()
        self._digest_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._is_running = False

        # Monotonic time markers for interval evaluation & uptime
        self._started_at_mono: Optional[float] = None
        self._last_prune_mono: float = 0.0
        self._last_digest_mono: float = 0.0
        self._shutdown_timeout_occurrences: int = 0
        self._last_shutdown_wait_seconds: Optional[float] = None
        self._worker_status: WorkerStatus = WorkerStatus.DISABLED if not self._enabled else WorkerStatus.STOPPED

        # Memory caches, metrics & in-flight tracking
        self._last_runs: Dict[str, MaintenanceTaskResult] = {}
        self._running_tasks: Dict[str, str] = {}
        self._recent_errors: List[str] = []
        self._task_metrics: Dict[str, TaskMetricsTracker] = {
            "prune": TaskMetricsTracker("prune", max_samples=self.duration_sample_limit),
            "health_digest": TaskMetricsTracker("health_digest", max_samples=self.duration_sample_limit),
        }
        self._failure_summaries: Dict[str, DiagnosticFailureSummary] = {}

        if self.db_path:
            self._init_sqlite_db()
            self._recover_state()

        if self.coordinator is not None and hasattr(self.coordinator, "attach_maintenance_scheduler"):
            self.coordinator.attach_maintenance_scheduler(self)

    @property
    def is_enabled(self) -> bool:
        """Check whether the scheduler is configured as enabled."""
        with self._lock:
            return self._enabled

    @property
    def is_running(self) -> bool:
        """Check whether the background maintenance worker thread is active."""
        with self._lock:
            return self._is_running

    @property
    def pruning_enabled(self) -> bool:
        """Check whether retention pruning task is enabled."""
        with self._lock:
            return self._pruning_enabled

    @property
    def health_digest_enabled(self) -> bool:
        """Check whether health digest task is enabled."""
        with self._lock:
            return self._health_digest_enabled

    @property
    def worker_status(self) -> str:
        """Detailed operational worker status (STOPPED, RUNNING, SHUTDOWN_TIMED_OUT, DISABLED)."""
        with self._lock:
            if not self._enabled:
                return WorkerStatus.DISABLED.value
            if self._worker_status == WorkerStatus.SHUTDOWN_TIMED_OUT and self._worker_thread is not None and self._worker_thread.is_alive():
                return WorkerStatus.SHUTDOWN_TIMED_OUT.value
            if self._is_running:
                return WorkerStatus.RUNNING.value
            return WorkerStatus.STOPPED.value

    @property
    def uptime_seconds(self) -> float:
        """Monotonic elapsed time in seconds since background worker thread started."""
        with self._lock:
            if not self._is_running or self._started_at_mono is None:
                return 0.0
            return max(0.0, self._mono_clock() - self._started_at_mono)

    def _get_or_create_tracker(self, task_name: str) -> TaskMetricsTracker:
        """Get or initialize thread-safe task metrics tracker."""
        with self._lock:
            if task_name not in self._task_metrics:
                self._task_metrics[task_name] = TaskMetricsTracker(task_name, max_samples=self.duration_sample_limit)
            return self._task_metrics[task_name]

    def _get_connection(self) -> sqlite3.Connection:
        """Open a SQLite connection with timeout."""
        if not self.db_path:
            raise ValidationError("db_path is not configured for this maintenance scheduler.")
        return sqlite3.connect(self.db_path, timeout=10.0)

    def _init_sqlite_db(self) -> None:
        """Initialize SQLite tables and indexes for maintenance audit runs and health digests."""
        if not self.db_path:
            return
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS paper_maintenance_runs (
                        run_id TEXT PRIMARY KEY,
                        task_name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        started_at REAL NOT NULL,
                        completed_at REAL NOT NULL,
                        duration_seconds REAL NOT NULL,
                        records_inspected INTEGER DEFAULT 0,
                        records_processed INTEGER DEFAULT 0,
                        records_skipped INTEGER DEFAULT 0,
                        has_more INTEGER DEFAULT 0,
                        skip_reason TEXT,
                        error_message TEXT,
                        details_json TEXT
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_maint_task_started ON paper_maintenance_runs(task_name, started_at)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_maint_status ON paper_maintenance_runs(status)"
                )

                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS paper_health_digests (
                        digest_id TEXT PRIMARY KEY,
                        timestamp REAL NOT NULL,
                        schema_version INTEGER NOT NULL DEFAULT 1,
                        correlation_id TEXT,
                        runtime_status TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        deterministic_digest TEXT
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_health_digest_ts ON paper_health_digests(timestamp)"
                )
        except Exception as err:
            logger.error("Failed to initialize maintenance tables in SQLite (%s): %s", self.db_path, err)
            raise

    def _record_claim(self, run_id: str, task_name: str, started_at: float) -> None:
        """Record task claim / RUNNING state in SQLite and memory before execution begins."""
        with self._lock:
            self._running_tasks[task_name] = run_id

        if not self.db_path:
            return

        try:
            with self._get_connection() as conn:
                with conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO paper_maintenance_runs (
                            run_id, task_name, status, started_at, completed_at,
                            duration_seconds, records_inspected, records_processed,
                            records_skipped, has_more, skip_reason, error_message, details_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            task_name,
                            MaintenanceTaskStatus.RUNNING.value,
                            started_at,
                            0.0,
                            0.0,
                            0,
                            0,
                            0,
                            0,
                            None,
                            None,
                            json.dumps({"phase": "CLAIMED"}),
                        ),
                    )
        except Exception as exc:
            logger.warning("Could not persist claimed task state in SQLite: %s", exc)

    def _recover_state(self) -> None:
        """Recover last task outcomes from SQLite on startup and reconcile in-flight runs. Idempotent."""
        if not self.db_path or not Path(self.db_path).is_file():
            return
        with self._lock:
            try:
                with self._get_connection() as conn:
                    with conn:
                        now = self._clock()
                        cursor = conn.execute(
                            """
                            UPDATE paper_maintenance_runs
                            SET status = ?,
                                completed_at = CASE WHEN completed_at <= 0 THEN ? ELSE completed_at END,
                                duration_seconds = CASE WHEN duration_seconds <= 0 THEN (? - started_at) ELSE duration_seconds END,
                                error_message = coalesce(error_message, 'Task was interrupted by process termination or restart.')
                            WHERE status = ?
                            """,
                            (
                                MaintenanceTaskStatus.INTERRUPTED.value,
                                now,
                                now,
                                MaintenanceTaskStatus.RUNNING.value,
                            ),
                        )
                        interrupted_count = cursor.rowcount
                        if interrupted_count > 0:
                            self._record_failure_summary(
                                "RECOVERY_ACTION",
                                "scheduler",
                                f"Reconciled {interrupted_count} in-flight maintenance runs to INTERRUPTED on startup.",
                                {"interrupted_count": interrupted_count},
                            )

                    cursor = conn.execute(
                        """
                        SELECT run_id, task_name, status, started_at, completed_at,
                               duration_seconds, records_inspected, records_processed,
                               records_skipped, has_more, skip_reason, error_message, details_json
                        FROM paper_maintenance_runs
                        ORDER BY started_at DESC
                        LIMIT 50
                        """
                    )
                    rows = cursor.fetchall()
                    for row in rows:
                        t_name = row[1]
                        if t_name not in self._last_runs:
                            try:
                                det = json.loads(row[12]) if row[12] else {}
                            except Exception:
                                det = {}
                            res = MaintenanceTaskResult(
                                run_id=row[0],
                                task_name=t_name,
                                status=MaintenanceTaskStatus(row[2]),
                                started_at=row[3],
                                completed_at=row[4],
                                duration_seconds=row[5],
                                records_inspected=row[6],
                                records_processed=row[7],
                                records_skipped=row[8],
                                has_more=bool(row[9]),
                                skip_reason=row[10],
                                error_message=row[11],
                                details=det,
                            )
                            self._last_runs[t_name] = res
                            tracker = self._get_or_create_tracker(t_name)
                            if res.status == MaintenanceTaskStatus.SUCCESS:
                                tracker.last_successful_run = res.to_dict()
                            elif res.status in (MaintenanceTaskStatus.FAILED, MaintenanceTaskStatus.TIMED_OUT, MaintenanceTaskStatus.INTERRUPTED):
                                tracker.last_failed_run = res.to_dict()
            except Exception as err:
                logger.warning("Could not recover previous maintenance runs: %s", err)

    def start(self) -> None:
        """Start the background maintenance worker thread if enabled."""
        with self._lock:
            if not self._enabled:
                logger.info("MaintenanceScheduler start() called, but scheduler is disabled in configuration.")
                return

            if self._is_running or (self._worker_thread is not None and self._worker_thread.is_alive()):
                logger.debug("MaintenanceScheduler is already running.")
                return

            self._stop_event.clear()
            self._is_running = True
            current_mono = self._mono_clock()
            self._started_at_mono = current_mono
            self._worker_status = WorkerStatus.RUNNING
            self._last_prune_mono = current_mono
            self._last_digest_mono = current_mono

            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="KAISER-MaintenanceScheduler",
                daemon=True,
            )
            self._worker_thread.start()
            logger.info("MaintenanceScheduler worker thread started.")

    def stop(self, timeout: Optional[float] = None) -> bool:
        """Gracefully stop the background worker thread within the timeout."""
        with self._shutdown_lock:
            m_stop_start = self._mono_clock()
            with self._lock:
                if not self._is_running and (self._worker_thread is None or not self._worker_thread.is_alive()):
                    self._worker_status = WorkerStatus.DISABLED if not self._enabled else WorkerStatus.STOPPED
                    return True

                self._is_running = False
                self._stop_event.set()
                thread = self._worker_thread

            t_out = timeout if timeout is not None else self.shutdown_timeout_seconds
            clean_exit = True
            if thread is not None and thread.is_alive():
                thread.join(timeout=t_out)
                if thread.is_alive():
                    logger.warning("MaintenanceScheduler worker thread did not terminate within %.2fs timeout.", t_out)
                    clean_exit = False
                    with self._lock:
                        self._shutdown_timeout_occurrences += 1
                        self._worker_status = WorkerStatus.SHUTDOWN_TIMED_OUT
                        self._record_failure_summary(
                            "SHUTDOWN_TIMEOUT",
                            None,
                            f"Worker thread did not terminate within {t_out:.2f}s timeout.",
                            {"timeout_seconds": t_out, "occurrences": self._shutdown_timeout_occurrences},
                        )

            m_stop_end = self._mono_clock()
            with self._lock:
                self._last_shutdown_wait_seconds = max(0.0, m_stop_end - m_stop_start)
                if clean_exit:
                    self._worker_thread = None
                    self._worker_status = WorkerStatus.DISABLED if not self._enabled else WorkerStatus.STOPPED
                logger.info("MaintenanceScheduler stopped (clean=%s, wait=%.4fs).", clean_exit, self._last_shutdown_wait_seconds)
            return clean_exit

    def _worker_loop(self) -> None:
        """Background maintenance loop polling intervals and executing tasks safely."""
        while not self._stop_event.is_set():
            now_mono = self._mono_clock()

            # 1. Prune task check
            if self._pruning_enabled:
                if (now_mono - self._last_prune_mono) >= self.pruning_interval_seconds:
                    delay = max(0.0, (now_mono - self._last_prune_mono) - self.pruning_interval_seconds)
                    try:
                        self.run_prune_now(scheduler_delay=delay)
                    except Exception as exc:
                        logger.error("Unexpected error in scheduled prune task: %s", exc, exc_info=True)
                    # Advance monotonic marker to prevent unbounded catch-up
                    self._last_prune_mono = self._mono_clock()

            # 2. Health digest task check
            if self._health_digest_enabled:
                if (now_mono - self._last_digest_mono) >= self.health_digest_interval_seconds:
                    delay = max(0.0, (now_mono - self._last_digest_mono) - self.health_digest_interval_seconds)
                    try:
                        self.run_health_digest_now(scheduler_delay=delay)
                    except Exception as exc:
                        logger.error("Unexpected error in scheduled health digest task: %s", exc, exc_info=True)
                    self._last_digest_mono = self._mono_clock()

            # Sleep in short slices to respond quickly to shutdown signal
            self._stop_event.wait(timeout=1.0)

    def _persist_task_run(self, result: MaintenanceTaskResult) -> bool:
        """Persist maintenance task run outcome to SQLite."""
        if not self.db_path:
            return True
        try:
            with self._get_connection() as conn:
                with conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO paper_maintenance_runs (
                            run_id, task_name, status, started_at, completed_at,
                            duration_seconds, records_inspected, records_processed,
                            records_skipped, has_more, skip_reason, error_message, details_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            result.run_id,
                            result.task_name,
                            result.status.value,
                            result.started_at,
                            result.completed_at,
                            result.duration_seconds,
                            result.records_inspected,
                            result.records_processed,
                            result.records_skipped,
                            1 if result.has_more else 0,
                            result.skip_reason,
                            result.error_message,
                            json.dumps(result.details),
                        ),
                    )
            return True
        except Exception as exc:
            logger.error("Failed to persist maintenance task run %s: %s", result.run_id, exc)
            return False

    def _persist_health_digest(self, digest: HealthDigestSnapshot) -> bool:
        """Persist health digest snapshot to SQLite."""
        if not self.db_path:
            return True
        try:
            with self._get_connection() as conn:
                with conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO paper_health_digests (
                            digest_id, timestamp, schema_version, correlation_id,
                            runtime_status, payload_json, deterministic_digest
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            digest.digest_id,
                            digest.timestamp,
                            digest.schema_version,
                            digest.correlation_id,
                            digest.runtime_status,
                            json.dumps(digest.to_dict()),
                            digest.deterministic_digest,
                        ),
                    )
            return True
        except Exception as exc:
            logger.error("Failed to persist health digest %s: %s", digest.digest_id, exc)
            return False

    def run_prune_now(
        self,
        force: bool = False,
        now: Optional[float] = None,
        retention_seconds: Optional[float] = None,
        batch_size: Optional[int] = None,
        scheduler_delay: Optional[float] = None,
    ) -> MaintenanceTaskResult:
        """Execute the outbox retention and pruning maintenance task with safety gates."""
        t_start = self._clock()
        m_start = self._mono_clock()
        run_id = f"mrun_prune_{uuid.uuid4().hex[:12]}"
        tracker = self._get_or_create_tracker("prune")
        tracker.record_attempt()

        # Prevent overlapping execution of pruning
        if not self._prune_lock.acquire(blocking=False):
            logger.warning("Pruning maintenance task is already running; skipping overlapping execution.")
            t_end = self._clock()
            dur = max(0.0, self._mono_clock() - m_start)
            res = MaintenanceTaskResult(
                run_id=run_id,
                task_name="prune",
                status=MaintenanceTaskStatus.SKIPPED_OVERLAPPING,
                started_at=t_start,
                completed_at=t_end,
                duration_seconds=dur,
                skip_reason="Overlapping execution prevented.",
            )
            tracker.record_completion(res)
            self._persist_task_run(res)
            return res

        try:
            # 1. Enablement Safety Gate
            if not force:
                if not self._enabled:
                    t_end = self._clock()
                    dur = max(0.0, self._mono_clock() - m_start)
                    res = MaintenanceTaskResult(
                        run_id=run_id,
                        task_name="prune",
                        status=MaintenanceTaskStatus.SKIPPED_DISABLED,
                        started_at=t_start,
                        completed_at=t_end,
                        duration_seconds=dur,
                        skip_reason="Maintenance scheduler is disabled.",
                    )
                    self._record_run("prune", res)
                    return res

                if not self._pruning_enabled:
                    t_end = self._clock()
                    dur = max(0.0, self._mono_clock() - m_start)
                    res = MaintenanceTaskResult(
                        run_id=run_id,
                        task_name="prune",
                        status=MaintenanceTaskStatus.SKIPPED_DISABLED,
                        started_at=t_start,
                        completed_at=t_end,
                        duration_seconds=dur,
                        skip_reason="Maintenance pruning is disabled.",
                    )
                    self._record_run("prune", res)
                    return res

            # 2. Session Coordinator Safety Gate
            if self.coordinator is not None:
                if hasattr(self.coordinator, "is_session_recovery_complete"):
                    if not self.coordinator.is_session_recovery_complete():
                        t_end = self._clock()
                        dur = max(0.0, self._mono_clock() - m_start)
                        res = MaintenanceTaskResult(
                            run_id=run_id,
                            task_name="prune",
                            status=MaintenanceTaskStatus.SKIPPED_RECOVERY_INCOMPLETE,
                            started_at=t_start,
                            completed_at=t_end,
                            duration_seconds=dur,
                            skip_reason="Session coordinator recovery is incomplete; fail-closed.",
                        )
                        self._record_run("prune", res)
                        return res

                if self.active_session_policy == "skip":
                    is_active = False
                    if hasattr(self.coordinator, "is_session_active"):
                        is_active = self.coordinator.is_session_active()
                    elif hasattr(self.coordinator, "get_active_session"):
                        is_active = self.coordinator.get_active_session() is not None

                    if is_active:
                        t_end = self._clock()
                        dur = max(0.0, self._mono_clock() - m_start)
                        res = MaintenanceTaskResult(
                            run_id=run_id,
                            task_name="prune",
                            status=MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION,
                            started_at=t_start,
                            completed_at=t_end,
                            duration_seconds=dur,
                            skip_reason="Simulation session is currently ACTIVE; destructive maintenance skipped by policy.",
                        )
                        self._record_run("prune", res)
                        return res

            # 3. Target Outbox Verification
            target_outbox = self.outbox
            if target_outbox is None and self.coordinator is not None:
                target_outbox = getattr(self.coordinator, "outbox", None)
            if target_outbox is None and self.dispatcher is not None:
                target_outbox = getattr(self.dispatcher, "outbox", None)

            if target_outbox is None:
                t_end = self._clock()
                dur = max(0.0, self._mono_clock() - m_start)
                res = MaintenanceTaskResult(
                    run_id=run_id,
                    task_name="prune",
                    status=MaintenanceTaskStatus.SKIPPED_DISABLED,
                    started_at=t_start,
                    completed_at=t_end,
                    duration_seconds=dur,
                    skip_reason="No TransactionalOutbox available for pruning.",
                )
                self._record_run("prune", res)
                return res

            self._record_claim(run_id, "prune", t_start)

            # 4. Immediate Session Recheck and atomic execution under coordinator lock (race protection)
            coord_lock = getattr(self.coordinator, "_lock", None) if self.coordinator else None
            lock_to_use = coord_lock if coord_lock is not None else threading.Lock()

            with lock_to_use:
                if self.coordinator is not None and self.active_session_policy == "skip":
                    if hasattr(self.coordinator, "is_session_active") and self.coordinator.is_session_active():
                        t_end = self._clock()
                        dur = max(0.0, self._mono_clock() - m_start)
                        res = MaintenanceTaskResult(
                            run_id=run_id,
                            task_name="prune",
                            status=MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION,
                            started_at=t_start,
                            completed_at=t_end,
                            duration_seconds=dur,
                            skip_reason="Active session detected on immediate pre-prune recheck.",
                        )
                        self._record_run("prune", res)
                        return res

                # 5. In-flight protection
                exclude_ids: Set[str] = set()
                disp = self.dispatcher
                if disp is None and self.coordinator is not None:
                    disp = getattr(self.coordinator, "outbox_dispatcher", None) or getattr(self.coordinator, "dispatcher", None)
                if disp is not None:
                    if hasattr(disp, "in_flight_event_ids"):
                        val = getattr(disp, "in_flight_event_ids")
                        exclude_ids = set(val() if callable(val) else val)
                    elif hasattr(disp, "_in_flight_event_ids"):
                        with getattr(disp, "_lock", threading.Lock()):
                            exclude_ids = set(disp._in_flight_event_ids)

                # 6. Execute Prune
                current_time = now if now is not None else self._clock()
                prune_res: OutboxPruneResult = target_outbox.prune_delivered_records(
                    retention_seconds=retention_seconds,
                    batch_size=batch_size,
                    now=current_time,
                    exclude_event_ids=exclude_ids,
                )

            t_end = self._clock()
            dur = max(0.0, self._mono_clock() - m_start)
            det = prune_res.to_dict()
            if scheduler_delay is not None:
                det["scheduler_delay_seconds"] = round(scheduler_delay, 6)

            res = MaintenanceTaskResult(
                run_id=run_id,
                task_name="prune",
                status=MaintenanceTaskStatus.SUCCESS,
                started_at=t_start,
                completed_at=t_end,
                duration_seconds=dur,
                records_inspected=prune_res.inspected_count,
                records_processed=prune_res.pruned_count,
                records_skipped=prune_res.skipped_count,
                has_more=prune_res.has_more,
                details=det,
            )
            self._record_run("prune", res)
            self.prune_diagnostic_history()
            return res

        except Exception as exc:
            t_end = self._clock()
            dur = max(0.0, self._mono_clock() - m_start)
            err_msg = str(exc)
            logger.error("Maintenance pruning failed [run_id=%s]: %s", run_id, exc, exc_info=True)
            self._record_error(f"Prune failure: {err_msg}")
            res = MaintenanceTaskResult(
                run_id=run_id,
                task_name="prune",
                status=MaintenanceTaskStatus.FAILED,
                started_at=t_start,
                completed_at=t_end,
                duration_seconds=dur,
                error_message=err_msg,
            )
            self._record_run("prune", res)
            return res

        finally:
            self._prune_lock.release()

    def run_health_digest_now(
        self,
        digest_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        now: Optional[float] = None,
        force: bool = False,
        scheduler_delay: Optional[float] = None,
    ) -> HealthDigestSnapshot:
        """Generate and durably record an immutable, versioned health digest snapshot."""
        t_start = self._clock()
        m_start = self._mono_clock()
        cur_time = now if now is not None else t_start
        did = digest_id or f"hdig_{uuid.uuid4().hex[:12]}"
        tracker = self._get_or_create_tracker("health_digest")
        tracker.record_attempt()

        # Acquire digest lock with bounded wait for concurrency safety
        acquired = self._digest_lock.acquire(blocking=True, timeout=self.shutdown_timeout_seconds)
        if not acquired:
            logger.warning("Timed out waiting for health digest lock for digest %s", did)
            if self.db_path and Path(self.db_path).is_file():
                existing = self.get_health_digest(did)
                if existing is not None:
                    return existing
            dur = max(0.0, self._mono_clock() - m_start)
            fail_res = MaintenanceTaskResult(
                run_id=f"mrun_digest_{uuid.uuid4().hex[:12]}",
                task_name="health_digest",
                status=MaintenanceTaskStatus.TIMED_OUT,
                started_at=t_start,
                completed_at=self._clock(),
                duration_seconds=dur,
                error_message=f"Health digest generation timed out waiting for lock ({did}).",
            )
            self._record_run("health_digest", fail_res)
            raise TimeoutError(f"Health digest generation timed out waiting for lock ({did}).")

        try:
            # Check for existing digest with same ID (Idempotency)
            if self.db_path and Path(self.db_path).is_file():
                existing = self.get_health_digest(did)
                if existing is not None:
                    return existing

            self._record_claim(f"mrun_digest_{did}", "health_digest", t_start)

            # 1. Collect outbox metrics
            target_outbox = self.outbox
            if target_outbox is None and self.coordinator is not None:
                target_outbox = getattr(self.coordinator, "outbox", None)
            if target_outbox is None and self.dispatcher is not None:
                target_outbox = getattr(self.dispatcher, "outbox", None)

            outbox_stats = target_outbox.get_stats() if target_outbox and hasattr(target_outbox, "get_stats") else {}
            outbox_retention = target_outbox.get_retention_status() if target_outbox and hasattr(target_outbox, "get_retention_status") else {}
            dispatcher_running = self.dispatcher.is_running if self.dispatcher and hasattr(self.dispatcher, "is_running") else None

            # 2. Collect session metrics
            sessions_total = 0
            sessions_active = 0
            if self.coordinator is not None and hasattr(self.coordinator, "list_sessions"):
                all_sessions = self.coordinator.list_sessions()
                sessions_total = len(all_sessions)
                sessions_active = sum(1 for s in all_sessions if getattr(s, "status", "") == "ACTIVE")

            # 3. Collect portfolio reconciliation & equity snapshot
            recon_data = None
            if self.coordinator is not None and hasattr(self.coordinator, "reconcile_allocations"):
                recon_obj = self.coordinator.reconcile_allocations()
                recon_data = recon_obj.to_dict() if recon_obj and hasattr(recon_obj, "to_dict") else None
                if recon_data is not None and "is_balanced" not in recon_data:
                    recon_data["is_balanced"] = recon_data.get("is_reconciled", False)

            latest_snap_id = None
            latest_snap_ts = None
            latest_snap_equity = None
            if self.coordinator is not None and hasattr(self.coordinator, "list_equity_snapshots"):
                snaps = self.coordinator.list_equity_snapshots(limit=1)
                if snaps:
                    latest = snaps[0]
                    latest_snap_id = latest.snapshot_id
                    latest_snap_ts = latest.timestamp
                    latest_snap_equity = latest.total_equity

            # 4. Tasks status
            with self._lock:
                tasks_stat = {k: v.to_dict() for k, v in self._last_runs.items()}
                recent_errs = [sanitize_sensitive_text(e) for e in self._recent_errors[-10:]]
                is_running = self._is_running

            # 5. Evaluate runtime status with deterministic rules
            runtime_status, health_reasons, _ = self.evaluate_health()

            # 6. Collect metrics, latency summaries, failure summaries & source indicators
            with self._lock:
                task_metrics_dict = {name: tracker.get_summary() for name, tracker in self._task_metrics.items()}
                duration_summaries_dict = {name: tracker.get_summary()["duration_summary"] for name, tracker in self._task_metrics.items()}
                failure_summaries_list = [s.to_dict() for s in self._failure_summaries.values()]
                telemetry_sources_dict = {
                    "outbox": target_outbox is not None,
                    "coordinator": self.coordinator is not None,
                    "dispatcher": self.dispatcher is not None,
                    "portfolio_reconciliation": (self.coordinator is not None and hasattr(self.coordinator, "reconcile_allocations") and recon_data is not None),
                    "database": bool(self.db_path and Path(self.db_path).is_file()),
                }

            digest = HealthDigestSnapshot(
                digest_id=did,
                timestamp=cur_time,
                schema_version=1,
                correlation_id=correlation_id,
                runtime_status=runtime_status,
                scheduler_enabled=self._enabled,
                scheduler_running=is_running,
                execution_locked=getattr(self.settings, "execution_locked", True),
                paper_execution_enabled=getattr(self.settings, "paper_execution_enabled", False),
                broker_provider=getattr(self.settings, "broker_provider", "none"),
                tasks_status=tasks_stat,
                outbox_counts=outbox_stats,
                outbox_retention=outbox_retention,
                outbox_dispatcher_running=dispatcher_running,
                sessions_total=sessions_total,
                sessions_active=sessions_active,
                portfolio_reconciliation=recon_data,
                latest_equity_snapshot_id=latest_snap_id,
                latest_equity_snapshot_timestamp=latest_snap_ts,
                latest_equity_total=latest_snap_equity,
                recent_errors=recent_errs,
                health_reasons=health_reasons,
                worker_status=self.worker_status,
                scheduler_uptime_seconds=round(self.uptime_seconds, 2),
                task_metrics=task_metrics_dict,
                duration_summaries=duration_summaries_dict,
                failure_summaries=failure_summaries_list,
                telemetry_sources=telemetry_sources_dict,
            )

            # Persist to SQLite
            persisted_ok = self._persist_health_digest(digest)

            # Record task run audit
            t_end = self._clock()
            dur = max(0.0, self._mono_clock() - m_start)
            det: Dict[str, Any] = {"digest_id": digest.digest_id, "runtime_status": runtime_status}
            if scheduler_delay is not None:
                det["scheduler_delay_seconds"] = round(scheduler_delay, 6)

            if not persisted_ok and self.db_path:
                self._record_error(f"Health digest persistence failed for {digest.digest_id}")
                run_result = MaintenanceTaskResult(
                    run_id=f"mrun_digest_{uuid.uuid4().hex[:12]}",
                    task_name="health_digest",
                    status=MaintenanceTaskStatus.FAILED,
                    started_at=t_start,
                    completed_at=t_end,
                    duration_seconds=dur,
                    error_message=f"Health digest database persistence failed for {digest.digest_id}",
                    details={"digest_id": digest.digest_id, "persistence_failed": True},
                )
                self._record_run("health_digest", run_result)
                return digest

            run_result = MaintenanceTaskResult(
                run_id=f"mrun_digest_{uuid.uuid4().hex[:12]}",
                task_name="health_digest",
                status=MaintenanceTaskStatus.SUCCESS,
                started_at=t_start,
                completed_at=t_end,
                duration_seconds=dur,
                records_processed=1,
                details=det,
            )
            self._record_run("health_digest", run_result)
            self.prune_diagnostic_history()
            return digest

        finally:
            self._digest_lock.release()

    def _record_run(self, task_name: str, result: MaintenanceTaskResult) -> None:
        """Update last run cache, remove from active tasks, record metrics, and persist to SQLite."""
        with self._lock:
            self._last_runs[task_name] = result
            self._running_tasks.pop(task_name, None)
            tracker = self._get_or_create_tracker(task_name)
            tracker.record_completion(result)

            if result.status == MaintenanceTaskStatus.FAILED:
                self._record_failure_summary(
                    "TASK_FAILURE",
                    task_name,
                    result.error_message or "Task failed",
                    result.details,
                )
                if tracker.consecutive_failures >= self.consecutive_failure_threshold:
                    self._record_failure_summary(
                        "REPEATED_TASK_FAILURE",
                        task_name,
                        f"Task '{task_name}' failed {tracker.consecutive_failures} consecutive times (threshold: {self.consecutive_failure_threshold}).",
                        {"consecutive_failures": tracker.consecutive_failures, "threshold": self.consecutive_failure_threshold},
                    )
            elif result.status == MaintenanceTaskStatus.TIMED_OUT:
                self._record_failure_summary(
                    "TASK_TIMEOUT",
                    task_name,
                    result.error_message or "Task timed out",
                    result.details,
                )
            elif result.status == MaintenanceTaskStatus.INTERRUPTED:
                self._record_failure_summary(
                    "TASK_INTERRUPTED",
                    task_name,
                    result.error_message or "Task was interrupted",
                    result.details,
                )

        self._persist_task_run(result)

    def _record_failure_summary(
        self,
        failure_type: str,
        task_name: Optional[str],
        error_message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> DiagnosticFailureSummary:
        """Record bounded, deduplicated alert-ready failure summary."""
        now = self._clock()
        clean_err = sanitize_sensitive_text(str(error_message))
        clean_details = sanitize_dict_secrets(dict(details or {}))
        dedup_key = f"{failure_type}:{task_name or 'scheduler'}"

        with self._lock:
            existing = self._failure_summaries.get(dedup_key)
            if existing:
                new_summary = DiagnosticFailureSummary(
                    summary_id=existing.summary_id,
                    failure_type=existing.failure_type,
                    task_name=existing.task_name,
                    first_seen_at=existing.first_seen_at,
                    last_seen_at=now,
                    occurrence_count=existing.occurrence_count + 1,
                    last_error_message=clean_err,
                    details=clean_details,
                )
            else:
                new_summary = DiagnosticFailureSummary(
                    summary_id=f"fail_{uuid.uuid4().hex[:10]}",
                    failure_type=failure_type,
                    task_name=task_name,
                    first_seen_at=now,
                    last_seen_at=now,
                    occurrence_count=1,
                    last_error_message=clean_err,
                    details=clean_details,
                )
                if len(self._failure_summaries) >= 50:
                    oldest_key = min(self._failure_summaries.keys(), key=lambda k: self._failure_summaries[k].last_seen_at)
                    self._failure_summaries.pop(oldest_key, None)

            self._failure_summaries[dedup_key] = new_summary
            return new_summary

    def _record_error(self, err: str) -> None:
        """Record bounded recent diagnostic error with sensitive text sanitization."""
        sanitized = sanitize_sensitive_text(str(err))
        with self._lock:
            self._recent_errors.append(f"[{time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime(self._clock()))}] {sanitized[:256]}")
            if len(self._recent_errors) > 50:
                self._recent_errors = self._recent_errors[-50:]

    def evaluate_health(self) -> Tuple[str, List[str], Dict[str, Any]]:
        """Evaluate deterministic maintenance health status according to explicit rules.

        Returns:
            Tuple of (health_status, reasons, diagnostics)
            where health_status is "HEALTHY", "DEGRADED", "FAILED", or "UNKNOWN".
        """
        reasons: List[str] = []
        is_failed = False
        is_degraded = False
        is_unknown = False

        # 1. Worker thread after shutdown timeout
        with self._lock:
            worker_alive = self._worker_thread.is_alive() if self._worker_thread else False
            if self._worker_status == WorkerStatus.SHUTDOWN_TIMED_OUT or (self._shutdown_timeout_occurrences > 0 and worker_alive):
                reasons.append("Worker thread still running after shutdown timeout.")
                is_failed = True

            # 2. Unexpected scheduler stop
            if self._enabled and self._is_running and not worker_alive:
                reasons.append("Scheduler worker thread terminated unexpectedly.")
                is_failed = True

            # 3. Repeated maintenance task failures
            for name, tracker in self._task_metrics.items():
                if tracker.consecutive_failures >= self.consecutive_failure_threshold:
                    reasons.append(
                        f"Task '{name}' failed {tracker.consecutive_failures} consecutive times (threshold: {self.consecutive_failure_threshold})."
                    )
                    is_failed = True
                elif tracker.consecutive_failures > 0:
                    reasons.append(f"Task '{name}' has {tracker.consecutive_failures} consecutive failure(s).")
                    is_degraded = True

        # 4. Session recovery check
        if self.coordinator is not None and hasattr(self.coordinator, "is_session_recovery_complete"):
            try:
                if not self.coordinator.is_session_recovery_complete():
                    reasons.append("Session coordinator recovery is incomplete.")
                    is_failed = True
            except Exception as exc:
                reasons.append(f"Telemetry error querying session recovery: {sanitize_sensitive_text(str(exc))}")
                is_unknown = True

        # 5. Portfolio reconciliation check
        if self.coordinator is not None and hasattr(self.coordinator, "reconcile_allocations"):
            try:
                recon_obj = self.coordinator.reconcile_allocations()
                if recon_obj is None:
                    reasons.append("Portfolio reconciliation telemetry is unavailable.")
                    is_unknown = True
                else:
                    recon_dict = recon_obj.to_dict() if hasattr(recon_obj, "to_dict") else recon_obj
                    is_bal = recon_dict.get("is_balanced") if isinstance(recon_dict, dict) else getattr(recon_obj, "is_balanced", None)
                    if is_bal is None and isinstance(recon_dict, dict):
                        is_bal = recon_dict.get("is_reconciled")
                    if is_bal is False:
                        reasons.append("Portfolio allocation reconciliation is unbalanced.")
                        is_degraded = True
                    elif is_bal is None:
                        reasons.append("Portfolio reconciliation returned unknown balance status.")
                        is_unknown = True
            except Exception as exc:
                reasons.append(f"Telemetry error during portfolio reconciliation: {sanitize_sensitive_text(str(exc))}")
                is_unknown = True

        # 6. Outbox telemetry check
        target_outbox = self.outbox or (getattr(self.coordinator, "outbox", None) if self.coordinator else None) or (getattr(self.dispatcher, "outbox", None) if self.dispatcher else None)
        if target_outbox is not None and hasattr(target_outbox, "get_stats"):
            try:
                stats = target_outbox.get_stats()
                if stats.get("exhausted", 0) > 0:
                    reasons.append(f"Outbox has {stats['exhausted']} exhausted event(s).")
                    is_degraded = True
                if stats.get("retryable", 0) > 0:
                    reasons.append(f"Outbox has {stats['retryable']} retryable failure(s).")
                    is_degraded = True
            except Exception as exc:
                reasons.append(f"Telemetry error querying outbox statistics: {sanitize_sensitive_text(str(exc))}")
                is_unknown = True
        elif self._pruning_enabled:
            reasons.append("Pruning is enabled but outbox telemetry source is unavailable.")
            is_unknown = True

        # Final health status mapping
        if is_failed:
            status = MaintenanceHealthStatus.FAILED.value
        elif is_degraded:
            status = MaintenanceHealthStatus.DEGRADED.value
        elif is_unknown:
            status = MaintenanceHealthStatus.UNKNOWN.value
        else:
            status = MaintenanceHealthStatus.HEALTHY.value
            if not self._enabled:
                reasons.append("Scheduler is disabled by configuration; offline status is intentional.")

        telemetry_sources = {
            "session_coordinator": "available" if self.coordinator is not None else "unavailable",
            "portfolio_reconciliation": "available" if (self.coordinator is not None and hasattr(self.coordinator, "reconcile_allocations") and not any("portfolio reconciliation" in r.lower() for r in reasons)) else "unavailable",
            "outbox": "available" if target_outbox is not None else ("unavailable" if self._pruning_enabled else "not_configured"),
        }

        diagnostics = {
            "reasons": list(reasons),
            "is_failed": is_failed,
            "is_degraded": is_degraded,
            "is_unknown": is_unknown,
            "worker_status": self.worker_status,
            "uptime_seconds": self.uptime_seconds,
            "telemetry_sources": telemetry_sources,
        }
        return status, reasons, diagnostics

    def _get_aggregate_metrics(self) -> Dict[str, Any]:
        """Aggregate attempts, completions, and outcome counters across all tasks."""
        with self._lock:
            total_attempts = sum(t.attempts_total for t in self._task_metrics.values())
            total_completions = sum(t.completions_total for t in self._task_metrics.values())
            combined_outcomes: Dict[str, int] = {}
            for t in self._task_metrics.values():
                for k, v in t.outcomes.items():
                    combined_outcomes[k] = combined_outcomes.get(k, 0) + v
            return {
                "attempts_total": total_attempts,
                "completions_total": total_completions,
                "outcomes": combined_outcomes,
            }

    def get_metrics(self) -> Dict[str, Any]:
        """Typed, bounded scheduler metrics and latency summaries."""
        with self._lock:
            return {
                "health_status": self.evaluate_health()[0],
                "worker_status": self.worker_status,
                "uptime_seconds": round(self.uptime_seconds, 2),
                "active_task_count": len(self._running_tasks),
                "shutdown_timeout_occurrences": self._shutdown_timeout_occurrences,
                "last_shutdown_wait_seconds": round(self._last_shutdown_wait_seconds, 4) if self._last_shutdown_wait_seconds is not None else None,
                "consecutive_failure_threshold": self.consecutive_failure_threshold,
                "duration_sample_limit": self.duration_sample_limit,
                "tasks": {k: v.get_summary() for k, v in self._task_metrics.items()},
                "aggregate": self._get_aggregate_metrics(),
            }

    def reset_metrics(self) -> None:
        """Reset in-memory metric counters and duration samples while preserving durable audit runs."""
        with self._lock:
            for tracker in self._task_metrics.values():
                tracker.reset()
            self._failure_summaries.clear()
            self._recent_errors.clear()
            self._shutdown_timeout_occurrences = 0
            self._last_shutdown_wait_seconds = None
            if self._is_running:
                self._started_at_mono = self._mono_clock()
            else:
                self._started_at_mono = None

    def get_failure_summaries(self) -> List[Dict[str, Any]]:
        """Return bounded, deduplicated alert-ready failure summaries."""
        with self._lock:
            return [s.to_dict() for s in self._failure_summaries.values()]

    def get_duration_summaries(self) -> Dict[str, Any]:
        """Return computed duration and latency summaries for all maintenance tasks."""
        with self._lock:
            return {
                name: tracker.get_summary()["duration_summary"]
                for name, tracker in self._task_metrics.items()
            }

    def get_health_digest(self, digest_id: str) -> Optional[HealthDigestSnapshot]:
        """Fetch a specific health digest snapshot by its ID from SQLite."""
        if not self.db_path or not Path(self.db_path).is_file():
            return None
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT digest_id, timestamp, schema_version, correlation_id,
                           runtime_status, payload_json, deterministic_digest
                    FROM paper_health_digests
                    WHERE digest_id = ?
                    """,
                    (digest_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                try:
                    payload = json.loads(row[5]) if row[5] else {}
                except Exception:
                    payload = {}
                return HealthDigestSnapshot(
                    digest_id=row[0],
                    timestamp=row[1],
                    schema_version=row[2],
                    correlation_id=row[3],
                    runtime_status=row[4],
                    scheduler_enabled=payload.get("scheduler_enabled", False),
                    scheduler_running=payload.get("scheduler_running", False),
                    execution_locked=payload.get("execution_locked", True),
                    paper_execution_enabled=payload.get("paper_execution_enabled", False),
                    broker_provider=payload.get("broker_provider", "none"),
                    tasks_status=payload.get("tasks_status", {}),
                    outbox_counts=payload.get("outbox_counts", {}),
                    outbox_retention=payload.get("outbox_retention", {}),
                    outbox_dispatcher_running=payload.get("outbox_dispatcher_running"),
                    sessions_total=payload.get("sessions_total", 0),
                    sessions_active=payload.get("sessions_active", 0),
                    portfolio_reconciliation=payload.get("portfolio_reconciliation"),
                    latest_equity_snapshot_id=payload.get("latest_equity_snapshot_id"),
                    latest_equity_snapshot_timestamp=payload.get("latest_equity_snapshot_timestamp"),
                    latest_equity_total=payload.get("latest_equity_total"),
                    data_quality=payload.get("data_quality"),
                    recent_errors=payload.get("recent_errors", []),
                    health_reasons=payload.get("health_reasons", []),
                    worker_status=payload.get("worker_status", "STOPPED"),
                    scheduler_uptime_seconds=payload.get("scheduler_uptime_seconds", 0.0),
                    task_metrics=payload.get("task_metrics", {}),
                    duration_summaries=payload.get("duration_summaries", {}),
                    failure_summaries=payload.get("failure_summaries", []),
                    telemetry_sources=payload.get("telemetry_sources", {}),
                    deterministic_digest=row[6] or "",
                )
        except Exception as exc:
            logger.error("Failed to query health digest %s: %s", digest_id, exc)
            return None

    def list_health_digests(self, limit: int = 50, offset: int = 0) -> List[HealthDigestSnapshot]:
        """List historical health digest snapshots with pagination."""
        if not self.db_path or not Path(self.db_path).is_file():
            return []
        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)
        results: List[HealthDigestSnapshot] = []
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT digest_id, timestamp, schema_version, correlation_id,
                           runtime_status, payload_json, deterministic_digest
                    FROM paper_health_digests
                    ORDER BY timestamp DESC
                    LIMIT ? OFFSET ?
                    """,
                    (clamped_limit, clamped_offset),
                )
                for row in cursor.fetchall():
                    try:
                        payload = json.loads(row[5]) if row[5] else {}
                    except Exception:
                        payload = {}
                    snap = HealthDigestSnapshot(
                        digest_id=row[0],
                        timestamp=row[1],
                        schema_version=row[2],
                        correlation_id=row[3],
                        runtime_status=row[4],
                        scheduler_enabled=payload.get("scheduler_enabled", False),
                        scheduler_running=payload.get("scheduler_running", False),
                        execution_locked=payload.get("execution_locked", True),
                        paper_execution_enabled=payload.get("paper_execution_enabled", False),
                        broker_provider=payload.get("broker_provider", "none"),
                        tasks_status=payload.get("tasks_status", {}),
                        outbox_counts=payload.get("outbox_counts", {}),
                        outbox_retention=payload.get("outbox_retention", {}),
                        outbox_dispatcher_running=payload.get("outbox_dispatcher_running"),
                        sessions_total=payload.get("sessions_total", 0),
                        sessions_active=payload.get("sessions_active", 0),
                        portfolio_reconciliation=payload.get("portfolio_reconciliation"),
                        latest_equity_snapshot_id=payload.get("latest_equity_snapshot_id"),
                        latest_equity_snapshot_timestamp=payload.get("latest_equity_snapshot_timestamp"),
                        latest_equity_total=payload.get("latest_equity_total"),
                        data_quality=payload.get("data_quality"),
                        recent_errors=payload.get("recent_errors", []),
                        health_reasons=payload.get("health_reasons", []),
                        worker_status=payload.get("worker_status", "STOPPED"),
                        scheduler_uptime_seconds=payload.get("scheduler_uptime_seconds", 0.0),
                        task_metrics=payload.get("task_metrics", {}),
                        duration_summaries=payload.get("duration_summaries", {}),
                        failure_summaries=payload.get("failure_summaries", []),
                        telemetry_sources=payload.get("telemetry_sources", {}),
                        deterministic_digest=row[6] or "",
                    )
                    results.append(snap)
        except Exception as exc:
            logger.error("Failed to list health digests from SQLite: %s", exc)
        return results

    def list_maintenance_runs(
        self,
        task_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[MaintenanceTaskResult]:
        """List maintenance runs with optional filtering and pagination."""
        if not self.db_path or not Path(self.db_path).is_file():
            with self._lock:
                recs = list(self._last_runs.values())
                if task_name:
                    recs = [r for r in recs if r.task_name == task_name]
                if status:
                    recs = [r for r in recs if r.status.value == status]
                return recs[offset : offset + limit]

        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)
        query = "SELECT run_id, task_name, status, started_at, completed_at, duration_seconds, records_inspected, records_processed, records_skipped, has_more, skip_reason, error_message, details_json FROM paper_maintenance_runs"
        params: List[Any] = []
        conditions: List[str] = []

        if task_name:
            conditions.append("task_name = ?")
            params.append(task_name)
        if status:
            conditions.append("status = ?")
            params.append(status)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.extend([clamped_limit, clamped_offset])

        results: List[MaintenanceTaskResult] = []
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(query, params)
                for row in cursor.fetchall():
                    try:
                        det = json.loads(row[12]) if row[12] else {}
                    except Exception:
                        det = {}
                    res = MaintenanceTaskResult(
                        run_id=row[0],
                        task_name=row[1],
                        status=MaintenanceTaskStatus(row[2]),
                        started_at=row[3],
                        completed_at=row[4],
                        duration_seconds=row[5],
                        records_inspected=row[6],
                        records_processed=row[7],
                        records_skipped=row[8],
                        has_more=bool(row[9]),
                        skip_reason=row[10],
                        error_message=row[11],
                        details=det,
                    )
                    results.append(res)
        except Exception as exc:
            logger.error("Failed to list maintenance runs from SQLite: %s", exc)
        return results

    def prune_diagnostic_history(self, keep_last_n: Optional[int] = None) -> int:
        """Bounded retention: Prune older maintenance run records and health digests in SQLite."""
        if not self.db_path or not Path(self.db_path).is_file():
            return 0
        limit_to_keep = keep_last_n if keep_last_n is not None else self.history_limit
        pruned_total = 0
        try:
            with self._get_connection() as conn:
                with conn:
                    # Prune runs
                    cursor = conn.execute(
                        """
                        DELETE FROM paper_maintenance_runs
                        WHERE run_id NOT IN (
                            SELECT run_id FROM paper_maintenance_runs
                            ORDER BY started_at DESC
                            LIMIT ?
                        )
                        """,
                        (limit_to_keep,),
                    )
                    pruned_total += cursor.rowcount

                    # Prune digests
                    cursor = conn.execute(
                        """
                        DELETE FROM paper_health_digests
                        WHERE digest_id NOT IN (
                            SELECT digest_id FROM paper_health_digests
                            ORDER BY timestamp DESC
                            LIMIT ?
                        )
                        """,
                        (limit_to_keep,),
                    )
                    pruned_total += cursor.rowcount
            return pruned_total
        except Exception as exc:
            logger.warning("Failed to prune diagnostic history: %s", exc)
            return 0

    def get_status(self) -> Dict[str, Any]:
        """Comprehensive observable scheduler and diagnostic status."""
        health_stat, reasons, _ = self.evaluate_health()
        with self._lock:
            now_mono = self._mono_clock()
            prune_last = self._last_runs.get("prune")
            digest_last = self._last_runs.get("health_digest")

            next_prune_seconds = None
            if self._is_running and self._pruning_enabled:
                elapsed = max(0.0, now_mono - self._last_prune_mono)
                next_prune_seconds = max(0.0, self.pruning_interval_seconds - elapsed)

            next_digest_seconds = None
            if self._is_running and self._health_digest_enabled:
                elapsed = max(0.0, now_mono - self._last_digest_mono)
                next_digest_seconds = max(0.0, self.health_digest_interval_seconds - elapsed)

            tasks_metric_dict = {k: v.get_summary() for k, v in self._task_metrics.items()}
            failures_list = [s.to_dict() for s in self._failure_summaries.values()]

            target_outbox = self.outbox or (getattr(self.coordinator, "outbox", None) if self.coordinator else None) or (getattr(self.dispatcher, "outbox", None) if self.dispatcher else None)
            telemetry_sources = {
                "outbox": target_outbox is not None,
                "coordinator": self.coordinator is not None,
                "dispatcher": self.dispatcher is not None,
                "database": bool(self.db_path and Path(self.db_path).is_file()),
            }

            return {
                "health_status": health_stat,
                "health_reasons": reasons,
                "worker_status": self.worker_status,
                "uptime_seconds": round(self.uptime_seconds, 2),
                "active_task_count": len(self._running_tasks),
                "shutdown_timeout_occurrences": self._shutdown_timeout_occurrences,
                "last_shutdown_wait_seconds": round(self._last_shutdown_wait_seconds, 4) if self._last_shutdown_wait_seconds is not None else None,
                "consecutive_failure_threshold": self.consecutive_failure_threshold,
                "duration_sample_limit": self.duration_sample_limit,
                "enabled": self._enabled,
                "is_running": self._is_running,
                "active_tasks": dict(self._running_tasks),
                "tasks": tasks_metric_dict,
                "aggregate_metrics": self._get_aggregate_metrics(),
                "failure_summaries": failures_list,
                "telemetry_sources": telemetry_sources,
                "pruning": {
                    "enabled": self._pruning_enabled,
                    "interval_seconds": self.pruning_interval_seconds,
                    "next_run_in_seconds": round(next_prune_seconds, 1) if next_prune_seconds is not None else None,
                    "last_run": prune_last.to_dict() if prune_last else None,
                },
                "health_digest": {
                    "enabled": self._health_digest_enabled,
                    "interval_seconds": self.health_digest_interval_seconds,
                    "next_run_in_seconds": round(next_digest_seconds, 1) if next_digest_seconds is not None else None,
                    "last_run": digest_last.to_dict() if digest_last else None,
                },
                "active_session_policy": self.active_session_policy,
                "shutdown_timeout_seconds": self.shutdown_timeout_seconds,
                "history_limit": self.history_limit,
                "worker_thread_alive": self._worker_thread.is_alive() if self._worker_thread else False,
            }
