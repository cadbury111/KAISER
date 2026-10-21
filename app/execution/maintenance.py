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


class MaintenanceTaskStatus(str, Enum):
    """Structured outcomes for scheduled and manual maintenance tasks."""

    SUCCESS = "SUCCESS"
    SKIPPED_DISABLED = "SKIPPED_DISABLED"
    SKIPPED_ACTIVE_SESSION = "SKIPPED_ACTIVE_SESSION"
    SKIPPED_RECOVERY_INCOMPLETE = "SKIPPED_RECOVERY_INCOMPLETE"
    SKIPPED_OVERLAPPING = "SKIPPED_OVERLAPPING"
    FAILED = "FAILED"


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
            "skip_reason": self.skip_reason,
            "error_message": self.error_message,
            "details": dict(self.details),
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
        health_digest_enabled: Optional[bool] = None,
        health_digest_interval_seconds: Optional[float] = None,
        active_session_policy: Optional[str] = None,
        shutdown_timeout_seconds: Optional[float] = None,
        history_limit: Optional[int] = None,
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
        raw_prune_interval = pruning_interval_seconds if pruning_interval_seconds is not None else getattr(self.settings, "maintenance_pruning_interval_seconds", 3600.0)
        raw_digest_interval = health_digest_interval_seconds if health_digest_interval_seconds is not None else getattr(self.settings, "maintenance_health_digest_interval_seconds", 300.0)
        raw_shutdown_timeout = shutdown_timeout_seconds if shutdown_timeout_seconds is not None else getattr(self.settings, "maintenance_shutdown_timeout_seconds", 5.0)
        raw_policy = active_session_policy if active_session_policy is not None else getattr(self.settings, "maintenance_active_session_policy", "skip")
        raw_history_limit = history_limit if history_limit is not None else getattr(self.settings, "maintenance_history_limit", 1000)

        # Validation
        if not math.isfinite(raw_prune_interval) or raw_prune_interval <= 0:
            raise ValidationError(f"pruning_interval_seconds must be a positive finite float, got {raw_prune_interval}")
        if not math.isfinite(raw_digest_interval) or raw_digest_interval <= 0:
            raise ValidationError(f"health_digest_interval_seconds must be a positive finite float, got {raw_digest_interval}")
        if not math.isfinite(raw_shutdown_timeout) or raw_shutdown_timeout <= 0 or raw_shutdown_timeout > 60.0:
            raise ValidationError(f"shutdown_timeout_seconds must be positive float <= 60.0, got {raw_shutdown_timeout}")
        if raw_history_limit < 10:
            raise ValidationError(f"history_limit must be at least 10, got {raw_history_limit}")

        norm_policy = str(raw_policy).strip().lower()
        if norm_policy not in ("skip", "allow"):
            raise ValidationError(f"active_session_policy must be 'skip' or 'allow', got '{raw_policy}'")

        self.pruning_interval_seconds = float(raw_prune_interval)
        self.health_digest_interval_seconds = float(raw_digest_interval)
        self.shutdown_timeout_seconds = float(raw_shutdown_timeout)
        self.active_session_policy = norm_policy
        self.history_limit = int(raw_history_limit)

        # Concurrency & lifecycle
        self._lock = threading.RLock()
        self._prune_lock = threading.Lock()
        self._digest_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._is_running = False

        # Monotonic time markers for interval evaluation
        self._last_prune_mono: float = 0.0
        self._last_digest_mono: float = 0.0

        # Memory caches
        self._last_runs: Dict[str, MaintenanceTaskResult] = {}
        self._recent_errors: List[str] = []

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

    def _recover_state(self) -> None:
        """Recover last task outcomes from SQLite on startup. Idempotent."""
        if not self.db_path or not Path(self.db_path).is_file():
            return
        with self._lock:
            try:
                with self._get_connection() as conn:
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
            except Exception as err:
                logger.warning("Could not recover previous maintenance runs: %s", err)

    def start(self) -> None:
        """Start the background maintenance worker thread if enabled."""
        with self._lock:
            if not self._enabled:
                logger.info("MaintenanceScheduler start() called, but scheduler is disabled in configuration.")
                return

            if self._is_running:
                logger.debug("MaintenanceScheduler is already running.")
                return

            self._stop_event.clear()
            self._is_running = True
            current_mono = self._mono_clock()
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
        with self._lock:
            if not self._is_running:
                return True

            self._is_running = False
            self._stop_event.set()

        t_out = timeout if timeout is not None else self.shutdown_timeout_seconds
        thread = self._worker_thread
        clean_exit = True
        if thread is not None and thread.is_alive():
            thread.join(timeout=t_out)
            if thread.is_alive():
                logger.warning("MaintenanceScheduler worker thread did not terminate within %.2fs timeout.", t_out)
                clean_exit = False

        with self._lock:
            self._worker_thread = None
            logger.info("MaintenanceScheduler stopped (clean=%s).", clean_exit)
        return clean_exit

    def _worker_loop(self) -> None:
        """Background maintenance loop polling intervals and executing tasks safely."""
        while not self._stop_event.is_set():
            now_mono = self._mono_clock()

            # 1. Prune task check
            if self._pruning_enabled:
                if (now_mono - self._last_prune_mono) >= self.pruning_interval_seconds:
                    try:
                        self.run_prune_now()
                    except Exception as exc:
                        logger.error("Unexpected error in scheduled prune task: %s", exc, exc_info=True)
                    # Advance monotonic marker to prevent unbounded catch-up
                    self._last_prune_mono = self._mono_clock()

            # 2. Health digest task check
            if self._health_digest_enabled:
                if (now_mono - self._last_digest_mono) >= self.health_digest_interval_seconds:
                    try:
                        self.run_health_digest_now()
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
    ) -> MaintenanceTaskResult:
        """Execute the outbox retention and pruning maintenance task with safety gates."""
        t_start = self._clock()
        run_id = f"mrun_prune_{uuid.uuid4().hex[:12]}"

        # Prevent overlapping execution of pruning
        if not self._prune_lock.acquire(blocking=False):
            logger.warning("Pruning maintenance task is already running; skipping overlapping execution.")
            res = MaintenanceTaskResult(
                run_id=run_id,
                task_name="prune",
                status=MaintenanceTaskStatus.SKIPPED_OVERLAPPING,
                started_at=t_start,
                completed_at=self._clock(),
                duration_seconds=self._clock() - t_start,
                skip_reason="Overlapping execution prevented.",
            )
            return res

        try:
            # 1. Enablement Safety Gate
            if not force:
                if not self._enabled:
                    res = MaintenanceTaskResult(
                        run_id=run_id,
                        task_name="prune",
                        status=MaintenanceTaskStatus.SKIPPED_DISABLED,
                        started_at=t_start,
                        completed_at=self._clock(),
                        duration_seconds=self._clock() - t_start,
                        skip_reason="Maintenance scheduler is disabled.",
                    )
                    self._record_run("prune", res)
                    return res

                if not self._pruning_enabled:
                    res = MaintenanceTaskResult(
                        run_id=run_id,
                        task_name="prune",
                        status=MaintenanceTaskStatus.SKIPPED_DISABLED,
                        started_at=t_start,
                        completed_at=self._clock(),
                        duration_seconds=self._clock() - t_start,
                        skip_reason="Maintenance pruning is disabled.",
                    )
                    self._record_run("prune", res)
                    return res

            # 2. Session Coordinator Safety Gate
            if self.coordinator is not None:
                if hasattr(self.coordinator, "is_session_recovery_complete"):
                    if not self.coordinator.is_session_recovery_complete():
                        res = MaintenanceTaskResult(
                            run_id=run_id,
                            task_name="prune",
                            status=MaintenanceTaskStatus.SKIPPED_RECOVERY_INCOMPLETE,
                            started_at=t_start,
                            completed_at=self._clock(),
                            duration_seconds=self._clock() - t_start,
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
                        res = MaintenanceTaskResult(
                            run_id=run_id,
                            task_name="prune",
                            status=MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION,
                            started_at=t_start,
                            completed_at=self._clock(),
                            duration_seconds=self._clock() - t_start,
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
                res = MaintenanceTaskResult(
                    run_id=run_id,
                    task_name="prune",
                    status=MaintenanceTaskStatus.SKIPPED_DISABLED,
                    started_at=t_start,
                    completed_at=self._clock(),
                    duration_seconds=self._clock() - t_start,
                    skip_reason="No TransactionalOutbox available for pruning.",
                )
                self._record_run("prune", res)
                return res

            # 4. Immediate Session Recheck right before destructive action
            if self.coordinator is not None and self.active_session_policy == "skip":
                if hasattr(self.coordinator, "is_session_active") and self.coordinator.is_session_active():
                    res = MaintenanceTaskResult(
                        run_id=run_id,
                        task_name="prune",
                        status=MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION,
                        started_at=t_start,
                        completed_at=self._clock(),
                        duration_seconds=self._clock() - t_start,
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
            res = MaintenanceTaskResult(
                run_id=run_id,
                task_name="prune",
                status=MaintenanceTaskStatus.SUCCESS,
                started_at=t_start,
                completed_at=t_end,
                duration_seconds=t_end - t_start,
                records_inspected=prune_res.inspected_count,
                records_processed=prune_res.pruned_count,
                records_skipped=prune_res.skipped_count,
                has_more=prune_res.has_more,
                details=prune_res.to_dict(),
            )
            self._record_run("prune", res)
            self.prune_diagnostic_history()
            return res

        except Exception as exc:
            t_end = self._clock()
            err_msg = str(exc)
            logger.error("Maintenance pruning failed [run_id=%s]: %s", run_id, exc, exc_info=True)
            self._record_error(f"Prune failure: {err_msg}")
            res = MaintenanceTaskResult(
                run_id=run_id,
                task_name="prune",
                status=MaintenanceTaskStatus.FAILED,
                started_at=t_start,
                completed_at=t_end,
                duration_seconds=t_end - t_start,
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
    ) -> HealthDigestSnapshot:
        """Generate and durably record an immutable, versioned health digest snapshot."""
        t_start = self._clock()
        cur_time = now if now is not None else t_start
        did = digest_id or f"hdig_{uuid.uuid4().hex[:12]}"

        # Prevent overlapping execution of digest task
        if not self._digest_lock.acquire(blocking=False):
            logger.warning("Health digest creation is already in progress; waiting...")
            with self._digest_lock:
                pass

        try:
            # Check for existing digest with same ID (Idempotency)
            if self.db_path and Path(self.db_path).is_file():
                existing = self.get_health_digest(did)
                if existing is not None:
                    return existing

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
                    recon_data["is_balanced"] = recon_data.get("is_reconciled", True)

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
                recent_errs = list(self._recent_errors[-10:])
                is_running = self._is_running

            # 5. Evaluate runtime status
            runtime_status = "HEALTHY"
            if recon_data and not recon_data.get("is_balanced", True):
                runtime_status = "DEGRADED"
            elif any(v.get("status") == "FAILED" for v in tasks_stat.values()):
                runtime_status = "DEGRADED"

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
            )

            # Persist to SQLite
            self._persist_health_digest(digest)

            # Record task run audit
            t_end = self._clock()
            run_result = MaintenanceTaskResult(
                run_id=f"mrun_digest_{uuid.uuid4().hex[:12]}",
                task_name="health_digest",
                status=MaintenanceTaskStatus.SUCCESS,
                started_at=t_start,
                completed_at=t_end,
                duration_seconds=t_end - t_start,
                records_processed=1,
                details={"digest_id": digest.digest_id, "runtime_status": runtime_status},
            )
            self._record_run("health_digest", run_result)
            self.prune_diagnostic_history()
            return digest

        finally:
            self._digest_lock.release()

    def _record_run(self, task_name: str, result: MaintenanceTaskResult) -> None:
        """Update last run cache and persist to SQLite."""
        with self._lock:
            self._last_runs[task_name] = result
        self._persist_task_run(result)

    def _record_error(self, err: str) -> None:
        """Record bounded recent diagnostic error."""
        with self._lock:
            self._recent_errors.append(f"[{time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime(self._clock()))}] {err[:256]}")
            if len(self._recent_errors) > 50:
                self._recent_errors = self._recent_errors[-50:]

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

            return {
                "enabled": self._enabled,
                "is_running": self._is_running,
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
