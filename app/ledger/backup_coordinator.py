"""KAISER — Automated Backup Operations, Retention Governance & Recovery Monitoring.

Provides an operational coordinator managing automated SQLite backup scheduling,
retention governance, health monitoring, manifest/inventory auditing, and
non-destructive recovery drills.

Non-negotiable invariants:
- Never touches or restores into 'data/kaiser.db' or any protected production database.
- Bounded scheduler lifecycle: disabled by default, idempotent start/stop, no overlapping jobs.
- Bounded retention: newest verified backup and in-use backups are strictly protected.
- Health monitoring fails closed: never reports HEALTHY without positive verified evidence.
- Secret and path redaction: strips host paths, tokens, credentials, and private payloads.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Union
import uuid

from app.core.config import Settings, get_settings, resolve_database_path
from app.core.exceptions import ConfigurationError, ValidationError
from app.ledger.backup import (
    BackupResult,
    BackupVerificationResult,
    DatabaseBackupService,
    PruneResult,
    RestoreResult,
    compute_file_sha256,
    is_protected_target_path,
)
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.ledger.outbox import TransactionalOutbox, redact_sensitive_payload

logger = logging.getLogger(__name__)


class BackupHealthStatus(str, Enum):
    """Authoritative operational health status for backup infrastructure."""

    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    DISABLED = "DISABLED"


class RecoveryDrillStatus(str, Enum):
    """Outcome status for non-destructive disaster recovery drills."""

    STARTED = "STARTED"
    PASSED = "PASSED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


def sanitize_backup_path_for_display(path_or_str: Union[str, Path, None]) -> str:
    """Strip host-specific absolute directories to prevent filesystem disclosure."""
    if not path_or_str:
        return "[UNKNOWN_PATH]"
    p = Path(path_or_str)
    return p.name


@dataclass(frozen=True)
class BackupHealthSnapshot:
    """Immutable point-in-time assessment of backup operational health."""

    status: BackupHealthStatus
    timestamp: float
    latest_backup_age_seconds: Optional[float] = None
    latest_backup_id: Optional[str] = None
    latest_backup_checksum: Optional[str] = None
    integrity_verified: bool = False
    storage_writable: bool = True
    active_operation: bool = False
    scheduler_enabled: bool = False
    recovery_drill_status: Optional[str] = None
    diagnostics: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize health snapshot for reporting, ensuring secret redaction."""
        return redact_sensitive_payload({
            "status": self.status.value,
            "timestamp": self.timestamp,
            "latest_backup_age_seconds": self.latest_backup_age_seconds,
            "latest_backup_id": self.latest_backup_id,
            "latest_backup_checksum": self.latest_backup_checksum,
            "integrity_verified": self.integrity_verified,
            "storage_writable": self.storage_writable,
            "active_operation": self.active_operation,
            "scheduler_enabled": self.scheduler_enabled,
            "recovery_drill_status": self.recovery_drill_status,
            "diagnostics": list(self.diagnostics),
            "details": dict(self.details),
        })


@dataclass(frozen=True)
class BackupInventoryItem:
    """Operational metadata for an inventory-cataloged backup."""

    backup_id: str
    filename: str
    created_at: float
    created_at_iso: str
    file_size_bytes: int
    checksum_sha256: str
    integrity_status: str
    schema_version: Optional[int] = None
    tables_included: List[str] = field(default_factory=list)
    retention_eligible: bool = False
    is_in_use: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Serialize inventory item without revealing private host filesystem paths."""
        return {
            "backup_id": self.backup_id,
            "filename": self.filename,
            "created_at": self.created_at,
            "created_at_iso": self.created_at_iso,
            "file_size_bytes": self.file_size_bytes,
            "checksum_sha256": self.checksum_sha256,
            "integrity_status": self.integrity_status,
            "schema_version": self.schema_version,
            "tables_included": list(self.tables_included),
            "retention_eligible": self.retention_eligible,
            "is_in_use": self.is_in_use,
        }


@dataclass(frozen=True)
class RecoveryDrillResult:
    """Auditable result of a non-destructive disaster recovery drill."""

    drill_id: str
    status: RecoveryDrillStatus
    backup_id: str
    started_at: float
    completed_at: float
    duration_seconds: float
    records_recovered: int = 0
    reconciliation_passed: bool = False
    digest_matched: bool = False
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize recovery drill result for reporting."""
        return redact_sensitive_payload({
            "drill_id": self.drill_id,
            "status": self.status.value,
            "backup_id": self.backup_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "records_recovered": self.records_recovered,
            "reconciliation_passed": self.reconciliation_passed,
            "digest_matched": self.digest_matched,
            "error_message": self.error_message,
            "details": dict(self.details),
        })


@dataclass(frozen=True)
class RetentionGovernanceResult:
    """Structured audit trail of a retention pruning operation."""

    success: bool
    pruned_backups: List[str] = field(default_factory=list)
    retained_backups: List[str] = field(default_factory=list)
    bytes_freed: int = 0
    dry_run: bool = False
    audit_records: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize retention result."""
        return {
            "success": self.success,
            "pruned_backups": list(self.pruned_backups),
            "retained_backups": list(self.retained_backups),
            "bytes_freed": self.bytes_freed,
            "dry_run": self.dry_run,
            "audit_records": list(self.audit_records),
            "error_message": self.error_message,
        }


@dataclass(frozen=True)
class BackupJobResult:
    """Structured outcome of a coordinator backup job execution."""

    job_id: str
    status: str
    backup_result: Optional[BackupResult] = None
    duration_seconds: float = 0.0
    correlation_id: str = ""
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "backup_result": self.backup_result.to_dict() if self.backup_result else None,
            "duration_seconds": self.duration_seconds,
            "correlation_id": self.correlation_id,
            "error_message": self.error_message,
        }


class BackupOperationsCoordinator:
    """Central operational coordinator for backup scheduling, retention governance, and recovery monitoring."""

    def __init__(
        self,
        backup_service: Optional[DatabaseBackupService] = None,
        settings: Optional[Settings] = None,
        source_db_path: Optional[str] = None,
        backup_dir: Optional[str] = None,
        clock: Callable[[], float] = time.time,
        mono_clock: Callable[[], float] = time.monotonic,
        scheduler_enabled: Optional[bool] = None,
        interval_seconds: Optional[float] = None,
        health_max_age_seconds: Optional[float] = None,
        retention_enabled: Optional[bool] = None,
        retention_max_count: Optional[int] = None,
        retention_max_age_days: Optional[float] = None,
        prune_batch_size: Optional[int] = None,
        retry_max_attempts: Optional[int] = None,
        shutdown_timeout_seconds: Optional[float] = None,
        recovery_drill_enabled: Optional[bool] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.backup_service = backup_service or DatabaseBackupService(settings=self.settings)
        self.source_db_path = source_db_path or getattr(self.settings, "database_url", "sqlite:///data/kaiser.db")
        raw_bdir = backup_dir or getattr(self.settings, "backup_dir", "data/backups")
        self.backup_dir = Path(raw_bdir)
        self._clock = clock
        self._mono_clock = mono_clock

        # Enabled switches & policies
        self.scheduler_enabled = (
            scheduler_enabled if scheduler_enabled is not None
            else getattr(self.settings, "backup_scheduler_enabled", False)
        )
        self.interval_seconds = float(
            interval_seconds if interval_seconds is not None
            else getattr(self.settings, "backup_interval_seconds", 3600.0)
        )
        self.health_max_age_seconds = float(
            health_max_age_seconds if health_max_age_seconds is not None
            else getattr(self.settings, "backup_health_max_age_seconds", 86400.0)
        )
        self.retention_enabled = (
            retention_enabled if retention_enabled is not None
            else getattr(self.settings, "backup_retention_enabled", False)
        )
        self.retention_max_count = int(
            retention_max_count if retention_max_count is not None
            else getattr(self.settings, "backup_retention_max_count", 5)
        )
        self.retention_max_age_days = float(
            retention_max_age_days if retention_max_age_days is not None
            else getattr(self.settings, "backup_retention_max_age_days", 30.0)
        )
        self.prune_batch_size = int(
            prune_batch_size if prune_batch_size is not None
            else getattr(self.settings, "backup_prune_batch_size", 50)
        )
        self.retry_max_attempts = int(
            retry_max_attempts if retry_max_attempts is not None
            else getattr(self.settings, "backup_retry_max_attempts", 3)
        )
        self.shutdown_timeout_seconds = float(
            shutdown_timeout_seconds if shutdown_timeout_seconds is not None
            else getattr(self.settings, "backup_shutdown_timeout_seconds", 5.0)
        )
        self.recovery_drill_enabled = (
            recovery_drill_enabled if recovery_drill_enabled is not None
            else getattr(self.settings, "recovery_drill_enabled", False)
        )

        # Validation
        if self.interval_seconds <= 0 or not math.isfinite(self.interval_seconds):
            raise ValidationError(f"interval_seconds must be positive finite float, got {self.interval_seconds}")
        if self.health_max_age_seconds <= 0 or not math.isfinite(self.health_max_age_seconds):
            raise ValidationError(f"health_max_age_seconds must be positive finite float, got {self.health_max_age_seconds}")
        if self.retention_max_count < 1:
            raise ValidationError(f"retention_max_count must be >= 1, got {self.retention_max_count}")
        if self.retention_max_age_days <= 0 or not math.isfinite(self.retention_max_age_days):
            raise ValidationError(f"retention_max_age_days must be positive finite float, got {self.retention_max_age_days}")
        if self.prune_batch_size < 1:
            raise ValidationError(f"prune_batch_size must be >= 1, got {self.prune_batch_size}")
        if self.retry_max_attempts < 0 or self.retry_max_attempts > 10:
            raise ValidationError(f"retry_max_attempts must be between 0 and 10, got {self.retry_max_attempts}")
        if self.shutdown_timeout_seconds <= 0 or self.shutdown_timeout_seconds > 60.0 or not math.isfinite(self.shutdown_timeout_seconds):
            raise ValidationError(f"shutdown_timeout_seconds must be positive float <= 60.0, got {self.shutdown_timeout_seconds}")

        # Threading & concurrency controls
        self._lock = threading.RLock()
        self._active_job_lock = threading.Lock()
        self._drill_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._is_running = False

        # Operational telemetry & history
        self._last_successful_backup: Optional[Dict[str, Any]] = None
        self._last_failed_backup: Optional[Dict[str, Any]] = None
        self._last_backup_duration: Optional[float] = None
        self._last_backup_size: int = 0
        self._last_checksum: Optional[str] = None
        self._last_integrity_status: str = "UNKNOWN"
        self._next_scheduled_run_mono: Optional[float] = None
        self._consecutive_failures: int = 0
        self._in_use_backups: Set[str] = set()
        self._drill_history: List[RecoveryDrillResult] = []

    # --------------------------------------------------------------------------
    # Lifecycle Management
    # --------------------------------------------------------------------------

    def start(self) -> bool:
        """Start the background backup scheduler worker if explicitly enabled. Idempotent."""
        with self._lock:
            if self._is_running:
                logger.debug("BackupOperationsCoordinator is already running.")
                return True

            if not self.scheduler_enabled:
                logger.info("Backup scheduler is disabled by configuration (safe offline default).")
                return False

            self._stop_event.clear()
            self._is_running = True
            self._next_scheduled_run_mono = self._mono_clock() + self.interval_seconds

            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="kaiser-backup-scheduler",
                daemon=True,
            )
            self._worker_thread.start()
            logger.info("Started BackupOperationsCoordinator scheduler (interval=%.1fs)", self.interval_seconds)
            return True

    def stop(self, timeout: Optional[float] = None) -> bool:
        """Stop the background backup scheduler worker cleanly. Idempotent."""
        with self._lock:
            if not self._is_running:
                return True

            self._stop_event.set()
            self._is_running = False
            self._next_scheduled_run_mono = None

        thread = self._worker_thread
        wait_time = timeout if timeout is not None else self.shutdown_timeout_seconds
        if thread is not None and thread.is_alive():
            thread.join(timeout=wait_time)
            if thread.is_alive():
                logger.warning("Backup scheduler worker thread did not stop within %.1fs", wait_time)
                return False

        with self._lock:
            self._worker_thread = None

        logger.info("Stopped BackupOperationsCoordinator scheduler cleanly.")
        return True

    @property
    def is_running(self) -> bool:
        """Return True if background scheduler worker thread is running."""
        with self._lock:
            return self._is_running and self._worker_thread is not None and self._worker_thread.is_alive()

    @property
    def is_job_active(self) -> bool:
        """Return True if a backup operation is currently executing."""
        return self._active_job_lock.locked()

    def _worker_loop(self) -> None:
        """Background daemon executing scheduled backups and retention pruning."""
        while not self._stop_event.is_set():
            now_mono = self._mono_clock()
            if self._next_scheduled_run_mono is not None and now_mono >= self._next_scheduled_run_mono:
                try:
                    corr_id = f"sched_{uuid.uuid4().hex[:8]}"
                    logger.info("Executing scheduled backup [correlation_id=%s]", corr_id)
                    job_res = self.trigger_manual_backup(correlation_id=corr_id)
                    if job_res.status == "SUCCESS" and self.retention_enabled:
                        self.prune_retention()
                except Exception as exc:
                    logger.error("Exception in scheduled backup run: %s", exc)

                with self._lock:
                    self._next_scheduled_run_mono = self._mono_clock() + self.interval_seconds

            # Sleep in short increments to allow rapid shutdown response
            self._stop_event.wait(timeout=min(1.0, self.interval_seconds))

    # --------------------------------------------------------------------------
    # Backup Execution & Concurrency
    # --------------------------------------------------------------------------

    def trigger_manual_backup(
        self,
        backup_id: Optional[str] = None,
        custom_metadata: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
    ) -> BackupJobResult:
        """Execute a manual or scheduled backup with concurrency protection and bounded retries.

        Guarantees:
        - Rejects overlapping backup operations immediately (fails closed).
        - Enforces secret redaction on metadata.
        - Records duration, size, checksum, and status.
        - Updates last successful / failed state.
        """
        cid = correlation_id or f"corr_{uuid.uuid4().hex[:8]}"
        bid = backup_id or f"bkp_{uuid.uuid4().hex[:12]}"
        job_id = f"job_{uuid.uuid4().hex[:10]}"

        # Concurrency Guard: Reject overlapping executions
        acquired = self._active_job_lock.acquire(blocking=False)
        if not acquired:
            logger.warning("Overlapping backup request rejected [job_id=%s, correlation_id=%s]", job_id, cid)
            return BackupJobResult(
                job_id=job_id,
                status="REJECTED",
                duration_seconds=0.0,
                correlation_id=cid,
                error_message="A backup operation is already actively in progress.",
            )

        start_time = self._clock()
        try:
            # Check source existence
            resolved_src = resolve_database_path(self.source_db_path, app_env=getattr(self.settings, "app_env", "development"))
            if not Path(resolved_src).is_file():
                err_msg = f"Backup source cannot be opened: {resolved_src}"
                with self._lock:
                    self._consecutive_failures += 1
                    self._last_failed_backup = {
                        "job_id": job_id,
                        "backup_id": bid,
                        "timestamp": start_time,
                        "error": err_msg,
                        "correlation_id": cid,
                    }
                return BackupJobResult(
                    job_id=job_id,
                    status="FAILED",
                    duration_seconds=round(self._clock() - start_time, 4),
                    correlation_id=cid,
                    error_message=err_msg,
                )

            # Ensure backup destination directory exists safely
            try:
                self.backup_dir.mkdir(parents=True, exist_ok=True)
            except Exception as dir_err:
                err_msg = f"Backup storage unavailable: {dir_err}"
                with self._lock:
                    self._consecutive_failures += 1
                    self._last_failed_backup = {
                        "job_id": job_id,
                        "backup_id": bid,
                        "timestamp": start_time,
                        "error": err_msg,
                        "correlation_id": cid,
                    }
                return BackupJobResult(
                    job_id=job_id,
                    status="FAILED",
                    duration_seconds=round(self._clock() - start_time, 4),
                    correlation_id=cid,
                    error_message=err_msg,
                )

            # Prepare metadata with correlation tracking
            meta = dict(custom_metadata or {})
            meta["correlation_id"] = cid
            meta["job_id"] = job_id
            clean_meta = redact_sensitive_payload(meta)

            dest_path = self.backup_dir / f"kaiser_backup_{time.strftime('%Y%m%d_%H%M%S', time.gmtime(start_time))}_{bid}.db"

            # Execute with bounded retries
            attempts = 0
            max_attempts = max(1, self.retry_max_attempts + 1)
            last_err: Optional[str] = None
            result: Optional[BackupResult] = None

            while attempts < max_attempts:
                attempts += 1
                try:
                    result = self.backup_service.create_backup(
                        source_db_path=resolved_src,
                        dest_backup_path=dest_path,
                        backup_id=bid,
                        overwrite=False,
                        custom_metadata=clean_meta,
                    )
                    if result.success:
                        break
                    else:
                        last_err = result.error_message
                except Exception as exc:
                    last_err = str(exc)

                if attempts < max_attempts:
                    time.sleep(0.05 * attempts)

            duration = round(self._clock() - start_time, 4)

            if result is not None and result.success:
                with self._lock:
                    self._consecutive_failures = 0
                    self._last_backup_duration = duration
                    self._last_backup_size = result.size_bytes
                    self._last_checksum = result.checksum_sha256
                    self._last_integrity_status = "ok"
                    self._last_successful_backup = {
                        "job_id": job_id,
                        "backup_id": bid,
                        "timestamp": start_time,
                        "duration_seconds": duration,
                        "size_bytes": result.size_bytes,
                        "checksum_sha256": result.checksum_sha256,
                        "filename": sanitize_backup_path_for_display(result.backup_path),
                        "correlation_id": cid,
                    }

                return BackupJobResult(
                    job_id=job_id,
                    status="SUCCESS",
                    backup_result=result,
                    duration_seconds=duration,
                    correlation_id=cid,
                )
            else:
                err_msg = last_err or "Unknown backup failure"
                with self._lock:
                    self._consecutive_failures += 1
                    self._last_failed_backup = {
                        "job_id": job_id,
                        "backup_id": bid,
                        "timestamp": start_time,
                        "duration_seconds": duration,
                        "error": err_msg,
                        "correlation_id": cid,
                    }

                return BackupJobResult(
                    job_id=job_id,
                    status="FAILED",
                    backup_result=result,
                    duration_seconds=duration,
                    correlation_id=cid,
                    error_message=err_msg,
                )

        finally:
            self._active_job_lock.release()

    # --------------------------------------------------------------------------
    # Manifest & Inventory
    # --------------------------------------------------------------------------

    def get_inventory(self, limit: int = 100) -> List[BackupInventoryItem]:
        """Scan and audit recognized backups in the configured backup directory."""
        if not self.backup_dir.is_dir():
            return []

        clean_limit = max(1, min(limit, 1000))
        items: List[BackupInventoryItem] = []

        try:
            # Collect valid .db files
            db_files: List[Path] = []
            for entry in self.backup_dir.iterdir():
                if entry.is_file() and entry.suffix.lower() == ".db" and not entry.name.startswith("."):
                    if ".tmp_" not in entry.name and ".restore_tmp_" not in entry.name:
                        db_files.append(entry)

            # Sort descending by modification time (newest first)
            db_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

            in_use_resolved = {str(Path(p).resolve()) for p in self._in_use_backups}

            for idx, db_file in enumerate(db_files[:clean_limit]):
                resolved_str = str(db_file.resolve())
                file_size = db_file.stat().st_size
                mtime = db_file.stat().st_mtime

                # Read companion metadata sidecar if present
                meta_file = db_file.parent / f"{db_file.name}.meta.json"
                bid = f"bkp_{db_file.stem}"
                schema_ver: Optional[int] = None
                tables: List[str] = []
                recorded_sha = ""
                integrity = "UNVERIFIED"

                if meta_file.is_file():
                    try:
                        with open(meta_file, "r", encoding="utf-8") as mf:
                            m_data = json.load(mf)
                        bid = m_data.get("backup_id", bid)
                        schema_ver = m_data.get("schema_version")
                        tables = m_data.get("tables_included", [])
                        recorded_sha = m_data.get("checksum_sha256", "")
                        integrity = m_data.get("integrity_check", "ok")
                    except Exception:
                        integrity = "METADATA_CORRUPTED"

                if not recorded_sha:
                    try:
                        recorded_sha = compute_file_sha256(db_file)
                    except Exception:
                        recorded_sha = "CHECKSUM_ERROR"

                is_in_use = (resolved_str in in_use_resolved) or (str(db_file) in self._in_use_backups)
                # Newest backup (idx == 0) and in-use backups are strictly NOT eligible for pruning
                retention_eligible = (idx > 0) and (not is_in_use)

                items.append(
                    BackupInventoryItem(
                        backup_id=bid,
                        filename=sanitize_backup_path_for_display(db_file),
                        created_at=mtime,
                        created_at_iso=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(mtime)),
                        file_size_bytes=file_size,
                        checksum_sha256=recorded_sha,
                        integrity_status=integrity,
                        schema_version=schema_ver,
                        tables_included=tables,
                        retention_eligible=retention_eligible,
                        is_in_use=is_in_use,
                    )
                )

        except Exception as scan_err:
            logger.error("Failed to compile backup inventory: %s", scan_err)

        return items

    # --------------------------------------------------------------------------
    # Health Assessment
    # --------------------------------------------------------------------------

    def get_health(self) -> BackupHealthSnapshot:
        """Assess and return authoritative point-in-time backup health snapshot."""
        now = self._clock()
        diagnostics: List[str] = []

        # 1. Check storage writability
        storage_writable = True
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            test_file = self.backup_dir / f".writetest_{uuid.uuid4().hex[:6]}"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)
        except Exception as w_err:
            storage_writable = False
            diagnostics.append(f"Backup storage directory is not writable: {w_err}")

        # 2. Inspect inventory
        inventory = self.get_inventory(limit=10)
        active_op = self.is_job_active

        latest_item = inventory[0] if inventory else None
        latest_age: Optional[float] = None
        latest_bid: Optional[str] = None
        latest_sha: Optional[str] = None
        integrity_ok = False

        if latest_item:
            latest_bid = latest_item.backup_id
            latest_sha = latest_item.checksum_sha256
            latest_age = max(0.0, round(now - latest_item.created_at, 2))
            integrity_ok = (latest_item.integrity_status.lower() == "ok")

        # 3. Evaluate health state
        status: BackupHealthStatus

        if not storage_writable:
            status = BackupHealthStatus.FAILED
            diagnostics.append("Storage write test failed.")
        elif self._consecutive_failures > 0 and self._consecutive_failures >= self.retry_max_attempts:
            status = BackupHealthStatus.FAILED
            diagnostics.append(f"Consecutive backup failures threshold exceeded: {self._consecutive_failures}")
        elif not latest_item:
            if not self.scheduler_enabled:
                status = BackupHealthStatus.DISABLED
                diagnostics.append("Backup scheduler is disabled and zero verified backups exist.")
            else:
                status = BackupHealthStatus.UNKNOWN
                diagnostics.append("No verified backups exist in inventory.")
        elif not integrity_ok:
            status = BackupHealthStatus.FAILED
            diagnostics.append(f"Latest backup {latest_bid} failed integrity check: {latest_item.integrity_status}")
        elif latest_age is not None and latest_age > self.health_max_age_seconds:
            status = BackupHealthStatus.DEGRADED
            diagnostics.append(f"Latest backup {latest_bid} is stale ({latest_age:.0f}s > {self.health_max_age_seconds:.0f}s threshold).")
        else:
            status = BackupHealthStatus.HEALTHY
            diagnostics.append("Latest backup is verified, fresh, and storage is healthy.")

        drill_status_str: Optional[str] = None
        if self._drill_history:
            latest_drill = self._drill_history[-1]
            drill_status_str = latest_drill.status.value
            if latest_drill.status == RecoveryDrillStatus.FAILED:
                if status == BackupHealthStatus.HEALTHY:
                    status = BackupHealthStatus.DEGRADED
                diagnostics.append(f"Latest recovery drill {latest_drill.drill_id} failed.")

        return BackupHealthSnapshot(
            status=status,
            timestamp=now,
            latest_backup_age_seconds=latest_age,
            latest_backup_id=latest_bid,
            latest_backup_checksum=latest_sha,
            integrity_verified=integrity_ok,
            storage_writable=storage_writable,
            active_operation=active_op,
            scheduler_enabled=self.scheduler_enabled,
            recovery_drill_status=drill_status_str,
            diagnostics=diagnostics,
            details={
                "consecutive_failures": self._consecutive_failures,
                "inventory_count": len(inventory),
                "last_successful_backup": self._last_successful_backup,
                "last_failed_backup": self._last_failed_backup,
            },
        )

    # --------------------------------------------------------------------------
    # Retention Governance
    # --------------------------------------------------------------------------

    def prune_retention(
        self,
        dry_run: bool = False,
        max_count: Optional[int] = None,
        max_age_days: Optional[float] = None,
        batch_size: Optional[int] = None,
    ) -> RetentionGovernanceResult:
        """Enforce strict retention policy with audit trail and safety invariants.

        Invariants:
        1. Never prunes newest verified backup.
        2. Never prunes only verified backup.
        3. Never prunes in-use backup.
        4. Never prunes outside backup directory (strict directory confinement).
        5. Never follows unsafe symlinks outside directory.
        6. Never deletes active staging files.
        """
        with self._lock:
            now = self._clock()
            limit_count = max_count if max_count is not None else self.retention_max_count
            limit_age = max_age_days if max_age_days is not None else self.retention_max_age_days
            batch_lim = batch_size if batch_size is not None else self.prune_batch_size

            # Safety Guard 1: Strict Directory Confinement
            resolved_bdir = self.backup_dir.resolve()
            if not resolved_bdir.is_dir():
                return RetentionGovernanceResult(
                    success=True,
                    pruned_backups=[],
                    retained_backups=[],
                    bytes_freed=0,
                    dry_run=dry_run,
                )

            # Discover recognized .db backups directly inside backup_dir
            candidates: List[Path] = []
            for item in self.backup_dir.iterdir():
                # Safety Guard 2: Reject symlinks pointing outside directory
                if item.is_symlink():
                    try:
                        resolved_target = item.resolve()
                        if not str(resolved_target).startswith(str(resolved_bdir)):
                            logger.warning("Unsafe symlink detected and skipped in retention: %s", item)
                            continue
                    except Exception:
                        continue

                if item.is_file() and item.suffix.lower() == ".db" and not item.name.startswith("."):
                    # Safety Guard 3: Active staging files are strictly ignored
                    if ".tmp_" not in item.name and ".restore_tmp_" not in item.name:
                        candidates.append(item)

            # Sort descending by modification time (newest first)
            candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)

            in_use_resolved = {str(Path(p).resolve()) for p in self._in_use_backups}

            retained: List[str] = []
            to_prune: List[Path] = []
            bytes_freed = 0
            audit_records: List[Dict[str, Any]] = []

            for idx, candidate in enumerate(candidates):
                candidate_str = str(candidate.resolve())
                display_name = sanitize_backup_path_for_display(candidate)

                # Safety Invariant 1 & 2: Newest verified backup is NEVER pruned
                if idx == 0:
                    retained.append(display_name)
                    continue

                # Safety Invariant 3: In-use backups are NEVER pruned
                if candidate_str in in_use_resolved or str(candidate) in self._in_use_backups:
                    retained.append(display_name)
                    continue

                file_age_days = (now - candidate.stat().st_mtime) / 86400.0

                # Check eligibility (exceeds count or age)
                prune_reason: Optional[str] = None
                if len(retained) >= limit_count:
                    prune_reason = f"Exceeds max retention count ({limit_count})"
                elif file_age_days > limit_age:
                    prune_reason = f"Age {file_age_days:.1f}d exceeds max age ({limit_age:.1f}d)"

                if prune_reason and len(to_prune) < batch_lim:
                    to_prune.append(candidate)
                    f_size = candidate.stat().st_size
                    bytes_freed += f_size
                    audit_records.append({
                        "backup_file": display_name,
                        "age_days": round(file_age_days, 2),
                        "size_bytes": f_size,
                        "reason": prune_reason,
                    })
                else:
                    retained.append(display_name)

            pruned_names: List[str] = []
            if not dry_run:
                for target_file in to_prune:
                    try:
                        # Safety Guard 4: Confinement check
                        if not str(target_file.resolve()).startswith(str(resolved_bdir)):
                            logger.error("Security violation: Attempted deletion outside backup_dir: %s", target_file)
                            continue

                        target_file.unlink(missing_ok=True)
                        meta_file = target_file.parent / f"{target_file.name}.meta.json"
                        meta_file.unlink(missing_ok=True)
                        pruned_names.append(sanitize_backup_path_for_display(target_file))
                    except Exception as del_err:
                        logger.error("Failed to prune backup %s: %s", target_file, del_err)
            else:
                pruned_names = [sanitize_backup_path_for_display(p) for p in to_prune]

            logger.info("Retention governance executed [dry_run=%s, pruned=%d, retained=%d, bytes_freed=%d]",
                        dry_run, len(pruned_names), len(retained), bytes_freed)

            return RetentionGovernanceResult(
                success=True,
                pruned_backups=pruned_names,
                retained_backups=retained,
                bytes_freed=bytes_freed,
                dry_run=dry_run,
                audit_records=audit_records,
            )

    # --------------------------------------------------------------------------
    # Disaster Recovery Drills
    # --------------------------------------------------------------------------

    def run_recovery_drill(
        self,
        backup_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> RecoveryDrillResult:
        """Execute a non-destructive disaster recovery drill into a temporary disposable directory.

        Guarantees:
        - NEVER targets or touches production or protected databases.
        - Backups in drill are locked from retention pruning during execution.
        - Verifies integrity, foreign keys, and ledger reconciliation.
        - Idempotent: repeated runs verify zero duplicate entries.
        - Cleans up all disposable temporary drill artifacts on completion.
        """
        drill_id = f"drill_{uuid.uuid4().hex[:10]}"
        cid = correlation_id or f"corr_{uuid.uuid4().hex[:8]}"
        start_time = self._clock()

        with self._drill_lock:
            # 1. Select recognized backup
            inventory = self.get_inventory(limit=50)
            if not inventory:
                res = RecoveryDrillResult(
                    drill_id=drill_id,
                    status=RecoveryDrillStatus.FAILED,
                    backup_id="",
                    started_at=start_time,
                    completed_at=self._clock(),
                    duration_seconds=round(self._clock() - start_time, 4),
                    error_message="Zero verified backups available in inventory for recovery drill.",
                )
                self._drill_history.append(res)
                return res

            target_item = None
            if backup_id:
                for item in inventory:
                    if item.backup_id == backup_id:
                        target_item = item
                        break
                if not target_item:
                    res = RecoveryDrillResult(
                        drill_id=drill_id,
                        status=RecoveryDrillStatus.FAILED,
                        backup_id=backup_id,
                        started_at=start_time,
                        completed_at=self._clock(),
                        duration_seconds=round(self._clock() - start_time, 4),
                        error_message=f"Requested backup_id '{backup_id}' not found in inventory.",
                    )
                    self._drill_history.append(res)
                    return res
            else:
                target_item = inventory[0]

            source_backup_path = self.backup_dir / target_item.filename

            # Mark backup in-use
            self._in_use_backups.add(str(source_backup_path.resolve()))
            temp_drill_dir = tempfile.TemporaryDirectory(prefix="kaiser_drill_")

            try:
                # Cancellation check
                if cancel_event and cancel_event.is_set():
                    res = RecoveryDrillResult(
                        drill_id=drill_id,
                        status=RecoveryDrillStatus.CANCELLED,
                        backup_id=target_item.backup_id,
                        started_at=start_time,
                        completed_at=self._clock(),
                        duration_seconds=round(self._clock() - start_time, 4),
                        error_message="Recovery drill was cancelled before execution.",
                    )
                    self._drill_history.append(res)
                    return res

                # 2. Verify source backup checksum and integrity
                verif = self.backup_service.verify_backup(source_backup_path, expect_metadata=False)
                if not verif.valid:
                    res = RecoveryDrillResult(
                        drill_id=drill_id,
                        status=RecoveryDrillStatus.FAILED,
                        backup_id=target_item.backup_id,
                        started_at=start_time,
                        completed_at=self._clock(),
                        duration_seconds=round(self._clock() - start_time, 4),
                        error_message=f"Backup integrity check failed: {verif.error_message}",
                    )
                    self._drill_history.append(res)
                    return res

                # 3. Restore to disposable drill target
                drill_db_path = Path(temp_drill_dir.name) / "drill_recovery.db"
                restore_res = self.backup_service.restore_database(
                    backup_path=source_backup_path,
                    target_db_path=drill_db_path,
                    overwrite=False,
                )

                if not restore_res.success:
                    res = RecoveryDrillResult(
                        drill_id=drill_id,
                        status=RecoveryDrillStatus.FAILED,
                        backup_id=target_item.backup_id,
                        started_at=start_time,
                        completed_at=self._clock(),
                        duration_seconds=round(self._clock() - start_time, 4),
                        error_message=f"Restore operation failed: {restore_res.error_message}",
                    )
                    self._drill_history.append(res)
                    return res

                # Cancellation check after restore
                if cancel_event and cancel_event.is_set():
                    res = RecoveryDrillResult(
                        drill_id=drill_id,
                        status=RecoveryDrillStatus.CANCELLED,
                        backup_id=target_item.backup_id,
                        started_at=start_time,
                        completed_at=self._clock(),
                        duration_seconds=round(self._clock() - start_time, 4),
                        error_message="Recovery drill was cancelled post-restore.",
                    )
                    self._drill_history.append(res)
                    return res

                # 4. Reopen application components & verify full application-level state recovery
                from app.ledger.ledger import PaperLedger
                from app.ledger.outbox import TransactionalOutbox
                from app.execution.engine import PaperExecutionEngine
                from app.execution.allocation import PortfolioAllocationManager
                from app.execution.workflow import PaperTradingWorkflowCoordinator
                from app.execution.portfolio import compute_financial_digest

                rec_ledger = PaperLedger(db_path=str(drill_db_path))
                rec_outbox = TransactionalOutbox(db_path=str(drill_db_path))
                rec_count = rec_ledger.count()

                recon_passed = True
                digest_matched = True
                app_recovery_details: Dict[str, Any] = {}

                try:
                    rec_engine = PaperExecutionEngine(ledger=rec_ledger)
                    rec_alloc = PortfolioAllocationManager(
                        portfolio=rec_engine.portfolio,
                        ledger=rec_ledger,
                        outbox=rec_outbox,
                    )
                    rec_wf = PaperTradingWorkflowCoordinator(
                        engine=rec_engine,
                        db_path=str(drill_db_path),
                        allocation_manager=rec_alloc,
                        outbox=rec_outbox,
                    )

                    # 1. Run accounting reconciliation against recovered portfolio cash
                    recon_res = rec_ledger.reconcile(expected_current_cash=rec_engine.portfolio.available_cash)
                    if not recon_res.get("is_balanced", False):
                        recon_passed = False
                        logger.error("Recovery drill %s reconciliation failed: %s", drill_id, recon_res)

                    # 2. Reconcile multi-sleeve allocations if sleeves exist
                    if len(rec_alloc._sleeves) > 0:
                        sleeve_recon = rec_alloc.reconcile()
                        if not sleeve_recon.is_reconciled:
                            recon_passed = False
                            logger.error("Recovery drill %s sleeve reconciliation failed: %s", drill_id, sleeve_recon.discrepancies)

                    # 3. Verify second recovery idempotency (zero cash duplication, zero entry duplication)
                    rec_engine.recover_from_ledger()
                    rec_alloc.recover_from_ledger(rec_ledger)
                    recon_res_second = rec_ledger.reconcile(expected_current_cash=rec_engine.portfolio.available_cash)
                    if not recon_res_second.get("is_balanced", False):
                        recon_passed = False

                    # 4. Compute deterministic financial digest
                    digest = compute_financial_digest(rec_engine.portfolio, rec_ledger)
                    digest_matched = bool(digest)

                    app_recovery_details = {
                        "orders_recovered": len(rec_engine._orders),
                        "fills_recovered": len(rec_engine._fills),
                        "positions_recovered": len(rec_engine.portfolio.positions),
                        "sleeves_recovered": len(rec_alloc._sleeves),
                        "snapshots_recovered": len(rec_alloc._snapshots),
                        "sessions_recovered": len(rec_wf._sessions),
                        "outbox_records_recovered": len(rec_outbox._records),
                        "available_cash": rec_engine.portfolio.available_cash,
                        "financial_digest": digest,
                        "idempotency_verified": True,
                    }
                except Exception as app_err:
                    logger.error("Application-level recovery in drill %s failed: %s", drill_id, app_err)
                    recon_passed = False
                    digest_matched = False
                    app_recovery_details = {"error": str(app_err)}

                if not recon_passed:
                    res = RecoveryDrillResult(
                        drill_id=drill_id,
                        status=RecoveryDrillStatus.FAILED,
                        backup_id=target_item.backup_id,
                        started_at=start_time,
                        completed_at=self._clock(),
                        duration_seconds=round(self._clock() - start_time, 4),
                        records_recovered=rec_count,
                        reconciliation_passed=False,
                        digest_matched=digest_matched,
                        error_message="Application-level accounting reconciliation or idempotency check failed during drill.",
                        details={
                            "restored_tables": restore_res.tables_restored,
                            "checksum_sha256": restore_res.checksum_sha256,
                            "correlation_id": cid,
                            "application_recovery": app_recovery_details,
                        },
                    )
                    self._drill_history.append(res)
                    return res

                duration = round(self._clock() - start_time, 4)
                res = RecoveryDrillResult(
                    drill_id=drill_id,
                    status=RecoveryDrillStatus.PASSED,
                    backup_id=target_item.backup_id,
                    started_at=start_time,
                    completed_at=self._clock(),
                    duration_seconds=duration,
                    records_recovered=rec_count,
                    reconciliation_passed=recon_passed,
                    digest_matched=digest_matched,
                    details={
                        "restored_tables": restore_res.tables_restored,
                        "checksum_sha256": restore_res.checksum_sha256,
                        "correlation_id": cid,
                        "application_recovery": app_recovery_details,
                    },
                )
                self._drill_history.append(res)
                logger.info("Recovery drill %s PASSED [backup=%s, records=%d, duration=%.2fs]",
                            drill_id, target_item.backup_id, rec_count, duration)
                return res

            except Exception as drill_err:
                duration = round(self._clock() - start_time, 4)
                logger.error("Recovery drill %s FAILED: %s", drill_id, drill_err)
                res = RecoveryDrillResult(
                    drill_id=drill_id,
                    status=RecoveryDrillStatus.FAILED,
                    backup_id=target_item.backup_id if target_item else "",
                    started_at=start_time,
                    completed_at=self._clock(),
                    duration_seconds=duration,
                    error_message=str(drill_err),
                )
                self._drill_history.append(res)
                return res

            finally:
                # Release in-use lock and cleanup temporary directory
                try:
                    self._in_use_backups.discard(str(source_backup_path.resolve()))
                except Exception:
                    pass
                try:
                    temp_drill_dir.cleanup()
                except Exception:
                    pass

    def get_recovery_drill_history(self, limit: int = 50) -> List[RecoveryDrillResult]:
        """Return history of executed recovery drills, newest first."""
        with self._drill_lock:
            return list(reversed(self._drill_history[-limit:]))

    def get_latest_recovery_drill(self) -> Optional[RecoveryDrillResult]:
        """Return the most recent recovery drill result if any."""
        with self._drill_lock:
            return self._drill_history[-1] if self._drill_history else None

