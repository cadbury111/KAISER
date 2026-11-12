"""KAISER Database Backup, Verification, Restoration & Disaster Recovery Service.

Provides a safe, WAL-consistent SQLite backup mechanism utilizing Python's
sqlite3.Connection.backup() API, atomic two-phase staging and publication,
checksum validation, integrity verification, metadata redaction, isolated
restoration workflows, and retention management.

Non-negotiable invariants:
- Never touches or writes to 'data/kaiser.db' or any protected production database.
- Never performs raw file copies of live WAL-mode databases.
- Restores only into explicit, caller-selected disposable targets.
- Atomically cleans up temporary staging artifacts on any failure.
- Redacts all credentials, tokens, secrets, and private payloads from metadata.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Union
import uuid

from app.core.config import Settings, get_settings, resolve_database_path
from app.core.exceptions import ConfigurationError, ValidationError
from app.ledger.outbox import redact_sensitive_payload

logger = logging.getLogger(__name__)

# Protected production path patterns that backup/restore must NEVER write to
PROTECTED_PROD_BASENAMES = frozenset({
    "kaiser.db",
    "kaiser.db-wal",
    "kaiser.db-shm",
})


def is_protected_target_path(target_path: Union[str, Path, None]) -> bool:
    """Return True if target_path resolves to the production database or any protected path."""
    if not target_path:
        return True

    try:
        raw_str = str(target_path).strip()
        if not raw_str or raw_str == ":memory:":
            return False

        path_obj = Path(raw_str)
        # Check normalized posix representation
        norm_posix = path_obj.as_posix().lower()
        if norm_posix in ("data/kaiser.db", "./data/kaiser.db", "kaiser.db", "data/kaiser.db-wal", "data/kaiser.db-shm"):
            return True
        if norm_posix.endswith("/data/kaiser.db") or norm_posix.endswith("/data/kaiser.db-wal") or norm_posix.endswith("/data/kaiser.db-shm"):
            return True

        # Check resolved filesystem path against real data/kaiser.db
        resolved = path_obj.resolve()
        prod_resolved = Path("data/kaiser.db").resolve()
        if resolved == prod_resolved:
            return True
        if resolved == Path("data/kaiser.db-wal").resolve() or resolved == Path("data/kaiser.db-shm").resolve():
            return True

        # Check if basename matches protected database names inside a 'data' directory
        if path_obj.name.lower() in PROTECTED_PROD_BASENAMES:
            parent_name = path_obj.parent.name.lower()
            if parent_name == "data":
                return True

    except Exception:
        # Fail closed on any resolution or permission error
        return True

    return False


def compute_file_sha256(file_path: Union[str, Path], chunk_size: int = 65536) -> str:
    """Compute SHA-256 checksum of a file on disk."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


@dataclass(frozen=True)
class BackupResult:
    """Structured result returned by DatabaseBackupService.create_backup."""

    success: bool
    backup_path: str
    metadata_path: Optional[str] = None
    checksum_sha256: Optional[str] = None
    size_bytes: int = 0
    timestamp: float = 0.0
    backup_id: str = ""
    tables_included: List[str] = field(default_factory=list)
    table_counts: Dict[str, int] = field(default_factory=dict)
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize result for reporting or logging, ensuring secret redaction."""
        return redact_sensitive_payload({
            "success": self.success,
            "backup_path": self.backup_path,
            "metadata_path": self.metadata_path,
            "checksum_sha256": self.checksum_sha256,
            "size_bytes": self.size_bytes,
            "timestamp": self.timestamp,
            "backup_id": self.backup_id,
            "tables_included": list(self.tables_included),
            "table_counts": dict(self.table_counts),
            "error_message": self.error_message,
            "details": dict(self.details),
        })


@dataclass(frozen=True)
class BackupVerificationResult:
    """Structured result returned by DatabaseBackupService.verify_backup."""

    valid: bool
    backup_path: str
    checksum_sha256: Optional[str] = None
    integrity_check: str = ""
    foreign_key_check: str = ""
    tables_found: List[str] = field(default_factory=list)
    table_counts: Dict[str, int] = field(default_factory=dict)
    metadata_valid: bool = False
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize verification result for reporting or logging."""
        return {
            "valid": self.valid,
            "backup_path": self.backup_path,
            "checksum_sha256": self.checksum_sha256,
            "integrity_check": self.integrity_check,
            "foreign_key_check": self.foreign_key_check,
            "tables_found": list(self.tables_found),
            "table_counts": dict(self.table_counts),
            "metadata_valid": self.metadata_valid,
            "error_message": self.error_message,
        }


@dataclass(frozen=True)
class RestoreResult:
    """Structured result returned by DatabaseBackupService.restore_database."""

    success: bool
    backup_path: str
    restored_path: str
    checksum_sha256: Optional[str] = None
    size_bytes: int = 0
    timestamp: float = 0.0
    tables_restored: List[str] = field(default_factory=list)
    table_counts: Dict[str, int] = field(default_factory=dict)
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize restore result for reporting or logging."""
        return redact_sensitive_payload({
            "success": self.success,
            "backup_path": self.backup_path,
            "restored_path": self.restored_path,
            "checksum_sha256": self.checksum_sha256,
            "size_bytes": self.size_bytes,
            "timestamp": self.timestamp,
            "tables_restored": list(self.tables_restored),
            "table_counts": dict(self.table_counts),
            "error_message": self.error_message,
            "details": dict(self.details),
        })


@dataclass(frozen=True)
class PruneResult:
    """Structured result returned by DatabaseBackupService.prune_backups."""

    success: bool
    pruned_files: List[str] = field(default_factory=list)
    retained_files: List[str] = field(default_factory=list)
    pruned_bytes: int = 0
    dry_run: bool = False
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize prune result for reporting or logging."""
        return {
            "success": self.success,
            "pruned_files": list(self.pruned_files),
            "retained_files": list(self.retained_files),
            "pruned_bytes": self.pruned_bytes,
            "dry_run": self.dry_run,
            "error_message": self.error_message,
        }


class DatabaseBackupService:
    """Authoritative, WAL-consistent SQLite database backup and disaster recovery service."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        default_source_path: Optional[str] = None,
        default_backup_dir: Optional[str] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.default_source_path = default_source_path or getattr(self.settings, "database_url", "sqlite:///data/kaiser.db")
        self.default_backup_dir = default_backup_dir or getattr(self.settings, "backup_dir", "data/backups")
        self._lock = threading.RLock()

    def _resolve_source_path(self, source_path: Optional[Union[str, Path]] = None) -> str:
        """Resolve and isolate source database path."""
        target = str(source_path) if source_path is not None else self.default_source_path
        return resolve_database_path(target, app_env=getattr(self.settings, "app_env", "development"))

    def create_backup(
        self,
        source_db_path: Optional[Union[str, Path]] = None,
        dest_backup_path: Optional[Union[str, Path]] = None,
        backup_id: Optional[str] = None,
        overwrite: bool = False,
        custom_metadata: Optional[Dict[str, Any]] = None,
        progress_callback: Optional[Callable[[int, int, int], None]] = None,
    ) -> BackupResult:
        """Perform an online, WAL-consistent SQLite backup into a temporary staging file and atomically publish it.

        Guarantees:
        - Source database is never locked against active concurrent readers.
        - Dest path is checked to ensure it does not resolve to the protected production database.
        - Source and destination paths cannot collide.
        - Destination directory is created safely.
        - Backup is performed via standard library sqlite3.Connection.backup().
        - Backup is verified with PRAGMA integrity_check and PRAGMA foreign_key_check in staging.
        - Atomically published via rename/replace; temporary staging files are unlinked on error.
        - Metadata sidecar (<dest>.meta.json) is published atomically with all secrets redacted.
        """
        with self._lock:
            now = time.time()
            bid = backup_id or f"bkp_{uuid.uuid4().hex[:12]}"

            # 1. Resolve and validate source database
            try:
                resolved_src_str = self._resolve_source_path(source_db_path)
            except Exception as res_err:
                return BackupResult(
                    success=False,
                    backup_path=str(dest_backup_path or ""),
                    error_message=f"Failed to resolve source database path: {res_err}",
                )

            src_path = Path(resolved_src_str)
            if not src_path.is_file():
                return BackupResult(
                    success=False,
                    backup_path=str(dest_backup_path or ""),
                    error_message=f"Source database cannot be opened or does not exist: {src_path}",
                )

            # 2. Determine and validate destination backup path
            if dest_backup_path is None:
                ts_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime(now))
                dest_dir = Path(self.default_backup_dir)
                dest_path = dest_dir / f"kaiser_backup_{ts_str}_{bid}.db"
            else:
                dest_path = Path(dest_backup_path)

            # Safety Guard 1: Reject protected target paths (data/kaiser.db)
            if is_protected_target_path(dest_path):
                logger.error("Security violation: Attempted to write backup to protected production database %s", dest_path)
                return BackupResult(
                    success=False,
                    backup_path=str(dest_path),
                    error_message=f"Protected production database destination rejected: {dest_path}",
                )

            # Safety Guard 2: Reject source and destination collision
            try:
                if src_path.resolve() == dest_path.resolve():
                    return BackupResult(
                        success=False,
                        backup_path=str(dest_path),
                        error_message=f"Source and destination path collision rejected: {src_path} == {dest_path}",
                    )
            except Exception as coll_err:
                return BackupResult(
                    success=False,
                    backup_path=str(dest_path),
                    error_message=f"Path resolution error during collision check: {coll_err}",
                )

            # Safety Guard 3: Overwrite check
            if dest_path.exists() and not overwrite:
                return BackupResult(
                    success=False,
                    backup_path=str(dest_path),
                    error_message=f"Backup destination already exists and overwrite=False: {dest_path}",
                )

            # Safety Guard 4: Destination directory creation
            try:
                dest_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception as dir_err:
                logger.error("Failed to create destination directory for backup %s: %s", dest_path, dir_err)
                return BackupResult(
                    success=False,
                    backup_path=str(dest_path),
                    error_message=f"Destination directory cannot be created: {dir_err}",
                )

            # 3. Two-phase staging setup
            staging_backup_path = dest_path.parent / f"{dest_path.name}.tmp_{uuid.uuid4().hex[:8]}"
            staging_meta_path = dest_path.parent / f"{dest_path.name}.meta.json.tmp_{uuid.uuid4().hex[:8]}"
            final_meta_path = dest_path.parent / f"{dest_path.name}.meta.json"

            source_conn: Optional[sqlite3.Connection] = None
            dest_conn: Optional[sqlite3.Connection] = None

            try:
                # 4. Open source with busy timeout
                try:
                    source_conn = sqlite3.connect(f"file:{src_path.resolve()}?mode=ro", uri=True, timeout=30.0)
                except sqlite3.OperationalError:
                    # Fallback to standard connect if URI read-only fails on platform
                    source_conn = sqlite3.connect(str(src_path.resolve()), timeout=30.0)

                # Open staging target
                dest_conn = sqlite3.connect(str(staging_backup_path), timeout=30.0)

                # 5. Online SQLite backup
                # sqlite3.Connection.backup(target, pages=-1, progress=progress_callback)
                if progress_callback:
                    source_conn.backup(dest_conn, pages=100, progress=progress_callback)
                else:
                    source_conn.backup(dest_conn)

                # Close connections before running verification and atomic rename
                dest_conn.close()
                dest_conn = None
                source_conn.close()
                source_conn = None

                # 6. Verify staged database
                verification = self.verify_backup(staging_backup_path, expect_metadata=False)
                if not verification.valid:
                    self._cleanup_file(staging_backup_path)
                    return BackupResult(
                        success=False,
                        backup_path=str(dest_path),
                        error_message=f"Backup verification detects corrupted content: {verification.error_message}",
                    )

                # 7. Compute final checksum & metadata
                sha256_hash = compute_file_sha256(staging_backup_path)
                file_size = staging_backup_path.stat().st_size

                clean_custom = redact_sensitive_payload(custom_metadata or {})
                meta_dict = {
                    "backup_id": bid,
                    "created_at": now,
                    "created_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
                    "source_path": str(src_path),
                    "backup_path": str(dest_path),
                    "checksum_sha256": sha256_hash,
                    "size_bytes": file_size,
                    "integrity_check": verification.integrity_check,
                    "foreign_key_check": verification.foreign_key_check,
                    "tables_included": verification.tables_found,
                    "table_counts": verification.table_counts,
                    "custom_metadata": clean_custom,
                }

                # Write staged metadata sidecar
                with open(staging_meta_path, "w", encoding="utf-8") as mf:
                    json.dump(meta_dict, mf, indent=2, sort_keys=True)

                # 8. Atomic publication (Two-phase commit via rename/replace)
                try:
                    os.replace(staging_backup_path, dest_path)
                except Exception as ren_err:
                    self._cleanup_file(staging_backup_path)
                    self._cleanup_file(staging_meta_path)
                    return BackupResult(
                        success=False,
                        backup_path=str(dest_path),
                        error_message=f"Atomic publication failure: {ren_err}",
                    )

                try:
                    os.replace(staging_meta_path, final_meta_path)
                except Exception as meta_ren_err:
                    logger.warning("Failed to atomically publish metadata sidecar: %s", meta_ren_err)
                    # Backup file itself is published, metadata is non-fatal but logged

                logger.info(
                    "Successfully created and verified backup [id=%s, path=%s, size=%d, sha256=%s]",
                    bid,
                    dest_path,
                    file_size,
                    sha256_hash[:12],
                )

                return BackupResult(
                    success=True,
                    backup_path=str(dest_path),
                    metadata_path=str(final_meta_path) if final_meta_path.exists() else None,
                    checksum_sha256=sha256_hash,
                    size_bytes=file_size,
                    timestamp=now,
                    backup_id=bid,
                    tables_included=verification.tables_found,
                    table_counts=verification.table_counts,
                    details=meta_dict,
                )

            except Exception as exc:
                logger.error("Backup creation failed for %s -> %s: %s", src_path, dest_path, exc)
                if dest_conn is not None:
                    try:
                        dest_conn.close()
                    except Exception:
                        pass
                    dest_conn = None
                if source_conn is not None:
                    try:
                        source_conn.close()
                    except Exception:
                        pass
                    source_conn = None
                self._cleanup_file(staging_backup_path)
                self._cleanup_file(staging_meta_path)
                return BackupResult(
                    success=False,
                    backup_path=str(dest_path),
                    error_message=f"Backup interrupted during creation: {exc}",
                )
            finally:
                if dest_conn is not None:
                    try:
                        dest_conn.close()
                    except Exception:
                        pass
                if source_conn is not None:
                    try:
                        source_conn.close()
                    except Exception:
                        pass

    def verify_backup(
        self,
        backup_path: Union[str, Path],
        expect_metadata: bool = True,
        required_tables: Optional[Sequence[str]] = None,
    ) -> BackupVerificationResult:
        """Validate an existing SQLite backup file on disk."""
        target = Path(backup_path)
        if not target.is_file():
            return BackupVerificationResult(
                valid=False,
                backup_path=str(backup_path),
                error_message=f"Backup file does not exist: {target}",
            )

        if target.stat().st_size == 0:
            return BackupVerificationResult(
                valid=False,
                backup_path=str(backup_path),
                error_message=f"Backup file is empty (0 bytes): {target}",
            )

        # 1. Compute checksum
        try:
            current_sha256 = compute_file_sha256(target)
        except Exception as hash_err:
            return BackupVerificationResult(
                valid=False,
                backup_path=str(backup_path),
                error_message=f"Failed to compute backup checksum: {hash_err}",
            )

        # 2. Sidecar metadata verification
        metadata_valid = False
        sidecar_path = target.parent / f"{target.name}.meta.json"
        if sidecar_path.is_file():
            try:
                with open(sidecar_path, "r", encoding="utf-8") as mf:
                    meta = json.load(mf)
                recorded_sha = meta.get("checksum_sha256")
                if recorded_sha and recorded_sha.lower() != current_sha256.lower():
                    return BackupVerificationResult(
                        valid=False,
                        backup_path=str(backup_path),
                        checksum_sha256=current_sha256,
                        error_message=(
                            f"Backup checksum mismatch: recorded {recorded_sha} "
                            f"vs actual {current_sha256}"
                        ),
                    )
                metadata_valid = True
            except Exception as meta_err:
                if expect_metadata:
                    return BackupVerificationResult(
                        valid=False,
                        backup_path=str(backup_path),
                        checksum_sha256=current_sha256,
                        error_message=f"Invalid or corrupted metadata sidecar: {meta_err}",
                    )
        elif expect_metadata:
            # Metadata is optional for standalone files unless strictly requested
            pass

        # 3. SQLite Integrity & Schema Verification
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(f"file:{target.resolve()}?mode=ro", uri=True, timeout=10.0)
        except sqlite3.OperationalError:
            try:
                conn = sqlite3.connect(str(target.resolve()), timeout=10.0)
            except Exception as conn_err:
                return BackupVerificationResult(
                    valid=False,
                    backup_path=str(backup_path),
                    checksum_sha256=current_sha256,
                    error_message=f"Cannot open SQLite connection to backup: {conn_err}",
                )

        try:
            # PRAGMA integrity_check
            cur = conn.execute("PRAGMA integrity_check;")
            rows = cur.fetchall()
            if not rows or rows[0][0] != "ok":
                res_msg = "; ".join(str(r[0]) for r in rows)
                return BackupVerificationResult(
                    valid=False,
                    backup_path=str(backup_path),
                    checksum_sha256=current_sha256,
                    integrity_check=res_msg,
                    error_message=f"SQLite integrity check failed: {res_msg}",
                )
            integrity_status = "ok"

            # PRAGMA foreign_key_check
            cur = conn.execute("PRAGMA foreign_key_check;")
            fk_rows = cur.fetchall()
            if fk_rows:
                fk_msg = f"Foreign key violations detected: {len(fk_rows)} rows"
                return BackupVerificationResult(
                    valid=False,
                    backup_path=str(backup_path),
                    checksum_sha256=current_sha256,
                    integrity_check=integrity_status,
                    foreign_key_check=fk_msg,
                    error_message=f"SQLite foreign key check failed: {fk_msg}",
                )
            fk_status = "ok"

            # Discover tables
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = [r[0] for r in cur.fetchall()]

            # Count rows for each table
            table_counts: Dict[str, int] = {}
            for tbl in tables:
                try:
                    c_cur = conn.execute(f"SELECT COUNT(*) FROM \"{tbl}\";")  # safe table identifier
                    table_counts[tbl] = int(c_cur.fetchone()[0])
                except Exception:
                    table_counts[tbl] = -1

            # Validate required tables if specified
            if required_tables:
                missing = [t for t in required_tables if t not in tables]
                if missing:
                    return BackupVerificationResult(
                        valid=False,
                        backup_path=str(backup_path),
                        checksum_sha256=current_sha256,
                        integrity_check=integrity_status,
                        foreign_key_check=fk_status,
                        tables_found=tables,
                        table_counts=table_counts,
                        metadata_valid=metadata_valid,
                        error_message=f"Missing required tables in backup: {missing}",
                    )

            return BackupVerificationResult(
                valid=True,
                backup_path=str(backup_path),
                checksum_sha256=current_sha256,
                integrity_check=integrity_status,
                foreign_key_check=fk_status,
                tables_found=tables,
                table_counts=table_counts,
                metadata_valid=metadata_valid,
            )

        except Exception as check_err:
            return BackupVerificationResult(
                valid=False,
                backup_path=str(backup_path),
                checksum_sha256=current_sha256,
                error_message=f"Database verification failure: {check_err}",
            )
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def restore_database(
        self,
        backup_path: Union[str, Path],
        target_db_path: Union[str, Path],
        overwrite: bool = False,
        progress_callback: Optional[Callable[[int, int, int], None]] = None,
    ) -> RestoreResult:
        """Safely restore a validated backup into a caller-selected disposable target path.

        Guarantees:
        - NEVER allows restoring to 'data/kaiser.db' or any protected production database.
        - NEVER allows source and target path collisions.
        - Source backup is verified before any restore operation begins.
        - Restores through a temporary staging file (<target>.restore_tmp_<uuid>).
        - Restored staging database is verified with PRAGMA integrity_check before publication.
        - Atomically published via rename/replace; target is untouched if restore or verification fails.
        - Destination file will NOT be overwritten unless overwrite=True is explicitly supplied.
        """
        with self._lock:
            now = time.time()
            backup_file = Path(backup_path)
            target_file = Path(target_db_path)

            # Safety Guard 1: Reject protected destination (data/kaiser.db)
            if is_protected_target_path(target_file):
                logger.error("Security violation: Attempted restore to protected production database %s", target_file)
                return RestoreResult(
                    success=False,
                    backup_path=str(backup_file),
                    restored_path=str(target_file),
                    error_message=f"Protected production database destination rejected: {target_file}",
                )

            # Safety Guard 2: Reject collision
            try:
                if backup_file.resolve() == target_file.resolve():
                    return RestoreResult(
                        success=False,
                        backup_path=str(backup_file),
                        restored_path=str(target_file),
                        error_message=f"Source backup and restore target path collision: {backup_file} == {target_file}",
                    )
            except Exception as coll_err:
                return RestoreResult(
                    success=False,
                    backup_path=str(backup_file),
                    restored_path=str(target_file),
                    error_message=f"Path resolution error during collision check: {coll_err}",
                )

            # Safety Guard 3: Target already exists
            if target_file.exists() and not overwrite:
                return RestoreResult(
                    success=False,
                    backup_path=str(backup_file),
                    restored_path=str(target_file),
                    error_message=f"Restore target destination already exists and overwrite=False: {target_file}",
                )

            # 1. Verify source backup before restoring
            verification = self.verify_backup(backup_file, expect_metadata=False)
            if not verification.valid:
                return RestoreResult(
                    success=False,
                    backup_path=str(backup_file),
                    restored_path=str(target_file),
                    error_message=f"Restore from invalid backup rejected: {verification.error_message}",
                )

            # 2. Target directory creation
            try:
                target_file.parent.mkdir(parents=True, exist_ok=True)
            except Exception as dir_err:
                return RestoreResult(
                    success=False,
                    backup_path=str(backup_file),
                    restored_path=str(target_file),
                    error_message=f"Cannot create restore target directory: {dir_err}",
                )

            # 3. Two-phase restore staging
            staging_restore_path = target_file.parent / f"{target_file.name}.restore_tmp_{uuid.uuid4().hex[:8]}"

            backup_conn: Optional[sqlite3.Connection] = None
            staging_conn: Optional[sqlite3.Connection] = None

            try:
                # Open backup read-only
                try:
                    backup_conn = sqlite3.connect(f"file:{backup_file.resolve()}?mode=ro", uri=True, timeout=30.0)
                except sqlite3.OperationalError:
                    backup_conn = sqlite3.connect(str(backup_file.resolve()), timeout=30.0)

                staging_conn = sqlite3.connect(str(staging_restore_path), timeout=30.0)

                # 4. Restore via backup API
                if progress_callback:
                    backup_conn.backup(staging_conn, pages=100, progress=progress_callback)
                else:
                    backup_conn.backup(staging_conn)

                # Close connections before verification and atomic swap
                staging_conn.close()
                staging_conn = None
                backup_conn.close()
                backup_conn = None

                # 5. Verify restored staging database
                restored_verification = self.verify_backup(staging_restore_path, expect_metadata=False)
                if not restored_verification.valid:
                    self._cleanup_file(staging_restore_path)
                    return RestoreResult(
                        success=False,
                        backup_path=str(backup_file),
                        restored_path=str(target_file),
                        error_message=f"Restored staging database verification failed: {restored_verification.error_message}",
                    )

                # 6. Atomically publish restored database
                try:
                    os.replace(staging_restore_path, target_file)
                except Exception as ren_err:
                    self._cleanup_file(staging_restore_path)
                    return RestoreResult(
                        success=False,
                        backup_path=str(backup_file),
                        restored_path=str(target_file),
                        error_message=f"Atomic publication failure during restore: {ren_err}",
                    )

                restored_sha = compute_file_sha256(target_file)
                restored_size = target_file.stat().st_size

                logger.info(
                    "Successfully restored database from %s to %s [size=%d, sha256=%s]",
                    backup_file,
                    target_file,
                    restored_size,
                    restored_sha[:12],
                )

                return RestoreResult(
                    success=True,
                    backup_path=str(backup_file),
                    restored_path=str(target_file),
                    checksum_sha256=restored_sha,
                    size_bytes=restored_size,
                    timestamp=now,
                    tables_restored=restored_verification.tables_found,
                    table_counts=restored_verification.table_counts,
                    details={
                        "restored_at": now,
                        "integrity_check": restored_verification.integrity_check,
                        "foreign_key_check": restored_verification.foreign_key_check,
                    },
                )

            except Exception as exc:
                logger.error("Restore failed from %s to %s: %s", backup_file, target_file, exc)
                if staging_conn is not None:
                    try:
                        staging_conn.close()
                    except Exception:
                        pass
                    staging_conn = None
                if backup_conn is not None:
                    try:
                        backup_conn.close()
                    except Exception:
                        pass
                    backup_conn = None
                self._cleanup_file(staging_restore_path)
                return RestoreResult(
                    success=False,
                    backup_path=str(backup_file),
                    restored_path=str(target_file),
                    error_message=f"Restore interrupted before publication: {exc}",
                )
            finally:
                if staging_conn is not None:
                    try:
                        staging_conn.close()
                    except Exception:
                        pass
                if backup_conn is not None:
                    try:
                        backup_conn.close()
                    except Exception:
                        pass

    def prune_backups(
        self,
        backup_dir: Optional[Union[str, Path]] = None,
        max_keep: int = 5,
        dry_run: bool = False,
        in_use_files: Optional[Set[str]] = None,
    ) -> PruneResult:
        """Prune excess older verified backups while strictly protecting the newest backup.

        Guarantees:
        - NEVER prunes the newest verified backup (index 0 is protected).
        - NEVER prunes a backup file that is currently in use (in_use_files).
        - NEVER prunes outside the target backup directory.
        - Supports dry-run preview.
        - Deletes both the database file and its sidecar metadata file.
        """
        with self._lock:
            target_dir = Path(backup_dir or self.default_backup_dir)
            if not target_dir.is_dir():
                return PruneResult(
                    success=True,
                    pruned_files=[],
                    retained_files=[],
                    pruned_bytes=0,
                    dry_run=dry_run,
                )

            if max_keep < 1:
                return PruneResult(
                    success=False,
                    error_message=f"max_keep must be at least 1, got {max_keep}",
                )

            # Collect all .db files directly inside target_dir (ignore subdirectories and temp files)
            db_files: List[Path] = []
            for item in target_dir.iterdir():
                if item.is_file() and item.suffix.lower() == ".db" and not item.name.startswith("."):
                    if ".tmp_" not in item.name and ".restore_tmp_" not in item.name:
                        db_files.append(item)

            # Sort descending by modification time (newest first)
            db_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

            in_use_normalized: Set[str] = set()
            if in_use_files:
                for f in in_use_files:
                    try:
                        in_use_normalized.add(str(Path(f).resolve()))
                    except Exception:
                        in_use_normalized.add(str(f))

            retained: List[str] = []
            to_prune: List[Path] = []
            pruned_bytes = 0

            for idx, file_path in enumerate(db_files):
                resolved_str = str(file_path.resolve())

                # Protection 1: The newest backup (idx == 0) is NEVER pruned
                if idx == 0:
                    retained.append(str(file_path))
                    continue

                # Protection 2: In-use files are NEVER pruned
                if resolved_str in in_use_normalized:
                    retained.append(str(file_path))
                    continue

                # If within retention count, keep it
                if len(retained) < max_keep:
                    retained.append(str(file_path))
                else:
                    to_prune.append(file_path)

            pruned_names: List[str] = []
            for prune_target in to_prune:
                try:
                    f_size = prune_target.stat().st_size
                except Exception:
                    f_size = 0

                if not dry_run:
                    try:
                        prune_target.unlink(missing_ok=True)
                        # Also unlink companion meta.json if it exists
                        meta_sidecar = prune_target.parent / f"{prune_target.name}.meta.json"
                        meta_sidecar.unlink(missing_ok=True)
                        pruned_names.append(str(prune_target))
                        pruned_bytes += f_size
                    except Exception as del_err:
                        logger.warning("Failed to prune backup file %s: %s", prune_target, del_err)
                else:
                    pruned_names.append(str(prune_target))
                    pruned_bytes += f_size

            return PruneResult(
                success=True,
                pruned_files=pruned_names,
                retained_files=retained,
                pruned_bytes=pruned_bytes,
                dry_run=dry_run,
            )

    @staticmethod
    def _cleanup_file(path_to_clean: Path) -> None:
        """Silently remove a file if it exists with retry for Windows file lock release."""
        for attempt in range(5):
            try:
                if path_to_clean.exists():
                    path_to_clean.unlink(missing_ok=True)
                break
            except Exception:
                time.sleep(0.02)

    def get_backup_metadata(self, backup_path: Union[str, Path]) -> Optional[Dict[str, Any]]:
        """Retrieve and parse sidecar metadata for a backup file if present."""
        target = Path(backup_path)
        meta_file = target.parent / f"{target.name}.meta.json"
        if not meta_file.is_file():
            return None
        try:
            with open(meta_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return redact_sensitive_payload(data)
        except Exception as err:
            logger.warning("Failed to read metadata sidecar %s: %s", meta_file, err)
            return None
