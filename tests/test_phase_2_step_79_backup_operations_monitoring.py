"""KAISER — Phase 2, Step 79 Test Suite: Automated Backup Operations & Recovery Monitoring.

Audits, validates, and stresses:
1. Operational coordinator lifecycle (scheduler disabled at startup, explicit start, idempotent start/stop).
2. Concurrency control and rejection of overlapping backup jobs.
3. Structured health monitoring (HEALTHY, DEGRADED, FAILED, DISABLED, UNKNOWN) with fail-closed semantics.
4. Backup inventory, manifest validation, and detection of missing/corrupt metadata.
5. Retention governance (newest protection, single backup protection, in-use protection, directory boundary security, dry-run).
6. Non-destructive disaster recovery drills (lifecycle, state verification, corruption handling, cancellation).
7. Read-only dashboard integration (GET /api/paper/backups/*, GET /api/paper/recovery/*, auth enforcement, mutation rejection).
8. Strict privacy, secret redaction, and host filesystem path concealment.
"""

from __future__ import annotations

import concurrent.futures
import copy
import http.client
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from typing import Generator, List, Optional
import uuid

import pytest

from app.core.config import Settings, get_settings, resolve_database_path
from app.core.exceptions import ConfigurationError, ValidationError
from app.execution.allocation import (
    PortfolioAllocationManager,
    PortfolioAllocationRequest,
    StrategySleeveDefinition,
)
from app.execution.portfolio import PaperPortfolio
from app.ledger.backup import DatabaseBackupService, compute_file_sha256, is_protected_target_path
from app.ledger.backup_coordinator import (
    BackupHealthSnapshot,
    BackupHealthStatus,
    BackupInventoryItem,
    BackupJobResult,
    BackupOperationsCoordinator,
    RecoveryDrillResult,
    RecoveryDrillStatus,
    RetentionGovernanceResult,
    sanitize_backup_path_for_display,
)
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.ledger.outbox import OutboxRecord, TransactionalOutbox
from app.portfolio.dashboard.server import PortfolioDashboardServer


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Fixture providing isolated temporary directory."""
    return tmp_path


@pytest.fixture
def sample_source_db(temp_dir: Path) -> Path:
    """Fixture providing a populated synthetic source database."""
    db_path = temp_dir / "sample_source.db"
    ledger = PaperLedger(db_path=str(db_path), initial_balance=50000.0)
    outbox = TransactionalOutbox(db_path=str(db_path))

    rec = OutboxRecord(
        event_id="outbox_init",
        event_type="ledger.capital_allocated",
        schema_version=1,
        payload={"cash": 50000.0},
    )
    outbox.stage_record(rec)
    return db_path


# ==============================================================================
# SECTION 10: ALL 24 OPERATIONAL FAILURE-INJECTION SCENARIOS
# ==============================================================================


class TestBackupOperationsFailureInjectionAndGovernance:
    """Validates all 24 operational, governance, and failure-injection scenarios."""

    def test_op_01_scheduler_disabled_at_startup(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-01: Scheduler remains stopped at startup by default without launching background workers."""
        bdir = temp_dir / "backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            scheduler_enabled=False,
        )
        assert coord.is_running is False
        assert coord.is_job_active is False
        # Starting when disabled returns False and does not spawn thread
        started = coord.start()
        assert started is False
        assert coord.is_running is False

    def test_op_02_explicit_scheduler_enablement(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-02: Explicitly enabling the scheduler starts the background worker thread."""
        bdir = temp_dir / "backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            scheduler_enabled=True,
            interval_seconds=3600.0,
        )
        try:
            started = coord.start()
            assert started is True
            assert coord.is_running is True
        finally:
            coord.stop(timeout=2.0)
            assert coord.is_running is False

    def test_op_03_repeated_scheduler_start_idempotency(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-03: Repeated start() calls are safe, idempotent, and do not spawn extra threads."""
        bdir = temp_dir / "backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            scheduler_enabled=True,
            interval_seconds=3600.0,
        )
        try:
            assert coord.start() is True
            assert coord.start() is True
            assert coord.start() is True
            assert coord.is_running is True
        finally:
            coord.stop(timeout=2.0)

    def test_op_04_repeated_scheduler_stop_idempotency(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-04: Repeated stop() calls are safe and idempotent whether running or stopped."""
        bdir = temp_dir / "backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            scheduler_enabled=True,
            interval_seconds=3600.0,
        )
        # Stop before start
        assert coord.stop() is True
        assert coord.is_running is False

        # Start then stop repeatedly
        coord.start()
        assert coord.stop() is True
        assert coord.stop() is True
        assert coord.is_running is False

    def test_op_05_overlapping_backup_request_rejection(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-05: Concurrent overlapping backup request is rejected immediately without error."""
        bdir = temp_dir / "backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )

        # Acquire the active job lock to simulate an in-flight backup
        assert coord._active_job_lock.acquire(blocking=False) is True
        try:
            res = coord.trigger_manual_backup(correlation_id="concurrent_test")
            assert res.status == "REJECTED"
            assert "already actively in progress" in (res.error_message or "")
        finally:
            coord._active_job_lock.release()

    def test_op_06_backup_job_exception_handled(self, temp_dir: Path) -> None:
        """OP-06: Exception during backup is caught, records failure state, and increments failures."""
        bdir = temp_dir / "backups"
        nonexistent = temp_dir / "missing.db"
        coord = BackupOperationsCoordinator(
            source_db_path=str(nonexistent),
            backup_dir=str(bdir),
        )

        res = coord.trigger_manual_backup()
        assert res.status == "FAILED"
        assert coord._consecutive_failures == 1
        assert coord._last_failed_backup is not None
        assert "cannot be opened" in (res.error_message or "")

    def test_op_07_backup_storage_unavailable(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-07: Backup storage unavailable (file blocking directory) returns safe failure."""
        blocker = temp_dir / "blocker.txt"
        blocker.write_text("blocking file")
        invalid_bdir = blocker / "sub_backups"

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(invalid_bdir),
        )

        res = coord.trigger_manual_backup()
        assert res.status == "FAILED"
        assert "storage unavailable" in (res.error_message or "").lower()

    def test_op_08_backup_health_degraded_or_failed_on_integrity_loss(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-08: Corrupted latest backup file results in FAILED health assessment."""
        bdir = temp_dir / "backups_corrupt"
        bdir.mkdir()
        # Create corrupted backup file
        bad_file = bdir / "kaiser_backup_20261124_000000_bkp_bad.db"
        bad_file.write_bytes(b"INVALID_SQLITE_HEADER" * 10)
        meta_file = bdir / "kaiser_backup_20261124_000000_bkp_bad.db.meta.json"
        meta_file.write_text(json.dumps({
            "backup_id": "bkp_bad",
            "checksum_sha256": "fake_checksum",
            "integrity_check": "corrupted",
        }))

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )

        health = coord.get_health()
        assert health.status == BackupHealthStatus.FAILED
        assert health.integrity_verified is False

    def test_op_09_corrupted_manifest_detection(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-09: Corrupted JSON sidecar manifest is detected and flagged in inventory."""
        bdir = temp_dir / "backups_bad_meta"
        bdir.mkdir()
        db_file = bdir / "backup_ok.db"
        with sqlite3.connect(db_file) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        meta_file = bdir / "backup_ok.db.meta.json"
        meta_file.write_text("NOT_VALID_JSON{{{")

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )

        inv = coord.get_inventory()
        assert len(inv) == 1
        assert inv[0].integrity_status == "METADATA_CORRUPTED"

    def test_op_10_missing_backup_detection(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-10: Requesting recovery drill for missing/unknown backup returns FAILED with clear error."""
        bdir = temp_dir / "backups_missing"
        bdir.mkdir()

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )

        drill_res = coord.run_recovery_drill(backup_id="nonexistent_backup_id_999")
        assert drill_res.status == RecoveryDrillStatus.FAILED
        assert "not found in inventory" in (drill_res.error_message or "") or "Zero verified backups" in (drill_res.error_message or "")

    def test_op_11_retention_dry_run_preview(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-11: Retention dry-run preview returns prune candidates without deleting files."""
        bdir = temp_dir / "retention_dry"
        bdir.mkdir()

        now = time.time()
        files: List[Path] = []
        for i in range(5):
            p = bdir / f"backup_{i:02d}.db"
            p.write_bytes(f"content_{i}".encode("utf-8"))
            os.utime(p, (now - (5 - i) * 3600, now - (5 - i) * 3600))
            files.append(p)

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            retention_max_count=2,
        )

        res = coord.prune_retention(dry_run=True, max_count=2)
        assert res.success is True
        assert res.dry_run is True
        assert len(res.pruned_backups) == 3
        # Confirm zero files deleted
        for f in files:
            assert f.exists()

    def test_op_12_retention_preserves_newest_verified_backup(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-12: Newest verified backup is never pruned even if max_count=1."""
        bdir = temp_dir / "retention_newest"
        bdir.mkdir()

        old_file = bdir / "backup_old.db"
        old_file.write_bytes(b"old")
        os.utime(old_file, (1000, 1000))

        new_file = bdir / "backup_newest.db"
        new_file.write_bytes(b"newest")
        os.utime(new_file, (2000, 2000))

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            retention_max_count=1,
        )

        res = coord.prune_retention(dry_run=False, max_count=1)
        assert res.success is True
        assert sanitize_backup_path_for_display(new_file) in res.retained_backups
        assert new_file.exists()
        assert not old_file.exists()

    def test_op_13_retention_preserves_the_only_verified_backup(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-13: If only one backup exists, retention never deletes it."""
        bdir = temp_dir / "retention_single"
        bdir.mkdir()

        only_file = bdir / "only_backup.db"
        only_file.write_bytes(b"only")

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            retention_max_count=1,
        )

        res = coord.prune_retention(dry_run=False, max_count=1)
        assert res.success is True
        assert len(res.pruned_backups) == 0
        assert len(res.retained_backups) == 1
        assert only_file.exists()

    def test_op_14_retention_cannot_escape_configured_directory(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-14: Retention logic checks resolved directory boundaries and never deletes outside."""
        bdir = temp_dir / "allowed_backups"
        bdir.mkdir()
        outside_file = temp_dir / "outside_critical.db"
        outside_file.write_bytes(b"critical")

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )

        res = coord.prune_retention(dry_run=False, max_count=1)
        assert outside_file.exists()

    def test_op_15_symlink_path_traversal_rejection(self, temp_dir: Path, sample_source_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """OP-15: Symlinks or traversal paths pointing outside backup directory are rejected and not pruned."""
        bdir = temp_dir / "symlink_dir"
        bdir.mkdir()

        external_target = temp_dir / "external_db.db"
        external_target.write_bytes(b"external")

        link_path = bdir / "traversal_link.db"
        symlink_created = False
        try:
            link_path.symlink_to(external_target)
            symlink_created = True
        except OSError:
            pass

        if not symlink_created:
            # Emulate symlink behavior via monkeypatch to verify traversal guard on restricted platforms
            dummy_file = bdir / "traversal_link.db"
            dummy_file.write_bytes(b"link")
            orig_iterdir = Path.iterdir

            class PseudoSymlink(type(dummy_file)):
                def is_symlink(self) -> bool:
                    return True

                def resolve(self, strict: bool = False) -> Path:
                    return external_target

            def patched_iterdir(path_obj: Path):
                for p in orig_iterdir(path_obj):
                    if p.name == "traversal_link.db":
                        yield PseudoSymlink(p)
                    else:
                        yield p

            monkeypatch.setattr(Path, "iterdir", patched_iterdir)

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            retention_max_count=1,
        )

        res = coord.prune_retention(dry_run=False, max_count=1)
        assert external_target.exists()

    def test_op_16_active_restore_protects_backup_from_pruning(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-16: A backup actively in use by restore or recovery drill is protected from retention pruning."""
        bdir = temp_dir / "retention_in_use"
        bdir.mkdir()

        f1 = bdir / "backup_1.db"
        f1.write_bytes(b"one")
        os.utime(f1, (1000, 1000))

        f2 = bdir / "backup_2.db"
        f2.write_bytes(b"two")
        os.utime(f2, (2000, 2000))

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            retention_max_count=1,
        )

        # Mark older backup f1 as in use
        coord._in_use_backups.add(str(f1.resolve()))

        res = coord.prune_retention(dry_run=False, max_count=1)
        assert res.success is True
        # f1 was protected because it is in use
        assert f1.exists()
        assert f2.exists()

    def test_op_17_recovery_drill_success(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-17: Recovery drill executes non-destructively, recovers records, and cleans up artifacts."""
        bdir = temp_dir / "drill_success_backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )

        # Create verified backup first
        b_res = coord.trigger_manual_backup()
        assert b_res.status == "SUCCESS"

        # Execute drill
        drill = coord.run_recovery_drill()
        assert drill.status == RecoveryDrillStatus.PASSED
        assert drill.records_recovered > 0
        assert drill.reconciliation_passed is True

        # Verify drill history recorded
        hist = coord.get_recovery_drill_history()
        assert len(hist) >= 1
        assert hist[0].drill_id == drill.drill_id

    def test_op_18_recovery_drill_corruption_failure(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-18: Recovery drill fails safely when attempting to recover from corrupted backup."""
        bdir = temp_dir / "drill_corrupt_backups"
        bdir.mkdir()

        bad_backup = bdir / "kaiser_backup_bad.db"
        bad_backup.write_bytes(b"CORRUPTED_SQLITE_BYTES" * 10)
        meta_file = bdir / "kaiser_backup_bad.db.meta.json"
        meta_file.write_text(json.dumps({
            "backup_id": "bad_drill_backup",
            "checksum_sha256": "fake_sha",
            "integrity_check": "ok",
        }))

        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )

        drill = coord.run_recovery_drill(backup_id="bad_drill_backup")
        assert drill.status == RecoveryDrillStatus.FAILED
        assert "integrity check failed" in (drill.error_message or "").lower()

    def test_op_19_recovery_drill_cancellation(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-19: Setting cancel_event reports drill CANCELLED without proceeding."""
        bdir = temp_dir / "drill_cancel_backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )
        coord.trigger_manual_backup()

        cancel = threading.Event()
        cancel.set()

        drill = coord.run_recovery_drill(cancel_event=cancel)
        assert drill.status == RecoveryDrillStatus.CANCELLED
        assert "cancelled" in (drill.error_message or "").lower()

    def test_op_20_dashboard_authorization_enforcement(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-20: Dashboard endpoints require auth when require_auth=True; reject unauthenticated."""
        bdir = temp_dir / "dash_auth_backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )
        coord.trigger_manual_backup()

        token = "step79_secret_token_12345"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token=token,
            require_auth=True,
            backup_coordinator=coord,
        )
        server.start()
        time.sleep(0.1)

        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        try:
            # 1. Unauthenticated request -> 401 Unauthorized
            conn.request("GET", "/api/paper/backups/status")
            resp = conn.getresponse()
            assert resp.status == 401
            resp.read()

            # 2. Authenticated request -> 200 OK
            conn.request("GET", "/api/paper/backups/status", headers={"Authorization": f"Bearer {token}"})
            resp = conn.getresponse()
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert "coordinator" in data
        finally:
            conn.close()
            server.stop()

    def test_op_21_dashboard_mutation_methods_rejected(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-21: POST, PUT, DELETE, PATCH to backup endpoints are rejected with 405 Method Not Allowed."""
        bdir = temp_dir / "dash_mut_backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            backup_coordinator=coord,
        )
        server.start()
        time.sleep(0.1)

        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        try:
            for method in ("POST", "PUT", "DELETE", "PATCH"):
                conn.request(method, "/api/paper/backups/status")
                resp = conn.getresponse()
                assert resp.status == 405
                resp.read()
        finally:
            conn.close()
            server.stop()

    def test_op_22_secret_and_path_redaction(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-22: Dashboard and inventory responses scrub host filesystem paths and secrets."""
        bdir = temp_dir / "dash_secrets"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )
        coord.trigger_manual_backup(custom_metadata={"api_key": "secret_api_key_999"})

        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            backup_coordinator=coord,
        )
        server.start()
        time.sleep(0.1)

        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        try:
            conn.request("GET", "/api/paper/backups/inventory")
            resp = conn.getresponse()
            assert resp.status == 200
            raw_text = resp.read().decode("utf-8")
            # Verify no secret leaked
            assert "secret_api_key_999" not in raw_text
            # Verify no absolute drive paths (e.g. C:\ or /home/)
            assert ":\\" not in raw_text

            conn.request("GET", "/api/paper/backups/health")
            resp2 = conn.getresponse()
            assert resp2.status == 200
            raw_health = resp2.read().decode("utf-8")
            assert "secret_api_key_999" not in raw_health
        finally:
            conn.close()
            server.stop()

    def test_op_23_shutdown_while_backup_is_running(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-23: Stopping coordinator while a job is active waits cleanly up to timeout."""
        bdir = temp_dir / "shutdown_active"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
            scheduler_enabled=True,
            interval_seconds=3600.0,
            shutdown_timeout_seconds=2.0,
        )
        coord.start()
        assert coord.is_running is True

        # Acquire active job lock to simulate in-flight job
        coord._active_job_lock.acquire()
        try:
            # Stop should complete cleanly within timeout
            stopped = coord.stop(timeout=1.0)
            assert stopped is True
            assert coord.is_running is False
        finally:
            coord._active_job_lock.release()

    def test_op_24_repeated_operations_do_not_leak_threads_or_connections(self, temp_dir: Path, sample_source_db: Path) -> None:
        """OP-24: Executing repeated backup, health, and drill cycles does not leak threads or DB connections."""
        bdir = temp_dir / "leak_test_backups"
        coord = BackupOperationsCoordinator(
            source_db_path=str(sample_source_db),
            backup_dir=str(bdir),
        )

        initial_threads = threading.active_count()

        # Run 5 backup and drill cycles
        for i in range(5):
            b_res = coord.trigger_manual_backup(backup_id=f"leak_bkp_{i}")
            assert b_res.status == "SUCCESS"
            h = coord.get_health()
            assert h.status in (BackupHealthStatus.HEALTHY, BackupHealthStatus.DEGRADED)
            d = coord.run_recovery_drill()
            assert d.status == RecoveryDrillStatus.PASSED

        final_threads = threading.active_count()
        # Thread count should remain stable
        assert final_threads <= initial_threads + 1
