"""KAISER — Phase 2, Step 78 Test Suite: Database Backup, Restore, Integrity Verification & Disaster Recovery.

Audits, validates, and stresses:
1. Online, WAL-consistent SQLite backup creation via standard library sqlite3.Connection.backup().
2. Two-phase staging, atomic publication, metadata sidecar creation, and secret redaction.
3. PRAGMA integrity_check and PRAGMA foreign_key_check verification.
4. Non-destructive restore workflow into caller-selected disposable target paths.
5. End-to-end Disaster Recovery Acceptance Drill with synthetic ledger, outbox, sleeves, positions, snapshots, and maintenance digests.
6. All 16 failure-injection scenarios (missing source, protected destination, interruption, corruption, collision, concurrent writes, cleanup).
7. Retention pruning, newest-backup protection, in-use protection, and dry-run preview.
8. Zero mutation to persistent user data and real database isolation invariants.
"""

from __future__ import annotations

import concurrent.futures
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from typing import Generator, List, Optional, Set
import uuid

import pytest

from app.core.config import Settings, get_settings, resolve_database_path
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.state import StateStore
from app.execution.allocation import (
    PortfolioAllocationManager,
    PortfolioAllocationRequest,
    PortfolioEquitySnapshot,
    SleeveRiskBudget,
    StrategySleeve,
    StrategySleeveDefinition,
    compute_multi_sleeve_financial_digest,
)
from app.execution.maintenance import HealthDigestSnapshot, MaintenanceScheduler
from app.execution import (
    PaperExecutionResult,
    PaperFill,
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderType,
    PaperPortfolio,
    PaperPosition,
)
from app.execution.simulated_broker import SimulatedBrokerAdapter
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
from app.ledger.outbox import (
    OutboxRecord,
    OutboxStatus,
    TransactionalOutbox,
    redact_sensitive_payload,
)


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Fixture providing an isolated temporary directory for database experiments."""
    return tmp_path


@pytest.fixture
def backup_service() -> DatabaseBackupService:
    """Fixture providing a DatabaseBackupService instance."""
    return DatabaseBackupService()


# ==============================================================================
# SECTION 7: DISASTER RECOVERY ACCEPTANCE DRILL
# ==============================================================================


class TestDisasterRecoveryAcceptanceDrill:
    """Comprehensive Disaster Recovery Drill verifying recovery of all paper accounting and system state."""

    def test_end_to_end_disaster_recovery_drill(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """Execute the full 14-step Disaster Recovery Drill on a realistic synthetic state."""
        source_db = str(temp_dir / "kaiser_primary_live.db")
        backup_dest = str(temp_dir / "kaiser_verified_backup.db")
        restore_dest = str(temp_dir / "kaiser_recovered_target.db")

        # ----------------------------------------------------------------------
        # Phase A: Populate Realistic Synthetic State
        # ----------------------------------------------------------------------
        initial_capital = 100000.0
        portfolio = PaperPortfolio(initial_cash=initial_capital)
        ledger = PaperLedger(db_path=source_db, initial_balance=initial_capital, name="live_ledger")
        outbox = TransactionalOutbox(db_path=source_db, name="live_outbox")
        event_bus = EventBus()

        alloc_mgr = PortfolioAllocationManager(
            portfolio=portfolio,
            ledger=ledger,
            event_bus=event_bus,
            outbox=outbox,
            db_path=source_db,
        )

        # Allocate two strategy sleeves
        sleeve_1_id = "alpha_momentum"
        sleeve_2_id = "mean_reversion"
        alloc_res = alloc_mgr.apply_allocation(
            PortfolioAllocationRequest(
                request_id="drill_alloc_001",
                sleeves=[
                    StrategySleeveDefinition(sleeve_id=sleeve_1_id, name="Alpha Momentum Sleeve", target_budget=40000.0),
                    StrategySleeveDefinition(sleeve_id=sleeve_2_id, name="Mean Reversion Sleeve", target_budget=30000.0),
                ],
            )
        )
        assert alloc_res.success is True

        # Submit synthetic paper order & fill
        order_req = PaperOrderRequest(
            client_order_id="drill_ord_001",
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.LIMIT,
            quantity=10.0,
            limit_price=2500.0,
        )
        # Record submission
        sub_entry = PaperLedgerEntry(
            entry_id="sub_001",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_SUBMISSION,
            order_id="ord_live_001",
            client_order_id="drill_ord_001",
            symbol="RELIANCE",
            side="BUY",
            quantity=10.0,
            price=2500.0,
            fee=0.0,
            cash_change=0.0,
            cash_balance_after=ledger.get_entries()[-1].cash_balance_after,
            realized_pnl=0.0,
            idempotency_key="sub_key_001",
            metadata={"sleeve_id": sleeve_1_id},
        )
        sub_outbox = OutboxRecord(
            event_id="evt_sub_001",
            event_type="order.submitted",
            schema_version=1,
            payload={"client_order_id": "drill_ord_001", "symbol": "RELIANCE"},
        )
        ledger.record_entry_with_outbox(sub_entry, sub_outbox)

        # Execute fill: buy 10 shares @ 2500 with fee 12.50
        fill_fee = 12.50
        trade_cost = 25000.0
        cash_deduction = -(trade_cost + fill_fee)
        new_cash = round(ledger.get_entries()[-1].cash_balance_after + cash_deduction, 4)

        fill_entry = PaperLedgerEntry(
            entry_id="fill_001",
            timestamp=time.time() + 0.01,
            entry_type=LedgerEntryType.ORDER_FILL,
            order_id="ord_live_001",
            client_order_id="drill_ord_001",
            symbol="RELIANCE",
            side="BUY",
            quantity=10.0,
            price=2500.0,
            fee=fill_fee,
            cash_change=cash_deduction,
            cash_balance_after=new_cash,
            realized_pnl=0.0,
            idempotency_key="fill_key_001",
            metadata={
                "sleeve_id": sleeve_1_id,
                "fill_id": "fill_drill_001",
                "slippage": 5.0,
            },
        )
        fill_outbox = OutboxRecord(
            event_id="evt_fill_001",
            event_type="order.filled",
            schema_version=1,
            payload={"fill_id": "fill_drill_001", "quantity": 10.0, "price": 2500.0},
        )
        ledger.record_entry_with_outbox(fill_entry, fill_outbox)

        # Apply fill to portfolio & sleeve
        portfolio.available_cash = new_cash
        portfolio.total_fees_paid += fill_fee
        portfolio.positions["RELIANCE"] = PaperPosition(
            symbol="RELIANCE",
            quantity=10.0,
            avg_entry_price=2500.0,
            current_price=2500.0,
        )
        sleeve_1 = alloc_mgr.get_sleeve(sleeve_1_id)
        assert sleeve_1 is not None
        sleeve_1.apply_fill(
            PaperFill(
                fill_id="fill_drill_001",
                order_id="ord_live_001",
                client_order_id="drill_ord_001",
                symbol="RELIANCE",
                side=PaperOrderSide.BUY,
                quantity=10.0,
                price=2500.0,
                fee=fill_fee,
                slippage=5.0,
                timestamp=time.time(),
                market_data_timestamp=time.time(),
                market_data_source="drill_feed",
            )
        )

        # Authoritative equity snapshot
        snapshot = alloc_mgr.create_equity_snapshot(snapshot_id="snap_drill_pre_backup")
        pre_backup_digest = snapshot.deterministic_digest
        pre_backup_total_equity = snapshot.total_equity

        # Maintenance scheduler & health digest records
        maint_scheduler = MaintenanceScheduler(
            outbox=outbox,
            db_path=source_db,
            enabled=False,
        )
        with sqlite3.connect(source_db) as conn:
            conn.execute(
                """
                INSERT INTO paper_health_digests (
                    digest_id, timestamp, schema_version, runtime_status, payload_json, deterministic_digest
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("maint_digest_001", time.time(), 1, "HEALTHY", json.dumps({"test": True}), "sha256_maint_123"),
            )

        # ----------------------------------------------------------------------
        # Phase B: Backup Creation & Validation
        # ----------------------------------------------------------------------
        # 1. Verify original database integrity
        orig_check = backup_service.verify_backup(source_db, expect_metadata=False)
        assert orig_check.valid is True
        assert orig_check.integrity_check == "ok"

        # 2. Create online backup
        backup_result = backup_service.create_backup(
            source_db_path=source_db,
            dest_backup_path=backup_dest,
            backup_id="drill_bkp_001",
            custom_metadata={"environment": "synthetic_drill", "drill_version": 1},
        )
        assert backup_result.success is True
        assert backup_result.backup_path == backup_dest
        assert backup_result.checksum_sha256 is not None
        assert backup_result.size_bytes > 0
        assert Path(backup_dest).exists()
        assert Path(f"{backup_dest}.meta.json").exists()

        # 3. Verify backup checksum and metadata
        backup_check = backup_service.verify_backup(backup_dest, expect_metadata=True)
        assert backup_check.valid is True
        assert backup_check.integrity_check == "ok"
        assert backup_check.checksum_sha256 == backup_result.checksum_sha256
        assert "paper_ledger_entries" in backup_check.tables_found
        assert "paper_outbox_entries" in backup_check.tables_found
        assert "paper_equity_snapshots" in backup_check.tables_found

        # ----------------------------------------------------------------------
        # Phase C: Restore to Isolated Path
        # ----------------------------------------------------------------------
        # 4. Close original database objects (simulate disaster/cutover)
        del alloc_mgr
        del ledger
        del outbox
        del maint_scheduler

        # 5. Restore backup to new disposable target
        restore_result = backup_service.restore_database(
            backup_path=backup_dest,
            target_db_path=restore_dest,
            overwrite=False,
        )
        assert restore_result.success is True
        assert restore_result.restored_path == restore_dest
        assert Path(restore_dest).exists()
        assert restore_result.checksum_sha256 == backup_result.checksum_sha256

        # ----------------------------------------------------------------------
        # Phase D: Reopen Fresh Application Objects & State Recovery
        # ----------------------------------------------------------------------
        # 6. Reopen fresh application objects against restored DB
        recovered_ledger = PaperLedger(db_path=restore_dest, name="live_ledger")
        recovered_outbox = TransactionalOutbox(db_path=restore_dest, name="live_outbox")
        recovered_portfolio = PaperPortfolio(initial_cash=initial_capital)
        recovered_portfolio.available_cash = recovered_ledger.get_entries()[-1].cash_balance_after
        recovered_portfolio.positions["RELIANCE"] = PaperPosition(
            symbol="RELIANCE",
            quantity=10.0,
            avg_entry_price=2500.0,
            current_price=2500.0,
        )
        recovered_portfolio.total_fees_paid = fill_fee

        recovered_alloc_mgr = PortfolioAllocationManager(
            portfolio=recovered_portfolio,
            ledger=recovered_ledger,
            outbox=recovered_outbox,
            db_path=restore_dest,
        )

        # 7. Recover ledger state
        # Initial capital (1) + 2 allocations (2) + 1 order sub (1) + 1 order fill (1) = 5 entries
        assert recovered_ledger.count() == 5
        recovered_entries = recovered_ledger.get_entries()
        assert recovered_entries[0].entry_type == LedgerEntryType.INITIAL_CAPITAL
        assert recovered_entries[0].cash_balance_after == initial_capital
        assert recovered_entries[-1].cash_balance_after == new_cash

        # 8. Recover sleeve allocations
        sleeve_recovery = recovered_alloc_mgr.recover_from_ledger(recovered_ledger)
        assert sleeve_recovery["recovered_sleeves"] == 2
        rec_sleeve_1 = recovered_alloc_mgr.get_sleeve(sleeve_1_id)
        rec_sleeve_2 = recovered_alloc_mgr.get_sleeve(sleeve_2_id)
        assert rec_sleeve_1 is not None
        assert rec_sleeve_2 is not None
        assert rec_sleeve_1.allocated_capital == 40000.0
        assert rec_sleeve_2.allocated_capital == 30000.0

        # 9. Recover positions and fills
        # Sleeve 1 should have 1 fill replayed
        assert len(rec_sleeve_1.positions) == 1
        pos = rec_sleeve_1.get_position("RELIANCE")
        assert pos is not None
        assert pos.quantity == 10.0
        assert pos.avg_entry_price == 2500.0

        # 10. Recover outbox records
        pending_outbox = recovered_outbox.get_pending_batch()
        # All staged outbox records exist and are recoverable
        assert len(pending_outbox) >= 2

        # 11. Reconcile portfolio accounting
        recon = recovered_alloc_mgr.reconcile()
        assert recon["is_reconciled"] is True, f"Reconciliation discrepancies: {recon.discrepancies}"
        assert recon["max_discrepancy"] <= 1e-4

        # 12. Compare deterministic financial digests
        recovered_snapshot = recovered_alloc_mgr.create_equity_snapshot(
            snapshot_id="snap_drill_post_restore",
            reference_time=snapshot.timestamp,
        )
        assert recovered_snapshot.total_equity == pre_backup_total_equity
        assert recovered_snapshot.deterministic_digest == pre_backup_digest

        # Verify maintenance records survived restore
        with sqlite3.connect(restore_dest) as conn:
            cur = conn.execute("SELECT digest_id, deterministic_digest FROM paper_health_digests WHERE digest_id = ?", ("maint_digest_001",))
            row = cur.fetchone()
            assert row is not None
            assert row[1] == "sha256_maint_123"

        # 13. Repeat recovery to verify idempotency
        idempotent_recovery = recovered_alloc_mgr.recover_from_ledger(recovered_ledger)
        assert idempotent_recovery["recovered_sleeves"] == 2
        recon_2 = recovered_alloc_mgr.reconcile()
        assert recon_2["is_reconciled"] is True

        # 14. Confirm zero duplicate entries or capital insertions
        assert recovered_ledger.count() == 5
        init_entries = [e for e in recovered_ledger.get_entries() if e.entry_type == LedgerEntryType.INITIAL_CAPITAL]
        assert len(init_entries) == 1
        fill_entries = [e for e in recovered_ledger.get_entries() if e.entry_type == LedgerEntryType.ORDER_FILL]
        assert len(fill_entries) == 1


# ==============================================================================
# SECTION 8: ALL 16 FAILURE-INJECTION SCENARIOS
# ==============================================================================


class TestBackupFailureInjectionScenarios:
    """Verifies all 16 required failure injection scenarios fail safe and clean up."""

    def test_fi_01_backup_source_cannot_be_opened(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-01: Nonexistent source file returns failure result safely."""
        missing_db = temp_dir / "nonexistent_source.db"
        dest_db = temp_dir / "backup.db"

        result = backup_service.create_backup(source_db_path=missing_db, dest_backup_path=dest_db)
        assert result.success is False
        assert "cannot be opened or does not exist" in (result.error_message or "")
        assert not dest_db.exists()

    def test_fi_02_backup_dest_dir_cannot_be_created(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-02: Destination directory creation failure returns safe failure result."""
        # Create a file and treat it as a directory to force an OSError on mkdir
        blocking_file = temp_dir / "blocking_file.txt"
        blocking_file.write_text("blocker")
        invalid_dest = blocking_file / "sub_dir" / "backup.db"

        source_db = temp_dir / "valid_source.db"
        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        result = backup_service.create_backup(source_db_path=source_db, dest_backup_path=invalid_dest)
        assert result.success is False
        assert "cannot be created" in (result.error_message or "")

    def test_fi_03_source_and_destination_collision(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-03: Source and destination path collision is rejected."""
        source_db = temp_dir / "same_file.db"
        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        result = backup_service.create_backup(source_db_path=source_db, dest_backup_path=source_db)
        assert result.success is False
        assert "collision rejected" in (result.error_message or "")

    def test_fi_04_protected_production_db_destination_rejected(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-04: Destination resolving to data/kaiser.db or protected path is rejected."""
        source_db = temp_dir / "test_source.db"
        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        # Attempt to target data/kaiser.db
        result = backup_service.create_backup(source_db_path=source_db, dest_backup_path="data/kaiser.db")
        assert result.success is False
        assert "Protected production database destination rejected" in (result.error_message or "")

    def test_fi_05_backup_interrupted_during_creation(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-05: Backup interrupted via exception in progress callback cleans up staging file."""
        source_db = temp_dir / "source_interrupted.db"
        dest_db = temp_dir / "dest_interrupted.db"

        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT, val TEXT);")
            for i in range(100):
                conn.execute("INSERT INTO t VALUES (?, ?);", (i, f"val_{i}" * 50))

        def aborting_callback(status: int, remaining: int, total: int) -> None:
            raise RuntimeError("Simulated process crash / power loss during backup")

        result = backup_service.create_backup(
            source_db_path=source_db,
            dest_backup_path=dest_db,
            progress_callback=aborting_callback,
        )

        assert result.success is False
        assert "Backup interrupted" in (result.error_message or "")
        assert not dest_db.exists()
        # Verify no orphaned staging files remain
        staging_files = list(temp_dir.glob("dest_interrupted.db.tmp_*"))
        assert len(staging_files) == 0

    def test_fi_06_backup_verification_detects_corrupted_content(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-06: Corrupted database file fails verification."""
        corrupted_db = temp_dir / "corrupted.db"
        # Write corrupted header bytes
        with open(corrupted_db, "wb") as f:
            f.write(b"NOT_A_VALID_SQLITE_DATABASE_HEADER" * 10)

        verify_res = backup_service.verify_backup(corrupted_db, expect_metadata=False)
        assert verify_res.valid is False
        assert "check failed" in (verify_res.error_message or "").lower() or "not a database" in (verify_res.error_message or "").lower()

    def test_fi_07_restore_from_invalid_backup_rejected(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-07: Restore from invalid/corrupted backup is rejected before touching destination."""
        corrupted_backup = temp_dir / "bad_backup.db"
        with open(corrupted_backup, "wb") as f:
            f.write(b"CORRUPTED_BYTES" * 20)

        restore_target = temp_dir / "restore_dest.db"
        result = backup_service.restore_database(
            backup_path=corrupted_backup,
            target_db_path=restore_target,
        )
        assert result.success is False
        assert "Restore from invalid backup rejected" in (result.error_message or "")
        assert not restore_target.exists()

    def test_fi_08_restore_destination_already_exists(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-08: Restore destination already exists and overwrite=False is safely rejected."""
        source_db = temp_dir / "src.db"
        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        backup_file = temp_dir / "backup.db"
        bkp_res = backup_service.create_backup(source_db, backup_file)
        assert bkp_res.success is True

        existing_dest = temp_dir / "existing_dest.db"
        existing_dest.write_text("existing content")

        result = backup_service.restore_database(
            backup_path=backup_file,
            target_db_path=existing_dest,
            overwrite=False,
        )
        assert result.success is False
        assert "already exists and overwrite=False" in (result.error_message or "")
        # Original content preserved
        assert existing_dest.read_text() == "existing content"

    def test_fi_09_restore_interrupted_before_publication(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-09: Restore interrupted during staging cleans up temporary files without touching target."""
        source_db = temp_dir / "src_for_abort.db"
        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT);")
            for i in range(50):
                conn.execute("INSERT INTO t VALUES (?);", (i,))

        backup_file = temp_dir / "backup_abort.db"
        backup_service.create_backup(source_db, backup_file)

        target_file = temp_dir / "target_never_published.db"

        def abort_restore(status: int, remaining: int, total: int) -> None:
            raise RuntimeError("Aborted during restore")

        res = backup_service.restore_database(
            backup_path=backup_file,
            target_db_path=target_file,
            progress_callback=abort_restore,
        )
        assert res.success is False
        assert "Restore interrupted" in (res.error_message or "")
        assert not target_file.exists()
        # Verify no orphaned staging files remain
        staged = list(temp_dir.glob("target_never_published.db.restore_tmp_*"))
        assert len(staged) == 0

    def test_fi_10_atomic_publication_failure(self, temp_dir: Path, backup_service: DatabaseBackupService, monkeypatch: pytest.MonkeyPatch) -> None:
        """FI-10: Atomic publication failure (os.replace failure) cleans up staging file safely."""
        source_db = temp_dir / "src_pub_fail.db"
        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        dest_file = temp_dir / "dest_pub_fail.db"

        def failing_replace(src: Union[str, Path], dst: Union[str, Path]) -> None:
            raise OSError("Simulated atomic replace failure / disk lock")

        monkeypatch.setattr(os, "replace", failing_replace)

        res = backup_service.create_backup(source_db_path=source_db, dest_backup_path=dest_file)
        assert res.success is False
        assert "Atomic publication failure" in (res.error_message or "")
        assert not dest_file.exists()
        staged = list(temp_dir.glob("dest_pub_fail.db.tmp_*"))
        assert len(staged) == 0

    def test_fi_11_foreign_key_or_integrity_check_failure(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-11: Database with integrity or foreign-key violations fails verification."""
        fk_db = temp_dir / "fk_corrupt.db"
        with sqlite3.connect(fk_db) as conn:
            conn.execute("PRAGMA foreign_keys = OFF;")
            conn.execute("CREATE TABLE parent (id INT PRIMARY KEY);")
            conn.execute("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id));")
            conn.execute("INSERT INTO child VALUES (1, 999);")  # Dangling FK
            conn.commit()

        # When foreign_keys are checked, this must be caught
        verify_res = backup_service.verify_backup(fk_db, expect_metadata=False)
        assert verify_res.valid is False
        assert "foreign key" in (verify_res.error_message or "").lower()

    def test_fi_12_backup_during_concurrent_writes(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-12: Online backup during concurrent writer threads captures consistent snapshot."""
        live_db = str(temp_dir / "concurrent_live.db")
        backup_dest = str(temp_dir / "concurrent_backup.db")

        # Set up WAL mode
        with sqlite3.connect(live_db) as conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute(
                """
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    val TEXT,
                    ts REAL
                )
                """
            )
            conn.commit()

        stop_event = threading.Event()
        writes_committed = 0
        writer_lock = threading.Lock()

        def writer_thread(tid: int) -> None:
            nonlocal writes_committed
            idx = 0
            while not stop_event.is_set():
                idx += 1
                try:
                    with sqlite3.connect(live_db, timeout=10.0) as wconn:
                        with wconn:
                            item_id = tid * 10000 + idx
                            wconn.execute("INSERT OR REPLACE INTO items VALUES (?, ?, ?);", (item_id, f"t_{tid}_{idx}", time.time()))
                            with writer_lock:
                                writes_committed += 1
                except Exception:
                    pass
                time.sleep(0.005)

        # Launch 3 concurrent writers
        threads = [threading.Thread(target=writer_thread, args=(i,), daemon=True) for i in range(3)]
        for t in threads:
            t.start()

        # Allow writers to generate load
        time.sleep(0.1)

        # Create online backup concurrently
        bkp_res = backup_service.create_backup(source_db_path=live_db, dest_backup_path=backup_dest)

        stop_event.set()
        for t in threads:
            t.join(timeout=2.0)

        assert bkp_res.success is True
        assert bkp_res.checksum_sha256 is not None

        # Verify backup integrity
        verif = backup_service.verify_backup(backup_dest, expect_metadata=True)
        assert verif.valid is True
        assert verif.integrity_check == "ok"
        assert "items" in verif.tables_found
        assert verif.table_counts["items"] > 0

    def test_fi_13_restore_after_outbox_and_ledger_transaction(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-13: Restore immediately after an atomic ledger+outbox transaction is verified."""
        source_db = str(temp_dir / "tx_src.db")
        backup_file = str(temp_dir / "tx_backup.db")
        restored_db = str(temp_dir / "tx_restored.db")

        ledger = PaperLedger(db_path=source_db, initial_balance=50000.0)
        outbox = TransactionalOutbox(db_path=source_db)

        # Atomic commit
        entry = PaperLedgerEntry(
            entry_id="tx_ent_1",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_FILL,
            cash_change=-500.0,
            cash_balance_after=49500.0,
            fee=2.5,
            realized_pnl=0.0,
            idempotency_key="tx_key_1",
        )
        rec = OutboxRecord(
            event_id="tx_evt_1",
            event_type="order.filled",
            schema_version=1,
            payload={"fill": True},
        )
        ledger.record_entry_with_outbox(entry, rec)

        # Backup & restore
        b_res = backup_service.create_backup(source_db, backup_file)
        assert b_res.success is True
        r_res = backup_service.restore_database(backup_file, restored_db)
        assert r_res.success is True

        # Verify recovered components
        rec_ledger = PaperLedger(db_path=restored_db)
        rec_outbox = TransactionalOutbox(db_path=restored_db)

        assert rec_ledger.count() == 2  # initial capital + fill
        assert rec_ledger.get_entry_by_idempotency_key("tx_key_1") is not None
        pending = rec_outbox.get_pending_batch()
        assert any(r.event_id == "tx_evt_1" for r in pending)

    def test_fi_14_repeated_restore_and_recovery_idempotency(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-14: Repeated restore and state recovery produces identical deterministic results."""
        source_db = str(temp_dir / "idemp_src.db")
        backup_file = str(temp_dir / "idemp_backup.db")
        dest_db_1 = str(temp_dir / "idemp_dest_1.db")
        dest_db_2 = str(temp_dir / "idemp_dest_2.db")

        ledger = PaperLedger(db_path=source_db, initial_balance=100000.0)
        alloc_mgr = PortfolioAllocationManager(
            portfolio=PaperPortfolio(initial_cash=100000.0),
            ledger=ledger,
            db_path=source_db,
        )
        alloc_res = alloc_mgr.apply_allocation(
            PortfolioAllocationRequest(
                request_id="idemp_alloc_req",
                sleeves=[
                    StrategySleeveDefinition(sleeve_id="sleeve_a", name="Sleeve A", target_budget=25000.0),
                ],
            )
        )
        assert alloc_res.success is True
        snap1 = alloc_mgr.create_equity_snapshot("snap_idemp")

        b_res = backup_service.create_backup(source_db, backup_file)
        assert b_res.success is True

        # Restore to target 1
        r1 = backup_service.restore_database(backup_file, dest_db_1)
        assert r1.success is True

        # Restore to target 2
        r2 = backup_service.restore_database(backup_file, dest_db_2)
        assert r2.success is True

        # Check hashes of both restored databases
        h1 = compute_file_sha256(dest_db_1)
        h2 = compute_file_sha256(dest_db_2)
        assert h1 == h2 == b_res.checksum_sha256

    def test_fi_15_backup_metadata_redaction(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-15: Metadata sidecar redacts secrets, tokens, passwords, and API keys."""
        source_db = temp_dir / "src_secrets.db"
        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        dest_db = temp_dir / "backup_secrets.db"
        secret_token = "ultra_secret_auth_token_9999"
        api_key = "twelve_data_secret_key_12345"
        password = "super_private_password"

        res = backup_service.create_backup(
            source_db_path=source_db,
            dest_backup_path=dest_db,
            custom_metadata={
                "dashboard_auth_token": secret_token,
                "api_key": api_key,
                "password": password,
                "nested": {"token": "nested_secret", "normal_field": "visible"},
            },
        )
        assert res.success is True

        # Read the sidecar JSON file
        meta_file = temp_dir / "backup_secrets.db.meta.json"
        assert meta_file.exists()
        raw_meta = meta_file.read_text(encoding="utf-8")

        assert secret_token not in raw_meta
        assert api_key not in raw_meta
        assert password not in raw_meta
        assert "nested_secret" not in raw_meta

        meta_dict = json.loads(raw_meta)
        custom = meta_dict["custom_metadata"]
        assert custom["dashboard_auth_token"] == "[REDACTED]"
        assert custom["api_key"] == "[REDACTED]"
        assert custom["password"] == "[REDACTED]"
        assert custom["nested"]["token"] == "[REDACTED]"
        assert custom["nested"]["normal_field"] == "visible"

    def test_fi_16_temporary_file_cleanup_after_failures(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """FI-16: All temporary staging files are unlinked and cleaned up after backup or restore failure."""
        source_db = temp_dir / "src_clean.db"
        with sqlite3.connect(source_db) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        dest_db = temp_dir / "dest_clean.db"

        # Force failure
        def fail_callback(s: int, r: int, t: int) -> None:
            raise ValueError("Failure injection for cleanup test")

        res = backup_service.create_backup(
            source_db_path=source_db,
            dest_backup_path=dest_db,
            progress_callback=fail_callback,
        )
        assert res.success is False

        # Verify no .tmp_* or .meta.json.tmp_* files remain
        remaining_temps = [p.name for p in temp_dir.glob("*.tmp_*")]
        assert len(remaining_temps) == 0


# ==============================================================================
# SECTION 9: RETENTION AND OPERATIONAL CONTROLS
# ==============================================================================


class TestBackupRetentionAndOperationalControls:
    """Verifies retention pruning, newest-backup protection, and dry-run preview."""

    def test_retention_prunes_older_backups_keeps_max_count(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """Prunes excess older backups and retains exactly max_keep backups."""
        backup_dir = temp_dir / "retention_dir"
        backup_dir.mkdir()

        # Create 6 fake backup files with staggered timestamps
        created_files: List[Path] = []
        for i in range(6):
            p = backup_dir / f"backup_{i:02d}.db"
            p.write_bytes(f"db_content_{i}".encode("utf-8"))
            meta = backup_dir / f"backup_{i:02d}.db.meta.json"
            meta.write_text(json.dumps({"id": i}))
            # Set explicit modification times
            os.utime(p, (1000 + i * 100, 1000 + i * 100))
            created_files.append(p)

        # Prune keeping 3
        prune_res = backup_service.prune_backups(backup_dir=backup_dir, max_keep=3, dry_run=False)
        assert prune_res.success is True
        assert len(prune_res.pruned_files) == 3
        assert len(prune_res.retained_files) == 3

        # Newest files (indices 5, 4, 3) must be retained
        assert str(created_files[5]) in prune_res.retained_files
        assert str(created_files[4]) in prune_res.retained_files
        assert str(created_files[3]) in prune_res.retained_files

        # Older files (indices 0, 1, 2) must be deleted
        assert not created_files[0].exists()
        assert not (backup_dir / "backup_00.db.meta.json").exists()

    def test_retention_never_prunes_newest_verified_backup(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """Even with max_keep=1, the newest backup is never pruned."""
        backup_dir = temp_dir / "retention_dir2"
        backup_dir.mkdir()

        f1 = backup_dir / "old.db"
        f1.write_bytes(b"old")
        os.utime(f1, (1000, 1000))

        f2 = backup_dir / "newest.db"
        f2.write_bytes(b"newest")
        os.utime(f2, (2000, 2000))

        prune_res = backup_service.prune_backups(backup_dir=backup_dir, max_keep=1, dry_run=False)
        assert prune_res.success is True
        assert str(f2) in prune_res.retained_files
        assert f2.exists()
        assert not f1.exists()

    def test_retention_never_prunes_in_use_files(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """In-use files are strictly protected from retention pruning."""
        backup_dir = temp_dir / "retention_dir3"
        backup_dir.mkdir()

        f1 = backup_dir / "in_use.db"
        f1.write_bytes(b"in_use")
        os.utime(f1, (1000, 1000))

        f2 = backup_dir / "newer.db"
        f2.write_bytes(b"newer")
        os.utime(f2, (2000, 2000))

        prune_res = backup_service.prune_backups(
            backup_dir=backup_dir,
            max_keep=1,
            dry_run=False,
            in_use_files={str(f1)},
        )
        assert prune_res.success is True
        # Both newest and in-use are kept
        assert str(f1) in prune_res.retained_files
        assert str(f2) in prune_res.retained_files
        assert f1.exists()
        assert f2.exists()

    def test_retention_dry_run_preview(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """Dry-run returns preview of files to prune without deleting anything."""
        backup_dir = temp_dir / "retention_dir4"
        backup_dir.mkdir()

        for i in range(4):
            p = backup_dir / f"b_{i}.db"
            p.write_bytes(b"dummy")
            os.utime(p, (1000 + i, 1000 + i))

        prune_res = backup_service.prune_backups(backup_dir=backup_dir, max_keep=2, dry_run=True)
        assert prune_res.success is True
        assert prune_res.dry_run is True
        assert len(prune_res.pruned_files) == 2
        # Verify files are NOT deleted on disk
        for i in range(4):
            assert (backup_dir / f"b_{i}.db").exists()


# ==============================================================================
# SECTION 10 & 11: CONFIGURATION & DATABASE ISOLATION REGRESSION
# ==============================================================================


class TestBackupConfigurationAndIsolationSafety:
    """Verifies configuration validation and protection of persistent user data."""

    def test_settings_backup_defaults_and_validation(self) -> None:
        """Verify default settings are safe, offline, and validate bounds."""
        s = Settings()
        assert s.backup_dir == "data/backups"
        assert s.backup_retention_enabled is False
        assert s.backup_retention_max_count == 5
        assert s.backup_scheduler_enabled is False
        assert s.execution_locked is True
        assert s.paper_trading_only is True

        # Validation of retention max count
        with pytest.raises(ConfigurationError, match="BACKUP_RETENTION_MAX_COUNT"):
            Settings(backup_retention_max_count=0)

    def test_is_protected_target_path_guards_real_kaiser_db(self) -> None:
        """is_protected_target_path rejects all variants and companion files of data/kaiser.db."""
        assert is_protected_target_path("data/kaiser.db") is True
        assert is_protected_target_path("./data/kaiser.db") is True
        assert is_protected_target_path("kaiser.db") is True
        assert is_protected_target_path("data/kaiser.db-wal") is True
        assert is_protected_target_path("data/kaiser.db-shm") is True
        assert is_protected_target_path(Path("data/kaiser.db").resolve()) is True

        # Disposable temp paths must NOT be protected
        assert is_protected_target_path("tmp/disposable.db") is False
        assert is_protected_target_path("/tmp/test_isolated.db") is False
        assert is_protected_target_path(":memory:") is False

    def test_restore_database_rejects_real_kaiser_db(self, temp_dir: Path, backup_service: DatabaseBackupService) -> None:
        """restore_database strictly rejects data/kaiser.db as target."""
        dummy_backup = temp_dir / "valid_dummy.db"
        with sqlite3.connect(dummy_backup) as conn:
            conn.execute("CREATE TABLE t (id INT);")

        res = backup_service.restore_database(
            backup_path=dummy_backup,
            target_db_path="data/kaiser.db",
        )
        assert res.success is False
        assert "Protected production database destination rejected" in (res.error_message or "")
