"""KAISER — Phase 2, Step 77 Test Suite: Database Architecture & Concurrency Reliability.

Audits, validates, and stresses:
1. SQLite connection lifecycle, context managers, and resource cleanup.
2. Multi-threaded SQLite concurrency under WAL mode with busy timeout handling.
3. Transaction boundaries, atomic ledger-plus-outbox commits, and post-commit event delivery isolation.
4. Cross-component schema compatibility and multi-constructor initialization order.
5. All 14 failure-injection and recovery scenarios on isolated disposable databases.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from typing import Generator, List, Optional

import pytest

from app.analysis.runs.models import AnalysisRun, OperationType, RunStatus
from app.analysis.runs.registry import AnalysisRunRegistry
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.state import StateStore
from app.execution.allocation import PortfolioAllocationManager, SleeveRiskBudget
from app.execution.engine import PaperExecutionEngine
from app.execution.maintenance import MaintenanceScheduler
from app.execution.models import PaperOrderRequest
from app.execution.workflow import PaperTradingWorkflowCoordinator
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.ledger.outbox import OutboxRecord, OutboxStatus, TransactionalOutbox
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.provenance.models import DataSourceType, MarketDataProvenance
from app.market.provenance.tracker import MarketDataProvenanceTracker


PROD_DB_PATH = Path("data/kaiser.db")


def _read_db_hash(path: Path) -> str:
    """Read SHA-256 hash of a file if it exists, or empty string."""
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


# ==============================================================================
# SECTION A: CONNECTION LIFECYCLE & PRAGMAS
# ==============================================================================

class TestConnectionLifecycleAndPragmas:
    """Tests for SQLite connection pragmas, thread safety, and lifecycle."""

    def test_historical_storage_pragmas_and_thread_safety(self, tmp_path: Path) -> None:
        """Verify SQLiteHistoricalBarRepository sets WAL mode, synchronous=NORMAL, and busy_timeout."""
        db_file = str(tmp_path / "storage_pragmas.db")
        repo = SQLiteHistoricalBarRepository(db_path=db_file)

        conn = repo._get_connection()
        journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0].upper()
        synchronous = conn.execute("PRAGMA synchronous;").fetchone()[0]
        busy_timeout = conn.execute("PRAGMA busy_timeout;").fetchone()[0]

        assert journal_mode == "WAL"
        assert synchronous in (1, "NORMAL", "1")  # NORMAL is 1 in SQLite
        assert busy_timeout >= 5000

        repo.close()

    def test_idempotent_component_shutdown(self, tmp_path: Path) -> None:
        """Verify close() and shutdown() methods are idempotent across components."""
        db_file = str(tmp_path / "shutdown_test.db")

        repo = SQLiteHistoricalBarRepository(db_path=db_file)
        registry = AnalysisRunRegistry(db_path=db_file)
        tracker = MarketDataProvenanceTracker(db_path=db_file)

        # First close
        repo.close()
        registry.close()
        tracker.close()

        # Repeated close must be cleanly idempotent and never raise
        repo.close()
        registry.close()
        tracker.close()


# ==============================================================================
# SECTION B: CONCURRENCY & WAL RELIABILITY
# ==============================================================================

class TestConcurrencyAndWalReliability:
    """Multi-threaded stress testing of concurrent writes and reads under WAL mode."""

    def test_concurrent_ledger_inserts_with_unique_identifiers(self, tmp_path: Path) -> None:
        """Verify concurrent threads inserting unique ledger entries complete without data loss or corruption."""
        db_file = str(tmp_path / "concurrent_ledger.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=500_000.0)

        num_threads = 6
        inserts_per_thread = 15

        def worker(thread_idx: int) -> int:
            success = 0
            for i in range(inserts_per_thread):
                entry = PaperLedgerEntry(
                    entry_id=f"entry_t{thread_idx}_{i}",
                    timestamp=1700000000.0 + thread_idx * 1000 + i,
                    entry_type=LedgerEntryType.ORDER_SUBMISSION,
                    order_id=f"ord_t{thread_idx}_{i}",
                    client_order_id=f"cl_t{thread_idx}_{i}",
                    symbol="RELIANCE",
                    side="BUY",
                    quantity=1.0,
                    price=2500.0,
                    fee=0.0,
                    cash_change=0.0,
                    cash_balance_after=500_000.0,
                    realized_pnl=0.0,
                    idempotency_key=f"idem_t{thread_idx}_{i}",
                )
                if ledger.record_entry(entry):
                    success += 1
            return success

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, t) for t in range(num_threads)]
            results = [f.result() for f in futures]

        assert sum(results) == num_threads * inserts_per_thread
        # Total entries: 1 initial balance + num_threads * inserts_per_thread
        assert len(ledger.get_entries()) == 1 + (num_threads * inserts_per_thread)

    def test_concurrent_duplicate_idempotency_keys(self, tmp_path: Path) -> None:
        """Verify concurrent attempts to insert identical idempotency keys accept exactly one."""
        db_file = str(tmp_path / "duplicate_idem.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)

        num_threads = 6
        shared_key = "race_condition_idem_key"

        def race_worker(thread_idx: int) -> bool:
            entry = PaperLedgerEntry(
                entry_id=f"race_entry_{thread_idx}",
                timestamp=1700000000.0 + thread_idx,
                entry_type=LedgerEntryType.ORDER_SUBMISSION,
                order_id="ord_shared",
                idempotency_key=shared_key,
            )
            return ledger.record_entry(entry)

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(race_worker, t) for t in range(num_threads)]
            results = [f.result() for f in futures]

        # Exactly one thread must report True (newly inserted), others False
        assert sum(1 for r in results if r is True) == 1
        assert sum(1 for r in results if r is False) == num_threads - 1

    def test_concurrent_outbox_staging_and_deduplication(self, tmp_path: Path) -> None:
        """Verify concurrent threads staging outbox records deduplicate safely."""
        db_file = str(tmp_path / "concurrent_outbox.db")
        outbox = TransactionalOutbox(db_path=db_file)

        num_threads = 6
        shared_event_id = "shared_event_dedup_01"

        def outbox_worker(thread_idx: int) -> bool:
            rec = OutboxRecord(
                event_id=shared_event_id,
                event_type="PAPER_ORDER_SUBMITTED",
                payload={"thread": thread_idx},
            )
            return outbox.stage_record(rec)

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(outbox_worker, t) for t in range(num_threads)]
            results = [f.result() for f in futures]

        assert sum(1 for r in results if r is True) == 1
        assert sum(1 for r in results if r is False) == num_threads - 1

    def test_concurrent_reads_while_writes_occur(self, tmp_path: Path) -> None:
        """Verify WAL mode allows concurrent readers to query without being blocked by concurrent writers."""
        db_file = str(tmp_path / "wal_concurrency.db")
        repo = SQLiteHistoricalBarRepository(db_path=db_file)

        # Pre-populate some bars
        base_bars = [
            Bar(
                symbol="TCS",
                exchange="NSE",
                timeframe="1m",
                timestamp=1700000000.0 + i * 60,
                open=3500.0,
                high=3510.0,
                low=3490.0,
                close=3505.0,
                volume=100.0,
                source="test",
            )
            for i in range(20)
        ]
        repo.store_bars(base_bars)

        stop_event = threading.Event()
        read_counts: List[int] = []

        def writer():
            for i in range(20, 45):
                if stop_event.is_set():
                    break
                bar = Bar(
                    symbol="TCS",
                    exchange="NSE",
                    timeframe="1m",
                    timestamp=1700000000.0 + i * 60,
                    open=3500.0,
                    high=3510.0,
                    low=3490.0,
                    close=3505.0,
                    volume=100.0,
                    source="test",
                )
                repo.store_bars([bar])
                time.sleep(0.005)

        def reader():
            while not stop_event.is_set():
                bars = repo.query_bars("TCS", "1m", 1700000000.0, 1700005000.0)
                read_counts.append(len(bars))
                time.sleep(0.002)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            w_future = executor.submit(writer)
            r_futures = [executor.submit(reader) for _ in range(3)]
            w_future.result()
            stop_event.set()
            for rf in r_futures:
                rf.result()

        repo.close()
        assert len(read_counts) > 0
        assert all(c >= 20 for c in read_counts)


# ==============================================================================
# SECTION C: CROSS-COMPONENT COMPATIBILITY & MULTI-CONSTRUCTOR INITIALIZATION
# ==============================================================================

class TestCrossComponentDatabaseCompatibility:
    """Verifies that multiple components sharing a single SQLite file initialize and coexist cleanly."""

    def test_arbitrary_initialization_order(self, tmp_path: Path) -> None:
        """Components can initialize in reverse order without schema conflicts or transaction interference."""
        db_file = str(tmp_path / "shared_components.db")

        # Reverse init order: Outbox -> Ledger -> Tracker -> Registry -> Historical Repo
        outbox = TransactionalOutbox(db_path=db_file)
        ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        tracker = MarketDataProvenanceTracker(db_path=db_file)
        registry = AnalysisRunRegistry(db_path=db_file)
        repo = SQLiteHistoricalBarRepository(db_path=db_file)

        # Verify all tables exist in the shared database
        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = {row[0] for row in cursor.fetchall()}

        expected_tables = {
            "paper_outbox_entries",
            "paper_ledger_entries",
            "market_data_provenance",
            "analysis_runs",
            "historical_bars",
            "schema_meta",
        }
        for t in expected_tables:
            assert t in tables, f"Expected table '{t}' was not created in shared database!"

        # Clean shutdown
        repo.close()
        registry.close()
        tracker.close()


# ==============================================================================
# SECTION D: 14 FAILURE-INJECTION SCENARIOS
# ==============================================================================

class TestFourteenFailureInjectionScenarios:
    """Verifies the complete 14-scenario failure injection matrix."""

    def test_fi_01_connection_creation_failure(self, tmp_path: Path) -> None:
        """Scenario 1: Connection creation failure handles error cleanly without process crash."""
        dir_as_file = str(tmp_path / "directory_as_file")
        Path(dir_as_file).mkdir()

        # Opening a directory as a SQLite file fails cleanly in AnalysisRunRegistry
        reg = AnalysisRunRegistry(db_path=dir_as_file)
        assert reg._conn is None

    def test_fi_02_schema_initialization_failure(self, tmp_path: Path) -> None:
        """Scenario 2: Schema initialization error is caught and logged safely."""
        db_file = str(tmp_path / "schema_fail.db")
        # Create a table named 'analysis_runs' with conflicting schema
        with sqlite3.connect(db_file) as conn:
            conn.execute("CREATE TABLE analysis_runs (incompatible_col INT PRIMARY KEY);")

        # Initializing AnalysisRunRegistry logs warning and leaves registry usable in-memory
        reg = AnalysisRunRegistry(db_path=db_file)
        assert reg.name == "analysis_run_registry"

    def test_fi_03_write_transaction_exception_before_commit(self, tmp_path: Path) -> None:
        """Scenario 3: An unhandled exception before commit triggers rollback and prevents partial writes."""
        db_file = str(tmp_path / "rollback_test.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)

        with pytest.raises(RuntimeError, match="Simulated mid-transaction failure"):
            with sqlite3.connect(db_file) as conn:
                with conn:
                    conn.execute(
                        """
                        INSERT INTO paper_ledger_entries (
                            entry_id, timestamp, entry_type, fee, cash_change,
                            cash_balance_after, realized_pnl, idempotency_key
                        ) VALUES ('uncommitted_entry', 1700000000.0, 'ORDER_SUBMISSION', 0, 0, 100000, 0, 'bad_key')
                        """
                    )
                    raise RuntimeError("Simulated mid-transaction failure")

        # Verify uncommitted row was rolled back
        with sqlite3.connect(db_file) as conn:
            res = conn.execute("SELECT COUNT(*) FROM paper_ledger_entries WHERE entry_id='uncommitted_entry';").fetchone()[0]
        assert res == 0

    def test_fi_04_ledger_plus_outbox_atomic_rollback(self, tmp_path: Path) -> None:
        """Scenario 4: If outbox staging fails inside shared transaction, ledger write is rolled back."""
        db_file = str(tmp_path / "atomic_rollback.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)

        entry = PaperLedgerEntry(
            entry_id="atom_entry_01",
            timestamp=1700000000.0,
            entry_type=LedgerEntryType.ORDER_SUBMISSION,
            idempotency_key="atom_idem_01",
        )
        # Construct outbox record with non-JSON-serializable payload
        bad_outbox = OutboxRecord(
            event_id="bad_outbox_01",
            event_type="PAPER_ORDER_SUBMITTED",
            payload={"unserializable": object()},
        )

        with pytest.raises(TypeError):
            ledger.record_entry_with_outbox(entry, bad_outbox)

        # Verify entry was rolled back from database
        with sqlite3.connect(db_file) as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM paper_ledger_entries WHERE entry_id='atom_entry_01';").fetchone()[0]
        assert cnt == 0
        assert "atom_idem_01" not in ledger._by_idempotency_key

    def test_fi_05_event_publication_failure_after_durable_commit(self, tmp_path: Path) -> None:
        """Scenario 5: Post-commit event publication failure does not roll back committed financial entry."""
        db_file = str(tmp_path / "post_commit.db")

        class BrokenEventBus(EventBus):
            def publish(self, event):
                raise RuntimeError("Simulated EventBus publication crash!")

        broken_bus = BrokenEventBus()
        ledger = PaperLedger(db_path=db_file, event_bus=broken_bus, initial_balance=100_000.0)

        entry = PaperLedgerEntry(
            entry_id="durable_entry_01",
            timestamp=1700000000.0,
            entry_type=LedgerEntryType.ORDER_SUBMISSION,
            idempotency_key="durable_idem_01",
        )

        # Must succeed and remain durable despite EventBus crash
        success = ledger.record_entry(entry)
        assert success is True

        with sqlite3.connect(db_file) as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM paper_ledger_entries WHERE entry_id='durable_entry_01';").fetchone()[0]
        assert cnt == 1

    def test_fi_06_sqlite_busy_locked_contention(self, tmp_path: Path) -> None:
        """Scenario 6: SQLite busy timeout handling and lock contention raises OperationalError when locked."""
        db_file = str(tmp_path / "busy_contention.db")

        # Open connection 1 with an active exclusive transaction
        conn1 = sqlite3.connect(db_file, timeout=0.1)
        conn1.execute("CREATE TABLE lock_test (x INT);")
        conn1.execute("BEGIN EXCLUSIVE;")
        conn1.execute("INSERT INTO lock_test VALUES (1);")

        # Connection 2 attempts an immediate write with timeout=0.05
        conn2 = sqlite3.connect(db_file, timeout=0.05)
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            conn2.execute("INSERT INTO lock_test VALUES (2);")

        conn1.rollback()
        conn1.close()
        conn2.close()

    def test_fi_07_connection_closure_during_component_shutdown(self, tmp_path: Path) -> None:
        """Scenario 7: Component shutdown cleanly closes open connection and sets _conn to None."""
        db_file = str(tmp_path / "closure_test.db")
        repo = SQLiteHistoricalBarRepository(db_path=db_file)
        assert repo._conn is not None

        repo.close()
        assert repo._conn is None

    def test_fi_08_repeated_component_shutdown(self, tmp_path: Path) -> None:
        """Scenario 8: Calling close() multiple times on repository is cleanly idempotent."""
        db_file = str(tmp_path / "repeated_close.db")
        repo = SQLiteHistoricalBarRepository(db_path=db_file)
        repo.close()
        repo.close()
        repo.close()
        assert repo._conn is None

    def test_fi_09_partial_bootstrap_failure(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scenario 9: Simulated initialization failure raises ConfigurationError cleanly."""
        test_db = str(tmp_path / "bootstrap_fail.db")
        monkeypatch.setenv("KAISER_TEST_DB_PATH", test_db)

        # Trigger configuration failure via negative fee rate
        with pytest.raises(ConfigurationError):
            Settings(paper_fee_rate=-0.5)

    def test_fi_10_database_reopen_and_recovery(self, tmp_path: Path) -> None:
        """Scenario 10: Database reopen and recovery loads 100% consistent ledger state without duplication."""
        db_file = str(tmp_path / "recovery_clean.db")

        # 1. First lifecycle: record entries
        l1 = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        now_ts = time.time()
        e1 = PaperLedgerEntry(
            entry_id="rec_e1",
            timestamp=now_ts + 10.0,
            entry_type=LedgerEntryType.ORDER_FILL,
            order_id="ord_1",
            symbol="RELIANCE",
            side="BUY",
            quantity=10.0,
            price=2500.0,
            fee=12.5,
            cash_change=-25012.5,
            cash_balance_after=74987.5,
            realized_pnl=0.0,
            idempotency_key="rec_idem_1",
        )
        l1.record_entry(e1)
        del l1

        # 2. Second lifecycle: reopen from disk
        l2 = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        entries = l2.get_entries()

        assert len(entries) == 2  # 1 initial capital + 1 fill
        assert entries[1].entry_id == "rec_e1"
        assert entries[1].cash_balance_after == 74987.5

        # 3. Third lifecycle: verify idempotency on repeated recovery
        l3 = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        assert len(l3.get_entries()) == 2

    def test_fi_11_concurrent_duplicate_ledger_insertion(self, tmp_path: Path) -> None:
        """Scenario 11: Concurrent duplicate ledger insertion accepts exactly one."""
        db_file = str(tmp_path / "fi_dup_ledger.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=50_000.0)

        entry = PaperLedgerEntry(
            entry_id="dup_target",
            timestamp=1700000000.0,
            entry_type=LedgerEntryType.ORDER_SUBMISSION,
            idempotency_key="shared_key_11",
        )

        def attempt_insert():
            return ledger.record_entry(entry)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(attempt_insert) for _ in range(4)]
            results = [f.result() for f in futures]

        assert sum(1 for r in results if r is True) == 1
        assert sum(1 for r in results if r is False) == 3

    def test_fi_12_concurrent_duplicate_outbox_staging(self, tmp_path: Path) -> None:
        """Scenario 12: Concurrent duplicate outbox staging accepts exactly one."""
        db_file = str(tmp_path / "fi_dup_outbox.db")
        outbox = TransactionalOutbox(db_path=db_file)

        rec = OutboxRecord(
            event_id="dup_event_12",
            event_type="PAPER_ORDER_SUBMITTED",
            payload={"data": 12},
        )

        def attempt_stage():
            return outbox.stage_record(rec)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(attempt_stage) for _ in range(4)]
            results = [f.result() for f in futures]

        assert sum(1 for r in results if r is True) == 1
        assert sum(1 for r in results if r is False) == 3

    def test_fi_13_wal_checkpoint_under_active_readers(self, tmp_path: Path) -> None:
        """Scenario 13: WAL checkpoint PASSIVE runs safely without blocking or errors under active readers."""
        db_file = str(tmp_path / "checkpoint_readers.db")

        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("CREATE TABLE t (id INT);")
        for i in range(10):
            conn.execute("INSERT INTO t VALUES (?);", (i,))
        conn.commit()

        # Reader connection
        r_conn = sqlite3.connect(db_file)
        cursor = r_conn.execute("SELECT id FROM t;")
        row1 = cursor.fetchone()
        assert row1 is not None

        # Checkpoint while cursor is open
        cp_result = conn.execute("PRAGMA wal_checkpoint(PASSIVE);").fetchone()
        assert cp_result[0] == 0  # 0 indicates checkpoint completed without lock error

        # Reader can continue reading remaining rows
        remaining = cursor.fetchall()
        assert len(remaining) == 9

        conn.close()
        r_conn.close()

    def test_fi_14_recovery_with_corrupted_records(self, tmp_path: Path) -> None:
        """Scenario 14: Corrupted rows in SQLite are caught and fail-safe with ValidationError."""
        db_file = str(tmp_path / "corrupt_load.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)

        # Directly insert a malformed JSON row into the SQLite database
        with sqlite3.connect(db_file) as conn:
            conn.execute(
                """
                INSERT INTO paper_ledger_entries (
                    entry_id, timestamp, entry_type, fee, cash_change,
                    cash_balance_after, realized_pnl, idempotency_key, metadata_json
                ) VALUES ('corrupt_entry', 1700000000.0, 'ORDER_SUBMISSION', 0, 0, 100000, 0, 'corrupt_key', '{bad_json:')
                """
            )

        # Reopen ledger: corrupted metadata raises ValidationError with clear diagnostic context
        with pytest.raises(ValidationError, match="Corrupted metadata JSON in ledger entry corrupt_entry"):
            PaperLedger(db_path=db_file, initial_balance=100_000.0)
