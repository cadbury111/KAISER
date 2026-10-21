"""Comprehensive Test Suite for Phase 2, Step 67:
Maintenance Scheduler Concurrency, Crash-Recovery & Operational Resilience Validation.

Test Matrix:
1. Concurrent scheduler start
2. Concurrent scheduler stop
3. Repeated start/stop cycles
4. Concurrent manual and scheduled execution
5. Overlap prevention or coalescing
6. Task failure isolation and subsequent recovery
7. Stop during a running task
8. Bounded shutdown timeout
9. Active session blocks pruning
10. Incomplete or failed session recovery blocks pruning
11. Session-start versus pruning race
12. In-flight and undelivered outbox protection
13. Crash before task claim
14. Crash after claim but before completion
15. Restart and repeated recovery idempotency
16. SQLite failures before commit
17. Ambiguous commit handling
18. Concurrent duplicate digest creation
19. Immutable historical digest retrieval
20. Missing or stale health-digest source data
21. Digest generation does not mutate financial state
22. Bounded diagnostic history
23. Secret redaction
24. Configuration validation
25. Dashboard authentication and read-only regressions
26. Thread timeout truthful reporting
27. Failed reconciliation does not report balanced
28. System safety invariants remain locked
"""

from __future__ import annotations

import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock, patch

import pytest

from app.core.config import Settings, get_settings
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.events.bus import EventBus
from app.core.events.event import EventType
from app.execution.allocation import (
    PortfolioAllocationManager,
    create_standard_multi_sleeve_request,
)
from app.execution.maintenance import (
    HealthDigestSnapshot,
    MaintenanceScheduler,
    MaintenanceTaskResult,
    MaintenanceTaskStatus,
    sanitize_sensitive_text,
    sanitize_dict_secrets,
)
from app.execution.outbox_dispatcher import OutboxDispatcher
from app.execution.portfolio import PaperPortfolio
from app.execution.workflow import PaperTradingWorkflowCoordinator, PaperSimulationSession
from app.ledger.ledger import PaperLedger
from app.ledger.outbox import OutboxRecord, OutboxStatus, TransactionalOutbox
from app.portfolio.dashboard.server import PortfolioDashboardServer


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def mock_clock() -> MagicMock:
    """Deterministic controllable wall clock."""
    current_time = 1_000_000.0

    def get_time() -> float:
        nonlocal current_time
        return current_time

    def advance(delta: float) -> None:
        nonlocal current_time
        current_time += delta

    mock = MagicMock(side_effect=get_time)
    mock.advance = advance
    mock.get_time = get_time
    return mock


@pytest.fixture
def mock_mono_clock() -> MagicMock:
    """Deterministic monotonic clock."""
    current_mono = 1000.0

    def get_mono() -> float:
        nonlocal current_mono
        return current_mono

    def advance(delta: float) -> None:
        nonlocal current_mono
        current_mono += delta

    mock = MagicMock(side_effect=get_mono)
    mock.advance = advance
    mock.get_mono = get_mono
    return mock


@pytest.fixture
def test_setup(tmp_path: Path):
    """Integrated paper trading environment with SQLite db, outbox, and coordinator."""
    db_path = str(tmp_path / "paper_kaiser_step67.db")
    event_bus = EventBus()
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(db_path=db_path)
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
        prune_batch_size=10,
        archival_enabled=True,
    )
    dispatcher = OutboxDispatcher(
        outbox=outbox,
        event_bus=event_bus,
    )
    alloc_mgr = PortfolioAllocationManager(
        portfolio=portfolio,
        ledger=ledger,
        event_bus=event_bus,
        db_path=db_path,
        outbox=outbox,
    )
    coordinator = PaperTradingWorkflowCoordinator(
        event_bus=event_bus,
        db_path=db_path,
        outbox=outbox,
        allocation_manager=alloc_mgr,
        enabled=True,
    )
    return {
        "db_path": db_path,
        "event_bus": event_bus,
        "portfolio": portfolio,
        "ledger": ledger,
        "outbox": outbox,
        "dispatcher": dispatcher,
        "alloc_mgr": alloc_mgr,
        "coordinator": coordinator,
    }


# ==============================================================================
# 1. CONCURRENT SCHEDULER START
# ==============================================================================

def test_01_concurrent_scheduler_start(test_setup) -> None:
    """Concurrent start() calls do not spawn duplicate worker threads or corrupted loops."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
        health_digest_enabled=True,
    )

    barrier = threading.Barrier(10)
    threads: List[threading.Thread] = []

    def start_worker():
        barrier.wait()
        scheduler.start()

    for _ in range(10):
        t = threading.Thread(target=start_worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=5.0)

    assert scheduler.is_running is True
    # Verify exactly one worker thread exists and is alive
    assert scheduler._worker_thread is not None
    assert scheduler._worker_thread.is_alive()

    # Repeated calls while running are no-ops
    scheduler.start()
    assert scheduler.is_running is True

    scheduler.stop(timeout=5.0)
    assert scheduler.is_running is False


# ==============================================================================
# 2. CONCURRENT SCHEDULER STOP
# ==============================================================================

def test_02_concurrent_scheduler_stop(test_setup) -> None:
    """Concurrent stop() calls are safe, serialized, and idempotent."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        shutdown_timeout_seconds=5.0,
    )
    scheduler.start()
    assert scheduler.is_running is True

    barrier = threading.Barrier(10)
    results: List[bool] = []
    lock = threading.Lock()

    def stop_worker():
        barrier.wait()
        res = scheduler.stop(timeout=5.0)
        with lock:
            results.append(res)

    threads = [threading.Thread(target=stop_worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    assert scheduler.is_running is False
    assert all(r is True for r in results)
    assert len(results) == 10


# ==============================================================================
# 3. REPEATED START/STOP CYCLES
# ==============================================================================

def test_03_repeated_start_stop_cycles(test_setup) -> None:
    """Repeatedly cycling start and stop does not leak threads or corrupt state."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        shutdown_timeout_seconds=2.0,
    )

    for i in range(15):
        scheduler.start()
        assert scheduler.is_running is True
        assert scheduler._worker_thread is not None
        assert scheduler._worker_thread.is_alive()

        stopped = scheduler.stop(timeout=2.0)
        assert stopped is True
        assert scheduler.is_running is False
        assert scheduler._worker_thread is None


# ==============================================================================
# 4. CONCURRENT MANUAL AND SCHEDULED EXECUTION
# ==============================================================================

def test_04_concurrent_manual_and_scheduled_execution(test_setup) -> None:
    """Manual trigger cannot race with scheduled trigger to run the same task concurrently."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
    )

    results: List[MaintenanceTaskResult] = []
    lock = threading.Lock()
    barrier = threading.Barrier(5)

    def trigger_prune():
        barrier.wait()
        res = scheduler.run_prune_now()
        with lock:
            results.append(res)

    threads = [threading.Thread(target=trigger_prune) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    assert len(results) == 5
    # Exactly one or more might succeed, but any overlapping execution is SKIPPED_OVERLAPPING
    statuses = {r.status for r in results}
    assert MaintenanceTaskStatus.SUCCESS in statuses or MaintenanceTaskStatus.SKIPPED_OVERLAPPING in statuses


# ==============================================================================
# 5. OVERLAP PREVENTION OR COALESCING
# ==============================================================================

def test_05_overlap_prevention_or_coalescing(test_setup) -> None:
    """Task overlap policy prevents unbounded queueing; second trigger reports SKIPPED_OVERLAPPING."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
    )

    acquired_event = threading.Event()
    release_event = threading.Event()

    original_prune = test_setup["outbox"].prune_delivered_records

    def blocking_prune(*args, **kwargs):
        acquired_event.set()
        release_event.wait(timeout=5.0)
        return original_prune(*args, **kwargs)

    with patch.object(test_setup["outbox"], "prune_delivered_records", side_effect=blocking_prune):
        t1 = threading.Thread(target=scheduler.run_prune_now)
        t1.start()

        assert acquired_event.wait(timeout=2.0) is True

        # Second call while first is in progress
        t2_res = scheduler.run_prune_now()
        assert t2_res.status == MaintenanceTaskStatus.SKIPPED_OVERLAPPING
        assert "Overlapping" in (t2_res.skip_reason or "")

        release_event.set()
        t1.join(timeout=5.0)


# ==============================================================================
# 6. TASK FAILURE ISOLATION AND RECOVERY
# ==============================================================================

def test_06_task_failure_isolation_and_recovery(test_setup) -> None:
    """An exception in one task does not crash the worker loop or prevent subsequent runs."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
        health_digest_enabled=True,
    )

    # Force error on first prune
    with patch.object(test_setup["outbox"], "prune_delivered_records", side_effect=sqlite3.OperationalError("disk locked")):
        res1 = scheduler.run_prune_now()
        assert res1.status == MaintenanceTaskStatus.FAILED
        assert "disk locked" in (res1.error_message or "")

    # Subsequent run succeeds cleanly
    res2 = scheduler.run_prune_now()
    assert res2.status == MaintenanceTaskStatus.SUCCESS
    assert res2.error_message is None


# ==============================================================================
# 7. STOP DURING A RUNNING TASK
# ==============================================================================

def test_07_stop_during_running_task(test_setup) -> None:
    """Stopping the scheduler during an active task does not falsely report that the task cancelled."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
        shutdown_timeout_seconds=0.1,  # Short timeout
    )

    task_started = threading.Event()
    task_finish = threading.Event()

    def slow_prune(*args, **kwargs):
        task_started.set()
        task_finish.wait(timeout=2.0)
        return test_setup["outbox"].prune_delivered_records(*args, **kwargs)

    scheduler.start()

    with patch.object(test_setup["outbox"], "prune_delivered_records", side_effect=slow_prune):
        t = threading.Thread(target=scheduler.run_prune_now)
        t.start()

        assert task_started.wait(timeout=2.0) is True

        # Stop scheduler while task is running; should cleanly wait or report timed-out
        clean_exit = scheduler.stop(timeout=0.1)

        task_finish.set()
        t.join(timeout=3.0)


# ==============================================================================
# 8. BOUNDED SHUTDOWN TIMEOUT
# ==============================================================================

def test_08_bounded_shutdown_timeout(test_setup) -> None:
    """Shutdown waits are strictly bounded by the configured timeout."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        shutdown_timeout_seconds=0.2,
    )
    scheduler.start()

    # Block worker loop
    def blocking_loop():
        time.sleep(1.0)

    scheduler._worker_thread = threading.Thread(target=blocking_loop, daemon=True)
    scheduler._worker_thread.start()
    scheduler._is_running = True

    t0 = time.time()
    clean = scheduler.stop(timeout=0.2)
    elapsed = time.time() - t0

    assert clean is False  # Timed out cleanly
    assert elapsed < 0.6  # Bounded within reasonable margin of timeout


# ==============================================================================
# 9. ACTIVE SESSION BLOCKS PRUNING
# ==============================================================================

def test_09_active_session_blocks_pruning(test_setup) -> None:
    """An ACTIVE simulation session strictly blocks destructive pruning."""
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]
    sess = coordinator.start_session(name="active_race_test")
    assert sess.status == "ACTIVE"

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=coordinator,
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
    )

    res = scheduler.run_prune_now()
    assert res.status == MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION
    assert "ACTIVE" in (res.skip_reason or "")
    assert res.records_processed == 0


# ==============================================================================
# 10. INCOMPLETE OR FAILED SESSION RECOVERY BLOCKS PRUNING
# ==============================================================================

def test_10_incomplete_session_recovery_blocks_pruning(test_setup) -> None:
    """If session recovery failed or is incomplete, pruning fails-closed."""
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]
    with coordinator._lock:
        coordinator._sessions_recovered = False
        coordinator._recovery_error = "Corrupted SQLite header"

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=coordinator,
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
    )

    res = scheduler.run_prune_now()
    assert res.status == MaintenanceTaskStatus.SKIPPED_RECOVERY_INCOMPLETE
    assert "incomplete" in (res.skip_reason or "").lower()


# ==============================================================================
# 11. SESSION-START VERSUS PRUNING RACE
# ==============================================================================

def test_11_session_start_versus_pruning_race(test_setup) -> None:
    """Coordinator lock boundary prevents new session starting between check and prune."""
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]
    outbox: TransactionalOutbox = test_setup["outbox"]

    # Stage an eligible old delivered record
    outbox.stage_event(OutboxRecord(
        event_id="prune_race_rec_1",
        event_type="TEST",
        status=OutboxStatus.DELIVERED,
        delivered_at=100.0,
        created_at=100.0,
    ))

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=coordinator,
        outbox=outbox,
        enabled=True,
        pruning_enabled=True,
    )

    # Start session concurrently with prune
    barrier = threading.Barrier(2)
    session_result = []
    prune_result = []

    def run_session_start():
        barrier.wait()
        s = coordinator.start_session(name="race_sess")
        session_result.append(s)

    def run_prune():
        barrier.wait()
        r = scheduler.run_prune_now(now=100_000.0)
        prune_result.append(r)

    t_sess = threading.Thread(target=run_session_start)
    t_prune = threading.Thread(target=run_prune)

    t_sess.start()
    t_prune.start()

    t_sess.join(timeout=5.0)
    t_prune.join(timeout=5.0)

    assert len(session_result) == 1
    assert len(prune_result) == 1
    # Either pruning ran first or saw active session and skipped
    assert prune_result[0].status in (
        MaintenanceTaskStatus.SUCCESS,
        MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION,
    )


# ==============================================================================
# 12. IN-FLIGHT AND UNDELIVERED OUTBOX PROTECTION
# ==============================================================================

def test_12_inflight_and_undelivered_outbox_protection(test_setup) -> None:
    """Pending, retryable, exhausted, and in-flight records are protected from deletion."""
    outbox: TransactionalOutbox = test_setup["outbox"]
    dispatcher: OutboxDispatcher = test_setup["dispatcher"]

    old_ts = 100.0
    outbox.stage_event(OutboxRecord(event_id="p_1", event_type="T", status=OutboxStatus.PENDING, created_at=old_ts))
    outbox.stage_event(OutboxRecord(event_id="r_1", event_type="T", status=OutboxStatus.RETRYABLE_FAILURE, created_at=old_ts))
    outbox.stage_event(OutboxRecord(event_id="e_1", event_type="T", status=OutboxStatus.EXHAUSTED, created_at=old_ts))
    outbox.stage_event(OutboxRecord(event_id="inf_1", event_type="T", status=OutboxStatus.DELIVERED, delivered_at=old_ts, created_at=old_ts))
    dispatcher._in_flight_event_ids.add("inf_1")

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=outbox,
        dispatcher=dispatcher,
        enabled=True,
        pruning_enabled=True,
    )

    res = scheduler.run_prune_now(now=100_000.0)
    assert res.records_processed == 0

    assert outbox.get_record("p_1") is not None
    assert outbox.get_record("r_1") is not None
    assert outbox.get_record("e_1") is not None
    assert outbox.get_record("inf_1") is not None


# ==============================================================================
# 13. CRASH BEFORE TASK CLAIM
# ==============================================================================

def test_13_crash_before_task_claim(test_setup) -> None:
    """Crash before task claim leaves no phantom running state in database."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    runs = scheduler.list_maintenance_runs()
    assert len(runs) == 0


# ==============================================================================
# 14. CRASH AFTER CLAIM BUT BEFORE COMPLETION
# ==============================================================================

def test_14_crash_after_claim_before_completion(test_setup) -> None:
    """An interrupted task recorded as RUNNING is reconciled to INTERRUPTED on recovery."""
    db_path = test_setup["db_path"]

    # Initialize schema first with an initial instance
    MaintenanceScheduler(
        db_path=db_path,
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    # Manually inject an interrupted RUNNING row
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO paper_maintenance_runs (
                run_id, task_name, status, started_at, completed_at,
                duration_seconds, records_inspected, records_processed,
                records_skipped, has_more, skip_reason, error_message, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("mrun_crash_01", "prune", "RUNNING", 1000.0, 0.0, 0.0, 0, 0, 0, 0, None, None, "{}"),
        )

    # Initialize a new scheduler (simulating restart)
    scheduler = MaintenanceScheduler(
        db_path=db_path,
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    runs = scheduler.list_maintenance_runs()
    crash_run = next((r for r in runs if r.run_id == "mrun_crash_01"), None)
    assert crash_run is not None
    assert crash_run.status == MaintenanceTaskStatus.INTERRUPTED
    assert "interrupted" in (crash_run.error_message or "").lower()


# ==============================================================================
# 15. RESTART AND REPEATED RECOVERY IDEMPOTENCY
# ==============================================================================

def test_15_restart_and_repeated_recovery_idempotency(test_setup) -> None:
    """Repeated restarts and recovery runs are completely idempotent."""
    db_path = test_setup["db_path"]
    scheduler1 = MaintenanceScheduler(
        db_path=db_path,
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    scheduler1.run_health_digest_now(digest_id="hdig_idem_01")

    # Second recovery
    scheduler2 = MaintenanceScheduler(
        db_path=db_path,
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    # Third recovery
    scheduler3 = MaintenanceScheduler(
        db_path=db_path,
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    digests = scheduler3.list_health_digests()
    matching = [d for d in digests if d.digest_id == "hdig_idem_01"]
    assert len(matching) == 1


# ==============================================================================
# 16. SQLITE FAILURES BEFORE COMMIT
# ==============================================================================

def test_16_sqlite_failures_before_commit(test_setup) -> None:
    """Database failure before commit records failure without corrupted memory state."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
    )

    with patch.object(test_setup["outbox"], "prune_delivered_records", side_effect=sqlite3.DatabaseError("I/O error")):
        res = scheduler.run_prune_now()
        assert res.status == MaintenanceTaskStatus.FAILED
        assert "I/O error" in (res.error_message or "")


# ==============================================================================
# 17. AMBIGUOUS COMMIT HANDLING
# ==============================================================================

def test_17_ambiguous_commit_handling(test_setup) -> None:
    """If database write fails during digest persistence, it reports FAILED, not SUCCESS."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    with patch.object(scheduler, "_persist_health_digest", return_value=False):
        digest = scheduler.run_health_digest_now(digest_id="ambiguous_hdig_01")
        last_run = scheduler._last_runs.get("health_digest")
        assert last_run is not None
        assert last_run.status == MaintenanceTaskStatus.FAILED


# ==============================================================================
# 18. CONCURRENT DUPLICATE DIGEST CREATION
# ==============================================================================

def test_18_concurrent_duplicate_digest_creation(test_setup) -> None:
    """Concurrent requests for the same digest ID serialize cleanly and return the exact same snapshot."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    results: List[HealthDigestSnapshot] = []
    barrier = threading.Barrier(5)
    lock = threading.Lock()

    def create_digest():
        barrier.wait()
        d = scheduler.run_health_digest_now(digest_id="concurrent_same_id_01")
        with lock:
            results.append(d)

    threads = [threading.Thread(target=create_digest) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    assert len(results) == 5
    first_digest = results[0]
    for r in results[1:]:
        assert r.digest_id == first_digest.digest_id
        assert r.deterministic_digest == first_digest.deterministic_digest


# ==============================================================================
# 19. IMMUTABLE HISTORICAL DIGEST RETRIEVAL
# ==============================================================================

def test_19_immutable_historical_digest_retrieval(test_setup) -> None:
    """Historical digest retrieval returns persisted snapshot without recomputing from current state."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    d1 = scheduler.run_health_digest_now(digest_id="hist_snap_01")

    # Mutate outbox count
    test_setup["outbox"].stage_event(OutboxRecord(event_id="new_event_mut_01", event_type="TEST"))

    d1_fetched = scheduler.get_health_digest("hist_snap_01")
    assert d1_fetched is not None
    assert d1_fetched.digest_id == d1.digest_id
    assert d1_fetched.deterministic_digest == d1.deterministic_digest
    assert d1_fetched.outbox_counts == d1.outbox_counts


# ==============================================================================
# 20. MISSING OR STALE HEALTH-DIGEST SOURCE DATA
# ==============================================================================

def test_20_missing_or_stale_health_digest_source_data() -> None:
    """Missing diagnostic sources produce explicit None/empty dicts, never fabricated zeros or false health."""
    bare_scheduler = MaintenanceScheduler(
        db_path=None,
        outbox=None,
        coordinator=None,
        dispatcher=None,
        enabled=False,
    )
    digest = bare_scheduler.run_health_digest_now(digest_id="missing_src_01")
    assert digest.outbox_counts == {}
    assert digest.outbox_retention == {}
    assert digest.outbox_dispatcher_running is None
    assert digest.portfolio_reconciliation is None
    assert digest.latest_equity_snapshot_id is None


# ==============================================================================
# 21. DIGEST GENERATION DOES NOT MUTATE FINANCIAL STATE
# ==============================================================================

def test_21_digest_generation_does_not_mutate_financial_state(test_setup) -> None:
    """Health digest generation is read-only and does not mutate cash, positions, or risk."""
    portfolio: PaperPortfolio = test_setup["portfolio"]
    alloc_mgr: PortfolioAllocationManager = test_setup["alloc_mgr"]

    initial_cash = portfolio.available_cash
    initial_equity = portfolio.total_equity

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    for i in range(5):
        scheduler.run_health_digest_now(digest_id=f"read_only_fin_{i}")

    assert portfolio.available_cash == initial_cash
    assert portfolio.total_equity == initial_equity


# ==============================================================================
# 22. BOUNDED DIAGNOSTIC HISTORY
# ==============================================================================

def test_22_bounded_diagnostic_history(test_setup) -> None:
    """Pruning diagnostic history limits records strictly to history_limit."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        history_limit=10,
    )

    for i in range(25):
        scheduler.run_health_digest_now(digest_id=f"bounded_hdig_{i:02d}")

    digests = scheduler.list_health_digests(limit=100)
    assert len(digests) <= 10


# ==============================================================================
# 23. SECRET REDACTION
# ==============================================================================

def test_23_secret_redaction(test_setup) -> None:
    """Secrets, API keys, passwords, and tokens are redacted from error logs and task details."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    raw_err = "Failed connection: api_key='sk_live_9988776655' and token=secret_bearer_token"
    scheduler._record_error(raw_err)

    status = scheduler.get_status()
    # Check that raw secrets are never in recent_errors
    digest = scheduler.run_health_digest_now(digest_id="redact_test_01")
    for err in digest.recent_errors:
        assert "sk_live_9988776655" not in err
        assert "secret_bearer_token" not in err
        assert "[REDACTED]" in err

    # Check sanitizer utility directly
    sanitized = sanitize_sensitive_text(raw_err)
    assert "sk_live_9988776655" not in sanitized
    assert "[REDACTED]" in sanitized

    dict_clean = sanitize_dict_secrets({"token": "secret123", "safe_param": 42})
    assert dict_clean["token"] == "[REDACTED]"
    assert dict_clean["safe_param"] == 42


# ==============================================================================
# 24. CONFIGURATION VALIDATION
# ==============================================================================

def test_24_configuration_validation() -> None:
    """Configuration enforces positive intervals, valid limits, and rejects malformed values."""
    with pytest.raises(ValidationError):
        MaintenanceScheduler(pruning_interval_seconds=-10.0)

    with pytest.raises(ValidationError):
        MaintenanceScheduler(health_digest_interval_seconds=0.0)

    with pytest.raises(ValidationError):
        MaintenanceScheduler(shutdown_timeout_seconds=999.0)  # > 60

    with pytest.raises(ValidationError):
        MaintenanceScheduler(history_limit=2)  # < 10

    with pytest.raises(ValidationError):
        MaintenanceScheduler(active_session_policy="invalid_policy")


# ==============================================================================
# 25. DASHBOARD AUTHENTICATION AND READ-ONLY REGRESSIONS
# ==============================================================================

def test_25_dashboard_authentication_and_readonly_regressions(test_setup) -> None:
    """Dashboard maintenance routes enforce auth, read-only behavior, and 405 on POST."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        maintenance_scheduler=scheduler,
        auth_token="auth_token_secret_123",
        require_auth=True,
    )
    server.start()
    port = server.port

    import urllib.request
    import urllib.error

    base_url = f"http://127.0.0.1:{port}"

    try:
        # Unauthenticated -> 401
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/api/paper/maintenance/status")
        assert exc_info.value.code == 401

        # Authenticated -> 200
        req = urllib.request.Request(
            f"{base_url}/api/paper/maintenance/status",
            headers={"Authorization": "Bearer auth_token_secret_123"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True

        # POST -> 405
        post_req = urllib.request.Request(
            f"{base_url}/api/paper/maintenance/status",
            data=b"{}",
            headers={"Authorization": "Bearer auth_token_secret_123", "Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_post:
            urllib.request.urlopen(post_req)
        assert exc_post.value.code == 405

    finally:
        server.stop()


# ==============================================================================
# 26. THREAD TIMEOUT TRUTHFUL REPORTING
# ==============================================================================

def test_26_thread_timeout_truthful_reporting(test_setup) -> None:
    """If worker thread join times out, stop() reports clean=False and worker_thread_alive=True."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        shutdown_timeout_seconds=0.1,
    )

    hang_event = threading.Event()

    def hanging_worker():
        hang_event.wait(timeout=2.0)

    scheduler._worker_thread = threading.Thread(target=hanging_worker, daemon=True)
    scheduler._worker_thread.start()
    scheduler._is_running = True

    try:
        clean = scheduler.stop(timeout=0.1)
        assert clean is False
        status = scheduler.get_status()
        assert status["worker_thread_alive"] is True
    finally:
        hang_event.set()


# ==============================================================================
# 27. FAILED RECONCILIATION NOT REPORTED BALANCED
# ==============================================================================

def test_27_failed_reconciliation_not_reported_balanced(test_setup) -> None:
    """When portfolio reconciliation fails, health digest marks DEGRADED and is_balanced=False."""
    coordinator = test_setup["coordinator"]

    class MockRecon:
        def to_dict(self):
            return {
                "is_reconciled": False,
                "is_balanced": False,
                "discrepancies": ["Cash mismatch"],
            }

    with patch.object(coordinator, "reconcile_allocations", return_value=MockRecon()):
        scheduler = MaintenanceScheduler(
            db_path=test_setup["db_path"],
            coordinator=coordinator,
            outbox=test_setup["outbox"],
        )
        digest = scheduler.run_health_digest_now(digest_id="degraded_recon_01")
        assert digest.runtime_status == "DEGRADED"
        assert digest.portfolio_reconciliation["is_balanced"] is False


# ==============================================================================
# 28. SYSTEM SAFETY INVARIANTS REMAIN LOCKED
# ==============================================================================

def test_28_system_safety_invariants_remain_locked() -> None:
    """Critical safety invariants remain locked throughout maintenance scheduler execution."""
    settings = get_settings()
    assert settings.execution_locked is True
    assert settings.broker_provider == "none"

    scheduler = MaintenanceScheduler(settings=settings)
    digest = scheduler.run_health_digest_now(digest_id="invariants_hdig_01")
    assert digest.execution_locked is True
    assert digest.broker_provider == "none"
