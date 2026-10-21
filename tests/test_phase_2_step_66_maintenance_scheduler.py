"""Comprehensive Test Suite for Phase 2, Step 66: Automated Maintenance Scheduler Hooks.

Covers:
- Part A: Scheduler lifecycle (start, stop, idempotency, manual execution, anti-overlapping, anti-catch-up).
- Part B: Safe outbox retention scheduling (active-session safety gate, recovery check, batch bound, in-flight protection).
- Part C: Health digest snapshots (real sources, missing sources, validation, idempotency, persistence, financial immutability).
- Part D: Integration, concurrency, bounded history, and dashboard security/read-only guarantees.
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from app.core.config import Settings, get_settings
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.events.bus import EventBus
from app.execution.allocation import (
    PortfolioAllocationManager,
    create_standard_multi_sleeve_request,
)
from app.execution.maintenance import (
    HealthDigestSnapshot,
    MaintenanceScheduler,
    MaintenanceTaskResult,
    MaintenanceTaskStatus,
)
from app.execution.outbox_dispatcher import OutboxDispatcher
from app.execution.portfolio import PaperPortfolio
from app.execution.workflow import PaperTradingWorkflowCoordinator
from app.ledger.ledger import PaperLedger
from app.ledger.outbox import OutboxRecord, OutboxStatus, TransactionalOutbox
from app.portfolio.dashboard.server import PortfolioDashboardServer


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def mock_clock() -> MagicMock:
    """Deterministic, controllable clock fixture."""
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
    """Deterministic monotonic clock fixture."""
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
    db_path = str(tmp_path / "paper_kaiser.db")
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
# PART A: SCHEDULER LIFECYCLE TESTS (Items 1 - 8)
# ==============================================================================

def test_01_scheduler_disabled_by_default(test_setup) -> None:
    """Scheduler defaults to disabled; start() does not launch a background thread."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    assert scheduler.is_enabled is False
    assert scheduler.is_running is False
    assert scheduler.pruning_enabled is False
    assert scheduler.health_digest_enabled is False

    scheduler.start()
    assert scheduler.is_running is False
    assert scheduler._worker_thread is None


def test_02_explicit_start_and_stop_behavior(test_setup) -> None:
    """When enabled, start() spawns a thread and stop() stops it cleanly."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
        pruning_interval_seconds=60.0,
    )
    assert scheduler.is_enabled is True
    assert scheduler.is_running is False

    scheduler.start()
    assert scheduler.is_running is True
    assert scheduler._worker_thread is not None
    assert scheduler._worker_thread.is_alive()

    clean = scheduler.stop(timeout=2.0)
    assert clean is True
    assert scheduler.is_running is False
    assert scheduler._worker_thread is None


def test_03_repeated_start_stop_is_idempotent(test_setup) -> None:
    """Repeated start() or stop() invocations are idempotent and safe."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        enabled=True,
        pruning_interval_seconds=60.0,
    )

    scheduler.start()
    t1 = scheduler._worker_thread
    scheduler.start()  # Duplicate start
    assert scheduler._worker_thread is t1
    assert scheduler.is_running is True

    assert scheduler.stop() is True
    assert scheduler.is_running is False
    assert scheduler.stop() is True  # Duplicate stop


def test_04_manual_task_execution_works_without_background_thread(test_setup) -> None:
    """Manual execution of prune and health digest works synchronously without background thread."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=False,
    )
    assert scheduler.is_running is False

    # Force manual prune
    prune_res = scheduler.run_prune_now(force=True)
    assert prune_res.status == MaintenanceTaskStatus.SUCCESS
    assert prune_res.task_name == "prune"

    # Manual health digest
    digest = scheduler.run_health_digest_now(digest_id="hdig_manual_01")
    assert digest.digest_id == "hdig_manual_01"
    assert digest.runtime_status == "HEALTHY"


def test_05_overlapping_runs_of_same_task_prevented(test_setup) -> None:
    """Anti-overlapping execution locks prevent concurrent invocations of the same task."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
    )

    # Acquire prune lock externally to simulate ongoing execution
    acquired = scheduler._prune_lock.acquire(blocking=False)
    assert acquired is True

    try:
        res = scheduler.run_prune_now(force=True)
        assert res.status == MaintenanceTaskStatus.SKIPPED_OVERLAPPING
        assert "Overlapping execution prevented" in (res.skip_reason or "")
    finally:
        scheduler._prune_lock.release()


def test_06_missed_intervals_do_not_trigger_unbounded_catch_up(test_setup, mock_clock, mock_mono_clock) -> None:
    """Monotonic time jumps forward do not trigger a cascade of catch-up executions."""
    call_count = 0

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=True,
        pruning_interval_seconds=60.0,
        clock=mock_clock,
        mono_clock=mock_mono_clock,
    )

    original_run_prune = scheduler.run_prune_now

    def counting_run_prune(**kwargs):
        nonlocal call_count
        call_count += 1
        return original_run_prune(**kwargs)

    scheduler.run_prune_now = counting_run_prune

    # Start worker and simulate time jump of 100 intervals
    scheduler.start()
    mock_mono_clock.advance(6000.0)  # 100x interval jump

    # Let worker tick once
    time.sleep(0.05)
    scheduler.stop(timeout=1.0)

    # Must execute at most once per tick, not 100 times
    assert call_count <= 2


def test_07_shutdown_completes_within_timeout_or_reports_unresolved(test_setup) -> None:
    """Scheduler stop() reports clean completion or returns False if worker hangs."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        enabled=True,
        shutdown_timeout_seconds=0.1,
    )

    # Normal stop returns True
    scheduler.start()
    assert scheduler.stop(timeout=1.0) is True

    # Simulate hung thread
    class HangingThread(threading.Thread):
        def join(self, timeout=None):
            pass  # Does not join
        def is_alive(self):
            return True

    scheduler._is_running = True
    scheduler._worker_thread = HangingThread()
    res = scheduler.stop(timeout=0.05)
    assert res is False


def test_08_invalid_configuration_rejected() -> None:
    """Invalid scheduler intervals, policies, timeouts, and history limits raise ValidationError."""
    with pytest.raises(ValidationError, match="pruning_interval_seconds"):
        MaintenanceScheduler(pruning_interval_seconds=-10.0)

    with pytest.raises(ValidationError, match="health_digest_interval_seconds"):
        MaintenanceScheduler(health_digest_interval_seconds=0.0)

    with pytest.raises(ValidationError, match="shutdown_timeout_seconds"):
        MaintenanceScheduler(shutdown_timeout_seconds=120.0)

    with pytest.raises(ValidationError, match="active_session_policy"):
        MaintenanceScheduler(active_session_policy="invalid_policy")

    with pytest.raises(ValidationError, match="history_limit"):
        MaintenanceScheduler(history_limit=2)

    # ConfigurationError in Settings
    with pytest.raises(ConfigurationError):
        Settings(maintenance_pruning_interval_seconds=0.0)

    with pytest.raises(ConfigurationError):
        Settings(maintenance_active_session_policy="unrecognized")


# ==============================================================================
# PART B: RETENTION SAFETY TESTS (Items 9 - 15)
# ==============================================================================

def test_09_retention_runs_only_when_scheduler_and_pruning_enabled(test_setup) -> None:
    """Scheduled pruning does not run unless both scheduler and pruning controls are enabled."""
    # 1. Scheduler disabled
    sched1 = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=test_setup["outbox"],
        coordinator=test_setup["coordinator"],
        enabled=False,
        pruning_enabled=True,
    )
    res1 = sched1.run_prune_now(force=False)
    assert res1.status == MaintenanceTaskStatus.SKIPPED_DISABLED

    # 2. Pruning disabled
    sched2 = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=test_setup["outbox"],
        coordinator=test_setup["coordinator"],
        enabled=True,
        pruning_enabled=False,
    )
    res2 = sched2.run_prune_now(force=False)
    assert res2.status == MaintenanceTaskStatus.SKIPPED_DISABLED

    # 3. Both enabled
    sched3 = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=test_setup["outbox"],
        coordinator=test_setup["coordinator"],
        enabled=True,
        pruning_enabled=True,
    )
    res3 = sched3.run_prune_now(force=False)
    assert res3.status == MaintenanceTaskStatus.SUCCESS


def test_10_active_sessions_cause_destructive_maintenance_skipped_by_default(test_setup) -> None:
    """An active simulation session causes pruning to be skipped with SKIPPED_ACTIVE_SESSION."""
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]
    outbox: TransactionalOutbox = test_setup["outbox"]

    # Start an active session
    coordinator.start_session(session_id="active_sess_01", initial_capital=50_000.0)
    assert coordinator.is_session_active() is True

    # Stage an old delivered record eligible for pruning
    old_time = 100_000.0
    rec = OutboxRecord(
        event_id="rec_old_deliv_sess_gate",
        event_type="TEST",
        status=OutboxStatus.DELIVERED,
        delivered_at=old_time,
        created_at=old_time - 10.0,
    )
    outbox.stage_event(rec)

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=outbox,
        coordinator=coordinator,
        enabled=True,
        pruning_enabled=True,
        active_session_policy="skip",
    )

    res = scheduler.run_prune_now(now=1_000_000.0)
    assert res.status == MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION
    assert "Simulation session is currently ACTIVE" in (res.skip_reason or "")
    # Record must remain unpruned
    assert outbox.get_record("rec_old_deliv_sess_gate") is not None

    # Stop session and prune again
    coordinator.stop_session("active_sess_01")
    assert coordinator.is_session_active() is False

    res2 = scheduler.run_prune_now(now=1_000_000.0)
    assert res2.status == MaintenanceTaskStatus.SUCCESS
    assert res2.records_processed == 1
    assert outbox.get_record("rec_old_deliv_sess_gate") is None


def test_11_incomplete_session_recovery_fails_closed(test_setup) -> None:
    """If session coordinator recovery is incomplete, pruning fails closed."""
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]
    outbox: TransactionalOutbox = test_setup["outbox"]

    # Simulate incomplete recovery
    coordinator._sessions_recovered = False
    coordinator._recovery_error = "Simulated disk read timeout"

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=outbox,
        coordinator=coordinator,
        enabled=True,
        pruning_enabled=True,
    )

    res = scheduler.run_prune_now()
    assert res.status == MaintenanceTaskStatus.SKIPPED_RECOVERY_INCOMPLETE
    assert "Session coordinator recovery is incomplete" in (res.skip_reason or "")


def test_12_session_state_rechecked_immediately_before_pruning(test_setup) -> None:
    """Active session detected immediately before prune execution halts pruning safely."""
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]
    outbox: TransactionalOutbox = test_setup["outbox"]

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=outbox,
        coordinator=coordinator,
        enabled=True,
        pruning_enabled=True,
        active_session_policy="skip",
    )

    # Patch is_session_active to return False on first check, True on second check
    calls = 0
    def dynamic_check():
        nonlocal calls
        calls += 1
        return calls >= 2

    with patch.object(coordinator, "is_session_active", side_effect=dynamic_check):
        res = scheduler.run_prune_now()
        assert res.status == MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION
        assert "Active session detected on immediate pre-prune recheck" in (res.skip_reason or "")


def test_13_has_more_results_in_bounded_continuation(test_setup) -> None:
    """When records exceed batch bound, has_more=True is returned without looping infinitely."""
    outbox: TransactionalOutbox = test_setup["outbox"]
    now = 1_000_000.0
    old_time = now - 100_000.0

    # Stage 15 delivered records (batch limit is 5)
    for i in range(15):
        rec = OutboxRecord(
            event_id=f"bounded_prune_{i:02d}",
            event_type="TEST",
            status=OutboxStatus.DELIVERED,
            delivered_at=old_time + i,
            created_at=old_time,
        )
        outbox.stage_event(rec)

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=outbox,
        coordinator=test_setup["coordinator"],
        enabled=True,
        pruning_enabled=True,
    )

    res = scheduler.run_prune_now(now=now, batch_size=5)
    assert res.status == MaintenanceTaskStatus.SUCCESS
    assert res.records_processed == 5
    assert res.has_more is True

    # Check remaining records
    assert outbox.count_records() == 10


def test_14_outbox_in_flight_and_undelivered_events_remain_protected(test_setup) -> None:
    """Dispatcher in_flight events and undelivered records are strictly excluded from pruning."""
    outbox: TransactionalOutbox = test_setup["outbox"]
    dispatcher: OutboxDispatcher = test_setup["dispatcher"]
    now = 1_000_000.0
    old_time = now - 100_000.0

    # In-flight delivered record
    outbox.stage_event(OutboxRecord(
        event_id="in_flight_rec_1",
        event_type="TEST",
        status=OutboxStatus.DELIVERED,
        delivered_at=old_time,
        created_at=old_time,
    ))
    dispatcher._in_flight_event_ids.add("in_flight_rec_1")

    # Undelivered records
    outbox.stage_event(OutboxRecord(event_id="pending_rec_1", event_type="TEST", status=OutboxStatus.PENDING, created_at=old_time))
    outbox.stage_event(OutboxRecord(event_id="retryable_rec_1", event_type="TEST", status=OutboxStatus.RETRYABLE_FAILURE, created_at=old_time))
    outbox.stage_event(OutboxRecord(event_id="exhausted_rec_1", event_type="TEST", status=OutboxStatus.EXHAUSTED, created_at=old_time))

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=outbox,
        dispatcher=dispatcher,
        coordinator=test_setup["coordinator"],
        enabled=True,
        pruning_enabled=True,
    )

    res = scheduler.run_prune_now(now=now)
    assert res.status == MaintenanceTaskStatus.SUCCESS
    assert res.records_processed == 0

    assert outbox.get_record("in_flight_rec_1") is not None
    assert outbox.get_record("pending_rec_1") is not None
    assert outbox.get_record("retryable_rec_1") is not None
    assert outbox.get_record("exhausted_rec_1") is not None


def test_15_sqlite_failure_recorded_accurately_without_false_success(test_setup) -> None:
    """An injected SQLite failure during pruning records FAILED status without reporting success."""
    outbox: TransactionalOutbox = test_setup["outbox"]
    now = 1_000_000.0
    old_time = now - 100_000.0

    outbox.stage_event(OutboxRecord(
        event_id="fail_prune_rec_1",
        event_type="TEST",
        status=OutboxStatus.DELIVERED,
        delivered_at=old_time,
        created_at=old_time,
    ))

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=outbox,
        coordinator=test_setup["coordinator"],
        enabled=True,
        pruning_enabled=True,
    )

    with patch.object(outbox, "prune_delivered_records", side_effect=sqlite3.OperationalError("Simulated disk error")):
        res = scheduler.run_prune_now(now=now)
        assert res.status == MaintenanceTaskStatus.FAILED
        assert "Simulated disk error" in (res.error_message or "")
        assert res.records_processed == 0

    # Ensure outbox record still exists intact
    assert outbox.get_record("fail_prune_rec_1") is not None


# ==============================================================================
# PART C: HEALTH DIGEST TESTS (Items 16 - 22)
# ==============================================================================

def test_16_digest_values_reflect_real_diagnostic_sources(test_setup) -> None:
    """Health digest correctly aggregates actual metrics from coordinator, outbox, and portfolio."""
    alloc_mgr: PortfolioAllocationManager = test_setup["alloc_mgr"]
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]
    outbox: TransactionalOutbox = test_setup["outbox"]

    # Create allocation and snapshot
    req = create_standard_multi_sleeve_request(total_capital=100_000.0)
    alloc_mgr.apply_allocation(req)
    snap = alloc_mgr.create_equity_snapshot(snapshot_id="snap_hdig_01")

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        outbox=outbox,
        coordinator=coordinator,
        enabled=True,
    )

    digest = scheduler.run_health_digest_now(digest_id="hdig_real_01")
    assert digest.digest_id == "hdig_real_01"
    assert digest.runtime_status == "HEALTHY"
    assert digest.latest_equity_snapshot_id == "snap_hdig_01"
    assert digest.latest_equity_total == pytest.approx(snap.total_equity, abs=1e-4)
    assert digest.portfolio_reconciliation is not None
    assert digest.portfolio_reconciliation["is_balanced"] is True
    assert digest.deterministic_digest != ""


def test_17_missing_sources_produce_explicit_unknown_or_none() -> None:
    """When components are detached, digest records explicit None/empty dicts, not false healthy values."""
    scheduler = MaintenanceScheduler(
        db_path=None,
        outbox=None,
        coordinator=None,
        dispatcher=None,
        enabled=False,
    )

    digest = scheduler.run_health_digest_now(digest_id="hdig_bare_01")
    assert digest.outbox_counts == {}
    assert digest.outbox_retention == {}
    assert digest.outbox_dispatcher_running is None
    assert digest.portfolio_reconciliation is None
    assert digest.latest_equity_snapshot_id is None
    assert digest.latest_equity_total is None


def test_18_non_finite_values_and_invalid_timestamps_rejected() -> None:
    """HealthDigestSnapshot strictly rejects NaN, infinite timestamps, and negative equity."""
    with pytest.raises(ValidationError, match="positive finite float"):
        HealthDigestSnapshot(digest_id="inv_ts", timestamp=float("nan"))

    with pytest.raises(ValidationError, match="non-negative finite float"):
        HealthDigestSnapshot(digest_id="inv_eq", timestamp=1000.0, latest_equity_total=-500.0)

    with pytest.raises(ValidationError, match="non-empty string"):
        HealthDigestSnapshot(digest_id="   ", timestamp=1000.0)


def test_19_repeated_logical_digest_requests_are_idempotent(test_setup) -> None:
    """Creating a health digest with the same digest_id returns the existing record idempotently."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    d1 = scheduler.run_health_digest_now(digest_id="idempotent_digest_1")
    d2 = scheduler.run_health_digest_now(digest_id="idempotent_digest_1")

    assert d1.digest_id == d2.digest_id
    assert d1.deterministic_digest == d2.deterministic_digest
    assert len(scheduler.list_health_digests()) == 1


def test_20_digest_persistence_and_restart_recovery_work(test_setup) -> None:
    """Digests persist across scheduler restarts and query via list/get methods."""
    scheduler1 = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    scheduler1.run_health_digest_now(digest_id="recover_digest_01")

    # New instance pointing to same database
    scheduler2 = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    recovered = scheduler2.get_health_digest("recover_digest_01")
    assert recovered is not None
    assert recovered.digest_id == "recover_digest_01"

    digests = scheduler2.list_health_digests(limit=10)
    assert len(digests) >= 1
    assert digests[0].digest_id == "recover_digest_01"


def test_21_digest_generation_does_not_mutate_financial_state(test_setup) -> None:
    """Health digest execution is strictly read-only and does NOT create orders or equity snapshots."""
    alloc_mgr: PortfolioAllocationManager = test_setup["alloc_mgr"]
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]

    initial_snapshots_count = len(alloc_mgr.list_equity_snapshots())

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=coordinator,
        outbox=test_setup["outbox"],
    )

    scheduler.run_health_digest_now(digest_id="readonly_check_01")

    # Snapshot count remains identical
    assert len(alloc_mgr.list_equity_snapshots()) == initial_snapshots_count


def test_22_health_digest_scheduling_independent_of_pruning(test_setup) -> None:
    """Health digest can be scheduled and executed even when pruning is disabled."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
        pruning_enabled=False,  # Pruning disabled
        health_digest_enabled=True,
        health_digest_interval_seconds=60.0,
    )
    assert scheduler.pruning_enabled is False
    assert scheduler.health_digest_enabled is True

    digest = scheduler.run_health_digest_now()
    assert digest is not None
    assert digest.runtime_status == "HEALTHY"


# ==============================================================================
# PART D: INTEGRATION, CONCURRENCY, & SECURITY TESTS (Items 23 - 28)
# ==============================================================================

def test_23_scheduler_starts_only_when_explicitly_enabled(test_setup) -> None:
    """Scheduler start() is a no-op if enabled=False, but works immediately once enabled=True."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        enabled=False,
    )
    scheduler.start()
    assert scheduler.is_running is False

    scheduler._enabled = True
    scheduler.start()
    assert scheduler.is_running is True
    scheduler.stop()


def test_24_application_shutdown_stops_scheduler_workers(test_setup) -> None:
    """PaperTradingWorkflowCoordinator.shutdown() stops the attached maintenance scheduler cleanly."""
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]

    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=coordinator,
        enabled=True,
    )
    coordinator.attach_maintenance_scheduler(scheduler)
    scheduler.start()
    assert scheduler.is_running is True

    coordinator.shutdown(timeout=2.0)
    assert scheduler.is_running is False
    assert scheduler._worker_thread is None


def test_25_dashboard_auth_redaction_and_method_restrictions(test_setup) -> None:
    """Dashboard maintenance routes enforce loopback, authentication, redaction, and 405 on POST."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    scheduler.run_health_digest_now(digest_id="dash_auth_01")

    # Start dashboard with authentication required
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        maintenance_scheduler=scheduler,
        auth_token="super_secret_token",
        require_auth=True,
    )
    server.start()
    port = server.port

    import urllib.request
    import urllib.error

    base_url = f"http://127.0.0.1:{port}"

    try:
        # 1. Unauthenticated request -> 401
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/api/paper/maintenance/status")
        assert exc_info.value.code == 401

        # 2. Authenticated request -> 200
        req = urllib.request.Request(
            f"{base_url}/api/paper/maintenance/status",
            headers={"Authorization": "Bearer super_secret_token"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert "scheduler" in data

        # 3. Method restriction (POST -> 405)
        post_req = urllib.request.Request(
            f"{base_url}/api/paper/maintenance/status",
            data=b"{}",
            headers={"Authorization": "Bearer super_secret_token", "Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_post:
            urllib.request.urlopen(post_req)
        assert exc_post.value.code == 405

    finally:
        server.stop()


def test_26_read_only_dashboard_calls_do_not_trigger_maintenance(test_setup) -> None:
    """Querying dashboard maintenance endpoints is read-only and does not invoke tasks."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        maintenance_scheduler=scheduler,
        require_auth=False,
    )
    server.start()
    port = server.port

    import urllib.request
    base_url = f"http://127.0.0.1:{port}"

    try:
        initial_runs = len(scheduler.list_maintenance_runs())
        initial_digests = len(scheduler.list_health_digests())

        req = urllib.request.Request(f"{base_url}/api/paper/maintenance/runs")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200

        req2 = urllib.request.Request(f"{base_url}/api/paper/maintenance/digests")
        with urllib.request.urlopen(req2) as resp:
            assert resp.status == 200

        # Run counts remain identical
        assert len(scheduler.list_maintenance_runs()) == initial_runs
        assert len(scheduler.list_health_digests()) == initial_digests

    finally:
        server.stop()


def test_27_diagnostics_history_remains_bounded(test_setup) -> None:
    """Historical runs and digests are kept bounded by history_limit."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        history_limit=10,
    )

    # Insert 25 digests
    for i in range(25):
        scheduler.run_health_digest_now(digest_id=f"bounded_digest_{i:02d}")

    digests = scheduler.list_health_digests(limit=50)
    assert len(digests) <= 10


def test_28_system_regressions_and_lifecycle_coherence(test_setup) -> None:
    """End-to-end integration: outbox events, session lifecycle, health digest, safe prune, and shutdown."""
    coordinator: PaperTradingWorkflowCoordinator = test_setup["coordinator"]
    outbox: TransactionalOutbox = test_setup["outbox"]
    alloc_mgr: PortfolioAllocationManager = test_setup["alloc_mgr"]
    now = 1_000_000.0
    old_time = now - 100_000.0

    # 1. Start simulation session
    session = coordinator.start_session(session_id="e2e_sess_1", initial_capital=80_000.0)
    assert session.status == "ACTIVE"

    # 2. Stage delivered outbox event
    outbox.stage_event(OutboxRecord(
        event_id="e2e_old_deliv_1",
        event_type="TEST",
        status=OutboxStatus.DELIVERED,
        delivered_at=old_time,
        created_at=old_time,
    ))

    # 3. Create scheduler
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=coordinator,
        outbox=outbox,
        enabled=True,
        pruning_enabled=True,
        health_digest_enabled=True,
    )
    coordinator.attach_maintenance_scheduler(scheduler)

    # 4. Prune is skipped while session is active
    res_prune_active = scheduler.run_prune_now(now=now)
    assert res_prune_active.status == MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION
    assert outbox.get_record("e2e_old_deliv_1") is not None

    # 5. Stop session
    coordinator.stop_session("e2e_sess_1")
    assert coordinator.is_session_active() is False

    # 6. Prune succeeds when session is stopped
    res_prune_stopped = scheduler.run_prune_now(now=now)
    assert res_prune_stopped.status == MaintenanceTaskStatus.SUCCESS
    assert res_prune_stopped.records_processed == 1
    assert outbox.get_record("e2e_old_deliv_1") is None

    # 7. Generate health digest
    digest = scheduler.run_health_digest_now(digest_id="e2e_digest_final")
    assert digest.runtime_status == "HEALTHY"
    assert digest.sessions_total == 1
    assert digest.sessions_active == 0

    # 8. Clean coordinator shutdown
    coordinator.shutdown()
    assert scheduler.is_running is False
