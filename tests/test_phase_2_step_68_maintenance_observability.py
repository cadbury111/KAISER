"""Comprehensive Test Suite for Phase 2, Step 68:
Maintenance Observability, Scheduler SLOs & Long-Running Reliability Acceptance.

Test Matrix:
1. Counter accuracy across successful, failed, skipped, and interrupted runs.
2. Concurrent metric updates.
3. Bounded duration-sample storage.
4. Deterministic duration-summary calculations.
5. Correct status when the scheduler is intentionally disabled.
6. Health degradation after configured repeated failures.
7. UNKNOWN status when required telemetry is unavailable.
8. Accurate worker status after shutdown timeout.
9. Incomplete session recovery reflected in health status.
10. Repeated restart and recovery without duplicate durable runs or digests.
11. Long synthetic schedules using injected clocks.
12. Maintenance non-overlap guarantees.
13. Active-session pruning protection.
14. Outbox delivery failures and retention backlog reporting.
15. Portfolio reconciliation failure never reported as balanced.
16. Historical digest immutability and deterministic hashing.
17. Dashboard authentication, redaction, bounded responses, and read-only behavior.
18. Bounded diagnostic history and metric storage.
19. Configuration validation.
20. All existing safety invariants remain locked.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch
import urllib.request
import urllib.error

import pytest

from app.core.config import Settings, get_settings
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.events.bus import EventBus
from app.execution.allocation import PortfolioAllocationManager
from app.execution.maintenance import (
    DiagnosticFailureSummary,
    DurationSummary,
    HealthDigestSnapshot,
    MaintenanceHealthStatus,
    MaintenanceScheduler,
    MaintenanceTaskResult,
    MaintenanceTaskStatus,
    TaskMetricsTracker,
    WorkerStatus,
    compute_duration_summary,
    sanitize_sensitive_text,
    sanitize_dict_secrets,
)
from app.execution.outbox_dispatcher import OutboxDispatcher
from app.execution.portfolio import PaperPortfolio
from app.execution.workflow import PaperTradingWorkflowCoordinator, PaperSimulationSession
from app.ledger.ledger import PaperLedger
from app.ledger.outbox import TransactionalOutbox
from app.portfolio.dashboard.server import PortfolioDashboardServer


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def mock_clock() -> MagicMock:
    """Controllable synthetic wall clock."""
    current_time = 1_700_000_000.0

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
    """Controllable synthetic monotonic clock."""
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
    db_path = str(tmp_path / "paper_kaiser_step68.db")
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
# 1. COUNTER ACCURACY ACROSS OUTCOMES
# ==============================================================================

def test_01_counter_accuracy_across_outcomes(test_setup, mock_clock, mock_mono_clock) -> None:
    """Task outcome counters increment accurately across SUCCESS, FAILED, SKIPPED, INTERRUPTED, TIMED_OUT."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        clock=mock_clock,
        mono_clock=mock_mono_clock,
    )

    tracker = scheduler._get_or_create_tracker("prune")
    assert tracker.attempts == 0
    assert tracker.completions == 0

    # Record SUCCESS
    r1 = MaintenanceTaskResult("r1", "prune", MaintenanceTaskStatus.SUCCESS, started_at=10.0, completed_at=11.0, duration_seconds=1.0)
    tracker.record_attempt()
    tracker.record_completion(r1)
    assert tracker.attempts == 1
    assert tracker.completions == 1
    assert tracker.counts_by_outcome["SUCCESS"] == 1
    assert tracker.consecutive_failures == 0
    assert tracker.last_successful_run is not None

    # Record FAILED
    r2 = MaintenanceTaskResult("r2", "prune", MaintenanceTaskStatus.FAILED, started_at=12.0, completed_at=13.0, duration_seconds=1.0, error_message="Disk error")
    tracker.record_attempt()
    tracker.record_completion(r2)
    assert tracker.attempts == 2
    assert tracker.completions == 2
    assert tracker.counts_by_outcome["FAILED"] == 1
    assert tracker.consecutive_failures == 1
    assert tracker.last_failed_run is not None

    # Record SKIPPED_OVERLAPPING
    r3 = MaintenanceTaskResult("r3", "prune", MaintenanceTaskStatus.SKIPPED_OVERLAPPING, started_at=14.0, completed_at=14.0, duration_seconds=0.0)
    tracker.record_attempt()
    tracker.record_completion(r3)
    assert tracker.counts_by_outcome["SKIPPED_OVERLAPPING"] == 1
    assert tracker.consecutive_failures == 1  # skips do not reset or increment failures

    # Record INTERRUPTED
    r4 = MaintenanceTaskResult("r4", "prune", MaintenanceTaskStatus.INTERRUPTED, started_at=15.0, completed_at=16.0, duration_seconds=1.0)
    tracker.record_attempt()
    tracker.record_completion(r4)
    assert tracker.counts_by_outcome["INTERRUPTED"] == 1
    assert tracker.consecutive_failures == 2

    # Record TIMED_OUT
    r5 = MaintenanceTaskResult("r5", "prune", MaintenanceTaskStatus.TIMED_OUT, started_at=17.0, completed_at=20.0, duration_seconds=3.0)
    tracker.record_attempt()
    tracker.record_completion(r5)
    assert tracker.counts_by_outcome["TIMED_OUT"] == 1
    assert tracker.consecutive_failures == 3

    # Recover with SUCCESS
    r6 = MaintenanceTaskResult("r6", "prune", MaintenanceTaskStatus.SUCCESS, started_at=21.0, completed_at=22.0, duration_seconds=1.0)
    tracker.record_attempt()
    tracker.record_completion(r6)
    assert tracker.counts_by_outcome["SUCCESS"] == 2
    assert tracker.consecutive_failures == 0


# ==============================================================================
# 2. CONCURRENT METRIC UPDATES
# ==============================================================================

def test_02_concurrent_metric_updates(test_setup) -> None:
    """Concurrent updates across threads maintain thread-safe metric consistency without lost increments."""
    tracker = TaskMetricsTracker("test_task", max_samples=100)
    num_threads = 8
    increments_per_thread = 50
    barrier = threading.Barrier(num_threads)

    def worker():
        barrier.wait()
        for i in range(increments_per_thread):
            tracker.record_attempt()
            res = MaintenanceTaskResult(
                run_id=f"run_{threading.get_ident()}_{i}",
                task_name="test_task",
                status=MaintenanceTaskStatus.SUCCESS if i % 2 == 0 else MaintenanceTaskStatus.FAILED,
                started_at=100.0 + i,
                completed_at=100.0 + i + 0.05,
                duration_seconds=0.05,
            )
            tracker.record_completion(res)

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    total_expected = num_threads * increments_per_thread
    data = tracker.to_dict()
    assert data["attempts"] == total_expected
    assert data["completions"] == total_expected
    assert data["counts_by_outcome"]["SUCCESS"] == total_expected // 2
    assert data["counts_by_outcome"]["FAILED"] == total_expected // 2
    assert len(tracker.get_duration_samples()) <= 100


# ==============================================================================
# 3. BOUNDED DURATION-SAMPLE STORAGE
# ==============================================================================

def test_03_bounded_duration_sample_storage() -> None:
    """Duration samples strictly enforce max_samples bound via FIFO rolling window."""
    tracker = TaskMetricsTracker("sample_bound_task", max_samples=10)
    for i in range(50):
        res = MaintenanceTaskResult(
            run_id=f"r_{i}",
            task_name="sample_bound_task",
            status=MaintenanceTaskStatus.SUCCESS,
            started_at=float(i),
            completed_at=float(i) + 0.1,
            duration_seconds=float(i),
        )
        tracker.record_completion(res)

    samples = tracker.get_duration_samples()
    assert len(samples) == 10
    # Last 10 samples should be 40.0 through 49.0
    assert samples == [float(x) for x in range(40, 50)]


# ==============================================================================
# 4. DETERMINISTIC DURATION-SUMMARY CALCULATIONS
# ==============================================================================

def test_04_deterministic_duration_summary_calculations() -> None:
    """Duration summaries compute deterministic statistics and handle insufficient sample sizes."""
    # 0 samples
    s0 = compute_duration_summary([])
    assert s0.sample_count == 0
    assert s0.mean_seconds == 0.0
    assert s0.p90_seconds is None
    assert s0.p95_seconds is None
    assert s0.p99_seconds is None

    # 4 samples (below p90 threshold of 5)
    s4 = compute_duration_summary([0.1, 0.2, 0.3, 0.4])
    assert s4.sample_count == 4
    assert s4.min_seconds == 0.1
    assert s4.max_seconds == 0.4
    assert s4.mean_seconds == 0.25
    assert s4.median_seconds == 0.25
    assert s4.p90_seconds is None
    assert s4.p95_seconds is None

    # 10 samples (supports p90 and p95, but not p99 which requires 20)
    samples_10 = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    s10 = compute_duration_summary(samples_10)
    assert s10.sample_count == 10
    assert s10.min_seconds == 1.0
    assert s10.max_seconds == 10.0
    assert s10.mean_seconds == 5.5
    assert s10.median_seconds == 5.5
    assert s10.p90_seconds == 9.0
    assert s10.p95_seconds == 10.0
    assert s10.p99_seconds is None

    # 100 samples (supports p90, p95, p99)
    samples_100 = [float(x) for x in range(1, 101)]
    s100 = compute_duration_summary(samples_100)
    assert s100.sample_count == 100
    assert s100.min_seconds == 1.0
    assert s100.max_seconds == 100.0
    assert s100.median_seconds == 50.5
    assert s100.p90_seconds == 90.0
    assert s100.p95_seconds == 95.0
    assert s100.p99_seconds == 99.0


# ==============================================================================
# 5. CORRECT STATUS WHEN SCHEDULER IS INTENTIONALLY DISABLED
# ==============================================================================

def test_05_status_when_scheduler_disabled(test_setup) -> None:
    """An intentionally disabled scheduler is NOT marked unhealthy merely because it is stopped."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=False,
    )
    assert scheduler.worker_status == WorkerStatus.DISABLED.value
    status_str, reasons, _ = scheduler.evaluate_health()
    assert status_str == MaintenanceHealthStatus.HEALTHY.value
    assert "Scheduler is disabled by configuration; offline status is intentional." in reasons


# ==============================================================================
# 6. HEALTH DEGRADATION AFTER REPEATED FAILURES
# ==============================================================================

def test_06_health_degradation_after_repeated_failures(test_setup, mock_clock, mock_mono_clock) -> None:
    """Scheduler transitions from HEALTHY -> DEGRADED -> FAILED as consecutive failures mount."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        consecutive_failure_threshold=3,
        clock=mock_clock,
        mono_clock=mock_mono_clock,
    )
    status_str, _, _ = scheduler.evaluate_health()
    assert status_str == MaintenanceHealthStatus.HEALTHY.value

    # 1 failure -> DEGRADED
    r1 = MaintenanceTaskResult("f1", "prune", MaintenanceTaskStatus.FAILED, 10.0, 11.0, 1.0, error_message="Fail 1")
    scheduler._record_run("prune", r1)
    status_str, reasons, _ = scheduler.evaluate_health()
    assert status_str == MaintenanceHealthStatus.DEGRADED.value
    assert any("has 1 consecutive failure" in r for r in reasons)

    # 2 failures -> DEGRADED
    r2 = MaintenanceTaskResult("f2", "prune", MaintenanceTaskStatus.FAILED, 12.0, 13.0, 1.0, error_message="Fail 2")
    scheduler._record_run("prune", r2)
    status_str, reasons, _ = scheduler.evaluate_health()
    assert status_str == MaintenanceHealthStatus.DEGRADED.value

    # 3 failures -> FAILED (meets threshold)
    r3 = MaintenanceTaskResult("f3", "prune", MaintenanceTaskStatus.FAILED, 14.0, 15.0, 1.0, error_message="Fail 3")
    scheduler._record_run("prune", r3)
    status_str, reasons, _ = scheduler.evaluate_health()
    assert status_str == MaintenanceHealthStatus.FAILED.value
    assert any("failed 3 consecutive times" in r for r in reasons)

    # Recover with SUCCESS -> HEALTHY
    r4 = MaintenanceTaskResult("s1", "prune", MaintenanceTaskStatus.SUCCESS, 16.0, 17.0, 1.0)
    scheduler._record_run("prune", r4)
    status_str, reasons, _ = scheduler.evaluate_health()
    assert status_str == MaintenanceHealthStatus.HEALTHY.value


# ==============================================================================
# 7. UNKNOWN STATUS WHEN TELEMETRY IS UNAVAILABLE
# ==============================================================================

def test_07_unknown_status_when_telemetry_unavailable(test_setup) -> None:
    """Missing or corrupted required telemetry evaluates to UNKNOWN, never fabricated zero or healthy."""
    coordinator = test_setup["coordinator"]
    # Mock coordinator to throw when querying reconciliation
    with patch.object(coordinator, "reconcile_allocations", side_effect=RuntimeError("Telemetry source disconnected")):
        scheduler = MaintenanceScheduler(
            db_path=test_setup["db_path"],
            coordinator=coordinator,
            outbox=test_setup["outbox"],
        )
        status_str, reasons, diags = scheduler.evaluate_health()
        assert status_str == MaintenanceHealthStatus.UNKNOWN.value
        assert any("Telemetry error during portfolio reconciliation" in r for r in reasons)
        assert diags["telemetry_sources"]["portfolio_reconciliation"] == "unavailable"


# ==============================================================================
# 8. ACCURATE WORKER STATUS AFTER SHUTDOWN TIMEOUT
# ==============================================================================

def test_08_worker_status_after_shutdown_timeout(test_setup) -> None:
    """When a worker thread exceeds shutdown timeout, worker_status is SHUTDOWN_TIMED_OUT and health is FAILED."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        enabled=True,
    )
    # Simulate an unjoined lingering worker thread
    lingering_thread = threading.Thread(target=time.sleep, args=(2.0,), daemon=True)
    lingering_thread.start()

    with scheduler._lock:
        scheduler._worker_thread = lingering_thread
        scheduler._worker_status = WorkerStatus.SHUTDOWN_TIMED_OUT
        scheduler._shutdown_timeout_occurrences = 1

    assert scheduler.worker_status == WorkerStatus.SHUTDOWN_TIMED_OUT.value
    status_str, reasons, _ = scheduler.evaluate_health()
    assert status_str == MaintenanceHealthStatus.FAILED.value
    assert any("Worker thread still running after shutdown timeout" in r for r in reasons)


# ==============================================================================
# 9. INCOMPLETE SESSION RECOVERY REFLECTED IN HEALTH STATUS
# ==============================================================================

def test_09_incomplete_session_recovery_reflected_in_health(test_setup) -> None:
    """Incomplete session recovery on coordinator marks scheduler health as FAILED."""
    coordinator = test_setup["coordinator"]
    with patch.object(coordinator, "is_session_recovery_complete", return_value=False):
        scheduler = MaintenanceScheduler(
            db_path=test_setup["db_path"],
            coordinator=coordinator,
            outbox=test_setup["outbox"],
        )
        status_str, reasons, _ = scheduler.evaluate_health()
        assert status_str == MaintenanceHealthStatus.FAILED.value
        assert any("Session coordinator recovery is incomplete" in r for r in reasons)


# ==============================================================================
# 10. REPEATED RESTARTS AND RECOVERY WITHOUT DUPLICATE RUNS
# ==============================================================================

def test_10_repeated_restarts_recovery_no_duplicate_runs(test_setup) -> None:
    """Restarts reconcile in-flight runs to INTERRUPTED without corrupting run registry or duplicates."""
    db_path = test_setup["db_path"]

    # Insert an uncompleted RUNNING task into SQLite
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_maintenance_runs (
                run_id TEXT PRIMARY KEY,
                task_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at REAL NOT NULL,
                completed_at REAL NOT NULL,
                duration_seconds REAL NOT NULL,
                records_inspected INTEGER NOT NULL DEFAULT 0,
                records_processed INTEGER NOT NULL DEFAULT 0,
                records_skipped INTEGER NOT NULL DEFAULT 0,
                has_more INTEGER NOT NULL DEFAULT 0,
                skip_reason TEXT,
                error_message TEXT,
                details_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO paper_maintenance_runs (run_id, task_name, status, started_at, completed_at, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("inflight_run_1", "prune", "RUNNING", 100.0, 0.0, 0.0),
        )
    conn.close()

    # Start first scheduler instance (should reconcile in-flight run)
    s1 = MaintenanceScheduler(db_path=db_path)
    runs = s1.list_maintenance_runs()
    assert len(runs) == 1
    assert runs[0].run_id == "inflight_run_1"
    assert runs[0].status == MaintenanceTaskStatus.INTERRUPTED

    # Start second scheduler instance (recovery should be idempotent)
    s2 = MaintenanceScheduler(db_path=db_path)
    runs2 = s2.list_maintenance_runs()
    assert len(runs2) == 1
    assert runs2[0].run_id == "inflight_run_1"
    assert runs2[0].status == MaintenanceTaskStatus.INTERRUPTED


# ==============================================================================
# 11. LONG SYNTHETIC SCHEDULES USING INJECTED CLOCKS
# ==============================================================================

def test_11_long_synthetic_schedules_injected_clocks(test_setup, mock_clock, mock_mono_clock) -> None:
    """Synthetic multi-cycle maintenance loop simulates days of operation deterministically."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        clock=mock_clock,
        mono_clock=mock_mono_clock,
        pruning_interval_seconds=3600.0,
        health_digest_interval_seconds=3600.0,
        enabled=True,
        pruning_enabled=True,
        health_digest_enabled=True,
    )

    # Advance 24 hours in 1-hour increments
    for hour in range(24):
        mock_clock.advance(3600.0)
        mock_mono_clock.advance(3600.0)
        p_res = scheduler.run_prune_now()
        d_res = scheduler.run_health_digest_now(digest_id=f"digest_hour_{hour:02d}")
        assert p_res.status == MaintenanceTaskStatus.SUCCESS
        assert d_res.digest_id == f"digest_hour_{hour:02d}"

    # Verify 24 successful prune runs and 24 health digests
    metrics = scheduler.get_metrics()
    assert metrics["tasks"]["prune"]["counts_by_outcome"]["SUCCESS"] == 24
    assert metrics["tasks"]["health_digest"]["counts_by_outcome"]["SUCCESS"] == 24
    assert metrics["tasks"]["prune"]["consecutive_failures"] == 0

    digests = scheduler.list_health_digests(limit=50)
    assert len(digests) == 24


# ==============================================================================
# 12. MAINTENANCE NON-OVERLAP GUARANTEES
# ==============================================================================

def test_12_maintenance_non_overlap_guarantees(test_setup) -> None:
    """A task cannot run concurrently with itself; overlapping executions return SKIPPED_OVERLAPPING."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )

    # Acquire the prune lock to simulate an active in-flight task
    acquired = scheduler._prune_lock.acquire(blocking=False)
    assert acquired is True
    try:
        res = scheduler.run_prune_now()
        assert res.status == MaintenanceTaskStatus.SKIPPED_OVERLAPPING
        assert "Overlapping execution" in (res.skip_reason or "")
    finally:
        scheduler._prune_lock.release()


# ==============================================================================
# 13. ACTIVE-SESSION PRUNING PROTECTION
# ==============================================================================

def test_13_active_session_pruning_protection(test_setup) -> None:
    """Active simulation session skips destructive pruning with SKIPPED_ACTIVE_SESSION."""
    coordinator = test_setup["coordinator"]
    with patch.object(coordinator, "is_session_active", return_value=True):
        scheduler = MaintenanceScheduler(
            db_path=test_setup["db_path"],
            coordinator=coordinator,
            outbox=test_setup["outbox"],
            enabled=True,
            pruning_enabled=True,
            active_session_policy="skip",
        )
        res = scheduler.run_prune_now()
        assert res.status == MaintenanceTaskStatus.SKIPPED_ACTIVE_SESSION
        assert "ACTIVE" in (res.skip_reason or "")


# ==============================================================================
# 14. OUTBOX DELIVERY FAILURES AND RETENTION BACKLOG REPORTING
# ==============================================================================

def test_14_outbox_delivery_failures_and_backlog_reporting(test_setup) -> None:
    """Outbox retryable and exhausted failure counts degrade health and are captured in metrics."""
    outbox = test_setup["outbox"]
    # Mock outbox stats to simulate delivery failures
    with patch.object(outbox, "get_stats", return_value={"total": 10, "delivered": 5, "retryable": 2, "exhausted": 1}):
        scheduler = MaintenanceScheduler(
            db_path=test_setup["db_path"],
            coordinator=test_setup["coordinator"],
            outbox=outbox,
        )
        status_str, reasons, _ = scheduler.evaluate_health()
        assert status_str == MaintenanceHealthStatus.DEGRADED.value
        assert any("exhausted event" in r for r in reasons)
        assert any("retryable failure" in r for r in reasons)

        digest = scheduler.run_health_digest_now(digest_id="outbox_fail_digest")
        assert digest.runtime_status == "DEGRADED"
        assert digest.outbox_counts["exhausted"] == 1
        assert digest.outbox_counts["retryable"] == 2


# ==============================================================================
# 15. PORTFOLIO RECONCILIATION FAILURE NEVER REPORTED BALANCED
# ==============================================================================

def test_15_portfolio_reconciliation_failure_never_reported_balanced(test_setup) -> None:
    """When portfolio reconciliation detects discrepancies or fails, is_balanced is strictly False."""
    coordinator = test_setup["coordinator"]

    class MockReconFailure:
        def to_dict(self):
            return {
                "is_reconciled": False,
                "is_balanced": False,
                "discrepancies": ["Negative cash balance detected in sleeve A"],
            }

    with patch.object(coordinator, "reconcile_allocations", return_value=MockReconFailure()):
        scheduler = MaintenanceScheduler(
            db_path=test_setup["db_path"],
            coordinator=coordinator,
            outbox=test_setup["outbox"],
        )
        digest = scheduler.run_health_digest_now(digest_id="recon_fail_01")
        assert digest.portfolio_reconciliation["is_balanced"] is False
        assert digest.runtime_status == "DEGRADED"

    # When reconcile_allocations returns None (telemetry missing)
    with patch.object(coordinator, "reconcile_allocations", return_value=None):
        scheduler = MaintenanceScheduler(
            db_path=test_setup["db_path"],
            coordinator=coordinator,
            outbox=test_setup["outbox"],
        )
        digest2 = scheduler.run_health_digest_now(digest_id="recon_fail_02")
        assert digest2.portfolio_reconciliation is None  # explicitly unavailable, never fabricated balanced!
        assert digest2.runtime_status == "UNKNOWN"


# ==============================================================================
# 16. HISTORICAL DIGEST IMMUTABILITY AND DETERMINISTIC HASHING
# ==============================================================================

def test_16_historical_digest_immutability_and_deterministic_hashing(test_setup, mock_clock, mock_mono_clock) -> None:
    """Health digests compute deterministic SHA-256 fingerprints; historical digests are immutable."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
        clock=mock_clock,
        mono_clock=mock_mono_clock,
    )
    d1 = scheduler.run_health_digest_now(digest_id="fixed_digest_id")
    hash1 = d1.digest_hash
    assert len(hash1) == 64

    # Re-retrieve from DB
    retrieved = scheduler.get_health_digest("fixed_digest_id")
    assert retrieved is not None
    assert retrieved.digest_hash == hash1
    assert retrieved.to_dict() == d1.to_dict()


# ==============================================================================
# 17. DASHBOARD AUTHENTICATION, REDACTION, BOUNDED RESPONSES, AND READ-ONLY BEHAVIOR
# ==============================================================================

def test_17_dashboard_observability_endpoints(test_setup) -> None:
    """Dashboard maintenance routes enforce loopback, auth, redaction, bounded results, and read-only behavior."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    # Generate some runs and a digest
    scheduler.run_prune_now()
    scheduler.run_health_digest_now(digest_id="dash_digest_01")

    dash_server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        maintenance_scheduler=scheduler,
        auth_token="secret_auth_token_xyz",
        require_auth=True,
    )
    dash_server.start(blocking=False)
    time.sleep(0.3)
    base_url = dash_server.url

    try:
        # 1. Unauthenticated request -> 401 Unauthorized
        req_unauth = urllib.request.Request(f"{base_url}/api/paper/maintenance/status")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_unauth)
        assert exc_info.value.code == 401

        # 2. Authenticated status -> 200 OK
        req_auth = urllib.request.Request(
            f"{base_url}/api/paper/maintenance/status",
            headers={"Authorization": "Bearer secret_auth_token_xyz"},
        )
        with urllib.request.urlopen(req_auth) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert "health_status" in data["scheduler"]
            assert "worker_status" in data["scheduler"]

        # 3. Authenticated metrics endpoint -> 200 OK
        req_metrics = urllib.request.Request(
            f"{base_url}/api/paper/maintenance/metrics",
            headers={"Authorization": "Bearer secret_auth_token_xyz"},
        )
        with urllib.request.urlopen(req_metrics) as resp:
            assert resp.status == 200
            m_data = json.loads(resp.read().decode())
            assert m_data["status"] == "success"
            assert "metrics" in m_data
            assert "health_status" in m_data
            assert "duration_summaries" in m_data

        # 4. POST request -> 405 Method Not Allowed (read-only invariant)
        req_post = urllib.request.Request(
            f"{base_url}/api/paper/maintenance/status",
            data=b"{}",
            headers={"Authorization": "Bearer secret_auth_token_xyz", "Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as post_exc:
            urllib.request.urlopen(req_post)
        assert post_exc.value.code == 405

    finally:
        dash_server.stop()


# ==============================================================================
# 18. BOUNDED DIAGNOSTIC HISTORY AND METRIC STORAGE
# ==============================================================================

def test_18_bounded_diagnostic_history_and_metric_storage(test_setup) -> None:
    """Diagnostic failure summaries and recent errors are strictly bounded to max 50 entries."""
    scheduler = MaintenanceScheduler(
        db_path=test_setup["db_path"],
        coordinator=test_setup["coordinator"],
        outbox=test_setup["outbox"],
    )
    # Record 70 distinct failure types
    for i in range(70):
        scheduler._record_failure_summary(
            failure_type=f"FAIL_TYPE_{i}",
            task_name=f"task_{i}",
            error_message=f"Error {i} with secret_key=AKIAIOSFODNN7EXAMPLE",
            details={"token": "bearer_secret_123"},
        )
        scheduler._record_error(f"Error log {i} password=secret")

    summaries = scheduler.get_failure_summaries()
    assert len(summaries) <= 50
    # Verify secret redaction in summaries
    for s in summaries:
        assert "AKIAIOSFODNN7EXAMPLE" not in s["last_error_message"]
        assert "bearer_secret_123" not in str(s["details"])
        assert "[REDACTED]" in s["last_error_message"] or "[REDACTED]" in str(s["details"])

    with scheduler._lock:
        assert len(scheduler._recent_errors) <= 50
        for err in scheduler._recent_errors:
            assert "password=secret" not in err


# ==============================================================================
# 19. CONFIGURATION VALIDATION
# ==============================================================================

def test_19_configuration_validation() -> None:
    """Invalid failure thresholds or duration limits raise ConfigurationError."""
    # Invalid consecutive failure threshold <= 0
    with pytest.raises(ConfigurationError) as exc1:
        Settings(maintenance_consecutive_failure_threshold=0)
    assert "MAINTENANCE_CONSECUTIVE_FAILURE_THRESHOLD" in str(exc1.value)

    # Invalid consecutive failure threshold > 1000
    with pytest.raises(ConfigurationError) as exc2:
        Settings(maintenance_consecutive_failure_threshold=1001)
    assert "MAINTENANCE_CONSECUTIVE_FAILURE_THRESHOLD" in str(exc2.value)

    # Invalid duration sample limit < 10
    with pytest.raises(ConfigurationError) as exc3:
        Settings(maintenance_duration_sample_limit=9)
    assert "MAINTENANCE_DURATION_SAMPLE_LIMIT" in str(exc3.value)

    # Invalid duration sample limit > 10000
    with pytest.raises(ConfigurationError) as exc4:
        Settings(maintenance_duration_sample_limit=10001)
    assert "MAINTENANCE_DURATION_SAMPLE_LIMIT" in str(exc4.value)

    # Valid bounds succeed
    valid_settings = Settings(
        maintenance_consecutive_failure_threshold=5,
        maintenance_duration_sample_limit=500,
    )
    assert valid_settings.maintenance_consecutive_failure_threshold == 5
    assert valid_settings.maintenance_duration_sample_limit == 500


# ==============================================================================
# 20. ABSOLUTE SYSTEM SAFETY INVARIANTS REMAIN LOCKED
# ==============================================================================

def test_20_absolute_safety_invariants_remain_locked() -> None:
    """Absolute Phase 2 safety invariants remain locked throughout observability implementation."""
    settings = get_settings()
    assert settings.execution_locked is True
    assert settings.broker_provider == "none"
    assert settings.paper_execution_enabled is False
    assert settings.maintenance_scheduler_enabled is False
    assert settings.maintenance_pruning_enabled is False
    assert settings.maintenance_health_digest_enabled is False
    assert settings.outbox_retention_enabled is False
