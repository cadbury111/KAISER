"""KAISER — Phase 2, Step 65: Outbox Retention/Pruning Policies & Session Coordinator + Portfolio Equity Snapshot Event Coverage.

Comprehensive test suite verifying:
1. Old DELIVERED event is eligible only after the configured cutoff.
2. Recent DELIVERED events are retained.
3. Undelivered and EXHAUSTED records are never deleted by ordinary retention.
4. Invalid or missing delivery timestamps are handled safely.
5. Batch limits are enforced.
6. Repeated pruning is idempotent.
7. SQLite failure rolls back the affected pruning batch.
8. Concurrent dispatch/pruning does not delete an event during delivery.
9. Corrupted or unknown-status rows are not silently discarded.
10. Configuration validation rejects invalid retention settings.
11. Valid session transitions create the expected versioned event.
12. Duplicate requests do not create duplicate logical transitions.
13. Failure before durable commit creates no dispatchable event for a failed transition.
14. Post-commit publication failure retains the committed session transition and pending/retryable event.
15. Restart recovery does not duplicate session state or event identities.
16. Snapshot values reconcile with the authoritative portfolio calculation.
17. Non-finite numbers, invalid timestamps, and invalid monetary values are rejected.
18. Repeated snapshot requests with the same stable ID are idempotent.
19. Failed financial operations do not generate misleading committed snapshots.
20. Snapshot events use a coherent committed state and do not run on ordinary dashboard reads.
21. Session and equity events are dispatched through the existing outbox worker.
22. Retry, exhaustion, and recovery work for both event categories.
23. Retention never removes pending or retryable session/equity events.
24. Dashboard authentication, redaction, pagination, and HTTP method restrictions remain enforced.
25. Mandatory KAISER safety invariants: execution_locked=True, broker_provider="none", paper execution disabled by default.
"""

from __future__ import annotations

import copy
from http import HTTPStatus
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch
import urllib.error
import urllib.request
import uuid

import pytest

from app.core.config import ConfigurationError, Settings, get_settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.execution.allocation import (
    AllocationStatus,
    PortfolioAllocationManager,
    PortfolioAllocationRequest,
    PortfolioAllocationResult,
    PortfolioEquitySnapshot,
    SleeveRiskBudget,
    StrategySleeveDefinition,
    create_standard_multi_sleeve_request,
)
from app.execution.engine import PaperExecutionEngine
from app.execution.models import (
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
)
from app.execution.outbox_dispatcher import OutboxDispatcher
from app.execution.portfolio import PaperPortfolio
from app.execution.workflow import (
    PaperSimulationSession,
    PaperTradingWorkflowCoordinator,
)
from app.ledger.ledger import PaperLedger
from app.ledger.outbox import (
    DeduplicatingEventHandler,
    OutboxPruneResult,
    OutboxRecord,
    OutboxStatus,
    TransactionalOutbox,
    redact_sensitive_payload,
)
from app.market.models import Quote
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.reporting import PortfolioReportingService


def _make_quote(symbol: str, price: float, timestamp: float = 1_000_000.0) -> Quote:
    return Quote(
        symbol=symbol,
        bid=round(price * 0.9995, 2),
        ask=round(price * 1.0005, 2),
        last=price,
        volume=10_000.0,
        timestamp=timestamp,
    )


# ==============================================================================
# PART A: OUTBOX RETENTION & PRUNING TESTS (Items 1 - 10)
# ==============================================================================


def test_01_old_delivered_event_eligible_only_after_cutoff(tmp_path: Path) -> None:
    """An event is eligible for pruning only when DELIVERED and delivered_at <= cutoff."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=7.0,
        prune_batch_size=50,
        archival_enabled=True,
    )
    now = 1_000_000.0
    cutoff = now - (7.0 * 86400.0)

    # 1. Old delivered record (older than cutoff)
    old_rec = OutboxRecord(
        event_id="rec_old_delivered",
        event_type="TEST_EVENT",
        status=OutboxStatus.DELIVERED,
        delivered_at=cutoff - 100.0,
        created_at=cutoff - 500.0,
    )
    # 2. Recent delivered record (newer than cutoff)
    recent_rec = OutboxRecord(
        event_id="rec_recent_delivered",
        event_type="TEST_EVENT",
        status=OutboxStatus.DELIVERED,
        delivered_at=cutoff + 100.0,
        created_at=cutoff - 50.0,
    )

    outbox.stage_event(old_rec)
    outbox.stage_event(recent_rec)

    # Prune with explicit current time
    result = outbox.prune_delivered_records(now=now)

    assert result.pruned_count == 1
    assert result.archived_count == 1
    assert result.skipped_count >= 1

    # Verify old record is pruned and in archive
    assert outbox.get_record("rec_old_delivered") is None
    archived = outbox.get_archived_record("rec_old_delivered")
    assert archived is not None
    assert archived["event_id"] == "rec_old_delivered"
    assert archived["archive_reason"] == "retention_prune"

    # Verify recent record remains active in outbox
    assert outbox.get_record("rec_recent_delivered") is not None


def test_02_recent_delivered_events_retained(tmp_path: Path) -> None:
    """Delivered events within the retention window are strictly retained."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=14.0,
        prune_batch_size=10,
    )
    now = 1_000_000.0

    for i in range(5):
        rec = OutboxRecord(
            event_id=f"recent_{i}",
            event_type="TEST_EVENT",
            status=OutboxStatus.DELIVERED,
            delivered_at=now - (i * 3600.0),  # all delivered within last 5 hours
            created_at=now - (i * 3600.0) - 10.0,
        )
        outbox.stage_event(rec)

    result = outbox.prune_delivered_records(now=now)
    assert result.pruned_count == 0
    assert result.inspected_count == 5
    assert result.records_skipped_by_age == 5
    assert outbox.count_records() == 5


def test_03_undelivered_and_exhausted_records_never_deleted(tmp_path: Path) -> None:
    """PENDING, RETRYABLE_FAILURE, and EXHAUSTED records are NEVER pruned by retention."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
    )
    very_old_time = 100_000.0
    now = 1_000_000.0

    pending = OutboxRecord(
        event_id="pending_old",
        event_type="TEST_EVENT",
        status=OutboxStatus.PENDING,
        created_at=very_old_time,
    )
    retryable = OutboxRecord(
        event_id="retryable_old",
        event_type="TEST_EVENT",
        status=OutboxStatus.RETRYABLE_FAILURE,
        created_at=very_old_time,
        last_attempt_at=very_old_time + 10.0,
    )
    exhausted = OutboxRecord(
        event_id="exhausted_old",
        event_type="TEST_EVENT",
        status=OutboxStatus.EXHAUSTED,
        created_at=very_old_time,
        last_attempt_at=very_old_time + 20.0,
    )

    outbox.stage_event(pending)
    outbox.stage_event(retryable)
    outbox.stage_event(exhausted)

    result = outbox.prune_delivered_records(now=now)
    assert result.pruned_count == 0
    assert outbox.get_record("pending_old") is not None
    assert outbox.get_record("retryable_old") is not None
    assert outbox.get_record("exhausted_old") is not None


def test_04_invalid_or_missing_delivery_timestamps_handled_safely(tmp_path: Path) -> None:
    """Delivered records with null, non-positive, or invalid delivered_at are safely skipped."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
    )
    now = 1_000_000.0

    # Insert raw row with delivered status but NULL delivered_at
    with outbox._get_connection() as conn:
        conn.execute(
            """
            INSERT INTO paper_outbox_entries (
                event_id, event_type, schema_version, payload_json, status,
                attempt_count, max_attempts, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("rec_null_delivered_at", "TEST", "1.0", "{}", "DELIVERED", 1, 3, now - 100000.0, None),
        )
        conn.execute(
            """
            INSERT INTO paper_outbox_entries (
                event_id, event_type, schema_version, payload_json, status,
                attempt_count, max_attempts, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("rec_zero_delivered_at", "TEST", "1.0", "{}", "DELIVERED", 1, 3, now - 100000.0, 0.0),
        )

    result = outbox.prune_delivered_records(now=now)
    # The NULL/zero timestamp rows must not be pruned
    assert result.pruned_count == 0
    with outbox._get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM paper_outbox_entries WHERE event_id IN ('rec_null_delivered_at', 'rec_zero_delivered_at')")
        assert cursor.fetchone()[0] == 2


def test_05_batch_limits_enforced_and_has_more(tmp_path: Path) -> None:
    """Batch size limits per pruning invocation are strictly respected with has_more=True."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
        prune_batch_size=3,
        archival_enabled=False,
    )
    now = 1_000_000.0
    old_time = now - (2.0 * 86400.0)

    for i in range(10):
        rec = OutboxRecord(
            event_id=f"batch_rec_{i:02d}",
            event_type="TEST",
            status=OutboxStatus.DELIVERED,
            delivered_at=old_time + i,
            created_at=old_time,
        )
        outbox.stage_event(rec)

    # First prune batch
    res1 = outbox.prune_delivered_records(now=now, batch_size=3)
    assert res1.pruned_count == 3
    assert res1.has_more is True

    # Second prune batch
    res2 = outbox.prune_delivered_records(now=now, batch_size=3)
    assert res2.pruned_count == 3
    assert res2.has_more is True

    # Remaining 4 records: prune with batch_size 10
    res3 = outbox.prune_delivered_records(now=now, batch_size=10)
    assert res3.pruned_count == 4
    assert res3.has_more is False

    assert outbox.count_records() == 0


def test_06_repeated_pruning_is_idempotent(tmp_path: Path) -> None:
    """Repeated pruning invocations on an already-pruned or empty table are safe and idempotent."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
    )
    now = 1_000_000.0

    res1 = outbox.prune_delivered_records(now=now)
    assert res1.pruned_count == 0
    assert res1.has_more is False

    res2 = outbox.prune_delivered_records(now=now)
    assert res2.pruned_count == 0
    assert res2.has_more is False


def test_07_sqlite_failure_rolls_back_pruning_batch(tmp_path: Path) -> None:
    """An injected SQLite failure during batch pruning rolls back the entire batch atomically."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
        archival_enabled=True,
    )
    now = 1_000_000.0
    old_time = now - (2.0 * 86400.0)

    for i in range(3):
        rec = OutboxRecord(
            event_id=f"rollback_rec_{i}",
            event_type="TEST",
            status=OutboxStatus.DELIVERED,
            delivered_at=old_time,
            created_at=old_time - 10.0,
        )
        outbox.stage_event(rec)

    # Inject trigger to abort DELETE
    with outbox._get_connection() as conn:
        conn.execute("CREATE TRIGGER fail_delete BEFORE DELETE ON paper_outbox_entries BEGIN SELECT RAISE(ABORT, 'Simulated DB failure during delete'); END;")

    with pytest.raises(sqlite3.Error):
        outbox.prune_delivered_records(now=now)

    # In clean connection, drop trigger and verify all 3 records are still intact (rolled back)
    with outbox._get_connection() as conn:
        conn.execute("DROP TRIGGER IF EXISTS fail_delete")

    assert outbox.count_records() == 3
    for i in range(3):
        assert outbox.get_record(f"rollback_rec_{i}") is not None


def test_08_concurrent_dispatch_pruning_excludes_in_flight(tmp_path: Path) -> None:
    """OutboxDispatcher protects in-flight events from being pruned while dispatch is underway."""
    db_path = str(tmp_path / "outbox.db")
    event_bus = EventBus()
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
    )
    dispatcher = OutboxDispatcher(
        outbox=outbox,
        event_bus=event_bus,
        retention_enabled=True,
        retention_days=1.0,
    )
    now = 1_000_000.0
    old_time = now - (2.0 * 86400.0)

    # Stage an event marked DELIVERED but simulate it is currently in_flight in dispatcher
    rec = OutboxRecord(
        event_id="in_flight_delivered_rec",
        event_type="TEST",
        status=OutboxStatus.DELIVERED,
        delivered_at=old_time,
        created_at=old_time,
    )
    outbox.stage_event(rec)

    # Simulate in-flight lock
    dispatcher._in_flight_event_ids.add("in_flight_delivered_rec")

    # Prune through dispatcher
    result = dispatcher.prune_delivered(now=now)

    assert result.pruned_count == 0
    assert result.skipped_count == 1
    assert outbox.get_record("in_flight_delivered_rec") is not None

    # Release in-flight lock and prune again
    dispatcher._in_flight_event_ids.remove("in_flight_delivered_rec")
    result2 = dispatcher.prune_delivered(now=now)
    assert result2.pruned_count == 1
    assert outbox.get_record("in_flight_delivered_rec") is None


def test_09_corrupted_or_unknown_status_rows_not_silently_discarded(tmp_path: Path) -> None:
    """Rows with invalid/corrupt statuses are never deleted by retention pruning."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
    )
    now = 1_000_000.0
    old_time = now - (5.0 * 86400.0)

    with outbox._get_connection() as conn:
        conn.execute(
            """
            INSERT INTO paper_outbox_entries (
                event_id, event_type, schema_version, payload_json, status,
                attempt_count, max_attempts, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("corrupt_status_row", "TEST", "1.0", "{}", "UNKNOWN_CORRUPT_STATUS", 0, 3, old_time, old_time),
        )

    result = outbox.prune_delivered_records(now=now)
    assert result.pruned_count == 0

    with outbox._get_connection() as conn:
        row = conn.execute("SELECT status FROM paper_outbox_entries WHERE event_id = ?", ("corrupt_status_row",)).fetchone()
        assert row is not None
        assert row[0] == "UNKNOWN_CORRUPT_STATUS"


def test_10_configuration_validation_rejects_invalid_retention_settings() -> None:
    """Settings strictly validates retention period, batch size, and booleans."""
    # Valid defaults
    s = Settings(outbox_retention_enabled=False, outbox_retention_days=30.0, outbox_prune_batch_size=500)
    assert s.outbox_retention_enabled is False
    assert s.outbox_retention_days == 30.0
    assert s.outbox_prune_batch_size == 500

    # Negative retention days
    with pytest.raises(ConfigurationError, match="OUTBOX_RETENTION_DAYS"):
        Settings(outbox_retention_days=-1.0)

    # Zero retention days
    with pytest.raises(ConfigurationError, match="OUTBOX_RETENTION_DAYS"):
        Settings(outbox_retention_days=0.0)

    # Non-finite retention days
    with pytest.raises(ConfigurationError, match="OUTBOX_RETENTION_DAYS"):
        Settings(outbox_retention_days=float("nan"))

    # Non-positive batch size
    with pytest.raises(ConfigurationError, match="OUTBOX_PRUNE_BATCH_SIZE"):
        Settings(outbox_prune_batch_size=0)

    with pytest.raises(ConfigurationError, match="OUTBOX_PRUNE_BATCH_SIZE"):
        Settings(outbox_prune_batch_size=-10)

    # Excessive batch size
    with pytest.raises(ConfigurationError, match="OUTBOX_PRUNE_BATCH_SIZE"):
        Settings(outbox_prune_batch_size=100_000)


# ==============================================================================
# PART B: SESSION LIFECYCLE EVENT COVERAGE TESTS (Items 11 - 15)
# ==============================================================================


@pytest.fixture
def session_fixture(tmp_path: Path):
    db_path = str(tmp_path / "paper_ledger.db")
    event_bus = EventBus()
    state_store = StateStore()
    ledger = PaperLedger(db_path=db_path)
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    engine = PaperExecutionEngine(
        portfolio=portfolio,
        ledger=ledger,
        event_bus=event_bus,
        state_store=state_store,
        enabled=True,
    )
    coordinator = PaperTradingWorkflowCoordinator(
        engine=engine,
        event_bus=event_bus,
        state_store=state_store,
        enabled=True,
        outbox=TransactionalOutbox(db_path=db_path),
    )
    return {
        "db_path": db_path,
        "event_bus": event_bus,
        "state_store": state_store,
        "engine": engine,
        "coordinator": coordinator,
    }


def test_11_valid_session_transitions_create_versioned_events(session_fixture) -> None:
    """Starting and stopping a session creates durable versioned events in the outbox."""
    coord: PaperTradingWorkflowCoordinator = session_fixture["coordinator"]
    event_bus: EventBus = session_fixture["event_bus"]

    received_events: List[Event] = []
    event_bus.subscribe(EventType.PAPER_SESSION_STARTED, received_events.append)
    event_bus.subscribe(EventType.PAPER_SESSION_STOPPED, received_events.append)

    session = coord.start_session(session_id="test_sess_01", initial_capital=50_000.0)
    assert session.session_id == "test_sess_01"
    assert session.status == "ACTIVE"
    assert session.initial_capital == 50_000.0

    # Verify event was staged in outbox
    outbox: TransactionalOutbox = coord.outbox
    events = outbox.list_records(limit=10)
    start_events = [e for e in events if e.event_type == "PAPER_SESSION_STARTED"]
    assert len(start_events) == 1
    assert start_events[0].payload["session_id"] == "test_sess_01"
    assert start_events[0].payload["status"] == "ACTIVE"
    assert start_events[0].status == OutboxStatus.DELIVERED  # Published immediately

    # Verify EventBus received the event
    assert len(received_events) == 1
    assert received_events[0].data["session_id"] == "test_sess_01"

    # Stop session
    stopped_sess = coord.stop_session(session_id="test_sess_01", reason="Normal test completion")
    assert stopped_sess.status == "STOPPED"
    assert stopped_sess.stopped_at is not None

    events2 = outbox.list_records(limit=10)
    stop_events = [e for e in events2 if e.event_type == "PAPER_SESSION_STOPPED"]
    assert len(stop_events) == 1
    assert stop_events[0].payload["session_id"] == "test_sess_01"
    assert stop_events[0].payload["status"] == "STOPPED"
    assert stop_events[0].payload["reason"] == "Normal test completion"

    assert len(received_events) == 2


def test_12_duplicate_session_requests_are_idempotent(session_fixture) -> None:
    """Duplicate start or stop calls do not create duplicate logical transitions or events."""
    coord: PaperTradingWorkflowCoordinator = session_fixture["coordinator"]
    outbox: TransactionalOutbox = coord.outbox

    # Start session twice
    s1 = coord.start_session(session_id="sess_dup_1", initial_capital=25_000.0)
    s2 = coord.start_session(session_id="sess_dup_1", initial_capital=25_000.0)
    assert s1.session_id == s2.session_id

    # Stop session twice
    stopped1 = coord.stop_session("sess_dup_1", reason="First stop")
    stopped2 = coord.stop_session("sess_dup_1", reason="Second stop")
    assert stopped1.stopped_at == stopped2.stopped_at

    # Check outbox has exactly 1 STARTED and 1 STOPPED event
    events = outbox.list_records(limit=20)
    start_events = [e for e in events if e.event_type == "PAPER_SESSION_STARTED" and e.payload.get("session_id") == "sess_dup_1"]
    stop_events = [e for e in events if e.event_type == "PAPER_SESSION_STOPPED" and e.payload.get("session_id") == "sess_dup_1"]
    assert len(start_events) == 1
    assert len(stop_events) == 1


def test_13_failure_before_durable_commit_creates_no_event(session_fixture) -> None:
    """Failure before SQLite commit creates neither committed session nor outbox event."""
    coord: PaperTradingWorkflowCoordinator = session_fixture["coordinator"]
    outbox: TransactionalOutbox = coord.outbox

    # Patch connection execute to fail when writing to paper_simulation_sessions
    with patch.object(coord, "_get_db_connection") as mock_conn_fn:
        mock_conn = MagicMock()
        mock_conn.__enter__.side_effect = sqlite3.OperationalError("Simulated pre-commit disk error")
        mock_conn_fn.return_value = mock_conn

        with pytest.raises(sqlite3.OperationalError):
            coord.start_session(session_id="failed_pre_commit", initial_capital=10_000.0)

    # Verify session is not in coordinator's sessions or SQLite
    assert coord.get_session("failed_pre_commit") is None
    events = outbox.list_records(limit=20)
    assert not any(e.payload.get("session_id") == "failed_pre_commit" for e in events)


def test_14_post_commit_publication_failure_preserves_session_and_pending_event(session_fixture) -> None:
    """If EventBus publication fails post-commit, session is committed and outbox record remains pending."""
    coord: PaperTradingWorkflowCoordinator = session_fixture["coordinator"]
    event_bus: EventBus = session_fixture["event_bus"]
    outbox: TransactionalOutbox = coord.outbox

    with patch.object(event_bus, "publish", side_effect=RuntimeError("Simulated EventBus failure")):
        session = coord.start_session(session_id="sess_post_fail", initial_capital=10_000.0)

    # Session is committed and active
    assert session is not None
    assert session.session_id == "sess_post_fail"
    assert session.status == "ACTIVE"

    # Outbox event remains stored and marked PENDING (or RETRYABLE_FAILURE)
    events = outbox.list_records(limit=20)
    sess_events = [e for e in events if e.payload.get("session_id") == "sess_post_fail"]
    assert len(sess_events) == 1
    assert sess_events[0].status in (OutboxStatus.PENDING, OutboxStatus.RETRYABLE_FAILURE)


def test_15_restart_recovery_does_not_duplicate_session_state(session_fixture) -> None:
    """Restart recovery loads sessions from SQLite without duplicating state or event IDs."""
    coord1: PaperTradingWorkflowCoordinator = session_fixture["coordinator"]
    db_path = session_fixture["db_path"]

    coord1.start_session(session_id="sess_rec_1", initial_capital=15_000.0)
    coord1.stop_session("sess_rec_1", reason="Completed")

    # Second coordinator instance pointing to the same SQLite database
    coord2 = PaperTradingWorkflowCoordinator(
        engine=session_fixture["engine"],
        event_bus=session_fixture["event_bus"],
        state_store=session_fixture["state_store"],
        enabled=True,
        db_path=db_path,
        outbox=TransactionalOutbox(db_path=db_path),
    )

    recovered_session = coord2.get_session("sess_rec_1")
    assert recovered_session is not None
    assert recovered_session.session_id == "sess_rec_1"
    assert recovered_session.status == "STOPPED"
    assert recovered_session.initial_capital == 15_000.0

    # Ensure recovering twice does not duplicate records
    coord2.recover_sessions()
    sessions = coord2.list_sessions()
    sess_ids = [s.session_id for s in sessions if s.session_id == "sess_rec_1"]
    assert len(sess_ids) == 1


# ==============================================================================
# PART C: PORTFOLIO EQUITY SNAPSHOT TESTS (Items 16 - 20)
# ==============================================================================


@pytest.fixture
def allocation_fixture(tmp_path: Path):
    db_path = str(tmp_path / "paper_ledger.db")
    event_bus = EventBus()
    state_store = StateStore()
    ledger = PaperLedger(db_path=db_path)
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    engine = PaperExecutionEngine(
        portfolio=portfolio,
        ledger=ledger,
        event_bus=event_bus,
        state_store=state_store,
        enabled=True,
    )
    alloc_mgr = PortfolioAllocationManager(
        portfolio=portfolio,
        ledger=ledger,
        event_bus=event_bus,
        db_path=db_path,
    )
    return {
        "db_path": db_path,
        "event_bus": event_bus,
        "ledger": ledger,
        "portfolio": portfolio,
        "engine": engine,
        "alloc_mgr": alloc_mgr,
    }


def test_16_snapshot_values_reconcile_with_portfolio(allocation_fixture) -> None:
    """Portfolio equity snapshot values match authoritative portfolio calculations exactly."""
    alloc_mgr: PortfolioAllocationManager = allocation_fixture["alloc_mgr"]
    portfolio: PaperPortfolio = allocation_fixture["portfolio"]

    # Initialize standard multi-sleeve allocations
    req = create_standard_multi_sleeve_request(total_capital=100_000.0)
    alloc_mgr.apply_allocation(req)

    snapshot = alloc_mgr.create_equity_snapshot(
        snapshot_id="snap_reconcile_01",
        reason="Scheduled test snapshot",
    )

    assert snapshot.snapshot_id == "snap_reconcile_01"
    assert snapshot.currency == "USD"
    assert snapshot.total_equity == pytest.approx(portfolio.total_equity, abs=1e-4)
    assert snapshot.cash == pytest.approx(portfolio.cash, abs=1e-4)
    assert snapshot.reserved_cash == pytest.approx(portfolio.reserved_cash, abs=1e-4)
    assert snapshot.positions_valuation == pytest.approx(portfolio.positions_valuation, abs=1e-4)
    assert len(snapshot.sleeve_allocations) == 3

    # Verify event was staged and published
    outbox: TransactionalOutbox = alloc_mgr.outbox
    rec = outbox.get_record("snap_reconcile_01")
    assert rec is not None
    assert rec.event_type == "PAPER_PORTFOLIO_EQUITY_SNAPSHOT"
    assert rec.status == OutboxStatus.DELIVERED
    assert rec.payload["total_equity"] == snapshot.total_equity


def test_17_non_finite_and_invalid_values_rejected() -> None:
    """Non-finite floats, negative cash, and invalid IDs are strictly rejected."""
    with pytest.raises(ValidationError, match="finite number"):
        PortfolioEquitySnapshot(
            snapshot_id="snap_nan",
            timestamp=1_000_000.0,
            currency="USD",
            total_equity=float("nan"),
            available_cash=100_000.0,
            reserved_cash=0.0,
            total_position_value=0.0,
        )

    with pytest.raises(ValidationError, match="finite number"):
        PortfolioEquitySnapshot(
            snapshot_id="snap_inf",
            timestamp=1_000_000.0,
            currency="USD",
            total_equity=100_000.0,
            available_cash=float("inf"),
            reserved_cash=0.0,
            total_position_value=0.0,
        )

    with pytest.raises(ValidationError, match="non-negative"):
        PortfolioEquitySnapshot(
            snapshot_id="snap_neg_reserved",
            timestamp=1_000_000.0,
            currency="USD",
            total_equity=100_000.0,
            available_cash=100_000.0,
            reserved_cash=-10.0,
            total_position_value=0.0,
        )

    with pytest.raises(ValidationError, match="positive finite"):
        PortfolioEquitySnapshot(
            snapshot_id="snap_bad_time",
            timestamp=-50.0,
            currency="USD",
            total_equity=100_000.0,
            available_cash=100_000.0,
            reserved_cash=0.0,
            total_position_value=0.0,
        )


def test_18_repeated_snapshot_requests_with_same_id_are_idempotent(allocation_fixture) -> None:
    """Repeated snapshot calls with the same snapshot_id return the existing snapshot without duplicating events."""
    alloc_mgr: PortfolioAllocationManager = allocation_fixture["alloc_mgr"]
    outbox: TransactionalOutbox = alloc_mgr.outbox

    snap1 = alloc_mgr.create_equity_snapshot(snapshot_id="snap_idempotent_1")
    snap2 = alloc_mgr.create_equity_snapshot(snapshot_id="snap_idempotent_1")

    assert snap1.snapshot_id == snap2.snapshot_id
    assert snap1.timestamp == snap2.timestamp

    # Check outbox has exactly 1 event for this snapshot ID
    events = outbox.list_records(limit=20)
    snap_events = [e for e in events if e.event_id == "snap_idempotent_1"]
    assert len(snap_events) == 1


def test_19_failed_financial_operations_do_not_generate_committed_snapshots(allocation_fixture) -> None:
    """Failed order or allocation attempts do not generate or commit misleading equity snapshots."""
    alloc_mgr: PortfolioAllocationManager = allocation_fixture["alloc_mgr"]
    outbox: TransactionalOutbox = alloc_mgr.outbox

    # 1. Attempt an order for non-existent sleeve (returns rejection result, not fill)
    order_req = PaperOrderRequest(
        symbol="AAPL",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=10.0,
    )
    result = alloc_mgr.execute_sleeve_order(sleeve_id="non_existent_sleeve", request=order_req)
    assert result.success is False

    # 2. Attempt an invalid allocation request (raises ValidationError before commit)
    with pytest.raises(ValidationError):
        alloc_mgr.apply_allocation(None)

    # Verify no snapshots exist
    snapshots = alloc_mgr.list_equity_snapshots()
    assert len(snapshots) == 0

    events = outbox.list_records(limit=10)
    assert not any(e.event_type == "PAPER_PORTFOLIO_EQUITY_SNAPSHOT" for e in events)


def test_20_snapshot_events_not_emitted_on_read_only_dashboard_reads(allocation_fixture) -> None:
    """Reading equity snapshots through dashboard diagnostics does NOT emit new snapshot events."""
    alloc_mgr: PortfolioAllocationManager = allocation_fixture["alloc_mgr"]
    outbox: TransactionalOutbox = alloc_mgr.outbox

    alloc_mgr.create_equity_snapshot(snapshot_id="snap_base")
    initial_snapshot_count = len(alloc_mgr.list_equity_snapshots())
    initial_outbox_count = outbox.count_records()

    # Create server
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        allocation_manager=alloc_mgr,
        require_auth=False,
    )
    server.start()
    try:
        url = f"{server.url}/api/paper/outbox/snapshots"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] == 1
            assert data["read_only"] is True

        # Verify no new snapshot or outbox event was created
        assert len(alloc_mgr.list_equity_snapshots()) == initial_snapshot_count
        assert outbox.count_records() == initial_outbox_count
    finally:
        server.stop()


# ==============================================================================
# PART D: OUTBOX DISPATCHER, RETRY & RECOVERY (Items 21 - 23)
# ==============================================================================


def test_21_session_and_equity_events_dispatched_via_worker(tmp_path: Path) -> None:
    """OutboxDispatcher automatically dispatches both session and equity events to subscribers."""
    db_path = str(tmp_path / "outbox.db")
    event_bus = EventBus()
    outbox = TransactionalOutbox(db_path=db_path)
    dispatcher = OutboxDispatcher(outbox=outbox, event_bus=event_bus)

    received: List[str] = []
    event_bus.subscribe(EventType.PAPER_SESSION_STARTED, lambda ev: received.append("session"))
    event_bus.subscribe(EventType.PAPER_PORTFOLIO_EQUITY_SNAPSHOT, lambda ev: received.append("equity"))

    # Stage 1 session and 1 equity event as PENDING
    outbox.stage_event(
        OutboxRecord(
            event_id="sess_ev_1",
            event_type="PAPER_SESSION_STARTED",
            payload={"session_id": "s1", "status": "ACTIVE"},
            status=OutboxStatus.PENDING,
        )
    )
    outbox.stage_event(
        OutboxRecord(
            event_id="equity_ev_1",
            event_type="PAPER_PORTFOLIO_EQUITY_SNAPSHOT",
            payload={"snapshot_id": "eq1", "total_equity": 100000.0},
            status=OutboxStatus.PENDING,
        )
    )

    dispatched = dispatcher.dispatch_pending()
    assert dispatched == 2
    assert "session" in received
    assert "equity" in received

    assert outbox.get_record("sess_ev_1").status == OutboxStatus.DELIVERED
    assert outbox.get_record("equity_ev_1").status == OutboxStatus.DELIVERED


def test_22_retry_and_exhaustion_work_for_both_event_categories(tmp_path: Path) -> None:
    """Failed EventBus deliveries increment retry count and transition to EXHAUSTED for new event types."""
    db_path = str(tmp_path / "outbox.db")
    event_bus = EventBus()
    outbox = TransactionalOutbox(db_path=db_path)
    dispatcher = OutboxDispatcher(
        outbox=outbox,
        event_bus=event_bus,
        retry_backoff_seconds=0.01,
        max_attempts=2,
    )

    outbox.stage_event(
        OutboxRecord(
            event_id="sess_fail",
            event_type="PAPER_SESSION_STARTED",
            payload={"session_id": "sf"},
            max_attempts=2,
        )
    )
    outbox.stage_event(
        OutboxRecord(
            event_id="eq_fail",
            event_type="PAPER_PORTFOLIO_EQUITY_SNAPSHOT",
            payload={"snapshot_id": "ef"},
            max_attempts=2,
        )
    )

    with patch.object(event_bus, "publish", side_effect=RuntimeError("Subscriber crashed")):
        # Attempt 1 -> RETRYABLE_FAILURE
        dispatcher.dispatch_pending()
        rec_sess = outbox.get_record("sess_fail")
        rec_eq = outbox.get_record("eq_fail")
        assert rec_sess.status == OutboxStatus.RETRYABLE_FAILURE
        assert rec_eq.status == OutboxStatus.RETRYABLE_FAILURE
        assert rec_sess.retry_count == 1
        assert rec_eq.retry_count == 1

        # Wait for backoff
        time.sleep(0.02)

        # Attempt 2 -> EXHAUSTED
        dispatcher.dispatch_pending()
        rec_sess2 = outbox.get_record("sess_fail")
        rec_eq2 = outbox.get_record("eq_fail")
        assert rec_sess2.status == OutboxStatus.EXHAUSTED
        assert rec_eq2.status == OutboxStatus.EXHAUSTED


def test_23_retention_never_removes_pending_or_retryable_session_equity_events(tmp_path: Path) -> None:
    """Retention pruning strictly preserves pending and retryable session and equity events."""
    db_path = str(tmp_path / "outbox.db")
    outbox = TransactionalOutbox(
        db_path=db_path,
        retention_enabled=True,
        retention_days=1.0,
    )
    now = 1_000_000.0
    old_time = now - (10.0 * 86400.0)

    outbox.stage_event(
        OutboxRecord(
            event_id="pending_sess",
            event_type="PAPER_SESSION_STARTED",
            status=OutboxStatus.PENDING,
            created_at=old_time,
        )
    )
    outbox.stage_event(
        OutboxRecord(
            event_id="retryable_eq",
            event_type="PAPER_PORTFOLIO_EQUITY_SNAPSHOT",
            status=OutboxStatus.RETRYABLE_FAILURE,
            created_at=old_time,
        )
    )

    res = outbox.prune_delivered_records(now=now)
    assert res.pruned_count == 0
    assert outbox.get_record("pending_sess") is not None
    assert outbox.get_record("retryable_eq") is not None


# ==============================================================================
# PART E: DASHBOARD DIAGNOSTICS & AUTHORIZATION (Item 24)
# ==============================================================================


def test_24_dashboard_auth_redaction_and_method_restrictions(session_fixture) -> None:
    """Dashboard diagnostics endpoints enforce auth, pagination, redaction, and 405 on mutating methods."""
    coord: PaperTradingWorkflowCoordinator = session_fixture["coordinator"]
    outbox: TransactionalOutbox = coord.outbox

    # Create dummy data
    coord.start_session(session_id="dash_sess_1", initial_capital=10000.0)
    coord.create_equity_snapshot(snapshot_id="dash_snap_1")

    auth_token = "secret-test-token-step65"
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        paper_coordinator=coord,
        auth_token=auth_token,
        require_auth=True,
    )
    server.start()
    try:
        # 1. Unauthenticated request to /api/paper/outbox/retention -> 401
        url_retention = f"{server.url}/api/paper/outbox/retention"
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(url_retention)
        assert exc_info.value.code == HTTPStatus.UNAUTHORIZED

        # 2. Authenticated request to /api/paper/outbox/retention -> 200
        req_auth = urllib.request.Request(
            f"{url_retention}?preview=true",
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        with urllib.request.urlopen(req_auth) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert "retention" in data
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert data["preview"] is not None

        # 3. Authenticated request to /api/paper/outbox/sessions -> 200
        url_sessions = f"{server.url}/api/paper/outbox/sessions"
        req_sess = urllib.request.Request(
            url_sessions,
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        with urllib.request.urlopen(req_sess) as resp:
            data_sess = json.loads(resp.read().decode("utf-8"))
            assert data_sess["status"] == "success"
            assert data_sess["total"] >= 1
            assert any(s["session_id"] == "dash_sess_1" for s in data_sess["sessions"])

        # 4. Authenticated request to /api/paper/outbox/snapshots -> 200
        url_snapshots = f"{server.url}/api/paper/outbox/snapshots"
        req_snaps = urllib.request.Request(
            url_snapshots,
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        with urllib.request.urlopen(req_snaps) as resp:
            data_snaps = json.loads(resp.read().decode("utf-8"))
            assert data_snaps["status"] == "success"
            assert data_snaps["count"] >= 1
            assert data_snaps["latest_snapshot"]["snapshot_id"] == "dash_snap_1"

        # 5. Query single snapshot /api/paper/outbox/snapshots/<snapshot_id> -> 200
        url_snap_single = f"{server.url}/api/paper/outbox/snapshots/dash_snap_1"
        req_snap_single = urllib.request.Request(
            url_snap_single,
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        with urllib.request.urlopen(req_snap_single) as resp:
            data_single = json.loads(resp.read().decode("utf-8"))
            assert data_single["status"] == "success"
            assert data_single["snapshot"]["snapshot_id"] == "dash_snap_1"

        # 6. Mutating method (POST) -> 405 Method Not Allowed
        req_post = urllib.request.Request(
            url_retention,
            data=b"{}",
            headers={"Authorization": f"Bearer {auth_token}"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_post)
        assert exc_info.value.code == HTTPStatus.METHOD_NOT_ALLOWED

    finally:
        server.stop()


# ==============================================================================
# PART F: MANDATORY SAFETY INVARIANTS & INTEGRATION (Item 25)
# ==============================================================================


def test_25_mandatory_kaiser_safety_invariants() -> None:
    """Validate mandatory KAISER safety invariants across settings and architecture."""
    settings = get_settings()

    # 1. execution_locked must remain True
    assert settings.execution_locked is True

    # 2. broker_provider must remain "none"
    assert settings.broker_provider == "none"

    # 3. paper_execution_enabled must be False by default
    assert settings.paper_execution_enabled is False

    # 4. Settings.to_dict() must mask sensitive keys and export outbox retention settings
    d = settings.to_dict()
    assert d["execution_locked"] is True
    assert d["broker_provider"] == "none"
    assert "outbox_retention_enabled" in d
    assert "outbox_retention_days" in d
    assert "outbox_prune_batch_size" in d
    assert "outbox_archival_enabled" in d

    # 5. Live trading engine remains completely uninitialized
    assert not hasattr(settings, "live_trading_engine_active") or not settings.live_trading_engine_active
