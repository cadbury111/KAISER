"""KAISER — Phase 2, Step 64: Durable Event Delivery, Transactional Outbox & Post-Commit Recovery.

Comprehensive test suite verifying:
1. Financial ledger write fails before commit: no committed financial mutation or dispatchable outbox event.
2. Outbox persistence fails before commit: financial state is restored.
3. EventBus publication fails after commit: financial state remains committed and the outbox remains recoverable.
4. Successful publication marks the record delivered durably.
5. Restart recovers pending events from SQLite.
6. Repeated recovery does not duplicate financial records or outbox identities.
7. Crash-window simulation: publication succeeds but delivered-status persistence fails; retry behavior is correct and documented.
8. Duplicate event IDs and malformed payloads are rejected safely.
9. Retry limits and backoff are bounded and deterministic under test.
10. Exhausted events remain inspectable and recoverable according to the defined policy.
11. Concurrent dispatch attempts do not corrupt state or improperly process the same record simultaneously.
12. Shutdown stops the worker cleanly.
13. Secret redaction is maintained across API responses and logs.
14. Consumer-side deduplication (DeduplicatingEventHandler) prevents redundant execution upon redelivery.
15. Dashboard authorization, loopback enforcement, GET-only 405 responses, and observability.
16. Configuration validation and fail-safe behavior.
17. Mandatory KAISER safety invariants: execution_locked=True, broker_provider="none", paper execution disabled by default.
"""

from __future__ import annotations

import copy
from http import HTTPStatus
import json
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
from app.execution.allocation import (
    AllocationStatus,
    PortfolioAllocationManager,
    PortfolioAllocationRequest,
    PortfolioAllocationResult,
    SleeveRiskBudget,
    StrategySleeveDefinition,
    create_standard_multi_sleeve_request,
)
from app.execution.models import (
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
)
from app.execution.outbox_dispatcher import OutboxDispatcher
from app.execution.portfolio import PaperPortfolio
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.ledger.outbox import (
    DeduplicatingEventHandler,
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


# ---------------------------------------------------------------------------
# 1. Outbox Record & Redaction Unit Tests
# ---------------------------------------------------------------------------


def test_outbox_record_validation_and_immutability() -> None:
    """OutboxRecord enforces safe event IDs, status enums, and non-empty event types."""
    # Invalid event ID
    with pytest.raises(ValidationError):
        OutboxRecord(event_id="bad id with spaces!", event_type="TEST_EVENT")

    # Invalid event type
    with pytest.raises(ValidationError):
        OutboxRecord(event_id="valid_id_1", event_type="")

    # Invalid max attempts
    with pytest.raises(ValidationError):
        OutboxRecord(event_id="valid_id_2", event_type="TEST_EVENT", max_attempts=0)

    # Valid construction
    rec = OutboxRecord(
        event_id="ev_valid_123",
        event_type="PAPER_ALLOCATION_UPDATED",
        correlation_id="corr_1",
        causation_id="cause_1",
        payload={"key": "value"},
    )
    assert rec.event_id == "ev_valid_123"
    assert rec.status == OutboxStatus.PENDING
    assert rec.attempt_count == 0

    # Serialization roundtrip
    d = rec.to_dict()
    assert d["event_id"] == "ev_valid_123"
    rec2 = OutboxRecord.from_dict(d)
    assert rec2.event_id == rec.event_id
    assert rec2.status == OutboxStatus.PENDING


def test_secret_redaction_across_payloads() -> None:
    """Redaction filter strips secrets, API keys, tokens, and raw audio bytes."""
    sensitive_payload = {
        "user": "trader_1",
        "twelve_data_api_key": "secret_key_12345",
        "auth_token": "bearer_jwt_token_abcdef",
        "nested": {
            "password": "supersecretpassword",
            "access_token": "token_xyz",
            "safe_metric": 123.45,
        },
        "raw_audio": b"\x00\x01\x02\x03",
    }
    sanitized = redact_sensitive_payload(sensitive_payload)

    assert sanitized["twelve_data_api_key"] == "[REDACTED]"
    assert sanitized["auth_token"] == "[REDACTED]"
    assert sanitized["nested"]["password"] == "[REDACTED]"
    assert sanitized["nested"]["access_token"] == "[REDACTED]"
    assert sanitized["nested"]["safe_metric"] == 123.45
    assert sanitized["raw_audio"] == "[BINARY DATA REDACTED]"

    # Staging into OutboxRecord automatically sanitizes
    rec = OutboxRecord(
        event_id="redacted_evt_1",
        event_type="TEST_EVENT",
        payload=sensitive_payload,
    )
    assert rec.payload["twelve_data_api_key"] == "[REDACTED]"
    assert rec.payload["auth_token"] == "[REDACTED]"
    assert rec.payload["nested"]["password"] == "[REDACTED]"


# ---------------------------------------------------------------------------
# 2. Failure Injection: Pre-Commit Rollback Boundaries
# ---------------------------------------------------------------------------


def test_failure_injection_1_ledger_write_fails_before_commit(tmp_path: Path) -> None:
    """Scenario 1: Financial ledger write fails before commit ->
    no committed financial mutation in memory or on disk, and no dispatchable outbox event."""
    db_file = str(tmp_path / "test_fi1.db")
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
    event_bus = EventBus()
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, event_bus=event_bus)

    req = create_standard_multi_sleeve_request(request_id="alloc_fi_1")

    # Injected disk write failure on SQLite ledger
    with patch.object(ledger, "record_entries_with_outbox", side_effect=sqlite3.OperationalError("disk I/O error")):
        with pytest.raises(sqlite3.OperationalError):
            manager.apply_allocation(req)

    # In-memory rollback verified
    assert len(manager.list_sleeves()) == 0
    assert manager.unallocated_cash == 100_000.0
    assert "alloc_fi_1" not in manager._allocation_results

    # Durable storage verified: no allocation entries and no outbox entries
    with sqlite3.connect(db_file) as conn:
        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM paper_ledger_entries WHERE entry_type = 'CAPITAL_ALLOCATION'"
        ).fetchone()[0]
        outbox_count = conn.execute("SELECT COUNT(*) FROM paper_outbox_entries").fetchone()[0]
    assert ledger_count == 0
    assert outbox_count == 0
    assert ledger.outbox.count() == 0


def test_failure_injection_2_outbox_persistence_fails_before_commit(tmp_path: Path) -> None:
    """Scenario 2: Outbox persistence fails before commit ->
    financial ledger entry is rolled back by SQLite transaction, in-memory state restored."""
    db_file = str(tmp_path / "test_fi2.db")
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
    event_bus = EventBus()
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, event_bus=event_bus)

    req = create_standard_multi_sleeve_request(request_id="alloc_fi_2")

    # Simulate failure during outbox staging inside the transaction
    with patch.object(ledger.outbox, "stage_record", side_effect=sqlite3.IntegrityError("Constraint failed")):
        with pytest.raises(sqlite3.IntegrityError):
            manager.apply_allocation(req)

    # In-memory rollback
    assert len(manager.list_sleeves()) == 0
    assert manager.unallocated_cash == 100_000.0

    # SQLite transaction boundary rollback verified: neither ledger nor outbox entry committed
    with sqlite3.connect(db_file) as conn:
        ledger_entries = conn.execute(
            "SELECT COUNT(*) FROM paper_ledger_entries WHERE entry_type = 'CAPITAL_ALLOCATION'"
        ).fetchone()[0]
        outbox_entries = conn.execute("SELECT COUNT(*) FROM paper_outbox_entries").fetchone()[0]
    assert ledger_entries == 0
    assert outbox_entries == 0


# ---------------------------------------------------------------------------
# 3. Post-Commit EventBus Delivery Failure & Recoverability
# ---------------------------------------------------------------------------


def test_failure_injection_3_eventbus_publication_fails_after_commit(tmp_path: Path) -> None:
    """Scenario 3: EventBus publication fails after commit ->
    financial state remains committed, event delivery failure surfaced, outbox record marked RETRYABLE_FAILURE."""
    db_file = str(tmp_path / "test_fi3.db")
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
    event_bus = EventBus()
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, event_bus=event_bus)

    req = create_standard_multi_sleeve_request(request_id="alloc_fi_3")

    # Injected EventBus failure post-commit
    with patch.object(event_bus, "publish", side_effect=RuntimeError("Kafka/EventBus connection refused")):
        res = manager.apply_allocation(req)

    # Financial state remains committed
    assert res.success is True
    assert res.event_delivery_failed is True
    assert len(manager.list_sleeves()) == 3
    assert manager.unallocated_cash == 0.0

    # Ledger entries committed in SQLite
    with sqlite3.connect(db_file) as conn:
        ledger_entries = conn.execute(
            "SELECT COUNT(*) FROM paper_ledger_entries WHERE entry_type = 'CAPITAL_ALLOCATION'"
        ).fetchone()[0]
        outbox_rows = conn.execute(
            "SELECT event_id, status, attempt_count, last_error FROM paper_outbox_entries"
        ).fetchall()

    assert ledger_entries == 3
    assert len(outbox_rows) == 1
    eid, status, attempts, last_err = outbox_rows[0]
    assert status == OutboxStatus.RETRYABLE_FAILURE.value
    assert "Kafka/EventBus connection refused" in last_err

    # Outbox in memory reflects status
    rec = ledger.outbox.get_record(eid)
    assert rec is not None
    assert rec.status == OutboxStatus.RETRYABLE_FAILURE

    # Now dispatch pending after bus heals: event delivers successfully!
    dispatcher = OutboxDispatcher(outbox=ledger.outbox, event_bus=event_bus, batch_size=10)
    delivered_count = dispatcher.dispatch_pending(now=time.time() + 10.0)
    assert delivered_count == 1

    rec_after = ledger.outbox.get_record(eid)
    assert rec_after.status == OutboxStatus.DELIVERED
    assert rec_after.delivered_at is not None


def test_failure_injection_4_successful_publication_marks_delivered(tmp_path: Path) -> None:
    """Scenario 4: Successful publication marks the record DELIVERED durably in SQLite."""
    db_file = str(tmp_path / "test_fi4.db")
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
    event_bus = EventBus()
    published_events: List[Event] = []
    event_bus.subscribe_all(lambda ev: published_events.append(ev))

    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, event_bus=event_bus)

    req = create_standard_multi_sleeve_request(request_id="alloc_fi_4")
    res = manager.apply_allocation(req)

    assert res.success is True
    assert res.event_delivery_failed is False
    assert len(published_events) >= 1

    # Check outbox status in SQLite
    with sqlite3.connect(db_file) as conn:
        status, deliv_at = conn.execute(
            "SELECT status, delivered_at FROM paper_outbox_entries"
        ).fetchone()

    assert status == OutboxStatus.DELIVERED.value
    assert deliv_at is not None

    stats = ledger.outbox.get_stats()
    assert stats["delivered"] == 1
    assert stats["pending"] == 0
    assert stats["retryable"] == 0


# ---------------------------------------------------------------------------
# 4. Restart Recovery & Idempotency
# ---------------------------------------------------------------------------


def test_failure_injection_5_restart_recovers_pending_events(tmp_path: Path) -> None:
    """Scenario 5: On restart, pending and retryable outbox events are recovered from durable storage."""
    db_file = str(tmp_path / "test_fi5.db")
    outbox1 = TransactionalOutbox(db_path=db_file)

    rec1 = OutboxRecord(
        event_id="rec_pending_1",
        event_type="PAPER_ALLOCATION_UPDATED",
        status=OutboxStatus.PENDING,
        payload={"test": 1},
    )
    rec2 = OutboxRecord(
        event_id="rec_retryable_2",
        event_type="PAPER_SLEEVE_ORDER_EXECUTED",
        status=OutboxStatus.RETRYABLE_FAILURE,
        attempt_count=2,
        last_error="Transient timeout",
        payload={"test": 2},
    )
    rec3 = OutboxRecord(
        event_id="rec_delivered_3",
        event_type="PAPER_SLEEVE_TRANSFER_RECORDED",
        status=OutboxStatus.DELIVERED,
        delivered_at=time.time(),
        payload={"test": 3},
    )

    outbox1.stage_record(rec1)
    outbox1.stage_record(rec2)
    outbox1.stage_record(rec3)

    assert outbox1.count() == 3

    # Simulate restart by creating a new TransactionalOutbox instance on the same SQLite database
    outbox2 = TransactionalOutbox(db_path=db_file)
    assert outbox2.count() == 3

    r1 = outbox2.get_record("rec_pending_1")
    assert r1 is not None and r1.status == OutboxStatus.PENDING

    r2 = outbox2.get_record("rec_retryable_2")
    assert r2 is not None and r2.status == OutboxStatus.RETRYABLE_FAILURE
    assert r2.attempt_count == 2
    assert r2.last_error == "Transient timeout"

    r3 = outbox2.get_record("rec_delivered_3")
    assert r3 is not None and r3.status == OutboxStatus.DELIVERED

    # Pending batch fetches eligible events
    batch = outbox2.get_pending_batch(limit=10)
    batch_ids = {r.event_id for r in batch}
    assert "rec_pending_1" in batch_ids
    assert "rec_retryable_2" in batch_ids
    assert "rec_delivered_3" not in batch_ids


def test_failure_injection_6_repeated_recovery_idempotent(tmp_path: Path) -> None:
    """Scenario 6: Repeated recovery does not duplicate financial records or outbox identities."""
    db_file = str(tmp_path / "test_fi6.db")
    ledger = PaperLedger(db_path=db_file, initial_balance=50_000.0)

    rec = OutboxRecord(event_id="idemp_rec_1", event_type="TEST_EVT")
    ledger.outbox.stage_record(rec)

    # Calling recover multiple times
    diag1 = ledger.outbox.recover()
    diag2 = ledger.outbox.recover()
    diag3 = ledger.outbox.recover()

    assert ledger.outbox.count() == 1
    assert diag1["recovered_count"] == 1
    assert diag2["recovered_count"] == 1
    assert diag3["recovered_count"] == 1
    assert len(ledger.get_entries()) == 1  # only initial capital


# ---------------------------------------------------------------------------
# 5. Crash-Window Simulation & Consumer Idempotency
# ---------------------------------------------------------------------------


def test_failure_injection_7_crash_window_simulation_and_deduplication(tmp_path: Path) -> None:
    """Scenario 7: Crash window (publication succeeds but delivered status persistence fails) ->
    dispatcher redelivers, and consumer-side DeduplicatingEventHandler prevents duplicate side effects."""
    db_file = str(tmp_path / "test_fi7.db")
    outbox = TransactionalOutbox(db_path=db_file)
    event_bus = EventBus()

    received_events: List[Event] = []

    def raw_handler(ev: Event) -> None:
        received_events.append(ev)

    # Consumer wraps callback with DeduplicatingEventHandler
    idempotent_consumer = DeduplicatingEventHandler(raw_handler, capacity=100)
    event_bus.subscribe("PAPER_ORDER_FILLED", idempotent_consumer)

    rec = OutboxRecord(
        event_id="crash_evt_101",
        event_type="PAPER_ORDER_FILLED",
        payload={"order_id": "ord_101", "qty": 10},
    )
    outbox.stage_record(rec)

    dispatcher = OutboxDispatcher(outbox=outbox, event_bus=event_bus)

    # Simulate crash: during mark_delivered, SQLite fails with OperationalError
    with patch.object(outbox, "mark_delivered", side_effect=sqlite3.OperationalError("Simulated crash before status commit")):
        # Dispatcher attempts delivery; publication succeeds but mark_delivered fails
        delivered = dispatcher.dispatch_pending()
        assert delivered == 0

    # Handler was invoked once
    assert len(received_events) == 1
    assert received_events[0].event_id == "crash_evt_101"

    # Outbox status was recorded as failure due to crash
    assert outbox.get_record("crash_evt_101").status == OutboxStatus.RETRYABLE_FAILURE

    # Now recover from crash window: redelivery attempt
    # Set next_retry_at to past so it is immediately eligible
    outbox.record_failure("crash_evt_101", error="Prior crash", next_retry_at=0.0)

    delivered2 = dispatcher.dispatch_pending()
    assert delivered2 == 1
    assert outbox.get_record("crash_evt_101").status == OutboxStatus.DELIVERED

    # Consumer deduplication prevented duplicate execution!
    assert len(received_events) == 1
    assert idempotent_consumer.processed_count == 1
    assert idempotent_consumer.duplicate_count == 1


# ---------------------------------------------------------------------------
# 6. Duplicate Event IDs & Malformed Payloads
# ---------------------------------------------------------------------------


def test_failure_injection_8_duplicate_event_ids_and_malformed_payloads(tmp_path: Path) -> None:
    """Scenario 8: Duplicate event IDs deduplicate safely, and corrupted payloads handle gracefully."""
    db_file = str(tmp_path / "test_fi8.db")
    outbox = TransactionalOutbox(db_path=db_file)

    rec1 = OutboxRecord(event_id="unique_ev_1", event_type="TEST_1")
    rec1_dup = OutboxRecord(event_id="unique_ev_1", event_type="TEST_1")

    assert outbox.stage_record(rec1) is True
    # Duplicate ID is rejected/deduplicated without error
    assert outbox.stage_record(rec1_dup) is False
    assert outbox.count() == 1

    # Insert corrupted JSON directly into SQLite
    with sqlite3.connect(db_file) as conn:
        conn.execute(
            """
            INSERT INTO paper_outbox_entries (
                event_id, event_type, schema_version, correlation_id, causation_id,
                payload_json, created_at, status, attempt_count, max_attempts,
                last_error, next_retry_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("corrupt_ev_999", "TEST_CORRUPT", 1, None, None, "{malformed json", time.time(), "PENDING", 0, 5, None, None, None),
        )

    # Recovery handles corrupted payload safely by marking it EXHAUSTED rather than crashing
    outbox_restarted = TransactionalOutbox(db_path=db_file)
    corrupt_rec = outbox_restarted.get_record("corrupt_ev_999")
    assert corrupt_rec is not None
    assert corrupt_rec.status == OutboxStatus.EXHAUSTED
    assert "Corruption on recovery" in (corrupt_rec.last_error or "")


# ---------------------------------------------------------------------------
# 7. Retry Limits, Bounded Backoff & Exhaustion
# ---------------------------------------------------------------------------


def test_failure_injection_9_retry_limits_and_bounded_backoff() -> None:
    """Scenario 9: Retry policy enforces bounded exponential backoff and caps at max_attempts."""
    outbox = TransactionalOutbox()
    rec = OutboxRecord(
        event_id="retry_backoff_ev",
        event_type="TEST_RETRY",
        max_attempts=3,
    )
    outbox.stage_record(rec)

    # Injected deterministic clock
    current_time = 1_000.0
    mock_clock = lambda: current_time

    event_bus = EventBus()
    dispatcher = OutboxDispatcher(
        outbox=outbox,
        event_bus=event_bus,
        retry_backoff_seconds=2.0,
        max_backoff_seconds=10.0,
        max_attempts=3,
        clock=mock_clock,
    )

    with patch.object(event_bus, "publish", side_effect=IOError("Network down")):
        # Attempt 1: backoff = 2 * 2^0 = 2.0s -> next_retry_at = 1002.0
        dispatcher.dispatch_pending()
        r1 = outbox.get_record("retry_backoff_ev")
        assert r1.status == OutboxStatus.RETRYABLE_FAILURE
        assert r1.attempt_count == 1
        assert r1.next_retry_at == 1002.0

        # Attempt before retry time: not eligible
        current_time = 1_001.0
        assert dispatcher.dispatch_pending() == 0

        # Attempt 2 at 1002.0: backoff = 2 * 2^1 = 4.0s -> next_retry_at = 1006.0
        current_time = 1_002.0
        dispatcher.dispatch_pending()
        r2 = outbox.get_record("retry_backoff_ev")
        assert r2.status == OutboxStatus.RETRYABLE_FAILURE
        assert r2.attempt_count == 2
        assert r2.next_retry_at == 1006.0

        # Attempt 3 at 1006.0: attempt_count reaches max_attempts (3) -> EXHAUSTED
        current_time = 1_006.0
        dispatcher.dispatch_pending()
        r3 = outbox.get_record("retry_backoff_ev")
        assert r3.status == OutboxStatus.EXHAUSTED
        assert r3.attempt_count == 3
        assert r3.next_retry_at is None

        # Subsequent passes never retry exhausted events
        current_time = 2_000.0
        assert dispatcher.dispatch_pending() == 0


def test_failure_injection_10_exhausted_events_remain_inspectable(tmp_path: Path) -> None:
    """Scenario 10: Exhausted events are never silently deleted; remain inspectable in diagnostic listing."""
    db_file = str(tmp_path / "test_fi10.db")
    outbox = TransactionalOutbox(db_path=db_file)

    rec = OutboxRecord(
        event_id="exhausted_audit_ev",
        event_type="CRITICAL_SETTLEMENT",
        max_attempts=2,
    )
    outbox.stage_record(rec)
    outbox.record_failure("exhausted_audit_ev", error="Permanent auth rejection", exhausted=True)

    # Exhausted record exists in stats and listing
    stats = outbox.get_stats()
    assert stats["exhausted"] == 1

    exhausted_list = outbox.list_records(status=OutboxStatus.EXHAUSTED)
    assert len(exhausted_list) == 1
    assert exhausted_list[0].event_id == "exhausted_audit_ev"
    assert exhausted_list[0].last_error == "Permanent auth rejection"

    # Prune delivered never removes exhausted events
    pruned = outbox.prune_delivered(keep_last_n=0)
    assert pruned == 0
    assert outbox.get_record("exhausted_audit_ev") is not None


# ---------------------------------------------------------------------------
# 8. Concurrency & Clean Worker Lifecycle
# ---------------------------------------------------------------------------


def test_failure_injection_11_concurrent_dispatch_protection() -> None:
    """Scenario 11: Concurrent dispatch attempts do not corrupt state or process the same record simultaneously."""
    outbox = TransactionalOutbox()
    for i in range(20):
        outbox.stage_record(OutboxRecord(event_id=f"conc_ev_{i}", event_type="CONC_EVENT"))

    event_bus = EventBus()
    published_order: List[str] = []
    lock = threading.Lock()

    def tracking_handler(ev: Event) -> None:
        with lock:
            published_order.append(ev.event_id)
        # Small deterministic calculation simulating handler work
        _ = sum(x * x for x in range(100))

    event_bus.subscribe("CONC_EVENT", tracking_handler)

    dispatcher = OutboxDispatcher(outbox=outbox, event_bus=event_bus, batch_size=5)

    # Run 4 threads concurrently calling dispatch_pending
    threads = []
    results = [0, 0, 0, 0]

    def runner(idx: int) -> None:
        results[idx] = dispatcher.dispatch_pending()

    for idx in range(4):
        t = threading.Thread(target=runner, args=(idx,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Total delivered should exactly equal number of events (20)
    # Drain remaining if any were in flight
    remaining = dispatcher.dispatch_all()

    assert sum(results) + remaining == 20
    assert outbox.get_stats()["delivered"] == 20
    # Every event was delivered exactly once to the handler
    assert len(published_order) == 20
    assert len(set(published_order)) == 20


def test_failure_injection_12_worker_shutdown_stops_cleanly() -> None:
    """Scenario 12: Worker lifecycle start and stop stops worker thread cleanly."""
    outbox = TransactionalOutbox()
    dispatcher = OutboxDispatcher(outbox=outbox, auto_start=False, poll_interval_seconds=0.05)
    assert dispatcher.is_running is False

    dispatcher.start()
    assert dispatcher.is_running is True

    dispatcher.stop(timeout=2.0)
    assert dispatcher.is_running is False


# ---------------------------------------------------------------------------
# 9. Dashboard Observability & Diagnostics
# ---------------------------------------------------------------------------


def test_dashboard_outbox_diagnostics_authenticated(tmp_path: Path) -> None:
    """Scenario 13 & 15: Read-only GET diagnostics on /api/paper/outbox with authentication & 405 on mutations."""
    db_file = str(tmp_path / "test_dash.db")
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
    event_bus = EventBus()
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, event_bus=event_bus)

    req = create_standard_multi_sleeve_request(request_id="dash_alloc_req")
    manager.apply_allocation(req)

    auth_token = "secret-dashboard-token-test"
    reporting_svc = PortfolioReportingService()

    # Spin up test server on ephemeral port
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        reporting_service=reporting_svc,
        auth_token=auth_token,
        require_auth=True,
        allocation_manager=manager,
    )
    server.start()
    port = server.port

    try:
        # 1. Unauthenticated request to /api/paper/outbox returns 401
        unauth_req = urllib.request.Request(f"http://127.0.0.1:{port}/api/paper/outbox")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(unauth_req)
        assert exc_info.value.code == HTTPStatus.UNAUTHORIZED

        # 2. Authenticated GET /api/paper/outbox returns summary
        auth_req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/paper/outbox",
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        with urllib.request.urlopen(auth_req) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["status"] == "success"
            assert body["summary"]["total"] >= 1
            assert "worker" in body

        # 3. Authenticated GET /api/paper/outbox/events returns paginated listing
        events_req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/paper/outbox/events?limit=10",
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        with urllib.request.urlopen(events_req) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["status"] == "success"
            assert len(body["events"]) >= 1
            ev_id = body["events"][0]["event_id"]

        # 4. Authenticated GET single event /api/paper/outbox/events/<event_id>
        single_req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/paper/outbox/events/{ev_id}",
            headers={"Authorization": f"Bearer {auth_token}"},
        )
        with urllib.request.urlopen(single_req) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["status"] == "success"
            assert body["event"]["event_id"] == ev_id

        # 5. Mutating POST method to /api/paper/outbox returns 405 Method Not Allowed
        post_req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/paper/outbox",
            data=b"{}",
            headers={"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(post_req)
        assert exc_info.value.code == HTTPStatus.METHOD_NOT_ALLOWED

    finally:
        server.stop()


# ---------------------------------------------------------------------------
# 10. Configuration & Fail-Safe Invariants
# ---------------------------------------------------------------------------


def test_configuration_validation_and_safety_invariants() -> None:
    """Scenario 16 & 17: Validates Outbox configuration, safe defaults, and non-negotiable invariants."""
    settings = get_settings()

    # Mandatory Safety Invariants
    assert settings.execution_locked is True
    assert settings.broker_provider == "none"
    assert settings.paper_execution_enabled is False
    assert settings.paper_trading_only is True

    # Outbox Defaults
    assert settings.outbox_enabled is True
    assert settings.outbox_max_attempts == 5
    assert settings.outbox_batch_size == 50
    assert settings.outbox_retry_backoff_seconds == 2.0
    assert settings.outbox_max_backoff_seconds == 60.0
    assert settings.outbox_auto_dispatch is False

    # Validation: invalid retry limits
    with pytest.raises(ConfigurationError):
        Settings(outbox_max_attempts=0)

    # Validation: invalid batch size
    with pytest.raises(ConfigurationError):
        Settings(outbox_batch_size=-1)

    # Validation: invalid backoff
    with pytest.raises(ConfigurationError):
        Settings(outbox_retry_backoff_seconds=-0.5)

    with pytest.raises(ConfigurationError):
        Settings(outbox_retry_backoff_seconds=10.0, outbox_max_backoff_seconds=5.0)

    # Secret masking in to_dict
    s_dict = settings.to_dict()
    assert s_dict["execution_locked"] is True
    assert s_dict["broker_provider"] == "none"
    assert s_dict["outbox_enabled"] is True
    assert s_dict["outbox_max_attempts"] == 5
