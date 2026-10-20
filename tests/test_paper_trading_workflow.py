"""KAISER — Tests for Phase 2, Step 55: Controlled Paper Trading Workflow & Simulation Orchestration.

Comprehensive test suite verifying:
1. Workflow disabled by default.
2. Explicitly enabled valid paper-order request.
3. Invalid order request rejection.
4. Missing quote rejection.
5. Stale or invalid quote rejection.
6. Successful simulated market-order lifecycle.
7. Accepted pending limit-order lifecycle.
8. Limit order filled by a later eligible quote.
9. Cancellation of a pending order.
10. Rejection of cancellation after fill.
11. Duplicate request and idempotency handling.
12. Correlation ID propagation.
13. Consistent order, fill, portfolio, and ledger state.
14. Rejected order leaves financial state unchanged.
15. Duplicate event processing does not duplicate fills.
16. Ledger persistence failure is surfaced.
17. Reconciliation failure is surfaced.
18. Recovery after a completed order.
19. Recovery with a pending limit order.
20. Session creation and termination.
21. Session summaries match portfolio and ledger state.
22. Dashboard summary is read-only.
23. Existing paper-execution risk controls remain enforced.
24. Global execution lock remains enabled.
25. Live trading engine remains uninitialized.
26. No broker provider is registered.
27. Existing mock and replay pipelines remain functional.
"""

from __future__ import annotations

import json
from pathlib import Path
import time
import urllib.request
import urllib.error
import pytest

from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.execution import (
    PaperExecutionEngine,
    PaperExecutionResult,
    PaperFill,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
    PaperPortfolio,
    PaperRejectionCode,
    PaperRiskConfig,
    PaperSimulationSession,
    PaperTradingWorkflowCoordinator,
    PaperWorkflowOutcome,
    PaperWorkflowRequest,
    WorkflowStage,
    WorkflowStatus,
)
from app.ledger import LedgerEntryType, PaperLedger, PaperLedgerEntry
from app.market.models import Bar, Quote
from app.market.service import MarketDataService
from app.portfolio.dashboard import PortfolioDashboardServer


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_wf_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    return StateStore(name="test_wf_state_store")


@pytest.fixture
def fresh_quote() -> Quote:
    now = time.time()
    return Quote(
        symbol="RELIANCE",
        exchange="NSE",
        timestamp=now,
        last_price=2500.0,
        bid=2499.5,
        ask=2500.5,
        bid_size=100.0,
        ask_size=100.0,
        volume=50000.0,
        source="mock",
    )


@pytest.fixture
def paper_ledger(event_bus: EventBus, tmp_path: Path) -> PaperLedger:
    db_file = str(tmp_path / "paper_wf_ledger.db")
    return PaperLedger(event_bus=event_bus, db_path=db_file, initial_balance=100_000.0)


@pytest.fixture
def paper_engine(
    event_bus: EventBus,
    state_store: StateStore,
    paper_ledger: PaperLedger,
    fresh_quote: Quote,
) -> PaperExecutionEngine:
    risk_config = PaperRiskConfig(
        max_order_notional=50_000.0,
        max_position_exposure_pct=50.0,
        freshness_threshold_seconds=60.0,
        allow_short_selling=False,
    )
    engine = PaperExecutionEngine(
        event_bus=event_bus,
        state_store=state_store,
        ledger=paper_ledger,
        initial_cash=100_000.0,
        risk_config=risk_config,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        enabled=True,
    )
    engine.on_market_quote(fresh_quote)
    return engine


@pytest.fixture
def coordinator(
    paper_engine: PaperExecutionEngine,
    event_bus: EventBus,
    state_store: StateStore,
) -> PaperTradingWorkflowCoordinator:
    return PaperTradingWorkflowCoordinator(
        engine=paper_engine,
        event_bus=event_bus,
        state_store=state_store,
        enabled=True,
    )


# ==============================================================================
# TESTS (1 to 27)
# ==============================================================================


def test_01_workflow_disabled_by_default(fresh_quote: Quote) -> None:
    """1. Workflow disabled by default."""
    settings = Settings()
    assert settings.paper_execution_enabled is False

    coordinator = PaperTradingWorkflowCoordinator(engine=None, settings=settings)
    assert coordinator.is_enabled is False

    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

    assert outcome.success is False
    assert outcome.status == WorkflowStatus.REJECTED
    assert outcome.stage == WorkflowStage.REQUEST_RECEIVED
    assert outcome.rejection_code == PaperRejectionCode.PAPER_EXECUTION_DISABLED.value
    assert outcome.is_rejected is True


def test_02_explicitly_enabled_valid_paper_order_request(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """2. Explicitly enabled valid paper-order request."""
    assert coordinator.is_enabled is True

    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
        correlation_id="test_corr_02",
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

    assert outcome.success is True
    assert outcome.status == WorkflowStatus.FILLED
    assert outcome.is_filled is True
    assert outcome.is_pending is False
    assert outcome.correlation_id == "test_corr_02"
    assert outcome.order is not None
    assert outcome.fill is not None
    assert outcome.fill.quantity == 5.0
    assert outcome.order.status == PaperOrderStatus.FILLED


def test_03_invalid_order_request_rejection(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """3. Invalid order request rejection."""
    # Negative / zero quantity
    with pytest.raises(ValidationError):
        PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=0.0,
        )

    with pytest.raises(ValidationError):
        PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=-5.0,
        )

    # Missing limit price on limit order
    with pytest.raises(ValidationError):
        PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.LIMIT,
            quantity=5.0,
            limit_price=None,
        )

    # Malicious injection in notes
    with pytest.raises(ValidationError):
        PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            notes="<script>alert(1)</script>",
        )

    with pytest.raises(ValidationError):
        PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            notes="eval('rm -rf /')",
        )


def test_04_missing_quote_rejection(
    coordinator: PaperTradingWorkflowCoordinator,
) -> None:
    """4. Missing quote rejection."""
    req = PaperWorkflowRequest(
        symbol="UNKNOWN_SYM",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    outcome = coordinator.submit_paper_order(req)

    assert outcome.success is False
    assert outcome.status == WorkflowStatus.REJECTED
    assert outcome.stage == WorkflowStage.MARKET_DATA_RESOLUTION
    assert outcome.rejection_code == PaperRejectionCode.MISSING_MARKET_DATA.value


def test_05_stale_or_invalid_quote_rejection(
    coordinator: PaperTradingWorkflowCoordinator,
) -> None:
    """5. Stale or invalid quote rejection."""
    now = time.time()

    # Stale quote
    stale_quote = Quote(
        symbol="RELIANCE",
        exchange="NSE",
        timestamp=now - 500.0,  # 500s old > 60s threshold
        last_price=2500.0,
        bid=2499.0,
        ask=2501.0,
    )
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=stale_quote, reference_time=now)
    assert outcome.status == WorkflowStatus.REJECTED
    assert outcome.rejection_code == PaperRejectionCode.STALE_MARKET_DATA.value

    # Inverted spread quote validation upon creation
    with pytest.raises(ValidationError):
        Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=now,
            last_price=2500.0,
            bid=2505.0,  # bid > ask
            ask=2495.0,
        )

    # Inverted spread quote detected by coordinator
    inverted_quote = Quote(
        symbol="RELIANCE",
        exchange="NSE",
        timestamp=now,
        last_price=2500.0,
        bid=2495.0,
        ask=2505.0,
    )
    object.__setattr__(inverted_quote, "ask", 2490.0)
    outcome = coordinator.submit_paper_order(req, market_quote=inverted_quote, reference_time=now)
    assert outcome.status == WorkflowStatus.REJECTED
    assert outcome.rejection_code == PaperRejectionCode.INVALID_MARKET_DATA.value

    # Symbol mismatch quote
    mismatch_quote = Quote(
        symbol="TCS",
        exchange="NSE",
        timestamp=now,
        last_price=3500.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=mismatch_quote, reference_time=now)
    assert outcome.status == WorkflowStatus.REJECTED
    assert outcome.rejection_code == PaperRejectionCode.INVALID_MARKET_DATA.value


def test_06_successful_simulated_market_order_lifecycle(
    coordinator: PaperTradingWorkflowCoordinator,
    event_bus: EventBus,
    fresh_quote: Quote,
) -> None:
    """6. Successful simulated market-order lifecycle."""
    events_captured: list[Event] = []
    event_bus.subscribe_all(lambda e: events_captured.append(e))

    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=10.0,
        correlation_id="corr_market_06",
    )
    initial_cash = coordinator.engine.portfolio.available_cash
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

    assert outcome.success is True
    assert outcome.status == WorkflowStatus.FILLED
    assert outcome.is_filled is True
    assert outcome.is_pending is False

    # Check cash and portfolio
    expected_spent = outcome.fill.gross_notional + outcome.fill.fee
    assert abs(coordinator.engine.portfolio.available_cash - (initial_cash - expected_spent)) < 0.001
    assert coordinator.engine.portfolio.get_position("RELIANCE").quantity == 10.0

    # Check events emitted
    event_types = [e.event_type for e in events_captured]
    assert EventType.PAPER_WORKFLOW_REQUESTED.value in event_types
    assert EventType.PAPER_WORKFLOW_VALIDATED.value in event_types
    assert EventType.PAPER_ORDER_SUBMITTED.value in event_types
    assert EventType.PAPER_ORDER_ACCEPTED.value in event_types
    assert EventType.PAPER_ORDER_FILLED.value in event_types
    assert EventType.PAPER_WORKFLOW_COMPLETED.value in event_types


def test_07_accepted_pending_limit_order_lifecycle(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """7. Accepted pending limit-order lifecycle."""
    initial_cash = coordinator.engine.portfolio.available_cash

    # Market ask is 2500.5. Limit BUY at 2400.0 cannot fill immediately.
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.LIMIT,
        quantity=5.0,
        limit_price=2400.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

    assert outcome.success is True
    assert outcome.status == WorkflowStatus.ACCEPTED
    assert outcome.is_pending is True
    assert outcome.is_filled is False
    assert outcome.fill is None
    assert outcome.order is not None
    assert outcome.order.status == PaperOrderStatus.ACCEPTED

    # Cash must NOT be deducted yet
    assert coordinator.engine.portfolio.available_cash == initial_cash
    assert coordinator.engine.portfolio.get_position("RELIANCE") is None


def test_08_limit_order_filled_by_later_eligible_quote(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """8. Limit order filled by a later eligible quote."""
    # Submit limit BUY at 2400.0
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.LIMIT,
        quantity=5.0,
        limit_price=2400.0,
    )
    outcome1 = coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome1.is_pending is True

    # Later eligible quote arrives where ask drops to 2390.0 <= 2400.0
    later_quote = Quote(
        symbol="RELIANCE",
        exchange="NSE",
        timestamp=time.time(),
        last_price=2390.0,
        bid=2389.5,
        ask=2390.0,
        volume=60000.0,
    )
    completed_outcomes = coordinator.on_market_quote(later_quote)

    assert len(completed_outcomes) == 1
    filled_outcome = completed_outcomes[0]
    assert filled_outcome.status == WorkflowStatus.FILLED
    assert filled_outcome.is_filled is True
    assert filled_outcome.fill is not None
    assert filled_outcome.fill.price <= 2400.0

    # Portfolio now has the position
    pos = coordinator.engine.portfolio.get_position("RELIANCE")
    assert pos is not None
    assert pos.quantity == 5.0


def test_09_cancellation_of_a_pending_order(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """9. Cancellation of a pending order."""
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.LIMIT,
        quantity=5.0,
        limit_price=2400.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome.is_pending is True
    order_id = outcome.order.order_id

    cancel_outcome = coordinator.cancel_order(order_id, reason="User cancelled limit order")
    assert cancel_outcome.success is True
    assert cancel_outcome.status == WorkflowStatus.CANCELLED
    assert cancel_outcome.order.status == PaperOrderStatus.CANCELLED

    # Check ledger recorded cancellation
    cancel_entry = coordinator.engine.ledger.get_entry_by_idempotency_key(f"order_cancel_{order_id}")
    assert cancel_entry is not None
    assert cancel_entry.entry_type == LedgerEntryType.ORDER_CANCELLATION


def test_10_rejection_of_cancellation_after_fill(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """10. Rejection of cancellation after fill."""
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome.is_filled is True
    order_id = outcome.order.order_id

    # Attempt to cancel already filled order
    cancel_outcome = coordinator.cancel_order(order_id)
    assert cancel_outcome.success is False
    assert cancel_outcome.rejection_code == PaperRejectionCode.ALREADY_TERMINAL.value

    # Position and cash remain completely untouched
    pos = coordinator.engine.portfolio.get_position("RELIANCE")
    assert pos.quantity == 5.0


def test_11_duplicate_request_and_idempotency_handling(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """11. Duplicate request and idempotency handling."""
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
        client_order_id="unique_client_id_11",
    )
    outcome1 = coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome1.is_filled is True
    cash_after_first = coordinator.engine.portfolio.available_cash

    # Re-submit duplicate request with exact same client_order_id
    outcome2 = coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome2.success is False
    assert outcome2.status == WorkflowStatus.REJECTED
    assert outcome2.rejection_code == PaperRejectionCode.DUPLICATE_ORDER.value

    # Zero additional orders, zero cash change
    assert coordinator.engine.portfolio.available_cash == cash_after_first
    assert coordinator.engine.portfolio.get_position("RELIANCE").quantity == 5.0


def test_12_correlation_id_propagation(
    coordinator: PaperTradingWorkflowCoordinator,
    event_bus: EventBus,
    fresh_quote: Quote,
) -> None:
    """12. Correlation ID propagation."""
    captured: list[Event] = []
    event_bus.subscribe_all(lambda e: captured.append(e))

    cid = "corr_trace_12_custom_id"
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
        correlation_id=cid,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

    assert outcome.correlation_id == cid
    assert outcome.order.correlation_id == cid
    assert outcome.fill.correlation_id == cid

    # Verify all published events match correlation_id
    matching_events = [e for e in captured if e.correlation_id == cid]
    assert len(matching_events) >= 4


def test_13_consistent_order_fill_portfolio_and_ledger_state(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """13. Consistent order, fill, portfolio, and ledger state."""
    # BUY 10
    buy_req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=10.0,
    )
    buy_outcome = coordinator.submit_paper_order(buy_req, market_quote=fresh_quote)
    assert buy_outcome.is_filled is True

    # SELL 4
    sell_req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.SELL,
        order_type=PaperOrderType.MARKET,
        quantity=4.0,
    )
    sell_outcome = coordinator.submit_paper_order(sell_req, market_quote=fresh_quote)
    assert sell_outcome.is_filled is True

    # Check portfolio state
    pos = coordinator.engine.portfolio.get_position("RELIANCE")
    assert pos.quantity == 6.0

    # Check ledger balance
    recon = coordinator.engine.ledger.reconcile(expected_current_cash=coordinator.engine.portfolio.available_cash)
    assert recon["is_balanced"] is True
    assert recon["discrepancy"] == 0.0


def test_14_rejected_order_leaves_financial_state_unchanged(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """14. Rejected order leaves financial state unchanged."""
    initial_cash = coordinator.engine.portfolio.available_cash

    # Order exceeds max notional (50,000 max)
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=100.0,  # 100 * 2500 = 250,000 > 50,000
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome.status == WorkflowStatus.REJECTED
    assert outcome.rejection_code == PaperRejectionCode.EXCEEDS_MAX_NOTIONAL.value

    # Financial state unchanged
    assert coordinator.engine.portfolio.available_cash == initial_cash
    assert coordinator.engine.portfolio.get_position("RELIANCE") is None

    recon = coordinator.engine.ledger.reconcile(expected_current_cash=initial_cash)
    assert recon["is_balanced"] is True


def test_15_duplicate_event_processing_does_not_duplicate_fills(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """15. Duplicate event processing does not duplicate fills."""
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome.is_filled is True
    fill = outcome.fill

    # Attempt to directly re-record the exact same fill in ledger
    fill_entry = coordinator.engine.ledger.get_entry_by_idempotency_key(f"order_fill_{fill.fill_id}")
    assert fill_entry is not None

    # Re-recording must deduplicate and return False
    newly_recorded = coordinator.engine.ledger.record_entry(fill_entry)
    assert newly_recorded is False


def test_16_ledger_persistence_failure_is_surfaced(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """16. Ledger persistence failure is surfaced."""
    # Force ledger to raise persistence error
    def _failing_record(entry: PaperLedgerEntry) -> bool:
        raise OSError("Simulated disk I/O persistence failure")

    monkeypatch.setattr(coordinator.engine.ledger, "record_entry", _failing_record)

    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

    assert outcome.success is False
    assert outcome.status == WorkflowStatus.FAILED
    assert "Simulated disk I/O" in str(outcome.error_message)


def test_17_reconciliation_failure_is_surfaced(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """17. Reconciliation failure is surfaced."""
    # Mock reconciliation to return an unbalanced discrepancy
    def _unbalanced_reconcile(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {
            "is_balanced": False,
            "discrepancy": 500.0,
            "computed_balance": 99500.0,
            "expected_balance": 100000.0,
        }

    monkeypatch.setattr(coordinator.engine.ledger, "reconcile", _unbalanced_reconcile)

    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

    assert outcome.success is False
    assert outcome.status == WorkflowStatus.RECONCILIATION_FAILED
    assert outcome.stage == WorkflowStage.STATE_RECONCILIATION
    assert "discrepancy=500.0" in str(outcome.error_message)


def test_18_recovery_after_a_completed_order(
    tmp_path: Path,
    fresh_quote: Quote,
) -> None:
    """18. Recovery after a completed order."""
    db_file = str(tmp_path / "recovery_test_18.db")
    eb1 = EventBus()
    l1 = PaperLedger(event_bus=eb1, db_path=db_file, initial_balance=100_000.0)
    eng1 = PaperExecutionEngine(event_bus=eb1, ledger=l1, initial_cash=100_000.0, enabled=True)
    coord1 = PaperTradingWorkflowCoordinator(engine=eng1, event_bus=eb1, enabled=True)

    # Submit and fill order
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=10.0,
        client_order_id="rec_client_18",
    )
    outcome1 = coord1.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome1.is_filled is True
    cash_before = eng1.portfolio.available_cash

    # Simulate restart: new ledger and engine pointing to same db_file
    eb2 = EventBus()
    l2 = PaperLedger(event_bus=eb2, db_path=db_file, initial_balance=100_000.0)
    eng2 = PaperExecutionEngine(event_bus=eb2, ledger=l2, initial_cash=100_000.0, enabled=True)

    assert eng2.portfolio.available_cash == cash_before
    assert eng2.portfolio.get_position("RELIANCE").quantity == 10.0
    assert eng2.get_order_by_client_id("rec_client_18").status == PaperOrderStatus.FILLED


def test_19_recovery_with_a_pending_limit_order(
    tmp_path: Path,
    fresh_quote: Quote,
) -> None:
    """19. Recovery with a pending limit order."""
    db_file = str(tmp_path / "recovery_test_19.db")
    eb1 = EventBus()
    l1 = PaperLedger(event_bus=eb1, db_path=db_file, initial_balance=100_000.0)
    eng1 = PaperExecutionEngine(event_bus=eb1, ledger=l1, initial_cash=100_000.0, enabled=True)
    coord1 = PaperTradingWorkflowCoordinator(engine=eng1, event_bus=eb1, enabled=True)

    # Submit pending limit BUY at 2400.0
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.LIMIT,
        quantity=5.0,
        limit_price=2400.0,
        client_order_id="rec_pending_19",
    )
    outcome1 = coord1.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome1.is_pending is True

    # Simulate restart
    eb2 = EventBus()
    l2 = PaperLedger(event_bus=eb2, db_path=db_file, initial_balance=100_000.0)
    eng2 = PaperExecutionEngine(event_bus=eb2, ledger=l2, initial_cash=100_000.0, enabled=True)
    coord2 = PaperTradingWorkflowCoordinator(engine=eng2, event_bus=eb2, enabled=True)

    recovered_order = eng2.get_order_by_client_id("rec_pending_19")
    assert recovered_order is not None
    assert recovered_order.status == PaperOrderStatus.ACCEPTED

    # Ingest matching quote in recovered coordinator to fill the recovered pending order
    matching_quote = Quote(
        symbol="RELIANCE",
        exchange="NSE",
        timestamp=time.time(),
        last_price=2395.0,
        bid=2394.0,
        ask=2395.0,
    )
    fills = coord2.on_market_quote(matching_quote)
    assert len(fills) == 1
    assert fills[0].is_filled is True


def test_20_session_creation_and_termination(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """20. Session creation and termination."""
    session = coordinator.start_session(name="Morning Session", notes="Testing session grouping")
    assert session.status == "ACTIVE"
    assert session.session_id.startswith("psess_")
    assert session.initial_cash == coordinator.engine.portfolio.available_cash
    assert coordinator.get_active_session() == session

    # Submit order within session
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
        session_id=session.session_id,
    )
    outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert outcome.is_filled is True

    # Order registered in session
    assert outcome.order.order_id in session.order_ids
    assert outcome.workflow_id in session.workflow_ids

    # Stop session
    stopped = coordinator.stop_session(session.session_id)
    assert stopped.status == "STOPPED"
    assert stopped.stopped_at is not None
    assert coordinator.get_active_session() is None


def test_21_session_summaries_match_portfolio_and_ledger_state(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """21. Session summaries match portfolio and ledger state."""
    session = coordinator.start_session(name="Summary Test Session")
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
        session_id=session.session_id,
    )
    coordinator.submit_paper_order(req, market_quote=fresh_quote)

    summary = coordinator.get_session_summary(session.session_id)
    assert summary is not None
    assert summary["label"] == "PAPER ONLY"
    assert summary["initial_cash"] == 100_000.0
    assert summary["current_cash"] == coordinator.engine.portfolio.available_cash
    assert summary["reconciliation"]["is_balanced"] is True
    assert len(summary["positions"]) == 1
    assert summary["positions"][0]["symbol"] == "RELIANCE"


def test_22_dashboard_summary_is_read_only(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """22. Dashboard summary is read-only."""
    # Place one paper order
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    coordinator.submit_paper_order(req, market_quote=fresh_quote)

    server = PortfolioDashboardServer(paper_coordinator=coordinator, port=0)
    server.start(blocking=False)

    try:
        # GET request succeeds
        url = f"{server.url}/api/paper/summary"
        with urllib.request.urlopen(url) as response:
            assert response.status == 200
            data = json.loads(response.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["label"] == "PAPER ONLY"
            assert data["paper_execution_enabled"] is True
            assert data["execution_locked"] is True
            assert data["broker_provider"] == "none"
            assert data["trading_engine"] == "not initialized"
            assert data["order_counts"]["FILLED"] == 1

        # POST request is rejected with 405 Method Not Allowed
        post_req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="POST")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(post_req)
        assert exc_info.value.code == 405

        # PUT request is rejected with 405 Method Not Allowed
        put_req = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"}, method="PUT")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(put_req)
        assert exc_info.value.code == 405

    finally:
        server.stop()


def test_23_existing_paper_execution_risk_controls_remain_enforced(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """23. Existing paper-execution risk controls remain enforced."""
    # 1. Short selling prohibited
    sell_req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.SELL,
        order_type=PaperOrderType.MARKET,
        quantity=10.0,
    )
    outcome1 = coordinator.submit_paper_order(sell_req, market_quote=fresh_quote)
    assert outcome1.status == WorkflowStatus.REJECTED
    assert outcome1.rejection_code == PaperRejectionCode.INSUFFICIENT_POSITION.value

    # 2. Max position exposure exceeded (e.g. 50% max)
    # 100,000 cash, 50% max exposure = 50,000 max single position value
    heavy_buy = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=25.0,  # 25 * 2500 = 62,500 > 50,000 max notional & exposure
    )
    outcome2 = coordinator.submit_paper_order(heavy_buy, market_quote=fresh_quote)
    assert outcome2.status == WorkflowStatus.REJECTED
    assert outcome2.rejection_code in (
        PaperRejectionCode.EXCEEDS_MAX_NOTIONAL.value,
        PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE.value,
    )


def test_24_global_execution_lock_remains_enabled(
    coordinator: PaperTradingWorkflowCoordinator,
    fresh_quote: Quote,
) -> None:
    """24. Global execution lock remains enabled."""
    assert coordinator.settings.execution_locked is True

    # Even after paper orders are submitted, execution_locked remains strictly True
    req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
    )
    coordinator.submit_paper_order(req, market_quote=fresh_quote)
    assert coordinator.settings.execution_locked is True

    # If execution_locked is violated, coordinator refuses to execute
    unlocked_settings = Settings(execution_locked=False)
    unlocked_coordinator = PaperTradingWorkflowCoordinator(
        engine=coordinator.engine,
        settings=unlocked_settings,
        enabled=True,
    )
    assert unlocked_coordinator.is_enabled is False


def test_25_live_trading_engine_remains_uninitialized() -> None:
    """25. Live trading engine remains uninitialized."""
    status = bootstrap_system(Settings(paper_execution_enabled=True))
    assert status.trading_engine == "not initialized"
    assert status.execution_engine == "locked"
    assert status.settings.execution_locked is True


def test_26_no_broker_provider_is_registered() -> None:
    """26. No broker provider is registered."""
    status = bootstrap_system(Settings(paper_execution_enabled=True))
    assert status.settings.broker_provider == "none"


def test_27_existing_mock_and_replay_pipelines_remain_functional(
    event_bus: EventBus,
    state_store: StateStore,
    paper_engine: PaperExecutionEngine,
) -> None:
    """27. Existing mock and replay pipelines remain functional."""
    # Ingest a real quote through MarketDataService
    market_service = MarketDataService(event_bus=event_bus, state_store=state_store)
    quote = Quote(
        symbol="INFY",
        exchange="NSE",
        timestamp=time.time(),
        last_price=1500.0,
        bid=1499.0,
        ask=1501.0,
    )
    market_service.ingest_quote(quote)

    coord = PaperTradingWorkflowCoordinator(
        engine=paper_engine,
        event_bus=event_bus,
        state_store=state_store,
        market_service=market_service,
        enabled=True,
    )

    req = PaperWorkflowRequest(
        symbol="INFY",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=10.0,
    )
    outcome = coord.submit_paper_order(req)
    assert outcome.is_filled is True
    assert outcome.fill.symbol == "INFY"
    assert outcome.fill.price > 0
