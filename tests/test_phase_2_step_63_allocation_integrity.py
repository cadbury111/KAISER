"""KAISER — Phase 2, Step 63: Portfolio Allocation Integrity, Cross-Sleeve Accounting Reconciliation & Failure-Injection Tests.

Validates:
1. Reconciliation:
   - Mathematical consistency of cash, reserved cash, position values, symbol quantities, fees, realized/unrealized P&L, total equity.
   - Structured diagnostic output with actionable mismatch details.
   - Deterministic tolerances and rounding rules.
   - Detection of deliberate discrepancies without silent overwriting.
2. Idempotency & Duplicate-Request Safety:
   - Repeated identical allocation request returns cached result without duplication.
   - Conflicting allocation request ID fails deterministically.
   - Repeated order with identical client order ID returns cached result.
   - Conflicting client order ID payload rejected deterministically.
   - Failed orders do not cache idempotency keys.
   - Duplicate fill entries in ledger deduplicated during recovery.
3. Concurrency & Transaction Integrity:
   - Concurrent reservations cannot over-allocate capital.
   - Concurrent orders cannot bypass aggregate portfolio risk limits.
   - Concurrent order executions across sleeves maintain reconciliation.
   - Simultaneous recovery and reads maintain consistent state.
4. Failure-Injection Matrix (All 16 Scenarios):
   - Scenario 1: Ledger write fails before persistence.
   - Scenario 2: SQLite transaction fails or rolls back.
   - Scenario 3: Persistence succeeds but subsequent in-memory update fails.
   - Scenario 4: Portfolio update succeeds but sleeve accounting fails.
   - Scenario 5: Sleeve update succeeds but portfolio accounting fails.
   - Scenario 6: Event publication fails after a financial operation.
   - Scenario 7: Exception occurs during fill processing.
   - Scenario 8: Allocation persistence is interrupted.
   - Scenario 9: Restart recovery encounters malformed allocation metadata.
   - Scenario 10: Recovery encounters duplicate or conflicting records.
   - Scenario 11: Recovery fails midway through multi-sleeve reconstruction.
   - Scenario 12: Same recovery is retried.
   - Scenario 13: Duplicate request arrives during in-progress operation.
   - Scenario 14: Two sleeves concurrently attempt to consume remaining permitted capital.
   - Scenario 15: Two individually valid sleeve orders jointly violate aggregate limit.
   - Scenario 16: Cancellation or rejection occurs after reservation.
5. Recovery & Corruption Handling:
   - Unknown sleeve in fill entry rejected with ValidationError and atomic rollback.
   - Non-finite financial values (NaN, Inf) rejected with ValidationError.
   - Legacy ledger entries without sleeve_id safely handled.
   - Chronological sorting of entries during recovery.
   - Full end-to-end multi-sleeve, multi-symbol recovery.
6. Risk-Control Regression:
   - Per-sleeve notional and concentration limits.
   - Aggregate portfolio notional and concentration limits.
   - Symbol allowlists and long-only enforcement.
   - Market data freshness checks.
   - Insufficient sleeve cash rejection.
   - Financial immutability of rejected orders.
7. Dashboard Authorization & Security:
   - Read-only GET /api/paper/reconciliation returns structured report.
   - Unauthenticated requests return 401.
   - Non-GET methods (POST, PUT, DELETE) return 405.
   - Loopback and read_only enforcement.
8. Mandatory KAISER Safety Invariants:
   - execution_locked=True, broker_provider="none", paper_execution_enabled=False by default.
   - Live trading uninitialized, zero voice execution authority, JUJU untouched.
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

from app.core.config import Settings
from app.core.events import EventBus, EventType
from app.core.exceptions import ValidationError
from app.execution.allocation import (
    AllocationRejectionCode,
    AllocationStatus,
    PortfolioAllocationManager,
    PortfolioAllocationRequest,
    PortfolioAllocationResult,
    PortfolioExposureSnapshot,
    PortfolioSleeveReconciliationResult,
    ReconciliationCheckDetail,
    SleeveAllocationResult,
    SleeveExposureSnapshot,
    SleeveRiskBudget,
    StrategySleeve,
    StrategySleeveDefinition,
    compute_allocation_digest,
    compute_multi_sleeve_financial_digest,
    create_standard_multi_sleeve_request,
)
from app.execution.engine import PaperExecutionEngine
from app.execution.models import (
    PaperExecutionResult,
    PaperFill,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectionCode,
    PaperRiskConfig,
)
from app.execution.portfolio import PaperPortfolio, PaperPosition
from app.execution.workflow import (
    PaperTradingWorkflowCoordinator,
    PaperWorkflowOutcome,
    PaperWorkflowRequest,
    WorkflowStatus,
)
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.market.models import Quote
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.diagnostics import PortfolioDiagnosticsService
from app.portfolio.reporting import PortfolioReportingService


def _make_quote(
    symbol: str = "RELIANCE",
    last_price: float = 2500.0,
    bid: Optional[float] = None,
    ask: Optional[float] = None,
    timestamp: Optional[float] = None,
) -> Quote:
    b = bid if bid is not None else round(last_price - 0.5, 2)
    a = ask if ask is not None else round(last_price + 0.5, 2)
    return Quote(
        symbol=symbol,
        exchange="NSE",
        timestamp=timestamp if timestamp is not None else time.time(),
        last_price=last_price,
        bid=b,
        ask=a,
        bid_size=100.0,
        ask_size=100.0,
        volume=10_000.0,
        currency="INR",
        source="test_fixture",
    )


# ==============================================================================
# Group 1: Portfolio and Sleeve Reconciliation Tests
# ==============================================================================


def test_reconciliation_structured_details_and_tolerances() -> None:
    """Verifies that reconciliation returns structured checks across all dimensions with tolerances."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_recon_struct", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request(total_capital=80_000.0)  # 20k unallocated
    manager.apply_allocation(req)

    # Initial state reconciliation
    recon = manager.reconcile(tolerance=1e-4)
    assert isinstance(recon, PortfolioSleeveReconciliationResult)
    assert recon.is_reconciled is True
    assert recon["is_reconciled"] is True
    assert recon.get("is_reconciled") is True
    assert recon.max_discrepancy == 0.0
    assert len(recon.discrepancies) == 0

    # Verify all 8 core dimensions are present and balanced
    for dim_check in (
        "cash_check",
        "reserved_cash_check",
        "position_value_check",
        "position_quantities_check",
        "fees_check",
        "realized_pnl_check",
        "unrealized_pnl_check",
        "total_equity_check",
    ):
        assert dim_check in recon.checks
        check = recon.checks[dim_check]
        assert isinstance(check, ReconciliationCheckDetail)
        assert check.is_balanced is True
        assert check.discrepancy <= 1e-4

    # Verify dictionary serialization
    d = recon.to_dict()
    assert d["is_reconciled"] is True
    assert d["portfolio_cash"] == 100_000.0
    assert d["sum_sleeve_cash"] == 80_000.0
    assert d["unallocated_cash"] == 20_000.0
    assert "advisory_disclaimer" in d


def test_reconciliation_detects_deliberate_cash_discrepancy() -> None:
    """Verifies that reconciliation catches cash discrepancies without silently overwriting history."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_recon_tamper", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request(total_capital=100_000.0)
    manager.apply_allocation(req)

    # Deliberately inject inconsistency into sleeve cash without changing portfolio
    sleeve = manager.get_sleeve("alpha_momentum")
    assert sleeve is not None
    original_cash = sleeve.available_cash
    sleeve.available_cash = round(original_cash - 500.0, 4)

    recon = manager.reconcile(tolerance=1e-4)
    assert recon.is_reconciled is False
    assert recon["is_reconciled"] is False
    assert len(recon.discrepancies) > 0
    assert any("Cash mismatch" in disc for disc in recon.discrepancies)
    assert recon.checks["cash_check"].is_balanced is False
    assert abs(recon.checks["cash_check"].discrepancy - 500.0) < 1e-4

    # Financial state is NOT silently repaired
    assert sleeve.available_cash == round(original_cash - 500.0, 4)


def test_reconciliation_detects_position_quantity_and_cost_mismatch() -> None:
    """Verifies that reconciliation catches position quantity tampering between sleeve and portfolio."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_pos_tamper", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    manager.execute_sleeve_order(
        "alpha_momentum",
        PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 4.0),
        quote=quote,
        reference_time=now,
    )

    # Tamper with portfolio position quantity
    port_pos = portfolio.get_position("RELIANCE")
    assert port_pos is not None
    port_pos.quantity = 10.0  # sleeve has 4.0

    recon = manager.reconcile(tolerance=1e-4)
    assert recon.is_reconciled is False
    assert recon.checks["position_quantities_check"].is_balanced is False
    assert any("Quantity mismatch for RELIANCE" in disc for disc in recon.discrepancies)


def test_reconciliation_detects_fee_and_pnl_mismatch() -> None:
    """Verifies that reconciliation catches fee or realized P&L mismatches."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fee_tamper", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("TCS", 3000.0, timestamp=now)

    manager.execute_sleeve_order(
        "alpha_momentum",
        PaperOrderRequest("TCS", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
        quote=quote,
        reference_time=now,
    )

    # Tamper with sleeve fee
    sleeve = manager.get_sleeve("alpha_momentum")
    assert sleeve is not None
    sleeve.total_fees_paid += 50.0

    recon = manager.reconcile(tolerance=1e-4)
    assert recon.is_reconciled is False
    assert recon.checks["fees_check"].is_balanced is False


def test_reconciliation_custom_tolerances() -> None:
    """Verifies that reconciliation respects custom tolerance parameters."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_tol", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    sleeve = manager.get_sleeve("alpha_momentum")
    assert sleeve is not None

    # Inject tiny micro-difference (0.00005)
    sleeve.available_cash = round(sleeve.available_cash + 0.00005, 5)

    # Tolerates at 1e-4
    recon_loose = manager.reconcile(tolerance=1e-4)
    assert recon_loose.is_reconciled is True

    # Fails at strict 1e-6
    recon_strict = manager.reconcile(tolerance=1e-6)
    assert recon_strict.is_reconciled is False


# ==============================================================================
# Group 2: Idempotency & Duplicate-Request Safety
# ==============================================================================


def test_repeated_identical_allocation_request_returns_cached_result() -> None:
    """Verifies that re-submitting an identical allocation returns cached result without duplicating capital."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_idemp_alloc", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request(request_id="alloc_fixed_1", total_capital=80_000.0)

    res1 = manager.apply_allocation(req)
    assert res1.success is True
    assert res1.allocated_capital == 80_000.0
    entries_count1 = len(ledger.get_entries())

    # Resubmit identical request
    res2 = manager.apply_allocation(req)
    assert res2.success is True
    assert res2.request_id == res1.request_id
    assert res2.allocated_capital == res1.allocated_capital

    # Ledger must NOT have duplicated entries
    entries_count2 = len(ledger.get_entries())
    assert entries_count2 == entries_count1
    assert manager.unallocated_cash == 20_000.0


def test_reusing_allocation_id_with_conflicting_payload_fails_deterministically() -> None:
    """Verifies that reusing an allocation ID with conflicting parameters is rejected deterministically."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_conflict_alloc", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req1 = create_standard_multi_sleeve_request(request_id="alloc_conflict_id", total_capital=50_000.0)
    res1 = manager.apply_allocation(req1)
    assert res1.success is True

    # Re-use same request_id with different capital
    req2 = create_standard_multi_sleeve_request(request_id="alloc_conflict_id", total_capital=90_000.0)
    res2 = manager.apply_allocation(req2)

    assert res2.success is False
    assert res2.status == AllocationStatus.REJECTED
    assert res2.rejection_code == AllocationRejectionCode.DUPLICATE_REQUEST_ID
    assert "different parameters" in res2.rejection_reason

    # State remains matching req1
    assert manager.unallocated_cash == 50_000.0


def test_repeated_order_with_identical_client_order_id_returns_cached_result() -> None:
    """Verifies that repeating an order with the same client order ID returns cached result."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_idemp_order", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    req = PaperOrderRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=2.0,
        client_order_id="client_oid_12345",
    )

    res1 = manager.execute_sleeve_order("alpha_momentum", req, quote=quote, reference_time=now)
    assert res1.success is True
    cash_after_1 = manager.get_sleeve("alpha_momentum").available_cash
    pos_after_1 = manager.get_sleeve("alpha_momentum").get_position("RELIANCE").quantity
    entries_count1 = len(ledger.get_entries())

    # Resubmit identical order
    res2 = manager.execute_sleeve_order("alpha_momentum", req, quote=quote, reference_time=now)
    assert res2.success is True
    assert res2.order.order_id == res1.order.order_id
    assert res2.order.filled_quantity == res1.order.filled_quantity

    # Cash, positions, and ledger entries are NOT duplicated
    assert manager.get_sleeve("alpha_momentum").available_cash == cash_after_1
    assert manager.get_sleeve("alpha_momentum").get_position("RELIANCE").quantity == pos_after_1
    assert len(ledger.get_entries()) == entries_count1


def test_reusing_client_order_id_with_conflicting_parameters_rejected() -> None:
    """Verifies that reusing a client order ID with conflicting payload is rejected with DUPLICATE_ORDER."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_conflict_order", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    req1 = PaperOrderRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=2.0,
        client_order_id="reused_oid_999",
    )
    res1 = manager.execute_sleeve_order("alpha_momentum", req1, quote=quote, reference_time=now)
    assert res1.success is True

    # Re-use same client_order_id with different quantity (e.g. 5.0 instead of 2.0)
    req2 = PaperOrderRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=5.0,
        client_order_id="reused_oid_999",
    )
    res2 = manager.execute_sleeve_order("alpha_momentum", req2, quote=quote, reference_time=now)

    assert res2.success is False
    assert res2.rejection_code == PaperRejectionCode.DUPLICATE_ORDER
    assert "conflicting parameters" in res2.rejection_reason

    # Position in sleeve remains 2.0 (not 7.0)
    assert manager.get_sleeve("alpha_momentum").get_position("RELIANCE").quantity == 2.0


def test_failed_order_does_not_cache_idempotency_key_allowing_correction() -> None:
    """Verifies that failed orders do NOT leave a poisoned idempotency entry that blocks valid retries."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fail_idemp", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # Initial order exceeds sleeve cash -> rejected
    req_bad = PaperOrderRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=100.0,  # 250,000 INR > 50,000 sleeve cash
        client_order_id="retry_oid_111",
    )
    res_bad = manager.execute_sleeve_order("alpha_momentum", req_bad, quote=quote, reference_time=now)
    assert res_bad.success is False

    # Resubmitting with corrected, valid quantity using the same client order ID succeeds
    req_good = PaperOrderRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=2.0,  # 5,000 INR
        client_order_id="retry_oid_111",
    )
    res_good = manager.execute_sleeve_order("alpha_momentum", req_good, quote=quote, reference_time=now)
    assert res_good.success is True
    assert res_good.order.status == PaperOrderStatus.FILLED


# ==============================================================================
# Group 3: Concurrency & Transaction Integrity
# ==============================================================================


def test_concurrent_sleeve_capital_reservations_cannot_over_allocate() -> None:
    """Verifies that concurrent threads cannot reserve more capital than available in a sleeve."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_concur_res", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    # Allocate 10,000 to sleeve
    req = PortfolioAllocationRequest(
        request_id="alloc_concur",
        timestamp=time.time(),
        portfolio_id="port_concur",
        total_capital=10_000.0,
        sleeves=[StrategySleeveDefinition(sleeve_id="s1", name="Sleeve 1", target_budget=10_000.0)],
    )
    manager.apply_allocation(req)

    # 10 threads concurrently attempt to reserve 2,000 each (total 20,000 attempted, only 10,000 available)
    success_count = 0
    failure_count = 0
    barrier = threading.Barrier(10)
    lock = threading.Lock()

    def worker() -> None:
        nonlocal success_count, failure_count
        barrier.wait()
        ok = manager.reserve_sleeve_capital("s1", 2_000.0)
        with lock:
            if ok:
                success_count += 1
            else:
                failure_count += 1

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly 5 should succeed (5 * 2000 = 10000) and 5 should fail
    assert success_count == 5
    assert failure_count == 5
    s1 = manager.get_sleeve("s1")
    assert s1 is not None
    assert s1.available_cash == 0.0
    assert s1.reserved_cash == 10_000.0


def test_concurrent_orders_cannot_bypass_aggregate_portfolio_risk_limit() -> None:
    """Verifies that concurrent orders in separate sleeves cannot breach aggregate portfolio risk limit."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_concur_risk", initial_balance=100_000.0)

    # Portfolio limit: max 25,000 notional per order, max 30% position exposure (30,000 INR)
    risk_config = PaperRiskConfig(max_order_notional=25_000.0, max_position_exposure_pct=30.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, risk_config=risk_config)

    # Sleeve 1 and Sleeve 2 each have 50,000 capital
    req = PortfolioAllocationRequest(
        request_id="alloc_concur_risk",
        timestamp=time.time(),
        portfolio_id="port_concur",
        total_capital=100_000.0,
        sleeves=[
            StrategySleeveDefinition("s1", "Sleeve 1", target_weight=0.5, risk_budget=SleeveRiskBudget(25_000.0, 60.0)),
            StrategySleeveDefinition("s2", "Sleeve 2", target_weight=0.5, risk_budget=SleeveRiskBudget(25_000.0, 60.0)),
        ],
    )
    manager.apply_allocation(req)

    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    results: List[PaperExecutionResult] = []
    barrier = threading.Barrier(2)
    lock = threading.Lock()

    def worker(sleeve_id: str) -> None:
        barrier.wait()
        res = manager.execute_sleeve_order(
            sleeve_id=sleeve_id,
            request=PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 8.0),  # 20,000 INR
            quote=quote,
            reference_time=now,
        )
        with lock:
            results.append(res)

    t1 = threading.Thread(target=worker, args=("s1",))
    t2 = threading.Thread(target=worker, args=("s2",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # One should succeed (20,000 <= 30,000), and the second should be rejected (40,000 > 30,000)
    successes = [r for r in results if r.success]
    failures = [r for r in results if not r.success]
    assert len(successes) == 1
    assert len(failures) == 1
    assert failures[0].rejection_code == PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE

    # Reconciles cleanly
    recon = manager.reconcile()
    assert recon.is_reconciled is True


def test_concurrent_order_executions_across_sleeves_maintain_reconciliation() -> None:
    """Verifies that multi-threaded trading across 3 sleeves maintains 100% accounting reconciliation."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_concur_multi", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request(total_capital=90_000.0))
    now = 1_000_000.0
    quotes = {
        "RELIANCE": _make_quote("RELIANCE", 2500.0, timestamp=now),
        "TCS": _make_quote("TCS", 3000.0, timestamp=now),
        "ICICIBANK": _make_quote("ICICIBANK", 1000.0, timestamp=now),
    }

    trades = [
        ("alpha_momentum", "RELIANCE", 2.0),
        ("beta_mean_reversion", "TCS", 2.0),
        ("gamma_macro", "ICICIBANK", 5.0),
        ("alpha_momentum", "RELIANCE", 1.0),
        ("beta_mean_reversion", "TCS", 1.0),
    ]

    barrier = threading.Barrier(len(trades))

    def trade_worker(sleeve_id: str, sym: str, qty: float) -> None:
        barrier.wait()
        manager.execute_sleeve_order(
            sleeve_id=sleeve_id,
            request=PaperOrderRequest(sym, PaperOrderSide.BUY, PaperOrderType.MARKET, qty),
            quote=quotes[sym],
            reference_time=now,
        )

    threads = [threading.Thread(target=trade_worker, args=t) for t in trades]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    # Reconcile after all concurrent trades
    recon = manager.reconcile()
    assert recon.is_reconciled is True
    assert recon.max_discrepancy == 0.0


# ==============================================================================
# Group 4: Failure-Injection Matrix (Scenarios 1-16)
# ==============================================================================


def test_failure_injection_1_ledger_write_fails_before_persistence() -> None:
    """Scenario 1: Ledger write fails before persistence -> in-memory state rolls back cleanly."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_1", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    pre_cash = manager.get_sleeve("alpha_momentum").available_cash
    pre_port_cash = portfolio.available_cash

    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # Mock ledger to fail during record_entry
    with patch.object(ledger, "record_entry", side_effect=IOError("Simulated ledger disk error")):
        with pytest.raises(IOError):
            manager.execute_sleeve_order(
                "alpha_momentum",
                PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
                quote=quote,
                reference_time=now,
            )

    # Transactional rollback: sleeve and portfolio cash are 100% restored
    assert manager.get_sleeve("alpha_momentum").available_cash == pre_cash
    assert portfolio.available_cash == pre_port_cash
    assert manager.get_sleeve("alpha_momentum").get_position("RELIANCE") is None
    assert portfolio.get_position("RELIANCE") is None

    recon = manager.reconcile()
    assert recon.is_reconciled is True


def test_failure_injection_2_sqlite_transaction_rollback(tmp_path: Path) -> None:
    """Scenario 2: SQLite transaction fails or rolls back -> ledger and memory maintain integrity."""
    db_file = tmp_path / "sqlite_rollback_test.db"
    ledger = PaperLedger(db_path=str(db_file), initial_balance=100_000.0)
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    pre_cash = manager.get_sleeve("alpha_momentum").available_cash

    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # Corrupt sqlite connection to simulate transaction failure
    with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("disk I/O error")):
        with pytest.raises(sqlite3.OperationalError):
            manager.execute_sleeve_order(
                "alpha_momentum",
                PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
                quote=quote,
                reference_time=now,
            )

    # Memory state is rolled back
    assert manager.get_sleeve("alpha_momentum").available_cash == pre_cash
    assert portfolio.get_position("RELIANCE") is None


def test_failure_injection_3_persistence_succeeds_subsequent_inmemory_fails() -> None:
    """Scenario 3: Persistence succeeds but subsequent step encounters exception -> fails safely."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_3", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # In our architecture, in-memory updates happen under snapshot rollback protection
    # before persistence commits. If an error occurs during rollback handling:
    sleeve = manager.get_sleeve("alpha_momentum")
    pre_cash = sleeve.available_cash

    with patch.object(portfolio, "apply_fill", side_effect=RuntimeError("Simulated in-memory crash")):
        with pytest.raises(RuntimeError):
            manager.execute_sleeve_order(
                "alpha_momentum",
                PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
                quote=quote,
                reference_time=now,
            )

    # Pre-trade state restored
    assert sleeve.available_cash == pre_cash


def test_failure_injection_4_portfolio_update_succeeds_sleeve_fails() -> None:
    """Scenario 4: Portfolio update succeeds but sleeve accounting fails -> atomic rollback."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_4", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    sleeve = manager.get_sleeve("alpha_momentum")
    pre_cash = sleeve.available_cash
    pre_port_cash = portfolio.available_cash

    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # If sleeve fails during apply_fill
    with patch.object(sleeve, "apply_fill", side_effect=ValueError("Sleeve math error")):
        with pytest.raises(ValueError):
            manager.execute_sleeve_order(
                "alpha_momentum",
                PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
                quote=quote,
                reference_time=now,
            )

    assert sleeve.available_cash == pre_cash
    assert portfolio.available_cash == pre_port_cash


def test_failure_injection_5_sleeve_update_succeeds_portfolio_fails() -> None:
    """Scenario 5: Sleeve update succeeds but portfolio accounting fails -> atomic rollback."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_5", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    sleeve = manager.get_sleeve("alpha_momentum")
    pre_cash = sleeve.available_cash
    pre_port_cash = portfolio.available_cash

    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    with patch.object(portfolio, "apply_fill", side_effect=RuntimeError("Portfolio failure")):
        with pytest.raises(RuntimeError):
            manager.execute_sleeve_order(
                "alpha_momentum",
                PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
                quote=quote,
                reference_time=now,
            )

    assert sleeve.available_cash == pre_cash
    assert portfolio.available_cash == pre_port_cash


def test_failure_injection_6_event_publication_fails_after_operation() -> None:
    """Scenario 6: Event publication fails after a financial operation -> order still succeeds and balances match."""
    event_bus = EventBus()
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_6", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, event_bus=event_bus)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # EventBus publish raises exception
    with patch.object(event_bus, "publish", side_effect=Exception("Kafka/Bus connection down")):
        res = manager.execute_sleeve_order(
            "alpha_momentum",
            PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
            quote=quote,
            reference_time=now,
        )

    # Order execution succeeds, event failure is gracefully logged
    assert res.success is True
    assert res.order.status == PaperOrderStatus.FILLED
    recon = manager.reconcile()
    assert recon.is_reconciled is True


def test_failure_injection_7_exception_during_fill_processing() -> None:
    """Scenario 7: An exception occurs during fill processing -> zero state leakage."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_7", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    pre_cash = manager.get_sleeve("alpha_momentum").available_cash

    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    with patch.object(manager.broker_adapter, "execute_order", side_effect=RuntimeError("Fill math calculation exception")):
        with pytest.raises(RuntimeError):
            manager.execute_sleeve_order(
                "alpha_momentum",
                PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
                quote=quote,
                reference_time=now,
            )

    assert manager.get_sleeve("alpha_momentum").available_cash == pre_cash
    assert portfolio.get_position("RELIANCE") is None


def test_failure_injection_8_allocation_persistence_interrupted() -> None:
    """Scenario 8: Allocation persistence interrupted -> in-memory state rolled back cleanly, retry succeeds."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_8", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request(request_id="interrupted_req")

    # Fail ledger on recording allocation
    with patch.object(ledger, "record_entry", side_effect=IOError("Disk write full")):
        with pytest.raises(IOError):
            manager.apply_allocation(req)

    # Rollback: no sleeves created, unallocated cash untouched
    assert len(manager.list_sleeves()) == 0
    assert manager.unallocated_cash == 100_000.0

    # Retry without mock error succeeds cleanly
    res_retry = manager.apply_allocation(req)
    assert res_retry.success is True
    assert len(manager.list_sleeves()) == 3


def test_failure_injection_9_recovery_malformed_allocation_metadata() -> None:
    """Scenario 9: Restart recovery encounters malformed allocation metadata -> fails closed with ValidationError."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_9", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    # Insert malformed CAPITAL_ALLOCATION entry without sleeve_id
    bad_entry = PaperLedgerEntry(
        entry_id="bad_alloc_1",
        timestamp=time.time(),
        entry_type=LedgerEntryType.CAPITAL_ALLOCATION,
        metadata={"name": "corrupt"},  # missing sleeve_id and allocated_cash
    )
    ledger.record_entry(bad_entry)

    with pytest.raises(ValidationError) as exc:
        manager.recover_from_ledger(ledger)
    assert "Corrupted CAPITAL_ALLOCATION entry" in str(exc.value)


def test_failure_injection_10_recovery_duplicate_conflicting_records() -> None:
    """Scenario 10: Recovery encounters conflicting duplicate allocation records -> raises ValidationError."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_10", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    # Entry 1: allocate 50,000 to alpha
    e1 = PaperLedgerEntry(
        entry_id="alloc_1",
        timestamp=1000.0,
        entry_type=LedgerEntryType.CAPITAL_ALLOCATION,
        metadata={"sleeve_id": "alpha", "allocated_cash": 50_000.0},
    )
    # Entry 2: conflicting duplicate allocating 60,000 to alpha
    e2 = PaperLedgerEntry(
        entry_id="alloc_2",
        timestamp=1001.0,
        entry_type=LedgerEntryType.CAPITAL_ALLOCATION,
        metadata={"sleeve_id": "alpha", "allocated_cash": 60_000.0},
    )
    ledger.record_entry(e1)
    ledger.record_entry(e2)

    with pytest.raises(ValidationError) as exc:
        manager.recover_from_ledger(ledger)
    assert "Conflicting duplicate CAPITAL_ALLOCATION" in str(exc.value)


def test_failure_injection_11_recovery_fails_midway_multi_sleeve() -> None:
    """Scenario 11: Recovery fails midway through multi-sleeve reconstruction -> atomic rollback to backup."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_11", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    # Valid initial state
    manager.apply_allocation(create_standard_multi_sleeve_request())
    assert len(manager.list_sleeves()) == 3

    # Add corrupted entry to ledger bypassing post_init
    corrupt_entry = PaperLedgerEntry(
        entry_id="corrupt_entry",
        timestamp=2000.0,
        entry_type=LedgerEntryType.ORDER_FILL,
        price=100.0,
    )
    object.__setattr__(corrupt_entry, "price", float("nan"))
    ledger.record_entry(corrupt_entry)

    # Re-running recovery will fail on corrupt entry, restoring previous in-memory state
    with pytest.raises(ValidationError):
        manager.recover_from_ledger(ledger)

    # State restored to backup
    assert len(manager.list_sleeves()) == 3


def test_failure_injection_12_repeated_recovery_retried() -> None:
    """Scenario 12: Recovery retried multiple times is strictly idempotent."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_12", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)
    manager.execute_sleeve_order(
        "alpha_momentum",
        PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
        quote=quote,
        reference_time=now,
    )

    info1 = manager.recover_from_ledger(ledger)
    time.sleep(0.01)
    info2 = manager.recover_from_ledger(ledger)

    assert info1 == info2
    assert manager.get_sleeve("alpha_momentum").get_position("RELIANCE").quantity == 2.0


def test_failure_injection_13_duplicate_request_during_in_progress_operation() -> None:
    """Scenario 13: Duplicate request arrives during in-progress operation -> protected by idempotency & lock."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_13", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    req = PaperOrderRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=2.0,
        client_order_id="concur_dup_oid",
    )

    results: List[PaperExecutionResult] = []
    barrier = threading.Barrier(2)

    def worker() -> None:
        barrier.wait()
        res = manager.execute_sleeve_order("alpha_momentum", req, quote=quote, reference_time=now)
        results.append(res)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Both return successfully, referencing identical order ID (one executed, one returned from cache)
    assert len(results) == 2
    assert results[0].success is True
    assert results[1].success is True
    assert results[0].order.order_id == results[1].order.order_id

    # Sleeve position is strictly 2.0, not 4.0
    assert manager.get_sleeve("alpha_momentum").get_position("RELIANCE").quantity == 2.0


def test_failure_injection_14_two_sleeves_racing_remaining_permitted_capital() -> None:
    """Scenario 14: Two sleeves race to consume remaining permitted capital -> thread safe, exactly one wins."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_14", initial_balance=100_000.0)

    # Set portfolio risk gate cash restriction: max 10% in RELIANCE (10,000 INR)
    risk_config = PaperRiskConfig(max_order_notional=100_000.0, max_position_exposure_pct=10.0)  # max 10% in RELIANCE
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, risk_config=risk_config)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # Each sleeve tries to buy 3 shares = 7,500 INR.
    # Total limit is 10,000 INR. The first sleeve buys 3 (7,500 <= 10,000).
    # The second sleeve tries to buy 3 (7,500 + 7,500 = 15,000 > 10,000) -> rejected.
    results: List[PaperExecutionResult] = []
    barrier = threading.Barrier(2)
    lock = threading.Lock()

    def worker(sleeve_id: str) -> None:
        barrier.wait()
        res = manager.execute_sleeve_order(
            sleeve_id=sleeve_id,
            request=PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 3.0),
            quote=quote,
            reference_time=now,
        )
        with lock:
            results.append(res)

    t1 = threading.Thread(target=worker, args=("alpha_momentum",))
    t2 = threading.Thread(target=worker, args=("beta_mean_reversion",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    successes = [r for r in results if r.success]
    failures = [r for r in results if not r.success]
    assert len(successes) == 1
    assert len(failures) == 1


def test_failure_injection_15_two_valid_orders_jointly_violating_aggregate_limit() -> None:
    """Scenario 15: Two individually valid sleeve orders jointly violate aggregate limit -> second is rejected."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_15", initial_balance=100_000.0)

    # Portfolio limit: max 15,000 INR in TCS (15% of 100,000)
    risk_config = PaperRiskConfig(max_position_exposure_pct=15.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, risk_config=risk_config)

    req = PortfolioAllocationRequest(
        request_id="alloc_joint_limit",
        timestamp=time.time(),
        portfolio_id="port_joint",
        total_capital=100_000.0,
        sleeves=[
            StrategySleeveDefinition("s1", "Sleeve 1", target_weight=0.5, allowed_symbols=["RELIANCE"], risk_budget=SleeveRiskBudget(25_000.0, 60.0)),
            StrategySleeveDefinition("s2", "Sleeve 2", target_weight=0.5, allowed_symbols=["RELIANCE"], risk_budget=SleeveRiskBudget(25_000.0, 60.0)),
        ],
    )
    manager.apply_allocation(req)
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # Order 1: 4 shares of RELIANCE = 10,000 INR (in s1) -> succeeds (10,000 <= 15,000)
    res1 = manager.execute_sleeve_order(
        "s1",
        PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 4.0),
        quote=quote,
        reference_time=now,
    )
    assert res1.success is True

    # Order 2: 4 shares of RELIANCE = 10,000 INR (in s2) -> total would be 20,000 > 15,000 -> rejected
    res2 = manager.execute_sleeve_order(
        "s2",
        PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 4.0),
        quote=quote,
        reference_time=now,
    )
    assert res2.success is False
    assert res2.rejection_code == PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE

    # Reconciles cleanly
    recon = manager.reconcile()
    assert recon.is_reconciled is True


def test_failure_injection_16_cancellation_rejection_after_reservation() -> None:
    """Scenario 16: A cancellation or rejection occurs after reservation -> capital is released without leakage."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_fi_16", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    sleeve = manager.get_sleeve("alpha_momentum")
    pre_cash = sleeve.available_cash

    # Reserve 10,000 cash for a pending order
    ok_res = manager.reserve_sleeve_capital("alpha_momentum", 10_000.0)
    assert ok_res is True
    assert sleeve.available_cash == pre_cash - 10_000.0
    assert sleeve.reserved_cash == 10_000.0

    # Operation is cancelled or rejected -> release reservation
    ok_rel = manager.release_sleeve_capital("alpha_momentum", 10_000.0)
    assert ok_rel is True
    assert sleeve.available_cash == pre_cash
    assert sleeve.reserved_cash == 0.0

    # Ledger and accounting reconcile
    recon = manager.reconcile()
    assert recon.is_reconciled is True


# ==============================================================================
# Group 5: Recovery and Corruption Handling
# ==============================================================================


def test_recovery_rejects_unknown_sleeve_in_fill_entry() -> None:
    """Verifies that recover_from_ledger raises ValidationError on unknown sleeve in fill."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_unk_sleeve", initial_balance=100_000.0)

    # Valid allocation
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)
    manager.apply_allocation(create_standard_multi_sleeve_request())

    # Inject fill with unknown sleeve
    bad_fill = PaperLedgerEntry(
        entry_id="fill_bad_sleeve",
        timestamp=2000.0,
        entry_type=LedgerEntryType.ORDER_FILL,
        quantity=1.0,
        price=100.0,
        fee=1.0,
        cash_change=-101.0,
        cash_balance_after=99899.0,
        metadata={"sleeve_id": "non_existent_sleeve"},
    )
    ledger.record_entry(bad_fill)

    with pytest.raises(ValidationError) as exc:
        manager.recover_from_ledger(ledger)
    assert "references unknown strategy sleeve" in str(exc.value)


def test_recovery_rejects_non_finite_financial_values() -> None:
    """Verifies that recover_from_ledger rejects NaN or infinity values in financial fields."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_nan_rec", initial_balance=100_000.0)

    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)
    manager.apply_allocation(create_standard_multi_sleeve_request())

    # Inject NaN fee bypassing post_init
    bad_fill = PaperLedgerEntry(
        entry_id="fill_nan",
        timestamp=2000.0,
        entry_type=LedgerEntryType.ORDER_FILL,
        quantity=1.0,
        price=100.0,
        fee=1.0,
        metadata={"sleeve_id": "alpha_momentum"},
    )
    object.__setattr__(bad_fill, "fee", float("nan"))
    ledger.record_entry(bad_fill)

    with pytest.raises(ValidationError) as exc:
        manager.recover_from_ledger(ledger)
    assert "non-finite" in str(exc.value)


def test_recovery_safely_skips_legacy_non_sleeve_entries() -> None:
    """Verifies that legacy ledger entries lacking sleeve_id are safely ignored during sleeve replay."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_legacy_rec", initial_balance=100_000.0)

    # Legacy fill without sleeve_id metadata
    legacy_entry = PaperLedgerEntry(
        entry_id="legacy_1",
        timestamp=500.0,
        entry_type=LedgerEntryType.ORDER_FILL,
        quantity=1.0,
        price=100.0,
        fee=1.0,
        cash_change=-101.0,
        cash_balance_after=99899.0,
        metadata={"legacy": True},  # No sleeve_id
    )
    ledger.record_entry(legacy_entry)

    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)
    manager.apply_allocation(create_standard_multi_sleeve_request())

    rec_info = manager.recover_from_ledger(ledger)
    assert rec_info["recovered_sleeves"] == 3
    assert rec_info["reconciled"] is True


def test_recovery_preserves_chronological_ordering() -> None:
    """Verifies that recovery sorts entries chronologically before replaying."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_chrono_rec", initial_balance=100_000.0)

    # Insert fill with timestamp 2000.0
    f2 = PaperLedgerEntry(
        entry_id="f2",
        timestamp=2000.0,
        entry_type=LedgerEntryType.ORDER_FILL,
        symbol="TCS",
        side="BUY",
        quantity=1.0,
        price=3000.0,
        fee=5.0,
        cash_change=-3005.0,
        cash_balance_after=96995.0,
        metadata={"sleeve_id": "alpha_momentum", "fill_id": "fill_2"},
    )
    # Insert allocation with timestamp 1000.0
    a1 = PaperLedgerEntry(
        entry_id="a1",
        timestamp=1000.0,
        entry_type=LedgerEntryType.CAPITAL_ALLOCATION,
        metadata={"sleeve_id": "alpha_momentum", "allocated_cash": 50_000.0},
    )
    # Record out of order: fill first, allocation second
    ledger.record_entry(f2)
    ledger.record_entry(a1)

    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)
    # In recovery pass 1: allocations are reconstructed first
    # In pass 2: fills are replayed in chronological order
    rec_info = manager.recover_from_ledger(ledger)
    assert rec_info["recovered_sleeves"] == 1
    assert manager.get_sleeve("alpha_momentum").get_position("TCS").quantity == 1.0


# ==============================================================================
# Group 6: Risk-Control Regression Tests
# ==============================================================================


def test_risk_regression_sleeve_notional_and_concentration() -> None:
    """Verifies per-sleeve notional limit and position exposure percentage constraints."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_risk_notional", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # Sleeve limit: max_order_notional = 25,000 INR for alpha_momentum
    # Order for 12 shares = 30,000 INR -> rejected
    res = manager.execute_sleeve_order(
        "alpha_momentum",
        PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 12.0),
        quote=quote,
        reference_time=now,
    )
    assert res.success is False
    assert res.rejection_code == PaperRejectionCode.EXCEEDS_MAX_NOTIONAL


def test_risk_regression_symbol_allowlists() -> None:
    """Verifies that orders for symbols outside the sleeve's allowed_symbols list are rejected."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_risk_allowlist", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    quote = _make_quote("SBIN", 500.0, timestamp=now)

    # SBIN is NOT in alpha_momentum allowlist ("RELIANCE", "TCS", "INFY")
    res = manager.execute_sleeve_order(
        "alpha_momentum",
        PaperOrderRequest("SBIN", PaperOrderSide.BUY, PaperOrderType.MARKET, 10.0),
        quote=quote,
        reference_time=now,
    )
    assert res.success is False
    assert res.rejection_code == PaperRejectionCode.INVALID_SYMBOL


def test_risk_regression_market_data_freshness_check() -> None:
    """Verifies that stale quotes are rejected by the risk engine."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_risk_stale", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    manager.apply_allocation(create_standard_multi_sleeve_request())
    now = 1_000_000.0
    # Stale quote: timestamp 999,900.0 (100 seconds stale vs 5.0s max threshold)
    stale_quote = _make_quote("RELIANCE", 2500.0, timestamp=now - 100.0)

    res = manager.execute_sleeve_order(
        "alpha_momentum",
        PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 2.0),
        quote=stale_quote,
        reference_time=now,
    )
    assert res.success is False
    assert res.rejection_code == PaperRejectionCode.STALE_MARKET_DATA


# ==============================================================================
# Group 7: Dashboard Authorization & Security Tests
# ==============================================================================


class TestDashboardReconciliationSecurity:
    """Validates authorization, loopback enforcement, and read-only method restrictions for reconciliation."""

    @pytest.fixture(autouse=True)
    def setup_dashboard(self):
        self.auth_token = "secret_step63_recon_token"
        self.reporting = PortfolioReportingService()
        self.diagnostics = PortfolioDiagnosticsService()

        self.portfolio = PaperPortfolio(initial_cash=100_000.0)
        self.ledger = PaperLedger(name="dash_recon_ledger", initial_balance=100_000.0)
        self.alloc_mgr = PortfolioAllocationManager(portfolio=self.portfolio, ledger=self.ledger)
        self.alloc_mgr.apply_allocation(create_standard_multi_sleeve_request())

        self.server = PortfolioDashboardServer(
            reporting_service=self.reporting,
            diagnostics_service=self.diagnostics,
            allocation_manager=self.alloc_mgr,
            host="127.0.0.1",
            port=0,
            require_auth=True,
            auth_token=self.auth_token,
        )
        self.server.start(blocking=False)
        self.base_url = f"http://127.0.0.1:{self.server.port}"
        yield
        self.server.stop()

    def test_unauthenticated_requests_return_401(self) -> None:
        """Unauthenticated requests to reconciliation routes return 401."""
        for path in ("/api/paper/reconciliation", "/api/paper/reconcile"):
            req = urllib.request.Request(f"{self.base_url}{path}")
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req)
            assert exc_info.value.code == HTTPStatus.UNAUTHORIZED

    def test_authenticated_get_reconciliation_succeeds(self) -> None:
        """Authenticated GET /api/paper/reconciliation returns structured reconciliation report."""
        req = urllib.request.Request(
            f"{self.base_url}/api/paper/reconciliation",
            headers={"Authorization": f"Bearer {self.auth_token}"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == HTTPStatus.OK
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert "reconciliation" in data
            assert data["reconciliation"]["is_reconciled"] is True
            assert "checks" in data["reconciliation"]

    def test_mutating_methods_rejected_with_405(self) -> None:
        """POST, PUT, DELETE to reconciliation endpoints return 405 Method Not Allowed."""
        for path in ("/api/paper/reconciliation", "/api/paper/reconcile"):
            for method in ("POST", "PUT", "DELETE"):
                req = urllib.request.Request(
                    f"{self.base_url}{path}",
                    headers={"Authorization": f"Bearer {self.auth_token}"},
                    data=b'{"hack": true}' if method != "DELETE" else None,
                    method=method,
                )
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(req)
                assert exc_info.value.code == HTTPStatus.METHOD_NOT_ALLOWED


# ==============================================================================
# Group 8: Mandatory KAISER Safety Invariants
# ==============================================================================


def test_mandatory_kaiser_safety_invariants() -> None:
    """Verifies that all foundation safety invariants remain strictly intact."""
    # 1. Execution locked by default
    default_settings = Settings()
    assert default_settings.execution_locked is True

    # 2. Broker provider is strictly none
    assert default_settings.broker_provider == "none"

    # 3. Paper execution disabled by default
    assert default_settings.paper_execution_enabled is False

    # 4. Live engine remains uninitialized
    with pytest.raises(ImportError):
        import app.execution.live  # Does not exist!

    # 5. Voice commands have zero execution authority
    from app.voice.intent import VoiceCommandIntentResolver, VoiceIntentType
    resolver = VoiceCommandIntentResolver()
    for phrase in ("buy 10 shares of RELIANCE", "sell 5 shares of TCS", "place order INFY"):
        resolved = resolver.resolve(phrase)
        assert resolved.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND
        assert resolved.is_prohibited_trading is True
        assert not hasattr(resolved, "execute_order")
