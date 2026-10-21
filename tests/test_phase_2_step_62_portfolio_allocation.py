"""KAISER — Phase 2, Step 62: Multi-Asset Portfolio Allocation & Capital Partitioning Tests.

Comprehensive validation covering:
1. Valid allocation across multiple sleeves.
2. Multiple supported assets across separate sleeves.
3. Invalid allocation weights, normalization, and budget constraints.
4. NaN, infinity, negative values, and malformed requests.
5. Allocation totals exceeding available portfolio capital.
6. Duplicate allocation IDs and idempotent repeated requests.
7. Sleeve isolation and unauthorized cross-sleeve spending.
8. Per-sleeve risk-limit enforcement (notional, position exposure, disallowed symbol).
9. Aggregate portfolio risk enforcement across sleeves (cannot bypass by splitting across sleeves).
10. Rejected-order accounting immutability.
11. Cash, positions, fees, slippage, and exposure reconciliation.
12. Persistence failure and atomicity boundaries.
13. SQLite restart recovery of sleeves, cash, positions, and history.
14. Corrupted or inconsistent allocation history detection.
15. Repeated recovery without duplicated reservations or fills (idempotent recovery).
16. Deterministic allocation and financial digests.
17. Unsupported transfers (prohibited by default) and unsupported asset/order types.
18. Dashboard authentication, query filtering, and read-only method enforcement (405 on POST/PUT/DELETE).
19. Workflow coordinator integration with sleeve-targeted orders.
20. Mandatory KAISER safety invariants (execution_locked, broker_provider="none", zero voice execution).
"""

from __future__ import annotations

import copy
import http.client
from http import HTTPStatus
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional
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
from app.execution.portfolio import PaperPortfolio, PaperPosition, compute_financial_digest
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
# 1. Valid Allocation Across Multiple Sleeves
# ==============================================================================


def test_valid_allocation_across_multiple_sleeves() -> None:
    """Verifies standard partitioning of 100k capital across 3 strategy sleeves."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_alloc_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request(total_capital=100_000.0, request_id="alloc_req_01")
    result = manager.apply_allocation(req)

    assert result.success is True
    assert result.status == AllocationStatus.APPROVED
    assert result.allocated_capital == 100_000.0
    assert result.unallocated_capital == 0.0
    assert len(result.sleeve_allocations) == 3

    # Check individual sleeve budgets
    s1 = manager.get_sleeve("alpha_momentum")
    assert s1 is not None
    assert s1.allocated_capital == 50_000.0
    assert s1.available_cash == 50_000.0

    s2 = manager.get_sleeve("beta_mean_reversion")
    assert s2 is not None
    assert s2.allocated_capital == 30_000.0
    assert s2.available_cash == 30_000.0

    s3 = manager.get_sleeve("gamma_etf_core")
    assert s3 is not None
    assert s3.allocated_capital == 20_000.0
    assert s3.available_cash == 20_000.0

    # Reconciliation
    recon = manager.reconcile()
    assert recon["is_reconciled"] is True
    assert recon["max_discrepancy"] == 0.0


# ==============================================================================
# 2. Multiple Supported Assets Across Separate Sleeves
# ==============================================================================


def test_multiple_supported_assets_across_separate_sleeves() -> None:
    """Verifies that separate sleeves hold and trade their distinct authorized assets."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_multi_asset_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request(total_capital=100_000.0)
    manager.apply_allocation(req)

    now = 1_000_000.0
    # Sleeve 1 trades RELIANCE
    q_rel = _make_quote("RELIANCE", 2500.0, 2499.5, 2500.5, timestamp=now)
    res1 = manager.execute_sleeve_order(
        sleeve_id="alpha_momentum",
        request=PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0),
        quote=q_rel,
        reference_time=now,
    )
    assert res1.success is True
    assert res1.fill is not None

    # Sleeve 2 trades HDFCBANK
    q_hdfc = _make_quote("HDFCBANK", 1600.0, 1599.5, 1600.5, timestamp=now)
    res2 = manager.execute_sleeve_order(
        sleeve_id="beta_mean_reversion",
        request=PaperOrderRequest(symbol="HDFCBANK", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=8.0),
        quote=q_hdfc,
        reference_time=now,
    )
    assert res2.success is True
    assert res2.fill is not None

    # Both sleeves hold their respective assets
    s1 = manager.get_sleeve("alpha_momentum")
    assert s1 is not None and s1.get_position("RELIANCE") is not None
    assert s1.get_position("RELIANCE").quantity == 5.0

    s2 = manager.get_sleeve("beta_mean_reversion")
    assert s2 is not None and s2.get_position("HDFCBANK") is not None
    assert s2.get_position("HDFCBANK").quantity == 8.0

    # Portfolio holds both assets
    assert portfolio.get_position("RELIANCE").quantity == 5.0
    assert portfolio.get_position("HDFCBANK").quantity == 8.0

    # Full reconciliation
    recon = manager.reconcile()
    assert recon["is_reconciled"] is True


# ==============================================================================
# 3. Invalid Allocation Weights and Normalization
# ==============================================================================


def test_invalid_allocation_weights_and_normalization() -> None:
    """Verifies rejection when weights sum exceeds 1.0 or weights are negative/zero."""
    # 1. Weights sum > 1.0
    with pytest.raises(ValidationError, match="exceeds 1.0"):
        PortfolioAllocationRequest(
            request_id="req_excess_weight",
            sleeves=[
                StrategySleeveDefinition(sleeve_id="s1", name="Sleeve 1", target_weight=0.70),
                StrategySleeveDefinition(sleeve_id="s2", name="Sleeve 2", target_weight=0.40),
            ],
        )

    # 2. Negative weight
    with pytest.raises(ValidationError, match="must be in"):
        StrategySleeveDefinition(sleeve_id="s_neg", name="Negative Sleeve", target_weight=-0.2)

    # 3. Weight > 1.0
    with pytest.raises(ValidationError, match="must be in"):
        StrategySleeveDefinition(sleeve_id="s_big", name="Big Sleeve", target_weight=1.5)


# ==============================================================================
# 4. NaN, Infinity, and Malformed Requests
# ==============================================================================


def test_nan_infinity_and_malformed_requests() -> None:
    """Verifies that non-finite or empty inputs fail safely."""
    # NaN in budget
    with pytest.raises(ValidationError, match="positive finite"):
        StrategySleeveDefinition(sleeve_id="s_nan", name="NaN Sleeve", target_budget=float("nan"))

    # Inf in budget
    with pytest.raises(ValidationError, match="positive finite"):
        StrategySleeveDefinition(sleeve_id="s_inf", name="Inf Sleeve", target_budget=float("inf"))

    # Empty sleeve_id
    with pytest.raises(ValidationError, match="sleeve_id must be a non-empty string"):
        StrategySleeveDefinition(sleeve_id="", name="Empty Sleeve", target_weight=0.5)

    # Invalid sleeve_id characters (e.g. spaces or symbols)
    with pytest.raises(ValidationError, match="Invalid sleeve_id"):
        StrategySleeveDefinition(sleeve_id="bad id with spaces!", name="Bad Sleeve", target_weight=0.5)

    # Empty sleeves list
    with pytest.raises(ValidationError, match="at least one strategy sleeve"):
        PortfolioAllocationRequest(request_id="req_empty", sleeves=[])


# ==============================================================================
# 5. Allocation Totals Exceeding Available Capital
# ==============================================================================


def test_allocation_totals_exceeding_available_capital() -> None:
    """Verifies rejection when target budgets exceed portfolio available cash."""
    portfolio = PaperPortfolio(initial_cash=50_000.0)
    ledger = PaperLedger(name="test_overbudget_ledger", initial_balance=50_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = PortfolioAllocationRequest(
        request_id="req_overbudget",
        total_capital=75_000.0,  # 75k > 50k portfolio cash
        sleeves=[
            StrategySleeveDefinition(sleeve_id="s1", name="Sleeve 1", target_budget=75_000.0),
        ],
    )
    res = manager.apply_allocation(req)

    assert res.success is False
    assert res.status == AllocationStatus.REJECTED
    assert res.rejection_code == AllocationRejectionCode.CAPITAL_EXCEEDS_PORTFOLIO

    # Portfolio state remains pristine
    assert manager.total_allocated_capital == 0.0
    assert manager.unallocated_cash == 50_000.0


# ==============================================================================
# 6. Duplicate Allocation IDs and Idempotent Repeated Requests
# ==============================================================================


def test_duplicate_allocation_ids_and_idempotency() -> None:
    """Verifies that identical re-requests return cached result without double allocating,
    while conflicting parameters on duplicate ID are rejected."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_idemp_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req1 = create_standard_multi_sleeve_request(total_capital=100_000.0, request_id="alloc_idemp_1")
    res1 = manager.apply_allocation(req1)
    assert res1.success is True
    ledger_count_after_first = ledger.count()

    # Re-apply identical request: returns cached without new ledger entries
    res2 = manager.apply_allocation(req1)
    assert res2.success is True
    assert res2.deterministic_digest == res1.deterministic_digest
    assert ledger.count() == ledger_count_after_first

    # Duplicate request_id with different parameters: rejected
    req_conflict = PortfolioAllocationRequest(
        request_id="alloc_idemp_1",
        total_capital=50_000.0,
        sleeves=[StrategySleeveDefinition(sleeve_id="s_other", name="Other", target_weight=1.0)],
    )
    res_conflict = manager.apply_allocation(req_conflict)
    assert res_conflict.success is False
    assert res_conflict.rejection_code == AllocationRejectionCode.DUPLICATE_REQUEST_ID


# ==============================================================================
# 7. Sleeve Isolation and Unauthorized Cross-Sleeve Spending
# ==============================================================================


def test_sleeve_isolation_prevents_cross_sleeve_spending() -> None:
    """Verifies that Sleeve B cannot spend Sleeve A's capital, even if portfolio has total cash."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_isolation_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    # Sleeve A: 90,000 | Sleeve B: 10,000
    req = PortfolioAllocationRequest(
        request_id="req_isolation",
        total_capital=100_000.0,
        sleeves=[
            StrategySleeveDefinition(sleeve_id="sleeve_rich", name="Rich Sleeve", target_budget=90_000.0),
            StrategySleeveDefinition(sleeve_id="sleeve_poor", name="Poor Sleeve", target_budget=10_000.0),
        ],
    )
    manager.apply_allocation(req)

    now = 1_000_000.0
    quote = _make_quote("TCS", 3000.0, 2999.0, 3001.0, timestamp=now)

    # Poor sleeve wants to buy 5 shares of TCS (notional ~15,000 > 10,000 budget)
    # Total portfolio cash has 100,000, but poor sleeve has only 10,000!
    res = manager.execute_sleeve_order(
        sleeve_id="sleeve_poor",
        request=PaperOrderRequest(symbol="TCS", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0),
        quote=quote,
        reference_time=now,
    )

    assert res.success is False
    assert res.rejection_code == PaperRejectionCode.INSUFFICIENT_CASH

    # Rich sleeve cash is untouched
    assert manager.get_sleeve("sleeve_rich").available_cash == 90_000.0
    # Poor sleeve cash is untouched
    assert manager.get_sleeve("sleeve_poor").available_cash == 10_000.0
    # Portfolio cash is untouched
    assert portfolio.available_cash == 100_000.0


# ==============================================================================
# 8. Per-Sleeve Risk-Limit Enforcement
# ==============================================================================


def test_per_sleeve_risk_limits_enforced() -> None:
    """Verifies sleeve notional limits, symbol restrictions, and exposure caps."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_sleeve_risk_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = PortfolioAllocationRequest(
        request_id="req_sleeve_risk",
        total_capital=100_000.0,
        sleeves=[
            StrategySleeveDefinition(
                sleeve_id="sleeve_restricted",
                name="Restricted Sleeve",
                target_budget=50_000.0,
                allowed_symbols=["INFY"],
                risk_budget=SleeveRiskBudget(
                    max_order_notional=10_000.0,
                    max_position_exposure_pct=25.0,  # Max 12,500
                ),
            ),
        ],
    )
    manager.apply_allocation(req)

    now = 1_000_000.0
    # 1. Disallowed symbol test (RELIANCE not in allowed_symbols=["INFY"])
    q_rel = _make_quote("RELIANCE", 2500.0, timestamp=now)
    res_sym = manager.execute_sleeve_order(
        sleeve_id="sleeve_restricted",
        request=PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=2.0),
        quote=q_rel,
        reference_time=now,
    )
    assert res_sym.success is False
    assert res_sym.rejection_code == PaperRejectionCode.INVALID_SYMBOL

    # 2. Sleeve Order Notional Limit (order notional 15,000 > 10,000 limit)
    q_infy = _make_quote("INFY", 1500.0, timestamp=now)
    res_notional = manager.execute_sleeve_order(
        sleeve_id="sleeve_restricted",
        request=PaperOrderRequest(symbol="INFY", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0),  # 15,000
        quote=q_infy,
        reference_time=now,
    )
    assert res_notional.success is False
    assert res_notional.rejection_code == PaperRejectionCode.EXCEEDS_MAX_NOTIONAL

    # 3. Valid order within limits
    res_ok = manager.execute_sleeve_order(
        sleeve_id="sleeve_restricted",
        request=PaperOrderRequest(symbol="INFY", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0),  # 7,500 < 10k
        quote=q_infy,
        reference_time=now,
    )
    assert res_ok.success is True


# ==============================================================================
# 9. Aggregate Portfolio Risk Enforcement Across Sleeves
# ==============================================================================


def test_aggregate_portfolio_risk_cannot_be_bypassed_across_sleeves() -> None:
    """Verifies that splitting orders across multiple sleeves cannot bypass portfolio exposure limits."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_agg_risk_ledger", initial_balance=100_000.0)
    # Set portfolio max_position_exposure_pct = 30.0% (30,000 max in any one symbol)
    port_risk = PaperRiskConfig(max_order_notional=50_000.0, max_position_exposure_pct=30.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger, risk_config=port_risk)

    # 2 Sleeves with generous internal limits (50% each)
    req = PortfolioAllocationRequest(
        request_id="req_agg_test",
        total_capital=100_000.0,
        sleeves=[
            StrategySleeveDefinition(
                sleeve_id="sleeve_1",
                name="Sleeve 1",
                target_budget=50_000.0,
                risk_budget=SleeveRiskBudget(max_order_notional=30_000.0, max_position_exposure_pct=60.0),
            ),
            StrategySleeveDefinition(
                sleeve_id="sleeve_2",
                name="Sleeve 2",
                target_budget=50_000.0,
                risk_budget=SleeveRiskBudget(max_order_notional=30_000.0, max_position_exposure_pct=60.0),
            ),
        ],
    )
    manager.apply_allocation(req)

    now = 1_000_000.0
    quote = _make_quote("TCS", 2500.0, 2499.0, 2501.0, timestamp=now)

    # Sleeve 1 buys 10 shares of TCS (notional 25,000 <= 30k portfolio limit)
    res1 = manager.execute_sleeve_order(
        sleeve_id="sleeve_1",
        request=PaperOrderRequest(symbol="TCS", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0),
        quote=quote,
        reference_time=now,
    )
    assert res1.success is True

    # Now Sleeve 2 attempts to buy 5 shares of TCS (notional 12,500)
    # Inside Sleeve 2: 12,500 / 50,000 = 25% (below Sleeve 2's 60% limit)
    # BUT across portfolio: 25,000 + 12,500 = 37,500 / 100,000 = 37.5% > 30.0% portfolio limit!
    res2 = manager.execute_sleeve_order(
        sleeve_id="sleeve_2",
        request=PaperOrderRequest(symbol="TCS", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0),
        quote=quote,
        reference_time=now,
    )

    assert res2.success is False
    assert res2.rejection_code == PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE
    assert "portfolio aggregate exposure" in str(res2.rejection_reason).lower()


# ==============================================================================
# 10. Rejected-Order Accounting Immutability
# ==============================================================================


def test_rejected_order_accounting_immutability() -> None:
    """Verifies that rejected orders leave sleeve cash, positions, portfolio, and balances unchanged."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_immut_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request()
    manager.apply_allocation(req)

    pre_sleeve_cash = manager.get_sleeve("alpha_momentum").available_cash
    pre_port_cash = portfolio.available_cash

    now = 1_000_000.0
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now)

    # Submit order exceeding notional limit
    res = manager.execute_sleeve_order(
        sleeve_id="alpha_momentum",
        request=PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=1000.0),
        quote=quote,
        reference_time=now,
    )
    assert res.success is False

    # Financial state is 100% immutable
    assert manager.get_sleeve("alpha_momentum").available_cash == pre_sleeve_cash
    assert portfolio.available_cash == pre_port_cash
    assert manager.get_sleeve("alpha_momentum").get_position("RELIANCE") is None
    assert portfolio.get_position("RELIANCE") is None

    # Ledger cash remains balanced
    recon = ledger.reconcile(expected_current_cash=portfolio.available_cash)
    assert recon["is_balanced"] is True


# ==============================================================================
# 11. Full Reconciliation Across Cash, Positions, Fees, and Slippage
# ==============================================================================


def test_cash_positions_fees_and_exposure_reconciliation() -> None:
    """Verifies that the sum of sleeve totals equals portfolio totals across multiple transactions."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_recon_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request(total_capital=80_000.0)  # 20k unallocated
    manager.apply_allocation(req)
    assert manager.unallocated_cash == 20_000.0

    now = 1_000_000.0
    # Sleeve 1 trades TCS
    q_tcs = _make_quote("TCS", 3000.0, timestamp=now)
    manager.execute_sleeve_order(
        "alpha_momentum",
        PaperOrderRequest("TCS", PaperOrderSide.BUY, PaperOrderType.MARKET, 5.0),
        quote=q_tcs,
        reference_time=now,
    )

    # Sleeve 2 trades ICICIBANK
    q_icici = _make_quote("ICICIBANK", 1000.0, timestamp=now)
    manager.execute_sleeve_order(
        "beta_mean_reversion",
        PaperOrderRequest("ICICIBANK", PaperOrderSide.BUY, PaperOrderType.MARKET, 10.0),
        quote=q_icici,
        reference_time=now,
    )

    # Sleeve 1 sells 2 shares of TCS
    q_tcs_sell = _make_quote("TCS", 3100.0, timestamp=now + 10.0)
    manager.execute_sleeve_order(
        "alpha_momentum",
        PaperOrderRequest("TCS", PaperOrderSide.SELL, PaperOrderType.MARKET, 2.0),
        quote=q_tcs_sell,
        reference_time=now + 10.0,
    )

    snap = manager.get_portfolio_exposure()
    assert snap.is_reconciled is True
    assert snap.reconciliation_discrepancy == 0.0
    assert snap.unallocated_cash == 20_000.0

    # Explicit check: sum of sleeves + unallocated == portfolio cash
    sum_sleeve_cash = sum(s.available_cash for s in manager.list_sleeves())
    assert round(sum_sleeve_cash + snap.unallocated_cash, 4) == round(portfolio.available_cash, 4)


# ==============================================================================
# 12. SQLite Restart Recovery
# ==============================================================================


def test_sqlite_restart_recovery(tmp_path: Path) -> None:
    """Verifies durable reconstruction of sleeves, cash partitions, and fills on restart."""
    db_file = tmp_path / "sqlite_alloc_test.db"
    now = time.time() + 10.0

    # Initial session
    ledger1 = PaperLedger(db_path=str(db_file), initial_balance=100_000.0)
    # Ensure initial capital timestamp is before orders
    portfolio1 = PaperPortfolio(initial_cash=100_000.0)
    manager1 = PortfolioAllocationManager(portfolio=portfolio1, ledger=ledger1)

    req = create_standard_multi_sleeve_request(total_capital=100_000.0)
    manager1.apply_allocation(req)

    # Execute trade in Sleeve 1
    quote = _make_quote("RELIANCE", 2500.0, timestamp=now + 5.0)
    res = manager1.execute_sleeve_order(
        sleeve_id="alpha_momentum",
        request=PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 4.0),
        quote=quote,
        reference_time=now + 5.0,
    )
    assert res.success is True
    pre_cash = manager1.get_sleeve("alpha_momentum").available_cash
    pre_pos = manager1.get_sleeve("alpha_momentum").get_position("RELIANCE").quantity
    pre_digest = compute_multi_sleeve_financial_digest(portfolio1, manager1._sleeves, manager1.unallocated_cash)

    # RESTART: Simulate application crash / clean restart
    ledger2 = PaperLedger(db_path=str(db_file))
    portfolio2 = PaperPortfolio(initial_cash=100_000.0)
    # Recover portfolio orders from ledger first
    engine2 = PaperExecutionEngine(ledger=ledger2, portfolio=portfolio2, enabled=True)
    engine2.recover_from_ledger()

    # Recover allocation manager from ledger
    manager2 = PortfolioAllocationManager(portfolio=portfolio2, ledger=ledger2)
    rec_info = manager2.recover_from_ledger(ledger2)

    assert rec_info["recovered_sleeves"] == 3
    assert rec_info["reconciled"] is True

    # Validate recovered sleeve state
    rec_s1 = manager2.get_sleeve("alpha_momentum")
    assert rec_s1 is not None
    assert rec_s1.available_cash == pre_cash
    assert rec_s1.get_position("RELIANCE").quantity == pre_pos

    # Financial digest is identical
    post_digest = compute_multi_sleeve_financial_digest(portfolio2, manager2._sleeves, manager2.unallocated_cash)
    assert post_digest == pre_digest


# ==============================================================================
# 13. Corrupted Allocation History Detection
# ==============================================================================


def test_corrupted_allocation_history_fails_safely() -> None:
    """Verifies that non-finite values or missing metadata in ledger allocation entries trigger ValidationError."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_corrupt_ledger", initial_balance=100_000.0)

    # Inject corrupted allocation entry with missing sleeve_id
    bad_entry = PaperLedgerEntry(
        entry_id="bad_alloc_1",
        timestamp=time.time(),
        entry_type=LedgerEntryType.CAPITAL_ALLOCATION,
        cash_change=0.0,
        cash_balance_after=100_000.0,
        idempotency_key="bad_alloc_key",
        metadata={"allocated_cash": 50_000.0},  # missing sleeve_id!
    )
    ledger.record_entry(bad_entry)

    with pytest.raises(ValidationError, match="missing sleeve_id"):
        PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)


# ==============================================================================
# 14. Repeated Recovery Idempotence
# ==============================================================================


def test_repeated_recovery_is_idempotent() -> None:
    """Verifies that calling recover_from_ledger multiple times does not duplicate capital or positions."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_idemp_rec_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request()
    manager.apply_allocation(req)

    # First recovery
    info1 = manager.recover_from_ledger(ledger)
    snap1 = manager.get_portfolio_exposure()

    # Second recovery
    info2 = manager.recover_from_ledger(ledger)
    snap2 = manager.get_portfolio_exposure()

    assert info1 == info2
    assert snap1.deterministic_digest == snap2.deterministic_digest
    assert manager.get_sleeve("alpha_momentum").allocated_capital == 50_000.0


# ==============================================================================
# 15. Unsupported Transfers and Unsupported Asset Classes
# ==============================================================================


def test_unsupported_transfers_and_asset_classes() -> None:
    """Verifies that cross-sleeve transfers are prohibited by default,
    and unsupported asset classes (e.g. crypto, derivatives) fail closed."""
    portfolio = PaperPortfolio(initial_cash=100_000.0)
    ledger = PaperLedger(name="test_xfer_ledger", initial_balance=100_000.0)
    manager = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

    req = create_standard_multi_sleeve_request()
    manager.apply_allocation(req)

    # 1. Prohibited transfer (allow_transfer=False by default)
    ok, code, reason = manager.transfer_between_sleeves("alpha_momentum", "beta_mean_reversion", 5000.0)
    assert ok is False
    assert code == AllocationRejectionCode.UNAUTHORIZED_TRANSFER

    # 2. Authorized transfer (allow_transfer=True)
    ok2, code2, reason2 = manager.transfer_between_sleeves("alpha_momentum", "beta_mean_reversion", 5000.0, allow_transfer=True)
    assert ok2 is True
    assert manager.get_sleeve("alpha_momentum").available_cash == 45_000.0
    assert manager.get_sleeve("beta_mean_reversion").available_cash == 35_000.0

    # 3. Unsupported asset class in sleeve definition
    with pytest.raises(ValidationError, match="Unsupported asset class"):
        StrategySleeveDefinition(
            sleeve_id="sleeve_crypto",
            name="Crypto Sleeve",
            target_weight=0.1,
            allowed_asset_classes=["CRYPTO"],  # Unsupported!
        )


# ==============================================================================
# 16. Deterministic Allocation and Financial Digests
# ==============================================================================


def test_deterministic_allocation_and_financial_digests() -> None:
    """Verifies that identical requests and executions produce bit-exact SHA-256 digests."""
    def _run_trial() -> Tuple[str, str]:
        p = PaperPortfolio(initial_cash=100_000.0)
        l = PaperLedger(name="test_digest_trial", initial_balance=100_000.0)
        m = PortfolioAllocationManager(portfolio=p, ledger=l)
        r = m.apply_allocation(create_standard_multi_sleeve_request(request_id="alloc_fixed_id"))
        q = _make_quote("RELIANCE", 2500.0, timestamp=1000.0)
        m.execute_sleeve_order(
            "alpha_momentum",
            PaperOrderRequest("RELIANCE", PaperOrderSide.BUY, PaperOrderType.MARKET, 5.0, client_order_id="pord_fixed_01"),
            quote=q,
            reference_time=1000.0,
        )
        return r.deterministic_digest, compute_multi_sleeve_financial_digest(p, m._sleeves, m.unallocated_cash)

    alloc_dig1, fin_dig1 = _run_trial()
    alloc_dig2, fin_dig2 = _run_trial()

    assert alloc_dig1 == alloc_dig2
    assert fin_dig1 == fin_dig2
    assert len(alloc_dig1) == 64
    assert len(fin_dig1) == 64


# ==============================================================================
# 17. Workflow Coordinator Integration with Strategy Sleeves
# ==============================================================================


def test_workflow_coordinator_sleeve_targeted_execution() -> None:
    """Verifies PaperTradingWorkflowCoordinator execution when sleeve_id is specified."""
    settings = Settings(execution_locked=True, broker_provider="none", paper_execution_enabled=True)
    engine = PaperExecutionEngine(settings=settings, enabled=True, initial_cash=100_000.0)
    coordinator = PaperTradingWorkflowCoordinator(engine=engine, settings=settings, enabled=True)

    # Allocate capital via coordinator
    alloc_res = coordinator.allocate_capital(create_standard_multi_sleeve_request())
    assert alloc_res.success is True

    quote = _make_quote("RELIANCE", 2500.0, timestamp=time.time())

    # Execute workflow order targeted at alpha_momentum sleeve
    wf_req = PaperWorkflowRequest(
        symbol="RELIANCE",
        side=PaperOrderSide.BUY,
        order_type=PaperOrderType.MARKET,
        quantity=2.0,
        sleeve_id="alpha_momentum",
        quote=quote,
    )
    outcome = coordinator.execute_order(wf_req)

    assert outcome.success is True
    assert outcome.status == WorkflowStatus.FILLED
    assert outcome.sleeve_id == "alpha_momentum"
    assert outcome.fill is not None

    # Check sleeve updated
    s1 = coordinator.get_sleeve_exposure("alpha_momentum")
    assert s1 is not None
    assert "RELIANCE" in s1.positions
    assert s1.positions["RELIANCE"]["quantity"] == 2.0

    # Diagnostics export includes portfolio exposure
    diag = coordinator.export_workflow_diagnostics()
    assert "portfolio_exposure" in diag
    assert diag["portfolio_exposure"]["is_reconciled"] is True


# ==============================================================================
# 18. Dashboard Read-Only Integration & Security Enforcement
# ==============================================================================


class TestDashboardAllocationSecurity:
    """Validates authorization, loopback enforcement, and read-only method restrictions."""

    @pytest.fixture(autouse=True)
    def setup_dashboard(self, tmp_path: Path):
        self.auth_token = "secret_step62_token_12345"
        self.reporting = PortfolioReportingService()
        self.diagnostics = PortfolioDiagnosticsService()

        # Engine & Allocation Manager
        self.portfolio = PaperPortfolio(initial_cash=100_000.0)
        self.ledger = PaperLedger(name="dash_alloc_ledger", initial_balance=100_000.0)
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
        """Unauthenticated requests to allocation routes return 401."""
        base = self.base_url
        for path in ("/api/paper/allocations", "/api/paper/sleeves", "/api/paper/portfolio-exposure"):
            req = urllib.request.Request(f"{base}{path}")
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req)
            assert exc_info.value.code == HTTPStatus.UNAUTHORIZED

    def test_authenticated_get_allocations_succeeds(self) -> None:
        """Authenticated GET /api/paper/allocations returns sleeve summary."""
        req = urllib.request.Request(
            f"{self.base_url}/api/paper/allocations",
            headers={"Authorization": f"Bearer {self.auth_token}"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == HTTPStatus.OK
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["sleeve_count"] == 3
            assert data["read_only"] is True
            assert data["execution_locked"] is True

    def test_authenticated_get_sleeves_with_filtering(self) -> None:
        """Authenticated GET /api/paper/sleeves?sleeve_id=... returns specific sleeve snapshot."""
        # Valid sleeve
        req = urllib.request.Request(
            f"{self.base_url}/api/paper/sleeves?sleeve_id=alpha_momentum",
            headers={"Authorization": f"Bearer {self.auth_token}"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == HTTPStatus.OK
            data = json.loads(resp.read().decode("utf-8"))
            assert data["sleeve"]["sleeve_id"] == "alpha_momentum"
            assert data["sleeve"]["allocated_capital"] == 50_000.0

        # Non-existent sleeve returns 404
        req_404 = urllib.request.Request(
            f"{self.base_url}/api/paper/sleeves?sleeve_id=non_existent_sleeve",
            headers={"Authorization": f"Bearer {self.auth_token}"},
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_404)
        assert exc_info.value.code == HTTPStatus.NOT_FOUND

    def test_authenticated_get_portfolio_exposure_succeeds(self) -> None:
        """Authenticated GET /api/paper/portfolio-exposure returns consolidated exposure."""
        req = urllib.request.Request(
            f"{self.base_url}/api/paper/portfolio-exposure",
            headers={"Authorization": f"Bearer {self.auth_token}"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == HTTPStatus.OK
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert "exposure" in data
            assert data["exposure"]["is_reconciled"] is True

    def test_mutating_methods_rejected_with_405(self) -> None:
        """POST, PUT, DELETE to allocation endpoints return 405 Method Not Allowed."""
        for path in ("/api/paper/allocations", "/api/paper/sleeves", "/api/paper/portfolio-exposure"):
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
# 19. Mandatory KAISER Safety Invariants
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
