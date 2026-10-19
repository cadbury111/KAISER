"""KAISER — Tests for Phase 2, Step 53: Paper Execution Engine & Simulated Broker Adapter.

Validates:
1. Paper order submission, acceptance, and simulated fill lifecycle.
2. Long-only portfolio accounting, cash, fees, and P&L calculations.
3. Pre-trade risk gating (cash, position, notional, exposure, market data freshness).
4. Deterministic simulated broker fill-price policies for market and limit orders.
5. Auditable idempotent ledger with cash reconciliation and SQLite persistence.
6. Event publication and correlation across the event bus.
7. Architectural safety invariants:
   - execution_locked = True
   - broker_provider = "none"
   - trading_engine = "not initialized"
   - No live network dependencies or order endpoints
"""

from __future__ import annotations

import time
import pytest
from pathlib import Path

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.bootstrap import bootstrap_system
from app.market.models import Quote
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
from app.execution.risk import PaperRiskGate
from app.execution.simulated_broker import SimulatedBrokerAdapter
from app.execution.engine import PaperExecutionEngine
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.ledger.ledger import PaperLedger


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_paper_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    return StateStore(name="test_paper_state_store")


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
    db_file = str(tmp_path / "paper_ledger_test.db")
    return PaperLedger(event_bus=event_bus, db_path=db_file, initial_balance=100000.0)


@pytest.fixture
def paper_engine(
    event_bus: EventBus,
    state_store: StateStore,
    paper_ledger: PaperLedger,
    fresh_quote: Quote,
) -> PaperExecutionEngine:
    risk_config = PaperRiskConfig(
        max_order_notional=50000.0,
        max_position_exposure_pct=50.0,
        freshness_threshold_seconds=60.0,
        allow_short_selling=False,
    )
    engine = PaperExecutionEngine(
        event_bus=event_bus,
        state_store=state_store,
        ledger=paper_ledger,
        initial_cash=100000.0,
        risk_config=risk_config,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        enabled=True,
    )
    engine.on_market_quote(fresh_quote)
    return engine


# ==============================================================================
# 1. DOMAIN MODELS & VALIDATION TESTS
# ==============================================================================


class TestPaperModelsAndValidation:
    """Validate request, fill, and status domain models."""

    def test_paper_order_request_valid(self) -> None:
        req = PaperOrderRequest(
            symbol="INFY",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=10.0,
        )
        assert req.symbol == "INFY"
        assert req.side == PaperOrderSide.BUY
        assert req.order_type == PaperOrderType.MARKET
        assert req.quantity == 10.0
        assert req.client_order_id.startswith("pord_")

    def test_paper_order_request_validation_failures(self) -> None:
        # Invalid / empty symbol
        with pytest.raises(ValidationError, match="symbol must be a non-empty string"):
            PaperOrderRequest(symbol="", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)

        # Zero or negative quantity
        with pytest.raises(ValidationError, match="quantity must be a positive finite float"):
            PaperOrderRequest(symbol="TCS", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=0.0)
        with pytest.raises(ValidationError, match="quantity must be a positive finite float"):
            PaperOrderRequest(symbol="TCS", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=-5.0)

        # Limit order without price
        with pytest.raises(ValidationError, match="Limit orders require a positive finite limit_price"):
            PaperOrderRequest(symbol="TCS", side=PaperOrderSide.BUY, order_type=PaperOrderType.LIMIT, quantity=10.0, limit_price=None)
        with pytest.raises(ValidationError, match="Limit orders require a positive finite limit_price"):
            PaperOrderRequest(symbol="TCS", side=PaperOrderSide.BUY, order_type=PaperOrderType.LIMIT, quantity=10.0, limit_price=-10.0)

    def test_paper_rejection_code_values(self) -> None:
        assert PaperRejectionCode.INSUFFICIENT_CASH.value == "INSUFFICIENT_CASH"
        assert PaperRejectionCode.INSUFFICIENT_POSITION.value == "INSUFFICIENT_POSITION"
        assert PaperRejectionCode.SHORT_SELLING_DISABLED.value == "SHORT_SELLING_PROHIBITED"
        assert PaperRejectionCode.SHORT_SELLING_PROHIBITED.value == "SHORT_SELLING_PROHIBITED"
        assert PaperRejectionCode.MAX_ORDER_NOTIONAL_EXCEEDED.value == "EXCEEDS_MAX_NOTIONAL"
        assert PaperRejectionCode.EXCEEDS_MAX_NOTIONAL.value == "EXCEEDS_MAX_NOTIONAL"
        assert PaperRejectionCode.STALE_MARKET_DATA.value == "STALE_MARKET_DATA"


# ==============================================================================
# 2. SIMULATED BROKER ADAPTER TESTS
# ==============================================================================


class TestSimulatedBrokerAdapter:
    """Validate deterministic fill pricing, fees, and broker simulation."""

    def test_market_buy_fill_pricing(self, fresh_quote: Quote) -> None:
        adapter = SimulatedBrokerAdapter(fee_rate=0.001, slippage_rate=0.0005)
        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)

        can_fill, fill, code, reason = adapter.simulate_fill(req, fresh_quote)
        assert can_fill is True
        assert fill is not None
        assert code is None
        # Ask was 2500.5. Slippage is 0.05% -> 2500.5 * 1.0005 = 2501.7503
        expected_price = round(2500.5 * (1 + 0.0005), 4)
        assert fill.fill_price == expected_price
        assert fill.quantity == 10.0
        assert fill.gross_notional == round(10.0 * expected_price, 4)
        assert fill.fee == round(fill.gross_notional * 0.001, 4)

    def test_market_sell_fill_pricing(self, fresh_quote: Quote) -> None:
        adapter = SimulatedBrokerAdapter(fee_rate=0.001, slippage_rate=0.0005)
        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=5.0)

        can_fill, fill, code, reason = adapter.simulate_fill(req, fresh_quote)
        assert can_fill is True
        assert fill is not None
        # Bid was 2499.5. Slippage is 0.05% -> 2499.5 * (1 - 0.0005) = 2498.2503
        expected_price = round(2499.5 * (1 - 0.0005), 4)
        assert fill.fill_price == expected_price
        assert fill.quantity == 5.0

    def test_limit_buy_fill_condition(self, fresh_quote: Quote) -> None:
        adapter = SimulatedBrokerAdapter(fee_rate=0.001, slippage_rate=0.0)
        # Limit price below ask (2500.5): should NOT fill
        req_unfilled = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.LIMIT,
            quantity=10.0,
            limit_price=2490.0,
        )
        can_fill, fill, code, reason = adapter.simulate_fill(req_unfilled, fresh_quote)
        assert can_fill is False
        assert fill is None
        assert code == PaperRejectionCode.LIMIT_PRICE_NOT_MET

        # Limit price at or above ask: SHOULD fill
        req_filled = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.LIMIT,
            quantity=10.0,
            limit_price=2501.0,
        )
        can_fill, fill, code, reason = adapter.simulate_fill(req_filled, fresh_quote)
        assert can_fill is True
        assert fill is not None
        # Fills at market ask (2500.5) because min(limit, market) gives price improvement
        assert fill.fill_price == 2500.5

    def test_limit_sell_fill_condition(self, fresh_quote: Quote) -> None:
        adapter = SimulatedBrokerAdapter(fee_rate=0.001, slippage_rate=0.0)
        # Limit price above bid (2499.5): should NOT fill
        req_unfilled = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.SELL,
            order_type=PaperOrderType.LIMIT,
            quantity=5.0,
            limit_price=2510.0,
        )
        can_fill, fill, code, reason = adapter.simulate_fill(req_unfilled, fresh_quote)
        assert can_fill is False
        assert code == PaperRejectionCode.LIMIT_PRICE_NOT_MET

        # Limit price at or below bid: SHOULD fill
        req_filled = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.SELL,
            order_type=PaperOrderType.LIMIT,
            quantity=5.0,
            limit_price=2495.0,
        )
        can_fill, fill, code, reason = adapter.simulate_fill(req_filled, fresh_quote)
        assert can_fill is True
        assert fill is not None
        # Fills at bid (2499.5) because max(limit, market) gives price improvement
        assert fill.fill_price == 2499.5

    def test_stale_or_invalid_quote_rejection(self) -> None:
        adapter = SimulatedBrokerAdapter()
        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)

        # Stale quote (older than 60s)
        stale_quote = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=time.time() - 120.0,
            last_price=2500.0,
            bid=2499.0,
            ask=2501.0,
        )
        can_fill, fill, code, reason = adapter.simulate_fill(req, stale_quote, max_age_seconds=60.0)
        assert can_fill is False
        assert code == PaperRejectionCode.STALE_MARKET_DATA

        # Verify that Quote model rejects inverted spreads at validation time
        with pytest.raises(ValidationError, match="Inverted spread invariant violated"):
            Quote(
                symbol="RELIANCE",
                exchange="NSE",
                timestamp=time.time(),
                last_price=2500.0,
                bid=2505.0,
                ask=2500.0,
            )

        # Mismatched symbol quote rejection
        mismatch_quote = Quote(
            symbol="TCS",
            exchange="NSE",
            timestamp=time.time(),
            last_price=3500.0,
            bid=3499.0,
            ask=3501.0,
        )
        can_fill, fill, code, reason = adapter.simulate_fill(req, mismatch_quote)
        assert can_fill is False
        assert code == PaperRejectionCode.INVALID_MARKET_DATA


# ==============================================================================
# 3. PORTFOLIO & LEDGER ACCOUNTING TESTS
# ==============================================================================


class TestPaperPortfolioAndLedger:
    """Validate cash, positions, fees, P&L, and ledger accounting."""

    def test_portfolio_buy_and_sell_accounting(self) -> None:
        portfolio = PaperPortfolio(initial_cash=100000.0)
        assert portfolio.available_cash == 100000.0
        assert portfolio.realized_pnl == 0.0

        # Fill 1: Buy 10 @ 2000.0, fee = 10.0
        fill_buy = PaperFill(
            fill_id="fill_1",
            order_id="ord_1",
            client_order_id="client_1",
            symbol="TCS",
            side=PaperOrderSide.BUY,
            quantity=10.0,
            price=2000.0,
            fee=10.0,
            slippage=0.0,
            timestamp=time.time(),
            market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        realized_pnl = portfolio.apply_fill(fill_buy)
        assert realized_pnl == 0.0
        assert portfolio.available_cash == round(100000.0 - 20010.0, 4)
        pos = portfolio.get_position("TCS")
        assert pos is not None
        assert pos.quantity == 10.0
        assert pos.avg_entry_price == 2000.0

        # Fill 2: Buy 10 @ 2200.0, fee = 11.0 -> Avg entry price should be (20000 + 22000)/20 = 2100.0
        fill_buy_2 = PaperFill(
            fill_id="fill_2",
            order_id="ord_2",
            client_order_id="client_2",
            symbol="TCS",
            side=PaperOrderSide.BUY,
            quantity=10.0,
            price=2200.0,
            fee=11.0,
            slippage=0.0,
            timestamp=time.time(),
            market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        portfolio.apply_fill(fill_buy_2)
        pos = portfolio.get_position("TCS")
        assert pos.quantity == 20.0
        assert pos.avg_entry_price == 2100.0
        assert pos.realized_pnl == 0.0

        # Fill 3: Sell 10 @ 2300.0, fee = 11.5
        # Cost basis for 10 units = 10 * 2100 = 21000.0
        # Gross revenue = 23000.0, fee = 11.5
        # Realized P&L = 23000.0 - 21000.0 - 11.5 = 1988.5
        fill_sell = PaperFill(
            fill_id="fill_3",
            order_id="ord_3",
            client_order_id="client_3",
            symbol="TCS",
            side=PaperOrderSide.SELL,
            quantity=10.0,
            price=2300.0,
            fee=11.5,
            slippage=0.0,
            timestamp=time.time(),
            market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        pnl = portfolio.apply_fill(fill_sell)
        assert pnl == 1988.5
        assert portfolio.realized_pnl == 1988.5
        assert pos.quantity == 10.0
        assert pos.avg_entry_price == 2100.0

    def test_portfolio_mark_to_market(self) -> None:
        portfolio = PaperPortfolio(initial_cash=50000.0)
        fill = PaperFill(
            fill_id="fill_mtm",
            order_id="ord_mtm",
            client_order_id="client_mtm",
            symbol="INFY",
            side=PaperOrderSide.BUY,
            quantity=20.0,
            price=1500.0,
            fee=15.0,
            slippage=0.0,
            timestamp=time.time(),
            market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        portfolio.apply_fill(fill)

        # Mark price up to 1600.0 -> Unrealized P&L = (1600 - 1500) * 20 = 2000.0
        portfolio.mark_to_market("INFY", 1600.0)
        pos = portfolio.get_position("INFY")
        assert pos.current_market_price == 1600.0
        assert pos.unrealized_pnl == 2000.0
        assert pos.market_value == 32000.0
        assert portfolio.unrealized_pnl == 2000.0
        assert portfolio.total_equity == round(portfolio.available_cash + 32000.0, 4)

    def test_ledger_append_and_idempotency(self, event_bus: EventBus, tmp_path: Path) -> None:
        db_file = str(tmp_path / "ledger_idempotency.db")
        ledger = PaperLedger(event_bus=event_bus, db_path=db_file, initial_balance=50000.0)

        entry = PaperLedgerEntry(
            entry_id="entry_test_1",
            entry_type=LedgerEntryType.ORDER_FILL,
            timestamp=time.time(),
            order_id="ord_test",
            client_order_id="client_test",
            symbol="RELIANCE",
            side="BUY",
            quantity=10.0,
            price=2500.0,
            fee=12.5,
            cash_change=-25012.5,
            cash_balance_after=24987.5,
            realized_pnl=0.0,
            idempotency_key="fill_test_unique_key",
        )

        # First recording must succeed
        recorded1 = ledger.record_entry(entry)
        assert recorded1 is True
        assert len(ledger.get_entries()) == 2  # initial capital + order fill

        # Second recording with identical idempotency_key must be skipped safely
        recorded2 = ledger.record_entry(entry)
        assert recorded2 is False
        assert len(ledger.get_entries()) == 2

        # Cash reconciliation
        recon = ledger.reconcile()
        assert recon["is_balanced"] is True
        assert recon["discrepancy"] == 0.0

    def test_ledger_sqlite_reload(self, event_bus: EventBus, tmp_path: Path) -> None:
        db_file = str(tmp_path / "ledger_reload.db")
        ledger1 = PaperLedger(event_bus=event_bus, db_path=db_file, initial_balance=75000.0)

        entry = PaperLedgerEntry(
            entry_id="entry_persist_1",
            entry_type=LedgerEntryType.ORDER_FILL,
            timestamp=time.time(),
            order_id="ord_p1",
            client_order_id="client_p1",
            symbol="INFY",
            side="BUY",
            quantity=5.0,
            price=1500.0,
            fee=3.75,
            cash_change=-7503.75,
            cash_balance_after=67496.25,
            realized_pnl=0.0,
            idempotency_key="fill_p1",
        )
        ledger1.record_entry(entry)

        # Create new ledger instance pointing to the same SQLite DB
        ledger2 = PaperLedger(event_bus=event_bus, db_path=db_file)
        entries = ledger2.get_entries()
        assert len(entries) >= 2
        p1 = ledger2.get_entry("entry_persist_1")
        assert p1 is not None
        assert p1.symbol == "INFY"
        assert p1.cash_change == -7503.75


# ==============================================================================
# 4. PAPER EXECUTION ENGINE LIFECYCLE & RISK TESTS
# ==============================================================================


class TestPaperExecutionEngine:
    """End-to-end testing of PaperExecutionEngine order management."""

    def test_valid_buy_and_fill(self, paper_engine: PaperExecutionEngine) -> None:
        req = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=10.0,
        )
        res = paper_engine.submit_order(req)
        assert res.success is True
        assert res.order.status == PaperOrderStatus.FILLED
        assert res.fill is not None
        assert res.fill.quantity == 10.0
        # Position should exist
        pos = paper_engine.portfolio.get_position("RELIANCE")
        assert pos is not None
        assert pos.quantity == 10.0

    def test_valid_sell_against_existing_long(self, paper_engine: PaperExecutionEngine) -> None:
        # Buy 10 units first
        buy_req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        paper_engine.submit_order(buy_req)

        # Sell 5 units
        sell_req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=5.0)
        res = paper_engine.submit_order(sell_req)

        assert res.success is True
        assert res.order.status == PaperOrderStatus.FILLED
        assert res.fill is not None
        assert res.fill.quantity == 5.0
        pos = paper_engine.portfolio.get_position("RELIANCE")
        assert pos.quantity == 5.0

    def test_rejection_of_short_selling(self, paper_engine: PaperExecutionEngine) -> None:
        # Attempt to sell without holding any shares
        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=10.0)
        res = paper_engine.submit_order(req)

        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.INSUFFICIENT_POSITION
        assert res.order.status == PaperOrderStatus.REJECTED
        assert paper_engine.portfolio.get_position("RELIANCE") is None

    def test_rejection_insufficient_position_quantity(self, paper_engine: PaperExecutionEngine) -> None:
        # Buy 5 shares, then attempt to sell 10 shares
        buy_req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0)
        paper_engine.submit_order(buy_req)

        sell_req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=10.0)
        res = paper_engine.submit_order(sell_req)

        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.INSUFFICIENT_POSITION
        # Position remains 5 shares
        assert paper_engine.portfolio.get_position("RELIANCE").quantity == 5.0

    def test_rejection_insufficient_cash(self, paper_engine: PaperExecutionEngine) -> None:
        # Available cash is 100,000. Try to buy 100 shares @ 2500 = 250,000
        req = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=100.0,
        )
        res = paper_engine.submit_order(req)
        assert res.success is False
        assert res.rejection_code in (
            PaperRejectionCode.EXCEEDS_MAX_NOTIONAL,
            PaperRejectionCode.INSUFFICIENT_CASH,
        )

    def test_rejection_max_order_notional(self, paper_engine: PaperExecutionEngine) -> None:
        # Max order notional is 50,000. Buy 25 shares @ 2500 = 62,500
        req = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=25.0,
        )
        res = paper_engine.submit_order(req)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.EXCEEDS_MAX_NOTIONAL

    def test_rejection_max_position_exposure(self, paper_engine: PaperExecutionEngine) -> None:
        # Total equity is 100,000. Max exposure is 50% = 50,000.
        # Order 1: Buy 15 shares @ 2500 = 37,500 (passes)
        req1 = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=15.0)
        res1 = paper_engine.submit_order(req1)
        assert res1.success is True

        # Order 2: Buy 10 more shares @ 2500 = 25,000. Total position would be 62,500 > 50,000 (rejected)
        req2 = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        res2 = paper_engine.submit_order(req2)
        assert res2.success is False
        assert res2.rejection_code == PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE

    def test_rejection_missing_market_data(self, paper_engine: PaperExecutionEngine) -> None:
        # Attempt to buy a symbol with no quotes registered
        req = PaperOrderRequest(symbol="UNKNOWN_SYM", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        res = paper_engine.submit_order(req)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.MISSING_MARKET_DATA

    def test_rejection_stale_market_data(self, paper_engine: PaperExecutionEngine) -> None:
        # Supply a quote that is 300 seconds old
        stale_quote = Quote(
            symbol="INFY",
            exchange="NSE",
            timestamp=time.time() - 300.0,
            last_price=1500.0,
            bid=1499.0,
            ask=1501.0,
        )
        paper_engine.on_market_quote(stale_quote)

        req = PaperOrderRequest(symbol="INFY", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0)
        res = paper_engine.submit_order(req)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.STALE_MARKET_DATA

    def test_rejection_invalid_market_data(self, paper_engine: PaperExecutionEngine) -> None:
        # 1. Inverted quote is rejected at Quote creation
        with pytest.raises(ValidationError, match="Inverted spread invariant violated"):
            Quote(
                symbol="WIPRO",
                exchange="NSE",
                timestamp=time.time(),
                last_price=400.0,
                bid=405.0,
                ask=395.0,
            )

        # 2. Mismatched symbol quote provided explicitly to submit_order
        mismatch_quote = Quote(
            symbol="TCS",
            exchange="NSE",
            timestamp=time.time(),
            last_price=3500.0,
            bid=3499.0,
            ask=3501.0,
        )
        req = PaperOrderRequest(symbol="WIPRO", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        res = paper_engine.submit_order(req, market_quote=mismatch_quote)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.INVALID_MARKET_DATA

    def test_duplicate_order_submission_idempotency(self, paper_engine: PaperExecutionEngine) -> None:
        req = PaperOrderRequest(
            client_order_id="idemp_test_ord_1",
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
        )
        # First submission succeeds
        res1 = paper_engine.submit_order(req)
        assert res1.success is True

        # Second submission with the same client_order_id is rejected with DUPLICATE_ORDER
        res2 = paper_engine.submit_order(req)
        assert res2.success is False
        assert res2.rejection_code == PaperRejectionCode.DUPLICATE_ORDER

    def test_rejected_order_leaves_portfolio_completely_unchanged(self, paper_engine: PaperExecutionEngine) -> None:
        initial_cash = paper_engine.portfolio.available_cash
        initial_positions = dict(paper_engine.portfolio.positions)

        # Invalid order
        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=50.0)
        res = paper_engine.submit_order(req)
        assert res.success is False

        assert paper_engine.portfolio.available_cash == initial_cash
        assert paper_engine.portfolio.positions == initial_positions

    def test_multiple_orders_same_symbol(self, paper_engine: PaperExecutionEngine) -> None:
        # Order 1: Buy 5 @ ~2500
        req1 = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0)
        res1 = paper_engine.submit_order(req1)
        assert res1.success is True
        assert paper_engine.portfolio.get_position("RELIANCE").quantity == 5.0

        # Order 2: Buy 5 more @ ~2500
        req2 = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0)
        res2 = paper_engine.submit_order(req2)
        assert res2.success is True
        assert paper_engine.portfolio.get_position("RELIANCE").quantity == 10.0

        # Order 3: Sell 10 (complete exit)
        req3 = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=10.0)
        res3 = paper_engine.submit_order(req3)
        assert res3.success is True
        assert paper_engine.portfolio.get_position("RELIANCE") is None

    def test_order_cancellation(self, paper_engine: PaperExecutionEngine, fresh_quote: Quote) -> None:
        # Submit a limit buy order below market that cannot fill immediately
        req = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.LIMIT,
            quantity=5.0,
            limit_price=2400.0,  # Below market ask (2500.5)
        )
        res = paper_engine.submit_order(req)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.LIMIT_PRICE_NOT_MET
        assert res.order.status == PaperOrderStatus.ACCEPTED

        # Cancel the pending order
        cancel_res = paper_engine.cancel_order(res.order.order_id, reason="Changed mind")
        assert cancel_res.success is True
        assert cancel_res.order.status == PaperOrderStatus.CANCELLED

    def test_event_bus_quote_subscription(self, paper_engine: PaperExecutionEngine, event_bus: EventBus) -> None:
        # Publishing a MARKET_QUOTE_UPDATED event should update quotes in the paper engine
        quote = Quote(
            symbol="HDFC",
            exchange="NSE",
            timestamp=time.time(),
            last_price=1650.0,
            bid=1649.0,
            ask=1651.0,
        )
        event_bus.publish(
            Event.create(
                event_type=EventType.MARKET_QUOTE_UPDATED,
                source="test_feed",
                payload={
                    "symbol": quote.symbol,
                    "exchange": quote.exchange,
                    "timestamp": quote.timestamp,
                    "last_price": quote.last_price,
                    "bid": quote.bid,
                    "ask": quote.ask,
                    "bid_size": quote.bid_size,
                    "ask_size": quote.ask_size,
                    "volume": quote.volume,
                    "currency": quote.currency,
                    "source": quote.source,
                },
            )
        )
        # Engine should now have the quote cached
        cached = paper_engine.get_quote("HDFC")
        assert cached is not None
        assert cached.last_price == 1650.0

    def test_event_publication_on_order_fill(self, paper_engine: PaperExecutionEngine, event_bus: EventBus) -> None:
        published_events: list[Event] = []

        def _recorder(evt: Event) -> None:
            published_events.append(evt)

        event_bus.subscribe(EventType.PAPER_ORDER_SUBMITTED, _recorder)
        event_bus.subscribe(EventType.PAPER_ORDER_ACCEPTED, _recorder)
        event_bus.subscribe(EventType.PAPER_ORDER_FILLED, _recorder)
        event_bus.subscribe(EventType.PAPER_PORTFOLIO_UPDATED, _recorder)
        event_bus.subscribe(EventType.PAPER_LEDGER_ENTRY_RECORDED, _recorder)

        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0)
        res = paper_engine.submit_order(req)
        assert res.success is True

        event_types = [e.event_type for e in published_events]
        assert EventType.PAPER_ORDER_SUBMITTED in event_types
        assert EventType.PAPER_ORDER_ACCEPTED in event_types
        assert EventType.PAPER_ORDER_FILLED in event_types
        assert EventType.PAPER_PORTFOLIO_UPDATED in event_types
        assert EventType.PAPER_LEDGER_ENTRY_RECORDED in event_types


# ==============================================================================
# 5. BOOTSTRAP, CONFIGURATION & SAFETY INVARIANTS
# ==============================================================================


class TestBootstrapAndSafetyInvariants:
    """Verify safety invariants, configuration loading, and isolation."""

    def test_paper_execution_disabled_by_default(self) -> None:
        status = bootstrap_system(log_to_file=False)
        assert status.settings.paper_execution_enabled is False
        assert status.paper_execution_engine is None
        assert status.paper_ledger is None
        # Safety invariants
        assert status.settings.execution_locked is True
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.settings.broker_provider == "none"

    def test_paper_execution_explicit_activation(self) -> None:
        custom_settings = Settings(
            paper_execution_enabled=True,
            paper_initial_cash=50000.0,
            paper_max_order_notional=25000.0,
        )
        status = bootstrap_system(settings=custom_settings, log_to_file=False)

        # Paper components are initialized
        assert status.paper_execution_engine is not None
        assert status.paper_ledger is not None
        assert status.paper_execution_engine.portfolio.available_cash == 50000.0

        # Architectural safety invariants MUST REMAIN LOCKED
        assert status.settings.execution_locked is True
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.settings.broker_provider == "none"

    def test_paper_trading_cannot_unlock_live_execution(self) -> None:
        custom_settings = Settings(paper_execution_enabled=True)
        status = bootstrap_system(settings=custom_settings, log_to_file=False)

        # Verify state store flags
        assert status.state_store.get("trading_engine") == "not initialized"
        assert status.state_store.get("execution_engine") == "locked"
        assert status.settings.execution_locked is True
        assert status.settings.broker_provider == "none"
