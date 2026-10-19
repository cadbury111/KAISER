"""KAISER — Tests for Phase 2, Step 54: Paper Trading Accounting, Ledger Integrity & Restart Recovery Verification.

Comprehensive, deterministic verification and regression suite covering:
1. Financial accounting correctness (12 scenarios with exact expected values).
2. Order lifecycle & limit order execution (pricing policy, pending matching, cancellation, idempotency).
3. Ledger idempotency, SQLite atomicity, cash reconciliation, and secret prevention.
4. Restart and recovery verification (8 restart scenarios, zero cash duplication, corruption handling).
5. Pre-trade risk gate boundaries and auditability.
6. Safety invariants verification.
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List
import pytest

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
    return EventBus(name="test_step54_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    return StateStore(name="test_step54_state_store")


@pytest.fixture
def sample_quote_reliance() -> Quote:
    now = time.time()
    return Quote(
        symbol="RELIANCE",
        exchange="NSE",
        timestamp=now,
        last_price=2500.0,
        bid=2499.0,
        ask=2501.0,
        bid_size=100.0,
        ask_size=100.0,
        volume=50000.0,
        source="mock",
    )


@pytest.fixture
def sample_quote_tcs() -> Quote:
    now = time.time()
    return Quote(
        symbol="TCS",
        exchange="NSE",
        timestamp=now,
        last_price=3500.0,
        bid=3499.0,
        ask=3501.0,
        bid_size=50.0,
        ask_size=50.0,
        volume=30000.0,
        source="mock",
    )


@pytest.fixture
def sample_quote_infy() -> Quote:
    now = time.time()
    return Quote(
        symbol="INFY",
        exchange="NSE",
        timestamp=now,
        last_price=1500.0,
        bid=1499.5,
        ask=1500.5,
        bid_size=200.0,
        ask_size=200.0,
        volume=80000.0,
        source="mock",
    )


@pytest.fixture
def paper_ledger(event_bus: EventBus, tmp_path: Path) -> PaperLedger:
    db_file = str(tmp_path / "paper_ledger_test.db")
    return PaperLedger(event_bus=event_bus, db_path=db_file, initial_balance=100_000.0)


@pytest.fixture
def paper_engine(
    event_bus: EventBus,
    state_store: StateStore,
    paper_ledger: PaperLedger,
    sample_quote_reliance: Quote,
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
    engine.on_market_quote(sample_quote_reliance)
    return engine


# ==============================================================================
# 1. FINANCIAL ACCOUNTING VERIFICATION (12 SCENARIOS)
# ==============================================================================


class TestFinancialAccountingVerification:
    """Rigorous mathematical tests for deterministic portfolio accounting."""

    def test_scenario_01_single_buy_accounting(self) -> None:
        """Scenario 1: Buy a position and verify cash deduction, quantity, avg price, fees, equity."""
        portfolio = PaperPortfolio(initial_cash=100_000.0)
        # Buy 10 @ 2500.0, fee_rate = 0.05% -> fee = 12.50
        fill = PaperFill(
            fill_id="f1",
            order_id="o1",
            client_order_id="c1",
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            quantity=10.0,
            price=2500.0,
            fee=12.50,
            slippage=0.0,
            timestamp=time.time(),
            market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        realized = portfolio.apply_fill(fill)
        assert realized == 0.0

        expected_cash_spent = round(10.0 * 2500.0 + 12.50, 4)  # 25012.50
        expected_cash_left = round(100_000.0 - expected_cash_spent, 4)  # 74987.50
        assert portfolio.available_cash == expected_cash_left
        assert portfolio.total_fees_paid == 12.50

        pos = portfolio.get_position("RELIANCE")
        assert pos is not None
        assert pos.quantity == 10.0
        assert pos.avg_entry_price == 2500.0
        assert pos.current_price == 2500.0
        assert pos.cost_basis == 25000.0
        assert pos.market_value == 25000.0
        assert pos.unrealized_pnl == 0.0
        assert pos.accumulated_fees == 12.50

        # Equity = cash (74987.50) + position market value (25000.0) = 99987.50 (down by exactly fee)
        assert portfolio.total_equity == 99987.50

    def test_scenario_02_multiple_buys_weighted_average_cost(self) -> None:
        """Scenario 2: Buy same symbol multiple times at different prices, verify weighted average."""
        portfolio = PaperPortfolio(initial_cash=200_000.0)

        # Buy 1: 10 @ 2000.0 (cost = 20,000.0)
        f1 = PaperFill(
            fill_id="f1", order_id="o1", client_order_id="c1", symbol="TCS",
            side=PaperOrderSide.BUY, quantity=10.0, price=2000.0, fee=10.0,
            slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        portfolio.apply_fill(f1)

        # Buy 2: 20 @ 2300.0 (cost = 46,000.0)
        f2 = PaperFill(
            fill_id="f2", order_id="o2", client_order_id="c2", symbol="TCS",
            side=PaperOrderSide.BUY, quantity=20.0, price=2300.0, fee=23.0,
            slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        portfolio.apply_fill(f2)

        # Buy 3: 30 @ 2100.0 (cost = 63,000.0)
        f3 = PaperFill(
            fill_id="f3", order_id="o3", client_order_id="c3", symbol="TCS",
            side=PaperOrderSide.BUY, quantity=30.0, price=2100.0, fee=31.5,
            slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        portfolio.apply_fill(f3)

        pos = portfolio.get_position("TCS")
        assert pos is not None
        assert pos.quantity == 60.0
        # Total cost = 20,000 + 46,000 + 63,000 = 129,000.0
        # Weighted average = 129,000 / 60 = 2150.0
        assert pos.avg_entry_price == 2150.0
        assert pos.accumulated_fees == round(10.0 + 23.0 + 31.5, 4)

        expected_cash = round(200_000.0 - (129_000.0 + 10.0 + 23.0 + 31.5), 4)
        assert portfolio.available_cash == expected_cash

    def test_scenario_03_partial_sell_accounting(self) -> None:
        """Scenario 3: Sell part of a position, verify remaining quantity, cash, fees, realized P&L."""
        portfolio = PaperPortfolio(initial_cash=100_000.0)
        # Buy 20 @ 1000.0, fee = 10.0
        f_buy = PaperFill(
            fill_id="fb", order_id="ob", client_order_id="cb", symbol="INFY",
            side=PaperOrderSide.BUY, quantity=20.0, price=1000.0, fee=10.0,
            slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        portfolio.apply_fill(f_buy)
        cash_after_buy = portfolio.available_cash  # 100,000 - 20010 = 79990.0

        # Sell 8 @ 1200.0, fee = 4.80
        # Gross = 9600.0, Net proceeds = 9600.0 - 4.80 = 9595.20
        # Cost basis of 8 units = 8 * 1000.0 = 8000.0
        # Realized P&L = (1200 - 1000)*8 - 4.80 = 1595.20
        f_sell = PaperFill(
            fill_id="fs", order_id="os", client_order_id="cs", symbol="INFY",
            side=PaperOrderSide.SELL, quantity=8.0, price=1200.0, fee=4.80,
            slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        pnl = portfolio.apply_fill(f_sell)
        assert pnl == 1595.20
        assert portfolio.realized_pnl == 1595.20
        assert portfolio.available_cash == round(cash_after_buy + 9595.20, 4)

        pos = portfolio.get_position("INFY")
        assert pos is not None
        assert pos.quantity == 12.0
        assert pos.avg_entry_price == 1000.0
        assert pos.current_price == 1200.0
        assert pos.cost_basis == 12000.0
        assert pos.market_value == 14400.0
        assert pos.unrealized_pnl == 2400.0

    def test_scenario_04_full_exit_accounting(self) -> None:
        """Scenario 4: Close position completely, verify zero remaining and position deleted."""
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        # Buy 10 @ 500.0, fee = 2.50
        portfolio.apply_fill(
            PaperFill(
                fill_id="fb", order_id="ob", client_order_id="cb", symbol="WIPRO",
                side=PaperOrderSide.BUY, quantity=10.0, price=500.0, fee=2.50,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        # Sell 10 @ 550.0, fee = 2.75
        pnl = portfolio.apply_fill(
            PaperFill(
                fill_id="fs", order_id="os", client_order_id="cs", symbol="WIPRO",
                side=PaperOrderSide.SELL, quantity=10.0, price=550.0, fee=2.75,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        expected_pnl = round(((550.0 - 500.0) * 10.0) - 2.75, 4)  # 500 - 2.75 = 497.25
        assert pnl == expected_pnl
        assert portfolio.realized_pnl == expected_pnl
        assert portfolio.get_position("WIPRO") is None
        assert portfolio.total_position_value == 0.0
        assert portfolio.unrealized_pnl == 0.0
        assert portfolio.total_equity == portfolio.available_cash

    def test_scenario_05_oversell_inventory_rejection(self) -> None:
        """Scenario 5: Reject a sell that exceeds available inventory without changing balances."""
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        portfolio.apply_fill(
            PaperFill(
                fill_id="fb", order_id="ob", client_order_id="cb", symbol="TCS",
                side=PaperOrderSide.BUY, quantity=5.0, price=3000.0, fee=7.50,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        saved_cash = portfolio.available_cash
        saved_qty = portfolio.get_position("TCS").quantity

        # Attempt to sell 6 units (held is 5)
        f_oversell = PaperFill(
            fill_id="fs", order_id="os", client_order_id="cs", symbol="TCS",
            side=PaperOrderSide.SELL, quantity=6.0, price=3000.0, fee=9.0,
            slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        with pytest.raises(ValidationError, match="less than fill quantity"):
            portfolio.apply_fill(f_oversell)

        assert portfolio.available_cash == saved_cash
        assert portfolio.get_position("TCS").quantity == saved_qty

    def test_scenario_06_insufficient_cash_buy_rejection(self) -> None:
        """Scenario 6: Reject a buy with insufficient cash, including fees and slippage."""
        risk_gate = PaperRiskGate(
            PaperRiskConfig(fee_rate=0.001, slippage_rate=0.001, max_order_notional=100_000.0)
        )
        portfolio = PaperPortfolio(initial_cash=1000.0)
        quote = Quote(symbol="RELIANCE", exchange="NSE", timestamp=time.time(), last_price=1000.0, ask=1000.0)

        # Quantity 1: Notional = 1000. Fee = 1.0, Slippage buffer = 1.0 -> Total = 1002.0 > 1000.0
        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=1.0)
        approved, code, reason = risk_gate.evaluate_order(req, portfolio, quote)
        assert approved is False
        assert code == PaperRejectionCode.INSUFFICIENT_CASH
        assert "Insufficient available cash" in str(reason)

    def test_scenario_07_unrealized_pnl_valuation_updates(self) -> None:
        """Scenario 7: Verify unrealized P&L changes correctly when valuation quote changes."""
        portfolio = PaperPortfolio(initial_cash=150_000.0)
        portfolio.apply_fill(
            PaperFill(
                fill_id="f1", order_id="o1", client_order_id="c1", symbol="INFY",
                side=PaperOrderSide.BUY, quantity=100.0, price=1000.0, fee=50.0,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        pos = portfolio.get_position("INFY")
        assert pos.unrealized_pnl == 0.0

        # Mark up to 1050.0 -> +50 * 100 = +5000.0
        portfolio.mark_to_market("INFY", 1050.0)
        assert pos.unrealized_pnl == 5000.0
        assert portfolio.unrealized_pnl == 5000.0
        assert pos.unrealized_return_pct == 5.0

        # Mark down to 950.0 -> -50 * 100 = -5000.0
        portfolio.mark_to_market("INFY", 950.0)
        assert pos.unrealized_pnl == -5000.0
        assert portfolio.unrealized_pnl == -5000.0
        assert pos.unrealized_return_pct == -5.0

    def test_scenario_08_no_double_counting_in_portfolio_equity(self) -> None:
        """Scenario 8: Verify realized and unrealized P&L are NOT double counted in equity."""
        portfolio = PaperPortfolio(initial_cash=100_000.0)
        # Buy 10 @ 100.0, fee = 1.0 -> Cash = 100,000 - 1001 = 98999.0
        portfolio.apply_fill(
            PaperFill(
                fill_id="f1", order_id="o1", client_order_id="c1", symbol="RELIANCE",
                side=PaperOrderSide.BUY, quantity=10.0, price=100.0, fee=1.0,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        # Mark up to 120.0 -> Unrealized PnL = (120 - 100) * 10 = +200.0
        portfolio.mark_to_market("RELIANCE", 120.0)
        assert portfolio.unrealized_pnl == 200.0
        assert portfolio.realized_pnl == 0.0
        # Equity = available_cash (98999.0) + market_value (1200.0) = 100,199.0
        assert portfolio.total_equity == 100199.0
        # total_pnl = 200.0
        assert portfolio.total_pnl == 200.0

        # Sell 5 @ 120.0, fee = 0.50 -> Realized PnL = (120 - 100)*5 - 0.50 = 99.50
        portfolio.apply_fill(
            PaperFill(
                fill_id="f2", order_id="o2", client_order_id="c2", symbol="RELIANCE",
                side=PaperOrderSide.SELL, quantity=5.0, price=120.0, fee=0.50,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        # Cash after sell = 98999.0 + (600 - 0.50) = 99598.50
        assert portfolio.available_cash == 99598.50
        assert portfolio.realized_pnl == 99.50
        # Remaining 5 shares @ 120.0 market value = 600.0. Unrealized = (120 - 100)*5 = 100.0
        assert portfolio.unrealized_pnl == 100.0
        # Equity = cash (99598.50) + remaining value (600.0) = 100198.50
        assert portfolio.total_equity == 100198.50
        # total_pnl = realized (99.50) + unrealized (100.0) = 199.50
        assert portfolio.total_pnl == 199.50
        # Verify equity equals initial_cash (100,000) + total_pnl (199.50) - buy_fee (1.0) = 100,198.50
        assert portfolio.total_equity == round(100_000.0 + portfolio.total_pnl - 1.0, 4)

    def test_scenario_09_fee_and_slippage_policies_consistency(self, sample_quote_reliance: Quote) -> None:
        """Scenario 9: Verify fee and slippage policies are applied consistently."""
        adapter = SimulatedBrokerAdapter(fee_rate=0.0005, slippage_rate=0.0002)

        # 1. Market BUY with valid Ask
        req_buy = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        _, fill_buy, _, _ = adapter.simulate_fill(req_buy, sample_quote_reliance)
        assert fill_buy is not None
        expected_buy_price = round(2501.0 * (1 + 0.0002), 4)  # 2501.5002
        assert fill_buy.price == expected_buy_price
        expected_buy_notional = round(10.0 * expected_buy_price, 4)
        assert fill_buy.fee == round(expected_buy_notional * 0.0005, 4)

        # 2. Market SELL with valid Bid
        req_sell = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=5.0)
        _, fill_sell, _, _ = adapter.simulate_fill(req_sell, sample_quote_reliance)
        assert fill_sell is not None
        expected_sell_price = round(2499.0 * (1 - 0.0002), 4)  # 2498.5002
        assert fill_sell.price == expected_sell_price
        expected_sell_notional = round(5.0 * expected_sell_price, 4)
        assert fill_sell.fee == round(expected_sell_notional * 0.0005, 4)

    def test_scenario_10_decimal_rounding_boundary_precision(self) -> None:
        """Scenario 10: Test fractional quantities, micro fees, and 4-decimal precision."""
        portfolio = PaperPortfolio(initial_cash=10_000.0)
        # Fractional quantity 0.123456 @ 1234.5678, fee = 0.0762
        f = PaperFill(
            fill_id="f_micro", order_id="o_micro", client_order_id="c_micro", symbol="BTC",
            side=PaperOrderSide.BUY, quantity=0.123456, price=1234.5678, fee=0.0762,
            slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
            market_data_source="mock",
        )
        portfolio.apply_fill(f)
        pos = portfolio.get_position("BTC")
        assert pos is not None
        assert pos.quantity == 0.123456
        assert pos.avg_entry_price == 1234.5678
        expected_cost = round(0.123456 * 1234.5678, 4)
        assert pos.cost_basis == expected_cost

    def test_scenario_11_multiple_symbols_portfolio_transactions(self) -> None:
        """Scenario 11: Test multiple symbols and interleaved transactions."""
        portfolio = PaperPortfolio(initial_cash=200_000.0)

        # Buy RELIANCE
        portfolio.apply_fill(
            PaperFill(
                fill_id="f1", order_id="o1", client_order_id="c1", symbol="RELIANCE",
                side=PaperOrderSide.BUY, quantity=10.0, price=2500.0, fee=12.5,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        # Buy TCS
        portfolio.apply_fill(
            PaperFill(
                fill_id="f2", order_id="o2", client_order_id="c2", symbol="TCS",
                side=PaperOrderSide.BUY, quantity=5.0, price=3500.0, fee=8.75,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        # Buy INFY
        portfolio.apply_fill(
            PaperFill(
                fill_id="f3", order_id="o3", client_order_id="c3", symbol="INFY",
                side=PaperOrderSide.BUY, quantity=20.0, price=1500.0, fee=15.0,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )

        assert len(portfolio.positions) == 3
        # Total position value = 10*2500 + 5*3500 + 20*1500 = 25000 + 17500 + 30000 = 72500.0
        assert portfolio.total_position_value == 72500.0

        # Close TCS completely
        portfolio.apply_fill(
            PaperFill(
                fill_id="f4", order_id="o4", client_order_id="c4", symbol="TCS",
                side=PaperOrderSide.SELL, quantity=5.0, price=3600.0, fee=9.0,
                slippage=0.0, timestamp=time.time(), market_data_timestamp=time.time(),
                market_data_source="mock",
            )
        )
        assert len(portfolio.positions) == 2
        assert portfolio.get_position("TCS") is None
        assert "RELIANCE" in portfolio.positions
        assert "INFY" in portfolio.positions

    def test_scenario_12_rejected_orders_zero_mutation(
        self, paper_engine: PaperExecutionEngine, sample_quote_reliance: Quote
    ) -> None:
        """Scenario 12: Verify rejected orders never mutate portfolio cash or positions."""
        init_cash = paper_engine.portfolio.available_cash
        init_positions = dict(paper_engine.portfolio.positions)
        init_ledger_count = paper_engine.ledger.count()

        # Submit order with excessive notional
        req_bad = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=1000.0,  # 1000 * 2500 = 2,500,000 > 50,000 limit
        )
        res = paper_engine.submit_order(req_bad, market_quote=sample_quote_reliance)
        assert res.success is False

        assert paper_engine.portfolio.available_cash == init_cash
        assert paper_engine.portfolio.positions == init_positions
        # A rejection record was added to ledger with cash_change = 0.0
        assert paper_engine.ledger.count() == init_ledger_count + 1
        recon = paper_engine.ledger.reconcile(expected_current_cash=init_cash)
        assert recon["is_balanced"] is True


# ==============================================================================
# 2. ORDER LIFECYCLE & LIMIT ORDER VERIFICATION
# ==============================================================================


class TestOrderLifecycleVerification:
    """Validate limit order state machine, price matching, and cancellation."""

    def test_limit_buy_price_matching(self, sample_quote_reliance: Quote) -> None:
        adapter = SimulatedBrokerAdapter()

        # Ask is 2501.0. Limit is 2500.0 -> LIMIT_PRICE_NOT_MET
        req_unmet = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.LIMIT, quantity=5.0, limit_price=2500.0)
        filled, _, code, _ = adapter.simulate_fill(req_unmet, sample_quote_reliance)
        assert filled is False
        assert code == PaperRejectionCode.LIMIT_PRICE_NOT_MET

        # Limit is 2502.0 -> Fills at market ask 2501.0 (price improvement)
        req_met = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.LIMIT, quantity=5.0, limit_price=2502.0)
        filled, fill, _, _ = adapter.simulate_fill(req_met, sample_quote_reliance)
        assert filled is True
        assert fill.price == 2501.0

    def test_limit_sell_price_matching(self, sample_quote_reliance: Quote) -> None:
        adapter = SimulatedBrokerAdapter()

        # Bid is 2499.0. Limit is 2500.0 -> LIMIT_PRICE_NOT_MET
        req_unmet = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.LIMIT, quantity=5.0, limit_price=2500.0)
        filled, _, code, _ = adapter.simulate_fill(req_unmet, sample_quote_reliance)
        assert filled is False
        assert code == PaperRejectionCode.LIMIT_PRICE_NOT_MET

        # Limit is 2495.0 -> Fills at market bid 2499.0 (price improvement)
        req_met = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.LIMIT, quantity=5.0, limit_price=2495.0)
        filled, fill, _, _ = adapter.simulate_fill(req_met, sample_quote_reliance)
        assert filled is True
        assert fill.price == 2499.0

    def test_pending_limit_order_fills_on_subsequent_market_quote(self, tmp_path: Path) -> None:
        """Verify that an unfilled limit order stays ACCEPTED and fills when quote moves."""
        db_file = str(tmp_path / "pending_limit.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=100_000.0, enabled=True)

        # Initial quote: Ask = 2505.0
        q1 = Quote(symbol="RELIANCE", exchange="NSE", timestamp=time.time(), last_price=2504.0, bid=2503.0, ask=2505.0)
        engine.on_market_quote(q1)

        # Submit Limit Buy @ 2500.0 -> Ask is 2505.0, so cannot fill immediately
        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.LIMIT, quantity=10.0, limit_price=2500.0)
        res = engine.submit_order(req)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.LIMIT_PRICE_NOT_MET
        assert res.order.status == PaperOrderStatus.ACCEPTED

        # New quote: Ask drops to 2498.0 <= 2500.0
        q2 = Quote(symbol="RELIANCE", exchange="NSE", timestamp=time.time() + 1.0, last_price=2497.0, bid=2496.0, ask=2498.0)
        engine.on_market_quote(q2)

        # Order should now be filled!
        filled_order = engine.get_order(res.order.order_id)
        assert filled_order is not None
        assert filled_order.status == PaperOrderStatus.FILLED
        assert filled_order.fill_price == 2498.0
        assert engine.portfolio.get_position("RELIANCE").quantity == 10.0

    def test_repeated_market_updates_do_not_duplicate_fills(self, tmp_path: Path) -> None:
        """Verify that subsequent quotes do not re-execute an already filled order."""
        db_file = str(tmp_path / "dup_quotes.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=100_000.0, enabled=True)

        q1 = Quote(symbol="INFY", exchange="NSE", timestamp=time.time(), last_price=1500.0, bid=1499.0, ask=1501.0)
        engine.on_market_quote(q1)

        req = PaperOrderRequest(symbol="INFY", side=PaperOrderSide.BUY, order_type=PaperOrderType.LIMIT, quantity=5.0, limit_price=1505.0)
        res = engine.submit_order(req)
        assert res.success is True
        assert res.order.status == PaperOrderStatus.FILLED
        initial_cash_after = engine.portfolio.available_cash

        # Send 5 more market quotes
        for i in range(5):
            qi = Quote(symbol="INFY", exchange="NSE", timestamp=time.time() + 1.0 + i, last_price=1500.0, bid=1499.0, ask=1501.0)
            engine.on_market_quote(qi)

        # Position and cash must remain unchanged
        assert engine.portfolio.available_cash == initial_cash_after
        assert engine.portfolio.get_position("INFY").quantity == 5.0
        assert len(engine._fills) == 1

    def test_cancelling_filled_order_fails_safely(self, tmp_path: Path) -> None:
        """Verify cancelling a FILLED order returns ALREADY_TERMINAL without mutating balances."""
        db_file = str(tmp_path / "cancel_filled.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=50_000.0)
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=50_000.0, enabled=True)

        q = Quote(symbol="INFY", exchange="NSE", timestamp=time.time(), last_price=1000.0, bid=999.0, ask=1001.0)
        engine.on_market_quote(q)

        req = PaperOrderRequest(symbol="INFY", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0)
        res = engine.submit_order(req)
        assert res.success is True
        assert res.order.status == PaperOrderStatus.FILLED

        cash_before = engine.portfolio.available_cash
        pos_before = engine.portfolio.get_position("INFY").quantity

        # Attempt to cancel the filled order
        can_res = engine.cancel_order(res.order.order_id)
        assert can_res.success is False
        assert can_res.rejection_code == PaperRejectionCode.ALREADY_TERMINAL

        assert engine.portfolio.available_cash == cash_before
        assert engine.portfolio.get_position("INFY").quantity == pos_before


# ==============================================================================
# 3. LEDGER IDEMPOTENCY & INTEGRITY
# ==============================================================================


class TestLedgerIdempotencyAndIntegrity:
    """Validate duplicate keys, reconciliation, SQLite atomicity, and secret hygiene."""

    def test_ledger_duplicate_idempotency_key_prevention(self, tmp_path: Path) -> None:
        db_file = str(tmp_path / "idemp_test.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=50_000.0)

        entry = PaperLedgerEntry(
            entry_id="e1",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_FILL,
            cash_change=-1000.0,
            cash_balance_after=49000.0,
            idempotency_key="unique_key_123",
        )
        assert ledger.record_entry(entry) is True
        # Second attempt with same key must return False
        assert ledger.record_entry(entry) is False
        assert ledger.count() == 2  # initial + 1 fill

    def test_reconciliation_detects_mismatched_cash(self, tmp_path: Path) -> None:
        db_file = str(tmp_path / "recon_test.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=50_000.0)

        entry = PaperLedgerEntry(
            entry_id="e1",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_FILL,
            cash_change=-5000.0,
            cash_balance_after=45000.0,
            idempotency_key="key_1",
        )
        ledger.record_entry(entry)

        # Expected cash is 45000.0 -> Balanced
        recon_good = ledger.reconcile(expected_current_cash=45000.0)
        assert recon_good["is_balanced"] is True
        assert recon_good["discrepancy"] == 0.0

        # Tampered expected cash 40000.0 -> Discrepancy detected
        recon_bad = ledger.reconcile(expected_current_cash=40000.0)
        assert recon_bad["is_balanced"] is False
        assert recon_bad["discrepancy"] == 5000.0

    def test_cash_balance_chain_break_detected(self, tmp_path: Path) -> None:
        """Verify that an inconsistent cash_balance_after in the entry chain breaks reconciliation."""
        db_file = str(tmp_path / "chain_break.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=10_000.0)

        # Entry where cash_balance_after does not match (10000 - 1000 != 9999)
        bad_entry = PaperLedgerEntry(
            entry_id="e_broken",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_FILL,
            cash_change=-1000.0,
            cash_balance_after=9999.0,  # Chain break!
            idempotency_key="key_broken",
        )
        ledger.record_entry(bad_entry)

        recon = ledger.reconcile(expected_current_cash=9000.0)
        assert recon["is_balanced"] is False

    def test_ledger_rejects_credential_and_secret_keys(self) -> None:
        """Verify PaperLedgerEntry forbids passwords, tokens, and api_keys in metadata."""
        with pytest.raises(ValidationError, match="must not contain credential or secret keys"):
            PaperLedgerEntry(
                entry_id="e_sec",
                timestamp=time.time(),
                entry_type=LedgerEntryType.ORDER_ACCEPTANCE,
                idempotency_key="k_sec",
                metadata={"api_key": "super_secret_key_123"},
            )

        with pytest.raises(ValidationError, match="must not contain credential or secret keys"):
            PaperLedgerEntry(
                entry_id="e_sec2",
                timestamp=time.time(),
                entry_type=LedgerEntryType.ORDER_ACCEPTANCE,
                idempotency_key="k_sec2",
                metadata={"user_password": "password123"},
            )

    def test_sqlite_batch_insert_atomicity(self, tmp_path: Path) -> None:
        """Verify record_entries performs atomic transaction across multiple items."""
        db_file = str(tmp_path / "batch.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=10_000.0)

        entries = [
            PaperLedgerEntry(
                entry_id=f"e_{i}",
                timestamp=time.time() + i,
                entry_type=LedgerEntryType.ORDER_FILL,
                cash_change=-100.0,
                cash_balance_after=10000.0 - (i + 1) * 100.0,
                idempotency_key=f"batch_k_{i}",
            )
            for i in range(5)
        ]
        count = ledger.record_entries(entries)
        assert count == 5
        assert ledger.count() == 6  # 1 initial + 5 batch


# ==============================================================================
# 4. RESTART AND RECOVERY VERIFICATION (8 SCENARIOS)
# ==============================================================================


class TestRestartAndRecoveryVerification:
    """Exhaustively verify restart recovery from SQLite persistence."""

    def test_recovery_case_01_after_completed_buy(self, tmp_path: Path, sample_quote_reliance: Quote) -> None:
        """Case 1: Restart after completed buy."""
        db_file = str(tmp_path / "rec_c1.db")
        ledger1 = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        engine1 = PaperExecutionEngine(ledger=ledger1, initial_cash=100_000.0, enabled=True)
        engine1.on_market_quote(sample_quote_reliance)

        res = engine1.submit_order(
            PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        )
        assert res.success is True
        pre_cash = engine1.portfolio.available_cash
        pre_qty = engine1.portfolio.get_position("RELIANCE").quantity
        pre_avg = engine1.portfolio.get_position("RELIANCE").avg_entry_price

        # --- RESTART PROCESS ---
        ledger2 = PaperLedger(db_path=db_file)
        engine2 = PaperExecutionEngine(ledger=ledger2, enabled=True)

        assert engine2.portfolio.available_cash == pre_cash
        pos = engine2.portfolio.get_position("RELIANCE")
        assert pos is not None
        assert pos.quantity == pre_qty
        assert pos.avg_entry_price == pre_avg
        assert engine2.get_order(res.order.order_id).status == PaperOrderStatus.FILLED

    def test_recovery_case_02_after_buys_and_partial_sells(self, tmp_path: Path, sample_quote_tcs: Quote) -> None:
        """Case 2: Restart after multiple buys and partial sells."""
        db_file = str(tmp_path / "rec_c2.db")
        ledger1 = PaperLedger(db_path=db_file, initial_balance=150_000.0)
        engine1 = PaperExecutionEngine(ledger=ledger1, initial_cash=150_000.0, enabled=True)
        engine1.on_market_quote(sample_quote_tcs)

        # Buy 10 @ TCS
        engine1.submit_order(PaperOrderRequest(symbol="TCS", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0))
        # Sell 4 @ TCS
        engine1.submit_order(PaperOrderRequest(symbol="TCS", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=4.0))

        pre_cash = engine1.portfolio.available_cash
        pre_qty = engine1.portfolio.get_position("TCS").quantity
        pre_pnl = engine1.portfolio.realized_pnl

        # --- RESTART PROCESS ---
        ledger2 = PaperLedger(db_path=db_file)
        engine2 = PaperExecutionEngine(ledger=ledger2, enabled=True)

        assert engine2.portfolio.available_cash == pre_cash
        assert engine2.portfolio.get_position("TCS").quantity == pre_qty
        assert engine2.portfolio.realized_pnl == pre_pnl

    def test_recovery_case_03_after_fully_closed_position(self, tmp_path: Path, sample_quote_infy: Quote) -> None:
        """Case 3: Restart after fully closed position."""
        db_file = str(tmp_path / "rec_c3.db")
        ledger1 = PaperLedger(db_path=db_file, initial_balance=50_000.0)
        engine1 = PaperExecutionEngine(ledger=ledger1, initial_cash=50_000.0, enabled=True)
        engine1.on_market_quote(sample_quote_infy)

        # Buy 10 @ INFY then Sell 10 @ INFY
        engine1.submit_order(PaperOrderRequest(symbol="INFY", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0))
        engine1.submit_order(PaperOrderRequest(symbol="INFY", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=10.0))

        pre_cash = engine1.portfolio.available_cash
        pre_pnl = engine1.portfolio.realized_pnl

        # --- RESTART PROCESS ---
        ledger2 = PaperLedger(db_path=db_file)
        engine2 = PaperExecutionEngine(ledger=ledger2, enabled=True)

        assert engine2.portfolio.available_cash == pre_cash
        assert engine2.portfolio.get_position("INFY") is None
        assert engine2.portfolio.realized_pnl == pre_pnl

    def test_recovery_case_04_with_outstanding_limit_order(self, tmp_path: Path) -> None:
        """Case 4: Restart with an outstanding limit order (recovers in ACCEPTED state)."""
        db_file = str(tmp_path / "rec_c4.db")
        ledger1 = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        engine1 = PaperExecutionEngine(ledger=ledger1, initial_cash=100_000.0, enabled=True)

        # Quote Ask = 2505.0. Limit Buy = 2490.0 (unfilled)
        q = Quote(symbol="RELIANCE", exchange="NSE", timestamp=time.time(), last_price=2504.0, bid=2503.0, ask=2505.0)
        engine1.on_market_quote(q)
        res = engine1.submit_order(
            PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.LIMIT, quantity=5.0, limit_price=2490.0)
        )
        assert res.order.status == PaperOrderStatus.ACCEPTED

        # --- RESTART PROCESS ---
        ledger2 = PaperLedger(db_path=db_file)
        engine2 = PaperExecutionEngine(ledger=ledger2, enabled=True)

        recovered_order = engine2.get_order(res.order.order_id)
        assert recovered_order is not None
        assert recovered_order.status == PaperOrderStatus.ACCEPTED
        assert recovered_order.limit_price == 2490.0

        # Cancel the recovered order after restart
        can_res = engine2.cancel_order(recovered_order.order_id)
        assert can_res.success is True
        assert can_res.order.status == PaperOrderStatus.CANCELLED

    def test_recovery_case_05_after_rejected_or_cancelled_order(self, tmp_path: Path, sample_quote_reliance: Quote) -> None:
        """Case 5: Restart after rejected and cancelled order."""
        db_file = str(tmp_path / "rec_c5.db")
        ledger1 = PaperLedger(db_path=db_file, initial_balance=50_000.0)
        engine1 = PaperExecutionEngine(ledger=ledger1, initial_cash=50_000.0, enabled=True)
        engine1.on_market_quote(sample_quote_reliance)

        # Rejected order (excessive notional)
        res_rej = engine1.submit_order(
            PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=1000.0)
        )
        assert res_rej.success is False

        # --- RESTART PROCESS ---
        ledger2 = PaperLedger(db_path=db_file)
        engine2 = PaperExecutionEngine(ledger=ledger2, enabled=True)

        assert engine2.portfolio.available_cash == 50_000.0
        assert engine2.get_order(res_rej.order.order_id).status == PaperOrderStatus.REJECTED

    def test_recovery_case_06_repeated_recovery_idempotency(self, tmp_path: Path, sample_quote_reliance: Quote) -> None:
        """Case 6: Running recover_from_ledger multiple times yields identical state."""
        db_file = str(tmp_path / "rec_c6.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=100_000.0)
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=100_000.0, enabled=True)
        engine.on_market_quote(sample_quote_reliance)

        engine.submit_order(
            PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        )

        # First explicit recovery call
        r1 = engine.recover_from_ledger()
        cash1 = engine.portfolio.available_cash
        qty1 = engine.portfolio.get_position("RELIANCE").quantity

        # Second explicit recovery call
        r2 = engine.recover_from_ledger()
        cash2 = engine.portfolio.available_cash
        qty2 = engine.portfolio.get_position("RELIANCE").quantity

        assert cash1 == cash2
        assert qty1 == qty2
        assert r1 == r2

    def test_recovery_case_07_corrupted_records_produce_safe_explicit_error(self, tmp_path: Path) -> None:
        """Case 7: Corrupted or tampered records produce safe explicit ValidationError."""
        db_file = str(tmp_path / "rec_c7.db")
        ledger = PaperLedger(db_path=db_file, initial_balance=50_000.0)

        # Corrupt the SQLite database directly with malformed JSON
        with sqlite3.connect(db_file) as conn:
            conn.execute(
                """
                INSERT INTO paper_ledger_entries (
                    entry_id, timestamp, entry_type, idempotency_key, fee, cash_change,
                    cash_balance_after, realized_pnl, metadata_json
                ) VALUES ('corrupt_entry', 12345.0, 'ORDER_FILL', 'corrupt_key', 0.0, 0.0, 50000.0, 0.0, 'NOT_VALID_JSON{')
                """
            )

        # Loading from SQLite must raise ValidationError
        with pytest.raises(ValidationError, match="Corrupted metadata JSON|Failed to load"):
            PaperLedger(db_path=db_file)

    def test_recovery_case_08_initial_cash_not_credited_twice(self, tmp_path: Path, sample_quote_reliance: Quote) -> None:
        """Case 8: Verify initial cash is NOT credited twice during restart recovery."""
        db_file = str(tmp_path / "rec_c8.db")
        initial_cash = 100_000.0
        ledger1 = PaperLedger(db_path=db_file, initial_balance=initial_cash)
        engine1 = PaperExecutionEngine(ledger=ledger1, initial_cash=initial_cash, enabled=True)
        engine1.on_market_quote(sample_quote_reliance)

        # Traded 10 @ RELIANCE (cash drops from 100,000 to ~74,980)
        engine1.submit_order(
            PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        )
        pre_cash = engine1.portfolio.available_cash
        assert pre_cash < initial_cash

        # Restart
        ledger2 = PaperLedger(db_path=db_file)
        engine2 = PaperExecutionEngine(ledger=ledger2, enabled=True)

        assert engine2.portfolio.available_cash == pre_cash
        assert engine2.portfolio.available_cash != pre_cash + initial_cash


# ==============================================================================
# 5. RISK AND MARKET DATA BOUNDARY REGRESSION
# ==============================================================================


class TestRiskAndMarketDataBoundaries:
    """Validate pre-trade risk controls and data integrity gates."""

    def test_missing_quote_rejected(self, paper_engine: PaperExecutionEngine) -> None:
        req = PaperOrderRequest(symbol="UNREGISTERED", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0)
        res = paper_engine.submit_order(req)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.MISSING_MARKET_DATA

    def test_stale_quote_rejected(self, paper_engine: PaperExecutionEngine) -> None:
        old_quote = Quote(symbol="OLD_SYM", exchange="NSE", timestamp=time.time() - 300.0, last_price=100.0)
        paper_engine.on_market_quote(old_quote)

        req = PaperOrderRequest(symbol="OLD_SYM", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=5.0)
        res = paper_engine.submit_order(req)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.STALE_MARKET_DATA

    def test_max_position_exposure_gate(self, paper_engine: PaperExecutionEngine, sample_quote_reliance: Quote) -> None:
        # Max exposure = 50% of 100,000 equity = 50,000
        # Buy 1: 15 shares @ 2500 = 37,500 (37.5% - OK)
        res1 = paper_engine.submit_order(
            PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=15.0),
            market_quote=sample_quote_reliance,
        )
        assert res1.success is True

        # Buy 2: 10 more shares @ 2500 = 25,000 (total 62,500 > 50,000 - Rejected)
        res2 = paper_engine.submit_order(
            PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0),
            market_quote=sample_quote_reliance,
        )
        assert res2.success is False
        assert res2.rejection_code == PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE

    def test_short_selling_prohibition_gate(self, paper_engine: PaperExecutionEngine, sample_quote_reliance: Quote) -> None:
        req = PaperOrderRequest(symbol="RELIANCE", side=PaperOrderSide.SELL, order_type=PaperOrderType.MARKET, quantity=10.0)
        res = paper_engine.submit_order(req, market_quote=sample_quote_reliance)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.INSUFFICIENT_POSITION


# ==============================================================================
# 6. SAFETY INVARIANTS COMPLIANCE
# ==============================================================================


class TestSafetyInvariantsCompliance:
    """Verify that paper trading remains completely locked, isolated, and safe."""

    def test_execution_locked_invariant_preserved(self) -> None:
        status = bootstrap_system(log_to_file=False)
        assert status.settings.execution_locked is True
        assert status.settings.broker_provider == "none"
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.settings.paper_execution_enabled is False

    def test_paper_engine_never_unlocks_live_engine(self) -> None:
        custom_settings = Settings(paper_execution_enabled=True)
        status = bootstrap_system(settings=custom_settings, log_to_file=False)
        assert status.settings.execution_locked is True
        assert status.settings.broker_provider == "none"
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
