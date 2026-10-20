"""KAISER — Tests for Phase 2, Step 59: Paper Trading Simulation Reliability & Deterministic Replay Validation.

Comprehensive deterministic verification and regression suite covering:
1. Deterministic Replay Validation (identical inputs produce bit-level identical financial outcomes and digests).
2. Replay Integrity & Input Validation (strict chronological ordering, non-finite rejection, inverted spread, idempotency).
3. Accounting & Persistence Invariants (rejected order invariance, weighted average cost, realized P&L, cash chain integrity).
4. Failure Injection & Restart Recovery (interrupted replay, corrupted ledger detection, publication failure, SQLite rollback).
5. Observability & Auditability (audit trails, input/financial digests, session summaries, and safety invariants).
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List, Optional
import pytest

from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ConfigurationError, ValidationError
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
    PaperPosition,
    PaperRejectionCode,
    PaperRiskConfig,
    PaperSimulationSession,
    PaperTradingWorkflowCoordinator,
    PaperWorkflowOutcome,
    PaperWorkflowRequest,
    SimulatedBrokerAdapter,
    WorkflowStage,
    WorkflowStatus,
    compute_financial_digest,
)
from app.ledger import LedgerEntryType, PaperLedger, PaperLedgerEntry
from app.market.feeds.replay import ReplayMarketFeed
from app.market.models import Bar, Quote
from app.market.service import MarketDataService
from app.voice.intent import VoiceCommandIntentResolver, VoiceIntentType


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_step59_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    return StateStore(name="test_step59_state_store")


@pytest.fixture
def deterministic_quotes() -> List[Quote]:
    """Pre-recorded deterministic quote stream for testing."""
    base_ts = time.time()
    return [
        Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=base_ts + 0.0,
            last_price=2500.0,
            bid=2499.5,
            ask=2500.5,
            bid_size=100.0,
            ask_size=100.0,
            volume=10000.0,
            source="replay",
        ),
        Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=base_ts + 10.0,
            last_price=2505.0,
            bid=2504.5,
            ask=2505.5,
            bid_size=150.0,
            ask_size=150.0,
            volume=15000.0,
            source="replay",
        ),
        Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=base_ts + 20.0,
            last_price=2510.0,
            bid=2509.5,
            ask=2510.5,
            bid_size=200.0,
            ask_size=200.0,
            volume=20000.0,
            source="replay",
        ),
        Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=base_ts + 30.0,
            last_price=2490.0,
            bid=2489.5,
            ask=2490.5,
            bid_size=100.0,
            ask_size=100.0,
            volume=25000.0,
            source="replay",
        ),
        Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=base_ts + 40.0,
            last_price=2520.0,
            bid=2519.5,
            ask=2520.5,
            bid_size=120.0,
            ask_size=120.0,
            volume=30000.0,
            source="replay",
        ),
    ]


@pytest.fixture
def paper_ledger(event_bus: EventBus, tmp_path: Path) -> PaperLedger:
    db_file = str(tmp_path / "step59_ledger.db")
    return PaperLedger(event_bus=event_bus, db_path=db_file, initial_balance=100_000.0)


@pytest.fixture
def paper_engine(
    event_bus: EventBus,
    state_store: StateStore,
    paper_ledger: PaperLedger,
    deterministic_quotes: List[Quote],
) -> PaperExecutionEngine:
    risk_config = PaperRiskConfig(
        max_order_notional=50_000.0,
        max_position_exposure_pct=50.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        freshness_threshold_seconds=120.0,
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
    engine.on_market_quote(deterministic_quotes[0])
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
# 1. DETERMINISTIC REPLAY VALIDATION
# ==============================================================================


class TestDeterministicReplayValidation:
    """Verifies that identical input data and order sequences yield bit-level identical outputs."""

    def test_repeated_simulation_produces_identical_financial_digest(
        self,
        deterministic_quotes: List[Quote],
        tmp_path: Path,
    ) -> None:
        """Run identical simulations in isolated environments and verify bit-level equivalent digests."""
        digests: List[str] = []
        final_cash: List[float] = []
        final_equity: List[float] = []

        for run_idx in range(3):
            bus = EventBus(name=f"bus_{run_idx}")
            store = StateStore(name=f"store_{run_idx}")
            db_file = str(tmp_path / f"sim_run_{run_idx}.db")
            ledger = PaperLedger(event_bus=bus, db_path=db_file, initial_balance=100_000.0)
            risk = PaperRiskConfig(
                max_order_notional=50_000.0,
                max_position_exposure_pct=50.0,
                fee_rate=0.0005,
                slippage_rate=0.0002,
                freshness_threshold_seconds=300.0,
            )
            engine = PaperExecutionEngine(
                event_bus=bus,
                state_store=store,
                ledger=ledger,
                initial_cash=100_000.0,
                risk_config=risk,
                fee_rate=0.0005,
                slippage_rate=0.0002,
                enabled=True,
            )
            coord = PaperTradingWorkflowCoordinator(engine=engine, event_bus=bus, state_store=store, enabled=True)

            # Sequence of actions with fixed reference clock
            # 1. First quote (ts=1700000000.0, ask=2500.5)
            q0 = deterministic_quotes[0]
            engine.on_market_quote(q0)

            # 2. Buy order at q0
            req1 = PaperWorkflowRequest(
                symbol="RELIANCE",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=10.0,
                client_order_id="det_ord_01",
                correlation_id="det_cid_01",
            )
            out1 = coord.submit_paper_order(req1, market_quote=q0, reference_time=q0.timestamp)
            assert out1.success is True

            # 3. Second quote (ts=1700000010.0, price rises to 2505)
            q1 = deterministic_quotes[1]
            engine.on_market_quote(q1)

            # 4. Partial sell order at q1
            req2 = PaperWorkflowRequest(
                symbol="RELIANCE",
                side=PaperOrderSide.SELL,
                order_type=PaperOrderType.MARKET,
                quantity=4.0,
                client_order_id="det_ord_02",
                correlation_id="det_cid_02",
            )
            out2 = coord.submit_paper_order(req2, market_quote=q1, reference_time=q1.timestamp)
            assert out2.success is True

            # 5. Third quote (ts=1700000020.0, price rises to 2510)
            q2 = deterministic_quotes[2]
            engine.on_market_quote(q2)

            digest = compute_financial_digest(engine.portfolio, engine.ledger)
            digests.append(digest)
            final_cash.append(engine.portfolio.available_cash)
            final_equity.append(engine.portfolio.total_equity)

        # All 3 independent simulation runs must produce identical financial state and SHA-256 digest
        assert digests[0] == digests[1] == digests[2]
        assert final_cash[0] == final_cash[1] == final_cash[2]
        assert final_equity[0] == final_equity[1] == final_equity[2]

    def test_replay_market_feed_deterministic_digest(self, deterministic_quotes: List[Quote]) -> None:
        """Verify ReplayMarketFeed compute_input_digest produces identical cryptographic fingerprints."""
        feed1 = ReplayMarketFeed(name="feed1", records=deterministic_quotes)
        feed2 = ReplayMarketFeed(name="feed2", records=deterministic_quotes)

        digest1 = feed1.compute_input_digest()
        digest2 = feed2.compute_input_digest()

        assert digest1 == digest2
        assert len(digest1) == 64  # SHA-256 hex length
        assert feed1.get_replay_stats()["input_digest"] == digest1

    def test_pending_limit_order_replay_triggers_in_exact_sequence(
        self,
        paper_engine: PaperExecutionEngine,
        coordinator: PaperTradingWorkflowCoordinator,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Limit order waiting for a price drop is filled at the exact quote where condition is satisfied."""
        # Current quote is q0 (last_price=2500). Submit a BUY limit at 2490.5
        limit_req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.LIMIT,
            quantity=5.0,
            limit_price=2490.5,
            client_order_id="limit_buy_2490",
            correlation_id="corr_limit_01",
        )
        out = coordinator.submit_paper_order(
            limit_req,
            market_quote=deterministic_quotes[0],
            reference_time=deterministic_quotes[0].timestamp,
        )
        assert out.status == WorkflowStatus.ACCEPTED
        assert out.is_pending is True

        # Quote 1 (2505): limit price not met
        pending_fills_q1 = paper_engine.on_market_quote(deterministic_quotes[1])
        assert len(pending_fills_q1) == 0

        # Quote 2 (2510): limit price not met
        pending_fills_q2 = paper_engine.on_market_quote(deterministic_quotes[2])
        assert len(pending_fills_q2) == 0

        # Quote 3 (2490.0, ask=2490.5): limit price met!
        pending_fills_q3 = paper_engine.on_market_quote(deterministic_quotes[3])
        assert len(pending_fills_q3) == 1
        assert pending_fills_q3[0].success is True
        assert pending_fills_q3[0].fill is not None
        assert pending_fills_q3[0].fill.price <= 2490.5

        # Verify order transitioned to FILLED
        order = paper_engine.get_order_by_client_id("limit_buy_2490")
        assert order is not None
        assert order.status == PaperOrderStatus.FILLED


# ==============================================================================
# 2. REPLAY INTEGRITY & INPUT VALIDATION
# ==============================================================================


class TestReplayIntegrityAndInputValidation:
    """Tests replay handling of out-of-order, malformed, non-finite, and duplicate records."""

    def test_strict_chronological_replay_detects_out_of_order_records(self) -> None:
        """ReplayMarketFeed with strict_chronological=True raises ValidationError on timestamp regression."""
        out_of_order_records = [
            {"symbol": "RELIANCE", "timestamp": 100.0, "last_price": 2500.0},
            {"symbol": "RELIANCE", "timestamp": 150.0, "last_price": 2510.0},
            {"symbol": "RELIANCE", "timestamp": 120.0, "last_price": 2505.0},  # Out of order!
        ]
        feed = ReplayMarketFeed(records=out_of_order_records, strict_chronological=True)

        rec1 = feed.next_record()
        assert rec1 is not None and rec1.timestamp == 100.0

        rec2 = feed.next_record()
        assert rec2 is not None and rec2.timestamp == 150.0

        with pytest.raises(ValidationError, match="Out-of-order replay record detected"):
            feed.next_record()

        stats = feed.get_replay_stats()
        assert stats["records_rejected"] == 1

    def test_stale_market_data_rejection(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Market quote older than freshness threshold is rejected fail-closed."""
        stale_quote = deterministic_quotes[0]
        # Current time is 500 seconds after the quote timestamp (threshold is 120s)
        now = stale_quote.timestamp + 500.0

        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            client_order_id="stale_test_order",
        )
        outcome = coordinator.submit_paper_order(req, market_quote=stale_quote, reference_time=now)

        assert outcome.success is False
        assert outcome.status == WorkflowStatus.REJECTED
        assert outcome.rejection_code == PaperRejectionCode.STALE_MARKET_DATA.value

    def test_non_finite_quote_values_rejected_at_boundary(self) -> None:
        """NaN or inf quote prices are rejected by Quote domain validation."""
        with pytest.raises(ValidationError, match="Quote last_price cannot be negative or non-finite"):
            Quote(
                symbol="RELIANCE",
                exchange="NSE",
                timestamp=1700000000.0,
                last_price=float("nan"),
            )

        with pytest.raises(ValidationError, match="Quote last_price cannot be negative or non-finite"):
            Quote(
                symbol="RELIANCE",
                exchange="NSE",
                timestamp=1700000000.0,
                last_price=float("inf"),
            )

    def test_inverted_spread_quote_rejected(self, coordinator: PaperTradingWorkflowCoordinator) -> None:
        """Inverted bid-ask spread (ask < bid) raises ValidationError upon instantiation."""
        with pytest.raises(ValidationError, match="Inverted spread invariant violated"):
            Quote(
                symbol="RELIANCE",
                exchange="NSE",
                timestamp=1700000000.0,
                last_price=2500.0,
                bid=2510.0,
                ask=2500.0,  # ask < bid
            )

    def test_non_finite_order_and_reference_time_rejected(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Orders or reference clocks with non-finite values are rejected with ValidationError."""
        # Non-finite quantity
        with pytest.raises(ValidationError, match="Quantity must be a positive finite number"):
            PaperWorkflowRequest(
                symbol="RELIANCE",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=float("nan"),
            )

        # Non-finite reference time
        valid_req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
        )
        with pytest.raises(ValidationError, match="reference_time must be a positive finite number"):
            coordinator.submit_paper_order(
                valid_req,
                market_quote=deterministic_quotes[0],
                reference_time=float("nan"),
            )

    def test_duplicate_order_idempotency_prevents_duplicate_financial_effects(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        paper_engine: PaperExecutionEngine,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Resubmitting identical client_order_id returns duplicate rejection and does not re-deduct cash."""
        q = deterministic_quotes[0]
        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            client_order_id="idemp_order_123",
        )
        out1 = coordinator.submit_paper_order(req, market_quote=q, reference_time=q.timestamp)
        assert out1.success is True
        cash_after_first = paper_engine.portfolio.available_cash

        # Resubmit with same client_order_id
        out2 = coordinator.submit_paper_order(req, market_quote=q, reference_time=q.timestamp)
        assert out2.success is False
        assert out2.rejection_code == PaperRejectionCode.DUPLICATE_ORDER.value
        # Financial state must be completely untouched
        assert paper_engine.portfolio.available_cash == cash_after_first


# ==============================================================================
# 3. ACCOUNTING & PERSISTENCE INVARIANTS
# ==============================================================================


class TestAccountingAndPersistenceInvariants:
    """Verifies cash, positions, fees, slippage, and P&L accounting invariants."""

    def test_rejected_order_causes_zero_financial_mutation(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        paper_engine: PaperExecutionEngine,
        deterministic_quotes: List[Quote],
    ) -> None:
        """A rejected order (e.g. exceeding max notional) causes zero changes to cash or positions."""
        init_cash = paper_engine.portfolio.available_cash
        init_pos_count = len(paper_engine.portfolio.positions)
        init_fees = paper_engine.portfolio.total_fees_paid

        # Order quantity 1000 * 2500 = 2,500,000 (exceeds max notional 50,000)
        huge_req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=1000.0,
            client_order_id="huge_reject_01",
        )
        outcome = coordinator.submit_paper_order(
            huge_req,
            market_quote=deterministic_quotes[0],
            reference_time=deterministic_quotes[0].timestamp,
        )
        assert outcome.success is False
        assert outcome.rejection_code == PaperRejectionCode.EXCEEDS_MAX_NOTIONAL.value

        assert paper_engine.portfolio.available_cash == init_cash
        assert len(paper_engine.portfolio.positions) == init_pos_count
        assert paper_engine.portfolio.total_fees_paid == init_fees

    def test_long_only_prevents_short_selling(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Selling an unheld symbol or selling more than held quantity is rejected."""
        q = deterministic_quotes[0]
        # Sell unheld symbol
        sell_req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.SELL,
            order_type=PaperOrderType.MARKET,
            quantity=10.0,
            client_order_id="short_reject_01",
        )
        outcome = coordinator.submit_paper_order(sell_req, market_quote=q, reference_time=q.timestamp)
        assert outcome.success is False
        assert outcome.rejection_code in (
            PaperRejectionCode.INSUFFICIENT_POSITION.value,
            PaperRejectionCode.SHORT_SELLING_PROHIBITED.value,
        )

    def test_weighted_average_cost_and_realized_pnl_mathematics(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        paper_engine: PaperExecutionEngine,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Verify weighted average cost basis across multiple buys and exact realized P&L on sell."""
        # Buy 1: 6 @ q0 (ask 2500.5, fee_rate 0.0005, slippage 0.0002)
        q0 = deterministic_quotes[0]
        req1 = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=6.0,
            client_order_id="buy_batch_1",
        )
        out1 = coordinator.submit_paper_order(req1, market_quote=q0, reference_time=q0.timestamp)
        assert out1.success is True

        pos = paper_engine.portfolio.get_position("RELIANCE")
        assert pos is not None
        assert pos.quantity == 6.0
        p1 = pos.avg_entry_price

        # Buy 2: 6 @ q1 (ask 2505.5)
        q1 = deterministic_quotes[1]
        req2 = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=6.0,
            client_order_id="buy_batch_2",
        )
        out2 = coordinator.submit_paper_order(req2, market_quote=q1, reference_time=q1.timestamp)
        assert out2.success is True

        pos = paper_engine.portfolio.get_position("RELIANCE")
        assert pos is not None
        assert pos.quantity == 12.0
        # Expected avg price = (6 * p1 + 6 * p2) / 12
        p2 = out2.fill.price  # type: ignore[union-attr]
        expected_avg = round((6.0 * p1 + 6.0 * p2) / 12.0, 4)
        assert pos.avg_entry_price == expected_avg

        # Sell 6 @ q4 (bid 2519.5)
        q4 = deterministic_quotes[4]
        req3 = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.SELL,
            order_type=PaperOrderType.MARKET,
            quantity=6.0,
            client_order_id="sell_batch_1",
        )
        out3 = coordinator.submit_paper_order(req3, market_quote=q4, reference_time=q4.timestamp)
        assert out3.success is True

        fill3 = out3.fill
        assert fill3 is not None
        expected_trade_realized = round(((fill3.price - expected_avg) * 6.0) - fill3.fee, 4)
        assert paper_engine.portfolio.realized_pnl == expected_trade_realized

        # Ledger reconciliation must balance
        recon = paper_engine.ledger.reconcile(expected_current_cash=paper_engine.portfolio.available_cash)
        assert recon["is_balanced"] is True
        assert recon["discrepancy"] == 0.0

    def test_full_close_removes_position_from_portfolio(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        paper_engine: PaperExecutionEngine,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Selling 100% of held shares removes the symbol from active positions."""
        q0 = deterministic_quotes[0]
        coordinator.submit_paper_order(
            PaperWorkflowRequest(
                symbol="RELIANCE",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="buy_5",
            ),
            market_quote=q0,
            reference_time=q0.timestamp,
        )
        assert "RELIANCE" in paper_engine.portfolio.positions

        # Sell all 5
        q1 = deterministic_quotes[1]
        coordinator.submit_paper_order(
            PaperWorkflowRequest(
                symbol="RELIANCE",
                side=PaperOrderSide.SELL,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="sell_5",
            ),
            market_quote=q1,
            reference_time=q1.timestamp,
        )
        assert "RELIANCE" not in paper_engine.portfolio.positions
        assert paper_engine.portfolio.get_position("RELIANCE") is None


# ==============================================================================
# 4. FAILURE INJECTION & RECOVERY TESTING
# ==============================================================================


class TestFailureInjectionAndRecoveryTesting:
    """Verifies crash consistency, interruption, corrupted persistence, and error surfacing."""

    def test_replay_feed_interruption_and_resumption(self, deterministic_quotes: List[Quote]) -> None:
        """Interrupted replay feed can resume from exact playback cursor without re-emitting records."""
        feed = ReplayMarketFeed(name="interruptible_feed", records=deterministic_quotes)

        # Emit first 2 records
        r1 = feed.next_record()
        r2 = feed.next_record()
        assert r1 is not None and r2 is not None
        assert feed.current_index == 2
        assert feed.get_replay_stats()["records_remaining"] == 3

        # Simulate interruption / stop
        feed.stop()
        assert feed.next_record() is None

        # Resume playback
        feed.start()
        r3 = feed.next_record()
        assert r3 is not None
        assert r3.timestamp == deterministic_quotes[2].timestamp
        assert feed.current_index == 3

    def test_sqlite_restart_recovery_preserves_financial_state(
        self,
        tmp_path: Path,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Execute orders, simulate process death, restart from SQLite, and verify state equivalence."""
        db_file = str(tmp_path / "crash_test.db")
        bus1 = EventBus(name="bus_crash_1")
        store1 = StateStore(name="store_crash_1")
        ledger1 = PaperLedger(event_bus=bus1, db_path=db_file, initial_balance=100_000.0)
        risk1 = PaperRiskConfig(max_order_notional=50_000.0)
        engine1 = PaperExecutionEngine(
            event_bus=bus1,
            state_store=store1,
            ledger=ledger1,
            initial_cash=100_000.0,
            risk_config=risk1,
            enabled=True,
        )
        coord1 = PaperTradingWorkflowCoordinator(engine=engine1, event_bus=bus1, state_store=store1, enabled=True)

        # Ensure order timestamps are strictly after initial capital timestamp
        init_ts = ledger1.get_entries()[0].timestamp
        ts0 = init_ts + 1.0
        ts1 = init_ts + 2.0

        q0 = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=ts0,
            last_price=2500.0,
            bid=2499.5,
            ask=2500.5,
            volume=10000.0,
            source="replay",
        )
        q1 = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=ts1,
            last_price=2505.0,
            bid=2504.5,
            ask=2505.5,
            volume=15000.0,
            source="replay",
        )

        # Order 1: Buy 5
        coord1.submit_paper_order(
            PaperWorkflowRequest(
                symbol="RELIANCE",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="crash_buy_1",
            ),
            market_quote=q0,
            reference_time=q0.timestamp,
        )

        # Order 2: Sell 2
        coord1.submit_paper_order(
            PaperWorkflowRequest(
                symbol="RELIANCE",
                side=PaperOrderSide.SELL,
                order_type=PaperOrderType.MARKET,
                quantity=2.0,
                client_order_id="crash_sell_1",
            ),
            market_quote=q1,
            reference_time=q1.timestamp,
        )

        pre_crash_cash = engine1.portfolio.available_cash
        pre_crash_equity = engine1.portfolio.total_equity
        pre_crash_realized = engine1.portfolio.realized_pnl
        pre_crash_pos_qty = engine1.portfolio.get_position("RELIANCE").quantity  # type: ignore[union-attr]
        pre_crash_digest = compute_financial_digest(engine1.portfolio, engine1.ledger)

        # Simulate process death: recreate engine connected to same SQLite db
        bus2 = EventBus(name="bus_crash_2")
        store2 = StateStore(name="store_crash_2")
        ledger2 = PaperLedger(event_bus=bus2, db_path=db_file, initial_balance=100_000.0)
        engine2 = PaperExecutionEngine(
            event_bus=bus2,
            state_store=store2,
            ledger=ledger2,
            initial_cash=100_000.0,
            risk_config=risk1,
            enabled=True,
        )

        # State must be recovered from ledger automatically upon init
        assert engine2.portfolio.available_cash == pre_crash_cash
        assert engine2.portfolio.realized_pnl == pre_crash_realized
        pos2 = engine2.portfolio.get_position("RELIANCE")
        assert pos2 is not None
        assert pos2.quantity == pre_crash_pos_qty

        post_crash_digest = compute_financial_digest(engine2.portfolio, engine2.ledger)
        assert post_crash_digest == pre_crash_digest

    def test_corrupted_sqlite_ledger_entry_fails_closed_on_recovery(
        self,
        tmp_path: Path,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Non-finite or corrupted values in SQLite raise ValidationError during restart recovery."""
        db_file = str(tmp_path / "corrupted_ledger.db")
        bus = EventBus(name="bus_corrupt")
        ledger = PaperLedger(event_bus=bus, db_path=db_file, initial_balance=100_000.0)

        # Directly inject a corrupted entry with invalid negative quantity into SQLite
        with sqlite3.connect(db_file) as conn:
            conn.execute(
                """
                INSERT INTO paper_ledger_entries (
                    entry_id, timestamp, entry_type, order_id, client_order_id,
                    symbol, side, quantity, price, fee, cash_change,
                    cash_balance_after, realized_pnl, idempotency_key, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "corrupt_entry_01",
                    1700000000.0,
                    LedgerEntryType.ORDER_FILL.value,
                    "ord_bad",
                    "client_bad",
                    "RELIANCE",
                    "BUY",
                    -5.0,  # Corrupted negative quantity!
                    2500.0,
                    12.5,
                    -12512.5,
                    87487.5,
                    0.0,
                    "bad_key_01",
                    "{}",
                ),
            )

        # Attempt recovery with new engine
        with pytest.raises(ValidationError, match="Corrupted.*entry"):
            PaperExecutionEngine(
                ledger=PaperLedger(event_bus=bus, db_path=db_file, initial_balance=100_000.0),
                initial_cash=100_000.0,
                enabled=True,
            )

    def test_event_publication_failure_surfaced_without_false_success(
        self,
        paper_engine: PaperExecutionEngine,
        deterministic_quotes: List[Quote],
    ) -> None:
        """When event publication throws an exception, workflow coordinator returns FAILED status."""

        class BrokenEventBus(EventBus):
            def publish(self, event: Event) -> int:
                raise RuntimeError("Simulated network/event-bus disruption")

        broken_bus = BrokenEventBus(name="broken_bus")
        coord = PaperTradingWorkflowCoordinator(
            engine=paper_engine,
            event_bus=broken_bus,
            enabled=True,
        )

        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            client_order_id="pub_fail_order",
        )
        q = deterministic_quotes[0]
        outcome = coord.submit_paper_order(req, market_quote=q, reference_time=q.timestamp)

        assert outcome.success is False
        assert outcome.status == WorkflowStatus.FAILED
        assert "Simulated network/event-bus disruption" in str(outcome.error_message)

    def test_reconciliation_failure_detected_and_surfaced(
        self,
        paper_engine: PaperExecutionEngine,
        coordinator: PaperTradingWorkflowCoordinator,
        deterministic_quotes: List[Quote],
    ) -> None:
        """If portfolio cash diverges from ledger records, RECONCILIATION_FAILED is surfaced."""
        q = deterministic_quotes[0]
        # Force a cash discrepancy on portfolio
        paper_engine.portfolio.available_cash += 999.0

        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            client_order_id="recon_fail_ord",
        )
        outcome = coordinator.submit_paper_order(req, market_quote=q, reference_time=q.timestamp)

        assert outcome.success is False
        assert outcome.status == WorkflowStatus.RECONCILIATION_FAILED
        assert outcome.reconciliation_result is not None
        assert outcome.reconciliation_result["is_balanced"] is False


# ==============================================================================
# 5. OBSERVABILITY, AUDITABILITY & SAFETY INVARIANTS
# ==============================================================================


class TestObservabilityAndSafetyInvariants:
    """Verifies audit trails, session digests, dashboard read-only summaries, and safety locks."""

    def test_workflow_audit_trail_contains_all_stages(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Verified execution records all lifecycle stages in the immutable audit trail."""
        q = deterministic_quotes[0]
        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            client_order_id="audit_trail_ord",
        )
        outcome = coordinator.submit_paper_order(req, market_quote=q, reference_time=q.timestamp)

        assert outcome.success is True
        assert len(outcome.audit_trail) >= 4
        assert any("REQUEST_RECEIVED" in step for step in outcome.audit_trail)
        assert any("MARKET_DATA_RESOLUTION" in step for step in outcome.audit_trail)
        assert any("FILLED" in step for step in outcome.audit_trail)
        assert any("STATE_RECONCILIATION" in step for step in outcome.audit_trail)

    def test_session_summary_includes_financial_digest(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        paper_engine: PaperExecutionEngine,
        deterministic_quotes: List[Quote],
    ) -> None:
        """PaperSimulationSession.summary includes deterministic financial_digest."""
        session = coordinator.start_session(name="audit_session_01")
        assert session.status == "ACTIVE"

        q = deterministic_quotes[0]
        coordinator.submit_paper_order(
            PaperWorkflowRequest(
                symbol="RELIANCE",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="sess_ord_01",
                session_id=session.session_id,
            ),
            market_quote=q,
            reference_time=q.timestamp,
        )

        summary = session.summary(paper_engine.portfolio, paper_engine.ledger)
        assert "financial_digest" in summary
        assert len(summary["financial_digest"]) == 64
        assert summary["reconciliation"]["is_balanced"] is True
        assert summary["label"] == "PAPER ONLY"

    def test_dashboard_summary_includes_financial_digest_and_safety_locks(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
    ) -> None:
        """Coordinator get_summary provides read-only stats and verifies safety locks."""
        summary = coordinator.get_summary()

        assert summary["status"] == "success"
        assert summary["label"] == "PAPER ONLY"
        assert summary["execution_locked"] is True
        assert summary["broker_provider"] == "none"
        assert summary["trading_engine"] == "not initialized"
        assert "financial_digest" in summary
        assert summary["financial_digest"] is not None

    def test_safety_invariants_remain_locked(self) -> None:
        """Verify global system invariants: execution locked, broker none, paper disabled by default."""
        settings = Settings()
        assert settings.execution_locked is True
        assert settings.broker_provider == "none"
        assert settings.paper_execution_enabled is False

    def test_voice_commands_prohibited_from_trading_execution(self) -> None:
        """Voice command resolver intercepts trading directives at the boundary."""
        resolver = VoiceCommandIntentResolver()
        trade_phrases = [
            "buy 10 shares of RELIANCE",
            "sell 50 TCS at market",
            "execute order to purchase INFY",
            "place order for NIFTY",
        ]
        for phrase in trade_phrases:
            resolution = resolver.resolve(phrase)
            assert resolution.is_prohibited_trading is True
            assert resolution.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND
            assert resolution.rejection_reason is not None

    def test_replay_feed_file_loading_and_reprocessing_idempotence(self, tmp_path: Path) -> None:
        """Loading records from JSON file and resetting buffer yields identical playback."""
        data = [
            {"symbol": "INFY", "timestamp": 1000.0, "last_price": 1500.0, "volume": 100.0},
            {"symbol": "INFY", "timestamp": 1010.0, "last_price": 1505.0, "volume": 200.0},
        ]
        json_file = tmp_path / "replay_data.json"
        json_file.write_text(json.dumps(data), encoding="utf-8")

        feed = ReplayMarketFeed(name="file_feed")
        count = feed.load_from_json_file(json_file)
        assert count == 2

        digest1 = feed.compute_input_digest()
        records_pass1 = feed.play_all()
        assert len(records_pass1) == 2
        assert feed.has_next() is False

        # Reset and reprocess already-consumed input
        feed.reset()
        assert feed.current_index == 0
        assert feed.has_next() is True
        digest2 = feed.compute_input_digest()
        assert digest1 == digest2

        records_pass2 = feed.play_all()
        assert len(records_pass2) == 2
        assert records_pass1[0].timestamp == records_pass2[0].timestamp
        assert records_pass1[1].timestamp == records_pass2[1].timestamp

    def test_replay_feed_exhaustion_and_invalid_file(self, tmp_path: Path) -> None:
        """Step returns None upon buffer exhaustion; invalid file raises appropriate errors."""
        feed = ReplayMarketFeed(name="empty_feed", records=[])
        assert feed.step() is None
        assert feed.has_next() is False

        # Non-existent file raises FileNotFoundError
        with pytest.raises(FileNotFoundError):
            feed.load_from_json_file(tmp_path / "non_existent.json")

        # Non-list JSON raises ValueError
        invalid_json = tmp_path / "dict_json.json"
        invalid_json.write_text(json.dumps({"not": "a list"}), encoding="utf-8")
        with pytest.raises(ValueError, match="Expected JSON array of records"):
            feed.load_from_json_file(invalid_json)
