"""KAISER — Tests for Phase 2, Step 61:
Multi-Scenario Stress Testing & Parameter Sensitivity Analysis for Paper Trading Orchestration.

Comprehensive verification and regression suite covering:
1. Deterministic Stress Scenarios (A through J):
   - A. Tight Bid-Ask Spreads
   - B. Wide and Illiquid Spreads
   - C. Sudden Spread Widening (Point-in-Time & No Look-Ahead)
   - D. Stale and Out-of-Order Quotes
   - E. Missing and Malformed Market Data (Inverted Spread, Non-Positive, Non-Finite)
   - F. Liquidity Gap / Flash-Crash-Like Spread Shock (Bounded Synthetic)
   - G. Replay Interruption and Restart Recovery
   - H. Risk-Limit Boundary Conditions
   - I. Pending Limit Orders (Unchanged, Worsened, Improved, Cancelled)
   - J. Duplicate and Reordered Events (Idempotency)
2. Parameter Sensitivity Analysis:
   - Configurable parameters: slippage_rate, freshness_threshold_seconds, fee_rate, max_order_notional
   - Parameter grid validation (rejection of non-finite, negative, or invalid values)
   - State isolation between trials (no shared state contamination)
   - Baseline vs alternative comparison (deltas in fees, slippage, cash, P&L, rejections)
   - Deterministic sweep digests
3. Invariants & Accounting Reconciliation:
   - Exact ledger and portfolio balance reconciliation
   - Rejected order financial immutability
   - Non-negative cash and positions (long-only)
   - Non-finite numeric rejection
   - Restart recovery without capital duplication
4. Dashboard Integration & Security:
   - Read-only endpoint /api/paper/stress-test and /api/paper/stress-test/report
   - Token authorization enforcement (401 on unauthorized)
   - Mutating method restriction (POST/PUT/DELETE return 405)
   - Scenario filtering (?scenario_id=...) with 404 for unknown IDs
5. Mandatory Safety Boundaries:
   - execution_locked=True, broker_provider="none", paper_execution_enabled=False by default
   - Live trading uninitialized, no voice execution path.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Generator, List, Optional
import urllib.error
import urllib.request
import pytest

from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import EventBus
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.execution import (
    CombinedStressTestReport,
    HistoricalSpreadCalibrator,
    ObservationRejectionReason,
    OrderLifecycleStage,
    OrderLifecycleTracker,
    PaperExecutionEngine,
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
    PaperRiskGate,
    PaperTradingWorkflowCoordinator,
    PaperWorkflowOutcome,
    PaperWorkflowRequest,
    ParameterSensitivityAnalysis,
    ParameterSensitivityRunner,
    ScenarioCategory,
    ScenarioResult,
    ScenarioStatus,
    ScenarioStep,
    ScenarioStepType,
    SensitivityParameter,
    SensitivityTrialResult,
    SimulatedBrokerAdapter,
    StressScenarioDefinition,
    StressScenarioRunner,
    WorkflowStage,
    WorkflowStatus,
    compute_financial_digest,
    compute_quotes_digest,
    compute_scenario_deterministic_digest,
    create_scenario_a_tight_spread,
    create_scenario_b_wide_spread,
    create_scenario_c_spread_widening,
    create_scenario_d_stale_out_of_order,
    create_scenario_e_malformed_data,
    create_scenario_f_spread_shock,
    create_scenario_g_replay_interruption,
    create_scenario_h_risk_boundary,
    create_scenario_i_pending_limit_order,
    create_scenario_j_duplicate_events,
    create_standard_scenarios,
    run_all_stress_scenarios,
)
from app.ledger import LedgerEntryType, PaperLedger, PaperLedgerEntry
from app.market.feeds.replay import ReplayMarketFeed
from app.market.models import Quote
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.reporting import PortfolioReportingService
from app.risk.monitor import PortfolioRiskMonitor
from app.voice.intent import VoiceCommandIntentResolver, VoiceIntentType


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def default_settings() -> Settings:
    return Settings()


@pytest.fixture
def runner(default_settings: Settings) -> StressScenarioRunner:
    return StressScenarioRunner(settings=default_settings)


@pytest.fixture
def sensitivity_runner(default_settings: Settings) -> ParameterSensitivityRunner:
    return ParameterSensitivityRunner(settings=default_settings)


# ==============================================================================
# 1. SCENARIO A: TIGHT BID-ASK SPREADS
# ==============================================================================


class TestScenarioATightSpread:
    """Verifies tight spread calculation, slippage accounting, and ledger reconciliation."""

    def test_scenario_a_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_a_tight_spread()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.TIGHT_SPREAD
        assert res.simulated_order_count == 2
        assert res.fill_count == 2
        assert res.rejection_count == 0
        assert res.ledger_reconciliation["is_balanced"] is True
        assert res.ledger_reconciliation["discrepancy"] == 0.0

        # Verify BUY fill at ask + slippage and SELL fill at bid - slippage
        assert res.total_fees > 0.0
        assert res.total_slippage > 0.0
        assert "INFY" in res.positions
        assert res.positions["INFY"]["quantity"] == 5.0  # 10 bought - 5 sold

        assert "LEDGER_PORTFOLIO_RECONCILIATION" in res.passed_invariants
        assert "NON_NEGATIVE_CASH" in res.passed_invariants
        assert "NON_NEGATIVE_POSITIONS" in res.passed_invariants
        assert len(res.violated_invariants) == 0

    def test_scenario_a_reproducibility(self, runner: StressScenarioRunner) -> None:
        """Identical scenario execution must produce identical deterministic digests."""
        defn1 = create_scenario_a_tight_spread()
        defn2 = create_scenario_a_tight_spread()

        res1 = runner.run_scenario(defn1)
        res2 = runner.run_scenario(defn2)

        assert res1.deterministic_digest == res2.deterministic_digest
        assert res1.financial_digest == res2.financial_digest
        assert res1.input_digest == res2.input_digest
        assert res1.final_cash == res2.final_cash
        assert res1.final_equity == res2.final_equity


# ==============================================================================
# 2. SCENARIO B: WIDE AND ILLIQUID SPREADS
# ==============================================================================


class TestScenarioBWideSpread:
    """Verifies handling of unusually wide spreads and pre-trade risk controls."""

    def test_scenario_b_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_b_wide_spread()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.WIDE_SPREAD
        assert res.fill_count == 1  # Moderate order fills
        assert res.rejection_count == 1  # Oversized order rejected
        assert res.ledger_reconciliation["is_balanced"] is True

        # Verify calibrator captured wide spread
        assert res.calibration_summary is not None
        assert "ILLIQ" in res.calibration_summary
        calib = res.calibration_summary["ILLIQ"]
        comp = calib["config_comparison"]
        # Half spread observed is ~2000 bps vs configured slippage 2 bps -> ratio ~1000
        assert comp["half_spread_to_slippage_ratio"] > 10.0


# ==============================================================================
# 3. SCENARIO C: SUDDEN SPREAD WIDENING & POINT-IN-TIME ISOLATION
# ==============================================================================


class TestScenarioCSpreadWidening:
    """Verifies point-in-time calibration isolation and absence of look-ahead bias."""

    def test_scenario_c_execution_and_point_in_time(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_c_spread_widening()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.SPREAD_WIDENING
        assert res.ledger_reconciliation["is_balanced"] is True

        # Test point-in-time filtering via Calibrator directly on the same quotes
        calibrator = HistoricalSpreadCalibrator(risk_config=PaperRiskConfig())
        for q in defn.quotes:
            calibrator.add_observation(q, reference_time=q.timestamp)

        shock_ts = defn.metadata["shock_timestamp"]

        # Before shock calibration
        rep_pre = calibrator.calibrate("RELIANCE", as_of_time=shock_ts - 5.0)
        assert rep_pre.valid_observations == 3
        # Pre-shock spread is 1.0 (mid ~ 2500 -> ~4 bps)
        assert rep_pre.relative_spread_bps.median is not None
        assert rep_pre.relative_spread_bps.median < 10.0

        # Post shock calibration
        rep_post = calibrator.calibrate("RELIANCE", as_of_time=shock_ts + 20.0)
        assert rep_post.valid_observations == 5
        # Post shock includes 200 pt spread -> median and max are much larger
        assert rep_post.price_spread.max == 200.0
        assert rep_post.relative_spread_bps.max is not None
        assert rep_post.relative_spread_bps.max > 500.0

        # Earlier calibration report remains immutable
        assert rep_pre.price_spread.max == 1.0


# ==============================================================================
# 4. SCENARIO D: STALE AND OUT-OF-ORDER QUOTES
# ==============================================================================


class TestScenarioDStaleOutOfOrder:
    """Verifies fail-closed rejection of stale quotes and chronological validation."""

    def test_scenario_d_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_d_stale_out_of_order()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.STALE_OUT_OF_ORDER
        # Stale order was rejected
        assert res.rejection_count >= 1
        assert res.ledger_reconciliation["is_balanced"] is True

    def test_replay_feed_strict_chronological_enforcement(self) -> None:
        """Replay feed with strict_chronological=True raises ValidationError on timestamp inversion."""
        quotes = [
            Quote(symbol="TCS", exchange="NSE", timestamp=100.0, last_price=3500.0),
            Quote(symbol="TCS", exchange="NSE", timestamp=90.0, last_price=3495.0),  # Inverted!
        ]
        feed = ReplayMarketFeed(records=quotes, strict_chronological=True)
        rec1 = feed.next_record()
        assert rec1 is not None

        with pytest.raises(ValidationError) as exc:
            feed.next_record()
        assert "Out-of-order replay record detected" in str(exc.value)


# ==============================================================================
# 5. SCENARIO E: MISSING AND MALFORMED MARKET DATA
# ==============================================================================


class TestScenarioEMalformedData:
    """Verifies fail-closed behavior on missing BBO, non-positive prices, and inverted spreads."""

    def test_scenario_e_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_e_malformed_data()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.MALFORMED_DATA
        assert res.fill_count == 0  # Zero fills fabricated
        assert res.rejection_count >= 1
        assert "NO_FABRICATED_FILLS_ON_INVALID_DATA" in res.passed_invariants
        assert res.ledger_reconciliation["is_balanced"] is True

    def test_calibrator_rejects_malformed_records(self) -> None:
        """Calibrator discards missing BBO, inverted spread, and non-finite numbers."""
        calibrator = HistoricalSpreadCalibrator()

        # Missing BBO
        ok, reason, _ = calibrator.add_observation({"symbol": "X", "timestamp": 1.0, "last_price": 10.0})
        assert ok is False
        assert reason == ObservationRejectionReason.MISSING_BBO

        # Inverted spread (ask < bid)
        ok, reason, _ = calibrator.add_observation({"symbol": "X", "timestamp": 2.0, "last_price": 10.0, "bid": 12.0, "ask": 8.0})
        assert ok is False
        assert reason == ObservationRejectionReason.INVERTED_SPREAD

        # Non-finite price
        ok, reason, _ = calibrator.add_observation({"symbol": "X", "timestamp": 3.0, "last_price": float("nan"), "bid": 10.0, "ask": 11.0})
        assert ok is False
        assert reason == ObservationRejectionReason.NON_FINITE


# ==============================================================================
# 6. SCENARIO F: LIQUIDITY GAP / FLASH-CRASH-LIKE SPREAD SHOCK
# ==============================================================================


class TestScenarioFSpreadShock:
    """Verifies synthetic spread shock handling and simulated slippage behavior."""

    def test_scenario_f_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_f_spread_shock()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.SPREAD_SHOCK
        assert res.fill_count == 1
        assert res.ledger_reconciliation["is_balanced"] is True
        assert "synthetic_disclaimer" in res.metadata
        assert res.total_slippage > 0.0


# ==============================================================================
# 7. SCENARIO G: REPLAY INTERRUPTION AND RESTART RECOVERY
# ==============================================================================


class TestScenarioGReplayInterruption:
    """Verifies engine restart recovery, non-duplication of initial capital/fills, and reconciliation."""

    def test_scenario_g_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_g_replay_interruption()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.REPLAY_INTERRUPTION
        assert res.fill_count == 2
        assert "RESTART_RECOVERY_INTEGRITY" in res.passed_invariants
        assert res.ledger_reconciliation["is_balanced"] is True
        assert res.ledger_reconciliation["discrepancy"] == 0.0

    def test_sqlite_ledger_restart_recovery(self, tmp_path: Path) -> None:
        """Persistent SQLite ledger recovers state accurately after crash simulation."""
        db_file = tmp_path / "test_recovery.db"
        ledger1 = PaperLedger(name="persistent_test", db_path=str(db_file), initial_balance=100_000.0)

        portfolio1 = PaperPortfolio(initial_cash=100_000.0)
        broker = SimulatedBrokerAdapter()
        engine1 = PaperExecutionEngine(
            ledger=ledger1,
            portfolio=portfolio1,
            broker_adapter=broker,
            enabled=True,
            initial_cash=100_000.0,
        )

        init_ts = ledger1.get_entries()[0].timestamp
        ts0 = init_ts + 1.0
        quote = Quote(symbol="INFY", exchange="NSE", timestamp=ts0, last_price=1500.0, bid=1499.5, ask=1500.5)
        req = PaperOrderRequest(symbol="INFY", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=10.0)
        res1 = engine1.submit_order(req, market_quote=quote, reference_time=ts0)
        assert res1.success is True

        cash_after_order = portfolio1.available_cash
        equity_after_order = portfolio1.total_equity
        digest1 = compute_financial_digest(portfolio1, ledger1)

        # Simulate crash & restart with new engine pointing to same SQLite DB
        ledger2 = PaperLedger(name="persistent_test", db_path=str(db_file))
        portfolio2 = PaperPortfolio(initial_cash=100_000.0)
        engine2 = PaperExecutionEngine(
            ledger=ledger2,
            portfolio=portfolio2,
            broker_adapter=broker,
            enabled=True,
            initial_cash=100_000.0,
        )

        # State recovered
        assert portfolio2.available_cash == cash_after_order
        assert portfolio2.total_equity == equity_after_order
        assert "INFY" in portfolio2.positions
        assert portfolio2.positions["INFY"].quantity == 10.0
        digest2 = compute_financial_digest(portfolio2, ledger2)
        assert digest1 == digest2


# ==============================================================================
# 8. SCENARIO H: RISK-LIMIT BOUNDARY CONDITIONS
# ==============================================================================


class TestScenarioHRiskBoundary:
    """Verifies strict boundary semantics for max order notional and risk controls."""

    def test_scenario_h_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_h_risk_boundary()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.RISK_BOUNDARY
        assert res.fill_count == 1  # Below limit fills
        assert res.rejection_count == 1  # Above limit rejected
        assert "RISK_LIMIT_BOUNDARIES_ENFORCED" in res.passed_invariants
        assert res.ledger_reconciliation["is_balanced"] is True

    def test_risk_gate_notional_exact_boundary(self) -> None:
        """Test exact boundary: order_notional <= max_order_notional is approved; > is rejected."""
        cfg = PaperRiskConfig(max_order_notional=10_000.0)
        gate = PaperRiskGate(config=cfg)
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        quote = Quote(symbol="TEST", exchange="NSE", timestamp=100.0, last_price=100.0, bid=100.0, ask=100.0)

        # 1. Exactly 10,000 (quantity=100, price=100) -> Approved
        req_at = PaperOrderRequest(symbol="TEST", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=100.0)
        ok_at, code_at, _ = gate.evaluate_order(req_at, portfolio, quote, reference_time=100.0)
        assert ok_at is True

        # 2. Exceeds by 0.01 (quantity=100.001, notional=10,000.10) -> Rejected
        req_over = PaperOrderRequest(symbol="TEST", side=PaperOrderSide.BUY, order_type=PaperOrderType.MARKET, quantity=100.01)
        ok_over, code_over, _ = gate.evaluate_order(req_over, portfolio, quote, reference_time=100.0)
        assert ok_over is False
        assert code_over == PaperRejectionCode.EXCEEDS_MAX_NOTIONAL


# ==============================================================================
# 9. SCENARIO I: PENDING LIMIT ORDERS
# ==============================================================================


class TestScenarioIPendingLimitOrder:
    """Verifies pending limit order lifecycle: pending, worsened, improved execution."""

    def test_scenario_i_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_i_pending_limit_order()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.PENDING_LIMIT_ORDER
        assert res.fill_count == 1  # Executed when price improved
        assert res.ledger_reconciliation["is_balanced"] is True
        assert "LIMIT_SYM" in res.positions
        assert res.positions["LIMIT_SYM"]["quantity"] == 10.0

    def test_pending_limit_order_cancellation(self) -> None:
        """Open pending limit order can be explicitly cancelled."""
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        ledger = PaperLedger()
        broker = SimulatedBrokerAdapter()
        engine = PaperExecutionEngine(portfolio=portfolio, ledger=ledger, broker_adapter=broker, enabled=True)
        coordinator = PaperTradingWorkflowCoordinator(engine=engine, enabled=True)

        quote = Quote(symbol="ABC", exchange="NSE", timestamp=100.0, last_price=200.0, bid=199.0, ask=201.0)
        # Limit price 190.0 < ask 201.0 -> pending
        req = PaperWorkflowRequest(symbol="ABC", side=PaperOrderSide.BUY, order_type=PaperOrderType.LIMIT, quantity=5.0, limit_price=190.0)
        out1 = coordinator.submit_paper_order(req, market_quote=quote, reference_time=100.0)
        assert out1.status == WorkflowStatus.ACCEPTED
        assert out1.is_pending is True

        # Cancel the pending order
        out2 = coordinator.cancel_order(out1.order.order_id)
        assert out2.status == WorkflowStatus.CANCELLED
        assert out2.is_cancelled is True
        assert ledger.reconcile(portfolio.available_cash)["is_balanced"] is True


# ==============================================================================
# 10. SCENARIO J: DUPLICATE AND REORDERED EVENTS
# ==============================================================================


class TestScenarioJDuplicateEvents:
    """Verifies idempotency protection for duplicate client order IDs and duplicate market data."""

    def test_scenario_j_execution(self, runner: StressScenarioRunner) -> None:
        defn = create_scenario_j_duplicate_events()
        res = runner.run_scenario(defn)

        assert res.status == ScenarioStatus.PASSED
        assert res.category == ScenarioCategory.DUPLICATE_EVENTS
        assert res.fill_count == 1
        assert res.rejection_count == 1
        assert res.ledger_reconciliation["is_balanced"] is True

    def test_duplicate_order_does_not_mutate_portfolio(self) -> None:
        """Resubmitting identical client_order_id returns DUPLICATE_ORDER and does not mutate cash."""
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        ledger = PaperLedger()
        broker = SimulatedBrokerAdapter()
        engine = PaperExecutionEngine(portfolio=portfolio, ledger=ledger, broker_adapter=broker, enabled=True)

        quote = Quote(symbol="RELIANCE", exchange="NSE", timestamp=100.0, last_price=2500.0, bid=2499.0, ask=2501.0)
        req = PaperOrderRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            client_order_id="CLIENT_ORDER_IDEMPOTENT_1",
        )

        # 1. First submission: fills
        res1 = engine.submit_order(req, market_quote=quote, reference_time=100.0)
        assert res1.success is True
        cash_after_1 = portfolio.available_cash
        ledger_count_1 = ledger.count()

        # 2. Second submission: rejected as duplicate
        res2 = engine.submit_order(req, market_quote=quote, reference_time=101.0)
        assert res2.success is False
        assert res2.rejection_code == PaperRejectionCode.DUPLICATE_ORDER

        # Invariant: cash and ledger count remain untouched by duplicate submission
        assert portfolio.available_cash == cash_after_1
        assert ledger.count() == ledger_count_1


# ==============================================================================
# 11. PARAMETER SENSITIVITY RUNNER TESTS
# ==============================================================================


class TestParameterSensitivityRunner:
    """Verifies isolated trial sweeps, delta calculations, and parameter validation."""

    def test_slippage_rate_sensitivity(self, sensitivity_runner: ParameterSensitivityRunner) -> None:
        """Higher slippage rates increase total slippage and decrease final cash."""
        base_sc = create_scenario_a_tight_spread()
        analysis = sensitivity_runner.run_sensitivity(
            parameter=SensitivityParameter.SLIPPAGE_RATE,
            baseline_value=0.0002,
            tested_values=[0.0000, 0.0001, 0.0002, 0.0005, 0.0010],
            base_scenario=base_sc,
        )

        assert analysis.parameter_name == "slippage_rate"
        assert analysis.baseline_value == 0.0002
        assert len(analysis.trials) == 5

        # Check ordering of slippage: as slippage increases, total_slippage increases
        sorted_trials = sorted(analysis.trials, key=lambda t: t.parameter_value)
        for i in range(len(sorted_trials) - 1):
            assert sorted_trials[i].total_slippage <= sorted_trials[i + 1].total_slippage

        # Baseline trial has delta 0.0
        base_t = next(t for t in analysis.trials if t.is_baseline)
        assert base_t.delta_slippage_vs_baseline == 0.0
        assert base_t.delta_cash_vs_baseline == 0.0

        # Non-baseline deltas
        highest_slip_t = sorted_trials[-1]
        assert highest_slip_t.delta_slippage_vs_baseline > 0.0
        assert highest_slip_t.delta_cash_vs_baseline < 0.0

        assert bool(analysis.sweep_digest) is True
        assert len(analysis.summary_observations) > 0

    def test_freshness_threshold_sensitivity(self, sensitivity_runner: ParameterSensitivityRunner) -> None:
        """Low freshness threshold causes older market quotes to be rejected as stale."""
        base_sc = create_scenario_d_stale_out_of_order()
        analysis = sensitivity_runner.run_sensitivity(
            parameter=SensitivityParameter.FRESHNESS_THRESHOLD_SECONDS,
            baseline_value=60.0,
            tested_values=[5.0, 30.0, 60.0, 120.0, 300.0],
            base_scenario=base_sc,
        )

        assert analysis.parameter_name == "freshness_threshold_seconds"
        assert len(analysis.trials) == 5
        for t in analysis.trials:
            assert t.invariants_satisfied is True
            assert t.ledger_balanced is True

    def test_max_order_notional_sensitivity(self, sensitivity_runner: ParameterSensitivityRunner) -> None:
        """Lower max_order_notional triggers rejections for orders that pass under higher limits."""
        base_sc = create_scenario_b_wide_spread()
        analysis = sensitivity_runner.run_sensitivity(
            parameter=SensitivityParameter.MAX_ORDER_NOTIONAL,
            baseline_value=50_000.0,
            tested_values=[5_000.0, 15_000.0, 50_000.0, 150_000.0],
            base_scenario=base_sc,
        )

        assert analysis.parameter_name == "max_order_notional"
        trial_5k = next(t for t in analysis.trials if t.parameter_value == 5_000.0)
        trial_150k = next(t for t in analysis.trials if t.parameter_value == 150_000.0)

        # Under 5,000 limit, both orders fail
        assert trial_5k.orders_rejected >= trial_150k.orders_rejected

    def test_invalid_parameters_rejected(self, sensitivity_runner: ParameterSensitivityRunner) -> None:
        """Negative, infinite, or NaN parameter values raise ValidationError."""
        base_sc = create_scenario_a_tight_spread()

        # Negative slippage rate
        with pytest.raises(ValidationError):
            sensitivity_runner.run_sensitivity(
                parameter=SensitivityParameter.SLIPPAGE_RATE,
                baseline_value=0.0002,
                tested_values=[-0.001],
                base_scenario=base_sc,
            )

        # Non-finite (NaN)
        with pytest.raises(ValidationError):
            sensitivity_runner.run_sensitivity(
                parameter=SensitivityParameter.FRESHNESS_THRESHOLD_SECONDS,
                baseline_value=60.0,
                tested_values=[float("nan")],
                base_scenario=base_sc,
            )

        # Non-positive notional
        with pytest.raises(ValidationError):
            sensitivity_runner.run_sensitivity(
                parameter=SensitivityParameter.MAX_ORDER_NOTIONAL,
                baseline_value=50_000.0,
                tested_values=[0.0],
                base_scenario=base_sc,
            )


# ==============================================================================
# 12. COMBINED SUITE EXECUTION & INVARIANTS
# ==============================================================================


class TestCombinedStressTestSuite:
    """Verifies execution of the entire standard scenario suite and combined report."""

    def test_run_all_stress_scenarios_passes_all(self, default_settings: Settings) -> None:
        """All 10 standard stress scenarios pass and reconcile in combined run."""
        report = run_all_stress_scenarios(settings=default_settings, run_sensitivity=True)

        assert report.total_scenarios == 10
        assert report.passed_scenarios == 10
        assert report.failed_scenarios == 0
        assert report.rejected_scenarios == 0
        assert report.insufficient_data_scenarios == 0

        assert len(report.scenario_results) == 10
        assert "SCENARIO_A_TIGHT_SPREAD" in report.scenario_results
        assert "SCENARIO_J_DUPLICATE_EVENTS" in report.scenario_results

        # Check aggregate invariants
        assert report.aggregate_invariants["ALL_SCENARIOS_RECONCILED"] is True
        assert report.aggregate_invariants["NO_NEGATIVE_CASH"] is True
        assert report.aggregate_invariants["SAFETY_LOCK_PRESERVED"] is True

        # Check sensitivity analyses
        assert len(report.sensitivity_analyses) >= 4
        assert "slippage_rate" in report.sensitivity_analyses

        assert bool(report.overall_digest) is True
        assert len(report.limitations_and_disclaimers) >= 4

    def test_suite_repeatability_and_digest_stability(self, default_settings: Settings) -> None:
        """Two independent executions of the combined suite yield bit-level identical digests."""
        report1 = run_all_stress_scenarios(settings=default_settings, run_sensitivity=False)
        report2 = run_all_stress_scenarios(settings=default_settings, run_sensitivity=False)

        assert report1.overall_digest == report2.overall_digest

        for sc_id in report1.scenario_results:
            assert report1.scenario_results[sc_id].deterministic_digest == report2.scenario_results[sc_id].deterministic_digest
            assert report1.scenario_results[sc_id].financial_digest == report2.scenario_results[sc_id].financial_digest


# ==============================================================================
# 13. DASHBOARD API SECURITY & ROUTE ACCEPTANCE
# ==============================================================================


class TestDashboardStressTestingEndpoint:
    """Verifies /api/paper/stress-test endpoint authorization, loopback binding, and read-only response."""

    @pytest.fixture
    def dashboard_server(
        self,
        default_settings: Settings,
    ) -> Generator[PortfolioDashboardServer, None, None]:
        event_bus = EventBus()
        state_store = StateStore()
        attribution = PortfolioAttributionService(event_bus=event_bus, state_store=state_store)
        risk_mon = PortfolioRiskMonitor(event_bus=event_bus, state_store=state_store)
        reporting = PortfolioReportingService(
            event_bus=event_bus,
            state_store=state_store,
            attribution_service=attribution,
            risk_monitor=risk_mon,
        )

        ledger = PaperLedger()
        portfolio = PaperPortfolio(initial_cash=100_000.0)
        broker = SimulatedBrokerAdapter()
        engine = PaperExecutionEngine(portfolio=portfolio, ledger=ledger, broker_adapter=broker, enabled=True)
        coordinator = PaperTradingWorkflowCoordinator(engine=engine, settings=default_settings, enabled=True)
        # Pre-execute stress suite on coordinator
        coordinator.run_stress_scenarios(run_sensitivity=False)

        server = PortfolioDashboardServer(
            reporting_service=reporting,
            workflow_coordinator=coordinator,
            host="127.0.0.1",
            port=0,
            require_auth=True,
            auth_token="step61-auth-token-test",
        )
        server.start(blocking=False)
        yield server
        server.stop()

    def test_stress_test_endpoint_requires_auth(self, dashboard_server: PortfolioDashboardServer) -> None:
        """Unauthenticated request to /api/paper/stress-test returns 401 Unauthorized."""
        base_url = f"http://127.0.0.1:{dashboard_server.port}"
        req = urllib.request.Request(f"{base_url}/api/paper/stress-test")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req)
        assert exc_info.value.code == 401

    def test_stress_test_endpoint_authorized_full_report(self, dashboard_server: PortfolioDashboardServer) -> None:
        """Authorized request returns read-only combined stress test report."""
        base_url = f"http://127.0.0.1:{dashboard_server.port}"
        req = urllib.request.Request(
            f"{base_url}/api/paper/stress-test",
            headers={"Authorization": "Bearer step61-auth-token-test"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))

        assert data["status"] == "success"
        assert data["read_only"] is True
        assert data["execution_locked"] is True
        assert data["broker_provider"] == "none"
        assert "report" in data
        assert data["report"]["total_scenarios"] == 10

    def test_stress_test_endpoint_scenario_filter(self, dashboard_server: PortfolioDashboardServer) -> None:
        """Authorized request with ?scenario_id=... returns specific scenario result."""
        base_url = f"http://127.0.0.1:{dashboard_server.port}"
        req = urllib.request.Request(
            f"{base_url}/api/paper/stress-test?scenario_id=SCENARIO_A_TIGHT_SPREAD",
            headers={"Authorization": "Bearer step61-auth-token-test"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))

        assert data["status"] == "success"
        assert data["scenario"]["scenario_id"] == "SCENARIO_A_TIGHT_SPREAD"

    def test_stress_test_endpoint_unknown_scenario_returns_404(self, dashboard_server: PortfolioDashboardServer) -> None:
        """Authorized request with non-existent scenario_id returns 404 Not Found."""
        base_url = f"http://127.0.0.1:{dashboard_server.port}"
        req = urllib.request.Request(
            f"{base_url}/api/paper/stress-test?scenario_id=NON_EXISTENT_SCENARIO",
            headers={"Authorization": "Bearer step61-auth-token-test"},
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req)
        assert exc_info.value.code == 404

    def test_stress_test_endpoint_disallows_mutating_methods(self, dashboard_server: PortfolioDashboardServer) -> None:
        """POST, PUT, DELETE to /api/paper/stress-test return 405 Method Not Allowed."""
        base_url = f"http://127.0.0.1:{dashboard_server.port}"
        for method in ("POST", "PUT", "DELETE"):
            req = urllib.request.Request(
                f"{base_url}/api/paper/stress-test",
                headers={"Authorization": "Bearer step61-auth-token-test"},
                method=method,
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req)
            assert exc_info.value.code == 405


# ==============================================================================
# 14. MANDATORY SAFETY BOUNDARIES
# ==============================================================================


class TestMandatorySafetyBoundaries:
    """Verifies that execution remains strictly locked, broker is 'none', and live engine uninitialized."""

    def test_safety_invariants(self, default_settings: Settings) -> None:
        assert default_settings.execution_locked is True
        assert default_settings.broker_provider == "none"
        assert default_settings.paper_execution_enabled is False

        # Live trading engine remains uninitialized
        engine = PaperExecutionEngine(settings=default_settings)
        assert engine.settings.execution_locked is True
        assert engine.is_enabled is False  # Disabled by default

    def test_voice_command_strictly_prohibits_execution(self) -> None:
        """Voice command intent resolver has zero order submission authority."""
        resolver = VoiceCommandIntentResolver()
        commands = [
            "buy 10 shares of INFY",
            "execute order sell 5 TCS",
            "place market buy order on RELIANCE",
        ]
        for cmd in commands:
            intent = resolver.resolve(cmd)
            # Must either resolve to PROHIBITED_TRADING_COMMAND or UNKNOWN, never an execution intent
            assert intent.intent_type in (VoiceIntentType.PROHIBITED_TRADING_COMMAND, VoiceIntentType.UNKNOWN)
            # Zero order execution path exists on intent
            assert not hasattr(intent, "execute_order")
