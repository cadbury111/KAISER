"""KAISER — Tests for Phase 2, Step 60:
Paper Trading Order Lifecycle Metrics, Execution Latency Profiling & Historical-Spread-Based Slippage Calibration.

Comprehensive verification covering:
1. Order Lifecycle Metrics:
   - Supported lifecycle stages (received, validated, quote validated, risk checked, accepted,
     rejected, pending, fill generated, cancelled, ledger persisted, reconciliation, completed, failed).
   - Duplicate event handling / deduplication.
   - Correlation ID, client order ID, and replay session ID propagation.
   - Bounded event storage (ring buffer retention).
   - Deterministic aggregation and SHA-256 lifecycle digest.
2. Execution Latency Profiling:
   - Deterministic injected synthetic monotonic clocks.
   - Aggregate statistics (sample count, min, max, mean, median, p95).
   - Percentile boundary cases (empty samples, single sample, multi-sample).
   - Safe rejection/exclusion of negative, NaN, and infinite durations.
   - Context manager timing and stage failure / incomplete measurement handling.
3. Historical-Spread-Based Slippage Calibration:
   - Bid-ask price spread and relative spread in basis points (bps).
   - Spread distribution by symbol and observation periods.
   - Sample counts and rejection reasons (missing BBO, non-positive, inverted, stale, non-finite).
   - Strict point-in-time filtering (no look-ahead bias with as_of_time).
   - Comparison with PaperRiskConfig and advisory recommendations.
   - Insufficient sample size warnings (< min_sample_size).
   - Deterministic SHA-256 report digest.
   - Immutability guarantee (zero mutation to execution config or financial state).
4. Workflow Integration & Dashboard API Security:
   - PaperTradingWorkflowCoordinator integration with metrics tracker and calibrator.
   - Read-only endpoints (/api/paper/metrics, /api/paper/calibration).
   - Loopback binding and token authorization enforcement (401 on unauthorized).
   - HTTP method restriction (POST/PUT/DELETE return 405 Method Not Allowed).
   - Query parameter validation (symbol, as_of timestamp).
5. Safety, Determinism & Regression Invariants:
   - Replay determinism (identical replay produces identical financial & metrics digests).
   - Financial state invariance (metrics collection does not alter cash, equity, or ledger).
   - Execution lock, broker_provider="none", paper_execution_enabled=False by default.
   - Voice prohibited trading boundaries preserved.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.request
from typing import Generator, List, Optional
import pytest

from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import EventBus
from app.core.state import StateStore
from app.execution import (
    ExecutionLatencyProfiler,
    HistoricalSpreadCalibrator,
    LatencyAggregate,
    LatencyStage,
    ObservationRejectionReason,
    OrderLifecycleEvent,
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
    PaperSimulationSession,
    PaperTradingWorkflowCoordinator,
    PaperWorkflowOutcome,
    PaperWorkflowRequest,
    SimulatedBrokerAdapter,
    SpreadCalibrationReport,
    SpreadObservation,
    SpreadStats,
    WorkflowStage,
    WorkflowStatus,
    calculate_quantiles,
    compute_financial_digest,
)
from app.ledger import LedgerEntryType, PaperLedger, PaperLedgerEntry
from app.market.models import Quote
from app.market.service import MarketDataService
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.reporting import PortfolioReportingService
from app.risk.monitor import PortfolioRiskMonitor
from app.voice.intent import VoiceCommandIntentResolver, VoiceIntentType


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_step60_bus")


@pytest.fixture
def state_store() -> StateStore:
    return StateStore(name="test_step60_store")


@pytest.fixture
def risk_config() -> PaperRiskConfig:
    return PaperRiskConfig(
        max_order_notional=100_000.0,
        max_position_exposure_pct=50.0,
        fee_rate=0.0005,
        slippage_rate=0.0025,  # 25 bps
        half_spread_rate=0.0010,
        freshness_threshold_seconds=60.0,
        allow_short_selling=False,
    )


@pytest.fixture
def deterministic_quotes() -> List[Quote]:
    """Deterministic sequence of valid quotes for calibration testing."""
    base_ts = 1700000000.0
    return [
        Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=base_ts + (i * 10.0),
            last_price=2500.0 + (i * 2.0),
            bid=2499.0 + (i * 2.0),
            ask=2501.0 + (i * 2.0),
            bid_size=100.0,
            ask_size=100.0,
            volume=50000.0,
            source="test_historical",
        )
        for i in range(25)
    ]


# ==============================================================================
# 1. ORDER LIFECYCLE METRICS TESTS
# ==============================================================================

class TestOrderLifecycleTracker:
    """Verifies typed, bounded, deduplicating order lifecycle tracking."""

    def test_order_lifecycle_tracking_standard_flow(self) -> None:
        """Tracks standard lifecycle progression and updates counters accurately."""
        tracker = OrderLifecycleTracker(max_events=100)

        # Record standard successful lifecycle sequence
        stages = [
            OrderLifecycleStage.REQUEST_RECEIVED,
            OrderLifecycleStage.REQUEST_VALIDATED,
            OrderLifecycleStage.QUOTE_VALIDATED,
            OrderLifecycleStage.RISK_CHECKED,
            OrderLifecycleStage.ORDER_ACCEPTED,
            OrderLifecycleStage.FILL_GENERATED,
            OrderLifecycleStage.LEDGER_PERSISTED,
            OrderLifecycleStage.PORTFOLIO_RECONCILED,
            OrderLifecycleStage.WORKFLOW_COMPLETED,
        ]

        for stage in stages:
            added = tracker.record_event(
                stage=stage,
                order_id="ORD_001",
                client_order_id="CLI_001",
                correlation_id="CORR_001",
                replay_session_id="REP_001",
                symbol="TCS",
                message=f"Stage {stage.value} reached",
            )
            assert added is True

        summary = tracker.get_summary()
        assert summary["total_events_recorded"] == 9
        assert summary["distinct_orders_tracked"] == 1
        assert summary["active_orders"] == 0  # Completed order is not active
        assert summary["terminal_orders"]["completed"] == 1
        assert summary["stage_counts"]["request_received"] == 1
        assert summary["stage_counts"]["workflow_completed"] == 1

        events = tracker.get_events(client_order_id="CLI_001")
        assert len(events) == 9
        assert events[0].stage == OrderLifecycleStage.REQUEST_RECEIVED
        assert events[-1].stage == OrderLifecycleStage.WORKFLOW_COMPLETED

    def test_order_lifecycle_deduplication(self) -> None:
        """Duplicate events for the same (order_id, stage) do not inflate counts."""
        tracker = OrderLifecycleTracker(max_events=100)

        # Record first time
        first = tracker.record_event(
            stage=OrderLifecycleStage.REQUEST_RECEIVED,
            order_id="ORD_DEDUP",
            client_order_id="CLI_DEDUP",
        )
        assert first is True

        # Duplicate recording attempt
        second = tracker.record_event(
            stage=OrderLifecycleStage.REQUEST_RECEIVED,
            order_id="ORD_DEDUP",
            client_order_id="CLI_DEDUP",
        )
        assert second is False

        summary = tracker.get_summary()
        assert summary["total_events_recorded"] == 1
        assert summary["duplicate_events_ignored"] == 1
        assert summary["stage_counts"]["request_received"] == 1

    def test_order_lifecycle_rejection_and_cancellation_outcomes(self) -> None:
        """Distinguishes rejected, pending, cancelled, and failed order outcomes."""
        tracker = OrderLifecycleTracker(max_events=100)

        # Order 1: Rejected
        tracker.record_event(OrderLifecycleStage.REQUEST_RECEIVED, order_id="ORD_REJ")
        tracker.record_event(OrderLifecycleStage.ORDER_REJECTED, order_id="ORD_REJ", message="Risk limit exceeded")
        tracker.record_event(OrderLifecycleStage.WORKFLOW_FAILED, order_id="ORD_REJ")

        # Order 2: Cancelled
        tracker.record_event(OrderLifecycleStage.REQUEST_RECEIVED, order_id="ORD_CANC")
        tracker.record_event(OrderLifecycleStage.ORDER_ACCEPTED, order_id="ORD_CANC")
        tracker.record_event(OrderLifecycleStage.ORDER_CANCELLED, order_id="ORD_CANC")

        # Order 3: Pending
        tracker.record_event(OrderLifecycleStage.REQUEST_RECEIVED, order_id="ORD_PEND")
        tracker.record_event(OrderLifecycleStage.ORDER_PENDING, order_id="ORD_PEND")

        summary = tracker.get_summary()
        assert summary["distinct_orders_tracked"] == 3
        assert summary["terminal_orders"]["rejected"] == 1
        assert summary["terminal_orders"]["cancelled"] == 1
        assert summary["terminal_orders"]["failed"] == 1
        assert summary["active_orders"] == 1  # ORD_PEND is pending/active

    def test_order_lifecycle_bounded_storage(self) -> None:
        """Ensures event history is bounded to max_events while cumulative counts persist."""
        tracker = OrderLifecycleTracker(max_events=10)

        # Ingest 30 events across different orders
        for i in range(30):
            tracker.record_event(
                stage=OrderLifecycleStage.REQUEST_RECEIVED,
                order_id=f"ORD_BOUNDED_{i}",
            )

        events = tracker.get_events()
        assert len(events) == 10  # Bounded to max_events
        # Most recent events retained
        assert events[-1].order_id == "ORD_BOUNDED_29"

        summary = tracker.get_summary()
        assert summary["total_events_recorded"] == 30
        assert summary["stage_counts"]["request_received"] == 30

    def test_order_lifecycle_correlation_and_replay_propagation(self) -> None:
        """Preserves correlation IDs, replay/session IDs, and metadata without leaking secrets."""
        tracker = OrderLifecycleTracker(max_events=50)

        tracker.record_event(
            stage=OrderLifecycleStage.REQUEST_RECEIVED,
            order_id="ORD_META",
            client_order_id="CLI_META",
            correlation_id="CORR_999",
            replay_session_id="REPLAY_STEP60",
            symbol="INFY",
            details={"route": "paper_workflow", "safe_param": 42},
        )

        events = tracker.get_events(order_id="ORD_META")
        assert len(events) == 1
        event = events[0]
        assert event.correlation_id == "CORR_999"
        assert event.replay_session_id == "REPLAY_STEP60"
        assert event.symbol == "INFY"
        assert event.details.get("safe_param") == 42

    def test_order_lifecycle_deterministic_digest(self) -> None:
        """Identical ordered event sequences produce identical deterministic digests."""
        tracker_a = OrderLifecycleTracker(max_events=50)
        tracker_b = OrderLifecycleTracker(max_events=50)

        for i in range(10):
            for trk in (tracker_a, tracker_b):
                trk.record_event(
                    stage=OrderLifecycleStage.REQUEST_RECEIVED,
                    order_id=f"ORD_{i}",
                    client_order_id=f"CLI_{i}",
                    symbol="RELIANCE",
                )
                trk.record_event(
                    stage=OrderLifecycleStage.ORDER_ACCEPTED,
                    order_id=f"ORD_{i}",
                    client_order_id=f"CLI_{i}",
                    symbol="RELIANCE",
                )

        digest_a = tracker_a.get_summary()["deterministic_digest"]
        digest_b = tracker_b.get_summary()["deterministic_digest"]
        assert digest_a == digest_b
        assert len(digest_a) == 64


# ==============================================================================
# 2. EXECUTION LATENCY PROFILING TESTS
# ==============================================================================

class TestExecutionLatencyProfiler:
    """Verifies monotonic execution latency profiling and statistical aggregation."""

    def test_latency_deterministic_synthetic_clock(self) -> None:
        """Uses injected synthetic clock to verify exact microsecond/millisecond measurement."""
        synthetic_time = [1000.0]

        def clock_fn() -> float:
            return synthetic_time[0]

        profiler = ExecutionLatencyProfiler(clock_fn=clock_fn, max_samples_per_stage=50)

        # Measure 15.5 milliseconds elapsed
        token = profiler.start_stage(LatencyStage.REQUEST_VALIDATION)
        synthetic_time[0] += 0.0155  # 15.5 ms
        elapsed = profiler.stop_stage(LatencyStage.REQUEST_VALIDATION, token)

        assert elapsed == pytest.approx(15.5, abs=1e-6)

        profile = profiler.get_stage_aggregate(LatencyStage.REQUEST_VALIDATION)
        assert profile.sample_count == 1
        assert profile.mean_ms == pytest.approx(15.5, abs=1e-6)
        assert profile.median_ms == pytest.approx(15.5, abs=1e-6)
        assert profile.p95_ms == pytest.approx(15.5, abs=1e-6)
        assert profile.unit == "milliseconds"

    def test_latency_quantiles_empty_and_single_sample(self) -> None:
        """Handles empty and single sample boundary conditions safely without errors."""
        # Empty sample list
        assert calculate_quantiles([], 0.5) is None
        assert calculate_quantiles([], 0.95) is None

        # Single sample list
        assert calculate_quantiles([42.0], 0.5) == 42.0
        assert calculate_quantiles([42.0], 0.95) == 42.0

        profiler = ExecutionLatencyProfiler()
        agg = profiler.get_stage_aggregate(LatencyStage.RISK_VALIDATION)
        assert agg.sample_count == 0
        assert agg.mean_ms is None
        assert agg.median_ms is None
        assert agg.p95_ms is None

    def test_latency_quantiles_multi_sample_percentiles(self) -> None:
        """Verifies linear interpolation quantile calculation across known test vectors."""
        # 10 samples: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
        samples = [float(i * 10) for i in range(1, 11)]

        median = calculate_quantiles(samples, 0.50)
        assert median == pytest.approx(55.0, abs=1e-6)

        p95 = calculate_quantiles(samples, 0.95)
        # N=10, q=0.95 -> idx = 0.95 * 9 = 8.55 -> between samples[8]=90 and samples[9]=100
        assert p95 == pytest.approx(95.5, abs=1e-6)

        p99 = calculate_quantiles(samples, 0.99)
        assert p99 == pytest.approx(99.1, abs=1e-6)

    def test_latency_negative_and_non_finite_rejection(self) -> None:
        """Safely excludes NaN, infinity, and negative elapsed durations."""
        profiler = ExecutionLatencyProfiler(max_samples_per_stage=50)

        # Attempt to record invalid values directly
        assert profiler.record_stage_duration(LatencyStage.BROKER_PROCESSING, -10.0) is False
        assert profiler.record_stage_duration(LatencyStage.BROKER_PROCESSING, float("nan")) is False
        assert profiler.record_stage_duration(LatencyStage.BROKER_PROCESSING, float("inf")) is False
        assert profiler.record_stage_duration(LatencyStage.BROKER_PROCESSING, float("-inf")) is False

        # Record one valid value
        assert profiler.record_stage_duration(LatencyStage.BROKER_PROCESSING, 5.0) is True

        agg = profiler.get_stage_aggregate(LatencyStage.BROKER_PROCESSING)
        assert agg.sample_count == 1
        assert agg.invalid_samples_count == 4
        assert agg.min_ms == 5.0
        assert agg.max_ms == 5.0

    def test_latency_context_manager_and_exception_handling(self) -> None:
        """Context manager records duration even when an exception is raised inside the block."""
        synthetic_time = [5000.0]

        def clock_fn() -> float:
            return synthetic_time[0]

        profiler = ExecutionLatencyProfiler(clock_fn=clock_fn)

        with pytest.raises(ValueError, match="simulated failure"):
            with profiler.measure_stage(LatencyStage.LEDGER_PERSISTENCE):
                synthetic_time[0] += 0.008  # 8 ms
                raise ValueError("simulated failure")

        agg = profiler.get_stage_aggregate(LatencyStage.LEDGER_PERSISTENCE)
        assert agg.sample_count == 1
        assert agg.mean_ms == pytest.approx(8.0, abs=1e-6)

    def test_latency_profile_summary_and_fingerprint(self) -> None:
        """Generates deterministic profile summary and SHA-256 fingerprint."""
        profiler = ExecutionLatencyProfiler()
        profiler.record_stage_duration(LatencyStage.WORKFLOW_E2E, 12.0)
        profiler.record_stage_duration(LatencyStage.WORKFLOW_E2E, 16.0)

        summary = profiler.get_profile_summary()
        assert "stages" in summary
        assert "workflow_e2e" in summary["stages"]
        assert summary["stages"]["workflow_e2e"]["sample_count"] == 2
        assert "fingerprint" in summary
        assert len(summary["fingerprint"]) == 64


# ==============================================================================
# 3. HISTORICAL-SPREAD-BASED SLIPPAGE CALIBRATION TESTS
# ==============================================================================

class TestHistoricalSpreadCalibrator:
    """Verifies deterministic, offline, point-in-time spread calibration."""

    def test_calibrator_valid_normal_and_wide_spreads(self) -> None:
        """Calculates price spread, relative spread in bps, and half-spread in bps correctly."""
        calibrator = HistoricalSpreadCalibrator()

        # Quote 1: 2500 mid, spread 2.0 (bid 2499, ask 2501)
        # Relative spread = 2.0 / 2500.0 * 10000 = 8.0 bps; half-spread = 4.0 bps
        q1 = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=1700000000.0,
            last_price=2500.0,
            bid=2499.0,
            ask=2501.0,
            volume=1000.0,
        )
        obs1 = calibrator.add_quote(q1)
        assert obs1 is not None
        assert obs1.price_spread == pytest.approx(2.0, abs=1e-6)
        assert obs1.mid_price == pytest.approx(2500.0, abs=1e-6)
        assert obs1.relative_spread_bps == pytest.approx(8.0, abs=1e-4)
        assert obs1.half_spread_bps == pytest.approx(4.0, abs=1e-4)

        # Quote 2: 100 mid, wide spread 2.0 (bid 99.0, ask 101.0)
        # Relative spread = 2.0 / 100.0 * 10000 = 200.0 bps; half-spread = 100.0 bps
        q2 = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=1700000010.0,
            last_price=100.0,
            bid=99.0,
            ask=101.0,
            volume=1000.0,
        )
        obs2 = calibrator.add_quote(q2)
        assert obs2 is not None
        assert obs2.relative_spread_bps == pytest.approx(200.0, abs=1e-4)

    def test_calibrator_multi_symbol_isolation(self) -> None:
        """Maintains clean observation isolation across multiple symbols."""
        calibrator = HistoricalSpreadCalibrator()

        q_rel = Quote(symbol="RELIANCE", exchange="NSE", timestamp=100.0, last_price=2500.0, bid=2499.0, ask=2501.0)
        q_infy = Quote(symbol="INFY", exchange="NSE", timestamp=100.0, last_price=1500.0, bid=1498.0, ask=1502.0)

        calibrator.add_quote(q_rel)
        calibrator.add_quote(q_infy)

        symbols = calibrator.get_symbols()
        assert "RELIANCE" in symbols
        assert "INFY" in symbols

        assert len(calibrator.get_observations("RELIANCE")) == 1
        assert len(calibrator.get_observations("INFY")) == 1

    def test_calibrator_invalid_quote_rejections(self) -> None:
        """Rejects missing BBO, inverted spreads, non-positive, non-finite, and stale quotes."""
        calibrator = HistoricalSpreadCalibrator(strict_chronological=True)

        # 1. Missing BBO
        q_missing = Quote(symbol="TCS", exchange="NSE", timestamp=100.0, last_price=3000.0, bid=None, ask=3001.0)
        assert calibrator.add_quote(q_missing) is None
        assert calibrator.get_rejected_counts()["TCS"][ObservationRejectionReason.MISSING_BBO] == 1

        # 2. Inverted spread (bid > ask) via raw feed observation
        rec_inverted = {"symbol": "TCS", "timestamp": 101.0, "last_price": 3000.0, "bid": 3005.0, "ask": 3000.0}
        ok, reason, _ = calibrator.add_observation(rec_inverted)
        assert ok is False
        assert reason == ObservationRejectionReason.INVERTED_SPREAD
        assert calibrator.get_rejected_counts()["TCS"][ObservationRejectionReason.INVERTED_SPREAD] == 1

        # 3. Non-positive price
        rec_non_pos = {"symbol": "TCS", "timestamp": 102.0, "last_price": 3000.0, "bid": -5.0, "ask": 10.0}
        ok, reason, _ = calibrator.add_observation(rec_non_pos)
        assert ok is False
        assert reason == ObservationRejectionReason.NON_POSITIVE
        assert calibrator.get_rejected_counts()["TCS"][ObservationRejectionReason.NON_POSITIVE] == 1

        # 4. Non-finite
        rec_nan = {"symbol": "TCS", "timestamp": 103.0, "last_price": 3000.0, "bid": float("nan"), "ask": 3000.0}
        ok, reason, _ = calibrator.add_observation(rec_nan)
        assert ok is False
        assert reason == ObservationRejectionReason.NON_FINITE
        assert calibrator.get_rejected_counts()["TCS"][ObservationRejectionReason.NON_FINITE] == 1

        # 5. Stale / out-of-order quote
        q_valid = Quote(symbol="TCS", exchange="NSE", timestamp=110.0, last_price=3000.0, bid=2999.0, ask=3001.0)
        assert calibrator.add_quote(q_valid) is not None

        q_stale = Quote(symbol="TCS", exchange="NSE", timestamp=105.0, last_price=3000.0, bid=2999.0, ask=3001.0)
        assert calibrator.add_quote(q_stale) is None
        assert calibrator.get_rejected_counts()["TCS"][ObservationRejectionReason.OUT_OF_ORDER] == 1

    def test_calibrator_no_look_ahead_bias(self, deterministic_quotes: List[Quote]) -> None:
        """Enforces point-in-time calibration by strictly excluding observations after as_of_time."""
        calibrator = HistoricalSpreadCalibrator()
        for q in deterministic_quotes:
            calibrator.add_quote(q)

        # All 25 quotes ingested (timestamps from base_ts to base_ts + 240)
        base_ts = deterministic_quotes[0].timestamp
        cutoff_time = base_ts + 100.0  # Exactly covers quotes 0 through 10 (11 quotes)

        report = calibrator.generate_report(symbol="RELIANCE", as_of_time=cutoff_time)
        assert report is not None
        assert report.stats.sample_count == 11
        assert report.as_of_time == cutoff_time

        # Verify full report without cutoff includes all 25
        full_report = calibrator.generate_report(symbol="RELIANCE")
        assert full_report is not None
        assert full_report.stats.sample_count == 25

    def test_calibrator_insufficient_samples_advisory_warning(self) -> None:
        """Flags statistical unreliability when sample size is below min_sample_size."""
        calibrator = HistoricalSpreadCalibrator(min_sample_size=20)

        # Ingest only 5 quotes
        for i in range(5):
            calibrator.add_quote(
                Quote(
                    symbol="HDFC",
                    exchange="NSE",
                    timestamp=100.0 + i,
                    last_price=1600.0,
                    bid=1599.0,
                    ask=1601.0,
                )
            )

        report = calibrator.generate_report(symbol="HDFC")
        assert report is not None
        assert report.is_statistically_reliable is False
        assert any("insufficient sample size" in w.lower() for w in report.warnings)

    def test_calibrator_risk_config_comparison_and_recommendation(
        self,
        deterministic_quotes: List[Quote],
        risk_config: PaperRiskConfig,
    ) -> None:
        """Compares observed spreads against configured slippage and provides advisory recommendations."""
        calibrator = HistoricalSpreadCalibrator(min_sample_size=10)
        for q in deterministic_quotes:
            calibrator.add_quote(q)

        report = calibrator.generate_report(symbol="RELIANCE", risk_config=risk_config)
        assert report is not None
        assert report.configured_slippage_bps == 25.0
        assert report.is_slippage_adequate is True  # Observed half-spread is ~4 bps, config is 25 bps
        assert report.recommended_slippage_bps is not None
        assert report.recommended_slippage_bps > 0.0

        # Safety invariant: Risk config must NOT be mutated
        assert risk_config.slippage_rate == 0.0025

    def test_calibrator_deterministic_digest(self, deterministic_quotes: List[Quote]) -> None:
        """Identical inputs produce identical SHA-256 calibration digests."""
        calibrator_a = HistoricalSpreadCalibrator()
        calibrator_b = HistoricalSpreadCalibrator()

        for q in deterministic_quotes:
            calibrator_a.add_quote(q)
            calibrator_b.add_quote(copy.deepcopy(q))

        report_a = calibrator_a.generate_report(symbol="RELIANCE", as_of_time=deterministic_quotes[-1].timestamp)
        report_b = calibrator_b.generate_report(symbol="RELIANCE", as_of_time=deterministic_quotes[-1].timestamp)

        assert report_a is not None and report_b is not None
        assert report_a.deterministic_digest == report_b.deterministic_digest
        assert len(report_a.deterministic_digest) == 64


# ==============================================================================
# 4. WORKFLOW INTEGRATION & DASHBOARD API SECURITY TESTS
# ==============================================================================

class TestWorkflowMetricsAndCalibrationIntegration:
    """Verifies seamless metrics and calibration integration inside PaperTradingWorkflowCoordinator."""

    def test_workflow_records_lifecycle_and_latency(
        self,
        event_bus: EventBus,
        state_store: StateStore,
        risk_config: PaperRiskConfig,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Workflow execution populates lifecycle stages and latency measurements."""
        ledger = PaperLedger()
        portfolio = PaperPortfolio(initial_cash=100_000.0)
        broker = SimulatedBrokerAdapter()
        engine = PaperExecutionEngine(
            ledger=ledger,
            portfolio=portfolio,
            broker_adapter=broker,
            risk_config=risk_config,
            enabled=True,
        )
        workflow = PaperTradingWorkflowCoordinator(
            engine=engine,
            market_service=None,
            enabled=True,
        )

        # Ingest a quote into calibrator
        workflow.add_market_observation(deterministic_quotes[0])

        # Create fresh quote for live workflow execution
        fresh_quote = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=time.time(),
            last_price=2500.0,
            bid=2499.5,
            ask=2500.5,
            bid_size=100.0,
            ask_size=100.0,
            volume=50000.0,
            source="test_live",
        )

        # Execute a paper workflow request
        req = PaperWorkflowRequest(
            client_order_id="WF_TEST_01",
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.LIMIT,
            quantity=10.0,
            limit_price=2500.0,
            quote=fresh_quote,
            correlation_id="CORR_WF_01",
            session_id="SESS_WF_01",
        )

        result = workflow.execute_workflow(req)
        assert result.status in (WorkflowStatus.FILLED, WorkflowStatus.ACCEPTED)
        assert result.success is True

        # Verify lifecycle summary
        lifecycle_summary = workflow.get_lifecycle_summary()
        assert lifecycle_summary["total_events_recorded"] >= 8
        assert lifecycle_summary["stage_counts"]["request_received"] == 1
        assert lifecycle_summary["stage_counts"]["workflow_completed"] == 1

        # Verify latency profile
        latency_profile = workflow.get_latency_profile()
        assert "stages" in latency_profile
        assert latency_profile["stages"]["workflow_e2e"]["sample_count"] == 1
        assert latency_profile["stages"]["workflow_e2e"]["mean_ms"] >= 0.0

        # Verify calibration summary
        calib_summary = workflow.get_calibration_summary("RELIANCE")
        assert calib_summary["symbol"] == "RELIANCE"
        assert calib_summary["stats"]["sample_count"] == 2

        # Verify top-level summary enrichment
        top_summary = workflow.get_summary()
        assert "lifecycle_metrics" in top_summary
        assert "latency_profile" in top_summary
        assert "spread_calibration" in top_summary


class TestDashboardMetricsAndCalibrationSecurity:
    """Verifies authorization, HTTP method boundaries, and query validation on dashboard endpoints."""

    @pytest.fixture
    def authenticated_dashboard(
        self,
        event_bus: EventBus,
        state_store: StateStore,
        risk_config: PaperRiskConfig,
    ) -> Generator[PortfolioDashboardServer, None, None]:
        """Runs a test dashboard server with auth enabled and paper workflow attached."""
        attribution = PortfolioAttributionService(event_bus=event_bus, state_store=state_store)
        risk_mon = PortfolioRiskMonitor(event_bus=event_bus, state_store=state_store)
        reporting = PortfolioReportingService(
            event_bus=event_bus,
            state_store=state_store,
            attribution_service=attribution,
            risk_monitor=risk_mon,
        )

        ledger = PaperLedger()
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        broker = SimulatedBrokerAdapter()
        engine = PaperExecutionEngine(
            ledger=ledger,
            portfolio=portfolio,
            broker_adapter=broker,
            risk_config=risk_config,
            enabled=True,
        )
        workflow = PaperTradingWorkflowCoordinator(engine=engine, enabled=True)

        # Ingest a sample quote
        workflow.add_market_observation(
            Quote(symbol="TCS", exchange="NSE", timestamp=1700000000.0, last_price=3500.0, bid=3499.0, ask=3501.0)
        )

        server = PortfolioDashboardServer(
            reporting_service=reporting,
            workflow_coordinator=workflow,
            host="127.0.0.1",
            port=0,
            require_auth=True,
            auth_token="step60-test-token-xyz",
        )
        server.start(blocking=False)
        yield server
        server.stop()

    def test_dashboard_endpoints_require_authorization(
        self,
        authenticated_dashboard: PortfolioDashboardServer,
    ) -> None:
        """Unauthenticated requests to /api/paper/metrics and /api/paper/calibration return 401."""
        base_url = f"http://127.0.0.1:{authenticated_dashboard.port}"

        # 1. /api/paper/metrics without token
        req = urllib.request.Request(f"{base_url}/api/paper/metrics")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req)
        assert exc_info.value.code == 401

        # 2. /api/paper/calibration without token
        req_cal = urllib.request.Request(f"{base_url}/api/paper/calibration")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_cal)
        assert exc_info.value.code == 401

        # 3. Authorized request with Bearer token
        auth_req = urllib.request.Request(
            f"{base_url}/api/paper/metrics",
            headers={"Authorization": "Bearer step60-test-token-xyz"},
        )
        with urllib.request.urlopen(auth_req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert "lifecycle" in data
            assert "latency" in data

    def test_dashboard_endpoints_reject_mutating_methods(
        self,
        authenticated_dashboard: PortfolioDashboardServer,
    ) -> None:
        """POST, PUT, and DELETE methods are strictly rejected with 405 Method Not Allowed."""
        base_url = f"http://127.0.0.1:{authenticated_dashboard.port}"
        headers = {"Authorization": "Bearer step60-test-token-xyz"}

        for endpoint in ("/api/paper/metrics", "/api/paper/calibration"):
            for method in ("POST", "PUT", "DELETE"):
                req = urllib.request.Request(
                    f"{base_url}{endpoint}",
                    headers=headers,
                    method=method,
                    data=b'{"test": "payload"}',
                )
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(req)
                assert exc_info.value.code == 405

    def test_dashboard_calibration_query_param_validation(
        self,
        authenticated_dashboard: PortfolioDashboardServer,
    ) -> None:
        """Validates query parameters (symbol, as_of) and rejects non-numeric as_of."""
        base_url = f"http://127.0.0.1:{authenticated_dashboard.port}"
        headers = {"Authorization": "Bearer step60-test-token-xyz"}

        # Valid symbol query
        req_sym = urllib.request.Request(
            f"{base_url}/api/paper/calibration?symbol=TCS",
            headers=headers,
        )
        with urllib.request.urlopen(req_sym) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["symbol"] == "TCS"
            assert data["stats"]["sample_count"] == 1

        # Invalid non-numeric as_of
        req_bad = urllib.request.Request(
            f"{base_url}/api/paper/calibration?as_of=invalid_timestamp",
            headers=headers,
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_bad)
        assert exc_info.value.code == 400


# ==============================================================================
# 5. REPRODUCIBILITY, SAFETY & REGRESSION INVARIANTS
# ==============================================================================

class TestReproducibilityAndSafetyInvariants:
    """Verifies replay reproducibility, zero financial mutation, and mandatory safety invariants."""

    def test_replay_determinism_financial_and_metrics_digests(
        self,
        risk_config: PaperRiskConfig,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Identical replay feeds produce identical financial state and metrics digests."""
        def run_simulation() -> tuple[str, str, str, float]:
            ledger = PaperLedger()
            portfolio = PaperPortfolio(initial_cash=100_000.0)
            broker = SimulatedBrokerAdapter()
            engine = PaperExecutionEngine(
                ledger=ledger,
                portfolio=portfolio,
                broker_adapter=broker,
                risk_config=risk_config,
                enabled=True,
            )
            wf = PaperTradingWorkflowCoordinator(engine=engine, enabled=True)

            for i, q in enumerate(deterministic_quotes[:5]):
                wf.add_market_observation(q)
                req = PaperWorkflowRequest(
                    client_order_id=f"REPLAY_ORD_{i}",
                    symbol=q.symbol,
                    side=PaperOrderSide.BUY,
                    order_type=PaperOrderType.LIMIT,
                    quantity=5.0,
                    limit_price=q.ask,
                    quote=q,
                )
                wf.execute_workflow(req)

            fin_digest = compute_financial_digest(portfolio=portfolio, ledger=ledger)
            life_digest = wf.get_lifecycle_summary()["deterministic_digest"]
            calib_digest = wf.get_calibration_summary("RELIANCE")["deterministic_digest"]
            return fin_digest, life_digest, calib_digest, portfolio.cash

        fin_1, life_1, cal_1, cash_1 = run_simulation()
        fin_2, life_2, cal_2, cash_2 = run_simulation()

        assert fin_1 == fin_2
        assert life_1 == life_2
        assert cal_1 == cal_2
        assert cash_1 == cash_2

    def test_metrics_collection_does_not_mutate_financial_state(
        self,
        risk_config: PaperRiskConfig,
        deterministic_quotes: List[Quote],
    ) -> None:
        """Observing lifecycle metrics and spread calibration causes zero financial or position mutation."""
        ledger = PaperLedger()
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        broker = SimulatedBrokerAdapter()
        engine = PaperExecutionEngine(
            ledger=ledger,
            portfolio=portfolio,
            broker_adapter=broker,
            risk_config=risk_config,
            enabled=True,
        )
        wf = PaperTradingWorkflowCoordinator(engine=engine, enabled=True)

        initial_cash = portfolio.cash
        initial_equity = portfolio.total_equity
        initial_entries = len(ledger.get_entries())

        # Feed 10 quotes to calibrator and query lifecycle / latency / calibration summaries repeatedly
        for q in deterministic_quotes[:10]:
            wf.add_market_observation(q)

        for _ in range(5):
            _ = wf.get_lifecycle_summary()
            _ = wf.get_latency_profile()
            _ = wf.get_calibration_summary("RELIANCE")
            _ = wf.get_summary()

        # Invariant: cash, equity, position count, and ledger entries remain untouched
        assert portfolio.cash == initial_cash
        assert portfolio.total_equity == initial_equity
        assert len(portfolio.positions) == 0
        assert len(ledger.get_entries()) == initial_entries

    def test_mandatory_system_safety_invariants(self) -> None:
        """Confirms execution_locked=True, broker_provider='none', paper_execution_enabled=False."""
        settings = Settings()
        assert settings.execution_locked is True
        assert settings.broker_provider == "none"
        assert settings.paper_execution_enabled is False

        # Verify bootstrap preserves safety boundaries
        sys_status = bootstrap_system()
        assert sys_status.initialized is True
        assert sys_status.event_bus is not None
        assert sys_status.trading_engine == "not initialized"
        assert sys_status.execution_engine == "locked"
        assert sys_status.settings.execution_locked is True
        assert sys_status.settings.broker_provider == "none"
        assert sys_status.settings.paper_execution_enabled is False

    def test_voice_prohibited_trading_boundary_preserved(self) -> None:
        """Verifies voice commands cannot trigger paper or live order execution."""
        resolver = VoiceCommandIntentResolver()

        prohibited_commands = [
            "buy 10 shares of RELIANCE",
            "sell 50 TCS at market",
            "execute order",
            "paper trade 100 INFY",
            "place order now",
        ]

        for cmd in prohibited_commands:
            intent = resolver.resolve(cmd)
            # Intent resolver must classify as PROHIBITED_TRADING_COMMAND or UNKNOWN, never an execution intent
            assert intent.intent_type in (VoiceIntentType.PROHIBITED_TRADING_COMMAND, VoiceIntentType.UNKNOWN)
            assert intent.intent_type != VoiceIntentType.DIRECT_COMMAND
            if intent.intent_type == VoiceIntentType.UNKNOWN:
                assert intent.confidence == 0.0

        explicit_prohibited = [
            "buy 10 shares of RELIANCE",
            "sell 50 TCS at market",
        ]
        for cmd in explicit_prohibited:
            intent = resolver.resolve(cmd)
            assert intent.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND
            assert intent.is_prohibited_trading is True
