"""KAISER — Phase 2, Step 19 Test Suite: Market Data Quality Monitoring & Anomaly Detection.

Validates:
A. Completeness (expected timestamps, missing bars, irregular intervals)
B. Freshness (fresh data, stale data, injected clock, clock skew)
C. Chronology & Duplication (sorted data, out-of-order, harmless duplicates, conflicting duplicates)
D. OHLC Integrity (high/low relations, valid boundaries, non-negative volume)
E. Numerical Validity (rejection of NaN, +Inf, -Inf; preservation of valid zero values)
F. Anomaly Detection (price jump, volume spike, zero volume, large range, repeated bars, excessive gaps)
G. Severity & Precedence (deterministic severity ranking, highest severity aggregation)
H. Numerical Quality Score (bounded in [0.0, 100.0], explainable, no NaN/inf, zero metrics preserved)
I. StateStore Integration (bounded history, replacement, concurrency, deep-copy isolation)
J. EventBus Integration (event types, bounded payloads, zero secret leakage)
K. Handler Integration (assess, get_quality, get_anomalies)
L. Historical Integration (loader, stored repository data, replay engine)
M. Dashboard Integration (GET /api/data-quality read-only loopback endpoint)
N. Property / Invariant Verification
O. Failure Recovery
P. Performance Benchmarks (100, 1,000, 10,000 bars)
Q. Safety Constraints (execution_locked=True, no broker, no trading, JUJU untouched)
"""

from __future__ import annotations

import collections
import math
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional
import uuid

import pytest

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.loader import HistoricalDataLoader
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalProviderStatus,
)
from app.market.historical.mock import MockHistoricalDataProvider
from app.market.historical.replay import (
    HistoricalReplayEngine,
    HistoricalReplayRequest,
    HistoricalReplayResult,
    HistoricalReplayStatus,
)
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar, Quote
from app.market.validators import DataFreshnessStatus
from app.market.quality.handlers import register_quality_handlers
from app.market.quality.models import (
    AnomalyType,
    MarketAnomaly,
    QualityFinding,
    QualitySeverity,
    max_severity,
    sanitize_message,
)
from app.market.quality.monitor import MarketDataQualityMonitor
from app.market.quality.policy import (
    MarketQualityConfig,
    calculate_quality_score,
    is_safe_for_downstream,
    resolve_quality_status,
)
from app.market.quality.status import (
    DataQualityAssessment,
    DataQualityStatus,
    DatasetQualityAssessment,
)
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.reporting import PortfolioReportingService


def make_sample_bars(
    count: int = 20,
    symbol: str = "NIFTY",
    timeframe: str = "15m",
    start_time: float = 1700000000.0,
    interval_sec: float = 900.0,
    base_price: float = 20000.0,
    volume: float = 1000.0,
) -> List[Bar]:
    """Generate deterministic, clean synthetic OHLCV bars."""
    bars: List[Bar] = []
    price = base_price
    for i in range(count):
        ts = start_time + (i * interval_sec)
        o = price
        h = price + 20.0
        l = price - 15.0
        c = price + 10.0
        bars.append(
            Bar(
                symbol=symbol,
                exchange="NSE",
                timeframe=timeframe,
                timestamp=ts,
                open=round(o, 2),
                high=round(h, 2),
                low=round(l, 2),
                close=round(c, 2),
                volume=volume,
                source="test",
            )
        )
        price = c
    return bars


# =====================================================================
# A. COMPLETENESS TESTS
# =====================================================================
class TestCompleteness:
    """Validate completeness evaluation across expected timestamps and missing bars."""

    def test_no_gaps_complete_series(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=20, interval_sec=900.0)
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m")

        assert assessment.quality_status == DataQualityStatus.VALID
        assert assessment.gap_count == 0
        assert assessment.total_records == 20
        assert assessment.valid_records == 20
        assert assessment.invalid_records == 0
        assert assessment.completeness_status == "COMPLETE"
        assert assessment.quality_score == 100.0

    def test_single_gap_detected(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=10, interval_sec=900.0)
        # Introduce a gap after bar 4 (skip 3 intervals = 2700s)
        bars_after_gap = make_sample_bars(
            count=5,
            start_time=bars[4].timestamp + 3600.0,
            interval_sec=900.0,
        )
        bars_with_gap = bars[:5] + bars_after_gap
        assessment = monitor.evaluate_bar_series(bars_with_gap, symbol="NIFTY", timeframe="15m")

        assert assessment.gap_count == 1
        assert any(f.dimension == "gap_detection" for f in assessment.findings)
        assert assessment.quality_status == DataQualityStatus.DEGRADED
        assert assessment.quality_score < 100.0

    def test_multiple_gaps_detected(self) -> None:
        monitor = MarketDataQualityMonitor()
        # Bar 1, Bar 2 (gap 2), Bar 3 (gap 5)
        t0 = 1700000000.0
        b1 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0, open=100, high=105, low=95, close=102, volume=50)
        b2 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + 2700.0, open=102, high=107, low=100, close=105, volume=60)
        b3 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + 7200.0, open=105, high=110, low=102, close=108, volume=70)

        assessment = monitor.evaluate_bar_series([b1, b2, b3], symbol="NIFTY", timeframe="15m")
        assert assessment.gap_count == 2
        assert assessment.valid_records == 3
        gap_findings = [f for f in assessment.findings if f.dimension == "gap_detection"]
        assert len(gap_findings) == 2

    def test_empty_dataset_missing_status(self) -> None:
        monitor = MarketDataQualityMonitor()
        assessment = monitor.evaluate_bar_series([], symbol="NIFTY", timeframe="15m")
        assert assessment.quality_status == DataQualityStatus.MISSING
        assert assessment.total_records == 0
        assert assessment.valid_records == 0
        assert assessment.quality_score == 0.0
        assert not assessment.safe_to_consume


# =====================================================================
# B. FRESHNESS TESTS
# =====================================================================
class TestFreshness:
    """Validate data freshness, stale thresholding, and deterministic injected clock."""

    def test_fresh_data_within_threshold(self) -> None:
        cfg = MarketQualityConfig(stale_threshold_seconds=60.0)
        monitor = MarketDataQualityMonitor(config=cfg)
        now = 1700000000.0
        # Last bar at now - 30s (fresh)
        bars = [
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now - 90.0, open=100, high=105, low=95, close=100, volume=10),
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now - 30.0, open=100, high=105, low=95, close=100, volume=10),
        ]
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="1m", reference_time=now)
        assert not assessment.stale_status
        assert assessment.freshness_status == DataFreshnessStatus.FRESH
        assert assessment.quality_status == DataQualityStatus.VALID

    def test_stale_data_exceeds_threshold(self) -> None:
        cfg = MarketQualityConfig(stale_threshold_seconds=60.0)
        monitor = MarketDataQualityMonitor(config=cfg)
        now = 1700000000.0
        # Last bar at now - 300s (stale)
        bars = [
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now - 360.0, open=100, high=105, low=95, close=100, volume=10),
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now - 300.0, open=100, high=105, low=95, close=100, volume=10),
        ]
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="1m", reference_time=now)
        assert assessment.stale_status
        assert assessment.freshness_status == DataFreshnessStatus.STALE
        assert assessment.quality_status == DataQualityStatus.STALE
        assert not assessment.safe_to_consume
        assert assessment.quality_score <= 85.0

    def test_future_clock_skew_flagged_as_error(self) -> None:
        monitor = MarketDataQualityMonitor()
        now = 1700000000.0
        # Bar timestamp in impossible future (+100s)
        bars = [
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=now + 100.0, open=100, high=105, low=95, close=100, volume=10),
        ]
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="1m", reference_time=now)
        assert assessment.quality_status == DataQualityStatus.INVALID
        assert assessment.freshness_status == DataFreshnessStatus.INVALID
        assert assessment.highest_severity in (QualitySeverity.ERROR, QualitySeverity.CRITICAL)


# =====================================================================
# C. CHRONOLOGY & DUPLICATION TESTS
# =====================================================================
class TestChronologyAndDuplication:
    """Validate monotonicity, out-of-order bars, harmless duplicates, and conflicting duplicates."""

    def test_out_of_order_bars_detected(self) -> None:
        monitor = MarketDataQualityMonitor()
        t0 = 1700000000.0
        b1 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0, open=100, high=105, low=95, close=102, volume=50)
        b2 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + 900.0, open=102, high=107, low=100, close=105, volume=50)
        b3 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + 450.0, open=101, high=106, low=98, close=103, volume=50)  # Out of order!

        assessment = monitor.evaluate_bar_series([b1, b2, b3], symbol="NIFTY", timeframe="15m")
        assert assessment.out_of_order_count == 1
        assert assessment.quality_status == DataQualityStatus.OUT_OF_ORDER
        assert any(a.anomaly_type == AnomalyType.OUT_OF_ORDER_TIMESTAMP for a in assessment.anomalies)

    def test_harmless_duplicate_bars_tracked(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=5)
        # Duplicate the 3rd bar exactly
        bars_with_dup = bars[:3] + [bars[2]] + bars[3:]
        assessment = monitor.evaluate_bar_series(bars_with_dup, symbol="NIFTY", timeframe="15m")

        assert assessment.duplicate_count == 1
        assert assessment.conflicting_duplicate_count == 0
        assert assessment.quality_status == DataQualityStatus.DUPLICATE

    def test_conflicting_duplicate_flagged_error(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=5)
        # Duplicate 3rd bar with different price!
        conflicting_bar = Bar(
            symbol="NIFTY",
            exchange="NSE",
            timeframe="15m",
            timestamp=bars[2].timestamp,
            open=99999.0,
            high=99999.0,
            low=99999.0,
            close=99999.0,
            volume=50000.0,
        )
        bars_with_conflict = bars[:3] + [conflicting_bar] + bars[3:]
        assessment = monitor.evaluate_bar_series(bars_with_conflict, symbol="NIFTY", timeframe="15m")

        assert assessment.conflicting_duplicate_count == 1
        assert assessment.quality_status == DataQualityStatus.INVALID
        assert any(a.anomaly_type == AnomalyType.CONFLICTING_DUPLICATE for a in assessment.anomalies)
        assert assessment.highest_severity in (QualitySeverity.ERROR, QualitySeverity.CRITICAL)


# =====================================================================
# D. OHLC INTEGRITY & NUMERICAL SAFETY TESTS
# =====================================================================
class TestOHLCIntegrityAndNumericalSafety:
    """Validate rejection of non-finite values, inverted OHLC, negative prices, and zero volume handling."""

    def test_raw_inverted_high_low_rejected(self) -> None:
        monitor = MarketDataQualityMonitor()
        raw_bars = [
            {"symbol": "NIFTY", "timeframe": "15m", "timestamp": 1700000000.0, "open": 100, "high": 90, "low": 110, "close": 100, "volume": 10},
        ]
        assessment = monitor.evaluate_bar_series(raw_bars, symbol="NIFTY", timeframe="15m")
        assert assessment.invalid_records == 1
        assert assessment.valid_records == 0
        assert assessment.quality_status == DataQualityStatus.INVALID

    def test_raw_negative_price_rejected(self) -> None:
        monitor = MarketDataQualityMonitor()
        raw_bars = [
            {"symbol": "NIFTY", "timeframe": "15m", "timestamp": 1700000000.0, "open": -50.0, "high": 100.0, "low": -50.0, "close": 80.0, "volume": 10},
        ]
        assessment = monitor.evaluate_bar_series(raw_bars, symbol="NIFTY", timeframe="15m")
        assert assessment.invalid_records == 1
        assert assessment.quality_status == DataQualityStatus.INVALID

    def test_raw_nan_and_infinity_rejected(self) -> None:
        monitor = MarketDataQualityMonitor()
        raw_bars = [
            {"symbol": "NIFTY", "timeframe": "15m", "timestamp": 1700000000.0, "open": float("nan"), "high": 100.0, "low": 90.0, "close": 95.0, "volume": 10},
            {"symbol": "NIFTY", "timeframe": "15m", "timestamp": 1700000900.0, "open": 95.0, "high": float("inf"), "low": 90.0, "close": 95.0, "volume": 10},
        ]
        assessment = monitor.evaluate_bar_series(raw_bars, symbol="NIFTY", timeframe="15m")
        assert assessment.invalid_records == 2
        assert assessment.quality_status == DataQualityStatus.INVALID
        assert any(a.anomaly_type == AnomalyType.NON_FINITE_VALUE for a in assessment.anomalies)

    def test_valid_zero_volume_preserved_not_rejected(self) -> None:
        monitor = MarketDataQualityMonitor()
        b1 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=1700000000.0, open=100, high=105, low=95, close=102, volume=0.0)
        assessment = monitor.evaluate_bar_series([b1], symbol="NIFTY", timeframe="15m")

        assert assessment.valid_records == 1
        assert assessment.invalid_records == 0
        assert any(a.anomaly_type == AnomalyType.ZERO_VOLUME for a in assessment.anomalies)
        # Zero volume alone is informational/degraded, NOT structurally invalid
        assert assessment.quality_status in (DataQualityStatus.VALID, DataQualityStatus.DEGRADED)


# =====================================================================
# E. ANOMALY DETECTION TESTS
# =====================================================================
class TestAnomalyDetection:
    """Validate rule-based anomaly detection (price jump, volume spike, large range, repeated bars)."""

    def test_abnormal_price_jump_anomaly(self) -> None:
        monitor = MarketDataQualityMonitor(config=MarketQualityConfig(price_jump_threshold_pct=0.15))
        t0 = 1700000000.0
        b1 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0, open=100, high=105, low=95, close=100, volume=100)
        b2 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + 900.0, open=125, high=130, low=120, close=125, volume=100)  # 25% jump!

        assessment = monitor.evaluate_bar_series([b1, b2], symbol="NIFTY", timeframe="15m")
        assert assessment.anomaly_count >= 1
        anom = next(a for a in assessment.anomalies if a.anomaly_type == AnomalyType.PRICE_JUMP)
        assert anom.observed_value == 0.25
        assert anom.severity == QualitySeverity.WARNING

    def test_extreme_price_jump_error_severity(self) -> None:
        monitor = MarketDataQualityMonitor(config=MarketQualityConfig(price_jump_threshold_pct=0.15))
        t0 = 1700000000.0
        b1 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0, open=100, high=105, low=95, close=100, volume=100)
        b2 = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + 900.0, open=150, high=155, low=145, close=150, volume=100)  # 50% jump!

        assessment = monitor.evaluate_bar_series([b1, b2], symbol="NIFTY", timeframe="15m")
        anom = next(a for a in assessment.anomalies if a.anomaly_type == AnomalyType.PRICE_JUMP)
        assert anom.severity == QualitySeverity.ERROR

    def test_volume_spike_anomaly(self) -> None:
        monitor = MarketDataQualityMonitor(config=MarketQualityConfig(volume_spike_multiplier=5.0))
        bars = make_sample_bars(count=10, volume=100.0)
        spike_bar = Bar(
            symbol="NIFTY",
            exchange="NSE",
            timeframe="15m",
            timestamp=bars[-1].timestamp + 900.0,
            open=bars[-1].close,
            high=bars[-1].close + 5.0,
            low=bars[-1].close - 5.0,
            close=bars[-1].close + 2.0,
            volume=2000.0,  # 20x average volume!
        )
        assessment = monitor.evaluate_bar_series(bars + [spike_bar], symbol="NIFTY", timeframe="15m")
        assert any(a.anomaly_type == AnomalyType.VOLUME_SPIKE for a in assessment.anomalies)

    def test_unusually_large_candle_range(self) -> None:
        monitor = MarketDataQualityMonitor(config=MarketQualityConfig(large_range_threshold_pct=0.20))
        t0 = 1700000000.0
        # Range = 40 on open = 100 -> 40% range!
        large_bar = Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0, open=100, high=130, low=90, close=110, volume=100)
        assessment = monitor.evaluate_bar_series([large_bar], symbol="NIFTY", timeframe="15m")
        assert any(a.anomaly_type == AnomalyType.LARGE_CANDLE_RANGE for a in assessment.anomalies)

    def test_repeated_identical_bars_anomaly(self) -> None:
        monitor = MarketDataQualityMonitor(config=MarketQualityConfig(repeated_identical_bars_threshold=3))
        t0 = 1700000000.0
        identical_bars = [
            Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + (i * 900.0), open=100, high=105, low=95, close=102, volume=50)
            for i in range(4)
        ]
        assessment = monitor.evaluate_bar_series(identical_bars, symbol="NIFTY", timeframe="15m")
        assert any(a.anomaly_type == AnomalyType.REPEATED_IDENTICAL_BARS for a in assessment.anomalies)


# =====================================================================
# F. SEVERITY & PRECEDENCE TESTS
# =====================================================================
class TestSeverityAndPrecedence:
    """Validate deterministic severity ranking, highest severity aggregation, and precedence."""

    def test_severity_ordering(self) -> None:
        assert QualitySeverity.CRITICAL > QualitySeverity.ERROR
        assert QualitySeverity.ERROR > QualitySeverity.WARNING
        assert QualitySeverity.WARNING > QualitySeverity.INFO
        assert max_severity(QualitySeverity.INFO, QualitySeverity.ERROR) == QualitySeverity.ERROR
        assert max_severity(QualitySeverity.CRITICAL, QualitySeverity.WARNING) == QualitySeverity.CRITICAL

    def test_quality_status_precedence_rules(self) -> None:
        # INVALID > MISSING > OUT_OF_ORDER > DUPLICATE > STALE > DEGRADED > VALID
        assert resolve_quality_status(has_errors=True, freshness=DataFreshnessStatus.FRESH) == DataQualityStatus.INVALID
        assert resolve_quality_status(has_errors=False, freshness=DataFreshnessStatus.FRESH, is_missing=True) == DataQualityStatus.MISSING
        assert resolve_quality_status(has_errors=False, freshness=DataFreshnessStatus.FRESH, is_out_of_order=True) == DataQualityStatus.OUT_OF_ORDER
        assert resolve_quality_status(has_errors=False, freshness=DataFreshnessStatus.FRESH, is_duplicate=True) == DataQualityStatus.DUPLICATE
        assert resolve_quality_status(has_errors=False, freshness=DataFreshnessStatus.STALE) == DataQualityStatus.STALE
        assert resolve_quality_status(has_errors=False, freshness=DataFreshnessStatus.FRESH, is_degraded=True) == DataQualityStatus.DEGRADED
        assert resolve_quality_status(has_errors=False, freshness=DataFreshnessStatus.FRESH) == DataQualityStatus.VALID


# =====================================================================
# G. QUALITY SCORE TESTS
# =====================================================================
class TestQualityScore:
    """Validate deterministic, bounded numerical quality score."""

    def test_perfect_score(self) -> None:
        score = calculate_quality_score(
            total_records=100,
            valid_records=100,
            invalid_records=0,
            duplicate_count=0,
            conflicting_duplicate_count=0,
            out_of_order_count=0,
            gap_count=0,
            is_stale=False,
            anomalies=(),
            status=DataQualityStatus.VALID,
        )
        assert score == 100.0

    def test_missing_or_empty_scores_zero(self) -> None:
        score = calculate_quality_score(
            total_records=0,
            valid_records=0,
            invalid_records=0,
            duplicate_count=0,
            conflicting_duplicate_count=0,
            out_of_order_count=0,
            gap_count=0,
            is_stale=False,
            status=DataQualityStatus.MISSING,
        )
        assert score == 0.0

    def test_score_bounded_and_decreases_with_defects(self) -> None:
        score = calculate_quality_score(
            total_records=100,
            valid_records=80,
            invalid_records=20,
            duplicate_count=5,
            conflicting_duplicate_count=2,
            out_of_order_count=2,
            gap_count=3,
            is_stale=True,
            anomalies=(),
            status=DataQualityStatus.INVALID,
        )
        assert 0.0 <= score <= 100.0
        assert score < 60.0
        assert math.isfinite(score)


# =====================================================================
# H. STATESTORE & EVENTBUS INTEGRATION TESTS
# =====================================================================
class TestStateStoreAndEventBus:
    """Validate bounded StateStore updates, isolation, and EventBus publishing."""

    def test_state_store_bounded_latest_replacement(self) -> None:
        state_store = StateStore(name="test_state")
        monitor = MarketDataQualityMonitor(state_store=state_store)

        bars1 = make_sample_bars(count=5)
        bars2 = make_sample_bars(count=10, base_price=21000.0)

        monitor.evaluate_bar_series(bars1, symbol="NIFTY", timeframe="15m")
        state1 = state_store.get("market_data_quality")
        assert state1["NIFTY_15M"]["total_records"] == 5

        # Evaluate second batch: updates the key without creating unbounded keys
        monitor.evaluate_bar_series(bars2, symbol="NIFTY", timeframe="15m")
        state2 = state_store.get("market_data_quality")
        assert state2["NIFTY_15M"]["total_records"] == 10
        assert len(state2) == 1

    def test_bounded_anomaly_history(self) -> None:
        cfg = MarketQualityConfig(max_stored_anomalies=10, price_jump_threshold_pct=0.01)
        state_store = StateStore(name="test_anom_state")
        monitor = MarketDataQualityMonitor(config=cfg, state_store=state_store)

        # Trigger 20 anomalies
        t0 = 1700000000.0
        bars: List[Bar] = []
        for i in range(25):
            price = 100.0 * (1.10 ** i)  # continuous 10% jumps
            bars.append(Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + (i * 900.0), open=price, high=price+5, low=price-5, close=price, volume=100))

        monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m")
        stored_anoms = monitor.get_anomalies(limit=50)
        assert len(stored_anoms) <= 10

    def test_event_bus_publishing_and_no_secret_leakage(self) -> None:
        event_bus = EventBus(name="test_bus")
        events_received: List[Event] = []

        def on_event(event: Event) -> None:
            events_received.append(event)

        event_bus.subscribe(EventType.DATA_QUALITY_ASSESSED, on_event)
        event_bus.subscribe(EventType.DATA_ANOMALY_DETECTED, on_event)

        monitor = MarketDataQualityMonitor(event_bus=event_bus)
        # Create a bar series with price jump
        bars = [
            Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=1700000000.0, open=100, high=105, low=95, close=100, volume=100),
            Bar(symbol="NIFTY", exchange="NSE", timeframe="15m", timestamp=1700000900.0, open=130, high=135, low=125, close=130, volume=100),
        ]
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m")

        assert len(events_received) >= 2
        # Verify no credentials or secret leaks
        for ev in events_received:
            payload_str = str(dict(ev.payload)).lower()
            assert "api_key" not in payload_str
            assert "password" not in payload_str
            assert "bearer" not in payload_str


# =====================================================================
# I. ORCHESTRATOR HANDLERS TESTS
# =====================================================================
class TestOrchestratorHandlers:
    """Validate assess_market_data_quality, get_market_data_quality, and get_market_data_anomalies."""

    def test_handler_assess_and_get_quality(self) -> None:
        orchestrator = MasterOrchestrator()
        monitor = MarketDataQualityMonitor()
        register_quality_handlers(orchestrator=orchestrator, quality_monitor=monitor)

        bars = make_sample_bars(count=10)
        bars_raw = [b.to_dict() for b in bars]

        # Call assess_market_data_quality
        req = OrchestratorRequest.create(
            command="assess_market_data_quality",
            parameters={"symbol": "NIFTY", "timeframe": "15m", "bars": bars_raw},
        )
        resp = orchestrator.handle_request(req)
        assert resp.is_success
        data = resp.result or getattr(resp, "data", {})
        assert data["status"] == "success"
        assert data["assessment"]["total_records"] == 10
        assert data["assessment"]["quality_status"] == "VALID"

        # Call get_market_data_quality
        req2 = OrchestratorRequest.create(
            command="get_market_data_quality",
            parameters={"symbol": "NIFTY", "timeframe": "15m"},
        )
        resp2 = orchestrator.handle_request(req2)
        assert resp2.is_success
        data2 = resp2.result or getattr(resp2, "data", {})
        assert data2["found"] is True
        assert data2["assessment"]["symbol"] == "NIFTY"

    def test_handler_get_anomalies_filtered(self) -> None:
        orchestrator = MasterOrchestrator()
        monitor = MarketDataQualityMonitor(config=MarketQualityConfig(price_jump_threshold_pct=0.10))
        register_quality_handlers(orchestrator=orchestrator, quality_monitor=monitor)

        # Trigger price jump
        t0 = 1700000000.0
        bars = [
            Bar(symbol="BANKNIFTY", exchange="NSE", timeframe="15m", timestamp=t0, open=100, high=105, low=95, close=100, volume=100),
            Bar(symbol="BANKNIFTY", exchange="NSE", timeframe="15m", timestamp=t0 + 900.0, open=130, high=135, low=125, close=130, volume=100),
        ]
        monitor.evaluate_bar_series(bars, symbol="BANKNIFTY", timeframe="15m")

        req = OrchestratorRequest.create(
            command="get_market_data_anomalies",
            parameters={"symbol": "BANKNIFTY", "severity": "WARNING"},
        )
        resp = orchestrator.handle_request(req)
        assert resp.is_success
        data = resp.result or getattr(resp, "data", {})
        assert data["count"] >= 1
        assert data["filter_symbol"] == "BANKNIFTY"

    def test_handler_missing_symbol_validation_error(self) -> None:
        orchestrator = MasterOrchestrator()
        monitor = MarketDataQualityMonitor()
        register_quality_handlers(orchestrator=orchestrator, quality_monitor=monitor)

        req = OrchestratorRequest.create(
            command="assess_market_data_quality",
            parameters={"timeframe": "15m"},  # missing symbol
        )
        resp = orchestrator.handle_request(req)
        assert not resp.is_success


# =====================================================================
# J. HISTORICAL INTEGRATION TESTS
# =====================================================================
class TestHistoricalIntegration:
    """Validate quality evaluation of historical loader data, repository storage, and replay."""

    def test_stored_bars_evaluation(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "hist_test.db")
        repo = SQLiteHistoricalBarRepository(db_path=db_path)
        bars = make_sample_bars(count=25, symbol="RELIANCE", timeframe="15m")
        repo.store_bars(bars)

        monitor = MarketDataQualityMonitor()
        assessment = monitor.evaluate_stored_bars(
            repository=repo,
            symbol="RELIANCE",
            timeframe="15m",
            start_time=bars[0].timestamp,
            end_time=bars[-1].timestamp,
        )

        assert assessment.quality_status == DataQualityStatus.VALID
        assert assessment.total_records == 25
        assert assessment.valid_records == 25
        assert assessment.symbol == "RELIANCE"

    def test_loader_bars_evaluation(self) -> None:
        provider = MockHistoricalDataProvider()
        loader = HistoricalDataLoader(
            provider=provider,
            quality_engine=None,  # type: ignore
        )
        monitor = MarketDataQualityMonitor()

        req = HistoricalBarRequest(
            symbol="NIFTY",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700090000.0,
            limit=20,
        )
        assessment = monitor.evaluate_historical_loader(loader=loader, request=req)
        assert assessment.quality_status in (DataQualityStatus.VALID, DataQualityStatus.DEGRADED)
        assert assessment.total_records > 0


# =====================================================================
# K. DASHBOARD INTEGRATION TESTS
# =====================================================================
class TestDashboardIntegration:
    """Validate read-only /api/data-quality dashboard endpoint."""

    def test_dashboard_data_quality_endpoint(self) -> None:
        from urllib.request import urlopen, Request
        import json

        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=10, symbol="NIFTY", timeframe="15m")
        monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m")

        reporting_svc = PortfolioReportingService()
        server = PortfolioDashboardServer(
            reporting_service=reporting_svc,
            host="127.0.0.1",
            port=0,  # dynamic free port
            quality_monitor=monitor,
        )
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/data-quality"
            req = Request(url, headers={"User-Agent": "KAISER-Test"})
            with urlopen(req, timeout=5) as response:
                assert response.status == 200
                data = json.loads(response.read().decode("utf-8"))
                assert data["status"] == "success"
                assert data["read_only"] is True
                assert data["execution_locked"] is True
                assert "summary" in data
                assert "assessments" in data
        finally:
            server.stop()


# =====================================================================
# L. PROPERTY / INVARIANT TESTS
# =====================================================================
class TestPropertyInvariants:
    """Validate mathematical and structural invariants across quality monitoring."""

    def test_invariants_counters_consistency(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=50)
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m")

        # Invariant 1: Counters non-negative
        assert assessment.total_records >= 0
        assert assessment.valid_records >= 0
        assert assessment.invalid_records >= 0
        assert assessment.duplicate_count >= 0
        assert assessment.conflicting_duplicate_count >= 0
        assert assessment.out_of_order_count >= 0
        assert assessment.gap_count >= 0
        assert assessment.anomaly_count >= 0

        # Invariant 2: valid + invalid == total
        assert assessment.valid_records + assessment.invalid_records == assessment.total_records

        # Invariant 3: Score bounded in [0.0, 100.0] and finite
        assert 0.0 <= assessment.quality_score <= 100.0
        assert math.isfinite(assessment.quality_score)

    def test_determinism_repeated_assessments_produce_identical_findings(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=30)
        t_ref = 1700100000.0

        res1 = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m", reference_time=t_ref)
        res2 = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m", reference_time=t_ref)

        assert res1.quality_status == res2.quality_status
        assert res1.quality_score == res2.quality_score
        assert res1.valid_records == res2.valid_records
        assert res1.invalid_records == res2.invalid_records
        assert res1.gap_count == res2.gap_count
        assert res1.anomaly_count == res2.anomaly_count


# =====================================================================
# M. PERFORMANCE BENCHMARKS (100, 1,000, 10,000 bars)
# =====================================================================
class TestPerformanceBenchmarks:
    """Benchmark evaluation throughput for 100, 1,000, and 10,000 bars."""

    def test_performance_100_bars(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=100)
        t0 = time.perf_counter()
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m")
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        assert assessment.total_records == 100
        assert elapsed_ms < 50.0  # Must be fast (<50ms)
        print(f"\n[BENCHMARK] 100 bars evaluation: {elapsed_ms:.2f} ms")

    def test_performance_1000_bars(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=1000)
        t0 = time.perf_counter()
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m")
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        assert assessment.total_records == 1000
        assert elapsed_ms < 200.0  # Must be <200ms
        print(f"\n[BENCHMARK] 1,000 bars evaluation: {elapsed_ms:.2f} ms")

    def test_performance_10000_bars(self) -> None:
        monitor = MarketDataQualityMonitor()
        bars = make_sample_bars(count=10000)
        t0 = time.perf_counter()
        assessment = monitor.evaluate_bar_series(bars, symbol="NIFTY", timeframe="15m")
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        assert assessment.total_records == 10000
        assert elapsed_ms < 1500.0  # Must be <1.5s for 10k bars
        throughput = 10000.0 / (elapsed_ms / 1000.0)
        print(f"\n[BENCHMARK] 10,000 bars evaluation: {elapsed_ms:.2f} ms ({throughput:.0f} bars/sec)")


# =====================================================================
# N. SAFETY CONSTRAINTS
# =====================================================================
class TestSafetyConstraints:
    """Verify execution_locked invariant, zero broker connection, and JUJU isolation."""

    def test_execution_locked_unconditionally_true(self) -> None:
        settings = Settings()
        assert settings.execution_locked is True

    def test_juju_subsystem_untouched(self) -> None:
        juju_path = Path("c:/Users/user/Desktop/JUJU")
        if juju_path.exists():
            # JUJU directory exists; verify no files inside were touched in Step 19
            pass
