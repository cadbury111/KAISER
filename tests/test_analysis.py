"""KAISER — Phase 2, Step 01: Technical & Quantitative Analysis Engine Tests.

Deterministic verification suite covering:
- Mathematical accuracy of core indicators (SMA, EMA, RSI, MACD, Bollinger Bands, ATR)
- Edge cases (flat markets, monotonic trends, zero variance, division by zero)
- Insufficient data handling and graceful degradation
- Bounded RollingBarSeries capacity enforcement
- Market Data Quality Gate enforcement (rejecting unsafe / quarantined bars)
- EventBus event publishing and StateStore state caching
- MasterOrchestrator command handler routing and voice-ready summaries
- Bootstrap lifecycle integration
- Trading safety invariants (execution_locked, uninitialized trading engine)
"""

from __future__ import annotations

import math
import threading
import time
from typing import List, Optional

import pytest

from app.analysis import (
    AnalysisConfig,
    IndicatorType,
    IndicatorValue,
    RollingBarSeries,
    SignalDirection,
    TechnicalAnalysisEngine,
    TechnicalFeatureVector,
    calculate_atr,
    calculate_bollinger_bands,
    calculate_ema,
    calculate_ema_series,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
    calculate_true_range,
    classify_bollinger,
    classify_macd,
    classify_rsi,
    register_analysis_handlers,
)
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.models import Bar
from app.orchestrator import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, RequestStatus


def _create_bar(
    symbol: str = "RELIANCE",
    timeframe: str = "1m",
    timestamp: float = 1700000000.0,
    open_p: float = 100.0,
    high_p: Optional[float] = None,
    low_p: Optional[float] = None,
    close_p: float = 102.0,
    volume: float = 1000.0,
    source: str = "mock",
) -> Bar:
    """Helper to construct valid Bar records with preserved OHLC relations."""
    resolved_high = high_p if high_p is not None else max(open_p, close_p) + 5.0
    resolved_low = low_p if low_p is not None else min(open_p, close_p) - 5.0
    resolved_high = max(resolved_high, open_p, close_p)
    resolved_low = max(0.0, min(resolved_low, open_p, close_p))

    return Bar(
        symbol=symbol,
        exchange="NSE",
        timeframe=timeframe,
        timestamp=timestamp,
        open=open_p,
        high=resolved_high,
        low=resolved_low,
        close=close_p,
        volume=volume,
        source=source,
    )


# =============================================================================
# 1. Pure Indicator Calculations & Mathematical Accuracy
# =============================================================================

class TestPureIndicatorCalculations:
    """Mathematical verification of pure indicator formulas against reference calculations."""

    def test_sma_calculation_accuracy(self):
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        # SMA of last 3: (12 + 13 + 14) / 3 = 13.0
        assert calculate_sma(prices, period=3) == 13.0
        # SMA of last 5: (10 + 11 + 12 + 13 + 14) / 5 = 12.0
        assert calculate_sma(prices, period=5) == 12.0

    def test_sma_insufficient_data(self):
        prices = [10.0, 11.0]
        assert calculate_sma(prices, period=3) is None
        assert calculate_sma([], period=5) is None
        assert calculate_sma([10.0], period=0) is None

    def test_ema_calculation_accuracy(self):
        # 5 prices, period=3, smoothing=2
        # alpha = 2 / (3 + 1) = 0.5
        # seed = SMA(10, 11, 12) = 11.0
        # step 1 (price 13): (13 - 11) * 0.5 + 11 = 12.0
        # step 2 (price 14): (14 - 12) * 0.5 + 12 = 13.0
        prices = [10.0, 11.0, 12.0, 13.0, 14.0]
        series = calculate_ema_series(prices, period=3, smoothing=2.0)
        assert series == [11.0, 12.0, 13.0]
        assert calculate_ema(prices, period=3) == 13.0

    def test_ema_insufficient_data(self):
        assert calculate_ema([10.0, 11.0], period=3) is None
        assert calculate_ema_series([10.0], period=3) == []

    def test_rsi_all_gains(self):
        # Monotonically increasing prices
        prices = [float(100 + i * 2) for i in range(20)]
        rsi = calculate_rsi(prices, period=14)
        assert rsi == 100.0

    def test_rsi_all_losses(self):
        # Monotonically decreasing prices
        prices = [float(200 - i * 2) for i in range(20)]
        rsi = calculate_rsi(prices, period=14)
        assert rsi == 0.0

    def test_rsi_flat_market(self):
        # Completely constant prices (no change)
        prices = [100.0] * 20
        rsi = calculate_rsi(prices, period=14)
        assert rsi == 50.0

    def test_rsi_standard_oscillating_series(self):
        # Oscillating prices around 100
        prices = [
            100.0, 102.0, 101.0, 103.0, 102.5, 104.0, 103.0, 105.0,
            104.5, 106.0, 105.0, 107.0, 106.0, 108.0, 107.0, 109.0,
        ]
        rsi = calculate_rsi(prices, period=14)
        assert rsi is not None
        assert 0.0 <= rsi <= 100.0
        assert rsi > 50.0  # Overall upward trend

    def test_rsi_insufficient_data(self):
        prices = [100.0, 102.0, 101.0]
        assert calculate_rsi(prices, period=14) is None
        assert calculate_rsi([], period=14) is None

    def test_macd_calculation_accuracy(self):
        # 40 sequential prices to give ample history for fast=12, slow=26, signal=9
        prices = [100.0 + (i * 0.5) for i in range(40)]
        macd_res = calculate_macd(prices, fast_period=12, slow_period=26, signal_period=9)
        assert macd_res is not None
        assert "macd" in macd_res
        assert "signal" in macd_res
        assert "histogram" in macd_res
        # In a steady linear uptrend, Fast EMA > Slow EMA so MACD line > 0
        assert macd_res["macd"] > 0
        # Histogram = MACD - Signal
        assert round(macd_res["macd"] - macd_res["signal"], 4) == macd_res["histogram"]

    def test_macd_invalid_parameters_and_insufficient_data(self):
        prices = [100.0 + i for i in range(10)]
        # Insufficient data
        assert calculate_macd(prices, fast_period=12, slow_period=26, signal_period=9) is None
        # Fast >= slow
        assert calculate_macd(prices, fast_period=26, slow_period=12, signal_period=9) is None
        # Negative period
        assert calculate_macd(prices, fast_period=-5, slow_period=26) is None

    def test_bollinger_bands_accuracy(self):
        # 20 prices with known mean and variance
        prices = [10.0] * 10 + [20.0] * 10
        # Mean = 15.0
        # Variance = (10*(10-15)^2 + 10*(20-15)^2)/20 = (250 + 250)/20 = 25.0
        # Std Dev = 5.0
        # Upper = 15 + 2*5 = 25.0
        # Lower = 15 - 2*5 = 5.0
        # Bandwidth = (25 - 5) / 15 = 20 / 15 = 1.3333
        # %B for last price 20.0 = (20 - 5) / (25 - 5) = 15 / 20 = 0.75
        bb = calculate_bollinger_bands(prices, period=20, num_std=2.0)
        assert bb is not None
        assert bb["middle"] == 15.0
        assert bb["upper"] == 25.0
        assert bb["lower"] == 5.0
        assert bb["bandwidth"] == 1.3333
        assert bb["percent_b"] == 0.75

    def test_bollinger_bands_zero_variance(self):
        # Constant prices
        prices = [50.0] * 20
        bb = calculate_bollinger_bands(prices, period=20, num_std=2.0)
        assert bb is not None
        assert bb["middle"] == 50.0
        assert bb["upper"] == 50.0
        assert bb["lower"] == 50.0
        assert bb["bandwidth"] == 0.0
        assert bb["percent_b"] == 0.5

    def test_bollinger_bands_insufficient_data(self):
        prices = [10.0] * 10
        assert calculate_bollinger_bands(prices, period=20) is None

    def test_true_range_calculation(self):
        # Case 1: High - Low dominates
        tr1 = calculate_true_range(high=110.0, low=90.0, prev_close=100.0)
        assert tr1 == 20.0

        # Case 2: Gap up, High - PrevClose dominates
        tr2 = calculate_true_range(high=125.0, low=115.0, prev_close=100.0)
        assert tr2 == 25.0

        # Case 3: Gap down, PrevClose - Low dominates
        tr3 = calculate_true_range(high=85.0, low=75.0, prev_close=100.0)
        assert tr3 == 25.0

        # Case 4: No previous close
        tr4 = calculate_true_range(high=105.0, low=95.0, prev_close=None)
        assert tr4 == 10.0

    def test_atr_calculation_accuracy(self):
        bars = [
            _create_bar(timestamp=1000.0 + i, high_p=105.0, low_p=95.0, close_p=100.0)
            for i in range(25)
        ]
        # Constant TR = 10.0 across all bars
        atr = calculate_atr(bars, period=14)
        assert atr == 10.0

    def test_atr_insufficient_data(self):
        bars = [_create_bar(timestamp=1000.0 + i) for i in range(10)]
        assert calculate_atr(bars, period=14) is None


# =============================================================================
# 2. Signal Classification Helpers
# =============================================================================

class TestSignalClassification:
    """Verification of categorical signal classifiers."""

    def test_rsi_classification(self):
        assert classify_rsi(75.0) == SignalDirection.OVERBOUGHT
        assert classify_rsi(70.0) == SignalDirection.OVERBOUGHT
        assert classify_rsi(25.0) == SignalDirection.OVERSOLD
        assert classify_rsi(30.0) == SignalDirection.OVERSOLD
        assert classify_rsi(50.0) == SignalDirection.NEUTRAL
        assert classify_rsi(None) == SignalDirection.NEUTRAL

    def test_macd_classification(self):
        assert classify_macd({"macd": 1.5, "signal": 1.0, "histogram": 0.5}) == SignalDirection.BULLISH
        assert classify_macd({"macd": -1.5, "signal": -1.0, "histogram": -0.5}) == SignalDirection.BEARISH
        assert classify_macd({"macd": 1.0, "signal": 1.0, "histogram": 0.0}) == SignalDirection.NEUTRAL
        assert classify_macd(None) == SignalDirection.NEUTRAL

    def test_bollinger_classification(self):
        bands = {"upper": 110.0, "middle": 100.0, "lower": 90.0}
        assert classify_bollinger(115.0, bands) == SignalDirection.OVERBOUGHT
        assert classify_bollinger(85.0, bands) == SignalDirection.OVERSOLD
        assert classify_bollinger(105.0, bands) == SignalDirection.BULLISH
        assert classify_bollinger(95.0, bands) == SignalDirection.BEARISH
        assert classify_bollinger(100.0, None) == SignalDirection.NEUTRAL


# =============================================================================
# 3. RollingBarSeries Bounded Container
# =============================================================================

class TestRollingBarSeries:
    """Verification of thread-safe, bounded rolling window series."""

    def test_series_creation_and_validation(self):
        series = RollingBarSeries(symbol="nifty", timeframe="5m", max_capacity=100)
        assert series.symbol == "NIFTY"
        assert series.timeframe == "5m"
        assert series.max_capacity == 100
        assert series.count == 0

        with pytest.raises(ValidationError):
            RollingBarSeries(symbol="", timeframe="1m")
        with pytest.raises(ValidationError):
            RollingBarSeries(symbol="NIFTY", timeframe="", max_capacity=100)
        with pytest.raises(ValidationError):
            RollingBarSeries(symbol="NIFTY", timeframe="1m", max_capacity=5)

    def test_series_in_order_addition(self):
        series = RollingBarSeries(symbol="INFY", timeframe="1m", max_capacity=50)
        b1 = _create_bar(symbol="INFY", timeframe="1m", timestamp=1000.0, close_p=1500.0)
        b2 = _create_bar(symbol="INFY", timeframe="1m", timestamp=1060.0, close_p=1510.0)

        assert series.add_bar(b1) is True
        assert series.add_bar(b2) is True
        assert series.count == 2
        assert series.latest_bar == b2
        assert series.get_closes() == [1500.0, 1510.0]

    def test_series_rejects_duplicate_and_out_of_order(self):
        series = RollingBarSeries(symbol="INFY", timeframe="1m", max_capacity=50)
        b1 = _create_bar(symbol="INFY", timeframe="1m", timestamp=1000.0)
        b_dup = _create_bar(symbol="INFY", timeframe="1m", timestamp=1000.0)
        b_old = _create_bar(symbol="INFY", timeframe="1m", timestamp=990.0)

        assert series.add_bar(b1) is True
        assert series.add_bar(b_dup) is False
        assert series.add_bar(b_old) is False
        assert series.count == 1

    def test_series_rejects_mismatched_stream(self):
        series = RollingBarSeries(symbol="INFY", timeframe="1m", max_capacity=50)
        b_diff_sym = _create_bar(symbol="TCS", timeframe="1m", timestamp=1000.0)
        b_diff_tf = _create_bar(symbol="INFY", timeframe="5m", timestamp=1000.0)

        assert series.add_bar(b_diff_sym) is False
        assert series.add_bar(b_diff_tf) is False
        assert series.count == 0

    def test_series_capacity_bounding(self):
        series = RollingBarSeries(symbol="INFY", timeframe="1m", max_capacity=25)
        for i in range(40):
            b = _create_bar(symbol="INFY", timeframe="1m", timestamp=1000.0 + i, close_p=100.0 + i)
            series.add_bar(b)

        assert series.count == 25
        closes = series.get_closes()
        assert len(closes) == 25
        assert closes[0] == 115.0  # Oldest retained is 100 + 15
        assert closes[-1] == 139.0


# =============================================================================
# 4. TechnicalAnalysisEngine & Quality Gate
# =============================================================================

class TestTechnicalAnalysisEngine:
    """Verification of the central technical analysis engine."""

    def test_engine_quality_gate_rejects_unsafe_bars(self):
        engine = TechnicalAnalysisEngine()
        bar = _create_bar(symbol="RELIANCE", timestamp=1000.0)

        # Bar flagged as unsafe by Quality Engine
        result = engine.add_bar(bar, safe_to_consume=False)
        assert result is None

        counters = engine.get_counters()
        assert counters["bars_rejected_unhealthy"] == 1
        assert counters["bars_processed"] == 0
        assert engine.get_history("RELIANCE") == []

    def test_engine_add_bar_produces_feature_vector(self):
        engine = TechnicalAnalysisEngine()
        vector = None
        for i in range(30):
            bar = _create_bar(
                symbol="RELIANCE",
                timestamp=1000.0 + i * 60,
                open_p=100.0 + i,
                high_p=105.0 + i,
                low_p=95.0 + i,
                close_p=102.0 + i,
            )
            vector = engine.add_bar(bar, safe_to_consume=True)

        assert vector is not None
        assert vector.symbol == "RELIANCE"
        assert vector.bar_count == 30
        assert vector.last_close == 102.0 + 29
        # Check standard indicators are present
        assert "SMA_20" in vector.indicators
        assert vector.indicators["SMA_20"].is_valid is True
        assert vector.indicators["SMA_20"].value is not None

        assert "RSI_14" in vector.indicators
        assert vector.indicators["RSI_14"].is_valid is True

        assert "BB_20_2.0" in vector.indicators
        assert vector.indicators["BB_20_2.0"].is_valid is True

        assert "ATR_14" in vector.indicators
        assert vector.indicators["ATR_14"].is_valid is True

        # Telemetry check
        counters = engine.get_counters()
        assert counters["bars_processed"] == 30
        assert counters["bars_rejected_unhealthy"] == 0
        assert counters["feature_vectors_computed"] == 30

    def test_engine_insufficient_bars_marks_indicators_invalid(self):
        engine = TechnicalAnalysisEngine()
        bar = _create_bar(symbol="TCS", timestamp=1000.0, close_p=3500.0)
        vector = engine.add_bar(bar, safe_to_consume=True)

        assert vector is not None
        # Only 1 bar, so 20-period SMA should be invalid
        sma20 = vector.indicators.get("SMA_20")
        assert sma20 is not None
        assert sma20.is_valid is False
        assert sma20.value is None

    def test_engine_state_store_and_event_bus_integration(self):
        eb = EventBus("test_bus")
        ss = StateStore()
        engine = TechnicalAnalysisEngine(event_bus=eb, state_store=ss)

        events_received: List[Event] = []
        eb.subscribe(EventType.ANALYSIS_FEATURE_VECTOR_READY, lambda e: events_received.append(e))

        for i in range(25):
            bar = _create_bar(symbol="HDFCBANK", timestamp=1000.0 + i * 60, close_p=1600.0 + i)
            engine.add_bar(bar, safe_to_consume=True, correlation_id="test_cid_123")

        # Check EventBus
        assert len(events_received) == 25
        assert events_received[-1].event_type == EventType.ANALYSIS_FEATURE_VECTOR_READY
        assert events_received[-1].correlation_id == "test_cid_123"
        assert events_received[-1].payload["symbol"] == "HDFCBANK"

        # Check StateStore
        stored_analysis = ss.get("technical_analysis")
        assert stored_analysis is not None
        assert "HDFCBANK_1m" in stored_analysis
        assert stored_analysis["HDFCBANK_1m"]["bar_count"] == 25

        stored_counters = ss.get("analysis_counters")
        assert stored_counters["bars_processed"] == 25

    def test_engine_query_and_reset(self):
        engine = TechnicalAnalysisEngine()
        for i in range(25):
            engine.add_bar(
                _create_bar(symbol="INFY", timeframe="1m", timestamp=1000.0 + i, close_p=1500.0 + i)
            )

        # Query methods
        vec = engine.get_feature_vector("INFY", "1m")
        assert vec is not None
        assert vec.symbol == "INFY"

        sma = engine.get_indicator("INFY", "SMA_20", "1m")
        assert sma is not None
        assert sma.is_valid is True

        history = engine.get_history("INFY", "1m")
        assert len(history) == 25

        # Reset specific symbol
        engine.reset(symbol="INFY", timeframe="1m")
        assert engine.get_feature_vector("INFY", "1m") is None
        assert engine.get_history("INFY", "1m") == []

    def test_engine_thread_safety_under_concurrent_ingestion(self):
        engine = TechnicalAnalysisEngine()
        errors: List[Exception] = []

        def worker(sym: str, offset: float):
            try:
                for i in range(50):
                    b = _create_bar(
                        symbol=sym,
                        timestamp=1000.0 + i * 10 + offset,
                        close_p=100.0 + i,
                    )
                    engine.add_bar(b, safe_to_consume=True)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=worker, args=("SYM_A", 0.0)),
            threading.Thread(target=worker, args=("SYM_B", 0.0)),
            threading.Thread(target=worker, args=("SYM_C", 0.0)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert engine.get_counters()["bars_processed"] == 150
        fv_a = engine.get_feature_vector("SYM_A")
        assert fv_a is not None and fv_a.bar_count == 50
        fv_b = engine.get_feature_vector("SYM_B")
        assert fv_b is not None and fv_b.bar_count == 50
        fv_c = engine.get_feature_vector("SYM_C")
        assert fv_c is not None and fv_c.bar_count == 50


# =============================================================================
# 5. Master Orchestrator Command Handlers
# =============================================================================

class TestAnalysisOrchestratorHandlers:
    """Verification of technical analysis commands routed through MasterOrchestrator."""

    def test_orchestrator_technical_analysis_command_success(self):
        orchestrator = MasterOrchestrator(name="test_orchestrator")
        engine = TechnicalAnalysisEngine()
        register_analysis_handlers(orchestrator, engine)

        # Populate bars
        for i in range(25):
            engine.add_bar(_create_bar(symbol="RELIANCE", timestamp=1000.0 + i, close_p=2500.0 + i))

        req = OrchestratorRequest.create(
            command="technical_analysis",
            context={"symbol": "RELIANCE", "timeframe": "1m"},
        )
        resp = orchestrator.handle_request(req)

        assert resp.status == RequestStatus.COMPLETED
        result = resp.result
        assert result["status"] == "success"
        assert result["symbol"] == "RELIANCE"
        assert result["last_close"] == 2500.0 + 24
        assert "Technical analysis for RELIANCE" in result["message"]

    def test_orchestrator_technical_analysis_command_missing_symbol(self):
        orchestrator = MasterOrchestrator(name="test_orchestrator")
        engine = TechnicalAnalysisEngine()
        register_analysis_handlers(orchestrator, engine)

        req = OrchestratorRequest.create(command="technical_analysis", context={})
        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.FAILED
        assert any("symbol" in str(err).lower() for err in resp.errors)

    def test_orchestrator_technical_analysis_command_no_data_available(self):
        orchestrator = MasterOrchestrator(name="test_orchestrator")
        engine = TechnicalAnalysisEngine()
        register_analysis_handlers(orchestrator, engine)

        req = OrchestratorRequest.create(
            command="technical_analysis",
            context={"symbol": "UNKNOWN"},
        )
        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.COMPLETED
        assert resp.result["status"] == "unavailable"
        assert "No technical analysis available" in resp.result["message"]

    def test_orchestrator_calculate_indicator_command_success(self):
        orchestrator = MasterOrchestrator(name="test_orchestrator")
        engine = TechnicalAnalysisEngine()
        register_analysis_handlers(orchestrator, engine)

        for i in range(25):
            engine.add_bar(_create_bar(symbol="TCS", timestamp=1000.0 + i, close_p=3600.0 + i))

        req = OrchestratorRequest.create(
            command="calculate_indicator",
            context={"symbol": "TCS", "indicator": "SMA_20"},
        )
        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.COMPLETED
        assert resp.result["status"] == "success"
        assert resp.result["indicator"] == "SMA_20"
        assert resp.result["value"] is not None
        assert "SMA_20 for TCS is" in resp.result["message"]

    def test_orchestrator_calculate_indicator_command_not_found(self):
        orchestrator = MasterOrchestrator(name="test_orchestrator")
        engine = TechnicalAnalysisEngine()
        register_analysis_handlers(orchestrator, engine)

        for i in range(25):
            engine.add_bar(_create_bar(symbol="TCS", timestamp=1000.0 + i))

        req = OrchestratorRequest.create(
            command="calculate_indicator",
            context={"symbol": "TCS", "indicator": "NON_EXISTENT_IND"},
        )
        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.COMPLETED
        assert resp.result["status"] == "not_found"


# =============================================================================
# 6. Bootstrap Lifecycle Integration
# =============================================================================

class TestBootstrapAnalysisIntegration:
    """Verification of TechnicalAnalysisEngine integration during bootstrap."""

    def test_bootstrap_initializes_analysis_engine(self):
        status = bootstrap_system(Settings(app_env="test"), log_to_file=False)
        assert status.initialized is True
        assert status.analysis_engine is not None
        assert status.analysis_engine.name == "kaiser_main_analysis_engine"

    def test_bootstrap_bar_event_auto_ingestion(self):
        status = bootstrap_system(Settings(app_env="test"), log_to_file=False)
        market_service = status.market_service
        analysis_engine = status.analysis_engine

        assert market_service is not None
        assert analysis_engine is not None

        # Ingest a bar through MarketDataService
        bar = _create_bar(symbol="RELIANCE", timestamp=1700000000.0, close_p=2500.0)
        market_service.ingest_bar(bar)

        # Bar should have auto-propagated via MARKET_BAR_UPDATED to analysis_engine
        history = analysis_engine.get_history("RELIANCE")
        assert len(history) == 1
        assert history[0].symbol == "RELIANCE"
        assert history[0].close == 2500.0


# =============================================================================
# 7. Trading Safety Invariants
# =============================================================================

class TestTradingSafetyInvariants:
    """Verification of non-negotiable safety guardrails."""

    def test_execution_engine_locked_and_trading_engine_uninitialized(self):
        status = bootstrap_system(Settings(app_env="test"), log_to_file=False)
        assert status.settings.execution_locked is True
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"

    def test_analysis_calculation_under_extreme_volatility_does_not_crash(self):
        engine = TechnicalAnalysisEngine()
        # Wild prices (penny stock spikes, near zero, large numbers)
        extreme_prices = [0.001, 10000.0, 0.05, 500000.0, 0.0001, 100.0] * 5
        for i, p in enumerate(extreme_prices):
            bar = _create_bar(
                symbol="VOLATILE",
                timestamp=1000.0 + i,
                open_p=p,
                high_p=max(p * 1.5, p + 1.0),
                low_p=min(p * 0.5, p - 0.00001),
                close_p=p,
                volume=0.0,
            )
            vec = engine.add_bar(bar, safe_to_consume=True)
            assert vec is not None

        assert engine.get_counters()["bars_processed"] == len(extreme_prices)
