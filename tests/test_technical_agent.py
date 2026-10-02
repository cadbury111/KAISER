"""KAISER — Phase 2, Step 02: Multi-Timeframe Synchronization & Technical Analysis Agent Tests.

Deterministic verification suite covering:
1. Multi-Timeframe Synchronization across 1m, 5m, 15m, 1h, 1D
2. Symbol and Timeframe Stream Isolation
3. Missing intervals (gaps), stale bars, duplicates, out-of-order, and quarantined bars
4. Insufficient indicator history and graceful degradation
5. Intra-timeframe indicator conflicts and evidence generation
6. Cross-timeframe agreement, disagreement, and mixed conditions
7. Partial timeframe availability and confidence reduction
8. Structured Intelligence Report verification (facts vs interpretations, explicit uncertainty)
9. MasterOrchestrator multi_timeframe_analysis command routing and voice summary
10. Trading safety invariants (execution_locked, uninitialized trading engine)
"""

from __future__ import annotations

import time
from typing import List, Optional

import pytest

from app.agents import (
    BaseAgent,
    ConfidenceLevel,
    CrossTimeframeAgreement,
    SignalCondition,
    TechnicalAnalysisAgent,
    TechnicalIntelligenceReport,
    TimeframeSignalReport,
    register_technical_agent_handlers,
)
from app.analysis import (
    SUPPORTED_TIMEFRAMES,
    TIMEFRAME_SECONDS,
    AnalysisConfig,
    IndicatorType,
    MultiTimeframeSyncState,
    RollingBarSeries,
    TechnicalAnalysisEngine,
    TimeframeHealth,
    TimeframeHealthStatus,
    normalize_timeframe,
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


def _feed_bars(
    engine: TechnicalAnalysisEngine,
    symbol: str,
    timeframe: str,
    count: int = 30,
    start_ts: float = 100000.0,
    interval_seconds: Optional[float] = None,
    base_price: float = 100.0,
    trend_step: float = 1.0,
) -> List[Bar]:
    """Feed a sequence of in-order bars into the analysis engine."""
    step_s = interval_seconds or TIMEFRAME_SECONDS.get(normalize_timeframe(timeframe), 60.0)
    bars = []
    for i in range(count):
        price = base_price + (i * trend_step)
        bar = _create_bar(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=start_ts + (i * step_s),
            open_p=price - 0.5,
            high_p=price + 2.0,
            low_p=price - 2.0,
            close_p=price,
        )
        engine.add_bar(bar, safe_to_consume=True)
        bars.append(bar)
    return bars


# =============================================================================
# 1. Multi-Timeframe Synchronization & Timeframe Validation
# =============================================================================

class TestMultiTimeframeSynchronization:
    """Verification of timeframe normalization, intervals, and multi-stream ingestion."""

    def test_supported_timeframes_constants(self):
        assert "1m" in SUPPORTED_TIMEFRAMES
        assert "5m" in SUPPORTED_TIMEFRAMES
        assert "15m" in SUPPORTED_TIMEFRAMES
        assert "1h" in SUPPORTED_TIMEFRAMES
        assert "1D" in SUPPORTED_TIMEFRAMES
        assert len(SUPPORTED_TIMEFRAMES) == 5

        assert TIMEFRAME_SECONDS["1m"] == 60.0
        assert TIMEFRAME_SECONDS["5m"] == 300.0
        assert TIMEFRAME_SECONDS["15m"] == 900.0
        assert TIMEFRAME_SECONDS["1h"] == 3600.0
        assert TIMEFRAME_SECONDS["1D"] == 86400.0

    def test_normalize_timeframe_valid_mappings(self):
        assert normalize_timeframe("1m") == "1m"
        assert normalize_timeframe("1M") == "1m"
        assert normalize_timeframe("5m") == "5m"
        assert normalize_timeframe("5M") == "5m"
        assert normalize_timeframe("15m") == "15m"
        assert normalize_timeframe("15M") == "15m"
        assert normalize_timeframe("1h") == "1h"
        assert normalize_timeframe("1H") == "1h"
        assert normalize_timeframe("1D") == "1D"
        assert normalize_timeframe("1d") == "1D"

    def test_normalize_timeframe_invalid_raises_validation_error(self):
        with pytest.raises(ValidationError):
            normalize_timeframe("")
        with pytest.raises(ValidationError):
            normalize_timeframe("   ")
        with pytest.raises(ValidationError):
            normalize_timeframe("2h")
        with pytest.raises(ValidationError):
            normalize_timeframe("30m")
        with pytest.raises(ValidationError):
            normalize_timeframe("1w")

    def test_ingestion_across_all_five_supported_timeframes(self):
        engine = TechnicalAnalysisEngine()
        start_t = 1000000.0

        for tf in SUPPORTED_TIMEFRAMES:
            _feed_bars(engine, symbol="RELIANCE", timeframe=tf, count=25, start_ts=start_t)

        available_tfs = engine.get_available_timeframes("RELIANCE")
        assert len(available_tfs) == 5
        assert set(available_tfs) == set(SUPPORTED_TIMEFRAMES)

        for tf in SUPPORTED_TIMEFRAMES:
            history = engine.get_history("RELIANCE", timeframe=tf)
            assert len(history) == 25
            vector = engine.get_feature_vector("RELIANCE", timeframe=tf)
            assert vector is not None
            assert vector.timeframe == tf


# =============================================================================
# 2. Symbol and Timeframe Stream Isolation
# =============================================================================

class TestStreamIsolation:
    """Verification that streams never contaminate each other across symbols or timeframes."""

    def test_symbol_isolation(self):
        engine = TechnicalAnalysisEngine()
        _feed_bars(engine, symbol="TCS", timeframe="1m", count=25, base_price=3500.0)
        _feed_bars(engine, symbol="INFY", timeframe="1m", count=15, base_price=1600.0)

        tcs_history = engine.get_history("TCS", "1m")
        infy_history = engine.get_history("INFY", "1m")

        assert len(tcs_history) == 25
        assert len(infy_history) == 15
        assert tcs_history[-1].symbol == "TCS"
        assert infy_history[-1].symbol == "INFY"

        tcs_vec = engine.get_feature_vector("TCS", "1m")
        infy_vec = engine.get_feature_vector("INFY", "1m")
        assert tcs_vec.last_close > 3500.0
        assert infy_vec.last_close < 2000.0

    def test_timeframe_isolation_for_same_symbol(self):
        engine = TechnicalAnalysisEngine()
        # Feed 30 bars to 1m, 10 bars to 5m, 5 bars to 1h
        _feed_bars(engine, symbol="HDFCBANK", timeframe="1m", count=30)
        _feed_bars(engine, symbol="HDFCBANK", timeframe="5m", count=10)
        _feed_bars(engine, symbol="HDFCBANK", timeframe="1h", count=5)

        assert len(engine.get_history("HDFCBANK", "1m")) == 30
        assert len(engine.get_history("HDFCBANK", "5m")) == 10
        assert len(engine.get_history("HDFCBANK", "1h")) == 5
        assert len(engine.get_history("HDFCBANK", "1D")) == 0

    def test_series_rejects_foreign_symbol_or_timeframe(self):
        series = RollingBarSeries(symbol="SBIN", timeframe="1m")
        # Same symbol, wrong timeframe
        bar_wrong_tf = _create_bar(symbol="SBIN", timeframe="5m", timestamp=100.0)
        assert series.add_bar(bar_wrong_tf) is False

        # Wrong symbol, same timeframe
        bar_wrong_sym = _create_bar(symbol="ICICIBANK", timeframe="1m", timestamp=100.0)
        assert series.add_bar(bar_wrong_sym) is False

        assert series.count == 0


# =============================================================================
# 3. Missing Intervals, Duplicates, Out-of-Order, Stale & Quarantined Bars
# =============================================================================

class TestQualityAndSynchronizationAnomalies:
    """Deterministic validation of data quality anomalies without silent data synthesis."""

    def test_duplicate_bar_rejection_and_counter(self):
        series = RollingBarSeries(symbol="RELIANCE", timeframe="1m")
        b1 = _create_bar(symbol="RELIANCE", timeframe="1m", timestamp=1000.0)
        b2_dup = _create_bar(symbol="RELIANCE", timeframe="1m", timestamp=1000.0)

        assert series.add_bar(b1) is True
        assert series.add_bar(b2_dup) is False
        assert series.count == 1
        assert series.duplicate_count == 1

    def test_out_of_order_bar_rejection_and_counter(self):
        series = RollingBarSeries(symbol="RELIANCE", timeframe="1m")
        b1 = _create_bar(symbol="RELIANCE", timeframe="1m", timestamp=2000.0)
        b2_past = _create_bar(symbol="RELIANCE", timeframe="1m", timestamp=1900.0)

        assert series.add_bar(b1) is True
        assert series.add_bar(b2_past) is False
        assert series.count == 1
        assert series.out_of_order_count == 1

    def test_missing_intervals_gap_detection(self):
        series = RollingBarSeries(symbol="RELIANCE", timeframe="1m")
        # 1m bar at timestamp 1000.0
        b1 = _create_bar(symbol="RELIANCE", timeframe="1m", timestamp=1000.0)
        # Next bar at 1240.0 (4 minutes later -> 3 missed 1m intervals)
        b2 = _create_bar(symbol="RELIANCE", timeframe="1m", timestamp=1240.0)

        assert series.add_bar(b1) is True
        assert series.add_bar(b2) is True
        assert series.count == 2
        assert series.has_gaps is True
        assert series.missing_intervals_count == 3
        assert len(series.gap_records) == 1
        assert series.gap_records[0] == (1000.0, 1240.0, 3)

    def test_quarantined_unsafe_bars_tracked_per_stream(self):
        engine = TechnicalAnalysisEngine()
        bar = _create_bar(symbol="RELIANCE", timeframe="5m", timestamp=5000.0)

        # Flagged unsafe by quality engine
        res = engine.add_bar(bar, safe_to_consume=False)
        assert res is None

        health = engine.check_timeframe_health("RELIANCE", "5m", reference_time=5000.0)
        assert health.unhealthy_bars_rejected == 1
        assert health.status == TimeframeHealthStatus.UNHEALTHY
        assert health.is_valid_for_analysis is False

    def test_stale_bar_detection(self):
        engine = TechnicalAnalysisEngine()
        # Bar at timestamp 1000.0 (for 1m, freshness threshold is ~150s)
        bar = _create_bar(symbol="RELIANCE", timeframe="1m", timestamp=1000.0)
        engine.add_bar(bar, safe_to_consume=True)

        # Check health at reference_time = 1500.0 (500s later -> stale)
        health = engine.check_timeframe_health("RELIANCE", "1m", reference_time=1500.0)
        assert health.status == TimeframeHealthStatus.STALE
        assert health.age_seconds == 500.0
        assert health.is_valid_for_analysis is False
        assert any("stale" in w.lower() for w in health.warnings)


# =============================================================================
# 4. Technical Analysis Agent & Single-Timeframe Indicator Aggregation
# =============================================================================

class TestTechnicalAnalysisAgentSingleTimeframe:
    """Verification of technical feature aggregation and signal classification."""

    def test_agent_initialization_and_status(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine, name="test_agent")

        assert agent.agent_type == "technical_analysis"
        status = agent.get_status()
        assert status["name"] == "test_agent"
        assert status["reports_generated"] == 0
        assert "1m" in status["supported_timeframes"]

    def test_insufficient_history_produces_insufficient_data_condition(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)

        # Only 5 bars fed (minimum 15 required for valid indicators)
        _feed_bars(engine, symbol="TCS", timeframe="1m", count=5, start_ts=1000.0)

        report = agent.generate_report(symbol="TCS", timeframes=["1m"], reference_time=1300.0)
        assert report.symbol == "TCS"
        assert "1m" in report.timeframe_reports
        tf_rep = report.timeframe_reports["1m"]
        assert tf_rep.condition == SignalCondition.INSUFFICIENT_DATA
        assert report.confidence in (ConfidenceLevel.NONE, ConfidenceLevel.LOW)
        assert any("insufficient" in w.lower() for w in tf_rep.warnings)

    def test_bullish_trend_signal_evidence_and_facts(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)

        # 40 bars in steady uptrend (trend_step=+2.0, enough for MACD 35 bars)
        _feed_bars(engine, symbol="RELIANCE", timeframe="1m", count=40, start_ts=1000.0, base_price=100.0, trend_step=2.0)
        ref_t = 1000.0 + (39 * 60) + 10  # 10s after last bar

        report = agent.generate_report(symbol="RELIANCE", timeframes=["1m"], reference_time=ref_t)
        tf_rep = report.timeframe_reports["1m"]

        assert tf_rep.condition == SignalCondition.BULLISH
        assert tf_rep.last_close > 150.0
        # Check facts dictionary contains SMA, EMA, RSI, MACD, BB, ATR
        assert "SMA_20" in tf_rep.facts["indicators"]
        assert "EMA_9" in tf_rep.facts["indicators"]
        assert "RSI_14" in tf_rep.facts["indicators"]
        assert "MACD" in tf_rep.facts["indicators"]
        assert "BollingerBands" in tf_rep.facts["indicators"]
        assert "ATR_14" in tf_rep.facts["indicators"]

        # Check evidence strings are populated with concrete facts
        assert len(tf_rep.evidence) > 0
        assert any("above 20-period SMA" in ev for ev in tf_rep.evidence)

    def test_bearish_trend_signal_evidence_and_facts(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)

        # 40 bars in steady downtrend (trend_step=-2.0)
        _feed_bars(engine, symbol="INFY", timeframe="1m", count=40, start_ts=1000.0, base_price=200.0, trend_step=-2.0)
        ref_t = 1000.0 + (39 * 60) + 10

        report = agent.generate_report(symbol="INFY", timeframes=["1m"], reference_time=ref_t)
        tf_rep = report.timeframe_reports["1m"]

        assert tf_rep.condition == SignalCondition.BEARISH
        assert any("below 20-period SMA" in ev for ev in tf_rep.evidence)

    def test_intra_timeframe_conflict_detection(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)

        # Construct a scenario where 20 bars rise sharply, then 10 bars drop sharply
        # Price is below short MA/EMA but above long MA, RSI/MACD negative vs Bollinger
        bars = []
        for i in range(25):
            p = 100.0 + i * 4.0
            bars.append(_create_bar("CONFLICT", "1m", timestamp=1000.0 + i * 60, open_p=p - 1, close_p=p))
        # Sudden drop
        for i in range(6):
            p = 196.0 - (i + 1) * 6.0
            bars.append(_create_bar("CONFLICT", "1m", timestamp=1000.0 + (25 + i) * 60, open_p=p + 2, close_p=p))

        for b in bars:
            engine.add_bar(b, safe_to_consume=True)

        ref_t = bars[-1].timestamp + 10
        report = agent.generate_report(symbol="CONFLICT", timeframes=["1m"], reference_time=ref_t)
        tf_rep = report.timeframe_reports["1m"]

        # Disagreement between indicators should yield CONFLICTING or explain conflicts
        assert len(tf_rep.conflicts) > 0 or tf_rep.condition in (SignalCondition.CONFLICTING, SignalCondition.NEUTRAL)
        assert len(tf_rep.evidence) > 0


# =============================================================================
# 5. Cross-Timeframe Agreement & Disagreement
# =============================================================================

class TestCrossTimeframeSynthesis:
    """Verification of cross-timeframe agreement, disagreement, and confidence calibration."""

    def test_cross_timeframe_bullish_agreement_across_all_five_horizons(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)
        start_t = 2000000.0

        for tf in SUPPORTED_TIMEFRAMES:
            _feed_bars(engine, symbol="RELIANCE", timeframe=tf, count=30, start_ts=start_t, trend_step=1.5)

        # Reference time right at the end of the daily bar sequence
        last_daily_ts = start_t + (29 * TIMEFRAME_SECONDS["1D"])
        ref_t = last_daily_ts + 60.0

        report = agent.generate_report(symbol="RELIANCE", reference_time=ref_t)

        assert report.overall_condition == SignalCondition.BULLISH
        assert report.cross_timeframe_agreement == CrossTimeframeAgreement.AGREEMENT
        assert len(report.timeframe_reports) == 5

    def test_cross_timeframe_directional_conflict(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)
        ref_t = 2000000.0

        # Feed 1m bars ending at ref_t - 10s (FRESH & Bullish)
        _feed_bars(engine, symbol="NIFTY", timeframe="1m", count=40, start_ts=ref_t - (40 * 60.0), trend_step=2.0)

        # Feed 1h bars ending at ref_t - 100s (FRESH & Bearish)
        _feed_bars(engine, symbol="NIFTY", timeframe="1h", count=40, start_ts=ref_t - (40 * 3600.0), trend_step=-2.0)

        report = agent.generate_report(symbol="NIFTY", timeframes=["1m", "1h"], reference_time=ref_t)

        assert report.overall_condition == SignalCondition.CONFLICTING
        assert report.cross_timeframe_agreement == CrossTimeframeAgreement.DISAGREEMENT
        assert report.confidence == ConfidenceLevel.LOW
        assert "disagreement" in report.cross_timeframe_summary.lower()


# =============================================================================
# 6. Partial Timeframe Availability & Quality Warnings
# =============================================================================

class TestPartialAvailabilityAndQualityGating:
    """Ensures incomplete or unhealthy timeframes never produce unsupported certainty."""

    def test_partial_timeframe_availability_caps_confidence(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)

        # Only 1m is provided. 5m, 15m, 1h, 1D are completely missing
        _feed_bars(engine, symbol="TATAMOTORS", timeframe="1m", count=30, start_ts=1000.0, trend_step=1.0)
        ref_t = 1000.0 + (29 * 60) + 10

        report = agent.generate_report(symbol="TATAMOTORS", reference_time=ref_t)

        assert report.timeframe_coverage["1m"] == "AVAILABLE"
        assert report.timeframe_coverage["5m"] == "MISSING"
        assert report.timeframe_coverage["15m"] == "MISSING"
        assert report.timeframe_coverage["1h"] == "MISSING"
        assert report.timeframe_coverage["1D"] == "MISSING"

        # Invariant: Confidence CANNOT be HIGH when 4 out of 5 timeframes are missing
        assert report.confidence in (ConfidenceLevel.LOW, ConfidenceLevel.MEDIUM)
        assert len(report.quality_warnings) > 0
        assert any("missing" in w.lower() for w in report.quality_warnings)

    def test_stale_timeframe_prevents_confident_conclusion(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)
        ref_t = 100000.0

        # Feed 1m ending at ref_t - 20s (FRESH)
        _feed_bars(engine, symbol="WIPRO", timeframe="1m", count=30, start_ts=ref_t - (30 * 60.0))
        # Feed 5m way in the past: ending at ref_t - 50000.0 (STALE)
        _feed_bars(engine, symbol="WIPRO", timeframe="5m", count=30, start_ts=ref_t - 50000.0 - (30 * 300.0))

        report = agent.generate_report(symbol="WIPRO", timeframes=["1m", "5m"], reference_time=ref_t)

        assert report.timeframe_coverage["1m"] == "AVAILABLE"
        assert report.timeframe_coverage["5m"] == "STALE"
        assert report.confidence in (ConfidenceLevel.LOW, ConfidenceLevel.MEDIUM)
        assert any("stale" in w.lower() for w in report.quality_warnings)


# =============================================================================
# 7. Structured Intelligence Report & Uncertainty Communication
# =============================================================================

class TestStructuredIntelligenceReport:
    """Verification of facts separation, explicit uncertainty, and voice summaries."""

    def test_facts_vs_interpretations_separation(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)
        _feed_bars(engine, symbol="BAJFINANCE", timeframe="1m", count=25, start_ts=1000.0)

        report = agent.generate_report(symbol="BAJFINANCE", timeframes=["1m"], reference_time=2600.0)

        # Facts are mathematical
        assert "symbol" in report.facts
        assert "reference_timestamp" in report.facts
        assert "1m" in report.facts["timeframes"]
        assert "last_close" in report.facts["timeframes"]["1m"]

        # Interpretation is descriptive analysis
        assert isinstance(report.interpretation, str)
        assert len(report.interpretation) > 10

        # Uncertainty caveats are explicitly stated
        assert len(report.uncertainty_caveats) >= 2
        assert any("do not guarantee" in c.lower() for c in report.uncertainty_caveats)

    def test_voice_summary_format(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)
        _feed_bars(engine, symbol="ITC", timeframe="1m", count=25, start_ts=1000.0)

        report = agent.generate_report(symbol="ITC", timeframes=["1m"], reference_time=2600.0)
        summary = report.voice_summary

        assert isinstance(summary, str)
        assert "ITC" in summary
        assert "report" in summary.lower()
        # Voice summary should not contain Markdown formatting characters
        assert "#" not in summary
        assert "*" not in summary
        assert "{" not in summary

    def test_report_json_serializability(self):
        engine = TechnicalAnalysisEngine()
        agent = TechnicalAnalysisAgent(analysis_engine=engine)
        _feed_bars(engine, symbol="MARUTI", timeframe="1m", count=20, start_ts=1000.0)

        report = agent.generate_report(symbol="MARUTI", timeframes=["1m"], reference_time=2300.0)
        rep_dict = report.to_dict()

        assert rep_dict["symbol"] == "MARUTI"
        assert "overall_condition" in rep_dict
        assert "timeframe_reports" in rep_dict
        assert "1m" in rep_dict["timeframe_reports"]
        assert isinstance(rep_dict["uncertainty_caveats"], list)


# =============================================================================
# 8. Orchestrator Integration & Bootstrap Lifecycle
# =============================================================================

class TestOrchestratorAndBootstrapIntegration:
    """Verification of orchestrator command dispatch and lifecycle bootstrap."""

    def test_orchestrator_multi_timeframe_analysis_command_success(self):
        status = bootstrap_system(Settings(app_env="test"), log_to_file=False)
        orchestrator = status.orchestrator
        analysis_engine = status.analysis_engine
        assert orchestrator is not None
        assert analysis_engine is not None

        # Feed data to analysis engine
        _feed_bars(analysis_engine, symbol="RELIANCE", timeframe="1m", count=30, start_ts=1000.0)
        _feed_bars(analysis_engine, symbol="RELIANCE", timeframe="5m", count=30, start_ts=1000.0)

        req = OrchestratorRequest.create(
            command="multi_timeframe_analysis",
            context={"symbol": "RELIANCE", "timeframes": ["1m", "5m"], "reference_time": 2800.0},
        )
        resp = orchestrator.handle_request(req)

        assert resp.status == RequestStatus.COMPLETED
        assert resp.result is not None
        assert resp.result["status"] == "success"
        assert resp.result["symbol"] == "RELIANCE"
        assert "overall_condition" in resp.result
        assert "data" in resp.result
        assert len(resp.response_text) > 0

    def test_orchestrator_multi_timeframe_analysis_command_missing_symbol(self):
        status = bootstrap_system(Settings(app_env="test"), log_to_file=False)
        orchestrator = status.orchestrator
        assert orchestrator is not None

        req = OrchestratorRequest.create(
            command="multi_timeframe_analysis",
            context={},
        )
        resp = orchestrator.handle_request(req)

        assert resp.status == RequestStatus.FAILED
        assert any("symbol" in err.lower() for err in resp.errors)

    def test_bootstrap_initializes_technical_agent(self):
        status = bootstrap_system(Settings(app_env="test"), log_to_file=False)
        assert status.technical_agent is not None
        assert status.technical_agent.agent_type == "technical_analysis"
        assert status.technical_agent.name == "kaiser_main_technical_agent"

    def test_event_bus_and_state_store_integration(self):
        eb = EventBus("agent_bus")
        ss = StateStore()
        engine = TechnicalAnalysisEngine(event_bus=eb, state_store=ss)
        agent = TechnicalAnalysisAgent(analysis_engine=engine, event_bus=eb, state_store=ss)

        events_received: List[Event] = []
        eb.subscribe(EventType.ANALYSIS_MULTI_TIMEFRAME_READY, lambda e: events_received.append(e))
        eb.subscribe(EventType.AGENT_RESPONSE, lambda e: events_received.append(e))

        _feed_bars(engine, symbol="LT", timeframe="1m", count=25, start_ts=1000.0)
        report = agent.generate_report("LT", timeframes=["1m"], reference_time=2600.0)

        # Check StateStore
        cached_reports = ss.get("technical_intelligence_reports")
        assert cached_reports is not None
        assert "LT" in cached_reports

        # Check EventBus
        assert len(events_received) == 2
        event_types = [e.event_type for e in events_received]
        assert EventType.AGENT_RESPONSE in event_types
        assert EventType.ANALYSIS_MULTI_TIMEFRAME_READY in event_types


# =============================================================================
# 9. Trading Safety Invariants
# =============================================================================

class TestTradingSafetyInvariants:
    """Verify zero live trading, locked execution engine, and uninitialized trading engine."""

    def test_execution_engine_locked_and_trading_engine_uninitialized(self):
        status = bootstrap_system(Settings(app_env="test"), log_to_file=False)
        assert status.settings.execution_locked is True
        assert status.execution_engine == "locked"
        assert status.trading_engine == "not initialized"

    def test_agent_generates_intelligence_without_broker_calls(self):
        status = bootstrap_system(Settings(app_env="test"), log_to_file=False)
        agent = status.technical_agent
        assert agent is not None

        # Even with extreme inputs or conflicting data, agent never calls broker or initiates trades
        _feed_bars(status.analysis_engine, symbol="RISK_CHECK", timeframe="1m", count=25, start_ts=1000.0)
        report = agent.generate_report("RISK_CHECK", timeframes=["1m"], reference_time=2600.0)

        assert report.overall_condition is not None
        # Verify no order proposals or execution records exist in state store
        assert status.state_store.get("active_orders") is None
        assert status.state_store.get("executed_trades") is None
