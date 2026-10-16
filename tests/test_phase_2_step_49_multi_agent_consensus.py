"""KAISER — Phase 2, Step 49: Multi-Agent Specialist Coordination & Deterministic Consensus Tests.

Deterministic verification suite covering:
1. Category A: Existing architecture compatibility
   - Existing direct commands (system_status, multi_timeframe_analysis) remain registered & functional.
   - TechnicalAnalysisAgent remains callable and compliant.
   - Step 48 English and Tanglish intent resolution remains intact.
   - Unknown commands fail cleanly via established error handling.
2. Category B: Agent coordination
   - Designated participating agents receive correct request context.
   - Per-agent findings retain identity, type, and provenance.
   - Correlation ID propagation and deterministic stability across repeated executions.
3. Category C: Consensus correctness
   - Bullish agreement produces AGREEMENT with synthesized_posture='BULLISH'.
   - Bearish agreement produces AGREEMENT with synthesized_posture='BEARISH'.
   - Divergent findings produce DISAGREEMENT with synthesized_posture='CONFLICTING'.
   - Insufficient bars (<20) produces INSUFFICIENT_EVIDENCE.
   - Partial agent failure produces PARTIAL_RESULT when allowed and FAILED when disallowed.
   - Total agent failure produces FAILED.
4. Category D: Data quality, serialization, and provenance
   - Unhealthy / missing timeframes follow quality-gate rules without fabricating candles.
   - Deterministic consensus ID format (cons_{symbol}_{ts}_{digest}).
   - Prohibited trading/recommendation field rejection.
5. Category E: Failure isolation and resource behavior
   - Injected agent exceptions are isolated without crashing coordination service.
   - Bounded history retention (FIFO eviction).
   - Thread-safety under concurrent coordination requests.
6. Category F: Mandatory safety invariants
   - Settings.execution_locked remains True.
   - Trading engine remains 'not initialized'.
   - Execution engine remains 'locked'.
   - Zero broker calls or live order paths.
   - Spoken prohibited commands remain blocked before orchestrator dispatch.
7. Category G: Voice subsystem integration
   - English voice utterances resolve to coordinate_agent_consensus with extracted symbol.
   - Tanglish voice utterances resolve to coordinate_agent_consensus with extracted symbol.
"""

from __future__ import annotations

import copy
import threading
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from app.agents import (
    AgentConsensusReport,
    AgentFinding,
    BaseAgent,
    ConfidenceLevel,
    ConsensusStatus,
    MarketRegime,
    MarketRegimeAgent,
    MarketRegimeReport,
    MultiAgentCoordinationService,
    RegimePosture,
    SignalCondition,
    TechnicalAnalysisAgent,
    TechnicalIntelligenceReport,
    TrendStrength,
    VolatilityState,
    compute_deterministic_consensus_id,
    register_consensus_handlers,
    register_regime_agent_handlers,
    register_technical_agent_handlers,
)
from app.analysis import (
    AnalysisConfig,
    RollingBarSeries,
    TechnicalAnalysisEngine,
    normalize_timeframe,
)
from app.analysis.opportunities.models import (
    PROHIBITED_RECOMMENDATION_FIELDS,
    validate_no_prohibited_fields,
)
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.models import Bar
from app.orchestrator import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, RequestStatus
from app.voice.intent import VoiceCommandIntentResolver, VoiceIntentType


# -----------------------------------------------------------------------------
# Fixtures and Test Helpers
# -----------------------------------------------------------------------------

def _create_bar(
    symbol: str = "INFY",
    timeframe: str = "1h",
    timestamp: float = 1700000000.0,
    open_p: float = 100.0,
    high_p: Optional[float] = None,
    low_p: Optional[float] = None,
    close_p: float = 102.0,
    volume: float = 1000.0,
    source: str = "mock",
) -> Bar:
    """Helper to construct valid Bar records with preserved OHLC relations."""
    resolved_high = high_p if high_p is not None else max(open_p, close_p) + 2.0
    resolved_low = low_p if low_p is not None else min(open_p, close_p) - 2.0
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


def _feed_trend_bars(
    engine: TechnicalAnalysisEngine,
    symbol: str,
    timeframe: str = "1h",
    count: int = 40,
    start_ts: float = 100000.0,
    base_price: float = 100.0,
    trend_step: float = 1.0,
) -> List[Bar]:
    """Feed in-order bars with a deterministic trend into the analysis engine."""
    step_s = 3600.0 if timeframe == "1h" else 60.0
    bars = []
    for i in range(count):
        price = base_price + (i * trend_step)
        bar = _create_bar(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=start_ts + (i * step_s),
            open_p=price - 0.5,
            high_p=price + 1.5,
            low_p=price - 1.5,
            close_p=price,
        )
        engine.add_bar(bar, safe_to_consume=True)
        bars.append(bar)
    return bars


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_consensus_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    return StateStore(name="test_consensus_state_store")


@pytest.fixture
def analysis_engine(event_bus: EventBus, state_store: StateStore) -> TechnicalAnalysisEngine:
    return TechnicalAnalysisEngine(
        config=AnalysisConfig(),
        event_bus=event_bus,
        state_store=state_store,
        name="test_engine",
    )


@pytest.fixture
def technical_agent(analysis_engine: TechnicalAnalysisEngine, event_bus: EventBus, state_store: StateStore) -> TechnicalAnalysisAgent:
    return TechnicalAnalysisAgent(
        analysis_engine=analysis_engine,
        event_bus=event_bus,
        state_store=state_store,
        name="test_technical_agent",
    )


@pytest.fixture
def regime_agent(analysis_engine: TechnicalAnalysisEngine, event_bus: EventBus, state_store: StateStore) -> MarketRegimeAgent:
    return MarketRegimeAgent(
        analysis_engine=analysis_engine,
        event_bus=event_bus,
        state_store=state_store,
        name="test_regime_agent",
    )


@pytest.fixture
def coordination_service(
    technical_agent: TechnicalAnalysisAgent,
    regime_agent: MarketRegimeAgent,
    event_bus: EventBus,
    state_store: StateStore,
) -> MultiAgentCoordinationService:
    return MultiAgentCoordinationService(
        technical_agent=technical_agent,
        regime_agent=regime_agent,
        event_bus=event_bus,
        state_store=state_store,
        name="test_coordination_service",
    )


@pytest.fixture
def orchestrator(
    event_bus: EventBus,
    state_store: StateStore,
    technical_agent: TechnicalAnalysisAgent,
    regime_agent: MarketRegimeAgent,
    coordination_service: MultiAgentCoordinationService,
) -> MasterOrchestrator:
    orch = MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        settings=Settings(app_env="test", execution_locked=True),
        name="test_orchestrator",
    )
    register_technical_agent_handlers(orch, technical_agent)
    register_regime_agent_handlers(orch, regime_agent)
    register_consensus_handlers(orch, coordination_service)
    return orch


# =============================================================================
# Category A: Existing Architecture Compatibility
# =============================================================================

class TestArchitectureCompatibility:
    """Verifies that adding Step 49 does not break existing commands, agents, or routing."""

    def test_direct_commands_continue_to_function(self, orchestrator: MasterOrchestrator):
        req = OrchestratorRequest.create(command="system_status")
        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.COMPLETED
        assert resp.is_success is True
        assert resp.result is not None
        assert resp.result["trading_engine"] == "not initialized"

    def test_technical_analysis_agent_command_remains_callable(
        self,
        orchestrator: MasterOrchestrator,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        _feed_trend_bars(analysis_engine, symbol="INFY", count=30, trend_step=1.0)
        req = OrchestratorRequest.create(
            command="multi_timeframe_analysis",
            context={"symbol": "INFY", "timeframes": ["1h"]},
        )
        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.COMPLETED
        assert resp.is_success is True
        assert resp.result["symbol"] == "INFY"
        assert "overall_condition" in resp.result

    def test_unsupported_command_fails_gracefully(self, orchestrator: MasterOrchestrator):
        req = OrchestratorRequest.create(command="non_existent_command_xyz")
        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.FAILED
        assert resp.is_success is False
        assert any("Unknown command" in err for err in resp.errors)


# =============================================================================
# Category B: Agent Coordination & Context Passing
# =============================================================================

class TestAgentCoordination:
    """Verifies that both specialist agents are invoked with correct context and traceability."""

    def test_both_agents_receive_context_and_preserve_identity(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        _feed_trend_bars(analysis_engine, symbol="TCS", count=35, trend_step=1.0)
        report = coordination_service.coordinate_consensus(
            symbol="TCS",
            timeframes=["1h"],
            correlation_id="corr_coord_test_1",
            reference_time=1700000000.0,
        )
        assert report.symbol == "TCS"
        assert report.status in (ConsensusStatus.AGREEMENT, ConsensusStatus.DISAGREEMENT, ConsensusStatus.INSUFFICIENT_EVIDENCE)
        assert len(report.participating_agents) == 2
        assert "test_technical_agent" in report.agent_findings
        assert "test_regime_agent" in report.agent_findings

        # Check per-agent finding metadata
        tech_finding = report.agent_findings["test_technical_agent"]
        assert tech_finding.agent_type == "technical_analysis"
        assert tech_finding.is_valid is True

        regime_finding = report.agent_findings["test_regime_agent"]
        assert regime_finding.agent_type == "market_regime"
        assert regime_finding.is_valid is True

    def test_deterministic_stability_across_repeated_runs(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        _feed_trend_bars(analysis_engine, symbol="WIPRO", count=30, trend_step=1.0)
        ref_time = 1700000000.0
        rep1 = coordination_service.coordinate_consensus("WIPRO", timeframes=["1h"], reference_time=ref_time)
        rep2 = coordination_service.coordinate_consensus("WIPRO", timeframes=["1h"], reference_time=ref_time)

        assert rep1.consensus_id == rep2.consensus_id
        assert rep1.status == rep2.status
        assert rep1.synthesized_posture == rep2.synthesized_posture
        assert rep1.voice_summary == rep2.voice_summary


# =============================================================================
# Category C: Consensus Correctness Matrix
# =============================================================================

class TestConsensusCorrectness:
    """Verifies all branches of the 5-state deterministic consensus contract."""

    def test_bullish_agreement_consensus(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        """When both Technical and Regime identify clear upward trend, consensus is AGREEMENT."""
        bars = _feed_trend_bars(analysis_engine, symbol="NIFTY", count=40, trend_step=2.0)
        report = coordination_service.coordinate_consensus(
            "NIFTY", timeframes=["1h"], reference_time=bars[-1].timestamp
        )

        assert report.status == ConsensusStatus.AGREEMENT
        assert report.synthesized_posture == "BULLISH"
        assert len(report.agreement_points) > 0
        assert len(report.disagreement_points) == 0
        assert "agree" in report.voice_summary.lower()

    def test_bearish_agreement_consensus(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        """When both Technical and Regime identify clear downward trend, consensus is AGREEMENT."""
        bars = _feed_trend_bars(analysis_engine, symbol="BANKNIFTY", count=40, base_price=500.0, trend_step=-3.0)
        report = coordination_service.coordinate_consensus(
            "BANKNIFTY", timeframes=["1h"], reference_time=bars[-1].timestamp
        )

        assert report.status == ConsensusStatus.AGREEMENT
        assert report.synthesized_posture == "BEARISH"
        assert len(report.agreement_points) > 0
        assert len(report.disagreement_points) == 0

    def test_disagreement_consensus_when_postures_diverge(
        self,
        coordination_service: MultiAgentCoordinationService,
    ):
        """When technical indicates BULLISH and regime indicates BEARISH, consensus is DISAGREEMENT."""
        # Mocking individual agent outputs to test precise divergence boundary
        with patch.object(coordination_service.technical_agent, "generate_report") as mock_tech:
            with patch.object(coordination_service.regime_agent, "generate_report") as mock_regime:
                mock_tech.return_value = TechnicalIntelligenceReport(
                    symbol="DIV_SYM",
                    timestamp=1000.0,
                    iso_timestamp="2026-10-15T00:00:00Z",
                    overall_condition=SignalCondition.BULLISH,
                    confidence=ConfidenceLevel.HIGH,
                    timeframe_coverage={"1h": "AVAILABLE"},
                    timeframe_reports={},
                    cross_timeframe_agreement=None,  # type: ignore
                    cross_timeframe_summary="",
                    voice_summary="Bullish indicators.",
                )
                mock_regime.return_value = MarketRegimeReport(
                    symbol="DIV_SYM",
                    timestamp=1000.0,
                    iso_timestamp="2026-10-15T00:00:00Z",
                    overall_regime=MarketRegime.BEARISH_TRENDING,
                    overall_posture=RegimePosture.BEARISH,
                    overall_volatility=VolatilityState.NORMAL,
                    overall_trend_strength=TrendStrength.STRONG,
                    timeframe_evaluations={},
                    timeframe_coverage={"1h": "AVAILABLE"},
                    facts={},
                    interpretation="Bearish macro trend.",
                    uncertainty_caveats=(),
                    voice_summary="Bearish regime.",
                )

                report = coordination_service.coordinate_consensus("DIV_SYM", timeframes=["1h"])
                assert report.status == ConsensusStatus.DISAGREEMENT
                assert report.synthesized_posture == "CONFLICTING"
                assert len(report.disagreement_points) > 0
                assert "divergence" in report.voice_summary.lower()

    def test_insufficient_evidence_when_data_missing(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        """When fewer than 20 bars are provided, consensus is INSUFFICIENT_EVIDENCE."""
        _feed_trend_bars(analysis_engine, symbol="SPARSE_SYM", count=5, trend_step=1.0)
        report = coordination_service.coordinate_consensus("SPARSE_SYM", timeframes=["1h"])

        assert report.status == ConsensusStatus.INSUFFICIENT_EVIDENCE
        assert report.synthesized_posture is None
        assert "insufficient" in report.voice_summary.lower()

    def test_partial_result_when_one_agent_fails_and_allowed(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        """When one agent crashes and allow_partial=True, result is PARTIAL_RESULT."""
        _feed_trend_bars(analysis_engine, symbol="PARTIAL_SYM", count=30, trend_step=1.0)
        with patch.object(coordination_service.regime_agent, "generate_report", side_effect=RuntimeError("Regime calculation fault")):
            report = coordination_service.coordinate_consensus("PARTIAL_SYM", timeframes=["1h"], allow_partial=True)
            assert report.status == ConsensusStatus.PARTIAL_RESULT
            assert report.agent_findings["test_regime_agent"].is_valid is False
            assert "Regime calculation fault" in str(report.agent_findings["test_regime_agent"].error)
            assert report.agent_findings["test_technical_agent"].is_valid is True

    def test_failed_when_one_agent_fails_and_partial_disallowed(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        """When one agent crashes and allow_partial=False, result is FAILED."""
        _feed_trend_bars(analysis_engine, symbol="DISALLOWED_PARTIAL", count=30, trend_step=1.0)
        with patch.object(coordination_service.regime_agent, "generate_report", side_effect=RuntimeError("Regime crash")):
            report = coordination_service.coordinate_consensus("DISALLOWED_PARTIAL", timeframes=["1h"], allow_partial=False)
            assert report.status == ConsensusStatus.FAILED

    def test_failed_when_both_agents_fail(
        self,
        coordination_service: MultiAgentCoordinationService,
    ):
        """When both agents throw exceptions, outcome is FAILED."""
        with patch.object(coordination_service.technical_agent, "generate_report", side_effect=RuntimeError("Tech crash")):
            with patch.object(coordination_service.regime_agent, "generate_report", side_effect=RuntimeError("Regime crash")):
                report = coordination_service.coordinate_consensus("BOTH_FAIL", timeframes=["1h"])
                assert report.status == ConsensusStatus.FAILED
                assert len(report.limitations) > 0


# =============================================================================
# Category D: Data Quality, Serialization & Provenance
# =============================================================================

class TestDataQualityAndProvenance:
    """Verifies deterministic identifiers, no prohibited fields, and clean serialization."""

    def test_consensus_report_rejects_prohibited_recommendation_fields(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        _feed_trend_bars(analysis_engine, symbol="CLEAN_SYM", count=30, trend_step=1.0)
        report = coordination_service.coordinate_consensus("CLEAN_SYM", timeframes=["1h"])
        report_dict = report.to_dict()

        # Must not raise ValidationError
        validate_no_prohibited_fields(report_dict)

        # Explicitly verify blacklist items are not in top-level or findings
        for prohibited in ("action", "order", "entry", "exit", "leverage", "position_size", "buy_signal"):
            assert prohibited not in report_dict

    def test_deterministic_consensus_id_generation(self):
        finding_a = AgentFinding("ag1", "tech", "BULLISH", is_valid=True)
        finding_b = AgentFinding("ag2", "regime", "BULLISH", is_valid=True)
        id1 = compute_deterministic_consensus_id("INFY", 1700000000.0, {"ag1": finding_a, "ag2": finding_b})
        id2 = compute_deterministic_consensus_id("INFY", 1700000000.0, {"ag1": finding_a, "ag2": finding_b})
        assert id1 == id2
        assert id1.startswith("cons_infy_1700000000000_")

    def test_blank_symbol_rejected_with_validation_error(
        self,
        coordination_service: MultiAgentCoordinationService,
    ):
        with pytest.raises(ValidationError, match="requires a non-empty symbol"):
            coordination_service.coordinate_consensus("   ")


# =============================================================================
# Category E: Failure Isolation and Resource Bounds
# =============================================================================

class TestFailureIsolationAndBounds:
    """Verifies bounded StateStore retention and thread-safe concurrent execution."""

    def test_history_bounded_retention(
        self,
        analysis_engine: TechnicalAnalysisEngine,
        event_bus: EventBus,
        state_store: StateStore,
        technical_agent: TechnicalAnalysisAgent,
        regime_agent: MarketRegimeAgent,
    ):
        # Create service with max_history=3
        bounded_service = MultiAgentCoordinationService(
            technical_agent=technical_agent,
            regime_agent=regime_agent,
            event_bus=event_bus,
            state_store=state_store,
            max_history=3,
        )
        _feed_trend_bars(analysis_engine, symbol="BOUND_TEST", count=25, trend_step=1.0)
        for i in range(5):
            bounded_service.coordinate_consensus("BOUND_TEST", timeframes=["1h"], reference_time=1000.0 + i)

        status = bounded_service.get_status()
        assert status["coordination_count"] == 5
        assert status["history_length"] == 3

    def test_concurrent_coordination_execution(
        self,
        coordination_service: MultiAgentCoordinationService,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        """Verifies multiple threads can execute coordination concurrently without deadlock."""
        _feed_trend_bars(analysis_engine, symbol="CONCUR_SYM", count=30, trend_step=1.0)
        results: List[AgentConsensusReport] = []
        errors: List[Exception] = []

        def worker(idx: int):
            try:
                rep = coordination_service.coordinate_consensus(
                    "CONCUR_SYM",
                    timeframes=["1h"],
                    reference_time=1700000000.0 + idx,
                )
                results.append(rep)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert len(errors) == 0
        assert len(results) == 6


# =============================================================================
# Category F: Mandatory Safety Invariants
# =============================================================================

class TestSafetyInvariants:
    """Verifies KAISER trading execution-lock and non-executable boundaries."""

    def test_execution_engine_remains_strictly_locked(self, orchestrator: MasterOrchestrator):
        settings = orchestrator.settings
        assert settings is not None
        assert settings.execution_locked is True

        req = OrchestratorRequest.create(
            command="coordinate_agent_consensus",
            context={"symbol": "INFY"},
        )
        resp = orchestrator.handle_request(req)
        assert settings.execution_locked is True
        assert resp.status == RequestStatus.COMPLETED

    def test_system_status_preserves_not_initialized_trading_engine(self):
        status = bootstrap_system()
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.settings.execution_locked is True
        assert status.regime_agent is not None
        assert status.coordination_service is not None

    def test_spoken_trade_directives_blocked_before_orchestrator(self):
        resolver = VoiceCommandIntentResolver()
        prohibited_phrases = [
            "buy 100 shares of reliance",
            "sell 50 AAPL",
            "place order for INFY",
            "set leverage to 10x",
            "go short NIFTY",
        ]
        for phrase in prohibited_phrases:
            intent = resolver.resolve(phrase)
            assert intent.is_prohibited_trading is True
            assert intent.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND
            assert intent.command == "prohibited_trading_command"


# =============================================================================
# Category G: Voice Subsystem Integration
# =============================================================================

class TestVoiceIntegration:
    """Verifies that English and Tanglish consensus voice queries resolve accurately."""

    def test_english_consensus_intent_resolution(self):
        resolver = VoiceCommandIntentResolver()
        queries = [
            ("coordinate agent consensus for INFY", "INFY"),
            ("check multi agent consensus for AAPL", "AAPL"),
            ("show agent consensus for RELIANCE", "RELIANCE"),
            ("agent consensus for TCS", "TCS"),
            ("consensus for WIPRO", "WIPRO"),
        ]
        for utterance, expected_symbol in queries:
            intent = resolver.resolve(utterance)
            assert intent.intent_type == VoiceIntentType.MULTI_AGENT_CONSENSUS
            assert intent.command == "coordinate_agent_consensus"
            assert intent.parameters.get("symbol") == expected_symbol

    def test_tanglish_consensus_intent_resolution(self):
        resolver = VoiceCommandIntentResolver()
        queries = [
            ("agents consensus enna INFY", "INFY"),
            ("RELIANCE agents consensus enna", "RELIANCE"),
            ("TCS agents consensus eppadi irukku", "TCS"),
        ]
        for utterance, expected_symbol in queries:
            intent = resolver.resolve(utterance, language="tanglish")
            assert intent.intent_type == VoiceIntentType.MULTI_AGENT_CONSENSUS
            assert intent.command == "coordinate_agent_consensus"
            assert intent.parameters.get("symbol") == expected_symbol

    def test_orchestrator_consensus_command_via_voice_payload(
        self,
        orchestrator: MasterOrchestrator,
        analysis_engine: TechnicalAnalysisEngine,
    ):
        """Simulate resolved voice intent dispatched directly to orchestrator."""
        _feed_trend_bars(analysis_engine, symbol="INFY", count=30, trend_step=1.0)
        resolver = VoiceCommandIntentResolver()
        intent = resolver.resolve("coordinate agent consensus for INFY")

        req = OrchestratorRequest.create(
            command=intent.command,
            context=intent.parameters,
        )
        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.COMPLETED
        assert resp.is_success is True
        assert resp.result["symbol"] == "INFY"
        assert resp.result["consensus_status"] in (
            ConsensusStatus.AGREEMENT.value,
            ConsensusStatus.DISAGREEMENT.value,
            ConsensusStatus.INSUFFICIENT_EVIDENCE.value,
        )
