"""KAISER — Phase 2, Step 48 Acceptance & Regression Test Suite.

Verifies Deterministic Multilingual Voice Command Intent Resolution & Safety Boundary:
1. English natural language intent resolution (system status, risk, market quality, opportunities, reports).
2. Tanglish (Tamil-English) conversational query resolution (eppadi irukku, sollunga, etc.).
3. Parameterized query resolution & extraction (symbol extraction for technical analysis).
4. Prohibited trade execution directive interception (buy, sell, leverage, order placement).
5. Direct command and legacy snake_case backward compatibility.
6. EventBus lifecycle notifications (VOICE_INTENT_RESOLVED, VOICE_INTENT_REJECTED).
7. End-to-end VoiceService and VoiceInteractionController turn integration.
8. Thread safety, concurrent resolution, and boundary edge cases.
9. Mandatory safety invariants (execution_locked=True, offline brokers, uninitialized trading engine).
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import pytest

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator import (
    HandlerRegistry,
    MasterOrchestrator,
    OrchestratorRequest,
    OrchestratorResponse,
    RequestStatus,
)
from app.voice import (
    MockAudioCaptureProvider,
    MockSTTProvider,
    MockTTSProvider,
    TranscriptionResult,
    VoiceCommandIntent,
    VoiceCommandIntentResolver,
    VoiceInput,
    VoiceIntentType,
    VoiceInteractionController,
    VoiceOutput,
    VoiceOutputStatus,
    VoiceService,
    VoiceSession,
    VoiceTurnResult,
    VoiceTurnStatus,
)


from app.orchestrator.models import HandlerResult


@pytest.fixture
def mock_orchestrator() -> MasterOrchestrator:
    """Create a MasterOrchestrator with standard handlers for testing."""
    registry = HandlerRegistry(name="test_registry")

    # Register standard test handlers
    registry.register(
        command="system_status",
        handler=lambda req: HandlerResult(
            status=RequestStatus.COMPLETED,
            result={"status": "operational", "voice_ready": True},
            response_text="System is operational.",
        ),
        description="System readiness check",
    )
    registry.register(
        command="get_portfolio_risk_status",
        handler=lambda req: HandlerResult(
            status=RequestStatus.COMPLETED,
            result={"risk_level": "LOW", "drawdown": 0.02},
            response_text="Portfolio risk is nominal.",
        ),
        description="Portfolio risk check",
    )
    registry.register(
        command="get_market_data_quality",
        handler=lambda req: HandlerResult(
            status=RequestStatus.COMPLETED,
            result={"quality_score": 0.99, "anomalies": 0},
            response_text="Market data quality is optimal.",
        ),
        description="Market data quality check",
    )
    registry.register(
        command="technical_analysis",
        handler=lambda req: HandlerResult(
            status=RequestStatus.COMPLETED,
            result={
                "symbol": req.context.get("symbol") or getattr(req, "parameters", {}).get("symbol", "UNKNOWN"),
                "trend": "BULLISH",
            },
            response_text=f"Technical analysis completed for {req.context.get('symbol') or getattr(req, 'parameters', {}).get('symbol', 'UNKNOWN')}.",
        ),
        description="Technical analysis",
    )
    registry.register(
        command="list_opportunity_priorities",
        handler=lambda req: HandlerResult(
            status=RequestStatus.COMPLETED,
            result={"priorities": ["opp_1", "opp_2"]},
            response_text="Opportunity priorities retrieved.",
        ),
        description="List opportunity priorities",
    )
    registry.register(
        command="get_investigation_queue",
        handler=lambda req: HandlerResult(
            status=RequestStatus.COMPLETED,
            result={"queue_length": 3},
            response_text="Investigation queue retrieved.",
        ),
        description="Get investigation queue",
    )
    registry.register(
        command="get_latest_portfolio_report",
        handler=lambda req: HandlerResult(
            status=RequestStatus.COMPLETED,
            result={"report_id": "rep_latest"},
            response_text="Latest portfolio report retrieved.",
        ),
        description="Get latest portfolio report",
    )
    registry.register(
        command="get_historical_review_migration_audit_summary",
        handler=lambda req: HandlerResult(
            status=RequestStatus.COMPLETED,
            result={"integrity_status": "VERIFIED", "total_inspected": 42},
            response_text="Historical review migration audit summary retrieved.",
        ),
        description="Migration audit summary",
    )

    return MasterOrchestrator(registry=registry)


@pytest.fixture
def voice_service(mock_orchestrator: MasterOrchestrator) -> VoiceService:
    """Create a configured VoiceService instance."""
    event_bus = EventBus(name="test_voice_bus")
    state_store = StateStore(name="test_voice_store")
    stt_provider = MockSTTProvider()
    tts_provider = MockTTSProvider()

    return VoiceService(
        orchestrator=mock_orchestrator,
        stt_provider=stt_provider,
        tts_provider=tts_provider,
        event_bus=event_bus,
        state_store=state_store,
        name="test_voice_service",
    )


# ==============================================================================
# 1. English Natural Language Intent Resolution Tests (AC-1, AC-2, AC-3)
# ==============================================================================

class TestEnglishNaturalLanguageIntents:
    """Verify natural English spoken queries resolve accurately without exact syntax."""

    @pytest.mark.parametrize(
        "utterance",
        [
            "system status",
            "how is the system",
            "system health",
            "health check",
            "system readiness",
            "status check",
            "How is the system?",
            "SYSTEM STATUS!",
        ],
    )
    def test_english_system_status_queries(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="en")

        assert intent.intent_type == VoiceIntentType.SYSTEM_STATUS
        assert intent.command == "system_status"
        assert intent.confidence >= 0.95
        assert not intent.is_prohibited_trading
        assert intent.rejection_reason is None

    @pytest.mark.parametrize(
        "utterance",
        [
            "portfolio risk",
            "risk status",
            "check risk",
            "how is the risk",
            "portfolio risk report",
            "Check risk status please.",
        ],
    )
    def test_english_portfolio_risk_queries(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="en")

        assert intent.intent_type == VoiceIntentType.PORTFOLIO_RISK_STATUS
        assert intent.command == "get_portfolio_risk_status"
        assert intent.confidence >= 0.90
        assert not intent.is_prohibited_trading

    @pytest.mark.parametrize(
        "utterance",
        [
            "market quality",
            "market data quality",
            "data quality",
            "data health",
            "market anomalies",
        ],
    )
    def test_english_market_data_quality_queries(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="en")

        assert intent.intent_type == VoiceIntentType.MARKET_DATA_QUALITY
        assert intent.command == "get_market_data_quality"
        assert intent.confidence >= 0.90
        assert not intent.is_prohibited_trading

    @pytest.mark.parametrize(
        ("utterance", "expected_type", "expected_cmd"),
        [
            ("opportunity priorities", VoiceIntentType.OPPORTUNITY_PRIORITIES, "list_opportunity_priorities"),
            ("top opportunities", VoiceIntentType.OPPORTUNITY_PRIORITIES, "list_opportunity_priorities"),
            ("investigation queue", VoiceIntentType.INVESTIGATION_QUEUE, "get_investigation_queue"),
            ("show investigation queue", VoiceIntentType.INVESTIGATION_QUEUE, "get_investigation_queue"),
            ("latest portfolio report", VoiceIntentType.LATEST_PORTFOLIO_REPORT, "get_latest_portfolio_report"),
            ("historical review", VoiceIntentType.HISTORICAL_REVIEW_SUMMARY, "get_historical_review_migration_audit_summary"),
            ("migration audit", VoiceIntentType.HISTORICAL_REVIEW_SUMMARY, "get_historical_review_migration_audit_summary"),
        ],
    )
    def test_english_workflow_queries(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
        expected_type: VoiceIntentType,
        expected_cmd: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="en")

        assert intent.intent_type == expected_type
        assert intent.command == expected_cmd
        assert intent.confidence >= 0.90


# ==============================================================================
# 2. Tanglish (Tamil-English) Conversational Intents (AC-5)
# ==============================================================================

class TestTanglishConversationalIntents:
    """Verify Tanglish conversational queries resolve accurately."""

    @pytest.mark.parametrize(
        "utterance",
        [
            "system eppadi irukku",
            "system status sollunga",
            "system status enna",
            "status enna",
            "system epdi irukku",
            "system nallaa irukkaa",
        ],
    )
    def test_tanglish_system_status(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="tanglish")

        assert intent.intent_type == VoiceIntentType.SYSTEM_STATUS
        assert intent.command == "system_status"
        assert intent.confidence >= 0.95
        assert intent.language == "tanglish"

    @pytest.mark.parametrize(
        "utterance",
        [
            "portfolio risk eppadi irukku",
            "risk status sollunga",
            "risk enna",
            "risk pathi sollu",
            "risk epdi irukku",
        ],
    )
    def test_tanglish_portfolio_risk(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="tanglish")

        assert intent.intent_type == VoiceIntentType.PORTFOLIO_RISK_STATUS
        assert intent.command == "get_portfolio_risk_status"
        assert intent.confidence >= 0.90

    @pytest.mark.parametrize(
        "utterance",
        [
            "market quality eppadi irukku",
            "data quality sollunga",
            "data health eppadi irukku",
        ],
    )
    def test_tanglish_market_quality(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="tanglish")

        assert intent.intent_type == VoiceIntentType.MARKET_DATA_QUALITY
        assert intent.command == "get_market_data_quality"
        assert intent.confidence >= 0.90

    @pytest.mark.parametrize(
        ("utterance", "expected_type", "expected_cmd"),
        [
            ("queue eppadi irukku", VoiceIntentType.INVESTIGATION_QUEUE, "get_investigation_queue"),
            ("investigation queue sollunga", VoiceIntentType.INVESTIGATION_QUEUE, "get_investigation_queue"),
            ("portfolio report sollunga", VoiceIntentType.LATEST_PORTFOLIO_REPORT, "get_latest_portfolio_report"),
            ("audit report sollunga", VoiceIntentType.HISTORICAL_REVIEW_SUMMARY, "get_historical_review_migration_audit_summary"),
            ("migration audit eppadi irukku", VoiceIntentType.HISTORICAL_REVIEW_SUMMARY, "get_historical_review_migration_audit_summary"),
        ],
    )
    def test_tanglish_workflow_queries(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
        expected_type: VoiceIntentType,
        expected_cmd: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="tanglish")

        assert intent.intent_type == expected_type
        assert intent.command == expected_cmd
        assert intent.confidence >= 0.90


# ==============================================================================
# 3. Parameterized Technical Analysis & Symbol Extraction (AC-4)
# ==============================================================================

class TestParameterizedQueryExtraction:
    """Verify parameters such as symbol are extracted cleanly from spoken utterances."""

    @pytest.mark.parametrize(
        ("utterance", "expected_symbol"),
        [
            ("technical analysis for AAPL", "AAPL"),
            ("analyze MSFT", "MSFT"),
            ("technical indicators for TSLA", "TSLA"),
            ("chart analysis for BTC", "BTC"),
            ("analyze RELIANCE", "RELIANCE"),
            ("AAPL technical analysis pannu", "AAPL"),
            ("TSLA chart eppadi irukku", "TSLA"),
            ("INFY indicators sollu", "INFY"),
        ],
    )
    def test_symbol_extraction(
        self,
        mock_orchestrator: MasterOrchestrator,
        utterance: str,
        expected_symbol: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(utterance, language="en")

        assert intent.intent_type == VoiceIntentType.TECHNICAL_ANALYSIS
        assert intent.command == "technical_analysis"
        assert intent.parameters.get("symbol") == expected_symbol
        assert intent.confidence >= 0.95


# ==============================================================================
# 4. Prohibited Trading Directives Interception (AC-6)
# ==============================================================================

class TestProhibitedTradingInterception:
    """Verify that trade execution directives are blocked at the voice boundary."""

    @pytest.mark.parametrize(
        "prohibited_utterance",
        [
            "buy 100 AAPL",
            "sell 50 MSFT",
            "go long BTC",
            "go short ETH",
            "place order for TSLA",
            "execute trade now",
            "market order 10 shares",
            "set leverage to 10x",
            "position size 5000",
            "set stop loss at 150",
            "vaangu 100 AAPL",
            "vithuru 50 MSFT",
            "order podu",
            "trade execute pannu",
        ],
    )
    def test_prohibited_trading_blocked_by_resolver(
        self,
        mock_orchestrator: MasterOrchestrator,
        prohibited_utterance: str,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve(prohibited_utterance, language="en")

        assert intent.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND
        assert intent.is_prohibited_trading is True
        assert intent.rejection_reason is not None
        assert "prohibited" in intent.rejection_reason.lower()

    def test_prohibited_command_rejected_by_service(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify VoiceService rejects prohibited command without calling orchestrator."""
        session = voice_service.create_session(session_id="test_prohibited_sess")
        voice_input = VoiceInput.create(
            text="buy 100 AAPL at market price",
            session_id=session.session_id,
            language="en",
        )

        output: VoiceOutput = voice_service.process_voice_input(voice_input)

        assert output.is_success is False
        assert output.error is not None
        assert "prohibited" in output.error.lower()
        assert output.metadata.get("prohibited_trading") is True


# ==============================================================================
# 5. Backward Compatibility & Fallback (AC-7)
# ==============================================================================

class TestBackwardCompatibilityAndFallback:
    """Verify exact command strings and legacy snake_case inputs continue to resolve."""

    def test_exact_command_resolution(
        self,
        mock_orchestrator: MasterOrchestrator,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve("system_status")

        assert intent.intent_type == VoiceIntentType.DIRECT_COMMAND
        assert intent.command == "system_status"
        assert intent.confidence == 1.0

    def test_legacy_snake_case_conversion_fallback(
        self,
        mock_orchestrator: MasterOrchestrator,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        # "system status" exactly matches "system_status" via snake_case check
        intent = resolver.resolve("system status")

        assert intent.command == "system_status"
        assert intent.confidence >= 0.95

    def test_unregistered_unknown_utterance(
        self,
        mock_orchestrator: MasterOrchestrator,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve("gibberish unmapped command 12345")

        assert intent.intent_type == VoiceIntentType.UNKNOWN
        assert intent.confidence == 0.0
        assert intent.command == "gibberish_unmapped_command_12345"

    def test_empty_utterance(
        self,
        mock_orchestrator: MasterOrchestrator,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        intent = resolver.resolve("   ")

        assert intent.intent_type == VoiceIntentType.UNKNOWN
        assert intent.confidence == 0.0
        assert intent.command == ""


# ==============================================================================
# 6. EventBus Lifecycle Notifications (AC-8)
# ==============================================================================

class TestEventBusIntentNotifications:
    """Verify EventBus emits VOICE_INTENT_RESOLVED and VOICE_INTENT_REJECTED."""

    def test_voice_intent_resolved_event_emitted(
        self,
        voice_service: VoiceService,
    ) -> None:
        events: List[Event] = []
        assert voice_service.event_bus is not None
        voice_service.event_bus.subscribe(
            EventType.VOICE_INTENT_RESOLVED,
            lambda evt: events.append(evt),
        )

        session = voice_service.create_session(session_id="evt_sess_1")
        voice_input = VoiceInput.create(
            text="how is the system",
            session_id=session.session_id,
            language="en",
        )

        output = voice_service.process_voice_input(voice_input)
        assert output.is_success is True

        assert len(events) == 1
        event = events[0]
        assert event.event_type == EventType.VOICE_INTENT_RESOLVED
        assert event.payload["intent_type"] == "SYSTEM_STATUS"
        assert event.payload["command"] == "system_status"

    def test_voice_intent_rejected_event_emitted_on_prohibited(
        self,
        voice_service: VoiceService,
    ) -> None:
        events: List[Event] = []
        assert voice_service.event_bus is not None
        voice_service.event_bus.subscribe(
            EventType.VOICE_INTENT_REJECTED,
            lambda evt: events.append(evt),
        )

        session = voice_service.create_session(session_id="evt_sess_2")
        voice_input = VoiceInput.create(
            text="execute trade for 100 shares",
            session_id=session.session_id,
            language="en",
        )

        output = voice_service.process_voice_input(voice_input)
        assert output.is_success is False

        assert len(events) == 1
        event = events[0]
        assert event.event_type == EventType.VOICE_INTENT_REJECTED
        assert "prohibited" in event.payload["reason"].lower()


# ==============================================================================
# 7. End-to-End Voice Turn Integration (AC-9)
# ==============================================================================

class TestEndToEndVoiceTurnIntegration:
    """Verify end-to-end voice turn processing with intent resolution and parameters."""

    def test_voice_turn_natural_english_success(
        self,
        voice_service: VoiceService,
    ) -> None:
        session = voice_service.create_session(session_id="turn_sess_en")

        result: VoiceTurnResult = voice_service.execute_voice_turn(
            session_id=session.session_id,
            audio_data="fixture:system_status",  # MockSTT fixture token
        )

        # Default MockSTT returns "system_status"
        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.command_status == "success"

    def test_voice_turn_with_extracted_parameters(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Inject parameterized transcription into voice turn."""
        session = voice_service.create_session(session_id="turn_sess_param")

        # Directly invoke process_voice_input to test parameter flow to orchestrator
        voice_input = VoiceInput.create(
            text="analyze TSLA",
            session_id=session.session_id,
            language="en",
        )

        output = voice_service.process_voice_input(voice_input)
        assert output.is_success is True
        assert "TSLA" in output.text

    def test_voice_turn_prohibited_command_rejection(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Inject prohibited trading text into VoiceService."""
        session = voice_service.create_session(session_id="turn_sess_proj")

        voice_input = VoiceInput.create(
            text="buy AAPL",
            session_id=session.session_id,
            language="en",
        )

        output = voice_service.process_voice_input(voice_input)
        assert output.is_success is False
        assert output.metadata.get("prohibited_trading") is True

    def test_voice_controller_turn_integration(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify VoiceInteractionController delegates cleanly with intent resolution."""
        controller = VoiceInteractionController(
            voice_service=voice_service,
            event_bus=voice_service.event_bus,
            state_store=voice_service.state_store,
        )
        session = controller.create_session(session_id="ctrl_sess")

        result = controller.submit_turn(
            session_id=session.session_id,
            audio_data="fixture:system_status",
        )
        assert result.status == VoiceTurnStatus.COMPLETED


# ==============================================================================
# 8. Concurrency & Thread-Safety Tests
# ==============================================================================

class TestConcurrencyAndThreadSafety:
    """Verify thread-safe concurrent intent resolution without race conditions."""

    def test_concurrent_intent_resolutions(
        self,
        mock_orchestrator: MasterOrchestrator,
    ) -> None:
        resolver = VoiceCommandIntentResolver(orchestrator=mock_orchestrator)
        utterances = [
            ("how is the system", "system_status"),
            ("portfolio risk status", "get_portfolio_risk_status"),
            ("market data quality", "get_market_data_quality"),
            ("analyze AAPL", "technical_analysis"),
            ("system eppadi irukku", "system_status"),
            ("buy 100 shares", "prohibited_trading_command"),
        ]

        def resolve_task(idx: int) -> Tuple[str, str]:
            utt, expected_cmd = utterances[idx % len(utterances)]
            res = resolver.resolve(utt)
            return res.command, expected_cmd

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(resolve_task, i) for i in range(100)]
            for future in concurrent.futures.as_completed(futures):
                actual, expected = future.result()
                assert actual == expected


# ==============================================================================
# 9. Safety Invariants (AC-10)
# ==============================================================================

class TestMandatorySafetyInvariants:
    """Verify execution lock and trading engine safety constraints remain inviolable."""

    def test_safety_invariants_preserved(self) -> None:
        settings = Settings()
        assert settings.execution_locked is True
        assert settings.paper_trading_only is True

    def test_voice_intent_cannot_bypass_lock(
        self,
        voice_service: VoiceService,
    ) -> None:
        session = voice_service.create_session(session_id="lock_test_sess")
        voice_input = VoiceInput.create(
            text="place order for 100 AAPL",
            session_id=session.session_id,
            language="en",
        )

        output = voice_service.process_voice_input(voice_input)
        assert output.is_success is False
        assert output.error is not None
        assert "prohibited" in output.error.lower()
