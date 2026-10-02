"""KAISER — Voice Interface Foundation Test Suite.

Comprehensive tests covering:
1. VoiceInput, VoiceOutput, TranscriptionResult, and SynthesisResult model validation & immutability
2. Multilingual metadata handling (English, Tamil, Tanglish, Mixed)
3. Mock STT provider fixture resolution, strict mode, and failure simulation
4. Mock TTS provider synthetic audio generation, duration calculation, and failure simulation
5. VoiceSession lifecycle state machine, valid transitions, and invalid transition rejection
6. VoiceSessionManager bounded capacity retention and cleanup
7. VoiceService request pipeline routing through MasterOrchestrator
8. Correlation ID and language metadata propagation
9. Unknown command rejection without execution
10. Event Bus publishing and subscriber failure isolation
11. Bounded StateStore operational telemetry
12. Secret-safe logging and persistence guardrails
13. Bootstrap subsystem integration and foundational safety invariants
"""

from __future__ import annotations

import threading
import time
from dataclasses import FrozenInstanceError
from typing import Any, Dict, List

import pytest

from app.bootstrap import SystemStatus, bootstrap_system
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator import MasterOrchestrator, RequestStatus
from app.voice import (
    MockSTTProvider,
    MockTTSProvider,
    SpeechToTextProvider,
    SynthesisResult,
    TextToSpeechProvider,
    TranscriptionResult,
    VoiceInput,
    VoiceOutput,
    VoiceOutputStatus,
    VoiceProviderError,
    VoiceService,
    VoiceSession,
    VoiceSessionError,
    VoiceSessionManager,
    VoiceSessionStatus,
)


# ==============================================================================
# 1. VOICE MODELS & IMMUTABILITY TESTS
# ==============================================================================

class TestVoiceModels:
    """Test suite for VoiceInput, VoiceOutput, and provider result models."""

    def test_voice_input_valid(self) -> None:
        """Verify VoiceInput creation with valid parameters."""
        inp = VoiceInput.create(
            text="system_status",
            session_id="sess_123",
            language="en",
            source="mic_mock",
            metadata={"device": "desktop"},
        )
        assert inp.text == "system_status"
        assert inp.session_id == "sess_123"
        assert inp.language == "en"
        assert inp.source == "mic_mock"
        assert inp.metadata["device"] == "desktop"
        assert len(inp.input_id) > 0
        assert inp.created_at > 0
        assert "T" in inp.iso_created_at

    def test_voice_input_rejects_empty_text(self) -> None:
        """Verify VoiceInput rejects empty or whitespace-only text."""
        with pytest.raises(ValidationError, match="must not be empty or whitespace-only"):
            VoiceInput.create(text="", session_id="s1")

        with pytest.raises(ValidationError, match="must not be empty or whitespace-only"):
            VoiceInput.create(text="   \t\n  ", session_id="s1")

    def test_voice_input_immutability(self) -> None:
        """Verify VoiceInput cannot be mutated once constructed."""
        inp = VoiceInput.create(text="ping", session_id="s1", metadata={"tag": "v1"})

        with pytest.raises(FrozenInstanceError):
            inp.text = "pong"  # type: ignore

        with pytest.raises(TypeError):
            inp.metadata["tag"] = "v2"  # type: ignore

    def test_voice_input_to_dict(self) -> None:
        """Verify VoiceInput dictionary serialization."""
        inp = VoiceInput.create(text="status", session_id="s1", metadata={"seq": 1})
        data = inp.to_dict()
        assert data["text"] == "status"
        assert data["session_id"] == "s1"
        assert data["metadata"]["seq"] == 1
        # Mutation of dict does not mutate original input
        data["metadata"]["seq"] = 999
        assert inp.metadata["seq"] == 1

    def test_voice_output_text_only(self) -> None:
        """Verify VoiceOutput.text_only constructor."""
        out = VoiceOutput.text_only(
            text="System is operational.",
            session_id="sess_1",
            language="en",
        )
        assert out.status == VoiceOutputStatus.TEXT_ONLY
        assert out.is_success is True
        assert out.text == "System is operational."
        assert out.audio_payload is None
        assert out.error is None

    def test_voice_output_synthesized(self) -> None:
        """Verify VoiceOutput.synthesized constructor with audio payload."""
        payload = b"RIFF_MOCK_WAV_BYTES"
        out = VoiceOutput.synthesized(
            text="Welcome to KAISER.",
            session_id="sess_1",
            audio_payload=payload,
            audio_format="mock_pcm",
            duration_ms=450.0,
            language="en",
        )
        assert out.status == VoiceOutputStatus.SYNTHESIZED
        assert out.is_success is True
        assert out.audio_payload == payload
        assert out.audio_format == "mock_pcm"
        assert out.duration_ms == 450.0

    def test_voice_output_failure(self) -> None:
        """Verify VoiceOutput.failure constructor."""
        out = VoiceOutput.failure(
            error="Speech recognition timed out",
            session_id="sess_1",
        )
        assert out.status == VoiceOutputStatus.FAILED
        assert out.is_success is False
        assert "Speech recognition timed out" in out.error  # type: ignore

    def test_voice_output_synthesized_with_error_rejected(self) -> None:
        """Verify VoiceOutput cannot have status SYNTHESIZED when error is present."""
        with pytest.raises(ValidationError, match="cannot have status SYNTHESIZED when error is present"):
            VoiceOutput(
                output_id="o1",
                session_id="s1",
                text="Some text",
                status=VoiceOutputStatus.SYNTHESIZED,
                error="Fatal synthesis error",
            )

    def test_transcription_result_bounds_and_immutability(self) -> None:
        """Verify TranscriptionResult clamps confidence and freezes metadata."""
        tr = TranscriptionResult(
            text="system_status",
            language="ta",
            confidence=1.5,  # Should clamp to 1.0
            duration_seconds=-2.0,  # Should clamp to 0.0
            metadata={"source": "fixture"},
        )
        assert tr.confidence == 1.0
        assert tr.duration_seconds == 0.0
        assert tr.language == "ta"

        with pytest.raises(FrozenInstanceError):
            tr.text = "other"  # type: ignore

        with pytest.raises(TypeError):
            tr.metadata["source"] = "new"  # type: ignore

    def test_synthesis_result_validation_and_immutability(self) -> None:
        """Verify SynthesisResult validates audio_bytes and freezes metadata."""
        with pytest.raises(ValidationError, match="audio_bytes must be bytes"):
            SynthesisResult(audio_bytes="not_bytes")  # type: ignore

        sr = SynthesisResult(
            audio_bytes=b"MOCK_BYTES",
            audio_format="mock_pcm",
            duration_ms=300.0,
            text="Hello",
            metadata={"voice": "kaiser_v1"},
        )
        assert sr.audio_bytes == b"MOCK_BYTES"
        assert sr.duration_ms == 300.0
        with pytest.raises(TypeError):
            sr.metadata["voice"] = "kaiser_v2"  # type: ignore


# ==============================================================================
# 2. LANGUAGE METADATA HANDLING TESTS
# ==============================================================================

class TestLanguageHandling:
    """Test suite verifying language metadata handling across voice models and providers."""

    def test_supported_language_codes_accepted(self) -> None:
        """Verify standard language tags (en, ta, tanglish, mixed) are accepted."""
        for lang in ("en", "ta", "tanglish", "mixed"):
            inp = VoiceInput.create(text="command", session_id="s", language=lang)
            assert inp.language == lang

    def test_language_aliases_normalized(self) -> None:
        """Verify common language aliases normalize correctly."""
        inp_en = VoiceInput.create(text="command", session_id="s", language="ENGLISH")
        assert inp_en.language == "en"

        inp_ta = VoiceInput.create(text="command", session_id="s", language="Tamil")
        assert inp_ta.language == "ta"

        inp_tanglish = VoiceInput.create(text="command", session_id="s", language="tamil_english")
        assert inp_tanglish.language == "tanglish"

        inp_mixed = VoiceInput.create(text="command", session_id="s", language="mixed_en_ta")
        assert inp_mixed.language == "mixed"

    def test_unknown_language_handled_explicitly(self) -> None:
        """Verify unknown language falls back or is explicitly preserved rather than fabricated."""
        out = VoiceOutput.text_only(text="ack", session_id="s", language="unknown")
        assert out.language == "unknown"


# ==============================================================================
# 3. MOCK STT & TTS PROVIDER TESTS
# ==============================================================================

class TestMockProviders:
    """Test suite for deterministic Mock STT and Mock TTS engines."""

    def test_mock_stt_transcribes_default_fixtures(self) -> None:
        """Verify MockSTTProvider transcribes default test fixtures deterministically."""
        stt = MockSTTProvider()

        # English fixture
        res_en = stt.transcribe("fixture:system_status")
        assert res_en.text == "system_status"
        assert res_en.language == "en"
        assert res_en.confidence > 0.9

        # Tamil fixture
        res_ta = stt.transcribe("fixture:tamil_status")
        assert res_ta.text == "system_status"
        assert res_ta.language == "ta"

        # Tanglish fixture
        res_tg = stt.transcribe("fixture:tanglish_status")
        assert res_tg.text == "system_status"
        assert res_tg.language == "tanglish"

        # Mixed fixture
        res_mx = stt.transcribe("fixture:mixed_status")
        assert res_mx.text == "system_status"
        assert res_mx.language == "mixed"

    def test_mock_stt_custom_fixture_registration(self) -> None:
        """Verify registering and transcribing custom STT fixtures."""
        stt = MockSTTProvider()
        stt.register_fixture(
            "custom_token",
            TranscriptionResult(text="quote_nifty", language="en", confidence=0.92),
        )
        res = stt.transcribe("custom_token")
        assert res.text == "quote_nifty"
        assert res.confidence == 0.92

    def test_mock_stt_strict_mode_rejects_unknown_fixture(self) -> None:
        """Verify strict mode raises VoiceProviderError for unregistered tokens."""
        stt = MockSTTProvider(strict=True)
        with pytest.raises(VoiceProviderError, match="fixture not found"):
            stt.transcribe("unknown_token_xyz")

    def test_mock_stt_empty_audio_rejected(self) -> None:
        """Verify empty audio token raises VoiceProviderError."""
        stt = MockSTTProvider()
        with pytest.raises(VoiceProviderError, match="Audio payload is empty"):
            stt.transcribe("")

    def test_mock_stt_simulated_failure(self) -> None:
        """Verify simulated STT failure mode."""
        stt = MockSTTProvider()
        stt.set_failure_mode(True, message="Microphone buffer underrun")
        assert stt.is_available is False

        with pytest.raises(VoiceProviderError, match="Microphone buffer underrun"):
            stt.transcribe("fixture:system_status")

    def test_mock_tts_synthesizes_audio(self) -> None:
        """Verify MockTTSProvider generates synthetic deterministic audio."""
        tts = MockTTSProvider()
        result = tts.synthesize(text="KAISER system is operational.", language="en")

        assert isinstance(result.audio_bytes, bytes)
        assert len(result.audio_bytes) > 0
        assert b"MOCK_AUDIO_PAYLOAD" in result.audio_bytes
        assert result.audio_format == "mock_pcm"
        assert result.duration_ms > 0.0

    def test_mock_tts_rejects_empty_text(self) -> None:
        """Verify MockTTSProvider rejects empty text."""
        tts = MockTTSProvider()
        with pytest.raises(VoiceProviderError, match="Cannot synthesize empty"):
            tts.synthesize("")

    def test_mock_tts_simulated_failure(self) -> None:
        """Verify simulated TTS synthesis failure."""
        tts = MockTTSProvider()
        tts.set_failure_mode(True, message="Audio driver busy")
        assert tts.is_available is False

        with pytest.raises(VoiceProviderError, match="Audio driver busy"):
            tts.synthesize("test text")


# ==============================================================================
# 4. VOICE SESSION & LIFECYCLE TESTS
# ==============================================================================

class TestVoiceSession:
    """Test suite for stateful VoiceSession management and transition validation."""

    def test_session_creation_and_defaults(self) -> None:
        """Verify VoiceSession initialization defaults to ACTIVE."""
        session = VoiceSession(session_id="sess_abc", language="ta")
        assert session.session_id == "sess_abc"
        assert session.status == VoiceSessionStatus.ACTIVE
        assert session.language == "ta"
        assert session.is_active is True
        assert session.is_closed is False
        assert session.created_at > 0

    def test_valid_session_transitions(self) -> None:
        """Verify valid sequence of session lifecycle transitions."""
        session = VoiceSession(session_id="sess_1")

        session.transition_to(VoiceSessionStatus.PROCESSING)
        assert session.status == VoiceSessionStatus.PROCESSING

        session.transition_to(VoiceSessionStatus.ACTIVE)
        assert session.status == VoiceSessionStatus.ACTIVE

        session.transition_to(VoiceSessionStatus.IDLE)
        assert session.status == VoiceSessionStatus.IDLE

        session.transition_to(VoiceSessionStatus.ACTIVE)
        assert session.status == VoiceSessionStatus.ACTIVE

        session.transition_to(VoiceSessionStatus.COMPLETED)
        assert session.status == VoiceSessionStatus.COMPLETED

        session.transition_to(VoiceSessionStatus.CLOSED)
        assert session.status == VoiceSessionStatus.CLOSED
        assert session.is_closed is True

    def test_invalid_session_transitions_rejected(self) -> None:
        """Verify illegal transitions raise VoiceSessionError."""
        session = VoiceSession(session_id="sess_1")
        session.close()

        # Cannot transition out of CLOSED
        with pytest.raises(VoiceSessionError, match="Illegal session transition"):
            session.transition_to(VoiceSessionStatus.ACTIVE)

        session2 = VoiceSession(session_id="sess_2")
        session2.transition_to(VoiceSessionStatus.IDLE)
        # Cannot transition IDLE -> COMPLETED directly
        with pytest.raises(VoiceSessionError, match="Illegal session transition"):
            session2.transition_to(VoiceSessionStatus.COMPLETED)

    def test_session_manager_capacity_and_retention(self) -> None:
        """Verify VoiceSessionManager creates sessions and bounds capacity."""
        manager = VoiceSessionManager(max_sessions=3)

        s1 = manager.create_session("s1")
        s2 = manager.create_session("s2")
        s3 = manager.create_session("s3")

        assert manager.active_count == 3
        assert manager.get_session("s1") is s1

        # Close s1 and create s4 -> s1 should be purged
        manager.close_session("s1")
        s4 = manager.create_session("s4")

        assert manager.get_session("s4") is s4
        assert manager.get_session("s1") is None  # Purged
        assert len(manager.list_active_sessions()) == 3


# ==============================================================================
# 5. VOICE SERVICE & ORCHESTRATOR INTEGRATION TESTS
# ==============================================================================

class TestVoiceServiceIntegration:
    """Test suite for end-to-end VoiceService processing through MasterOrchestrator."""

    def test_process_voice_input_text_only(self) -> None:
        """Verify process_voice_input routes to orchestrator and returns text-only output."""
        event_bus = EventBus()
        state_store = StateStore()
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)

        service = VoiceService(
            orchestrator=orchestrator,
            event_bus=event_bus,
            state_store=state_store,
        )

        inp = VoiceInput.create(text="system_status", session_id="sess_test_1")
        output = service.process_voice_input(inp, synthesize_output=False)

        assert output.is_success is True
        assert output.status == VoiceOutputStatus.TEXT_ONLY
        assert output.session_id == "sess_test_1"
        assert "operational" in output.text
        assert output.audio_payload is None

    def test_process_voice_input_synthesized(self) -> None:
        """Verify synthesize_output=True invokes MockTTSProvider to attach audio payload."""
        orchestrator = MasterOrchestrator()
        tts = MockTTSProvider()

        service = VoiceService(
            orchestrator=orchestrator,
            tts_provider=tts,
        )

        inp = VoiceInput.create(text="system_status", session_id="sess_test_2")
        output = service.process_voice_input(inp, synthesize_output=True)

        assert output.is_success is True
        assert output.status == VoiceOutputStatus.SYNTHESIZED
        assert output.audio_payload is not None
        assert output.audio_format == "mock_pcm"
        assert output.duration_ms is not None
        assert output.duration_ms > 0

    def test_process_audio_end_to_end_mock_pipeline(self) -> None:
        """Verify process_audio performs STT transcription, routes, and produces output."""
        orchestrator = MasterOrchestrator()
        stt = MockSTTProvider()
        tts = MockTTSProvider()

        service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt,
            tts_provider=tts,
        )

        output = service.process_audio(
            audio_data="fixture:system_status",
            session_id="audio_sess_1",
            synthesize_output=True,
        )

        assert output.is_success is True
        assert output.status == VoiceOutputStatus.SYNTHESIZED
        assert "operational" in output.text
        assert output.audio_payload is not None

    def test_correlation_id_propagation(self) -> None:
        """Verify caller correlation_id propagates to voice events and response."""
        event_bus = EventBus()
        orchestrator = MasterOrchestrator(event_bus=event_bus)

        events: List[Event] = []
        event_bus.subscribe("*", lambda ev: events.append(ev))

        service = VoiceService(orchestrator=orchestrator, event_bus=event_bus)

        test_corr_id = "corr_voice_tracing_123"
        inp = VoiceInput.create(text="system_status", session_id="sess_corr")

        output = service.process_voice_input(inp, correlation_id=test_corr_id)

        assert output.is_success is True

        # Check events published with correlation_id
        voice_events = [e for e in events if e.source == "voice"]
        assert len(voice_events) > 0
        for ev in voice_events:
            assert ev.correlation_id == test_corr_id

    def test_unknown_command_rejection(self) -> None:
        """Verify unrecognized speech text safely fails at orchestrator level."""
        orchestrator = MasterOrchestrator()
        service = VoiceService(orchestrator=orchestrator)

        inp = VoiceInput.create(text="unregistered_voice_command", session_id="s_unk")
        output = service.process_voice_input(inp)

        assert output.is_success is False
        assert output.status == VoiceOutputStatus.FAILED
        assert "Unknown command" in output.error  # type: ignore

    def test_closed_session_rejection(self) -> None:
        """Verify inputs targeting closed sessions are rejected with structured error."""
        orchestrator = MasterOrchestrator()
        service = VoiceService(orchestrator=orchestrator)

        sess = service.create_session("sess_to_close")
        service.close_session(sess.session_id)

        inp = VoiceInput.create(text="system_status", session_id=sess.session_id)
        output = service.process_voice_input(inp)

        assert output.is_success is False
        assert "is closed" in output.error  # type: ignore

    def test_tts_failure_returns_structured_error(self) -> None:
        """Verify TTS provider failure returns structured failure output."""
        orchestrator = MasterOrchestrator()
        tts = MockTTSProvider()
        tts.set_failure_mode(True, message="Speech synthesizer memory allocation failure")

        service = VoiceService(orchestrator=orchestrator, tts_provider=tts)

        inp = VoiceInput.create(text="system_status", session_id="s_tts_fail")
        output = service.process_voice_input(inp, synthesize_output=True)

        assert output.is_success is False
        assert output.status == VoiceOutputStatus.FAILED
        assert "TTS synthesis failed" in output.error  # type: ignore

    def test_stt_failure_returns_structured_error(self) -> None:
        """Verify STT provider failure returns structured failure output."""
        orchestrator = MasterOrchestrator()
        stt = MockSTTProvider()
        stt.set_failure_mode(True, message="Acoustic model offline")

        service = VoiceService(orchestrator=orchestrator, stt_provider=stt)

        output = service.process_audio(audio_data="fixture:system_status", session_id="s_stt_fail")

        assert output.is_success is False
        assert output.status == VoiceOutputStatus.FAILED
        assert "STT transcription failed" in output.error  # type: ignore


# ==============================================================================
# 6. EVENT BUS & STATE STORE INTEGRATION TESTS
# ==============================================================================

class TestVoiceEventsAndState:
    """Test suite for EventBus voice events and StateStore bounded persistence."""

    def test_lifecycle_event_publishing(self) -> None:
        """Verify voice events published across session, input, and response lifecycle."""
        event_bus = EventBus()
        orchestrator = MasterOrchestrator(event_bus=event_bus)

        events: List[Event] = []
        event_bus.subscribe("*", lambda ev: events.append(ev))

        service = VoiceService(orchestrator=orchestrator, event_bus=event_bus)

        inp = VoiceInput.create(text="system_status", session_id="ev_sess")
        service.process_voice_input(inp)

        event_types = [e.event_type for e in events]
        assert EventType.VOICE_SESSION_UPDATED in event_types
        assert EventType.VOICE_INPUT_RECEIVED in event_types
        assert EventType.VOICE_RESPONSE_CREATED in event_types

    def test_subscriber_failure_isolation(self) -> None:
        """Verify crashing subscriber does not disrupt VoiceService or fail response."""
        event_bus = EventBus()
        orchestrator = MasterOrchestrator(event_bus=event_bus)

        # Flaky subscriber that crashes on VOICE_RESPONSE_CREATED
        def bad_subscriber(ev: Event) -> None:
            if ev.event_type == EventType.VOICE_RESPONSE_CREATED:
                raise RuntimeError("Subscriber crashed during voice event handling!")

        event_bus.subscribe("*", bad_subscriber)

        service = VoiceService(orchestrator=orchestrator, event_bus=event_bus)

        inp = VoiceInput.create(text="system_status", session_id="sub_fail_sess")
        output = service.process_voice_input(inp)

        # Invariant: Subscriber failure must not fail voice output
        assert output.is_success is True
        assert output.status == VoiceOutputStatus.TEXT_ONLY

    def test_bounded_state_store_updates(self) -> None:
        """Verify VoiceService maintains bounded operational keys in StateStore."""
        state_store = StateStore()
        orchestrator = MasterOrchestrator(state_store=state_store)
        stt = MockSTTProvider()
        tts = MockTTSProvider()

        service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt,
            tts_provider=tts,
            state_store=state_store,
        )

        # Process multiple voice requests
        for i in range(10):
            inp = VoiceInput.create(text="system_status", session_id=f"sess_{i}")
            service.process_voice_input(inp, synthesize_output=True)

        keys = state_store.keys()
        expected = {
            "voice_request_counters",
            "voice_provider_status",
            "voice_active_session",
            "voice_last_output",
            # Orchestrator state keys
            "orchestrator_request_counters",
            "orchestrator_current_status",
            "orchestrator_active_requests",
            "orchestrator_last_completed",
        }
        assert set(keys).issubset(expected)

        counters = state_store.get("voice_request_counters")
        assert counters["total_inputs"] == 10
        assert counters["processed"] == 10
        assert counters["synthesized"] == 10
        assert counters["failed"] == 0

        # Verify no unbounded list of transcripts was stored
        assert "transcripts" not in keys
        assert "audio_history" not in keys

    def test_secret_safe_logging_in_voice(self) -> None:
        """Verify API keys and passwords in metadata are scrubbed before publishing and state."""
        event_bus = EventBus()
        state_store = StateStore()
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)

        events: List[Event] = []
        event_bus.subscribe("*", lambda e: events.append(e))

        service = VoiceService(
            orchestrator=orchestrator,
            event_bus=event_bus,
            state_store=state_store,
        )

        inp = VoiceInput.create(
            text="system_status",
            session_id="sec_sess",
            metadata={"api_key": "sensitive_credential_xyz", "normal": "val"},
        )
        output = service.process_voice_input(inp)
        assert output.is_success is True

        # Check event payload was scrubbed
        received_event = next(e for e in events if e.event_type == EventType.VOICE_INPUT_RECEIVED)
        assert "sensitive_credential_xyz" not in str(received_event.payload)


# ==============================================================================
# 7. BOOTSTRAP INTEGRATION TESTS
# ==============================================================================

class TestVoiceBootstrapIntegration:
    """Test suite verifying VoiceService integration during bootstrap_system()."""

    def test_bootstrap_initializes_voice_service(self) -> None:
        """Verify bootstrap_system initializes VoiceService with mock STT/TTS in safe mode."""
        status = bootstrap_system(log_to_file=False)

        assert status.voice_service is not None
        assert isinstance(status.voice_service, VoiceService)
        assert status.voice_service.stt_provider is not None
        assert status.voice_service.tts_provider is not None

        # Verify safety invariants preserved
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.voice_architecture == "ready"

    def test_bootstrap_voice_service_executes_status_request(self) -> None:
        """Verify end-to-end execution of voice status check via bootstrapped voice service."""
        status = bootstrap_system(log_to_file=False)
        assert status.voice_service is not None

        output = status.voice_service.process_audio(
            audio_data="fixture:system_status",
            session_id="boot_voice_sess",
            synthesize_output=True,
        )

        assert output.is_success is True
        assert output.status == VoiceOutputStatus.SYNTHESIZED
        assert "initialized" in output.text or "operational" in output.text
        assert output.audio_payload is not None
        assert status.execution_engine == "locked"
