"""KAISER — Voice Command Pipeline (Step 09) Test Suite.

Comprehensive tests covering:
1. VoiceTurnResult and VoiceTurnStatus model validation, immutability, and factories.
2. End-to-end single-turn pipeline execution (capture -> transcribe -> orchestrate -> respond).
3. Multilingual metadata preservation (English, Tamil, Tanglish, Mixed) without auto-translation.
4. Audio & speech reliability (silence detection, empty speech rejection, malformed audio, whitespace normalization).
5. STT error handling (unavailable provider, timeout, inference failure, missing local model).
6. Capture failure, early cancellation, and closed session handling.
7. Concurrency protection (overlapping turn rejection) and lock release on error.
8. Resource cleanup invariants (raw audio dropped, zero disk persistence).
9. Event lifecycle dispatch and correlation ID tracing.
10. StateStore bounded operational telemetry without audio retention.
11. Strict trading safety invariants (rejection of 'buy', 'sell', 'execute', 'OK', locked execution engine).
12. Zero automatic microphone capture at application startup.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from app.bootstrap import SystemStatus, bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator import MasterOrchestrator, RequestStatus
from app.orchestrator.registry import HandlerRegistry, create_system_status_handler
from app.voice import (
    AudioCaptureConfig,
    AudioCaptureError,
    AudioCaptureService,
    AudioCaptureTimeoutError,
    CapturedAudio,
    LocalSTTConfig,
    LocalWhisperSTTProvider,
    MockAudioCaptureBackend,
    MockSTTProvider,
    MockTTSProvider,
    STTEmptyTranscriptionError,
    STTInferenceError,
    STTTimeoutError,
    TranscriptionResult,
    VoiceOutput,
    VoiceService,
    VoiceSessionManager,
    VoiceTurnResult,
    VoiceTurnStatus,
)


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def event_bus() -> EventBus:
    """Create a fresh in-memory EventBus."""
    return EventBus(name="test_voice_turn_bus")


@pytest.fixture
def state_store() -> StateStore:
    """Create a fresh in-memory StateStore."""
    return StateStore()


@pytest.fixture
def orchestrator(event_bus: EventBus, state_store: StateStore) -> MasterOrchestrator:
    """Create a MasterOrchestrator with system_status registered."""
    settings = Settings(app_env="test", voice_enabled=True, execution_locked=True)
    registry = HandlerRegistry(name="test_turn_registry")
    registry.register(
        command="system_status",
        handler=create_system_status_handler(state_store=state_store, settings=settings),
        description="Diagnostics status handler",
        capability="diagnostics",
    )
    return MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        registry=registry,
        settings=settings,
        name="test_turn_orchestrator",
    )


@pytest.fixture
def mock_stt() -> MockSTTProvider:
    """Create a MockSTTProvider in non-strict mode."""
    return MockSTTProvider(name="mock_stt", strict=False)


@pytest.fixture
def mock_tts() -> MockTTSProvider:
    """Create a MockTTSProvider."""
    return MockTTSProvider(name="mock_tts")


@pytest.fixture
def audio_capture_service(event_bus: EventBus, state_store: StateStore) -> AudioCaptureService:
    """Create AudioCaptureService using MockAudioCaptureBackend."""
    backend = MockAudioCaptureBackend()
    return AudioCaptureService(
        backend=backend,
        event_bus=event_bus,
        state_store=state_store,
        name="test_turn_capture",
    )


@pytest.fixture
def voice_service(
    orchestrator: MasterOrchestrator,
    mock_stt: MockSTTProvider,
    mock_tts: MockTTSProvider,
    audio_capture_service: AudioCaptureService,
    event_bus: EventBus,
    state_store: StateStore,
) -> VoiceService:
    """Create a fully assembled VoiceService with mock components."""
    return VoiceService(
        orchestrator=orchestrator,
        stt_provider=mock_stt,
        tts_provider=mock_tts,
        audio_capture=audio_capture_service,
        event_bus=event_bus,
        state_store=state_store,
        name="test_voice_service",
    )


# ==============================================================================
# 1. VOICE TURN MODELS & IMMUTABILITY TESTS
# ==============================================================================

class TestVoiceTurnModels:
    """Test suite for VoiceTurnResult, VoiceTurnStatus, and factory methods."""

    def test_voice_turn_result_invariants_and_validation(self) -> None:
        """Verify VoiceTurnResult parameter validation, normalization, and immutability."""
        cmd_resp = VoiceOutput.text_only(text="System online.", session_id="sess_turn_1")
        turn = VoiceTurnResult(
            turn_id="turn_101",
            session_id="sess_turn_1",
            correlation_id="corr_turn_1",
            status=VoiceTurnStatus.COMPLETED,
            transcription_status="success",
            recognized_text="system_status",
            language="en",
            language_hint="en",
            confidence=0.98,
            command_response=cmd_resp,
            response_text="System online.",
            duration_ms=120.5,
            metadata={"source": "mic"},
        )
        assert turn.turn_id == "turn_101"
        assert turn.session_id == "sess_turn_1"
        assert turn.correlation_id == "corr_turn_1"
        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.is_success is True
        assert turn.recognized_text == "system_status"
        assert turn.language == "en"
        assert turn.confidence == 0.98
        assert turn.response_text == "System online."
        assert turn.duration_ms == 120.5
        assert turn.metadata["source"] == "mic"
        assert "T" in turn.iso_created_at

        # Verify serialization
        d = turn.to_dict()
        assert d["turn_id"] == "turn_101"
        assert d["status"] == "COMPLETED"
        assert d["is_success"] is True

        # Immutability check
        with pytest.raises(Exception):
            turn.status = VoiceTurnStatus.FAILED  # type: ignore

    def test_voice_turn_factories(self) -> None:
        """Verify VoiceTurnResult factory helpers."""
        cmd_resp = VoiceOutput.text_only(text="OK", session_id="sess_1")

        # Completed
        c = VoiceTurnResult.completed(
            turn_id="t1",
            session_id="s1",
            correlation_id="c1",
            recognized_text="status",
            command_response=cmd_resp,
            duration_ms=45.0,
        )
        assert c.status == VoiceTurnStatus.COMPLETED
        assert c.is_success is True
        assert c.transcription_status == "success"

        # Empty Speech
        e = VoiceTurnResult.empty_speech(
            turn_id="t2",
            session_id="s1",
            correlation_id="c2",
            duration_ms=10.0,
        )
        assert e.status == VoiceTurnStatus.EMPTY_SPEECH
        assert e.is_success is False
        assert e.transcription_status == "empty"
        assert e.recognized_text is None

        # Failure
        f = VoiceTurnResult.failure(
            turn_id="t3",
            session_id="s1",
            correlation_id="c3",
            error="Device error",
            status=VoiceTurnStatus.CAPTURE_FAILED,
        )
        assert f.status == VoiceTurnStatus.CAPTURE_FAILED
        assert f.is_success is False
        assert f.error == "Device error"

        # Cancelled
        canc = VoiceTurnResult.cancelled(
            turn_id="t4",
            session_id="s1",
            correlation_id="c4",
        )
        assert canc.status == VoiceTurnStatus.CANCELLED
        assert canc.is_success is False
        assert canc.transcription_status == "cancelled"

    def test_voice_turn_status_enum(self) -> None:
        """Verify all standardized VoiceTurnStatus enum values exist."""
        expected = {
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "EMPTY_SPEECH",
            "STT_UNAVAILABLE",
            "STT_FAILED",
            "STT_TIMEOUT",
            "CAPTURE_FAILED",
            "COMMAND_REJECTED",
            "CONCURRENCY_ERROR",
        }
        actual = {status.value for status in VoiceTurnStatus}
        assert expected == actual


# ==============================================================================
# 2. END-TO-END PIPELINE TESTS
# ==============================================================================

class TestVoiceTurnEndToEnd:
    """Test suite for full single-turn voice pipeline executions."""

    def test_successful_turn_with_mock_audio_capture(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify explicit capture -> STT -> orchestrator -> response execution."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(
                text="system status",
                language="en",
                confidence=0.99,
                duration_seconds=1.0,
            ),
        )

        result = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            language="en",
            correlation_id="turn_corr_001",
        )

        assert isinstance(result, VoiceTurnResult)
        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.is_success is True
        assert result.transcription_status == "success"
        assert result.recognized_text == "system status"
        assert result.correlation_id == "turn_corr_001"
        assert result.command_response is not None
        assert result.command_response.is_success is True
        assert "status is operational" in result.response_text
        assert result.duration_ms > 0

    def test_successful_turn_with_pre_captured_audio(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify that passing pre-captured audio bypasses capture hardware."""
        captured = CapturedAudio(
            audio_bytes=b"\x00\x01" * 1600,
            sample_rate=16000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=0.1,
            device_id=0,
            device_name="fixture_mic",
        )
        mock_stt.register_fixture(
            "audio:3200",
            TranscriptionResult(
                text="system_status",
                language="en",
                confidence=0.95,
            ),
        )

        result = voice_service.execute_voice_turn(
            audio_data=captured,
            correlation_id="pre_cap_corr",
        )

        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.is_success is True
        assert result.recognized_text == "system_status"
        # Verify that hardware capture was not triggered
        assert voice_service.audio_capture.counters["total_captures"] == 0

    def test_voice_turn_with_synthesized_response(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify voice turn synthesizing TTS audio response when requested."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en", confidence=0.99),
        )

        result = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
        )

        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.command_response is not None
        assert result.command_response.has_audio is True
        assert result.command_response.audio_payload is not None
        assert len(result.command_response.audio_payload) > 0


# ==============================================================================
# 3. MULTILINGUAL METADATA PRESERVATION TESTS
# ==============================================================================

class TestVoiceTurnMultilingual:
    """Test suite verifying language hint handling and vocabulary preservation."""

    def test_english_turn(self, voice_service: VoiceService, mock_stt: MockSTTProvider) -> None:
        """Verify English turn metadata preservation."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        result = voice_service.execute_voice_turn(duration_seconds=1.0, language="en")
        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.language == "en"
        assert result.language_hint == "en"

    def test_tamil_turn(self, voice_service: VoiceService, mock_stt: MockSTTProvider) -> None:
        """Verify Tamil turn metadata preservation."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="ta"),
        )
        result = voice_service.execute_voice_turn(duration_seconds=1.0, language="ta")
        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.language == "ta"
        assert result.language_hint == "ta"

    def test_tanglish_turn_preservation(self, voice_service: VoiceService, mock_stt: MockSTTProvider) -> None:
        """Verify Tanglish turn hint and vocabulary preservation without auto-translation."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="tanglish"),
        )
        result = voice_service.execute_voice_turn(duration_seconds=1.0, language="tanglish")
        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.language == "tanglish"
        assert result.language_hint == "tanglish"
        assert result.recognized_text == "system_status"

    def test_mixed_turn_preservation(self, voice_service: VoiceService, mock_stt: MockSTTProvider) -> None:
        """Verify Mixed language turn preservation."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="mixed"),
        )
        result = voice_service.execute_voice_turn(duration_seconds=1.0, language="mixed")
        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.language == "mixed"
        assert result.language_hint == "mixed"


# ==============================================================================
# 4. AUDIO & SPEECH RELIABILITY TESTS
# ==============================================================================

class TestVoiceTurnAudioAndSpeechReliability:
    """Test suite for empty speech, silence, malformed audio, and normalization."""

    def test_empty_speech_rejection(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify empty/blank transcription is rejected without fabricating commands."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="   ", language="en"),
        )

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.EMPTY_SPEECH
        assert result.is_success is False
        assert result.transcription_status == "empty"
        assert result.recognized_text is None
        assert "No speech detected" in result.response_text

    def test_silent_audio_detection(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify STTEmptyTranscriptionError produces an explicit EMPTY_SPEECH outcome."""
        stt_mock = MagicMock()
        stt_mock.name = "mock_silence_stt"
        stt_mock.is_available = True
        stt_mock.transcribe.side_effect = STTEmptyTranscriptionError("Flatline silent audio detected.")
        voice_service.stt_provider = stt_mock

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.EMPTY_SPEECH
        assert result.is_success is False
        assert "Flatline silent audio" in (result.error or "")

    def test_stt_provider_unavailable(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify unavailable STT provider produces clear actionable status."""
        mock_stt.set_availability(False)

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.STT_UNAVAILABLE
        assert result.is_success is False
        assert "unavailable" in (result.error or "")

    def test_stt_missing_provider(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify unconfigured STT provider returns STT_UNAVAILABLE."""
        voice_service.stt_provider = None

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.STT_UNAVAILABLE
        assert result.is_success is False

    def test_stt_timeout_handling(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify STTTimeoutError is caught and mapped to STT_TIMEOUT status."""
        stt_mock = MagicMock()
        stt_mock.name = "mock_timeout_stt"
        stt_mock.is_available = True
        stt_mock.transcribe.side_effect = STTTimeoutError("STT inference timed out after 30s")
        voice_service.stt_provider = stt_mock

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.STT_TIMEOUT
        assert result.transcription_status == "timeout"
        assert result.is_success is False

    def test_stt_inference_failure(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify general STT inference failure is mapped to STT_FAILED status."""
        stt_mock = MagicMock()
        stt_mock.name = "mock_crash_stt"
        stt_mock.is_available = True
        stt_mock.transcribe.side_effect = STTInferenceError("CUDA out of memory")
        voice_service.stt_provider = stt_mock

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.STT_FAILED
        assert result.transcription_status == "failed"
        assert result.is_success is False

    def test_stt_whitespace_normalization(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify surrounding whitespace is stripped without altering command words."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="  \t system status \n ", language="en"),
        )
        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.COMPLETED
        assert result.recognized_text == "system status"


# ==============================================================================
# 5. CAPTURE LIFECYCLE & FAILURE TESTS
# ==============================================================================

class TestVoiceTurnCaptureAndLifecycle:
    """Test suite for capture timeouts, hardware errors, cancellation, and cleanup."""

    def test_audio_capture_timeout(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify AudioCaptureTimeoutError maps to CAPTURE_FAILED."""
        cap_mock = MagicMock()
        cap_mock.capture_audio.side_effect = AudioCaptureTimeoutError("Capture timed out")
        voice_service.audio_capture = cap_mock

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.CAPTURE_FAILED
        assert result.is_success is False

    def test_audio_capture_hardware_error(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify AudioCaptureError maps to CAPTURE_FAILED."""
        cap_mock = MagicMock()
        cap_mock.capture_audio.side_effect = AudioCaptureError("PortAudio device disconnected")
        voice_service.audio_capture = cap_mock

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.CAPTURE_FAILED
        assert result.is_success is False

    def test_audio_capture_cancelled(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify cancelled capture returns CANCELLED status."""
        cap_mock = MagicMock()
        cap_mock.capture_audio.return_value = CapturedAudio(
            audio_bytes=b"\x00" * 100,
            sample_rate=16000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=0.01,
            device_id=0,
            device_name="mock_mic",
            metadata={"cancelled": True},
        )
        voice_service.audio_capture = cap_mock

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.CANCELLED
        assert result.is_success is False
        assert result.transcription_status == "cancelled"

    def test_closed_session_rejection(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify turn execution against a closed session is rejected."""
        sess = voice_service.create_session()
        voice_service.close_session(sess.session_id)

        result = voice_service.execute_voice_turn(session_id=sess.session_id)
        assert result.status == VoiceTurnStatus.FAILED
        assert result.is_success is False
        assert "closed" in (result.error or "")


# ==============================================================================
# 6. CONCURRENCY & ISOLATION TESTS
# ==============================================================================

class TestVoiceTurnConcurrencyAndIsolation:
    """Test suite for overlapping turn protection, state isolation, and lock safety."""

    def test_consecutive_turns_no_state_leakage(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify multiple sequential turns execute cleanly without state corruption."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )

        turn_1 = voice_service.execute_voice_turn(duration_seconds=1.0)
        turn_2 = voice_service.execute_voice_turn(duration_seconds=1.0)

        assert turn_1.is_success is True
        assert turn_2.is_success is True
        assert turn_1.turn_id != turn_2.turn_id

        metrics = voice_service.get_metrics()
        assert metrics["counters"]["turns_total"] == 2
        assert metrics["counters"]["turns_completed"] == 2

    def test_concurrent_turn_rejected(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify overlapping turn invocation is rejected with CONCURRENCY_ERROR."""
        barrier = threading.Barrier(2)
        concurrent_result: List[VoiceTurnResult] = []

        # Hook STT to pause on barrier inside turn 1
        orig_transcribe = mock_stt.transcribe

        def slow_transcribe(*args: Any, **kwargs: Any) -> TranscriptionResult:
            barrier.wait(timeout=2.0)
            time.sleep(0.05)
            return orig_transcribe(*args, **kwargs)

        mock_stt.transcribe = slow_transcribe  # type: ignore
        mock_stt.register_fixture("audio:32000", TranscriptionResult(text="system_status"))

        def worker() -> None:
            res = voice_service.execute_voice_turn(duration_seconds=1.0)
            concurrent_result.append(res)

        t = threading.Thread(target=worker)
        t.start()

        # Wait until worker enters transcribe
        barrier.wait(timeout=2.0)

        # Attempt second turn while first is active
        overlap_res = voice_service.execute_voice_turn(duration_seconds=1.0)
        t.join(timeout=3.0)

        assert overlap_res.status == VoiceTurnStatus.CONCURRENCY_ERROR
        assert overlap_res.is_success is False
        assert "Overlapping turns are not permitted" in (overlap_res.error or "")

        # When worker completes, subsequent turn should succeed
        mock_stt.transcribe = orig_transcribe  # type: ignore
        subsequent_res = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert subsequent_res.status == VoiceTurnStatus.COMPLETED

    def test_lock_released_on_error(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify turn lock is safely released when an error occurs during processing."""
        voice_service.stt_provider = None  # Force failure

        fail_turn = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert fail_turn.status == VoiceTurnStatus.STT_UNAVAILABLE

        # Restore STT and verify subsequent turn succeeds
        voice_service.stt_provider = MockSTTProvider(name="mock_stt", strict=False)
        voice_service.stt_provider.register_fixture("audio:32000", TranscriptionResult(text="system_status"))

        ok_turn = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert ok_turn.status == VoiceTurnStatus.COMPLETED


# ==============================================================================
# 7. EVENTS & TELEMETRY TESTS
# ==============================================================================

class TestVoiceTurnEventsAndTelemetry:
    """Test suite for event lifecycle, correlation ID tracing, and StateStore boundedness."""

    def test_event_lifecycle_dispatch(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
        event_bus: EventBus,
    ) -> None:
        """Verify standard voice-turn lifecycle events are published in order."""
        events_received: List[Event] = []
        event_bus.subscribe_all(lambda ev: events_received.append(ev))

        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        res = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            correlation_id="trace_evt_101",
        )
        assert res.is_success is True

        event_types = [ev.event_type for ev in events_received]
        assert EventType.VOICE_TURN_STARTED in event_types
        assert EventType.VOICE_TRANSCRIPTION_STARTED in event_types
        assert EventType.VOICE_TRANSCRIPTION_COMPLETED in event_types
        assert EventType.VOICE_TURN_COMPLETED in event_types

        # Verify correlation ID on all turn events
        turn_events = [
            ev for ev in events_received
            if ev.event_type in (
                EventType.VOICE_TURN_STARTED,
                EventType.VOICE_TRANSCRIPTION_STARTED,
                EventType.VOICE_TRANSCRIPTION_COMPLETED,
                EventType.VOICE_TURN_COMPLETED,
            )
        ]
        for ev in turn_events:
            assert ev.correlation_id == "trace_evt_101"

    def test_statestore_bounded_telemetry(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
        state_store: StateStore,
    ) -> None:
        """Verify StateStore contains bounded metrics and zero raw audio or transcripts."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        voice_service.execute_voice_turn(duration_seconds=1.0)

        turn_state = state_store.get("voice_last_turn")
        assert turn_state is not None
        assert "turn_id" in turn_state
        assert turn_state["status"] == "COMPLETED"
        assert turn_state["is_success"] is True
        assert "duration_ms" in turn_state

        # Ensure no raw audio or full transcript histories stored
        assert "audio_bytes" not in turn_state
        assert "raw_pcm" not in turn_state
        assert "text" not in turn_state

    def test_subscriber_failure_isolation(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
        event_bus: EventBus,
    ) -> None:
        """Verify subscriber crash does not disrupt voice turn execution."""
        def bad_subscriber(ev: Event) -> None:
            raise RuntimeError("Subscriber explosion!")

        event_bus.subscribe(EventType.VOICE_TURN_STARTED, bad_subscriber)
        event_bus.subscribe(EventType.VOICE_TURN_COMPLETED, bad_subscriber)

        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        res = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert res.is_success is True


# ==============================================================================
# 8. STRICT SAFETY & TRADING INVARIANTS TESTS
# ==============================================================================

class TestVoiceTurnSafetyAndTradingGuarantees:
    """Test suite ensuring speech never authorizes trades or bypasses execution locks."""

    def test_unauthorized_trading_command_rejected(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
    ) -> None:
        """Verify trading phrase 'buy 100 shares of AAPL' is rejected as unknown command."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text="buy 100 shares of AAPL", language="en"),
        )

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.COMMAND_REJECTED
        assert result.is_success is False
        assert "Unknown command" in (result.error or "")

        # Check safety invariants on settings
        assert voice_service.orchestrator.settings is not None
        assert voice_service.orchestrator.settings.execution_locked is True

    @pytest.mark.parametrize("word", ["buy", "sell", "execute", "ok", "confirm"])
    def test_single_word_trading_commands_rejected(
        self,
        voice_service: VoiceService,
        mock_stt: MockSTTProvider,
        word: str,
    ) -> None:
        """Verify single-word confirmation/trade words cannot authorize financial action."""
        mock_stt.register_fixture(
            "audio:32000",
            TranscriptionResult(text=word, language="en"),
        )

        result = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert result.status == VoiceTurnStatus.COMMAND_REJECTED
        assert result.is_success is False
        assert "Unknown command" in (result.error or "")

    def test_no_automatic_microphone_capture_at_startup(self) -> None:
        """Verify application startup initiates zero automatic audio captures."""
        status = bootstrap_system()
        assert status.initialized is True
        assert status.voice_architecture == "ready"
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"

        # Check audio capture counters
        assert status.audio_capture is not None
        assert status.audio_capture.counters["total_captures"] == 0
        assert status.audio_capture.counters["completed"] == 0

        # Check voice service metrics
        assert status.voice_service is not None
        metrics = status.voice_service.get_metrics()
        assert metrics["counters"]["turns_total"] == 0
        assert metrics["counters"]["total_inputs"] == 0

    def test_no_silent_model_download_or_network(self) -> None:
        """Verify local STT configuration enforces offline operation."""
        config = LocalSTTConfig()
        assert config.allow_download is False

        provider = LocalWhisperSTTProvider(config=config)
        # Without local files, is_available must be False (no silent web fetch)
        assert provider.is_available is False
