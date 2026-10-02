"""KAISER — Phase 1, Step 11: Unified Voice Turn Orchestration Tests.

Deterministic test suite verifying end-to-end single-turn voice orchestration:
- Explicit user invocation -> Audio capture -> Local STT -> Orchestrator -> Local TTS -> Playback -> Completed turn.
- Explicit output controls: text-only, synthesis-only, synthesize-and-play.
- Fault isolation: synthesis or playback failures NEVER erase or corrupt successful command responses.
- Language preservation: English, Tamil limitation handling, Tanglish, and Mixed language integrity.
- Lifecycle, concurrency, and cancellation: lock acquisition, rejection events, cleanup in finally.
- Trading safety invariants: locked execution engine, uninitialized trading engine, rejection of trading verbs.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional
import pytest

from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, OrchestratorResponse
from app.orchestrator.registry import HandlerRegistry, create_system_status_handler
from app.bootstrap import bootstrap_system
from app.voice import (
    AudioCaptureConfig,
    AudioCaptureService,
    AudioDeviceInfo,
    AudioDeviceManager,
    AudioPlaybackResult,
    AudioPlaybackService,
    AudioPlaybackStatus,
    CapturedAudio,
    MockAudioCaptureBackend,
    MockAudioDeviceBackend,
    MockAudioPlaybackBackend,
    MockSTTProvider,
    MockTTSProvider,
    Pyttsx3Config,
    Pyttsx3TTSProvider,
    SynthesisResult,
    TranscriptionResult,
    TTSUnsupportedLanguageError,
    VoiceInput,
    VoiceOutput,
    VoiceOutputStatus,
    VoiceService,
    VoiceSessionManager,
    VoiceTurnResult,
    VoiceTurnStatus,
)


# ==============================================================================
# DETERMINISTIC TEST HELPERS & FIXTURES
# ==============================================================================


class MockSapiVoice:
    """Mock pyttsx3 voice descriptor."""

    def __init__(self, voice_id: str, name: str, languages: Optional[List[str]] = None) -> None:
        self.id = voice_id
        self.name = name
        self.languages = languages or ["en-US"]
        self.gender = "female"
        self.age = "adult"


class MockPyttsx3Engine:
    """Deterministic mock pyttsx3 engine for testing."""

    def __init__(self, voices: Optional[List[MockSapiVoice]] = None) -> None:
        self.voices = voices or [
            MockSapiVoice("voice_david", "Microsoft David - English (United States)", ["en-US"]),
            MockSapiVoice("voice_zira", "Microsoft Zira - English (United States)", ["en-US"]),
        ]
        self.properties: Dict[str, Any] = {
            "voices": self.voices,
            "rate": 175,
            "volume": 1.0,
            "voice": "voice_david",
        }
        self.save_calls: List[tuple[str, str]] = []
        self.should_fail = False
        self.failure_message = "Mock engine synthesis failure"

    def getProperty(self, name: str) -> Any:
        return self.properties.get(name)

    def setProperty(self, name: str, value: Any) -> None:
        self.properties[name] = value

    def save_to_file(self, text: str, filename: str) -> None:
        if self.should_fail:
            raise RuntimeError(self.failure_message)
        self.save_calls.append((text, filename))

        # Generate minimal valid WAV file
        import wave
        with wave.open(filename, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(22050)
            wf.writeframes(b"\x00\x00" * 2205)  # 0.1s of silence

    def runAndWait(self) -> None:
        if self.should_fail:
            raise RuntimeError(self.failure_message)


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_unified_bus")


@pytest.fixture
def state_store() -> StateStore:
    store = StateStore(name="test_unified_store")
    store.set("execution_engine", "locked")
    store.set("trading_engine", "not initialized")
    return store


@pytest.fixture
def mock_devices() -> List[AudioDeviceInfo]:
    return [
        AudioDeviceInfo(
            device_id=0,
            name="Mock Studio Microphone",
            max_input_channels=1,
            default_sample_rate=16000.0,
            is_default=True,
            extra_info={"kind": "mic", "max_output_channels": 0},
        ),
        AudioDeviceInfo(
            device_id=1,
            name="Mock Output Speakers",
            max_input_channels=0,
            default_sample_rate=44100.0,
            is_default=True,
            extra_info={"kind": "speaker", "max_output_channels": 2},
        ),
    ]


@pytest.fixture
def device_manager(mock_devices: List[AudioDeviceInfo]) -> AudioDeviceManager:
    backend = MockAudioDeviceBackend(
        devices=mock_devices,
        default_input_id=0,
        default_output_id=1,
    )
    return AudioDeviceManager(backend=backend)


@pytest.fixture
def mock_capture_backend() -> MockAudioCaptureBackend:
    return MockAudioCaptureBackend()


@pytest.fixture
def audio_capture(
    mock_capture_backend: MockAudioCaptureBackend,
    device_manager: AudioDeviceManager,
    event_bus: EventBus,
    state_store: StateStore,
) -> AudioCaptureService:
    return AudioCaptureService(
        backend=mock_capture_backend,
        device_manager=device_manager,
        event_bus=event_bus,
        state_store=state_store,
        name="test_unified_capture",
    )


@pytest.fixture
def mock_playback_backend() -> MockAudioPlaybackBackend:
    return MockAudioPlaybackBackend()


@pytest.fixture
def audio_playback(
    mock_playback_backend: MockAudioPlaybackBackend,
    device_manager: AudioDeviceManager,
    event_bus: EventBus,
    state_store: StateStore,
) -> AudioPlaybackService:
    return AudioPlaybackService(
        backend=mock_playback_backend,
        device_manager=device_manager,
        event_bus=event_bus,
        state_store=state_store,
        name="test_unified_playback",
    )


@pytest.fixture
def orchestrator(event_bus: EventBus, state_store: StateStore) -> MasterOrchestrator:
    settings = Settings(app_env="test", voice_enabled=True, execution_locked=True)
    registry = HandlerRegistry(name="test_unified_registry")
    registry.register(
        command="system_status",
        handler=create_system_status_handler(state_store=state_store, settings=settings),
        description="Status diagnostics",
        capability="diagnostics",
    )

    def failing_handler(req: OrchestratorRequest) -> OrchestratorResponse:
        raise RuntimeError("Internal diagnostics engine malfunction")

    registry.register(
        command="fail_command",
        handler=failing_handler,
        description="Handler that always fails",
        capability="diagnostics",
    )

    return MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        registry=registry,
        settings=settings,
        name="test_unified_orchestrator",
    )


@pytest.fixture
def mock_engine() -> MockPyttsx3Engine:
    return MockPyttsx3Engine()


@pytest.fixture
def tts_provider(mock_engine: MockPyttsx3Engine) -> Pyttsx3TTSProvider:
    config = Pyttsx3Config(rate=175, volume=1.0)
    return Pyttsx3TTSProvider(config=config, engine=mock_engine, name="pyttsx3")


@pytest.fixture
def stt_provider() -> MockSTTProvider:
    provider = MockSTTProvider()
    provider.register_fixture(
        "fixture:status",
        TranscriptionResult(text="system_status", language="en", confidence=0.98),
    )
    return provider


@pytest.fixture
def voice_service(
    orchestrator: MasterOrchestrator,
    stt_provider: MockSTTProvider,
    tts_provider: Pyttsx3TTSProvider,
    audio_capture: AudioCaptureService,
    audio_playback: AudioPlaybackService,
    event_bus: EventBus,
    state_store: StateStore,
) -> VoiceService:
    return VoiceService(
        orchestrator=orchestrator,
        stt_provider=stt_provider,
        tts_provider=tts_provider,
        audio_capture=audio_capture,
        audio_playback=audio_playback,
        event_bus=event_bus,
        state_store=state_store,
        name="test_unified_voice_service",
    )


# ==============================================================================
# 1. UNIFIED VOICE TURN PIPELINE & EXPLICIT OUTPUT CONTROLS
# ==============================================================================


class TestUnifiedVoiceTurnPipeline:
    """Test suite for full single-turn voice orchestration and explicit output controls."""

    def test_full_turn_text_only_mode(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
        mock_engine: MockPyttsx3Engine,
    ) -> None:
        """Verify explicit user invocation -> capture -> STT -> orchestrator -> text output.

        TTS and Playback must NOT be invoked in text-only mode.
        """
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en", confidence=0.95),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=False,
            play_output=False,
            correlation_id="corr_text_only",
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.is_success is True
        assert turn.transcription_status == "success"
        assert turn.command_status == "success"
        assert turn.synthesis_status == "skipped"
        assert turn.playback_status == "skipped"
        assert turn.recognized_text == "system_status"
        assert "operational" in turn.response_text.lower()
        assert turn.command_response is not None
        assert turn.command_response.status == VoiceOutputStatus.TEXT_ONLY
        assert turn.command_response.has_audio is False

        # Verify TTS engine and playback backend were NOT invoked
        assert len(mock_engine.save_calls) == 0
        assert mock_playback_backend.play_count == 0

    def test_full_turn_synthesis_only_mode(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
        mock_engine: MockPyttsx3Engine,
    ) -> None:
        """Verify turn with synthesize_output=True synthesizes audio without physical playback."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en", confidence=0.95),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=False,
            correlation_id="corr_synth_only",
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.is_success is True
        assert turn.command_status == "success"
        assert turn.synthesis_status == "completed"
        assert turn.playback_status == "skipped"
        assert turn.command_response is not None
        assert turn.command_response.status == VoiceOutputStatus.SYNTHESIZED
        assert turn.command_response.has_audio is True
        assert len(turn.command_response.audio_payload) > 0

        # Verify TTS engine was called, but playback backend was NOT invoked
        assert len(mock_engine.save_calls) == 1
        assert mock_playback_backend.play_count == 0

    def test_full_turn_synthesize_and_play_mode(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
        mock_engine: MockPyttsx3Engine,
    ) -> None:
        """Verify turn with play_output=True completes capture, STT, command, synthesis, and playback."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en", confidence=0.95),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
            correlation_id="corr_full_play",
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.is_success is True
        assert turn.command_status == "success"
        assert turn.synthesis_status == "completed"
        assert turn.playback_status == "completed"
        assert turn.command_response is not None
        assert turn.command_response.has_audio is True

        # Verify both TTS and playback backends were invoked
        assert len(mock_engine.save_calls) == 1
        assert mock_playback_backend.play_count == 1

    def test_play_output_auto_enables_synthesis(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        """Verify setting play_output=True automatically synthesizes audio without needing synthesize_output=True."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=False,
            play_output=True,
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.synthesis_status == "completed"
        assert turn.playback_status == "completed"
        assert mock_playback_backend.play_count == 1


# ==============================================================================
# 2. FAULT ISOLATION: SYNTHESIS & PLAYBACK FAILURES NEVER ERASE COMMAND RESPONSE
# ==============================================================================


class TestUnifiedVoiceTurnFaultIsolation:
    """Test suite ensuring downstream TTS or playback failures do not erase command responses."""

    def test_unsupported_tamil_voice_preserves_command_response(
        self,
        voice_service: VoiceService,
        event_bus: EventBus,
    ) -> None:
        """Verify requesting Tamil synthesis when Tamil voice is absent preserves the response text.

        The command response must NOT be replaced with an error.
        VOICE_SYNTHESIS_UNSUPPORTED event must be published.
        """
        events: List[Event] = []
        event_bus.subscribe_all(lambda ev: events.append(ev))

        # Register Tamil speech transcription
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="ta"),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            language="ta",
            synthesize_output=True,
            play_output=False,
            correlation_id="corr_ta_unsupported",
        )

        # Turn completed with command response intact
        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.is_success is True
        assert turn.command_status == "success"
        assert turn.synthesis_status == "unsupported"
        assert turn.playback_status == "skipped"
        assert "not supported" in (turn.synthesis_error or "").lower()

        # Command response text is fully preserved
        assert "operational" in turn.response_text.lower()
        assert turn.command_response is not None
        assert turn.command_response.status == VoiceOutputStatus.TEXT_ONLY
        assert turn.command_response.has_audio is False

        # Verify VOICE_SYNTHESIS_UNSUPPORTED event was emitted
        event_types = [ev.event_type for ev in events]
        assert EventType.VOICE_SYNTHESIS_UNSUPPORTED in event_types

    def test_tts_engine_crash_preserves_command_response(
        self,
        voice_service: VoiceService,
        mock_engine: MockPyttsx3Engine,
    ) -> None:
        """Verify that a fatal crash in the TTS engine preserves the successful command response."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        mock_engine.should_fail = True
        mock_engine.failure_message = "Segmentation fault in SAPI5 core"

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=False,
            correlation_id="corr_tts_crash",
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.is_success is True
        assert turn.command_status == "success"
        assert turn.synthesis_status == "failed"
        assert "Segmentation fault" in (turn.synthesis_error or "")
        assert "operational" in turn.response_text.lower()
        assert turn.command_response is not None
        assert turn.command_response.text == turn.response_text

    def test_playback_failure_preserves_command_response(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        """Verify that playback failure preserves the synthesized audio and response text."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        mock_playback_backend.set_failure_mode(True, message="Hardware audio output device disconnect")

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
            correlation_id="corr_play_fail",
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.is_success is True
        assert turn.command_status == "success"
        assert turn.synthesis_status == "completed"
        assert turn.playback_status == "failed"
        assert "disconnect" in (turn.playback_error or "")
        assert "operational" in turn.response_text.lower()
        assert turn.command_response is not None
        assert turn.command_response.has_audio is True

    def test_capture_failure_skips_orchestrator_and_synthesis(
        self,
        voice_service: VoiceService,
        mock_capture_backend: MockAudioCaptureBackend,
        mock_engine: MockPyttsx3Engine,
    ) -> None:
        """Verify capture failure does not fabricate a command or execute synthesis."""
        mock_capture_backend.set_failure_mode(True, message="Microphone driver failure")

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
        )

        assert turn.status == VoiceTurnStatus.CAPTURE_FAILED
        assert turn.is_success is False
        assert turn.transcription_status == "skipped"
        assert turn.command_status == "skipped"
        assert turn.synthesis_status == "skipped"
        assert turn.playback_status == "skipped"
        assert len(mock_engine.save_calls) == 0

    def test_empty_speech_skips_orchestrator_and_synthesis(
        self,
        voice_service: VoiceService,
        mock_engine: MockPyttsx3Engine,
    ) -> None:
        """Verify silent audio or empty transcription skips orchestrator routing and synthesis."""
        # STT returns empty text
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="   ", language="en"),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
        )

        assert turn.status == VoiceTurnStatus.EMPTY_SPEECH
        assert turn.is_success is False
        assert turn.command_status == "skipped"
        assert turn.synthesis_status == "skipped"
        assert turn.playback_status == "skipped"
        assert len(mock_engine.save_calls) == 0

    def test_unknown_command_rejected_skips_synthesis_and_playback(
        self,
        voice_service: VoiceService,
        mock_engine: MockPyttsx3Engine,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        """Verify unrecognized command is rejected and does NOT invoke TTS or playback."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="launch_nuclear_missiles", language="en"),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
        )

        assert turn.status == VoiceTurnStatus.COMMAND_REJECTED
        assert turn.is_success is False
        assert turn.command_status == "rejected"
        assert turn.synthesis_status == "skipped"
        assert turn.playback_status == "skipped"
        assert "Unknown command" in (turn.error or "")
        assert len(mock_engine.save_calls) == 0
        assert mock_playback_backend.play_count == 0

    def test_handler_exception_skips_synthesis_and_playback(
        self,
        voice_service: VoiceService,
        mock_engine: MockPyttsx3Engine,
    ) -> None:
        """Verify handler failure results in FAILED status and skips synthesis."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="fail_command", language="en"),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
        )

        assert turn.status == VoiceTurnStatus.FAILED
        assert turn.is_success is False
        assert turn.command_status == "failed"
        assert turn.synthesis_status == "skipped"
        assert turn.playback_status == "skipped"
        assert len(mock_engine.save_calls) == 0


# ==============================================================================
# 3. LIFECYCLE, CONCURRENCY, CANCELLATION & CLEANUP
# ==============================================================================


class TestUnifiedVoiceTurnLifecycleAndConcurrency:
    """Test suite for lifecycle guarantees, concurrency lock, cancellation, and cleanup."""

    def test_concurrent_turn_rejected_with_event(
        self,
        voice_service: VoiceService,
        event_bus: EventBus,
    ) -> None:
        """Verify overlapping voice turn is immediately rejected and emits VOICE_TURN_REJECTED."""
        events: List[Event] = []
        event_bus.subscribe_all(lambda ev: events.append(ev))

        turn_1_entered = threading.Event()
        turn_1_can_finish = threading.Event()
        results: List[VoiceTurnResult] = []

        def worker_turn_1() -> None:
            voice_service.stt_provider.register_fixture(
                "audio:32000",
                TranscriptionResult(text="system_status", language="en"),
            )
            turn_1_entered.set()
            # Hold lock until signaled
            with voice_service._turn_lock:
                turn_1_can_finish.wait(timeout=3.0)

        t1 = threading.Thread(target=worker_turn_1)
        t1.start()

        assert turn_1_entered.wait(timeout=2.0)
        time.sleep(0.05)  # Ensure thread 1 holds lock

        # Attempt concurrent turn
        overlap_turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            correlation_id="corr_overlap",
        )

        assert overlap_turn.status == VoiceTurnStatus.CONCURRENCY_ERROR
        assert overlap_turn.is_success is False
        assert overlap_turn.command_status == "skipped"

        # Verify rejection event emitted
        event_types = [ev.event_type for ev in events]
        assert EventType.VOICE_TURN_REJECTED in event_types

        # Release worker 1
        turn_1_can_finish.set()
        t1.join(timeout=2.0)

        # Subsequent turn must succeed
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        turn_subsequent = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert turn_subsequent.status == VoiceTurnStatus.COMPLETED

    def test_closed_session_rejection_emits_event(
        self,
        voice_service: VoiceService,
        event_bus: EventBus,
    ) -> None:
        """Verify turn targeting a closed session emits VOICE_TURN_REJECTED and is rejected."""
        events: List[Event] = []
        event_bus.subscribe_all(lambda ev: events.append(ev))

        sess = voice_service.create_session("sess_to_close_step11")
        voice_service.close_session(sess.session_id)

        turn = voice_service.execute_voice_turn(session_id=sess.session_id)
        assert turn.status == VoiceTurnStatus.FAILED
        assert "is closed" in (turn.error or "")

        event_types = [ev.event_type for ev in events]
        assert EventType.VOICE_TURN_REJECTED in event_types

    def test_cancellation_during_playback(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify cancellation during playback returns playback_status='cancelled' while preserving response."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )

        stop_evt = threading.Event()
        stop_evt.set()  # Signal immediate cancellation

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
            stop_event=stop_evt,
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.command_status == "success"
        assert turn.synthesis_status == "completed"
        assert turn.playback_status == "cancelled"
        assert "operational" in turn.response_text.lower()

    def test_cleanup_in_finally_releases_turn_lock(
        self,
        voice_service: VoiceService,
        mock_capture_backend: MockAudioCaptureBackend,
    ) -> None:
        """Verify that _turn_lock is released even when fatal unhandled exceptions occur."""
        mock_capture_backend.set_failure_mode(True, message="Fatal hardware lock")

        turn = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert turn.status == VoiceTurnStatus.CAPTURE_FAILED
        assert voice_service._turn_lock.locked() is False

    def test_sequential_turns_no_state_leakage(
        self,
        voice_service: VoiceService,
    ) -> None:
        """Verify 5 consecutive turns execute without state cross-contamination or correlation leakage."""
        for i in range(5):
            corr = f"corr_seq_{i}"
            voice_service.stt_provider.register_fixture(
                "audio:32000",
                TranscriptionResult(text="system_status", language="en"),
            )
            turn = voice_service.execute_voice_turn(
                duration_seconds=1.0,
                correlation_id=corr,
            )
            assert turn.status == VoiceTurnStatus.COMPLETED
            assert turn.correlation_id == corr
            assert turn.command_status == "success"


# ==============================================================================
# 4. EVENTS & CORRELATION ID PROPAGATION
# ==============================================================================


class TestUnifiedVoiceTurnEventsAndCorrelation:
    """Test suite for correlation ID propagation and strict event lifecycle ordering."""

    def test_correlation_id_propagated_to_all_events(
        self,
        voice_service: VoiceService,
        event_bus: EventBus,
    ) -> None:
        """Verify caller correlation_id appears on every lifecycle event in the turn."""
        events: List[Event] = []
        event_bus.subscribe_all(lambda ev: events.append(ev))

        test_corr_id = "corr_e2e_unified_test_99"
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
            correlation_id=test_corr_id,
        )

        assert turn.correlation_id == test_corr_id

        # Verify all emitted events carry the correlation_id
        for ev in events:
            assert ev.correlation_id == test_corr_id

    def test_full_pipeline_event_ordering(
        self,
        voice_service: VoiceService,
        event_bus: EventBus,
    ) -> None:
        """Verify lifecycle events are published in the correct pipeline order."""
        event_sequence: List[str] = []
        event_bus.subscribe_all(lambda ev: event_sequence.append(ev.event_type))

        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )

        voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
        )

        # Check key events exist in sequence
        assert EventType.VOICE_TURN_STARTED.value in event_sequence
        assert EventType.AUDIO_CAPTURE_STARTED.value in event_sequence
        assert EventType.AUDIO_CAPTURE_COMPLETED.value in event_sequence
        assert EventType.VOICE_TRANSCRIPTION_STARTED.value in event_sequence
        assert EventType.VOICE_TRANSCRIPTION_COMPLETED.value in event_sequence
        assert EventType.VOICE_SYNTHESIS_STARTED.value in event_sequence
        assert EventType.VOICE_SYNTHESIS_COMPLETED.value in event_sequence
        assert EventType.AUDIO_PLAYBACK_STARTED.value in event_sequence
        assert EventType.AUDIO_PLAYBACK_COMPLETED.value in event_sequence
        assert EventType.VOICE_TURN_COMPLETED.value in event_sequence

        # Verify relative index ordering
        idx_turn_start = event_sequence.index(EventType.VOICE_TURN_STARTED.value)
        idx_cap_start = event_sequence.index(EventType.AUDIO_CAPTURE_STARTED.value)
        idx_stt_comp = event_sequence.index(EventType.VOICE_TRANSCRIPTION_COMPLETED.value)
        idx_synth_comp = event_sequence.index(EventType.VOICE_SYNTHESIS_COMPLETED.value)
        idx_play_comp = event_sequence.index(EventType.AUDIO_PLAYBACK_COMPLETED.value)
        idx_turn_comp = event_sequence.index(EventType.VOICE_TURN_COMPLETED.value)

        assert idx_turn_start < idx_cap_start < idx_stt_comp < idx_synth_comp < idx_play_comp < idx_turn_comp


# ==============================================================================
# 5. LANGUAGE PRESERVATION & METADATA
# ==============================================================================


class TestUnifiedVoiceTurnLanguageHandling:
    """Test suite for language preservation and honesty."""

    def test_english_turn_preservation(self, voice_service: VoiceService) -> None:
        """Verify English language metadata is preserved."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        turn = voice_service.execute_voice_turn(duration_seconds=1.0, language="en")
        assert turn.language == "en"
        assert turn.language_hint == "en"

    def test_tanglish_turn_preservation(self, voice_service: VoiceService) -> None:
        """Verify Tanglish language metadata is preserved without automated translation."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="tanglish"),
        )
        turn = voice_service.execute_voice_turn(duration_seconds=1.0, language="tanglish")
        assert turn.language == "tanglish"
        assert turn.recognized_text == "system_status"

    def test_mixed_language_turn_preservation(self, voice_service: VoiceService) -> None:
        """Verify Mixed language metadata is preserved."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="mixed"),
        )
        turn = voice_service.execute_voice_turn(duration_seconds=1.0, language="mixed")
        assert turn.language == "mixed"


# ==============================================================================
# 6. TRADING SAFETY INVARIANTS
# ==============================================================================


class TestUnifiedVoiceTurnTradingSafety:
    """Test suite verifying voice recognition never serves as financial authorization."""

    def test_no_audio_playback_or_capture_at_startup(self) -> None:
        """Verify bootstrap initiates zero audio captures or playback operations."""
        settings = Settings(app_env="test", voice_enabled=True, execution_locked=True)
        status = bootstrap_system(settings=settings, log_to_file=False)

        assert status.initialized is True
        assert status.audio_capture is not None
        assert status.audio_capture.counters["total_captures"] == 0
        assert status.audio_playback is not None
        assert status.audio_playback.counters["total_playbacks"] == 0
        assert status.execution_engine == "locked"
        assert status.trading_engine == "not initialized"

    def test_execution_engine_locked_before_during_and_after_turn(
        self,
        voice_service: VoiceService,
        state_store: StateStore,
    ) -> None:
        """Verify execution engine remains locked through all stages of voice execution."""
        assert state_store.get("execution_engine") == "locked"

        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=True,
        )

        assert turn.status == VoiceTurnStatus.COMPLETED
        assert state_store.get("execution_engine") == "locked"
        assert state_store.get("trading_engine") == "not initialized"

    @pytest.mark.parametrize("trading_word", ["buy", "sell", "confirm", "execute", "ok"])
    def test_spoken_trading_words_rejected_and_cannot_authorize(
        self,
        voice_service: VoiceService,
        state_store: StateStore,
        trading_word: str,
    ) -> None:
        """Verify spoken single-word or multi-word trading attempts are rejected.

        Execution engine must remain strictly locked.
        """
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text=f"{trading_word} 100 shares of reliance", language="en"),
        )

        turn = voice_service.execute_voice_turn(duration_seconds=1.0)
        assert turn.status == VoiceTurnStatus.COMMAND_REJECTED
        assert turn.is_success is False
        assert turn.command_status == "rejected"
        assert "Unknown command" in (turn.error or "")

        # Execution engine MUST remain locked
        assert state_store.get("execution_engine") == "locked"
        assert state_store.get("trading_engine") == "not initialized"
