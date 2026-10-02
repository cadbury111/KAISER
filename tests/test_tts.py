"""KAISER — Text-to-Speech (TTS) & Controlled Audio Playback (Step 10) Test Suite.

Comprehensive tests covering:
1. Pyttsx3Config & TTS Exception Hierarchy validation, bounds, and immutability.
2. Local Pyttsx3TTSProvider interface compliance, deterministic mock engine synthesis,
   and real offline SAPI5 execution.
3. Multilingual metadata preservation (English, Tamil, Tanglish, Mixed) and honest
   unsupported-language error reporting.
4. AudioPlayback models, backends, device discovery, and format validation.
5. AudioPlaybackService non-overlapping concurrency protection, cancellation, and cleanup.
6. VoiceService separate synthesis and playback operations, and turn integration.
7. EventBus lifecycle dispatch and StateStore bounded telemetry without raw audio retention.
8. Safety invariants: zero audio playback at startup, execution engine remains locked,
   and trading engine remains uninitialized.
"""

from __future__ import annotations

import io
import struct
import threading
import time
import wave
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

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
    AudioCaptureService,
    AudioDeviceInfo,
    AudioDeviceManager,
    AudioPlaybackBackend,
    AudioPlaybackCancelledError,
    AudioPlaybackConcurrencyError,
    AudioPlaybackDeviceNotFoundError,
    AudioPlaybackError,
    AudioPlaybackFormatError,
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
    SoundDeviceBackend,
    SoundDevicePlaybackBackend,
    SynthesisResult,
    TextToSpeechProvider,
    TranscriptionResult,
    TTSDependencyError,
    TTSEmptyTextError,
    TTSError,
    TTSSynthesisError,
    TTSTextTooLongError,
    TTSTimeoutError,
    TTSUnsupportedLanguageError,
    TTSVoiceNotFoundError,
    VoiceInput,
    VoiceOutput,
    VoiceOutputStatus,
    VoiceProviderError,
    VoiceService,
    VoiceSessionManager,
    VoiceTurnResult,
    VoiceTurnStatus,
)


# ==============================================================================
# DETERMINISTIC TEST HELPERS & MOCKS
# ==============================================================================


class MockVoice:
    """Mock pyttsx3 voice descriptor."""

    def __init__(self, voice_id: str, name: str, languages: Optional[List[str]] = None) -> None:
        self.id = voice_id
        self.name = name
        self.languages = languages or ["en-US"]
        self.gender = "female"
        self.age = "adult"


class MockPyttsx3Engine:
    """Deterministic mock pyttsx3 engine for headless, offline testing."""

    def __init__(self, voices: Optional[List[MockVoice]] = None) -> None:
        self.voices = voices or [
            MockVoice("voice_david", "Microsoft David - English (United States)", ["en-US"]),
            MockVoice("voice_zira", "Microsoft Zira - English (United States)", ["en-US"]),
            MockVoice("voice_hazel", "Microsoft Hazel - English (Great Britain)", ["en-GB"]),
        ]
        self.properties: Dict[str, Any] = {
            "voices": self.voices,
            "rate": 175,
            "volume": 1.0,
            "voice": "voice_david",
        }
        self.save_calls: List[tuple[str, str]] = []
        self.should_fail = False
        self.failure_message = "Simulated pyttsx3 engine crash"

    def getProperty(self, name: str) -> Any:
        return self.properties.get(name)

    def setProperty(self, name: str, value: Any) -> None:
        self.properties[name] = value

    def save_to_file(self, text: str, filename: str) -> None:
        if self.should_fail:
            raise RuntimeError(self.failure_message)
        self.save_calls.append((text, filename))

        # Generate a valid, minimal deterministic WAV audio file
        sample_rate = 22050
        duration_sec = max(0.1, len(text.split()) * 0.15)
        num_frames = int(sample_rate * duration_sec)
        pcm_samples = struct.pack(f"<{num_frames}h", *([0] * num_frames))

        with wave.open(filename, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(sample_rate)
            wf.writeframes(pcm_samples)

    def runAndWait(self) -> None:
        if self.should_fail:
            raise RuntimeError(self.failure_message)


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_tts_bus")


@pytest.fixture
def state_store() -> StateStore:
    store = StateStore(name="test_tts_store")
    store.set("execution_engine", "locked")
    store.set("trading_engine", "not initialized")
    return store


@pytest.fixture
def mock_output_devices() -> List[AudioDeviceInfo]:
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
        AudioDeviceInfo(
            device_id=2,
            name="Mock USB Headphones",
            max_input_channels=0,
            default_sample_rate=48000.0,
            is_default=False,
            extra_info={"kind": "headphones", "max_output_channels": 2},
        ),
    ]


@pytest.fixture
def device_manager(mock_output_devices: List[AudioDeviceInfo]) -> AudioDeviceManager:
    backend = MockAudioDeviceBackend(
        devices=mock_output_devices,
        default_input_id=0,
        default_output_id=1,
    )
    return AudioDeviceManager(backend=backend)


@pytest.fixture
def mock_playback_backend() -> MockAudioPlaybackBackend:
    return MockAudioPlaybackBackend()


@pytest.fixture
def playback_service(
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
        name="test_playback_service",
    )


@pytest.fixture
def orchestrator(event_bus: EventBus, state_store: StateStore) -> MasterOrchestrator:
    settings = Settings(app_env="test", voice_enabled=True, execution_locked=True)
    registry = HandlerRegistry(name="test_tts_registry")
    registry.register(
        command="system_status",
        handler=create_system_status_handler(state_store=state_store, settings=settings),
        description="Status diagnostics",
        capability="diagnostics",
    )
    return MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        registry=registry,
        settings=settings,
        name="test_tts_orchestrator",
    )


@pytest.fixture
def mock_pyttsx3_engine() -> MockPyttsx3Engine:
    return MockPyttsx3Engine()


@pytest.fixture
def pyttsx3_provider(mock_pyttsx3_engine: MockPyttsx3Engine) -> Pyttsx3TTSProvider:
    config = Pyttsx3Config(rate=175, volume=1.0)
    return Pyttsx3TTSProvider(config=config, engine=mock_pyttsx3_engine, name="pyttsx3")


@pytest.fixture
def voice_service(
    orchestrator: MasterOrchestrator,
    pyttsx3_provider: Pyttsx3TTSProvider,
    playback_service: AudioPlaybackService,
    event_bus: EventBus,
    state_store: StateStore,
) -> VoiceService:
    mock_stt = MockSTTProvider(name="mock_stt", strict=False)
    mock_capture = AudioCaptureService(
        backend=MockAudioCaptureBackend(),
        event_bus=event_bus,
        state_store=state_store,
        name="test_voice_capture",
    )
    return VoiceService(
        orchestrator=orchestrator,
        stt_provider=mock_stt,
        tts_provider=pyttsx3_provider,
        audio_capture=mock_capture,
        audio_playback=playback_service,
        event_bus=event_bus,
        state_store=state_store,
        name="test_voice_service",
    )


# ==============================================================================
# 1. PYTTSX3 MODELS & CONFIGURATION TESTS
# ==============================================================================


class TestPyttsx3ModelsAndConfig:
    """Test suite for Pyttsx3Config validation, bounds, and exception hierarchy."""

    def test_valid_config(self) -> None:
        cfg = Pyttsx3Config(rate=180, volume=0.8, timeout_seconds=15.0, max_text_length=3000)
        assert cfg.rate == 180
        assert cfg.volume == 0.8
        assert cfg.timeout_seconds == 15.0
        assert cfg.max_text_length == 3000
        assert cfg.allow_network is False

    def test_config_rate_bounds(self) -> None:
        with pytest.raises(ValidationError, match="rate must be an integer between 50 and 500"):
            Pyttsx3Config(rate=30)
        with pytest.raises(ValidationError, match="rate must be an integer between 50 and 500"):
            Pyttsx3Config(rate=600)

    def test_config_volume_bounds(self) -> None:
        with pytest.raises(ValidationError, match="volume must be a float between 0.0 and 1.0"):
            Pyttsx3Config(volume=-0.1)
        with pytest.raises(ValidationError, match="volume must be a float between 0.0 and 1.0"):
            Pyttsx3Config(volume=1.5)

    def test_config_timeout_bounds(self) -> None:
        with pytest.raises(ValidationError, match="timeout_seconds must be positive"):
            Pyttsx3Config(timeout_seconds=0.0)

    def test_config_max_text_bounds(self) -> None:
        with pytest.raises(ValidationError, match="max_text_length must be a positive integer"):
            Pyttsx3Config(max_text_length=0)

    def test_strict_offline_invariant(self) -> None:
        """Verify that allow_network=True is strictly rejected by configuration."""
        with pytest.raises(ValidationError, match="allow_network must be False"):
            Pyttsx3Config(allow_network=True)

    def test_exception_hierarchy(self) -> None:
        assert issubclass(TTSError, VoiceProviderError)
        assert issubclass(TTSDependencyError, TTSError)
        assert issubclass(TTSVoiceNotFoundError, TTSError)
        assert issubclass(TTSSynthesisError, TTSError)
        assert issubclass(TTSTimeoutError, TTSError)
        assert issubclass(TTSEmptyTextError, TTSError)
        assert issubclass(TTSUnsupportedLanguageError, TTSError)
        assert issubclass(TTSTextTooLongError, TTSError)


# ==============================================================================
# 2. LOCAL PYTTSX3 PROVIDER TESTS
# ==============================================================================


class TestPyttsx3TTSProvider:
    """Test suite for Pyttsx3TTSProvider synthesis, voices, and error handling."""

    def test_interface_compliance(self, pyttsx3_provider: Pyttsx3TTSProvider) -> None:
        assert isinstance(pyttsx3_provider, TextToSpeechProvider)
        assert pyttsx3_provider.name == "pyttsx3"
        assert pyttsx3_provider.is_available is True

    def test_successful_synthesis_metadata(self, pyttsx3_provider: Pyttsx3TTSProvider) -> None:
        """Verify successful WAV audio synthesis with parsed metadata."""
        result = pyttsx3_provider.synthesize(text="KAISER system is ready.", language="en")
        assert isinstance(result, SynthesisResult)
        assert result.audio_format == "wav"
        assert result.sample_rate == 22050
        assert result.duration_ms > 0
        assert result.language == "en"
        assert result.text == "KAISER system is ready."
        assert len(result.audio_bytes) > 0
        assert result.metadata["offline"] is True
        assert result.metadata["channels"] == 1

    def test_empty_text_rejection(self, pyttsx3_provider: Pyttsx3TTSProvider) -> None:
        with pytest.raises(TTSEmptyTextError, match="Cannot synthesize empty or whitespace-only text"):
            pyttsx3_provider.synthesize(text="")
        with pytest.raises(TTSEmptyTextError, match="Cannot synthesize empty or whitespace-only text"):
            pyttsx3_provider.synthesize(text="   \t\n  ")

    def test_oversized_text_rejection(self, mock_pyttsx3_engine: MockPyttsx3Engine) -> None:
        cfg = Pyttsx3Config(max_text_length=50)
        provider = Pyttsx3TTSProvider(config=cfg, engine=mock_pyttsx3_engine)
        with pytest.raises(TTSTextTooLongError, match="Input text exceeds maximum allowed length"):
            provider.synthesize(text="A" * 60)

    def test_voice_listing_and_selection(self, pyttsx3_provider: Pyttsx3TTSProvider) -> None:
        voices = pyttsx3_provider.list_voices()
        assert len(voices) == 3
        assert any(v["id"] == "voice_david" for v in voices)

        # Synthesize with explicit valid voice
        res = pyttsx3_provider.synthesize("Hello", voice_id="voice_david")
        assert res.metadata["voice_id"] == "voice_david"

    def test_missing_voice_error(self, pyttsx3_provider: Pyttsx3TTSProvider) -> None:
        with pytest.raises(TTSVoiceNotFoundError, match="was not found among installed voices"):
            pyttsx3_provider.synthesize("Hello", voice_id="non_existent_voice_id_xyz")

    def test_unsupported_language_tamil_without_voice(self, pyttsx3_provider: Pyttsx3TTSProvider) -> None:
        """Verify that requesting Tamil without an installed Tamil voice raises structured error."""
        with pytest.raises(TTSUnsupportedLanguageError, match="no Tamil speech synthesis voice was found"):
            pyttsx3_provider.synthesize("வணக்கம்", language="ta")

    def test_tanglish_metadata_preservation(self, pyttsx3_provider: Pyttsx3TTSProvider) -> None:
        """Verify Tanglish passes original text to English voice and documents pronunciation honestly."""
        res = pyttsx3_provider.synthesize(text="system eppadi irukku", language="tanglish")
        assert res.text == "system eppadi irukku"
        assert res.language == "tanglish"
        assert res.metadata["engine_language"] == "en"
        assert "pronunciation_note" in res.metadata

    def test_mixed_language_metadata_preservation(self, pyttsx3_provider: Pyttsx3TTSProvider) -> None:
        res = pyttsx3_provider.synthesize(text="market status check pannu", language="mixed")
        assert res.text == "market status check pannu"
        assert res.language == "mixed"
        assert res.metadata["engine_language"] == "en"

    def test_engine_failure_wrapped(
        self,
        pyttsx3_provider: Pyttsx3TTSProvider,
        mock_pyttsx3_engine: MockPyttsx3Engine,
    ) -> None:
        mock_pyttsx3_engine.should_fail = True
        with pytest.raises(TTSSynthesisError, match="pyttsx3 synthesis execution failed"):
            pyttsx3_provider.synthesize("Will fail")

    def test_real_pyttsx3_smoke_test(self) -> None:
        """Verify real installed pyttsx3 on host system without mocks."""
        real_provider = Pyttsx3TTSProvider()
        if not real_provider.is_available:
            pytest.skip("pyttsx3 or SAPI5 voice drivers not available on this host.")

        res = real_provider.synthesize(text="KAISER voice test.")
        assert res.audio_format == "wav"
        assert len(res.audio_bytes) > 0
        assert res.duration_ms > 0


# ==============================================================================
# 3. AUDIO PLAYBACK MODELS & BACKEND TESTS
# ==============================================================================


class TestAudioPlaybackModelsAndBackends:
    """Test suite for AudioPlaybackResult, backends, and device identification."""

    def test_playback_status_enum(self) -> None:
        expected = {"IDLE", "PLAYING", "COMPLETED", "CANCELLED", "FAILED"}
        actual = {s.value for s in AudioPlaybackStatus}
        assert expected == actual

    def test_playback_result_factories(self) -> None:
        comp = AudioPlaybackResult.completed(
            playback_id="p1",
            correlation_id="c1",
            device_id=1,
            device_name="Speakers",
            sample_rate=44100,
            channels=2,
            duration_ms=500.0,
        )
        assert comp.is_success is True
        assert comp.status == AudioPlaybackStatus.COMPLETED

        canc = AudioPlaybackResult.cancelled(
            playback_id="p2",
            correlation_id="c2",
            device_id=1,
            device_name="Speakers",
        )
        assert canc.is_success is False
        assert canc.status == AudioPlaybackStatus.CANCELLED

        fail = AudioPlaybackResult.failure(
            playback_id="p3",
            correlation_id="c3",
            error="Device error",
        )
        assert fail.is_success is False
        assert fail.status == AudioPlaybackStatus.FAILED
        assert fail.error == "Device error"

    def test_device_info_output_detection(self) -> None:
        mic = AudioDeviceInfo(device_id=0, name="Mic", max_input_channels=1, default_sample_rate=16000.0)
        assert mic.is_input_device is True
        assert mic.is_output_device is False

        spk = AudioDeviceInfo(
            device_id=1,
            name="Speakers",
            max_input_channels=0,
            default_sample_rate=44100.0,
            extra_info={"max_output_channels": 2},
        )
        assert spk.is_input_device is False
        assert spk.is_output_device is True

    def test_mock_playback_backend_lifecycle(self) -> None:
        backend = MockAudioPlaybackBackend()
        assert backend.name == "mock_playback"
        assert backend.is_available is True
        assert backend.play_count == 0

        backend.play(b"\x00\x00" * 100, sample_rate=16000, channels=1, device_id=1)
        assert backend.play_count == 1
        assert backend.last_played_bytes is not None

        # Failure mode
        backend.set_failure_mode(True, message="Hardware blown")
        assert backend.is_available is False
        with pytest.raises(AudioPlaybackError, match="Hardware blown"):
            backend.play(b"\x00\x00", sample_rate=16000, channels=1)


# ==============================================================================
# 4. AUDIO PLAYBACK SERVICE TESTS
# ==============================================================================


class TestAudioPlaybackService:
    """Test suite for AudioPlaybackService operations, validation, and safety."""

    def test_successful_wav_playback(
        self,
        playback_service: AudioPlaybackService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        # Create minimal valid WAV bytes
        buf = io.BytesIO()
        with wave.open(buf, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(16000)
            wf.writeframes(b"\x00\x00" * 800)
        wav_bytes = buf.getvalue()

        result = playback_service.play(audio_data=wav_bytes, correlation_id="play_corr_1")
        assert result.status == AudioPlaybackStatus.COMPLETED
        assert result.is_success is True
        assert result.correlation_id == "play_corr_1"
        assert mock_playback_backend.play_count == 1

    def test_synthesis_result_playback(
        self,
        playback_service: AudioPlaybackService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        synth = SynthesisResult(
            audio_bytes=b"\x00\x01" * 1600,
            audio_format="mock_pcm",
            sample_rate=16000,
            duration_ms=100.0,
            text="hello",
        )
        res = playback_service.play(synth)
        assert res.status == AudioPlaybackStatus.COMPLETED
        assert mock_playback_backend.play_count == 1

    def test_voice_output_with_audio_playback(
        self,
        playback_service: AudioPlaybackService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        output = VoiceOutput.synthesized(
            text="System online.",
            audio_payload=b"\x00\x00" * 3200,
            session_id="s1",
            duration_ms=200.0,
        )
        res = playback_service.play(output)
        assert res.status == AudioPlaybackStatus.COMPLETED
        assert res.is_success is True

    def test_voice_output_without_audio_rejected(
        self,
        playback_service: AudioPlaybackService,
    ) -> None:
        text_out = VoiceOutput.text_only(text="No audio here", session_id="s1")
        with pytest.raises(AudioPlaybackFormatError, match="does not contain synthesized audio payload"):
            playback_service.play(text_out)

    def test_empty_payload_rejected(self, playback_service: AudioPlaybackService) -> None:
        with pytest.raises(AudioPlaybackFormatError, match="Audio payload is empty"):
            playback_service.play(b"")

    def test_oversized_payload_rejected(self, playback_service: AudioPlaybackService) -> None:
        huge = b"\x00" * (11 * 1024 * 1024)
        with pytest.raises(AudioPlaybackFormatError, match="exceeds maximum allowed size"):
            playback_service.play(huge)

    def test_invalid_device_rejected(self, playback_service: AudioPlaybackService) -> None:
        # Device 0 is Microphone (no output channels)
        with pytest.raises(AudioPlaybackError, match="does not support audio output"):
            playback_service.play(b"\x00\x00" * 100, device_id=0)

        # Device 99 does not exist
        with pytest.raises(AudioPlaybackDeviceNotFoundError, match="was not found among available devices"):
            playback_service.play(b"\x00\x00" * 100, device_id=99)

    def test_overlapping_playback_concurrency_protection(
        self,
        playback_service: AudioPlaybackService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        mock_playback_backend.set_delay(0.2)
        barrier = threading.Barrier(2)
        errors: List[Exception] = []

        def worker():
            barrier.wait()
            try:
                playback_service.play(b"\x00\x00" * 100)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 1
        assert isinstance(errors[0], AudioPlaybackConcurrencyError)
        assert "Overlapping playback is prohibited" in str(errors[0])

    def test_playback_cancellation(
        self,
        playback_service: AudioPlaybackService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        mock_playback_backend.set_delay(0.5)
        stop_event = threading.Event()

        def cancel_after_start():
            time.sleep(0.02)
            stop_event.set()

        canceller = threading.Thread(target=cancel_after_start)
        canceller.start()

        res = playback_service.play(b"\x00\x00" * 500, stop_event=stop_event)
        canceller.join()

        assert res.status == AudioPlaybackStatus.CANCELLED
        assert res.is_success is False


# ==============================================================================
# 5. VOICESERVICE SYNTHESIS & PLAYBACK INTEGRATION TESTS
# ==============================================================================


class TestVoiceServiceSynthesisAndPlayback:
    """Test suite for separate synthesis and playback methods on VoiceService."""

    def test_synthesize_text_separate_from_playback(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        """Verify synthesize_text produces audio without playing it."""
        res = voice_service.synthesize_text(text="Status check.", language="en")
        assert isinstance(res, SynthesisResult)
        assert res.text == "Status check."
        assert len(res.audio_bytes) > 0
        # Verify playback backend was NOT invoked
        assert mock_playback_backend.play_count == 0

    def test_speak_output_synthesizes_and_plays(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        """Verify speak_output synthesizes text-only output and plays it."""
        output = VoiceOutput.text_only(text="Ready to execute diagnostics.", session_id="s1")
        assert output.has_audio is False

        res = voice_service.speak_output(output)
        assert res.status == AudioPlaybackStatus.COMPLETED
        assert mock_playback_backend.play_count == 1

    def test_execute_voice_turn_with_play_output(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        """Verify execute_voice_turn with play_output=True executes synthesis and playback."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            play_output=True,
            correlation_id="turn_play_corr_1",
        )
        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.is_success is True
        assert turn.command_response.has_audio is True
        assert turn.metadata.get("playback_status") == "COMPLETED"
        assert mock_playback_backend.play_count == 1

    def test_execute_voice_turn_synthesize_only_no_playback(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        """Verify synthesize_output=True synthesizes audio without calling playback."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )

        turn = voice_service.execute_voice_turn(
            duration_seconds=1.0,
            synthesize_output=True,
            play_output=False,
        )
        assert turn.status == VoiceTurnStatus.COMPLETED
        assert turn.command_response.has_audio is True
        assert mock_playback_backend.play_count == 0

    def test_playback_failure_does_not_corrupt_subsequent_turns(
        self,
        voice_service: VoiceService,
        mock_playback_backend: MockAudioPlaybackBackend,
    ) -> None:
        """Verify failure in playback does not break subsequent turns or leave locks held."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )

        # Trigger playback failure
        mock_playback_backend.set_failure_mode(True, message="Speaker driver fault")
        turn_1 = voice_service.execute_voice_turn(duration_seconds=1.0, play_output=True)
        assert turn_1.status == VoiceTurnStatus.COMPLETED
        assert turn_1.metadata.get("playback_status") is None

        # Reset failure mode
        mock_playback_backend.set_failure_mode(False)
        turn_2 = voice_service.execute_voice_turn(duration_seconds=1.0, play_output=True)
        assert turn_2.status == VoiceTurnStatus.COMPLETED
        assert turn_2.metadata.get("playback_status") == "COMPLETED"


# ==============================================================================
# 6. EVENTS, TELEMETRY & STATSTORE TESTS
# ==============================================================================


class TestTTSEventsAndTelemetry:
    """Test suite for synthesis/playback event publication and StateStore boundedness."""

    def test_synthesis_and_playback_events_emitted(
        self,
        voice_service: VoiceService,
        event_bus: EventBus,
    ) -> None:
        events: List[Event] = []
        event_bus.subscribe_all(lambda ev: events.append(ev))

        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        voice_service.execute_voice_turn(
            duration_seconds=1.0,
            play_output=True,
            correlation_id="corr_events_test",
        )

        event_types = [ev.event_type for ev in events]
        assert EventType.VOICE_SYNTHESIS_STARTED in event_types
        assert EventType.VOICE_SYNTHESIS_COMPLETED in event_types
        assert EventType.AUDIO_PLAYBACK_STARTED in event_types
        assert EventType.AUDIO_PLAYBACK_COMPLETED in event_types

        # Verify correlation ID propagation
        playback_ev = [ev for ev in events if ev.event_type == EventType.AUDIO_PLAYBACK_COMPLETED][0]
        assert playback_ev.correlation_id == "corr_events_test"

    def test_statestore_retains_bounded_metrics_only(
        self,
        voice_service: VoiceService,
        state_store: StateStore,
    ) -> None:
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        voice_service.execute_voice_turn(duration_seconds=1.0, play_output=True)

        provider_status = state_store.get("voice_provider_status")
        assert provider_status["tts_provider"] == "pyttsx3"
        assert provider_status["playback_available"] is True

        last_playback = state_store.get("audio_playback_last")
        assert last_playback is not None
        assert last_playback["status"] == "COMPLETED"
        assert "audio_bytes" not in last_playback
        assert "pcm_bytes" not in last_playback


# ==============================================================================
# 7. SAFETY & TRADING INVARIANTS
# ==============================================================================


class TestTTSSafetyAndTradingInvariants:
    """Test suite ensuring strict trading isolation and silence on startup."""

    def test_no_audio_playback_at_startup(self) -> None:
        """Verify bootstrapping KAISER is completely silent and never initiates playback."""
        settings = Settings(app_env="test", voice_enabled=True, tts_provider="mock")
        status = bootstrap_system(settings=settings, log_to_file=False)

        assert status.initialized is True
        assert status.voice_service is not None
        assert status.audio_playback is not None
        assert status.audio_playback.counters["total_playbacks"] == 0

    def test_execution_engine_remains_locked_during_synthesis_and_playback(
        self,
        voice_service: VoiceService,
        state_store: StateStore,
    ) -> None:
        """Verify that synthesis and playback never unlock the execution engine."""
        voice_service.stt_provider.register_fixture(
            "audio:32000",
            TranscriptionResult(text="system_status", language="en"),
        )
        voice_service.execute_voice_turn(duration_seconds=1.0, play_output=True)

        assert state_store.get("execution_engine") == "locked"
        assert state_store.get("trading_engine") == "not initialized"
