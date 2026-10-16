"""KAISER — Local Speech-to-Text (STT) Test Suite (Step 08).

Comprehensive tests covering:
1. STT interface compliance (SpeechToTextProvider, LocalWhisperSTTProvider, MockSTTProvider).
2. Audio payload validation and memory bounding (validate_audio_payload).
3. Audio unpacking, multi-channel downmixing, resampling, and silence detection.
4. LocalSTTConfig validation and offline invariants (allow_download=False).
5. Missing dependencies (STTDependencyError) and missing models (STTModelNotFoundError).
6. Deterministic inference with injected engine, calibrated confidence, and metadata preservation.
7. Multilingual handling and hints ('en', 'ta', 'tanglish', 'mixed').
8. Explicit empty transcription outcomes on silence and empty recognized text.
9. VoiceService integration, correlation ID propagation, and lifecycle events.
10. StateStore boundedness and zero-audio-history verification.
11. System bootstrap integration and safety invariants.
"""

from __future__ import annotations

import math
import os
import threading
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from app.bootstrap import SystemStatus, bootstrap_system
from app.core.config import Settings, get_settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator import MasterOrchestrator
from app.voice import (
    AudioCaptureConfig,
    CapturedAudio,
    LocalSTTConfig,
    LocalWhisperSTTProvider,
    MockAudioCaptureProvider,
    MockSTTProvider,
    MockTTSProvider,
    STTAudioConversionError,
    STTAudioValidationError,
    STTDependencyError,
    STTEmptyTranscriptionError,
    STTError,
    STTInferenceError,
    STTModelNotFoundError,
    SpeechToTextProvider,
    TranscriptionResult,
    VoiceOutput,
    VoiceOutputStatus,
    VoiceService,
    calculate_rms_amplitude,
    convert_to_whisper_pcm_float32,
    is_silence,
    validate_audio_payload,
)


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    """Provide a clean EventBus."""
    return EventBus(name="test_stt_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    """Provide a clean StateStore."""
    return StateStore(name="test_stt_state_store")


@pytest.fixture
def orchestrator(event_bus: EventBus, state_store: StateStore) -> MasterOrchestrator:
    """Provide a MasterOrchestrator with system_status handler."""
    return MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        name="test_orchestrator",
    )


class MockWhisperSegment:
    """Mock Segment object simulating faster-whisper output."""

    def __init__(self, text: str, avg_logprob: float = -0.05) -> None:
        self.text = text
        self.avg_logprob = avg_logprob


class MockWhisperInfo:
    """Mock Info object simulating faster-whisper output."""

    def __init__(self, language: str = "en", language_probability: float = 0.98) -> None:
        self.language = language
        self.language_probability = language_probability


class MockWhisperEngine:
    """Deterministic mock inference engine simulating faster-whisper WhisperModel."""

    def __init__(
        self,
        default_text: str = "system status",
        language: str = "en",
        avg_logprob: float = -0.05,
    ) -> None:
        self.default_text = default_text
        self.language = language
        self.avg_logprob = avg_logprob
        self.transcribe_called = False
        self.last_audio = None
        self.last_kwargs = {}
        self.should_raise: Optional[Exception] = None

    def transcribe(self, audio: np.ndarray, **kwargs: Any) -> Any:
        self.transcribe_called = True
        self.last_audio = audio
        self.last_kwargs = kwargs

        if self.should_raise:
            raise self.should_raise

        segments = [MockWhisperSegment(text=self.default_text, avg_logprob=self.avg_logprob)]
        info = MockWhisperInfo(language=self.language)
        return iter(segments), info


@pytest.fixture
def mock_engine() -> MockWhisperEngine:
    """Provide a deterministic MockWhisperEngine."""
    return MockWhisperEngine(default_text="system_status", language="en")


@pytest.fixture
def sample_captured_audio() -> CapturedAudio:
    """Provide a realistic 16kHz 16-bit mono CapturedAudio with synthetic sine wave."""
    provider = MockAudioCaptureProvider()
    pcm = provider.generate_synthetic_pcm(
        sample_rate=16000,
        channels=1,
        duration_seconds=0.5,
        sample_width_bytes=2,
    )
    return CapturedAudio(
        audio_bytes=pcm,
        sample_rate=16000,
        channels=1,
        sample_width_bytes=2,
        duration_seconds=0.5,
        device_id=0,
        device_name="Test Mic",
    )


# ==============================================================================
# 1. STT INTERFACE COMPLIANCE
# ==============================================================================


class TestSTTInterfaceCompliance:
    """Tests verifying interface contracts and polymorphism."""

    def test_mock_stt_implements_interface(self) -> None:
        """Verify MockSTTProvider adheres to SpeechToTextProvider ABC."""
        provider = MockSTTProvider()
        assert isinstance(provider, SpeechToTextProvider)
        assert provider.name == "mock_stt"
        assert provider.is_available is True

    def test_local_whisper_implements_interface(self, mock_engine: MockWhisperEngine) -> None:
        """Verify LocalWhisperSTTProvider adheres to SpeechToTextProvider ABC."""
        provider = LocalWhisperSTTProvider(engine=mock_engine)
        assert isinstance(provider, SpeechToTextProvider)
        assert provider.name == "local_whisper"
        assert provider.is_available is True


# ==============================================================================
# 2. AUDIO VALIDATION & CONVERSION PIPELINE
# ==============================================================================


class TestAudioValidationAndConversion:
    """Tests for validating audio payloads and converting to normalized float32 samples."""

    def test_validate_audio_payload_success(self, sample_captured_audio: CapturedAudio) -> None:
        """Verify valid CapturedAudio passes validation unchanged."""
        validated = validate_audio_payload(sample_captured_audio)
        assert validated is sample_captured_audio

    def test_validate_raw_bytes_payload(self) -> None:
        """Verify raw PCM bytes are automatically wrapped into CapturedAudio."""
        raw_pcm = b"\x00\x00" * 8000  # 0.5s at 16kHz
        validated = validate_audio_payload(raw_pcm)
        assert isinstance(validated, CapturedAudio)
        assert validated.sample_rate == 16000
        assert validated.channels == 1
        assert validated.byte_count == 16000

    def test_validate_rejects_none_or_empty(self) -> None:
        """Verify None and empty payloads are rejected."""
        with pytest.raises(STTAudioValidationError, match="cannot be None"):
            validate_audio_payload(None)

        with pytest.raises(STTAudioValidationError, match="buffer is empty"):
            validate_audio_payload(b"")

    def test_validate_rejects_buffer_overflow(self) -> None:
        """Verify payloads exceeding max_buffer_bytes are rejected."""
        huge_pcm = b"\x00" * 2000
        with pytest.raises(STTAudioValidationError, match="exceeds maximum buffer limit"):
            validate_audio_payload(huge_pcm, max_buffer_bytes=1000)

    def test_validate_rejects_out_of_bounds_duration(self) -> None:
        """Verify durations below min or above max limits are rejected."""
        short_audio = CapturedAudio(
            audio_bytes=b"\x00\x00" * 400,  # 0.025s
            sample_rate=16000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=0.025,
        )
        with pytest.raises(STTAudioValidationError, match="below minimum duration"):
            validate_audio_payload(short_audio, min_duration_seconds=0.1)

    def test_convert_16bit_pcm_mono(self, sample_captured_audio: CapturedAudio) -> None:
        """Verify 16-bit signed PCM is converted to normalized float32."""
        samples, meta = convert_to_whisper_pcm_float32(sample_captured_audio)
        assert isinstance(samples, np.ndarray)
        assert samples.dtype == np.float32
        assert len(samples) == 8000
        assert meta["target_sample_rate"] == 16000
        assert meta["downmixed"] is False
        assert meta["resampled"] is False
        assert meta["is_silence"] is False
        assert np.all(samples >= -1.0) and np.all(samples <= 1.0)

    def test_convert_multi_channel_downmixing(self) -> None:
        """Verify stereo (2-channel) audio is correctly averaged to mono."""
        # 4 stereo frames of 16-bit PCM: left=16384 (0.5), right=32767 (1.0)
        # Average should be ~0.75
        frame = np.array([16384, 32767], dtype=np.int16).tobytes()
        audio = CapturedAudio(
            audio_bytes=frame * 1000,
            sample_rate=16000,
            channels=2,
            sample_width_bytes=2,
            duration_seconds=1000 / 16000.0,
        )
        samples, meta = convert_to_whisper_pcm_float32(audio)
        assert meta["downmixed"] is True
        assert meta["original_channels"] == 2
        assert len(samples) == 1000
        # Mean value should be ~0.75
        assert np.isclose(np.mean(samples), 0.75, atol=0.02)

    def test_convert_resampling(self) -> None:
        """Verify audio sampled at 8000 Hz is resampled to 16000 Hz."""
        provider = MockAudioCaptureProvider()
        pcm_8k = provider.generate_synthetic_pcm(
            sample_rate=8000,
            channels=1,
            duration_seconds=0.25,
            sample_width_bytes=2,
        )
        audio = CapturedAudio(
            audio_bytes=pcm_8k,
            sample_rate=8000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=0.25,
        )
        samples, meta = convert_to_whisper_pcm_float32(audio, target_sample_rate=16000)
        assert meta["resampled"] is True
        assert meta["original_sample_rate"] == 8000
        # 0.25s at 16kHz = 4000 samples
        assert len(samples) == 4000

    def test_silence_detection(self) -> None:
        """Verify pure flatline silence is detected."""
        silent_pcm = b"\x00\x00" * 8000  # 0.5s of 0 amplitude
        audio = CapturedAudio(
            audio_bytes=silent_pcm,
            sample_rate=16000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=0.5,
        )
        samples, meta = convert_to_whisper_pcm_float32(audio, silence_threshold_rms=0.005)
        assert meta["is_silence"] is True
        assert meta["rms_amplitude"] == 0.0
        assert is_silence(samples) is True


# ==============================================================================
# 3. LOCAL STT CONFIG & OFFLINE GUARANTEES
# ==============================================================================


class TestLocalSTTConfigAndGuarantees:
    """Tests verifying configuration bounds and strict offline invariants."""

    def test_config_defaults_and_offline_invariant(self) -> None:
        """Verify allow_download is False by default to prevent silent downloads."""
        cfg = LocalSTTConfig()
        assert cfg.allow_download is False
        assert cfg.device == "cpu"
        assert cfg.compute_type == "int8"
        assert cfg.beam_size == 5

    def test_config_validation_errors(self) -> None:
        """Verify invalid configuration values are rejected."""
        with pytest.raises(ValidationError, match="device"):
            LocalSTTConfig(device="tpu")

        with pytest.raises(ValidationError, match="compute_type"):
            LocalSTTConfig(compute_type="invalid_compute")

        with pytest.raises(ValidationError, match="beam_size"):
            LocalSTTConfig(beam_size=0)

        with pytest.raises(ValidationError, match="timeout_seconds"):
            LocalSTTConfig(timeout_seconds=-5.0)

    def test_missing_dependency_raises_structured_error(self) -> None:
        """Verify missing faster-whisper raises STTDependencyError without crashing."""
        provider = LocalWhisperSTTProvider()

        # Mock import failure of faster_whisper
        with patch.dict("sys.modules", {"faster_whisper": None}):
            with pytest.raises(STTDependencyError, match="faster-whisper"):
                provider._ensure_model_loaded()

    def test_missing_model_path_raises_structured_error(self) -> None:
        """Verify missing local model path raises STTModelNotFoundError."""
        cfg = LocalSTTConfig(model_path="C:/non_existent/whisper_weights")
        provider = LocalWhisperSTTProvider(config=cfg)

        with patch.dict("sys.modules", {"faster_whisper": MagicMock()}):
            with pytest.raises(STTModelNotFoundError, match="Silent model downloads"):
                provider._ensure_model_loaded()


# ==============================================================================
# 4. DETERMINISTIC INFERENCE WITH INJECTED ENGINE
# ==============================================================================


class TestDeterministicInference:
    """Tests verifying model inference, confidence calculation, and metadata."""

    def test_successful_transcription(
        self,
        mock_engine: MockWhisperEngine,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify transcribing valid audio yields structured TranscriptionResult."""
        provider = LocalWhisperSTTProvider(engine=mock_engine)

        result = provider.transcribe(sample_captured_audio, language="en")

        assert isinstance(result, TranscriptionResult)
        assert result.text == "system_status"
        assert result.language == "en"
        assert result.is_final is True
        # Calibrated confidence: e^(-0.05) ~ 0.9512
        assert 0.90 <= result.confidence <= 1.0
        assert result.metadata["provider"] == "local_whisper"
        assert result.metadata["confidence_is_calibrated"] is True
        assert mock_engine.transcribe_called is True
        assert mock_engine.last_kwargs["task"] == "transcribe"

    def test_silence_raises_empty_transcription_error(self) -> None:
        """Verify flatline audio is rejected as STTEmptyTranscriptionError without model inference."""
        engine = MockWhisperEngine()
        provider = LocalWhisperSTTProvider(engine=engine)

        silent_pcm = b"\x00\x00" * 8000
        silent_audio = CapturedAudio(
            audio_bytes=silent_pcm,
            sample_rate=16000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=0.5,
        )

        with pytest.raises(STTEmptyTranscriptionError, match="silence threshold"):
            provider.transcribe(silent_audio)

        # Model should NOT have been called for silent audio!
        assert engine.transcribe_called is False

    def test_model_empty_segments_raises_empty_error(
        self,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify model returning empty text segments raises STTEmptyTranscriptionError."""
        engine = MockWhisperEngine(default_text="")  # Empty output
        provider = LocalWhisperSTTProvider(engine=engine)

        with pytest.raises(STTEmptyTranscriptionError, match="empty or unintelligible"):
            provider.transcribe(sample_captured_audio)

    def test_engine_inference_error_propagation(
        self,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify runtime exceptions inside model are wrapped in STTInferenceError."""
        engine = MockWhisperEngine()
        engine.should_raise = RuntimeError("CUDA out of memory in driver")
        provider = LocalWhisperSTTProvider(engine=engine)

        with pytest.raises(STTInferenceError, match="CUDA out of memory"):
            provider.transcribe(sample_captured_audio)


# ==============================================================================
# 5. MULTILINGUAL HANDLING & LANGUAGE HINTS
# ==============================================================================


class TestMultilingualHandling:
    """Tests verifying KAISER language metadata conventions across STT inference."""

    def test_english_language_hint(
        self,
        mock_engine: MockWhisperEngine,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify 'en' language hint is forwarded to model."""
        provider = LocalWhisperSTTProvider(engine=mock_engine)
        res = provider.transcribe(sample_captured_audio, language="en")
        assert res.language == "en"
        assert mock_engine.last_kwargs["language"] == "en"

    def test_tamil_language_hint(
        self,
        mock_engine: MockWhisperEngine,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify 'ta' language hint is forwarded to model."""
        provider = LocalWhisperSTTProvider(engine=mock_engine)
        res = provider.transcribe(sample_captured_audio, language="ta")
        assert res.language == "ta"
        assert mock_engine.last_kwargs["language"] == "ta"

    def test_tanglish_language_hint_preservation(
        self,
        mock_engine: MockWhisperEngine,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify 'tanglish' hint prompts model with 'ta' while preserving 'tanglish' tag in result."""
        provider = LocalWhisperSTTProvider(engine=mock_engine)
        res = provider.transcribe(sample_captured_audio, language="tanglish")
        # Result metadata retains 'tanglish'
        assert res.language == "tanglish"
        assert res.metadata["preserved_language_tag"] == "tanglish"
        assert res.metadata["caller_language_hint"] == "tanglish"
        # Model receives 'ta' phonetics prompt
        assert mock_engine.last_kwargs["language"] == "ta"

    def test_mixed_language_hint_triggers_auto_detect(
        self,
        mock_engine: MockWhisperEngine,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify 'mixed' hint triggers auto-detection (None) in model while preserving 'mixed' tag."""
        provider = LocalWhisperSTTProvider(engine=mock_engine)
        res = provider.transcribe(sample_captured_audio, language="mixed")
        assert res.language == "mixed"
        assert mock_engine.last_kwargs["language"] is None

    def test_no_translation_task_invariant(
        self,
        mock_engine: MockWhisperEngine,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify task is strictly 'transcribe' and never 'translate'."""
        provider = LocalWhisperSTTProvider(engine=mock_engine)
        provider.transcribe(sample_captured_audio, language="ta")
        assert mock_engine.last_kwargs["task"] == "transcribe"
        assert mock_engine.last_kwargs["task"] != "translate"


# ==============================================================================
# 6. VOICESERVICE INTEGRATION & LIFECYCLE EVENTS
# ==============================================================================


class TestVoiceServiceSTTIntegration:
    """Tests verifying VoiceService integration with LocalWhisperSTTProvider."""

    def test_process_audio_end_to_end_with_local_whisper(
        self,
        mock_engine: MockWhisperEngine,
        orchestrator: MasterOrchestrator,
        event_bus: EventBus,
        state_store: StateStore,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify complete pipeline: CapturedAudio -> LocalWhisper -> VoiceInput -> Orchestrator."""
        stt = LocalWhisperSTTProvider(engine=mock_engine)
        tts = MockTTSProvider()

        events: List[Event] = []
        event_bus.subscribe(EventType.VOICE_TRANSCRIPTION_STARTED, events.append)
        event_bus.subscribe(EventType.VOICE_TRANSCRIPTION_COMPLETED, events.append)
        event_bus.subscribe(EventType.VOICE_RESPONSE_CREATED, events.append)

        voice_service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt,
            tts_provider=tts,
            event_bus=event_bus,
            state_store=state_store,
        )

        output = voice_service.process_audio(
            audio_data=sample_captured_audio,
            correlation_id="trace_stt_001",
        )

        assert output.is_success is True
        assert output.status == VoiceOutputStatus.TEXT_ONLY
        assert "System Status" in output.text or "not initialized" in output.text

        # Verify lifecycle events
        event_types = [e.event_type for e in events]
        assert EventType.VOICE_TRANSCRIPTION_STARTED in event_types
        assert EventType.VOICE_TRANSCRIPTION_COMPLETED in event_types
        assert EventType.VOICE_RESPONSE_CREATED in event_types

        # Verify correlation ID
        for ev in events:
            assert ev.correlation_id == "trace_stt_001"

    def test_empty_audio_emits_transcription_empty_event(
        self,
        orchestrator: MasterOrchestrator,
        event_bus: EventBus,
        state_store: StateStore,
    ) -> None:
        """Verify silent audio emits VOICE_TRANSCRIPTION_EMPTY and does not invoke orchestrator."""
        engine = MockWhisperEngine()
        stt = LocalWhisperSTTProvider(engine=engine)
        tts = MockTTSProvider()

        empty_events: List[Event] = []
        event_bus.subscribe(EventType.VOICE_TRANSCRIPTION_EMPTY, empty_events.append)

        voice_service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt,
            tts_provider=tts,
            event_bus=event_bus,
            state_store=state_store,
        )

        silent_audio = CapturedAudio(
            audio_bytes=b"\x00\x00" * 8000,
            sample_rate=16000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=0.5,
        )

        output = voice_service.process_audio(
            audio_data=silent_audio,
            correlation_id="empty_audio_trace",
        )

        assert output.is_success is False
        assert output.status == VoiceOutputStatus.FAILED
        assert "silence" in str(output.error).lower() or "empty" in str(output.error).lower()

        assert len(empty_events) == 1
        assert empty_events[0].correlation_id == "empty_audio_trace"

    def test_stt_failure_emits_transcription_failed_event(
        self,
        orchestrator: MasterOrchestrator,
        event_bus: EventBus,
        state_store: StateStore,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify engine crash emits VOICE_TRANSCRIPTION_FAILED and returns structured error."""
        engine = MockWhisperEngine()
        engine.should_raise = RuntimeError("GPU memory exhausted")
        stt = LocalWhisperSTTProvider(engine=engine)
        tts = MockTTSProvider()

        failed_events: List[Event] = []
        event_bus.subscribe(EventType.VOICE_TRANSCRIPTION_FAILED, failed_events.append)

        voice_service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt,
            tts_provider=tts,
            event_bus=event_bus,
            state_store=state_store,
        )

        output = voice_service.process_audio(
            audio_data=sample_captured_audio,
            correlation_id="fail_trace",
        )

        assert output.is_success is False
        assert "GPU memory exhausted" in str(output.error)

        assert len(failed_events) == 1
        assert failed_events[0].correlation_id == "fail_trace"
        assert failed_events[0].payload["error_type"] == "STTInferenceError"


# ==============================================================================
# 7. STATESTORE BOUNDEDNESS & TELEMETRY
# ==============================================================================


class TestStateStoreSTTBoundedness:
    """Tests verifying StateStore bounds and zero-audio-history invariant."""

    def test_statestore_holds_stt_telemetry(
        self,
        mock_engine: MockWhisperEngine,
        orchestrator: MasterOrchestrator,
        state_store: StateStore,
        sample_captured_audio: CapturedAudio,
    ) -> None:
        """Verify StateStore contains bounded STT duration and provider metrics."""
        stt = LocalWhisperSTTProvider(engine=mock_engine)
        service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt,
            state_store=state_store,
        )

        service.process_audio(sample_captured_audio)

        provider_status = state_store.get("voice_provider_status")
        assert provider_status is not None
        assert provider_status["stt_provider"] == "local_whisper"
        assert "last_stt_duration_seconds" in provider_status

        # Safety: verify NO raw audio or transcript logs in StateStore
        keys = state_store.keys()
        assert "audio_history" not in keys
        assert "transcripts" not in keys


# ==============================================================================
# 8. BOOTSTRAP INTEGRATION & SAFETY GUARANTEES
# ==============================================================================


class TestBootstrapSTTIntegration:
    """Tests verifying bootstrap integration and safety invariants."""

    def test_bootstrap_with_mock_stt_by_default(self) -> None:
        """Verify default bootstrap uses safe mock STT provider."""
        status = bootstrap_system(log_to_file=False)
        assert status.voice_service is not None
        assert status.voice_service.stt_provider is not None
        assert status.voice_service.stt_provider.name == "mock_stt"

        # Invariant checks
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"

    def test_bootstrap_with_local_whisper_configured(self) -> None:
        """Verify bootstrap instantiates LocalWhisperSTTProvider when configured."""
        custom_settings = Settings(
            stt_provider="local_whisper",
            stt_model_id="whisper-base",
            stt_device="cpu",
        )
        status = bootstrap_system(settings=custom_settings, log_to_file=False)

        assert status.voice_service is not None
        assert status.voice_service.stt_provider is not None
        assert status.voice_service.stt_provider.name == "local_whisper"
        # Since no model path is installed, is_available is False, but system boots safely
        assert status.voice_service.stt_provider.is_available is False

        # Invariant checks preserved
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
