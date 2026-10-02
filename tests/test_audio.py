"""KAISER — Local Audio Input Foundation Test Suite (Step 07).

Comprehensive tests covering:
1. Audio models: AudioDeviceInfo, AudioCaptureConfig, CapturedAudio validation & immutability
2. Device discovery: Mock & SoundDevice backends, input filtering, fallback logic, error isolation
3. No-microphone and invalid device error cases
4. Configuration boundary validations and pre-recording rejection
5. Capture duration bounds and buffer limits enforcement
6. Cancellation and guaranteed resource cleanup across all paths
7. Mock capture provider deterministic PCM generation & error simulation
8. EventBus lifecycle publication & subscriber failure isolation
9. StateStore boundedness and zero-audio-history invariant
10. VoiceService integration with explicit capture delegates
11. Bootstrap integration and safety invariants
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import FrozenInstanceError
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from app.bootstrap import SystemStatus, bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import KaiserError, ValidationError
from app.core.state import StateStore
from app.orchestrator import MasterOrchestrator
from app.voice import (
    AudioBufferOverflowError,
    AudioCaptureBackend,
    AudioCaptureConfig,
    AudioCaptureError,
    AudioCaptureHandle,
    AudioCaptureService,
    AudioCaptureStatus,
    AudioCaptureTimeoutError,
    AudioConfigurationError,
    AudioDeviceBackend,
    AudioDeviceError,
    AudioDeviceInfo,
    AudioDeviceManager,
    AudioDeviceNotFoundError,
    AudioError,
    AudioPermissionError,
    CapturedAudio,
    MockAudioCaptureBackend,
    MockAudioCaptureProvider,
    MockAudioDeviceBackend,
    MockSTTProvider,
    MockTTSProvider,
    SoundDeviceBackend,
    SoundDeviceCaptureBackend,
    VoiceService,
)


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    """Provide a clean EventBus."""
    return EventBus(name="test_audio_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    """Provide a clean StateStore."""
    return StateStore(name="test_audio_state_store")


@pytest.fixture
def mock_devices() -> List[AudioDeviceInfo]:
    """Provide standard deterministic mock audio devices."""
    return [
        AudioDeviceInfo(
            device_id=0,
            name="Default Studio Mic",
            max_input_channels=1,
            default_sample_rate=16000.0,
            is_default=True,
            hostapi="TestAPI",
            extra_info={"kind": "usb"},
        ),
        AudioDeviceInfo(
            device_id=1,
            name="Secondary Stereo Mic",
            max_input_channels=2,
            default_sample_rate=44100.0,
            is_default=False,
            hostapi="TestAPI",
        ),
        AudioDeviceInfo(
            device_id=2,
            name="Headphones Output Only",
            max_input_channels=0,
            default_sample_rate=48000.0,
            is_default=False,
            hostapi="TestAPI",
        ),
    ]


@pytest.fixture
def mock_device_backend(mock_devices: List[AudioDeviceInfo]) -> MockAudioDeviceBackend:
    """Provide a MockAudioDeviceBackend initialized with mock devices."""
    return MockAudioDeviceBackend(devices=mock_devices, default_input_id=0)


@pytest.fixture
def device_manager(
    mock_device_backend: MockAudioDeviceBackend,
    event_bus: EventBus,
    state_store: StateStore,
) -> AudioDeviceManager:
    """Provide an AudioDeviceManager with mock backend, EventBus, and StateStore."""
    return AudioDeviceManager(
        backend=mock_device_backend,
        event_bus=event_bus,
        state_store=state_store,
    )


@pytest.fixture
def mock_capture_provider() -> MockAudioCaptureProvider:
    """Provide a deterministic MockAudioCaptureProvider."""
    return MockAudioCaptureProvider(name="test_mock_capture")


@pytest.fixture
def capture_service(
    device_manager: AudioDeviceManager,
    mock_capture_provider: MockAudioCaptureProvider,
    event_bus: EventBus,
    state_store: StateStore,
) -> AudioCaptureService:
    """Provide an AudioCaptureService configured with mock components."""
    backend = MockAudioCaptureBackend(provider=mock_capture_provider)
    return AudioCaptureService(
        device_manager=device_manager,
        backend=backend,
        event_bus=event_bus,
        state_store=state_store,
        default_config=AudioCaptureConfig(
            sample_rate=16000,
            channels=1,
            max_duration_seconds=0.2,
            chunk_duration_ms=50,
        ),
    )


# ==============================================================================
# 1. AUDIO MODEL VALIDATION & IMMUTABILITY
# ==============================================================================


class TestAudioModels:
    """Tests for typed audio domain models, configuration bounds, and immutability."""

    def test_audio_device_info_valid(self) -> None:
        """Verify AudioDeviceInfo creation and property access."""
        dev = AudioDeviceInfo(
            device_id=1,
            name="USB Mic",
            max_input_channels=2,
            default_sample_rate=48000.0,
            is_default=True,
            hostapi="WASAPI",
            extra_info={"subsystem": "test"},
        )
        assert dev.device_id == 1
        assert dev.name == "USB Mic"
        assert dev.max_input_channels == 2
        assert dev.default_sample_rate == 48000.0
        assert dev.is_default is True
        assert dev.is_input_device is True
        assert dev.is_available is True
        assert dev.hostapi == "WASAPI"
        assert dev.extra_info["subsystem"] == "test"

    def test_audio_device_info_immutability(self) -> None:
        """Verify AudioDeviceInfo is frozen and rejects mutation."""
        dev = AudioDeviceInfo(
            device_id=0,
            name="Mic",
            max_input_channels=1,
            default_sample_rate=16000.0,
        )
        with pytest.raises(FrozenInstanceError):
            dev.name = "New Name"  # type: ignore

    def test_audio_device_info_invalid_fields(self) -> None:
        """Verify invalid device info parameters are rejected."""
        with pytest.raises(ValidationError):
            AudioDeviceInfo(device_id=-1, name="Mic", max_input_channels=1, default_sample_rate=16000.0)

        with pytest.raises(ValidationError):
            AudioDeviceInfo(device_id=0, name="", max_input_channels=1, default_sample_rate=16000.0)

        with pytest.raises(ValidationError):
            AudioDeviceInfo(device_id=0, name="Mic", max_input_channels=-1, default_sample_rate=16000.0)

        with pytest.raises(ValidationError):
            AudioDeviceInfo(device_id=0, name="Mic", max_input_channels=1, default_sample_rate=0.0)

    def test_audio_capture_config_defaults(self) -> None:
        """Verify default AudioCaptureConfig values and properties."""
        cfg = AudioCaptureConfig()
        assert cfg.sample_rate == 16000
        assert cfg.channels == 1
        assert cfg.sample_width_bytes == 2
        assert cfg.max_duration_seconds == 10.0
        assert cfg.chunk_duration_ms == 100
        assert cfg.bytes_per_second == 32000
        assert cfg.bytes_per_frame == 2
        assert cfg.chunk_samples == 1600
        assert cfg.chunk_bytes == 3200
        assert cfg.expected_max_bytes == 320000

    def test_audio_capture_config_bounds_validation(self) -> None:
        """Verify that out-of-bounds capture configuration is rejected before recording."""
        # Invalid sample rate
        with pytest.raises(AudioConfigurationError, match="sample_rate"):
            AudioCaptureConfig(sample_rate=4000)
        with pytest.raises(AudioConfigurationError, match="sample_rate"):
            AudioCaptureConfig(sample_rate=192000)

        # Invalid channels
        with pytest.raises(AudioConfigurationError, match="channels"):
            AudioCaptureConfig(channels=0)
        with pytest.raises(AudioConfigurationError, match="channels"):
            AudioCaptureConfig(channels=10)

        # Invalid sample width
        with pytest.raises(AudioConfigurationError, match="sample_width_bytes"):
            AudioCaptureConfig(sample_width_bytes=3)

        # Invalid duration
        with pytest.raises(AudioConfigurationError, match="max_duration_seconds"):
            AudioCaptureConfig(max_duration_seconds=0.05)
        with pytest.raises(AudioConfigurationError, match="max_duration_seconds"):
            AudioCaptureConfig(max_duration_seconds=120.0)

        # Invalid buffer size
        with pytest.raises(AudioConfigurationError, match="max_buffer_bytes"):
            AudioCaptureConfig(max_buffer_bytes=500)

        # Buffer smaller than expected duration
        with pytest.raises(AudioConfigurationError, match="exceeds max_buffer_bytes"):
            AudioCaptureConfig(
                sample_rate=48000,
                channels=2,
                sample_width_bytes=2,
                max_duration_seconds=30.0,
                max_buffer_bytes=100000,  # 30s * 192KB/s = 5.76MB > 100KB
            )

    def test_captured_audio_container_and_privacy(self) -> None:
        """Verify CapturedAudio calculation, immutability, and privacy-safe representations."""
        raw_pcm = b"\x00\x00" * 8000  # 8000 samples of 16-bit mono = 0.5s at 16000Hz
        captured = CapturedAudio(
            audio_bytes=raw_pcm,
            sample_rate=16000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=0.5,
            device_id=0,
            device_name="Test Mic",
        )
        assert captured.byte_count == 16000
        assert captured.num_samples == 8000
        assert captured.duration_ms == 500.0
        assert captured.is_empty is False

        # Privacy invariant: raw audio bytes must NOT appear in __repr__ or __str__
        rep = repr(captured)
        assert "duration=0.50s" in rep
        assert "bytes=16000" in rep
        assert "\\x00" not in rep

        # Privacy invariant: to_dict(include_audio_bytes=False) must redact raw samples
        dict_rep = captured.to_dict(include_audio_bytes=False)
        assert dict_rep["byte_count"] == 16000
        assert "REDACTED" in str(dict_rep["audio_bytes"])

        # Explicit export
        dict_with_raw = captured.to_dict(include_audio_bytes=True)
        assert dict_with_raw["audio_bytes"] == raw_pcm

        # Immutability
        with pytest.raises(FrozenInstanceError):
            captured.duration_seconds = 1.0  # type: ignore

    def test_audio_exception_hierarchy(self) -> None:
        """Verify that audio exceptions properly derive from KaiserError."""
        assert issubclass(AudioError, KaiserError)
        assert issubclass(AudioDeviceError, AudioError)
        assert issubclass(AudioDeviceNotFoundError, AudioDeviceError)
        assert issubclass(AudioCaptureError, AudioError)
        assert issubclass(AudioConfigurationError, AudioError)
        assert issubclass(AudioBufferOverflowError, AudioCaptureError)
        assert issubclass(AudioCaptureTimeoutError, AudioCaptureError)
        assert issubclass(AudioPermissionError, AudioCaptureError)


# ==============================================================================
# 2. AUDIO DEVICE DISCOVERY
# ==============================================================================


class TestAudioDeviceDiscovery:
    """Tests for discovering audio devices, error handling, and hardware isolation."""

    def test_mock_backend_discovery(self, device_manager: AudioDeviceManager) -> None:
        """Verify querying all devices and filtering input-only devices."""
        all_devs = device_manager.get_all_devices()
        assert len(all_devs) == 3

        input_devs = device_manager.get_input_devices()
        assert len(input_devs) == 2
        assert all(d.is_input_device for d in input_devs)

        default_dev = device_manager.get_default_input_device()
        assert default_dev is not None
        assert default_dev.device_id == 0
        assert default_dev.name == "Default Studio Mic"

    def test_device_lookup_by_id(self, device_manager: AudioDeviceManager) -> None:
        """Verify looking up devices by system ID."""
        dev = device_manager.get_device_by_id(1)
        assert dev is not None
        assert dev.name == "Secondary Stereo Mic"

        assert device_manager.get_device_by_id(999) is None

    def test_backend_failure_isolation(self, mock_device_backend: MockAudioDeviceBackend) -> None:
        """Verify that device discovery failures are safely isolated without crashing."""
        mock_device_backend.set_failure_mode(True, message="Audio hardware disconnected")
        manager = AudioDeviceManager(backend=mock_device_backend)

        # Refresh shouldn't raise exception, returns empty list
        devs = manager.refresh_devices()
        assert devs == []
        assert manager.has_input_device() is False

    def test_sounddevice_backend_graceful_handling(self) -> None:
        """Verify SoundDeviceBackend safely handles presence/absence of sounddevice."""
        backend = SoundDeviceBackend()
        # Even on systems without PortAudio, is_available reflects state safely
        if backend.is_available:
            devs = backend.query_devices()
            assert isinstance(devs, list)
        else:
            assert backend.query_devices() == []


# ==============================================================================
# 3. NO-MICROPHONE & INVALID DEVICE CASES
# ==============================================================================


class TestDeviceValidationEdgeCases:
    """Tests for validating audio devices under missing or invalid conditions."""

    def test_no_microphones_available(self, mock_device_backend: MockAudioDeviceBackend) -> None:
        """Verify AudioDeviceNotFoundError when no input devices exist."""
        # Configure backend with speaker-only device
        speaker_only = [
            AudioDeviceInfo(
                device_id=0,
                name="Speakers Only",
                max_input_channels=0,
                default_sample_rate=48000.0,
            )
        ]
        mock_device_backend.set_devices(speaker_only)
        manager = AudioDeviceManager(backend=mock_device_backend)

        with pytest.raises(AudioDeviceNotFoundError, match="No audio input device"):
            manager.validate_input_device(None)

    def test_invalid_device_id_requested(self, device_manager: AudioDeviceManager) -> None:
        """Verify requesting an invalid device index raises AudioDeviceNotFoundError."""
        with pytest.raises(AudioDeviceNotFoundError, match="device ID 999 was not found"):
            device_manager.validate_input_device(999)

    def test_non_input_device_requested(self, device_manager: AudioDeviceManager) -> None:
        """Verify requesting an output-only device raises AudioDeviceError."""
        # Device 2 has max_input_channels=0
        with pytest.raises(AudioDeviceError, match="does not support audio input"):
            device_manager.validate_input_device(2)

    def test_unavailable_device_requested(self, mock_device_backend: MockAudioDeviceBackend) -> None:
        """Verify requesting an unavailable device raises AudioDeviceError."""
        dev = AudioDeviceInfo(
            device_id=5,
            name="Disabled Mic",
            max_input_channels=1,
            default_sample_rate=16000.0,
            is_available=False,
        )
        mock_device_backend.set_devices([dev])
        manager = AudioDeviceManager(backend=mock_device_backend)

        with pytest.raises(AudioDeviceError, match="is currently unavailable"):
            manager.validate_input_device(5)


# ==============================================================================
# 4. AUDIO CAPTURE LIMITS & BUFFER BOUNDARIES
# ==============================================================================


class TestAudioCaptureBounds:
    """Tests verifying bounded recording, buffer boundaries, and duration limits."""

    def test_explicit_capture_success(self, capture_service: AudioCaptureService) -> None:
        """Verify explicit synchronous audio capture returns valid CapturedAudio."""
        result = capture_service.capture_audio(duration_seconds=0.1)

        assert isinstance(result, CapturedAudio)
        assert result.byte_count > 0
        assert result.duration_seconds >= 0.08
        assert result.sample_rate == 16000
        assert result.channels == 1
        assert result.sample_width_bytes == 2
        assert result.device_id == 0
        assert capture_service.counters["completed"] == 1

    def test_capture_custom_configuration(self, capture_service: AudioCaptureService) -> None:
        """Verify capturing with custom sample rate and duration overrides."""
        cfg = AudioCaptureConfig(
            sample_rate=22050,
            channels=2,
            sample_width_bytes=2,
            max_duration_seconds=0.1,
            chunk_duration_ms=25,
            device_id=1,
        )
        result = capture_service.capture_audio(config=cfg)

        assert result.sample_rate == 22050
        assert result.channels == 2
        assert result.device_id == 1
        assert result.byte_count > 0

    def test_buffer_overflow_prevention(
        self,
        capture_service: AudioCaptureService,
        mock_capture_provider: MockAudioCaptureProvider,
    ) -> None:
        """Verify that buffer overflow raises AudioBufferOverflowError and updates failure counter."""
        mock_capture_provider.set_failure_mode(
            True,
            exception_cls=AudioBufferOverflowError,
            message="Captured bytes exceeded maximum buffer boundary of 10000 bytes.",
        )
        with pytest.raises(AudioBufferOverflowError, match="maximum buffer boundary"):
            capture_service.capture_audio(duration_seconds=0.1)

        assert capture_service.counters["failed"] == 1


# ==============================================================================
# 5. CANCELLATION & RESOURCE CLEANUP
# ==============================================================================


class TestCancellationAndCleanup:
    """Tests for explicit recording cancellation and guaranteed resource release."""

    def test_async_capture_cancellation(
        self,
        capture_service: AudioCaptureService,
        mock_capture_provider: MockAudioCaptureProvider,
    ) -> None:
        """Verify cancelling an in-flight async recording cleans up resources."""
        cfg = AudioCaptureConfig(
            sample_rate=16000,
            channels=1,
            max_duration_seconds=1.0,  # 1 second duration
            chunk_duration_ms=20,
        )
        handle = capture_service.start_capture_async(config=cfg)
        assert handle.is_active is True

        # Allow slight progress, then cancel
        time.sleep(0.06)
        handle.cancel()

        # Wait for worker thread completion
        captured = handle.wait(timeout=2.0)
        assert captured is not None
        assert handle.status == AudioCaptureStatus.CANCELLED
        assert mock_capture_provider.resources_cleaned_up is True
        assert capture_service.counters["cancelled"] == 1

    def test_resource_cleanup_on_driver_failure(
        self,
        capture_service: AudioCaptureService,
        mock_capture_provider: MockAudioCaptureProvider,
    ) -> None:
        """Verify resources are released when driver raises an exception."""
        mock_capture_provider.set_failure_mode(
            True,
            exception_cls=AudioPermissionError,
            message="OS microphone permission denied",
        )

        with pytest.raises(AudioPermissionError, match="permission denied"):
            capture_service.capture_audio(duration_seconds=0.1)

        assert mock_capture_provider.resources_cleaned_up is True
        assert capture_service.counters["failed"] == 1


# ==============================================================================
# 6. EVENT PUBLICATION & ERROR ISOLATION
# ==============================================================================


class TestEventBusIntegration:
    """Tests verifying EventBus lifecycle event dispatch and subscriber failure isolation."""

    def test_capture_lifecycle_events_emitted(
        self,
        capture_service: AudioCaptureService,
        event_bus: EventBus,
    ) -> None:
        """Verify AUDIO_CAPTURE_STARTED and AUDIO_CAPTURE_COMPLETED events."""
        events: List[Event] = []
        event_bus.subscribe(EventType.AUDIO_CAPTURE_STARTED, events.append)
        event_bus.subscribe(EventType.AUDIO_CAPTURE_COMPLETED, events.append)

        correlation_id = "test_trace_123"
        captured = capture_service.capture_audio(
            duration_seconds=0.1,
            correlation_id=correlation_id,
        )

        assert len(events) == 2
        start_event = events[0]
        complete_event = events[1]

        assert start_event.event_type == EventType.AUDIO_CAPTURE_STARTED
        assert start_event.correlation_id == correlation_id
        assert start_event.payload["sample_rate"] == 16000
        # Verify NO raw audio bytes in start event
        assert "audio_bytes" not in start_event.payload

        assert complete_event.event_type == EventType.AUDIO_CAPTURE_COMPLETED
        assert complete_event.correlation_id == correlation_id
        assert complete_event.payload["byte_count"] == captured.byte_count
        # Verify NO raw audio bytes in completion event
        assert "audio_bytes" not in complete_event.payload

    def test_capture_failed_event_emitted(
        self,
        capture_service: AudioCaptureService,
        mock_capture_provider: MockAudioCaptureProvider,
        event_bus: EventBus,
    ) -> None:
        """Verify AUDIO_CAPTURE_FAILED event emission on error."""
        failed_events: List[Event] = []
        event_bus.subscribe(EventType.AUDIO_CAPTURE_FAILED, failed_events.append)

        mock_capture_provider.set_failure_mode(True, message="Hardware disconnect")

        with pytest.raises(AudioCaptureError):
            capture_service.capture_audio(duration_seconds=0.1, correlation_id="fail_corr_id")

        assert len(failed_events) == 1
        fail_ev = failed_events[0]
        assert fail_ev.correlation_id == "fail_corr_id"
        assert fail_ev.payload["error_type"] == "AudioCaptureError"
        assert "Hardware disconnect" in fail_ev.payload["error_message"]

    def test_subscriber_failure_isolation(
        self,
        capture_service: AudioCaptureService,
        event_bus: EventBus,
    ) -> None:
        """Verify a crashing event subscriber does not disrupt audio capture."""
        def crashing_subscriber(event: Event) -> None:
            raise RuntimeError("Subscriber explosion!")

        event_bus.subscribe(EventType.AUDIO_CAPTURE_STARTED, crashing_subscriber)
        event_bus.subscribe(EventType.AUDIO_CAPTURE_COMPLETED, crashing_subscriber)

        # Recording should succeed completely despite crashing subscribers
        captured = capture_service.capture_audio(duration_seconds=0.1)
        assert captured.byte_count > 0


# ==============================================================================
# 7. STATESTORE BOUNDEDNESS
# ==============================================================================


class TestStateStoreBoundedness:
    """Tests verifying StateStore bounds and the zero-audio-history invariant."""

    def test_statestore_holds_metadata_not_audio(
        self,
        capture_service: AudioCaptureService,
        state_store: StateStore,
    ) -> None:
        """Verify that StateStore stores only bounded telemetry and never stores audio bytes."""
        capture_service.capture_audio(duration_seconds=0.1)

        cap_status = state_store.get("audio.capture_status")
        assert cap_status is not None
        assert cap_status["status"] == "COMPLETED"
        assert cap_status["byte_count"] > 0
        assert "audio_bytes" not in cap_status
        assert "pcm" not in cap_status

        device_summary = state_store.get("audio.device_summary")
        assert device_summary is not None
        assert device_summary["total_devices"] == 3
        assert device_summary["has_microphone"] is True

        counters = state_store.get("audio.counters")
        assert counters["completed"] == 1
        assert counters["failed"] == 0

    def test_statestore_deepcopy_safety(
        self,
        capture_service: AudioCaptureService,
        state_store: StateStore,
    ) -> None:
        """Verify that StateStore does not contain unpicklable mapping proxies."""
        capture_service.capture_audio(duration_seconds=0.1)
        # Attempting deepcopy of entire state should not raise error
        all_state = state_store.snapshot()
        assert isinstance(all_state, dict)


# ==============================================================================
# 8. VOICE SERVICE INTEGRATION
# ==============================================================================


class TestVoiceServiceIntegration:
    """Tests verifying VoiceService integration with AudioCaptureService."""

    def test_voice_service_capture_audio_delegate(
        self,
        capture_service: AudioCaptureService,
        event_bus: EventBus,
        state_store: StateStore,
    ) -> None:
        """Verify VoiceService.capture_audio calls underlying AudioCaptureService."""
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)
        stt = MockSTTProvider()
        tts = MockTTSProvider()

        voice_service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt,
            tts_provider=tts,
            audio_capture=capture_service,
            event_bus=event_bus,
            state_store=state_store,
        )

        captured = voice_service.capture_audio(duration_seconds=0.1)
        assert isinstance(captured, CapturedAudio)
        assert captured.byte_count > 0

    def test_captured_audio_to_mock_stt_pipeline(
        self,
        capture_service: AudioCaptureService,
        event_bus: EventBus,
        state_store: StateStore,
    ) -> None:
        """Verify passing CapturedAudio to VoiceService.process_audio for transcription."""
        orchestrator = MasterOrchestrator(event_bus=event_bus, state_store=state_store)
        stt = MockSTTProvider()
        tts = MockTTSProvider()

        voice_service = VoiceService(
            orchestrator=orchestrator,
            stt_provider=stt,
            tts_provider=tts,
            audio_capture=capture_service,
            event_bus=event_bus,
            state_store=state_store,
        )

        # Capture audio with metadata fixture token
        captured = capture_service.capture_audio(duration_seconds=0.1)
        # Attach fixture token in metadata
        captured_with_token = CapturedAudio(
            audio_bytes=captured.audio_bytes,
            sample_rate=captured.sample_rate,
            channels=captured.channels,
            sample_width_bytes=captured.sample_width_bytes,
            duration_seconds=captured.duration_seconds,
            metadata={"fixture_token": "fixture:system_status"},
        )

        output = voice_service.process_audio(audio_data=captured_with_token)
        assert output.is_success is True
        assert "System Status" in output.text or "not initialized" in output.text


# ==============================================================================
# 9. BOOTSTRAP INTEGRATION & SAFETY GUARANTEES
# ==============================================================================


class TestBootstrapAudioIntegration:
    """Tests verifying audio subsystem bootstrapping and strict safety invariants."""

    def test_bootstrap_initializes_audio_capture(self) -> None:
        """Verify bootstrap_system provides initialized AudioCaptureService in SystemStatus."""
        status = bootstrap_system(log_to_file=False)

        assert status.voice_service is not None
        assert status.audio_capture is not None
        assert isinstance(status.audio_capture, AudioCaptureService)

        # Invariant checks: execution locked, trading uninitialized
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.settings.execution_locked is True

    def test_bootstrap_no_background_recording(self) -> None:
        """Verify bootstrap does not start any recording threads or capture handles."""
        status = bootstrap_system(log_to_file=False)
        assert status.audio_capture is not None
        assert len(status.audio_capture._active_handles) == 0
        assert status.audio_capture.counters["total_captures"] == 0
