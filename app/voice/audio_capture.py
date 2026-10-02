"""KAISER — Local Audio Capture Service.

Implements bounded, explicit microphone audio capture for future speech recognition.
Guarantees:
- Recording begins ONLY upon explicit invocation (no background or automatic recording).
- Strict bounds on sample rate, channels, recording duration, and buffer memory.
- Early cancellation and bounded timeout enforcement.
- Safe resource acquisition and release across success, cancellation, and error paths.
- Zero audio persistence to disk.
- Zero network transmission.
- Zero logging/printing of raw audio samples (privacy-safe metadata only).
- EventBus lifecycle notifications and bounded StateStore metrics.
"""

from __future__ import annotations

import copy
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Mapping, Optional, Union

from app.core.events import Event, EventBus, EventType
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.orchestrator.lifecycle import sanitize_container
from app.voice.audio_devices import AudioDeviceManager, SoundDeviceBackend
from app.voice.audio_models import (
    AudioBufferOverflowError,
    AudioCaptureConfig,
    AudioCaptureError,
    AudioCaptureStatus,
    AudioCaptureTimeoutError,
    AudioConfigurationError,
    AudioDeviceInfo,
    AudioDeviceNotFoundError,
    AudioPermissionError,
    CapturedAudio,
)
from app.voice.providers.mock_audio import MockAudioCaptureProvider

logger = get_logger("voice.capture")


# ---------------------------------------------------------------------------
# Capture Backend Interface
# ---------------------------------------------------------------------------


class AudioCaptureBackend(ABC):
    """Abstract interface for audio capture engines."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name descriptor of the capture engine."""

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Check if capture backend is operational."""

    @abstractmethod
    def capture(
        self,
        config: AudioCaptureConfig,
        stop_event: threading.Event,
        on_chunk: Optional[Callable[[bytes], None]] = None,
    ) -> bytes:
        """Capture audio synchronously according to config until duration reached or stop_event set.

        Returns raw PCM bytes.
        """


# ---------------------------------------------------------------------------
# Real SoundDevice Capture Backend
# ---------------------------------------------------------------------------


class SoundDeviceCaptureBackend(AudioCaptureBackend):
    """Local hardware capture backend utilizing `sounddevice` and PortAudio."""

    def __init__(self) -> None:
        self._available = False
        self._sd_module: Optional[Any] = None
        self._init_backend()

    def _init_backend(self) -> None:
        try:
            import sounddevice as sd  # type: ignore

            self._sd_module = sd
            self._available = True
        except Exception as exc:
            self._available = False
            self._sd_module = None
            logger.warning("SoundDeviceCaptureBackend unavailable: %s", exc)

    @property
    def name(self) -> str:
        return "sounddevice_capture"

    @property
    def is_available(self) -> bool:
        return self._available and (self._sd_module is not None)

    def capture(
        self,
        config: AudioCaptureConfig,
        stop_event: threading.Event,
        on_chunk: Optional[Callable[[bytes], None]] = None,
    ) -> bytes:
        """Record bounded PCM audio from local microphone."""
        if not self.is_available:
            raise AudioCaptureError("SoundDevice library or audio driver is not available.")

        sd = self._sd_module
        dtype_map = {1: "int8", 2: "int16", 4: "int32"}
        dtype = dtype_map.get(config.sample_width_bytes, "int16")
        chunk_samples = config.chunk_samples

        accumulated = bytearray()
        stream = None

        try:
            stream = sd.InputStream(
                samplerate=config.sample_rate,
                channels=config.channels,
                dtype=dtype,
                device=config.device_id,
                blocksize=chunk_samples,
            )
            stream.start()

            start_time = time.monotonic()
            max_duration = config.max_duration_seconds

            while not stop_event.is_set():
                elapsed = time.monotonic() - start_time
                if elapsed >= max_duration:
                    logger.debug("Recording reached configured max duration: %.2fs", max_duration)
                    break

                # Read audio block
                data, overflowed = stream.read(chunk_samples)
                if overflowed:
                    logger.warning("Audio input buffer overflowed in driver layer.")

                chunk_bytes = data.tobytes()

                # Boundary check
                if len(accumulated) + len(chunk_bytes) > config.max_buffer_bytes:
                    raise AudioBufferOverflowError(
                        f"Captured bytes exceeded maximum buffer boundary of {config.max_buffer_bytes} bytes."
                    )

                accumulated.extend(chunk_bytes)

                if on_chunk:
                    try:
                        on_chunk(chunk_bytes)
                    except Exception as exc:
                        logger.debug("on_chunk callback error: %s", exc)

            return bytes(accumulated)

        except Exception as exc:
            err_str = str(exc).lower()
            if "permission" in err_str or "denied" in err_str or "access" in err_str:
                raise AudioPermissionError(f"Microphone access permission denied: {exc}") from exc
            elif "invalid device" in err_str or "device unavailable" in err_str:
                raise AudioDeviceNotFoundError(f"Selected audio input device is invalid: {exc}") from exc
            elif isinstance(exc, (AudioCaptureError, AudioBufferOverflowError)):
                raise
            else:
                raise AudioCaptureError(f"Audio driver error during capture: {exc}") from exc

        finally:
            if stream is not None:
                try:
                    stream.stop()
                    stream.close()
                except Exception as exc:
                    logger.debug("Error closing sounddevice stream: %s", exc)


# ---------------------------------------------------------------------------
# Mock Capture Adapter
# ---------------------------------------------------------------------------


class MockAudioCaptureBackend(AudioCaptureBackend):
    """Adapter bridging MockAudioCaptureProvider into AudioCaptureBackend."""

    def __init__(self, provider: Optional[MockAudioCaptureProvider] = None) -> None:
        self.provider = provider or MockAudioCaptureProvider()

    @property
    def name(self) -> str:
        return self.provider.name

    @property
    def is_available(self) -> bool:
        return self.provider.is_available

    def capture(
        self,
        config: AudioCaptureConfig,
        stop_event: threading.Event,
        on_chunk: Optional[Callable[[bytes], None]] = None,
    ) -> bytes:
        return self.provider.capture_stream(
            config=config,
            stop_event=stop_event,
            on_chunk=on_chunk,
            chunk_delay_seconds=0.005,
        )


# ---------------------------------------------------------------------------
# Audio Capture Handle (Asynchronous / Controlled Recording)
# ---------------------------------------------------------------------------


class AudioCaptureHandle:
    """Thread-safe handle for an active or completed audio recording operation."""

    def __init__(
        self,
        capture_id: str,
        correlation_id: str,
        config: AudioCaptureConfig,
        device_info: AudioDeviceInfo,
    ) -> None:
        self.capture_id = capture_id
        self.correlation_id = correlation_id
        self.config = config
        self.device_info = device_info
        self.stop_event = threading.Event()

        self._lock = threading.RLock()
        self._status = AudioCaptureStatus.RECORDING
        self._result: Optional[CapturedAudio] = None
        self._error: Optional[Exception] = None
        self._thread: Optional[threading.Thread] = None
        self.started_at = time.time()
        self.completed_at: Optional[float] = None

    @property
    def status(self) -> AudioCaptureStatus:
        with self._lock:
            return self._status

    @property
    def is_active(self) -> bool:
        with self._lock:
            return self._status == AudioCaptureStatus.RECORDING

    def cancel(self) -> None:
        """Signal recording to stop immediately and mark as cancelled."""
        with self._lock:
            if self._status == AudioCaptureStatus.RECORDING:
                self.stop_event.set()
                self._status = AudioCaptureStatus.CANCELLED

    def wait(self, timeout: Optional[float] = None) -> Optional[CapturedAudio]:
        """Wait for capture completion and return CapturedAudio or raise stored error."""
        if self._thread:
            self._thread.join(timeout=timeout)

        with self._lock:
            if self._error:
                raise self._error
            return self._result


# ---------------------------------------------------------------------------
# Audio Capture Service
# ---------------------------------------------------------------------------


class AudioCaptureService:
    """Service orchestrating bounded, explicit microphone audio capture.

    Responsibilities:
    1. Validates capture configuration and selected audio input device.
    2. Enforces explicit invocation — recording begins ONLY when explicitly requested.
    3. Handles timeouts, cancellations, driver exceptions, and buffer overflow.
    4. Ensures audio hardware streams are guaranteed to be closed and freed.
    5. Dispatches lifecycle events over EventBus (without leaking raw audio).
    6. Synchronizes bounded telemetry and status to StateStore (without audio persistence).
    """

    def __init__(
        self,
        device_manager: Optional[AudioDeviceManager] = None,
        backend: Optional[AudioCaptureBackend] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        default_config: Optional[AudioCaptureConfig] = None,
        name: str = "audio_capture_service",
    ) -> None:
        self.name = name
        self.event_bus = event_bus
        self.state_store = state_store
        self.default_config = default_config or AudioCaptureConfig()
        self._lock = threading.RLock()

        self.device_manager = device_manager or AudioDeviceManager(
            event_bus=event_bus, state_store=state_store
        )

        if backend is not None:
            self.backend = backend
        else:
            # Prefer SoundDeviceCaptureBackend if available, else MockAudioCaptureBackend
            sd_cap = SoundDeviceCaptureBackend()
            if sd_cap.is_available:
                self.backend = sd_cap
            else:
                logger.info(
                    "SoundDeviceCaptureBackend unavailable; defaulting to MockAudioCaptureBackend."
                )
                self.backend = MockAudioCaptureBackend()

        self._counters: Dict[str, int] = {
            "total_captures": 0,
            "completed": 0,
            "cancelled": 0,
            "failed": 0,
        }

        self._active_handles: Dict[str, AudioCaptureHandle] = {}
        self._sync_state_store(status=AudioCaptureStatus.IDLE)

        logger.info(
            "AudioCaptureService '%s' initialized (backend=%s, device_mgr=%s)",
            self.name,
            self.backend.name,
            self.device_manager.backend_name,
        )

    @property
    def counters(self) -> Dict[str, int]:
        """Return shallow copy of operational counters."""
        with self._lock:
            return dict(self._counters)

    def capture_audio(
        self,
        duration_seconds: Optional[float] = None,
        device_id: Optional[int] = None,
        config: Optional[AudioCaptureConfig] = None,
        correlation_id: Optional[str] = None,
        handle: Optional[AudioCaptureHandle] = None,
    ) -> CapturedAudio:
        """Capture bounded audio synchronously upon explicit request.

        Args:
            duration_seconds: Optional duration override (seconds).
            device_id: Optional device index override.
            config: Optional custom AudioCaptureConfig.
            correlation_id: Optional tracing identifier.
            handle: Optional pre-created AudioCaptureHandle (for async capture).

        Returns:
            CapturedAudio: Immutable container with PCM bytes and metadata.

        Raises:
            AudioConfigurationError: If configuration is invalid.
            AudioDeviceNotFoundError: If input device cannot be found.
            AudioDeviceError: If input device is unavailable or lacks input capability.
            AudioPermissionError: If microphone permission is denied.
            AudioBufferOverflowError: If recording exceeds buffer bounds.
            AudioCaptureError: If audio capture fails.
        """
        if handle is not None:
            active_handle = handle
            resolved_corr_id = handle.correlation_id
            capture_id = handle.capture_id
            resolved_config = handle.config
            device_info = handle.device_info
        else:
            resolved_corr_id = (correlation_id or uuid.uuid4().hex).strip()
            capture_id = f"cap_{uuid.uuid4().hex[:12]}"
            resolved_config = self._resolve_config(
                base_config=config or self.default_config,
                duration_seconds=duration_seconds,
                device_id=device_id,
            )
            device_info = self.device_manager.validate_input_device(resolved_config.device_id)
            active_handle = AudioCaptureHandle(
                capture_id=capture_id,
                correlation_id=resolved_corr_id,
                config=resolved_config,
                device_info=device_info,
            )

        with self._lock:
            self._counters["total_captures"] += 1
            self._active_handles[capture_id] = active_handle

        # Notify start of capture
        self._publish_event(
            EventType.AUDIO_CAPTURE_STARTED,
            correlation_id=resolved_corr_id,
            payload={
                "capture_id": capture_id,
                "device_id": device_info.device_id,
                "device_name": device_info.name,
                "sample_rate": resolved_config.sample_rate,
                "channels": resolved_config.channels,
                "sample_width_bytes": resolved_config.sample_width_bytes,
                "max_duration_seconds": resolved_config.max_duration_seconds,
            },
        )
        self._sync_state_store(
            status=AudioCaptureStatus.RECORDING,
            active_device=device_info.name,
            duration_s=resolved_config.max_duration_seconds,
        )

        start_time = time.monotonic()

        try:
            # Execute capture via backend
            raw_pcm = self.backend.capture(
                config=resolved_config,
                stop_event=active_handle.stop_event,
            )
            elapsed = time.monotonic() - start_time
            audio_duration = (
                len(raw_pcm) / float(resolved_config.bytes_per_second)
                if resolved_config.bytes_per_second > 0
                else 0.0
            )

            # Check if cancelled
            if active_handle.stop_event.is_set():
                with self._lock:
                    self._counters["cancelled"] += 1
                    active_handle._status = AudioCaptureStatus.CANCELLED
                    active_handle.completed_at = time.time()

                self._publish_event(
                    EventType.AUDIO_CAPTURE_CANCELLED,
                    correlation_id=resolved_corr_id,
                    payload={
                        "capture_id": capture_id,
                        "device_id": device_info.device_id,
                        "bytes_captured": len(raw_pcm),
                        "duration_seconds": audio_duration,
                    },
                )
                self._sync_state_store(status=AudioCaptureStatus.CANCELLED)

                # Return whatever was captured before cancellation
                captured = CapturedAudio(
                    audio_bytes=raw_pcm,
                    sample_rate=resolved_config.sample_rate,
                    channels=resolved_config.channels,
                    sample_width_bytes=resolved_config.sample_width_bytes,
                    duration_seconds=audio_duration,
                    device_id=device_info.device_id,
                    device_name=device_info.name,
                    metadata={
                        "cancelled": True,
                        "capture_id": capture_id,
                        "elapsed_wall_seconds": elapsed,
                    },
                )
                active_handle._result = captured
                return captured

            # Completed normally
            with self._lock:
                self._counters["completed"] += 1
                active_handle._status = AudioCaptureStatus.COMPLETED
                active_handle.completed_at = time.time()

            captured = CapturedAudio(
                audio_bytes=raw_pcm,
                sample_rate=resolved_config.sample_rate,
                channels=resolved_config.channels,
                sample_width_bytes=resolved_config.sample_width_bytes,
                duration_seconds=audio_duration,
                device_id=device_info.device_id,
                device_name=device_info.name,
                metadata={
                    "capture_id": capture_id,
                    "backend": self.backend.name,
                    "elapsed_wall_seconds": elapsed,
                },
            )
            active_handle._result = captured

            self._publish_event(
                EventType.AUDIO_CAPTURE_COMPLETED,
                correlation_id=resolved_corr_id,
                payload={
                    "capture_id": capture_id,
                    "device_id": device_info.device_id,
                    "byte_count": captured.byte_count,
                    "num_samples": captured.num_samples,
                    "duration_seconds": captured.duration_seconds,
                    "duration_ms": captured.duration_ms,
                },
            )
            self._sync_state_store(
                status=AudioCaptureStatus.COMPLETED,
                active_device=device_info.name,
                byte_count=captured.byte_count,
                duration_s=captured.duration_seconds,
            )

            logger.info(
                "Audio capture completed successfully: %s (bytes=%d, duration=%.2fs)",
                capture_id,
                captured.byte_count,
                captured.duration_seconds,
            )
            return captured

        except Exception as exc:
            with self._lock:
                self._counters["failed"] += 1
                active_handle._status = AudioCaptureStatus.FAILED
                active_handle._error = exc
                active_handle.completed_at = time.time()

            self._publish_event(
                EventType.AUDIO_CAPTURE_FAILED,
                correlation_id=resolved_corr_id,
                payload={
                    "capture_id": capture_id,
                    "device_id": device_info.device_id,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            self._sync_state_store(status=AudioCaptureStatus.FAILED, error=str(exc))

            logger.error("Audio capture '%s' failed: %s", capture_id, exc)
            raise

        finally:
            with self._lock:
                self._active_handles.pop(capture_id, None)

    def start_capture_async(
        self,
        duration_seconds: Optional[float] = None,
        device_id: Optional[int] = None,
        config: Optional[AudioCaptureConfig] = None,
        correlation_id: Optional[str] = None,
    ) -> AudioCaptureHandle:
        """Begin asynchronous audio capture in a background thread upon explicit request.

        Returns an AudioCaptureHandle to inspect status, cancel, or await completion.
        """
        resolved_corr_id = (correlation_id or uuid.uuid4().hex).strip()
        capture_id = f"cap_async_{uuid.uuid4().hex[:12]}"

        resolved_config = self._resolve_config(
            base_config=config or self.default_config,
            duration_seconds=duration_seconds,
            device_id=device_id,
        )
        device_info = self.device_manager.validate_input_device(resolved_config.device_id)

        handle = AudioCaptureHandle(
            capture_id=capture_id,
            correlation_id=resolved_corr_id,
            config=resolved_config,
            device_info=device_info,
        )

        with self._lock:
            self._active_handles[capture_id] = handle

        def _worker() -> None:
            try:
                self.capture_audio(handle=handle)
            except Exception as exc:
                logger.debug("Async capture thread ended with exception: %s", exc)

        t = threading.Thread(
            target=_worker,
            name=f"kaiser_audio_capture_{capture_id}",
            daemon=True,
        )
        handle._thread = t
        t.start()
        return handle

    def _resolve_config(
        self,
        base_config: AudioCaptureConfig,
        duration_seconds: Optional[float],
        device_id: Optional[int],
    ) -> AudioCaptureConfig:
        """Apply overrides to capture configuration and validate bounds."""
        new_duration = (
            float(duration_seconds)
            if duration_seconds is not None
            else base_config.max_duration_seconds
        )
        new_dev_id = device_id if device_id is not None else base_config.device_id

        return AudioCaptureConfig(
            sample_rate=base_config.sample_rate,
            channels=base_config.channels,
            sample_width_bytes=base_config.sample_width_bytes,
            max_duration_seconds=new_duration,
            chunk_duration_ms=base_config.chunk_duration_ms,
            max_buffer_bytes=base_config.max_buffer_bytes,
            device_id=new_dev_id,
            device_name=base_config.device_name,
        )

    # --------------------------------------------------------------------------
    # Telemetry and StateStore Integration
    # --------------------------------------------------------------------------

    def _sync_state_store(
        self,
        status: AudioCaptureStatus,
        active_device: Optional[str] = None,
        duration_s: float = 0.0,
        byte_count: int = 0,
        error: Optional[str] = None,
    ) -> None:
        """Store bounded telemetry in StateStore without audio history."""
        if not self.state_store:
            return

        with self._lock:
            status_data = {
                "status": status.value,
                "active_device": active_device,
                "duration_seconds": duration_s,
                "byte_count": byte_count,
                "error": error,
                "timestamp": time.time(),
            }
            try:
                self.state_store.set("audio.capture_status", sanitize_container(status_data))
                self.state_store.set("audio.counters", sanitize_container(self._counters))
            except Exception as exc:
                logger.debug("StateStore update failed in AudioCaptureService: %s", exc)

    def _publish_event(
        self,
        event_type: EventType,
        correlation_id: str,
        payload: Dict[str, Any],
    ) -> None:
        """Safely emit audio capture lifecycle events."""
        if not self.event_bus:
            return

        try:
            event = Event.create(
                event_type=event_type,
                source=self.name,
                correlation_id=correlation_id,
                payload=payload,
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.debug("Failed to dispatch audio capture event '%s': %s", event_type, exc)
