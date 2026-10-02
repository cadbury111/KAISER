"""KAISER — Audio Playback Service & Backend Foundation.

Provides modular, failure-isolated, and controlled audio playback for synthesized
voice outputs across local hardware and automated test environments. Guarantees:
- Explicit playback invocation (no automatic playback at application boot).
- Controlled output device selection and validation (no conflation with input devices).
- Strict non-overlapping playback concurrency protection.
- Immediate stream and driver resource cleanup across success, error, and cancellation.
- Zero audio payload persistence in StateStore or local disk.
- Complete lifecycle event dispatch and subscriber failure isolation.
"""

from __future__ import annotations

import io
import math
import threading
import time
import uuid
import wave
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Mapping, Optional, Union

from app.core.events import Event, EventBus, EventType
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.orchestrator.lifecycle import sanitize_container
from app.voice.audio_devices import AudioDeviceManager, SoundDeviceBackend
from app.voice.audio_models import (
    AudioDeviceInfo,
    AudioDeviceNotFoundError,
    AudioPlaybackCancelledError,
    AudioPlaybackConcurrencyError,
    AudioPlaybackDeviceNotFoundError,
    AudioPlaybackError,
    AudioPlaybackFormatError,
    AudioPlaybackResult,
    AudioPlaybackStatus,
    AudioPlaybackTimeoutError,
)
from app.voice.models import SynthesisResult, VoiceOutput

logger = get_logger("voice.playback")


# ---------------------------------------------------------------------------
# Backend Abstraction
# ---------------------------------------------------------------------------


class AudioPlaybackBackend(ABC):
    """Abstract base class for audio playback hardware/mock adapters."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name descriptor for the playback backend."""

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Check if backend driver and hardware are accessible."""

    @abstractmethod
    def play(
        self,
        pcm_bytes: bytes,
        sample_rate: int,
        channels: int,
        sample_width_bytes: int = 2,
        device_id: Optional[int] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        """Play raw PCM audio bytes to the specified hardware device.

        Args:
            pcm_bytes: Raw 16-bit or 8-bit PCM audio payload.
            sample_rate: Sampling frequency in Hz (e.g. 16000, 22050, 44100).
            channels: Channel count (1 for mono, 2 for stereo).
            sample_width_bytes: Bytes per sample (typically 2 for int16).
            device_id: Target output device index.
            stop_event: Optional event to cancel active playback immediately.

        Raises:
            AudioPlaybackCancelledError: If stop_event was triggered.
            AudioPlaybackError: If driver or hardware encounters an error.
        """


# ---------------------------------------------------------------------------
# SoundDevice Hardware Backend
# ---------------------------------------------------------------------------


class SoundDevicePlaybackBackend(AudioPlaybackBackend):
    """Hardware audio playback backend using `sounddevice` and PortAudio."""

    def __init__(self) -> None:
        self._available = False
        self._sd_module: Optional[Any] = None
        self._np_module: Optional[Any] = None
        self._init_backend()

    def _init_backend(self) -> None:
        """Safely import sounddevice and numpy without unhandled crashes."""
        try:
            import numpy as np  # type: ignore
            import sounddevice as sd  # type: ignore

            self._sd_module = sd
            self._np_module = np
            self._available = True
            logger.debug("SoundDevicePlaybackBackend initialized successfully.")
        except Exception as exc:
            self._available = False
            self._sd_module = None
            self._np_module = None
            logger.warning(
                "SoundDevicePlaybackBackend unavailable (library or audio drivers missing): %s",
                exc,
            )

    @property
    def name(self) -> str:
        return "sounddevice_playback"

    @property
    def is_available(self) -> bool:
        return self._available and (self._sd_module is not None) and (self._np_module is not None)

    def play(
        self,
        pcm_bytes: bytes,
        sample_rate: int,
        channels: int,
        sample_width_bytes: int = 2,
        device_id: Optional[int] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        """Play PCM audio using chunked sounddevice OutputStream."""
        if not self.is_available:
            raise AudioPlaybackError("SoundDevice library or driver is unavailable on this system.")

        if stop_event is not None and stop_event.is_set():
            raise AudioPlaybackCancelledError("Playback cancelled before initiation.")

        np = self._np_module
        sd = self._sd_module

        # Convert 16-bit PCM bytes to float32 samples [-1.0, 1.0]
        try:
            if sample_width_bytes == 2:
                samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32768.0
            elif sample_width_bytes == 1:
                samples = (np.frombuffer(pcm_bytes, dtype=np.uint8).astype(np.float32) - 128.0) / 128.0
            else:
                samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32768.0

            if channels > 1:
                samples = samples.reshape(-1, channels)
            elif channels == 1 and samples.ndim == 1:
                # 1D array is suitable for mono stream
                pass
        except Exception as exc:
            raise AudioPlaybackFormatError(f"Failed to unpack PCM audio bytes: {exc}") from exc

        # Stream playback in chunks to allow responsive cancellation
        chunk_size = 2048
        total_samples = len(samples)
        current_offset = 0

        try:
            with sd.OutputStream(
                samplerate=sample_rate,
                channels=channels,
                device=device_id,
                dtype="float32",
            ) as stream:
                while current_offset < total_samples:
                    if stop_event is not None and stop_event.is_set():
                        logger.info("Playback cancelled during active stream.")
                        raise AudioPlaybackCancelledError("Playback was cancelled during execution.")

                    end_offset = min(current_offset + chunk_size, total_samples)
                    chunk = samples[current_offset:end_offset]
                    stream.write(chunk)
                    current_offset = end_offset

        except AudioPlaybackCancelledError:
            raise
        except Exception as exc:
            raise AudioPlaybackError(f"SoundDevice output stream error: {exc}") from exc


# ---------------------------------------------------------------------------
# Mock Playback Backend (Deterministic Test Fixtures)
# ---------------------------------------------------------------------------


class MockAudioPlaybackBackend(AudioPlaybackBackend):
    """Deterministic mock playback backend for automated tests without physical speakers."""

    def __init__(self, available: bool = True) -> None:
        self._lock = threading.RLock()
        self._available = available
        self._should_fail = False
        self._failure_message = "Simulated audio playback hardware failure"
        self._delay_seconds = 0.001
        self._play_count = 0
        self._last_played_bytes: Optional[bytes] = None
        self._last_sample_rate = 0
        self._last_channels = 0
        self._last_device_id: Optional[int] = None
        self._history: List[Dict[str, Any]] = []

    @property
    def name(self) -> str:
        return "mock_playback"

    @property
    def is_available(self) -> bool:
        with self._lock:
            return self._available and not self._should_fail

    def set_availability(self, available: bool) -> None:
        """Toggle backend availability."""
        with self._lock:
            self._available = available

    def set_failure_mode(
        self,
        should_fail: bool,
        message: str = "Simulated audio playback hardware failure",
    ) -> None:
        """Enable or disable simulated playback failure."""
        with self._lock:
            self._should_fail = should_fail
            self._failure_message = message

    def set_delay(self, seconds: float) -> None:
        """Set simulated delay per playback."""
        with self._lock:
            self._delay_seconds = max(0.0, seconds)

    @property
    def play_count(self) -> int:
        with self._lock:
            return self._play_count

    @property
    def last_played_bytes(self) -> Optional[bytes]:
        with self._lock:
            return self._last_played_bytes

    def get_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._history)

    def play(
        self,
        pcm_bytes: bytes,
        sample_rate: int,
        channels: int,
        sample_width_bytes: int = 2,
        device_id: Optional[int] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        """Simulate deterministic audio playback."""
        with self._lock:
            if not self._available or self._should_fail:
                raise AudioPlaybackError(self._failure_message)

        if stop_event is not None and stop_event.is_set():
            raise AudioPlaybackCancelledError("Playback cancelled before execution.")

        delay = self._delay_seconds
        if delay > 0.0:
            # Check stop event during delay in small increments
            slice_time = 0.005
            elapsed = 0.0
            while elapsed < delay:
                if stop_event is not None and stop_event.is_set():
                    raise AudioPlaybackCancelledError("Playback cancelled during execution.")
                time.sleep(min(slice_time, delay - elapsed))
                elapsed += slice_time

        with self._lock:
            self._play_count += 1
            self._last_played_bytes = pcm_bytes
            self._last_sample_rate = sample_rate
            self._last_channels = channels
            self._last_device_id = device_id
            self._history.append(
                {
                    "timestamp": time.time(),
                    "byte_count": len(pcm_bytes),
                    "sample_rate": sample_rate,
                    "channels": channels,
                    "device_id": device_id,
                }
            )


# ---------------------------------------------------------------------------
# Audio Playback Service
# ---------------------------------------------------------------------------


class AudioPlaybackService:
    """Manages audio playback lifecycle, device selection, and event reporting.

    Guarantees:
    - Explicit invocation: Audio is never played automatically.
    - Output device separation: Never conflates input devices with output speakers.
    - Concurrency protection: Overlapping playback is rejected via a non-blocking lock.
    - Guaranteed cleanup: Stream and device resources are released in finally blocks.
    - Memory boundedness: Audio is never persisted to disk or written to StateStore.
    """

    def __init__(
        self,
        backend: Optional[AudioPlaybackBackend] = None,
        device_manager: Optional[AudioDeviceManager] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        name: str = "audio_playback_service",
    ) -> None:
        self.name = name
        self.event_bus = event_bus
        self.state_store = state_store
        self._lock = threading.RLock()
        self._playback_lock = threading.Lock()  # Non-blocking lock to prevent overlapping playback

        if backend is not None:
            self.backend = backend
        else:
            sd_backend = SoundDevicePlaybackBackend()
            self.backend = sd_backend if sd_backend.is_available else MockAudioPlaybackBackend()

        if device_manager is not None:
            self.device_manager = device_manager
        else:
            self.device_manager = AudioDeviceManager(event_bus=event_bus, state_store=state_store)

        self._counters = {
            "total_playbacks": 0,
            "completed_playbacks": 0,
            "failed_playbacks": 0,
            "cancelled_playbacks": 0,
        }
        self._last_playback_summary: Optional[Dict[str, Any]] = None

        logger.info(
            "AudioPlaybackService '%s' initialized (backend=%s, device_mgr=%s)",
            self.name,
            self.backend.name,
            self.device_manager.backend_name,
        )

    @property
    def is_available(self) -> bool:
        """Check whether playback backend and output devices are operational."""
        with self._lock:
            return self.backend.is_available and self.device_manager.has_output_device()

    @property
    def counters(self) -> Dict[str, int]:
        """Return a copy of operational counters."""
        with self._lock:
            return dict(self._counters)

    def _extract_pcm_from_payload(
        self,
        audio_data: Any,
        default_sample_rate: int = 16000,
        default_channels: int = 1,
    ) -> tuple[bytes, int, int, int, float]:
        """Extract raw PCM bytes and metadata from varied audio payload types.

        Supports:
        - VoiceOutput (with audio_payload)
        - SynthesisResult (with audio_bytes)
        - Raw WAV bytes (parsed via wave module)
        - Raw PCM bytes

        Returns:
            Tuple of (pcm_bytes, sample_rate, channels, sample_width_bytes, duration_ms)
        """
        payload_bytes: bytes
        sample_rate = default_sample_rate
        channels = default_channels
        sample_width = 2
        duration_ms = 0.0

        if isinstance(audio_data, VoiceOutput):
            if not audio_data.has_audio or audio_data.audio_payload is None:
                raise AudioPlaybackFormatError("VoiceOutput does not contain synthesized audio payload.")
            payload_bytes = audio_data.audio_payload
            duration_ms = audio_data.duration_ms or 0.0

        elif isinstance(audio_data, SynthesisResult):
            payload_bytes = audio_data.audio_bytes
            sample_rate = audio_data.sample_rate
            duration_ms = audio_data.duration_ms

        elif isinstance(audio_data, bytes):
            payload_bytes = audio_data

        else:
            raise AudioPlaybackFormatError(
                f"Unsupported audio payload type: {type(audio_data).__name__}. "
                "Expected bytes, SynthesisResult, or VoiceOutput."
            )

        if not payload_bytes or len(payload_bytes) == 0:
            raise AudioPlaybackFormatError("Audio payload is empty (0 bytes).")

        # Maximum payload limit: 10MB
        max_bytes = 10 * 1024 * 1024
        if len(payload_bytes) > max_bytes:
            raise AudioPlaybackFormatError(
                f"Audio payload exceeds maximum allowed size ({len(payload_bytes)} > {max_bytes} bytes)."
            )

        # Detect WAV container (RIFF header)
        if len(payload_bytes) >= 12 and payload_bytes[:4] == b"RIFF" and payload_bytes[8:12] == b"WAVE":
            try:
                with wave.open(io.BytesIO(payload_bytes), "rb") as wf:
                    channels = wf.getnchannels()
                    sample_rate = wf.getframerate()
                    sample_width = wf.getsampwidth()
                    nframes = wf.getnframes()
                    pcm_bytes = wf.readframes(nframes)
                    duration_ms = round((nframes / float(sample_rate)) * 1000.0, 2)
                    return pcm_bytes, sample_rate, channels, sample_width, duration_ms
            except Exception as exc:
                raise AudioPlaybackFormatError(f"Failed to parse WAV audio container: {exc}") from exc

        # Treat as raw PCM
        if duration_ms <= 0.0 and sample_rate > 0 and channels > 0 and sample_width > 0:
            duration_ms = round((len(payload_bytes) / (sample_rate * channels * sample_width)) * 1000.0, 2)

        return payload_bytes, sample_rate, channels, sample_width, duration_ms

    def play(
        self,
        audio_data: Any,
        device_id: Optional[int] = None,
        correlation_id: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> AudioPlaybackResult:
        """Play an audio payload through a validated output device.

        Args:
            audio_data: SynthesisResult, VoiceOutput, or raw audio bytes.
            device_id: Optional target output device identifier.
            correlation_id: Optional tracing correlation identifier.
            stop_event: Optional threading.Event to signal cancellation.

        Returns:
            AudioPlaybackResult: Structured result of playback execution.

        Raises:
            AudioPlaybackConcurrencyError: If another playback is already active.
            AudioPlaybackDeviceNotFoundError: If no output device is found.
            AudioPlaybackFormatError: If payload is invalid or corrupted.
            AudioPlaybackError: If playback encounters a driver or hardware error.
        """
        resolved_corr_id = (correlation_id or uuid.uuid4().hex).strip()
        playback_id = f"play_{uuid.uuid4().hex[:12]}"

        # 1. Enforce non-overlapping playback concurrency protection
        if not self._playback_lock.acquire(blocking=False):
            err_msg = "Another audio playback is currently active. Overlapping playback is prohibited."
            logger.warning(err_msg)
            with self._lock:
                self._counters["failed_playbacks"] += 1
            res = AudioPlaybackResult.failure(
                playback_id=playback_id,
                correlation_id=resolved_corr_id,
                error=err_msg,
            )
            self._publish_event(
                EventType.AUDIO_PLAYBACK_FAILED,
                correlation_id=resolved_corr_id,
                payload={"playback_id": playback_id, "error": err_msg},
            )
            self._sync_state_store(last_result=res)
            raise AudioPlaybackConcurrencyError(err_msg)

        start_time = time.monotonic()
        pcm_bytes_ref: Optional[bytes] = None

        try:
            with self._lock:
                self._counters["total_playbacks"] += 1

            # 2. Extract and validate audio payload
            pcm_bytes, sample_rate, channels, sample_width, duration_ms = self._extract_pcm_from_payload(
                audio_data=audio_data
            )
            pcm_bytes_ref = pcm_bytes

            # 3. Validate and resolve output device
            try:
                device_info = self.device_manager.validate_output_device(device_id=device_id)
            except AudioDeviceNotFoundError as exc:
                raise AudioPlaybackDeviceNotFoundError(str(exc)) from exc
            except Exception as exc:
                raise AudioPlaybackError(f"Output device resolution failed: {exc}") from exc

            # 4. Publish AUDIO_PLAYBACK_STARTED
            self._publish_event(
                EventType.AUDIO_PLAYBACK_STARTED,
                correlation_id=resolved_corr_id,
                payload={
                    "playback_id": playback_id,
                    "device_id": device_info.device_id,
                    "device_name": device_info.name,
                    "sample_rate": sample_rate,
                    "channels": channels,
                    "duration_ms": duration_ms,
                },
            )

            # 5. Execute playback through backend
            self.backend.play(
                pcm_bytes=pcm_bytes,
                sample_rate=sample_rate,
                channels=channels,
                sample_width_bytes=sample_width,
                device_id=device_info.device_id,
                stop_event=stop_event,
            )

            elapsed_ms = round((time.monotonic() - start_time) * 1000.0, 2)
            with self._lock:
                self._counters["completed_playbacks"] += 1

            result = AudioPlaybackResult.completed(
                playback_id=playback_id,
                correlation_id=resolved_corr_id,
                device_id=device_info.device_id,
                device_name=device_info.name,
                sample_rate=sample_rate,
                channels=channels,
                duration_ms=elapsed_ms,
            )

            # 6. Publish AUDIO_PLAYBACK_COMPLETED
            self._publish_event(
                EventType.AUDIO_PLAYBACK_COMPLETED,
                correlation_id=resolved_corr_id,
                payload={
                    "playback_id": playback_id,
                    "device_id": device_info.device_id,
                    "duration_ms": elapsed_ms,
                },
            )
            self._sync_state_store(last_result=result)
            return result

        except AudioPlaybackCancelledError as exc:
            elapsed_ms = round((time.monotonic() - start_time) * 1000.0, 2)
            with self._lock:
                self._counters["cancelled_playbacks"] += 1

            result = AudioPlaybackResult.cancelled(
                playback_id=playback_id,
                correlation_id=resolved_corr_id,
                device_id=device_id if device_id is not None else -1,
                device_name="unknown",
                duration_ms=elapsed_ms,
                error=str(exc),
            )
            self._publish_event(
                EventType.AUDIO_PLAYBACK_CANCELLED,
                correlation_id=resolved_corr_id,
                payload={"playback_id": playback_id, "error": str(exc)},
            )
            self._sync_state_store(last_result=result)
            return result

        except Exception as exc:
            elapsed_ms = round((time.monotonic() - start_time) * 1000.0, 2)
            err_msg = str(exc)
            with self._lock:
                self._counters["failed_playbacks"] += 1

            result = AudioPlaybackResult.failure(
                playback_id=playback_id,
                correlation_id=resolved_corr_id,
                error=err_msg,
                device_id=device_id if device_id is not None else -1,
                duration_ms=elapsed_ms,
            )
            self._publish_event(
                EventType.AUDIO_PLAYBACK_FAILED,
                correlation_id=resolved_corr_id,
                payload={"playback_id": playback_id, "error": err_msg},
            )
            self._sync_state_store(last_result=result)
            raise

        finally:
            # Resource Invariant: Immediately drop references to raw PCM bytes and release lock
            pcm_bytes_ref = None
            self._playback_lock.release()

    def _publish_event(
        self,
        event_type: EventType | str,
        correlation_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        """Publish lifecycle event with subscriber failure isolation."""
        if self.event_bus is None:
            return

        try:
            safe_payload = sanitize_container(dict(payload))
            event = Event.create(
                event_type=event_type,
                source="audio_playback",
                correlation_id=correlation_id,
                payload=safe_payload,
                metadata={"source": "audio_playback", "service": self.name},
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.warning("EventBus dispatch failed for playback event '%s': %s", event_type, exc)

    def _sync_state_store(self, last_result: Optional[AudioPlaybackResult] = None) -> None:
        """Synchronize bounded operational summaries to StateStore (zero audio retention)."""
        if self.state_store is None:
            return

        try:
            with self._lock:
                counters_copy = dict(self._counters)

            self.state_store.set("audio_playback_counters", counters_copy)

            if last_result:
                summary = {
                    "playback_id": last_result.playback_id,
                    "status": last_result.status.value,
                    "device_id": last_result.device_id,
                    "device_name": last_result.device_name,
                    "duration_ms": last_result.duration_ms,
                    "is_success": last_result.is_success,
                    "error": last_result.error,
                }
                with self._lock:
                    self._last_playback_summary = summary
                self.state_store.set("audio_playback_last", summary)

        except Exception as exc:
            logger.warning("Failed to sync playback telemetry to StateStore: %s", exc)
