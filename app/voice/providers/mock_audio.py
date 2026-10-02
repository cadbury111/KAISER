"""KAISER — Mock Audio Capture & Device Providers.

Provides deterministic, hardware-independent mock audio capture and device
backends for automated testing. Guarantees:
- Zero microphone hardware access
- Zero network calls
- Deterministic PCM sample generation
- Explicit failure simulation (permission denial, hardware disconnect, driver errors, buffer overflow)
- Verification of resource cleanup in success, failure, and cancellation paths
"""

from __future__ import annotations

import math
import struct
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Type

from app.core.logging_config import get_logger
from app.voice.audio_models import (
    AudioBufferOverflowError,
    AudioCaptureError,
    AudioCaptureStatus,
    AudioCaptureTimeoutError,
    AudioConfigurationError,
    AudioDeviceInfo,
    AudioDeviceNotFoundError,
    AudioPermissionError,
    CapturedAudio,
)

logger = get_logger("voice.providers.mock_audio")


class MockAudioCaptureProvider:
    """Deterministic mock provider for simulating microphone audio capture in tests.

    Produces synthetic 16-bit PCM audio waves (or silence) matching the requested
    configuration, without requiring any real microphone or OS audio drivers.
    """

    def __init__(
        self,
        name: str = "mock_audio_capture",
        available: bool = True,
        waveform_type: str = "sine",  # 'sine', 'silence', 'pattern'
        frequency: float = 440.0,
    ) -> None:
        self.name = name
        self._lock = threading.RLock()
        self._available = available
        self._waveform_type = waveform_type
        self._frequency = frequency

        # Error simulation state
        self._should_fail = False
        self._failure_exception: Type[Exception] = AudioCaptureError
        self._failure_message = "Simulated microphone driver failure"

        # Tracking for testing verification
        self.capture_count = 0
        self.last_captured_bytes_len = 0
        self.last_config = None
        self.resources_cleaned_up = True
        self.cancellation_detected = False

    @property
    def is_available(self) -> bool:
        with self._lock:
            return self._available

    def set_available(self, available: bool) -> None:
        """Set whether the mock capture provider is available."""
        with self._lock:
            self._available = available

    def set_failure_mode(
        self,
        should_fail: bool,
        exception_cls: Type[Exception] = AudioCaptureError,
        message: str = "Simulated microphone driver failure",
    ) -> None:
        """Enable or disable simulated capture failure."""
        with self._lock:
            self._should_fail = should_fail
            self._failure_exception = exception_cls
            self._failure_message = message

    def generate_synthetic_pcm(
        self,
        sample_rate: int,
        channels: int,
        duration_seconds: float,
        sample_width_bytes: int = 2,
    ) -> bytes:
        """Generate deterministic raw PCM audio bytes.

        Generates a 16-bit or 8-bit PCM waveform (sine wave or silence).
        """
        total_samples = int(sample_rate * duration_seconds)
        buffer = bytearray()

        if self._waveform_type == "silence":
            if sample_width_bytes == 2:
                # 16-bit signed PCM silence is 0x0000
                return bytes(total_samples * channels * sample_width_bytes)
            elif sample_width_bytes == 1:
                # 8-bit unsigned PCM silence is 128 (0x80)
                return bytes([128] * (total_samples * channels))
            else:
                return bytes(total_samples * channels * sample_width_bytes)

        # Generate sine wave PCM
        for i in range(total_samples):
            # Compute sample amplitude (-1.0 to 1.0)
            t = float(i) / float(sample_rate)
            val = math.sin(2.0 * math.pi * self._frequency * t)

            if sample_width_bytes == 2:
                # 16-bit signed little-endian PCM (-32767 to 32767)
                int_val = max(-32767, min(32767, int(val * 32000.0)))
                sample_bytes = struct.pack("<h", int_val)
                for _ in range(channels):
                    buffer.extend(sample_bytes)
            elif sample_width_bytes == 1:
                # 8-bit unsigned PCM (0 to 255, center 128)
                int_val = max(0, min(255, int(val * 120.0 + 128.0)))
                sample_bytes = struct.pack("B", int_val)
                for _ in range(channels):
                    buffer.extend(sample_bytes)
            elif sample_width_bytes == 4:
                # 32-bit signed PCM
                int_val = max(-2147483647, min(2147483647, int(val * 2140000000.0)))
                sample_bytes = struct.pack("<i", int_val)
                for _ in range(channels):
                    buffer.extend(sample_bytes)

        return bytes(buffer)

    def capture_stream(
        self,
        config: Any,
        stop_event: threading.Event,
        on_chunk: Optional[Callable[[bytes], None]] = None,
        chunk_delay_seconds: float = 0.01,
    ) -> bytes:
        """Simulate streaming microphone capture with chunk callback and cancellation support.

        Args:
            config: AudioCaptureConfig instance.
            stop_event: Threading event to signal early cancellation/stop.
            on_chunk: Optional callback invoked for each captured chunk.
            chunk_delay_seconds: Artificial delay between chunks to simulate real-time recording.

        Returns:
            bytes: Accumulated PCM bytes.

        Raises:
            AudioCaptureError (or configured exception): If failure mode is set or buffer exceeds limit.
        """
        with self._lock:
            self.resources_cleaned_up = False
            self.cancellation_detected = False
            self.last_config = config
            self.capture_count += 1

            if not self._available:
                self.resources_cleaned_up = True
                raise AudioCaptureError("Mock audio capture device is unavailable.")

            if self._should_fail:
                self.resources_cleaned_up = True
                raise self._failure_exception(self._failure_message)

        accumulated = bytearray()
        chunk_bytes_len = config.chunk_bytes
        total_bytes_needed = int(config.bytes_per_second * config.max_duration_seconds)
        bytes_captured = 0

        try:
            while bytes_captured < total_bytes_needed:
                # Check for explicit cancellation
                if stop_event.is_set():
                    logger.debug("Mock capture received stop/cancellation signal.")
                    self.cancellation_detected = True
                    break

                # Determine next slice size
                remaining = total_bytes_needed - bytes_captured
                current_chunk_size = min(chunk_bytes_len, remaining)

                # Generate a single chunk of synthetic PCM
                chunk_duration = current_chunk_size / float(config.bytes_per_second)
                chunk_pcm = self.generate_synthetic_pcm(
                    sample_rate=config.sample_rate,
                    channels=config.channels,
                    duration_seconds=chunk_duration,
                    sample_width_bytes=config.sample_width_bytes,
                )

                # Buffer boundary check
                if len(accumulated) + len(chunk_pcm) > config.max_buffer_bytes:
                    raise AudioBufferOverflowError(
                        f"Capture exceeded buffer limit of {config.max_buffer_bytes} bytes "
                        f"(accumulated={len(accumulated)}, chunk={len(chunk_pcm)})."
                    )

                accumulated.extend(chunk_pcm)
                bytes_captured += len(chunk_pcm)

                if on_chunk:
                    try:
                        on_chunk(chunk_pcm)
                    except Exception as exc:
                        logger.debug("Mock on_chunk callback error: %s", exc)

                # Simulate small chunk timing if requested
                if chunk_delay_seconds > 0:
                    time.sleep(chunk_delay_seconds)

        finally:
            with self._lock:
                self.resources_cleaned_up = True
                self.last_captured_bytes_len = len(accumulated)

        return bytes(accumulated)
