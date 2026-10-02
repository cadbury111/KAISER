"""KAISER — Audio Domain Models & Exceptions.

Defines strongly-typed, immutable data structures for local audio capture:
- AudioCaptureStatus enum
- AudioDeviceInfo (immutable device descriptor)
- AudioCaptureConfig (immutable, validated capture configuration)
- CapturedAudio (immutable container for captured PCM samples and metadata)
- Domain-specific audio exceptions
"""

from __future__ import annotations

import copy
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional

from app.core.events.event import _freeze_container
from app.core.exceptions import KaiserError, ValidationError


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class AudioError(KaiserError):
    """Base exception for all audio subsystem errors."""


class AudioDeviceError(AudioError):
    """Raised when an audio input device fails or is invalid."""


class AudioDeviceNotFoundError(AudioDeviceError):
    """Raised when a requested audio input device is not found."""


class AudioCaptureError(AudioError):
    """Raised when audio capture fails during execution."""


class AudioConfigurationError(AudioError):
    """Raised when audio capture configuration parameters are invalid or out of bounds."""


class AudioBufferOverflowError(AudioCaptureError):
    """Raised when captured audio exceeds maximum buffer boundaries."""


class AudioCaptureTimeoutError(AudioCaptureError):
    """Raised when audio recording reaches or exceeds bounded timeout limits."""


class AudioPermissionError(AudioCaptureError):
    """Raised when OS or hardware permissions deny access to the microphone."""


# ---------------------------------------------------------------------------
# Audio Playback Exceptions
# ---------------------------------------------------------------------------


class AudioPlaybackError(AudioError):
    """Base exception for audio playback failures."""


class AudioPlaybackDeviceNotFoundError(AudioPlaybackError):
    """Raised when an audio output device is missing or invalid."""


class AudioPlaybackFormatError(AudioPlaybackError):
    """Raised when an audio payload has an unsupported or corrupted format."""


class AudioPlaybackConcurrencyError(AudioPlaybackError):
    """Raised when overlapping playback requests are rejected."""


class AudioPlaybackTimeoutError(AudioPlaybackError):
    """Raised when audio playback exceeds bounded duration limits."""


class AudioPlaybackCancelledError(AudioPlaybackError):
    """Raised when audio playback is cancelled before completion."""


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class AudioCaptureStatus(str, Enum):
    """Operational status of the audio capture lifecycle."""

    IDLE = "IDLE"
    RECORDING = "RECORDING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class AudioPlaybackStatus(str, Enum):
    """Operational status of the audio playback lifecycle."""

    IDLE = "IDLE"
    PLAYING = "PLAYING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Audio Device Info Model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AudioDeviceInfo:
    """Immutable representation of an audio input device.

    Attributes:
        device_id: System index or identifier for the device.
        name: Human-readable device name.
        max_input_channels: Number of input channels (>= 1 for microphone).
        default_sample_rate: Default sample rate supported by device in Hz.
        is_default: Whether this is the system default input device.
        is_available: Whether the device is currently accessible.
        hostapi: Optional audio host API description (e.g. MME, WASAPI, ALSA).
        extra_info: Immutable auxiliary hardware or driver metadata.
    """

    device_id: int
    name: str
    max_input_channels: int
    default_sample_rate: float
    is_default: bool = False
    is_available: bool = True
    hostapi: Optional[str] = None
    extra_info: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate device invariants and freeze metadata."""
        if not isinstance(self.device_id, int) or self.device_id < 0:
            raise ValidationError(f"device_id must be a non-negative integer, got {self.device_id}")

        if not self.name or not isinstance(self.name, str) or not self.name.strip():
            raise ValidationError("Device name must be a non-empty string.")
        object.__setattr__(self, "name", self.name.strip())

        if not isinstance(self.max_input_channels, int) or self.max_input_channels < 0:
            raise ValidationError(
                f"max_input_channels must be a non-negative integer, got {self.max_input_channels}"
            )

        if not isinstance(self.default_sample_rate, (int, float)) or self.default_sample_rate <= 0:
            raise ValidationError(
                f"default_sample_rate must be a positive number, got {self.default_sample_rate}"
            )

        clean_extra = dict(self.extra_info or {})
        object.__setattr__(self, "extra_info", _freeze_container(clean_extra))

    @property
    def max_output_channels(self) -> int:
        """Number of output channels supported by the device."""
        if "max_output_channels" in self.extra_info:
            return int(self.extra_info["max_output_channels"])
        lower = self.name.lower()
        if self.max_input_channels == 0 and any(k in lower for k in ("output", "speaker", "headphone")):
            return 2
        return 0

    @property
    def is_input_device(self) -> bool:
        """Check whether the device supports audio input channels."""
        return self.max_input_channels > 0

    @property
    def is_output_device(self) -> bool:
        """Check whether the device supports audio output channels."""
        return self.max_output_channels > 0

    def to_dict(self) -> Dict[str, Any]:
        """Serialize device info to dictionary."""
        return {
            "device_id": self.device_id,
            "name": self.name,
            "max_input_channels": self.max_input_channels,
            "max_output_channels": self.max_output_channels,
            "default_sample_rate": float(self.default_sample_rate),
            "is_default": self.is_default,
            "is_available": self.is_available,
            "is_input_device": self.is_input_device,
            "is_output_device": self.is_output_device,
            "hostapi": self.hostapi,
            "extra_info": copy.deepcopy(dict(self.extra_info)),
        }


# ---------------------------------------------------------------------------
# Audio Capture Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AudioCaptureConfig:
    """Immutable, strictly-validated configuration for bounded audio recording.

    Enforces:
    - Bounded sample rate, channels, and sample width
    - Strict maximum recording duration to prevent indefinite listening
    - Maximum buffer byte bounds to prevent memory exhaustion
    - Chunk duration bounds for streaming slices
    """

    sample_rate: int = 16000
    channels: int = 1
    sample_width_bytes: int = 2
    max_duration_seconds: float = 10.0
    chunk_duration_ms: int = 100
    max_buffer_bytes: int = 5 * 1024 * 1024  # 5 MB safety boundary
    device_id: Optional[int] = None
    device_name: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate audio configuration bounds."""
        if not isinstance(self.sample_rate, int) or not (8000 <= self.sample_rate <= 96000):
            raise AudioConfigurationError(
                f"sample_rate must be an integer between 8000 and 96000 Hz, got {self.sample_rate}"
            )

        if not isinstance(self.channels, int) or not (1 <= self.channels <= 8):
            raise AudioConfigurationError(
                f"channels must be an integer between 1 and 8, got {self.channels}"
            )

        if self.sample_width_bytes not in (1, 2, 4):
            raise AudioConfigurationError(
                f"sample_width_bytes must be 1 (8-bit), 2 (16-bit PCM), or 4 (32-bit), got {self.sample_width_bytes}"
            )

        if not isinstance(self.max_duration_seconds, (int, float)) or not (0.1 <= self.max_duration_seconds <= 60.0):
            raise AudioConfigurationError(
                f"max_duration_seconds must be between 0.1 and 60.0 seconds, got {self.max_duration_seconds}"
            )
        object.__setattr__(self, "max_duration_seconds", float(self.max_duration_seconds))

        if not isinstance(self.chunk_duration_ms, int) or not (10 <= self.chunk_duration_ms <= 1000):
            raise AudioConfigurationError(
                f"chunk_duration_ms must be an integer between 10 and 1000 ms, got {self.chunk_duration_ms}"
            )

        if not isinstance(self.max_buffer_bytes, int) or not (1024 <= self.max_buffer_bytes <= 50 * 1024 * 1024):
            raise AudioConfigurationError(
                f"max_buffer_bytes must be between 1024 bytes and 50MB, got {self.max_buffer_bytes}"
            )

        if self.device_id is not None:
            if not isinstance(self.device_id, int) or self.device_id < 0:
                raise AudioConfigurationError(
                    f"device_id must be a non-negative integer or None, got {self.device_id}"
                )

        # Cross-validation: Check if maximum possible bytes exceeds buffer limit
        required_bytes = int(self.sample_rate * self.channels * self.sample_width_bytes * self.max_duration_seconds)
        if required_bytes > self.max_buffer_bytes:
            raise AudioConfigurationError(
                f"Configuration requires {required_bytes} bytes for {self.max_duration_seconds}s "
                f"which exceeds max_buffer_bytes={self.max_buffer_bytes}."
            )

    @property
    def bytes_per_second(self) -> int:
        """Number of PCM audio bytes per second."""
        return self.sample_rate * self.channels * self.sample_width_bytes

    @property
    def bytes_per_frame(self) -> int:
        """Number of PCM audio bytes per single sample frame."""
        return self.channels * self.sample_width_bytes

    @property
    def chunk_samples(self) -> int:
        """Number of sample frames per chunk."""
        return int(self.sample_rate * (self.chunk_duration_ms / 1000.0))

    @property
    def chunk_bytes(self) -> int:
        """Expected byte size of each audio chunk."""
        return self.chunk_samples * self.bytes_per_frame

    @property
    def expected_max_bytes(self) -> int:
        """Maximum expected byte size for max_duration_seconds."""
        return int(self.bytes_per_second * self.max_duration_seconds)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize configuration parameters to dictionary."""
        return {
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "sample_width_bytes": self.sample_width_bytes,
            "max_duration_seconds": self.max_duration_seconds,
            "chunk_duration_ms": self.chunk_duration_ms,
            "max_buffer_bytes": self.max_buffer_bytes,
            "bytes_per_second": self.bytes_per_second,
            "device_id": self.device_id,
            "device_name": self.device_name,
        }


# ---------------------------------------------------------------------------
# Captured Audio Container
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CapturedAudio:
    """Immutable result container representing captured PCM audio samples.

    Safety:
    - Never prints or logs raw audio bytes in __repr__ or __str__.
    - Guarantees complete metadata immutability.
    - Provides accurate duration and sample count properties.
    """

    audio_bytes: bytes
    sample_rate: int
    channels: int
    sample_width_bytes: int
    duration_seconds: float
    device_id: Optional[int] = None
    device_name: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    captured_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        """Validate captured audio integrity and freeze metadata."""
        if not isinstance(self.audio_bytes, bytes):
            raise ValidationError("audio_bytes must be raw bytes.")

        if not isinstance(self.sample_rate, int) or self.sample_rate <= 0:
            raise ValidationError(f"sample_rate must be a positive integer, got {self.sample_rate}")

        if not isinstance(self.channels, int) or self.channels <= 0:
            raise ValidationError(f"channels must be a positive integer, got {self.channels}")

        if not isinstance(self.sample_width_bytes, int) or self.sample_width_bytes <= 0:
            raise ValidationError(
                f"sample_width_bytes must be a positive integer, got {self.sample_width_bytes}"
            )

        if not isinstance(self.duration_seconds, (int, float)) or self.duration_seconds < 0:
            raise ValidationError(
                f"duration_seconds must be a non-negative float, got {self.duration_seconds}"
            )
        object.__setattr__(self, "duration_seconds", float(self.duration_seconds))

        clean_metadata = dict(self.metadata or {})
        object.__setattr__(self, "metadata", _freeze_container(clean_metadata))

    @property
    def byte_count(self) -> int:
        """Total size of captured audio payload in bytes."""
        return len(self.audio_bytes)

    @property
    def num_samples(self) -> int:
        """Total number of sample frames captured."""
        frame_size = self.channels * self.sample_width_bytes
        return len(self.audio_bytes) // frame_size if frame_size > 0 else 0

    @property
    def duration_ms(self) -> float:
        """Duration in milliseconds."""
        return self.duration_seconds * 1000.0

    @property
    def is_empty(self) -> bool:
        """Check if audio payload is empty."""
        return len(self.audio_bytes) == 0

    @property
    def iso_captured_at(self) -> str:
        """Return captured timestamp formatted as ISO-8601 UTC string."""
        return datetime.fromtimestamp(self.captured_at, tz=timezone.utc).isoformat()

    def __repr__(self) -> str:
        """Privacy-safe string representation (redacts raw audio samples)."""
        return (
            f"<CapturedAudio: duration={self.duration_seconds:.2f}s, "
            f"rate={self.sample_rate}Hz, channels={self.channels}, "
            f"bytes={self.byte_count}, device_id={self.device_id}>"
        )

    def to_dict(self, include_audio_bytes: bool = False) -> Dict[str, Any]:
        """Convert container to dictionary.

        Args:
            include_audio_bytes: When False (default), raw binary is redacted for safety.
        """
        data: Dict[str, Any] = {
            "byte_count": self.byte_count,
            "num_samples": self.num_samples,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "sample_width_bytes": self.sample_width_bytes,
            "duration_seconds": self.duration_seconds,
            "duration_ms": self.duration_ms,
            "device_id": self.device_id,
            "device_name": self.device_name,
            "captured_at": self.captured_at,
            "iso_captured_at": self.iso_captured_at,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }
        if include_audio_bytes:
            data["audio_bytes"] = self.audio_bytes
        else:
            data["audio_bytes"] = f"[REDACTED_AUDIO_BYTES_SIZE={self.byte_count}]"
        return data


# ---------------------------------------------------------------------------
# Audio Playback Result Model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AudioPlaybackResult:
    """Immutable result of an audio playback operation.

    Attributes:
        playback_id: Unique identifier for this playback execution.
        correlation_id: Distributed tracing correlation ID.
        status: Operational status of playback (COMPLETED, CANCELLED, FAILED).
        device_id: System audio device identifier used for playback.
        device_name: Name of audio output device.
        sample_rate: Sampling rate of played audio in Hz.
        channels: Number of channels in playback stream.
        duration_ms: Duration of playback in milliseconds.
        error: Descriptive error message if playback failed.
        created_at: POSIX UTC creation timestamp.
        metadata: Immutable auxiliary playback metrics.
    """

    playback_id: str
    correlation_id: str
    status: AudioPlaybackStatus
    device_id: int
    device_name: str
    sample_rate: int
    channels: int
    duration_ms: float
    error: Optional[str] = None
    created_at: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate playback result invariants and freeze metadata."""
        if not self.playback_id or not isinstance(self.playback_id, str):
            raise ValidationError("playback_id must be a non-empty string.")
        object.__setattr__(self, "playback_id", self.playback_id.strip())

        if not self.correlation_id or not isinstance(self.correlation_id, str):
            raise ValidationError("correlation_id must be a non-empty string.")
        object.__setattr__(self, "correlation_id", self.correlation_id.strip())

        if not isinstance(self.status, AudioPlaybackStatus):
            object.__setattr__(self, "status", AudioPlaybackStatus(str(self.status)))

        resolved_created_at = time.time() if self.created_at == 0.0 else self.created_at
        object.__setattr__(self, "created_at", resolved_created_at)

        clean_meta = dict(self.metadata or {})
        object.__setattr__(self, "metadata", _freeze_container(clean_meta))

    @property
    def is_success(self) -> bool:
        """Check whether playback completed successfully without errors."""
        return self.status == AudioPlaybackStatus.COMPLETED and self.error is None

    @property
    def iso_created_at(self) -> str:
        """Return creation timestamp formatted as ISO-8601 UTC string."""
        return datetime.fromtimestamp(self.created_at, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert result into serializable dictionary."""
        return {
            "playback_id": self.playback_id,
            "correlation_id": self.correlation_id,
            "status": self.status.value,
            "device_id": self.device_id,
            "device_name": self.device_name,
            "sample_rate": self.sample_rate,
            "channels": self.channels,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "created_at": self.created_at,
            "iso_created_at": self.iso_created_at,
            "is_success": self.is_success,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }

    @classmethod
    def completed(
        cls,
        playback_id: str,
        correlation_id: str,
        device_id: int,
        device_name: str,
        sample_rate: int,
        channels: int,
        duration_ms: float,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> AudioPlaybackResult:
        """Factory for a successfully completed playback."""
        return cls(
            playback_id=playback_id,
            correlation_id=correlation_id,
            status=AudioPlaybackStatus.COMPLETED,
            device_id=device_id,
            device_name=device_name,
            sample_rate=sample_rate,
            channels=channels,
            duration_ms=duration_ms,
            error=None,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def cancelled(
        cls,
        playback_id: str,
        correlation_id: str,
        device_id: int,
        device_name: str,
        sample_rate: int = 16000,
        channels: int = 1,
        duration_ms: float = 0.0,
        error: str = "Playback cancelled.",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> AudioPlaybackResult:
        """Factory for a cancelled playback."""
        return cls(
            playback_id=playback_id,
            correlation_id=correlation_id,
            status=AudioPlaybackStatus.CANCELLED,
            device_id=device_id,
            device_name=device_name,
            sample_rate=sample_rate,
            channels=channels,
            duration_ms=duration_ms,
            error=error,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def failure(
        cls,
        playback_id: str,
        correlation_id: str,
        error: str,
        device_id: int = -1,
        device_name: str = "unknown",
        sample_rate: int = 16000,
        channels: int = 1,
        duration_ms: float = 0.0,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> AudioPlaybackResult:
        """Factory for a failed playback."""
        return cls(
            playback_id=playback_id,
            correlation_id=correlation_id,
            status=AudioPlaybackStatus.FAILED,
            device_id=device_id,
            device_name=device_name,
            sample_rate=sample_rate,
            channels=channels,
            duration_ms=duration_ms,
            error=error,
            metadata=dict(metadata or {}),
        )
