"""KAISER — Local Speech-to-Text (STT) Models and Configuration.

Defines strongly-typed, immutable configuration and structured exceptions for
local offline STT inference engines:
- LocalSTTConfig (validated model, compute, and runtime configuration)
- STT exception hierarchy (missing dependencies, model not found, conversion errors, inference errors)
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Mapping, Optional

from app.core.exceptions import ValidationError
from app.voice.interfaces import VoiceProviderError


# ---------------------------------------------------------------------------
# Structured STT Exceptions
# ---------------------------------------------------------------------------


class STTError(VoiceProviderError):
    """Base exception for all local Speech-to-Text provider failures."""


class STTDependencyError(STTError):
    """Raised when required local STT libraries (e.g. faster-whisper) are missing."""


class STTModelNotFoundError(STTError):
    """Raised when the specified local model path or weights directory does not exist."""


class STTAudioValidationError(STTError):
    """Raised when incoming audio buffer is malformed, corrupt, or out of bounds."""


class STTAudioConversionError(STTError):
    """Raised when audio PCM conversion, downmixing, or resampling fails."""


class STTInferenceError(STTError):
    """Raised when local STT inference crashes or fails during execution."""


class STTTimeoutError(STTInferenceError):
    """Raised when STT inference exceeds configured execution timeout."""


class STTEmptyTranscriptionError(STTError):
    """Raised when audio contains no detectable speech or only background silence."""


# ---------------------------------------------------------------------------
# Local STT Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LocalSTTConfig:
    """Immutable, strictly-validated configuration for local Speech-to-Text engines.

    Guarantees:
    - allow_download is False by default: models must be explicitly present locally.
    - Strict validation of compute types, device selection, and timeout bounds.
    - Configurable silence detection thresholds and duration limits.
    """

    model_path: Optional[str] = None
    model_id: str = "whisper-base"
    device: str = "cpu"
    compute_type: str = "int8"
    language: Optional[str] = None
    beam_size: int = 5
    temperature: float = 0.0
    vad_filter: bool = True
    allow_download: bool = False  # Invariant: Never silently download models
    timeout_seconds: float = 30.0
    silence_threshold_rms: float = 0.005
    min_duration_seconds: float = 0.1
    max_duration_seconds: float = 60.0
    max_buffer_bytes: int = 5 * 1024 * 1024  # 5 MB safety boundary
    extra_options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate configuration parameters and freeze extra options."""
        if not self.model_id or not isinstance(self.model_id, str) or not self.model_id.strip():
            raise ValidationError("model_id must be a non-empty string.")
        object.__setattr__(self, "model_id", self.model_id.strip())

        norm_device = self.device.strip().lower()
        if norm_device not in ("cpu", "cuda", "auto"):
            raise ValidationError(f"device must be 'cpu', 'cuda', or 'auto', got '{self.device}'")
        object.__setattr__(self, "device", norm_device)

        norm_compute = self.compute_type.strip().lower()
        valid_compute = (
            "int8",
            "int8_float16",
            "int16",
            "float16",
            "float32",
            "auto",
            "default",
        )
        if norm_compute not in valid_compute:
            raise ValidationError(
                f"compute_type '{self.compute_type}' is invalid. Supported: {valid_compute}"
            )
        object.__setattr__(self, "compute_type", norm_compute)

        if not isinstance(self.beam_size, int) or self.beam_size < 1:
            raise ValidationError(f"beam_size must be an integer >= 1, got {self.beam_size}")

        if not isinstance(self.temperature, (int, float)) or self.temperature < 0.0:
            raise ValidationError(
                f"temperature must be a non-negative float, got {self.temperature}"
            )

        if not isinstance(self.timeout_seconds, (int, float)) or self.timeout_seconds <= 0.0:
            raise ValidationError(
                f"timeout_seconds must be a positive float, got {self.timeout_seconds}"
            )

        if not isinstance(self.min_duration_seconds, (int, float)) or self.min_duration_seconds <= 0.0:
            raise ValidationError(
                f"min_duration_seconds must be a positive float, got {self.min_duration_seconds}"
            )

        if (
            not isinstance(self.max_duration_seconds, (int, float))
            or self.max_duration_seconds < self.min_duration_seconds
        ):
            raise ValidationError(
                f"max_duration_seconds must be >= min_duration_seconds, got {self.max_duration_seconds}"
            )

        if not isinstance(self.max_buffer_bytes, int) or self.max_buffer_bytes < 1024:
            raise ValidationError(
                f"max_buffer_bytes must be an integer >= 1024, got {self.max_buffer_bytes}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "model_path": self.model_path,
            "model_id": self.model_id,
            "device": self.device,
            "compute_type": self.compute_type,
            "language": self.language,
            "beam_size": self.beam_size,
            "temperature": self.temperature,
            "vad_filter": self.vad_filter,
            "allow_download": self.allow_download,
            "timeout_seconds": self.timeout_seconds,
            "silence_threshold_rms": self.silence_threshold_rms,
            "min_duration_seconds": self.min_duration_seconds,
            "max_duration_seconds": self.max_duration_seconds,
            "max_buffer_bytes": self.max_buffer_bytes,
            "extra_options": copy.deepcopy(dict(self.extra_options)),
        }
