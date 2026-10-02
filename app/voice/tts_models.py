"""KAISER — Text-to-Speech (TTS) Domain Models and Exception Hierarchy.

Defines strongly-typed, immutable data structures and exceptions for local,
offline Text-to-Speech synthesis adhering to KAISER voice invariants.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional

from app.core.events.event import _freeze_container
from app.core.exceptions import ValidationError
from app.voice.interfaces import VoiceProviderError


# ---------------------------------------------------------------------------
# TTS Exception Hierarchy
# ---------------------------------------------------------------------------


class TTSError(VoiceProviderError):
    """Base exception for all Text-to-Speech synthesis errors."""


class TTSDependencyError(TTSError):
    """Raised when pyttsx3 or required speech runtime is missing or cannot be imported."""


class TTSVoiceNotFoundError(TTSError):
    """Raised when a requested voice profile/ID is not found among installed voices."""


class TTSSynthesisError(TTSError):
    """Raised when speech synthesis fails during audio generation."""


class TTSTimeoutError(TTSError):
    """Raised when speech synthesis exceeds configured timeout limits."""


class TTSEmptyTextError(TTSError):
    """Raised when speech synthesis is requested on empty or whitespace-only text."""


class TTSUnsupportedLanguageError(TTSError):
    """Raised when the requested language has no compatible installed local voice."""


class TTSTextTooLongError(TTSError):
    """Raised when input text exceeds configured maximum character limits."""


# ---------------------------------------------------------------------------
# Local TTS Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Pyttsx3Config:
    """Immutable, validated configuration for the local pyttsx3 TTS provider.

    Attributes:
        voice_id: Optional installed voice token/ID (e.g. SAPI5 registry key).
        rate: Speech rate in words per minute (typically 100 to 300, default 175).
        volume: Speech volume level (0.0 to 1.0, default 1.0).
        timeout_seconds: Maximum bounded execution time for synthesis (default 30.0s).
        max_text_length: Maximum allowed input text characters (default 5000).
        allow_network: Strict offline invariant flag (must remain False).
        metadata: Immutable auxiliary engine parameters.
    """

    voice_id: Optional[str] = None
    rate: int = 175
    volume: float = 1.0
    timeout_seconds: float = 30.0
    max_text_length: int = 5000
    allow_network: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if not isinstance(self.rate, int) or self.rate < 50 or self.rate > 500:
            raise ValidationError(f"rate must be an integer between 50 and 500, got {self.rate}")

        if not isinstance(self.volume, (int, float)) or self.volume < 0.0 or self.volume > 1.0:
            raise ValidationError(f"volume must be a float between 0.0 and 1.0, got {self.volume}")

        if not isinstance(self.timeout_seconds, (int, float)) or self.timeout_seconds <= 0.0:
            raise ValidationError(f"timeout_seconds must be positive, got {self.timeout_seconds}")

        if not isinstance(self.max_text_length, int) or self.max_text_length <= 0:
            raise ValidationError(f"max_text_length must be a positive integer, got {self.max_text_length}")

        if self.allow_network:
            raise ValidationError("allow_network must be False; KAISER TTS strictly prohibits cloud calls.")

        clean_meta = dict(self.metadata or {})
        object.__setattr__(self, "metadata", _freeze_container(clean_meta))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize configuration to dictionary."""
        return {
            "voice_id": self.voice_id,
            "rate": self.rate,
            "volume": float(self.volume),
            "timeout_seconds": float(self.timeout_seconds),
            "max_text_length": self.max_text_length,
            "allow_network": self.allow_network,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }
