"""KAISER — Voice Domain Models.

Provides strongly-typed, immutable data structures for voice interactions:
- VoiceOutputStatus & VoiceSessionStatus enums
- VoiceInput (immutable incoming speech/text input)
- VoiceOutput (immutable structured voice/text response)
- TranscriptionResult & SynthesisResult (provider result containers)
"""

from __future__ import annotations

import copy
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Tuple, Union

from app.core.events.event import _freeze_container
from app.core.exceptions import ValidationError
from app.orchestrator.lifecycle import sanitize_container
from app.orchestrator.models import InputLanguage, normalize_language


class VoiceOutputStatus(str, Enum):
    """Execution status for generated voice outputs."""

    SYNTHESIZED = "SYNTHESIZED"
    TEXT_ONLY = "TEXT_ONLY"
    FAILED = "FAILED"


class VoiceSessionStatus(str, Enum):
    """Operational lifecycle statuses for voice sessions."""

    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    CLOSED = "CLOSED"
    FAILED = "FAILED"


class VoiceTurnStatus(str, Enum):
    """Operational lifecycle statuses for a single voice command turn."""

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    EMPTY_SPEECH = "EMPTY_SPEECH"
    STT_UNAVAILABLE = "STT_UNAVAILABLE"
    STT_FAILED = "STT_FAILED"
    STT_TIMEOUT = "STT_TIMEOUT"
    CAPTURE_FAILED = "CAPTURE_FAILED"
    COMMAND_REJECTED = "COMMAND_REJECTED"
    CONCURRENCY_ERROR = "CONCURRENCY_ERROR"


@dataclass(frozen=True)
class VoiceInput:
    """Immutable representation of incoming voice/speech interaction.

    Attributes:
        input_id: Unique identifier for this speech input.
        session_id: Active session identifier threading the conversation.
        text: Transcribed speech text. Must be non-empty and non-whitespace.
        language: Language metadata tag ('en', 'ta', 'tanglish', 'mixed').
        source: Audio source tag (e.g. 'mock_stt', 'text_input').
        created_at: POSIX UTC timestamp when input occurred.
        metadata: Immutable auxiliary routing or acoustic metadata.
    """

    input_id: str
    session_id: str
    text: str
    language: str = "en"
    source: str = "mock_stt"
    created_at: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate voice input invariants and freeze nested metadata."""
        if not self.input_id or not isinstance(self.input_id, str) or not self.input_id.strip():
            raise ValidationError("input_id must be a non-empty string.")
        object.__setattr__(self, "input_id", self.input_id.strip())

        if not self.session_id or not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValidationError("session_id must be a non-empty string.")
        object.__setattr__(self, "session_id", self.session_id.strip())

        if not self.text or not isinstance(self.text, str) or not self.text.strip():
            raise ValidationError("Voice input text must not be empty or whitespace-only.")
        object.__setattr__(self, "text", self.text.strip())

        norm_lang = normalize_language(self.language)
        object.__setattr__(self, "language", norm_lang)

        if not self.source or not isinstance(self.source, str) or not self.source.strip():
            raise ValidationError("source must be a non-empty string.")
        object.__setattr__(self, "source", self.source.strip())

        if self.created_at < 0:
            raise ValidationError(f"created_at must be a positive float, got {self.created_at}")
        resolved_created_at = time.time() if self.created_at == 0.0 else self.created_at
        object.__setattr__(self, "created_at", resolved_created_at)

        clean_meta = dict(self.metadata or {})
        object.__setattr__(self, "metadata", _freeze_container(clean_meta))

    @classmethod
    def create(
        cls,
        text: str,
        session_id: Optional[str] = None,
        input_id: Optional[str] = None,
        language: str = "en",
        source: str = "mock_stt",
        created_at: Optional[float] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceInput:
        """Factory method to construct a VoiceInput with safe defaults.

        Args:
            text: Transcribed speech text.
            session_id: Optional session identifier (auto-generated if omitted).
            input_id: Optional input identifier (auto-generated if omitted).
            language: Language metadata tag (default 'en').
            source: Source channel tag (default 'mock_stt').
            created_at: Optional timestamp (defaults to current time).
            metadata: Optional dictionary of contextual metadata.

        Returns:
            VoiceInput: Validated immutable voice input.
        """
        resolved_input_id = (input_id or uuid.uuid4().hex).strip()
        resolved_session_id = (session_id or uuid.uuid4().hex).strip()
        resolved_created_at = time.time() if created_at is None else float(created_at)
        clean_metadata = copy.deepcopy(sanitize_container(metadata or {}))

        return cls(
            input_id=resolved_input_id,
            session_id=resolved_session_id,
            text=text,
            language=language,
            source=source,
            created_at=resolved_created_at,
            metadata=clean_metadata,
        )

    @property
    def iso_created_at(self) -> str:
        """Return the creation timestamp formatted as ISO-8601 UTC string."""
        return datetime.fromtimestamp(self.created_at, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert VoiceInput into a serializable dictionary."""
        return {
            "input_id": self.input_id,
            "session_id": self.session_id,
            "text": self.text,
            "language": self.language,
            "source": self.source,
            "created_at": self.created_at,
            "iso_created_at": self.iso_created_at,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class VoiceOutput:
    """Immutable representation of voice output returned to user.

    Attributes:
        output_id: Unique identifier for this speech output.
        session_id: Active session identifier matching the request.
        text: Response message text.
        language: Language of the response.
        status: Delivery status (SYNTHESIZED, TEXT_ONLY, FAILED).
        created_at: POSIX UTC creation timestamp.
        audio_payload: Optional synthesized audio bytes (mock or binary).
        audio_format: Format descriptor (e.g. 'mock_pcm', 'wav', 'none').
        duration_ms: Duration of synthesized audio in milliseconds.
        error: Descriptive error message if output synthesis or processing failed.
        metadata: Immutable auxiliary metrics or provider metadata.
    """

    output_id: str
    session_id: str
    text: str
    language: str = "en"
    status: VoiceOutputStatus = VoiceOutputStatus.TEXT_ONLY
    created_at: float = 0.0
    audio_payload: Optional[bytes] = None
    audio_format: str = "none"
    duration_ms: Optional[float] = None
    error: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate invariants and freeze nested metadata."""
        if not self.output_id or not isinstance(self.output_id, str) or not self.output_id.strip():
            raise ValidationError("output_id must be a non-empty string.")
        object.__setattr__(self, "output_id", self.output_id.strip())

        if not self.session_id or not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValidationError("session_id must be a non-empty string.")
        object.__setattr__(self, "session_id", self.session_id.strip())

        if not self.text or not isinstance(self.text, str) or not self.text.strip():
            raise ValidationError("Voice output text must not be empty or whitespace-only.")
        object.__setattr__(self, "text", self.text.strip())

        norm_lang = normalize_language(self.language) if self.language != "unknown" else "unknown"
        object.__setattr__(self, "language", norm_lang)

        if not isinstance(self.status, VoiceOutputStatus):
            object.__setattr__(self, "status", VoiceOutputStatus(str(self.status)))

        if self.created_at < 0:
            raise ValidationError(f"created_at must be a positive float, got {self.created_at}")
        resolved_created_at = time.time() if self.created_at == 0.0 else self.created_at
        object.__setattr__(self, "created_at", resolved_created_at)

        # Invariant: Do not represent failed output as successful
        if self.status == VoiceOutputStatus.SYNTHESIZED and self.error is not None:
            raise ValidationError(
                "Invariant violated: VoiceOutput cannot have status SYNTHESIZED when error is present."
            )

        if self.status == VoiceOutputStatus.FAILED and not self.error:
            object.__setattr__(self, "error", "Voice output processing failed.")

        clean_meta = dict(self.metadata or {})
        object.__setattr__(self, "metadata", _freeze_container(clean_meta))

    @property
    def is_success(self) -> bool:
        """Check whether voice output generation succeeded without errors."""
        return self.status != VoiceOutputStatus.FAILED and self.error is None

    @property
    def has_audio(self) -> bool:
        """Check whether voice output contains a synthesized audio payload."""
        return self.audio_payload is not None and len(self.audio_payload) > 0

    @property
    def iso_created_at(self) -> str:
        """Return creation timestamp formatted as ISO-8601 UTC string."""
        return datetime.fromtimestamp(self.created_at, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert VoiceOutput into a serializable dictionary."""
        return {
            "output_id": self.output_id,
            "session_id": self.session_id,
            "text": self.text,
            "language": self.language,
            "status": self.status.value,
            "created_at": self.created_at,
            "iso_created_at": self.iso_created_at,
            "audio_format": self.audio_format,
            "has_audio": self.audio_payload is not None,
            "audio_bytes_len": len(self.audio_payload) if self.audio_payload else 0,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "is_success": self.is_success,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }

    @classmethod
    def text_only(
        cls,
        text: str,
        session_id: str,
        output_id: Optional[str] = None,
        language: str = "en",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceOutput:
        """Factory creating a text-only VoiceOutput (no audio synthesis)."""
        return cls(
            output_id=(output_id or uuid.uuid4().hex).strip(),
            session_id=session_id.strip(),
            text=text,
            language=language,
            status=VoiceOutputStatus.TEXT_ONLY,
            created_at=time.time(),
            audio_payload=None,
            audio_format="none",
            duration_ms=None,
            error=None,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def synthesized(
        cls,
        text: str,
        session_id: str,
        audio_payload: bytes,
        audio_format: str = "mock_pcm",
        duration_ms: float = 0.0,
        output_id: Optional[str] = None,
        language: str = "en",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceOutput:
        """Factory creating a synthesized VoiceOutput with audio payload."""
        return cls(
            output_id=(output_id or uuid.uuid4().hex).strip(),
            session_id=session_id.strip(),
            text=text,
            language=language,
            status=VoiceOutputStatus.SYNTHESIZED,
            created_at=time.time(),
            audio_payload=audio_payload,
            audio_format=audio_format,
            duration_ms=duration_ms,
            error=None,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def failure(
        cls,
        error: str,
        session_id: str,
        text: str = "",
        output_id: Optional[str] = None,
        language: str = "en",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceOutput:
        """Factory creating a failed VoiceOutput with error description."""
        fallback_text = text.strip() if text and text.strip() else f"Voice processing error: {error}"
        return cls(
            output_id=(output_id or uuid.uuid4().hex).strip(),
            session_id=session_id.strip(),
            text=fallback_text,
            language=language,
            status=VoiceOutputStatus.FAILED,
            created_at=time.time(),
            audio_payload=None,
            audio_format="none",
            duration_ms=None,
            error=error,
            metadata=dict(metadata or {}),
        )


@dataclass(frozen=True)
class TranscriptionResult:
    """Immutable transcription result from a Speech-to-Text provider.

    Attributes:
        text: Recognized speech text.
        language: Identified or supplied language tag.
        confidence: Recognition confidence score (0.0 to 1.0).
        is_final: Whether this is the final transcription result.
        duration_seconds: Duration of the transcribed audio segment.
        metadata: Additional provider diagnostic attributes.
    """

    text: str
    language: str = "en"
    confidence: float = 1.0
    is_final: bool = True
    duration_seconds: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate bounds and freeze metadata."""
        norm_lang = normalize_language(self.language) if self.language != "unknown" else "unknown"
        object.__setattr__(self, "language", norm_lang)

        bounded_conf = max(0.0, min(1.0, float(self.confidence)))
        object.__setattr__(self, "confidence", round(bounded_conf, 4))

        if self.duration_seconds < 0:
            object.__setattr__(self, "duration_seconds", 0.0)

        clean_meta = dict(self.metadata or {})
        object.__setattr__(self, "metadata", _freeze_container(clean_meta))

    def to_dict(self) -> Dict[str, Any]:
        """Convert TranscriptionResult into a serializable dictionary."""
        return {
            "text": self.text,
            "language": self.language,
            "confidence": self.confidence,
            "is_final": self.is_final,
            "duration_seconds": self.duration_seconds,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class SynthesisResult:
    """Immutable audio synthesis result from a Text-to-Speech provider.

    Attributes:
        audio_bytes: Synthesized audio binary payload.
        audio_format: Encoded audio format descriptor (e.g. 'mock_pcm', 'wav').
        sample_rate: Audio sampling frequency in Hz (e.g. 16000, 24000).
        duration_ms: Duration of synthesized audio in milliseconds.
        text: Source text synthesized.
        language: Language of synthesis.
        metadata: Additional acoustic or provider metadata.
    """

    audio_bytes: bytes
    audio_format: str = "mock_pcm"
    sample_rate: int = 16000
    duration_ms: float = 0.0
    text: str = ""
    language: str = "en"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Freeze metadata and validate properties."""
        if not isinstance(self.audio_bytes, bytes):
            raise ValidationError(f"audio_bytes must be bytes, got {type(self.audio_bytes).__name__}")

        norm_lang = normalize_language(self.language) if self.language != "unknown" else "unknown"
        object.__setattr__(self, "language", norm_lang)

        if self.duration_ms < 0:
            object.__setattr__(self, "duration_ms", 0.0)

        clean_meta = dict(self.metadata or {})
        object.__setattr__(self, "metadata", _freeze_container(clean_meta))

    def to_dict(self) -> Dict[str, Any]:
        """Convert SynthesisResult into a serializable dictionary."""
        return {
            "audio_bytes_len": len(self.audio_bytes),
            "audio_format": self.audio_format,
            "sample_rate": self.sample_rate,
            "duration_ms": self.duration_ms,
            "text": self.text,
            "language": self.language,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class VoiceTurnResult:
    """Immutable result of a single-turn voice command pipeline.

    Encapsulates:
    - Tracing identifiers (turn_id, session_id, correlation_id)
    - Turn execution status (status, transcription_status, command_status, synthesis_status, playback_status)
    - Speech recognition metadata (recognized_text, language, language_hint, confidence)
    - Orchestrator command response (command_response: Optional[VoiceOutput], response_text)
    - Diagnostics & telemetry (error, synthesis_error, playback_error, duration_ms, created_at, metadata)
    """

    turn_id: str
    session_id: str
    correlation_id: str
    status: VoiceTurnStatus
    transcription_status: str = "skipped"
    command_status: str = "skipped"
    synthesis_status: str = "skipped"
    playback_status: str = "skipped"
    recognized_text: Optional[str] = None
    language: str = "en"
    language_hint: Optional[str] = None
    confidence: float = 0.0
    command_response: Optional[VoiceOutput] = None
    response_text: str = ""
    error: Optional[str] = None
    synthesis_error: Optional[str] = None
    playback_error: Optional[str] = None
    duration_ms: float = 0.0
    created_at: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate voice turn result invariants and freeze nested metadata."""
        if not self.turn_id or not isinstance(self.turn_id, str) or not self.turn_id.strip():
            raise ValidationError("turn_id must be a non-empty string.")
        object.__setattr__(self, "turn_id", self.turn_id.strip())

        if not self.session_id or not isinstance(self.session_id, str) or not self.session_id.strip():
            raise ValidationError("session_id must be a non-empty string.")
        object.__setattr__(self, "session_id", self.session_id.strip())

        if not self.correlation_id or not isinstance(self.correlation_id, str) or not self.correlation_id.strip():
            raise ValidationError("correlation_id must be a non-empty string.")
        object.__setattr__(self, "correlation_id", self.correlation_id.strip())

        if not isinstance(self.status, VoiceTurnStatus):
            object.__setattr__(self, "status", VoiceTurnStatus(str(self.status)))

        norm_lang = normalize_language(self.language) if self.language != "unknown" else "unknown"
        object.__setattr__(self, "language", norm_lang)

        if self.language_hint:
            norm_hint = normalize_language(self.language_hint) if self.language_hint != "unknown" else "unknown"
            object.__setattr__(self, "language_hint", norm_hint)

        bounded_conf = max(0.0, min(1.0, float(self.confidence)))
        object.__setattr__(self, "confidence", round(bounded_conf, 4))

        if self.duration_ms < 0:
            object.__setattr__(self, "duration_ms", 0.0)

        if self.created_at < 0:
            raise ValidationError(f"created_at must be a positive float, got {self.created_at}")
        resolved_created_at = time.time() if self.created_at == 0.0 else self.created_at
        object.__setattr__(self, "created_at", resolved_created_at)

        clean_meta = sanitize_container(dict(self.metadata or {}))
        object.__setattr__(self, "metadata", _freeze_container(clean_meta))

    @property
    def is_success(self) -> bool:
        """Check whether the voice turn completed successfully without errors."""
        return self.status == VoiceTurnStatus.COMPLETED and self.error is None

    @property
    def iso_created_at(self) -> str:
        """Return creation timestamp formatted as ISO-8601 UTC string."""
        return datetime.fromtimestamp(self.created_at, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert VoiceTurnResult into a serializable dictionary."""
        return {
            "turn_id": self.turn_id,
            "session_id": self.session_id,
            "correlation_id": self.correlation_id,
            "status": self.status.value,
            "transcription_status": self.transcription_status,
            "command_status": self.command_status,
            "synthesis_status": self.synthesis_status,
            "playback_status": self.playback_status,
            "recognized_text": self.recognized_text,
            "language": self.language,
            "language_hint": self.language_hint,
            "confidence": self.confidence,
            "command_response": self.command_response.to_dict() if self.command_response else None,
            "response_text": self.response_text,
            "error": self.error,
            "synthesis_error": self.synthesis_error,
            "playback_error": self.playback_error,
            "duration_ms": self.duration_ms,
            "created_at": self.created_at,
            "iso_created_at": self.iso_created_at,
            "is_success": self.is_success,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }

    @classmethod
    def completed(
        cls,
        turn_id: str,
        session_id: str,
        correlation_id: str,
        recognized_text: str,
        command_response: VoiceOutput,
        language: str = "en",
        language_hint: Optional[str] = None,
        confidence: float = 1.0,
        duration_ms: float = 0.0,
        command_status: str = "success",
        synthesis_status: str = "skipped",
        playback_status: str = "skipped",
        synthesis_error: Optional[str] = None,
        playback_error: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceTurnResult:
        """Factory for a successfully executed voice command turn."""
        return cls(
            turn_id=turn_id,
            session_id=session_id,
            correlation_id=correlation_id,
            status=VoiceTurnStatus.COMPLETED,
            transcription_status="success",
            command_status=command_status,
            synthesis_status=synthesis_status,
            playback_status=playback_status,
            recognized_text=recognized_text,
            language=language,
            language_hint=language_hint,
            confidence=confidence,
            command_response=command_response,
            response_text=command_response.text,
            error=None,
            synthesis_error=synthesis_error,
            playback_error=playback_error,
            duration_ms=duration_ms,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def empty_speech(
        cls,
        turn_id: str,
        session_id: str,
        correlation_id: str,
        language: str = "en",
        language_hint: Optional[str] = None,
        duration_ms: float = 0.0,
        error: str = "No speech detected in audio.",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceTurnResult:
        """Factory for an empty/silent speech outcome."""
        return cls(
            turn_id=turn_id,
            session_id=session_id,
            correlation_id=correlation_id,
            status=VoiceTurnStatus.EMPTY_SPEECH,
            transcription_status="empty",
            command_status="skipped",
            synthesis_status="skipped",
            playback_status="skipped",
            recognized_text=None,
            language=language,
            language_hint=language_hint,
            confidence=0.0,
            command_response=None,
            response_text="No speech detected.",
            error=error,
            synthesis_error=None,
            playback_error=None,
            duration_ms=duration_ms,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def failure(
        cls,
        turn_id: str,
        session_id: str,
        correlation_id: str,
        error: str,
        status: VoiceTurnStatus = VoiceTurnStatus.FAILED,
        transcription_status: str = "failed",
        command_status: str = "skipped",
        synthesis_status: str = "skipped",
        playback_status: str = "skipped",
        synthesis_error: Optional[str] = None,
        playback_error: Optional[str] = None,
        recognized_text: Optional[str] = None,
        command_response: Optional[VoiceOutput] = None,
        language: str = "en",
        language_hint: Optional[str] = None,
        duration_ms: float = 0.0,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceTurnResult:
        """Factory for a failed voice turn."""
        resp_text = command_response.text if command_response else f"Voice command failed: {error}"
        return cls(
            turn_id=turn_id,
            session_id=session_id,
            correlation_id=correlation_id,
            status=status,
            transcription_status=transcription_status,
            command_status=command_status,
            synthesis_status=synthesis_status,
            playback_status=playback_status,
            recognized_text=recognized_text,
            language=language,
            language_hint=language_hint,
            confidence=0.0,
            command_response=command_response,
            response_text=resp_text,
            error=error,
            synthesis_error=synthesis_error,
            playback_error=playback_error,
            duration_ms=duration_ms,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def cancelled(
        cls,
        turn_id: str,
        session_id: str,
        correlation_id: str,
        duration_ms: float = 0.0,
        error: str = "Voice turn capture was cancelled.",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceTurnResult:
        """Factory for a cancelled voice turn."""
        return cls(
            turn_id=turn_id,
            session_id=session_id,
            correlation_id=correlation_id,
            status=VoiceTurnStatus.CANCELLED,
            transcription_status="cancelled",
            command_status="skipped",
            synthesis_status="skipped",
            playback_status="skipped",
            recognized_text=None,
            language="en",
            language_hint=None,
            confidence=0.0,
            command_response=None,
            response_text="Voice turn cancelled.",
            error=error,
            synthesis_error=None,
            playback_error=None,
            duration_ms=duration_ms,
            metadata=dict(metadata or {}),
        )
