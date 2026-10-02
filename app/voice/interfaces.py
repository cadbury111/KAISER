"""KAISER — Voice Provider Interfaces (STT & TTS).

Defines abstract base classes for modular Speech-to-Text and Text-to-Speech
providers. Ensures interchangeable provider backends without altering orchestrator
or voice service routing logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping, Optional

from app.core.exceptions import KaiserError
from app.voice.models import SynthesisResult, TranscriptionResult


class VoiceProviderError(KaiserError):
    """Raised when an STT or TTS provider encounters a processing failure."""


class SpeechToTextProvider(ABC):
    """Abstract interface for Speech-to-Text transcription engines."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name descriptor (e.g. 'mock_stt', 'whisper_local')."""

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Check whether the STT backend is ready for inference."""

    @abstractmethod
    def transcribe(
        self,
        audio_data: Any,
        language: Optional[str] = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> TranscriptionResult:
        """Transcribe an audio buffer/payload into a TranscriptionResult.

        Args:
            audio_data: Audio buffer, file path, bytes, or fixture token.
            language: Optional expected language hint ('en', 'ta', 'tanglish', 'mixed').
            context: Optional contextual parameters.

        Returns:
            TranscriptionResult: Validated transcription text and confidence score.

        Raises:
            VoiceProviderError: If transcription fails or audio input is invalid.
        """


class TextToSpeechProvider(ABC):
    """Abstract interface for Text-to-Speech synthesis engines."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name descriptor (e.g. 'mock_tts', 'piper_local')."""

    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Check whether the TTS backend is ready for synthesis."""

    @abstractmethod
    def synthesize(
        self,
        text: str,
        language: str = "en",
        voice_id: Optional[str] = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> SynthesisResult:
        """Synthesize text into a structured audio SynthesisResult.

        Args:
            text: Text to synthesize into speech.
            language: Language tag for pronunciation/phoneme selection.
            voice_id: Optional speaker/voice profile identifier.
            context: Optional contextual synthesis parameters.

        Returns:
            SynthesisResult: Validated binary audio payload and metadata.

        Raises:
            VoiceProviderError: If synthesis fails or text is invalid.
        """
