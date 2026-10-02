"""KAISER — Mock Text-to-Speech (TTS) Provider.

Provides a deterministic mock audio synthesis engine returning synthetic binary
payloads without invoking external speech APIs or hardware audio drivers.
"""

from __future__ import annotations

import copy
import threading
from typing import Any, Dict, Mapping, Optional

from app.core.logging_config import get_logger
from app.orchestrator.models import normalize_language
from app.voice.interfaces import TextToSpeechProvider, VoiceProviderError
from app.voice.models import SynthesisResult

logger = get_logger("voice.providers.mock_tts")


class MockTTSProvider(TextToSpeechProvider):
    """Deterministic Mock TTS engine generating synthetic audio bytes.

    Guarantees:
    1. Zero audio driver interaction or audio hardware playback.
    2. Zero external speech synthesis network calls.
    3. Deterministic synthetic audio output generation.
    4. Controlled failure simulation for robustness testing.
    """

    def __init__(self, name: str = "mock_tts") -> None:
        self._name = name
        self._lock = threading.RLock()
        self._is_available = True
        self._should_fail = False
        self._failure_message = "Simulated TTS synthesis failure"

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_available(self) -> bool:
        with self._lock:
            return self._is_available and not self._should_fail

    def set_availability(self, available: bool) -> None:
        """Set whether the mock TTS provider is available."""
        with self._lock:
            self._is_available = available

    def set_failure_mode(self, should_fail: bool, message: str = "Simulated TTS synthesis failure") -> None:
        """Enable or disable simulated synthesis failure."""
        with self._lock:
            self._should_fail = should_fail
            self._failure_message = message

    def synthesize(
        self,
        text: str,
        language: str = "en",
        voice_id: Optional[str] = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> SynthesisResult:
        """Generate a deterministic synthetic audio SynthesisResult.

        Args:
            text: Text to synthesize. Must be non-empty string.
            language: Target language tag ('en', 'ta', 'tanglish', 'mixed').
            voice_id: Optional voice profile identifier.
            context: Optional contextual parameters.

        Returns:
            SynthesisResult: Synthetic audio bytes and calculated duration.

        Raises:
            VoiceProviderError: If failure mode is enabled or text is empty.
        """
        with self._lock:
            if not self._is_available or self._should_fail:
                logger.error("TTS provider failure simulated: %s", self._failure_message)
                raise VoiceProviderError(f"TTS provider error: {self._failure_message}")

        if not text or not isinstance(text, str) or not text.strip():
            raise VoiceProviderError("Cannot synthesize empty or whitespace-only text.")

        clean_text = text.strip()
        norm_lang = normalize_language(language)

        # Generate deterministic synthetic PCM audio header and body
        header = f"MOCK_AUDIO_PAYLOAD[format=pcm16,lang={norm_lang},voice={voice_id or 'default'}]:".encode("utf-8")
        body = clean_text.encode("utf-8")
        synthetic_bytes = header + body

        # Estimate duration deterministically (~120ms per word + base 100ms)
        words = clean_text.split()
        estimated_duration_ms = round(100.0 + (len(words) * 120.0), 2)

        meta = {
            "provider": self._name,
            "word_count": len(words),
            "voice_id": voice_id or "default",
        }
        if context:
            meta.update(copy.deepcopy(dict(context)))

        logger.debug(
            "Synthesized mock audio for text ('%s'...) [lang=%s, duration_ms=%s]",
            clean_text[:30],
            norm_lang,
            estimated_duration_ms,
        )

        return SynthesisResult(
            audio_bytes=synthetic_bytes,
            audio_format="mock_pcm",
            sample_rate=16000,
            duration_ms=estimated_duration_ms,
            text=clean_text,
            language=norm_lang,
            metadata=meta,
        )
