"""KAISER — Mock Speech-to-Text (STT) Provider.

Provides a deterministic, testable mock STT engine consuming predefined fixtures
without microphone hardware access or network calls.
"""

from __future__ import annotations

import copy
import threading
from typing import Any, Dict, Mapping, Optional, Union

from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.voice.interfaces import SpeechToTextProvider, VoiceProviderError
from app.voice.models import TranscriptionResult

logger = get_logger("voice.providers.mock_stt")


class MockSTTProvider(SpeechToTextProvider):
    """Deterministic Mock STT engine backed by predefined fixtures.

    Guarantees:
    1. Zero microphone capture or OS audio access.
    2. Zero external network calls.
    3. Deterministic test fixture resolution.
    4. Explicit error simulation capabilities for failure testing.
    """

    def __init__(
        self,
        name: str = "mock_stt",
        strict: bool = True,
        initial_fixtures: Optional[Dict[str, TranscriptionResult]] = None,
    ) -> None:
        self._name = name
        self._strict = strict
        self._lock = threading.RLock()
        self._is_available = True
        self._should_fail = False
        self._failure_message = "Simulated STT engine failure"
        self._fixtures: Dict[str, TranscriptionResult] = {}

        # Register default test fixtures
        self._init_default_fixtures()
        if initial_fixtures:
            self._fixtures.update(initial_fixtures)

    def _init_default_fixtures(self) -> None:
        """Populate foundational deterministic fixtures."""
        self._fixtures["fixture:system_status"] = TranscriptionResult(
            text="system_status",
            language="en",
            confidence=0.99,
            duration_seconds=1.2,
            metadata={"fixture_name": "system_status_en"},
        )
        self._fixtures["fixture:tamil_status"] = TranscriptionResult(
            text="system_status",
            language="ta",
            confidence=0.96,
            duration_seconds=1.8,
            metadata={"fixture_name": "system_status_ta", "raw_script": "கணினி நிலை என்ன"},
        )
        self._fixtures["fixture:tanglish_status"] = TranscriptionResult(
            text="system_status",
            language="tanglish",
            confidence=0.94,
            duration_seconds=1.5,
            metadata={"fixture_name": "system_status_tanglish", "raw_script": "system eppadi irukku"},
        )
        self._fixtures["fixture:mixed_status"] = TranscriptionResult(
            text="system_status",
            language="mixed",
            confidence=0.95,
            duration_seconds=1.4,
            metadata={"fixture_name": "system_status_mixed", "raw_script": "status என்ன"},
        )
        self._fixtures["fixture:unknown_cmd"] = TranscriptionResult(
            text="unregistered_voice_command",
            language="en",
            confidence=0.88,
            duration_seconds=1.0,
            metadata={"fixture_name": "unknown_cmd"},
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_available(self) -> bool:
        with self._lock:
            return self._is_available and not self._should_fail

    def set_availability(self, available: bool) -> None:
        """Set whether the mock STT provider is available."""
        with self._lock:
            self._is_available = available

    def set_failure_mode(self, should_fail: bool, message: str = "Simulated STT engine failure") -> None:
        """Enable or disable simulated transcription failure."""
        with self._lock:
            self._should_fail = should_fail
            self._failure_message = message

    def register_fixture(self, token: str, result: TranscriptionResult) -> None:
        """Register a custom audio token fixture."""
        with self._lock:
            self._fixtures[str(token).strip()] = result
            logger.debug("Registered STT fixture for token '%s'", token)

    def transcribe(
        self,
        audio_data: Any,
        language: Optional[str] = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> TranscriptionResult:
        """Transcribe simulated audio input into a TranscriptionResult.

        Args:
            audio_data: Fixture key string, byte buffer, or text token.
            language: Optional language override/hint.
            context: Optional contextual parameters.

        Returns:
            TranscriptionResult: Matching fixture result.

        Raises:
            VoiceProviderError: If failure mode is active or fixture is missing in strict mode.
        """
        with self._lock:
            if not self._is_available or self._should_fail:
                logger.error("STT provider failure simulated: %s", self._failure_message)
                raise VoiceProviderError(f"STT provider error: {self._failure_message}")

        if audio_data is None:
            raise VoiceProviderError("Audio data cannot be None.")

        # Resolve fixture token
        token: str
        if isinstance(audio_data, str):
            token = audio_data.strip()
        elif isinstance(audio_data, bytes):
            try:
                token = audio_data.decode("utf-8").strip()
            except UnicodeDecodeError:
                token = f"bytes:{len(audio_data)}"
        elif hasattr(audio_data, "metadata") and "fixture_token" in getattr(audio_data, "metadata", {}):
            token = str(audio_data.metadata["fixture_token"]).strip()
        elif hasattr(audio_data, "audio_bytes"):
            audio_key = f"audio:{len(audio_data.audio_bytes)}"
            with self._lock:
                has_key = audio_key in self._fixtures
            if has_key:
                token = audio_key
            else:
                try:
                    decoded = audio_data.audio_bytes.decode("utf-8").strip()
                    if any(ord(c) < 32 and c not in "\r\n\t" for c in decoded):
                        token = audio_key
                    else:
                        token = decoded
                except UnicodeDecodeError:
                    token = audio_key
        else:
            token = str(audio_data).strip()

        if not token:
            raise VoiceProviderError("Audio payload is empty: no speech detected.")

        with self._lock:
            if token in self._fixtures:
                res = self._fixtures[token]
                # If language hint was provided, apply it
                if language:
                    return TranscriptionResult(
                        text=res.text,
                        language=language,
                        confidence=res.confidence,
                        is_final=res.is_final,
                        duration_seconds=res.duration_seconds,
                        metadata=dict(res.metadata),
                    )
                return res

            if self._strict:
                err_msg = (
                    f"MockSTTProvider fixture not found for token '{token}'. "
                    f"Registered fixtures: {list(self._fixtures.keys())}"
                )
                logger.warning(err_msg)
                raise VoiceProviderError(err_msg)

            # Non-strict fallback: use token text directly
            logger.info("MockSTTProvider non-strict pass-through for token '%s'", token)
            return TranscriptionResult(
                text=token,
                language=language or "en",
                confidence=0.90,
                duration_seconds=1.0,
                metadata={"synthetic": True},
            )
