"""KAISER — Local Offline pyttsx3 TTS Provider Adapter.

Implements a local, offline Text-to-Speech synthesis provider adhering to the
TextToSpeechProvider abstract interface. Guarantees:
- Zero cloud speech API dependencies.
- Zero network calls during speech synthesis.
- Zero silent downloads of models, voices, or weights.
- Lazy engine initialization: speech engines are initialized only on demand.
- Input validation: rejects empty text and bounded excessive payloads.
- Language preservation: honest handling of English, Tamil, Tanglish, and Mixed.
- Output formatting: returns standard SynthesisResult with in-memory WAV audio.
- Resource cleanup: temporary audio files are strictly unlinked immediately.
- Thread safety: synchronized engine access with COM runtime initialization.
"""

from __future__ import annotations

import copy
import io
import os
import platform
import tempfile
import threading
import time
import uuid
import wave
from typing import Any, Dict, List, Mapping, Optional

from app.core.logging_config import get_logger
from app.orchestrator.models import normalize_language
from app.voice.interfaces import TextToSpeechProvider
from app.voice.models import SynthesisResult
from app.voice.tts_models import (
    Pyttsx3Config,
    TTSDependencyError,
    TTSEmptyTextError,
    TTSError,
    TTSSynthesisError,
    TTSTextTooLongError,
    TTSTimeoutError,
    TTSUnsupportedLanguageError,
    TTSVoiceNotFoundError,
)

logger = get_logger("voice.providers.local_pyttsx3")


class Pyttsx3TTSProvider(TextToSpeechProvider):
    """Local, offline Text-to-Speech provider adapter backed by pyttsx3 / Windows SAPI5.

    Safety & Operational Invariants:
    1. Zero external network calls: synthesis executes entirely through local system drivers.
    2. Lazy initialization: speech driver is not initialized until explicitly queried or invoked.
    3. Engine injection: supports mock engine injection for deterministic, headless unit tests.
    4. Immediate cleanup: synthesized temporary audio files on disk are unlinked in finally blocks.
    """

    def __init__(
        self,
        config: Optional[Pyttsx3Config] = None,
        engine: Optional[Any] = None,
        name: str = "pyttsx3",
    ) -> None:
        self.config = config or Pyttsx3Config()
        self._name = name
        self._lock = threading.RLock()
        self._engine = engine  # Optional injected engine for deterministic unit testing
        self._is_initialized = engine is not None
        self._cached_voices: Optional[List[Dict[str, Any]]] = None

        logger.info(
            "Pyttsx3TTSProvider '%s' created (rate=%d, volume=%.2f, voice_id=%s)",
            self._name,
            self.config.rate,
            self.config.volume,
            self.config.voice_id or "default",
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_available(self) -> bool:
        """Check whether local TTS engine is functional and has installed voices."""
        with self._lock:
            if self._engine is not None:
                return True
            try:
                engine = self._get_engine()
                voices = self.list_voices()
                return len(voices) > 0
            except Exception as exc:
                logger.debug("pyttsx3 availability check failed: %s", exc)
                return False

    def _init_com(self) -> None:
        """Initialize Windows COM apartment if running on Windows."""
        if platform.system() == "Windows":
            try:
                import pythoncom  # type: ignore

                pythoncom.CoInitialize()
            except Exception:
                pass

    def _get_engine(self) -> Any:
        """Lazily initialize and return the underlying pyttsx3 engine."""
        with self._lock:
            if self._engine is not None:
                return self._engine

            self._init_com()
            try:
                import pyttsx3  # type: ignore

                engine = pyttsx3.init()
                self._engine = engine
                self._is_initialized = True
                logger.debug("Initialized pyttsx3 engine successfully.")
                return self._engine
            except ImportError as exc:
                raise TTSDependencyError(
                    "pyttsx3 library is not installed. Install via 'pip install pyttsx3'."
                ) from exc
            except Exception as exc:
                raise TTSDependencyError(
                    f"Failed to initialize pyttsx3 audio engine: {exc}"
                ) from exc

    def list_voices(self) -> List[Dict[str, Any]]:
        """Return descriptors for all voices discovered on the local system."""
        with self._lock:
            if self._cached_voices is not None:
                return list(self._cached_voices)

            try:
                engine = self._get_engine()
                raw_voices = engine.getProperty("voices") or []
                voice_list: List[Dict[str, Any]] = []

                for v in raw_voices:
                    v_id = str(getattr(v, "id", "")).strip()
                    v_name = str(getattr(v, "name", "")).strip() or v_id
                    v_langs = getattr(v, "languages", [])
                    if isinstance(v_langs, (list, tuple)):
                        languages = [str(lang) for lang in v_langs]
                    else:
                        languages = [str(v_langs)] if v_langs else []

                    voice_list.append(
                        {
                            "id": v_id,
                            "name": v_name,
                            "languages": languages,
                            "gender": getattr(v, "gender", "unknown"),
                            "age": getattr(v, "age", "unknown"),
                        }
                    )

                self._cached_voices = voice_list
                return list(voice_list)
            except Exception as exc:
                logger.error("Failed to query pyttsx3 voices: %s", exc)
                return []

    def get_voice_by_id(self, voice_id: str) -> Optional[Dict[str, Any]]:
        """Find an installed voice matching the given identifier."""
        clean_id = voice_id.strip()
        for v in self.list_voices():
            if v["id"] == clean_id or clean_id.lower() in v["name"].lower():
                return v
        return None

    def has_language_voice(self, language: str) -> bool:
        """Check whether at least one installed voice supports the language tag."""
        norm_lang = normalize_language(language)
        if norm_lang == "en":
            for v in self.list_voices():
                name_lower = v["name"].lower()
                langs = [l.lower() for l in v["languages"]]
                if "english" in name_lower or "en" in langs or "en-us" in langs or "en-gb" in langs:
                    return True
            return len(self.list_voices()) > 0

        if norm_lang == "ta":
            for v in self.list_voices():
                name_lower = v["name"].lower()
                langs = [l.lower() for l in v["languages"]]
                if "tamil" in name_lower or "ta" in langs or "ta-in" in langs:
                    return True
            return False

        if norm_lang in ("tanglish", "mixed"):
            return self.has_language_voice("en")

        return False

    def synthesize(
        self,
        text: str,
        language: str = "en",
        voice_id: Optional[str] = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> SynthesisResult:
        """Synthesize text into a structured audio SynthesisResult.

        Args:
            text: Text to synthesize. Must be non-empty and within bounds.
            language: Target language tag ('en', 'ta', 'tanglish', 'mixed').
            voice_id: Optional installed voice identifier to use.
            context: Optional contextual parameters (e.g. rate, volume overrides).

        Returns:
            SynthesisResult: Validated binary audio payload and metadata.

        Raises:
            TTSEmptyTextError: If text is empty or whitespace-only.
            TTSTextTooLongError: If text exceeds max_text_length limit.
            TTSUnsupportedLanguageError: If requested language has no compatible voice.
            TTSVoiceNotFoundError: If explicit voice_id is not found on system.
            TTSSynthesisError: If engine crashes or fails during audio rendering.
        """
        if not text or not isinstance(text, str) or not text.strip():
            raise TTSEmptyTextError("Cannot synthesize empty or whitespace-only text.")

        clean_text = text.strip()
        if len(clean_text) > self.config.max_text_length:
            raise TTSTextTooLongError(
                f"Input text exceeds maximum allowed length "
                f"({len(clean_text)} > {self.config.max_text_length} characters)."
            )

        norm_lang = normalize_language(language)

        # Honest language verification: check for native voice availability
        if norm_lang == "ta" and not self.has_language_voice("ta"):
            raise TTSUnsupportedLanguageError(
                f"Requested language '{norm_lang}' is not supported by installed local voices: "
                "no Tamil speech synthesis voice was found on this system."
            )

        with self._lock:
            self._init_com()
            engine = self._get_engine()

            # Resolve voice ID
            target_voice_id = voice_id or self.config.voice_id
            resolved_voice_meta: Optional[Dict[str, Any]] = None
            if target_voice_id:
                resolved_voice_meta = self.get_voice_by_id(target_voice_id)
                if resolved_voice_meta is None:
                    raise TTSVoiceNotFoundError(
                        f"Requested voice_id '{target_voice_id}' was not found among installed voices. "
                        f"Available voices: {[v['id'] for v in self.list_voices()]}"
                    )
                target_voice_id = resolved_voice_meta["id"]

            # Resolve rate and volume
            ctx_rate = context.get("rate") if context else None
            rate = int(ctx_rate) if ctx_rate is not None and isinstance(ctx_rate, (int, float)) else self.config.rate

            ctx_volume = context.get("volume") if context else None
            volume = float(ctx_volume) if ctx_volume is not None and isinstance(ctx_volume, (int, float)) else self.config.volume

            # Synthesize to temporary WAV file (safely cleaned up in finally block)
            temp_filename = f"kaiser_tts_{uuid.uuid4().hex[:12]}.wav"
            temp_dir = tempfile.gettempdir()
            temp_path = os.path.join(temp_dir, temp_filename)

            start_time = time.monotonic()
            try:
                engine.setProperty("rate", rate)
                engine.setProperty("volume", volume)
                if target_voice_id:
                    engine.setProperty("voice", target_voice_id)

                engine.save_to_file(clean_text, temp_path)
                engine.runAndWait()

                if not os.path.isfile(temp_path) or os.path.getsize(temp_path) == 0:
                    raise TTSSynthesisError("pyttsx3 synthesis produced an empty or missing audio file.")

                with open(temp_path, "rb") as f:
                    audio_bytes = f.read()

                # Parse WAV headers to obtain exact audio metadata
                with wave.open(io.BytesIO(audio_bytes), "rb") as wf:
                    channels = wf.getnchannels()
                    sample_rate = wf.getframerate()
                    sample_width = wf.getsampwidth()
                    nframes = wf.getnframes()
                    duration_ms = round((nframes / float(sample_rate)) * 1000.0, 2)

            except TTSError:
                raise
            except Exception as exc:
                raise TTSSynthesisError(f"pyttsx3 synthesis execution failed: {exc}") from exc
            finally:
                # Guaranteed cleanup: Zero persistent audio files on disk
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass

            elapsed_ms = round((time.monotonic() - start_time) * 1000.0, 2)

            # Metadata reporting
            pronunciation_note = None
            engine_lang = "en"
            if norm_lang == "tanglish":
                engine_lang = "en"
                pronunciation_note = (
                    "Phonetic romanized Tanglish pronounced using English acoustic model; "
                    "native prosody not guaranteed."
                )
            elif norm_lang == "mixed":
                engine_lang = "en"
                pronunciation_note = "Code-switched text synthesized with English voice."
            elif norm_lang == "ta":
                engine_lang = "ta"

            meta: Dict[str, Any] = {
                "provider": self._name,
                "engine_language": engine_lang,
                "voice_id": target_voice_id or getattr(engine.getProperty("voice"), "id", "default"),
                "rate": rate,
                "volume": volume,
                "channels": channels,
                "sample_width_bytes": sample_width,
                "synthesis_duration_ms": elapsed_ms,
                "offline": True,
            }
            if pronunciation_note:
                meta["pronunciation_note"] = pronunciation_note
            if context:
                meta.update(copy.deepcopy(dict(context)))

            logger.debug(
                "Synthesized text ('%s'...) [format=wav, rate=%dHz, dur=%.1fms, elapsed=%.1fms]",
                clean_text[:40],
                sample_rate,
                duration_ms,
                elapsed_ms,
            )

            return SynthesisResult(
                audio_bytes=audio_bytes,
                audio_format="wav",
                sample_rate=sample_rate,
                duration_ms=duration_ms,
                text=clean_text,
                language=norm_lang,
                metadata=meta,
            )
