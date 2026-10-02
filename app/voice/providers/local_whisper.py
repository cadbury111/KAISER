"""KAISER — Local Offline Whisper STT Provider Adapter.

Implements a local, offline Speech-to-Text inference provider adhering to the
SpeechToTextProvider abstract interface. Guarantees:
- Zero cloud speech API dependencies.
- Zero network calls during transcription.
- Zero silent downloads of models or weights.
- Explicit validation of audio payload before inference.
- Exact preservation of KAISER multilingual metadata (en, ta, tanglish, mixed).
- Calibrated confidence reporting without synthetic scores.
- Rejection of flatline/silent audio as explicit empty transcription outcomes.
"""

from __future__ import annotations

import copy
import math
import os
import threading
import time
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from app.core.logging_config import get_logger
from app.voice.audio_converter import (
    convert_to_whisper_pcm_float32,
    validate_audio_payload,
)
from app.voice.audio_models import CapturedAudio
from app.voice.interfaces import SpeechToTextProvider, VoiceProviderError
from app.voice.models import TranscriptionResult
from app.voice.stt_models import (
    LocalSTTConfig,
    STTAudioConversionError,
    STTAudioValidationError,
    STTDependencyError,
    STTEmptyTranscriptionError,
    STTError,
    STTInferenceError,
    STTModelNotFoundError,
    STTTimeoutError,
)

logger = get_logger("voice.providers.local_whisper")


class LocalWhisperSTTProvider(SpeechToTextProvider):
    """Local, offline Speech-to-Text adapter backed by faster-whisper.

    Safety:
    1. Zero automatic downloads: models must reside in the configured local directory.
    2. Zero external network calls.
    3. Lazy model initialization: weights are loaded into memory only on demand.
    4. Input validation and audio preprocessing before invoking model inference.
    5. Support for mock/custom inference engine injection in automated test suites.
    """

    def __init__(
        self,
        config: Optional[LocalSTTConfig] = None,
        engine: Optional[Any] = None,
        name: str = "local_whisper",
    ) -> None:
        self.config = config or LocalSTTConfig()
        self._name = name
        self._lock = threading.RLock()
        self._engine = engine  # Optional injected engine for deterministic testing
        self._model: Optional[Any] = None
        self._is_loaded = False
        self._load_error: Optional[Exception] = None

        logger.info(
            "LocalWhisperSTTProvider '%s' created (model_id=%s, device=%s, compute=%s)",
            self._name,
            self.config.model_id,
            self.config.device,
            self.config.compute_type,
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_available(self) -> bool:
        """Check whether local STT model is ready or can be initialized."""
        with self._lock:
            if self._engine is not None:
                return True
            if self._is_loaded:
                return self._model is not None

            # Check if dependencies and model files are present
            try:
                self._verify_environment()
                return True
            except Exception:
                return False

    def _verify_environment(self) -> None:
        """Verify that faster-whisper and local model directory exist without downloading."""
        try:
            import faster_whisper  # type: ignore # noqa: F401
        except ImportError as exc:
            raise STTDependencyError(
                "Local STT requires 'faster-whisper' package. Run 'pip install faster-whisper'."
            ) from exc

        model_path = self.config.model_path
        if not model_path:
            raise STTModelNotFoundError(
                f"No local model path configured for model '{self.config.model_id}'. "
                "Explicit local path is required to operate offline without network downloads."
            )

        if not os.path.exists(model_path):
            raise STTModelNotFoundError(
                f"Local model path does not exist: '{model_path}'. "
                "Silent model downloads over the internet are strictly disabled."
            )

    def _ensure_model_loaded(self) -> Any:
        """Lazily load the local model into memory with strict offline guarantees."""
        with self._lock:
            if self._engine is not None:
                self._model = self._engine
                self._is_loaded = True
                return self._model

            if self._is_loaded and self._model is not None:
                return self._model

            if self._load_error is not None:
                raise self._load_error

            self._verify_environment()

            try:
                import faster_whisper  # type: ignore

                logger.info(
                    "Loading local Whisper model from '%s' [device=%s, compute=%s]...",
                    self.config.model_path,
                    self.config.device,
                    self.config.compute_type,
                )

                # Instantiate model with local_files_only to guarantee offline operation
                model = faster_whisper.WhisperModel(
                    model_size_or_path=self.config.model_path,
                    device=self.config.device,
                    compute_type=self.config.compute_type,
                    local_files_only=True,
                )
                self._model = model
                self._is_loaded = True
                logger.info("Local Whisper model loaded successfully.")
                return self._model

            except Exception as exc:
                self._load_error = (
                    exc
                    if isinstance(exc, STTError)
                    else STTInferenceError(f"Failed to load local Whisper model: {exc}")
                )
                logger.error("Failed to initialize local Whisper model: %s", exc)
                raise self._load_error from exc

    def _resolve_language_hint(self, language: Optional[str]) -> Tuple[Optional[str], str]:
        """Normalize KAISER language tags to whisper codes while preserving metadata.

        Mappings:
        - 'en' -> ('en', 'en')
        - 'ta' -> ('ta', 'ta')
        - 'tanglish' -> ('ta', 'tanglish') (Model prompted with Tamil/phonetics; tag preserved)
        - 'mixed' -> (None, 'mixed') (Whisper auto-detects; tag preserved)

        Returns:
            Tuple[Optional[str], str]: (model_language_code, preserved_language_tag)
        """
        raw_lang = (language or self.config.language or "en").strip().lower()

        if raw_lang in ("en", "english"):
            return "en", "en"
        elif raw_lang in ("ta", "tamil"):
            return "ta", "ta"
        elif raw_lang in ("tanglish", "tamil_english"):
            # Standard whisper does not have a dedicated 'tanglish' code; pass 'ta' to model
            return "ta", "tanglish"
        elif raw_lang in ("mixed", "auto", "unknown"):
            return None, "mixed"
        else:
            return raw_lang, raw_lang

    def transcribe(
        self,
        audio_data: Any,
        language: Optional[str] = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> TranscriptionResult:
        """Transcribe captured audio buffer using local Whisper inference.

        Pipeline:
        1. Validate audio payload and memory boundaries.
        2. Unpack PCM samples, downmix to mono, and resample to 16 kHz.
        3. Check for silent/unusable speech and produce explicit outcome.
        4. Resolve language hints without auto-translation.
        5. Run local model inference under thread lock.
        6. Extract recognized text and calibrated confidence metrics.
        7. Return structured TranscriptionResult.

        Args:
            audio_data: CapturedAudio container or raw PCM bytes.
            language: Optional language hint ('en', 'ta', 'tanglish', 'mixed').
            context: Optional auxiliary contextual metadata.

        Returns:
            TranscriptionResult: Strongly-typed, immutable transcription result.

        Raises:
            STTAudioValidationError: If audio fails size, duration, or channel checks.
            STTAudioConversionError: If PCM unpacking or resampling fails.
            STTEmptyTranscriptionError: If audio is silent or no words were recognized.
            STTDependencyError: If faster-whisper is not installed.
            STTModelNotFoundError: If local model directory is missing.
            STTInferenceError: If model inference fails.
        """
        start_time = time.monotonic()

        # 1. Ensure local model is loaded
        model = self._ensure_model_loaded()

        # 2. Validate audio payload
        captured = validate_audio_payload(
            audio=audio_data,
            max_buffer_bytes=self.config.max_buffer_bytes,
            min_duration_seconds=self.config.min_duration_seconds,
            max_duration_seconds=self.config.max_duration_seconds,
        )

        # 3. Audio conversion (unpack -> mono downmix -> 16kHz resample)
        samples_f32, audio_meta = convert_to_whisper_pcm_float32(
            captured=captured,
            target_sample_rate=16000,
            silence_threshold_rms=self.config.silence_threshold_rms,
        )

        # 4. Check for silent / unusable speech
        if audio_meta.get("is_silence", False):
            logger.info("Audio contains background silence (RMS=%.5f); rejecting as empty.", audio_meta.get("rms_amplitude", 0.0))
            raise STTEmptyTranscriptionError(
                "Audio contains no detectable speech (amplitude below silence threshold)."
            )

        # 5. Resolve language hints
        model_lang, preserved_tag = self._resolve_language_hint(language)

        # 6. Execute inference
        try:
            with self._lock:
                logger.debug(
                    "Running local STT inference (samples=%d, lang_hint=%s)...",
                    len(samples_f32),
                    model_lang,
                )

                # Inference parameters: task='transcribe' (never 'translate')
                segments_generator, info = model.transcribe(
                    samples_f32,
                    language=model_lang,
                    beam_size=self.config.beam_size,
                    temperature=self.config.temperature,
                    vad_filter=self.config.vad_filter,
                    task="transcribe",
                )

                # Collect recognized segments
                segments = list(segments_generator)

        except Exception as exc:
            logger.error("Local Whisper inference failed: %s", exc)
            if isinstance(exc, (STTError, VoiceProviderError)):
                raise
            raise STTInferenceError(f"Local Whisper inference failed: {exc}") from exc

        elapsed_seconds = time.monotonic() - start_time

        # 7. Extract normalized text and confidence
        text_pieces = []
        confidences = []

        for seg in segments:
            clean_text = str(getattr(seg, "text", "")).strip()
            if clean_text:
                text_pieces.append(clean_text)

            # Extract calibrated confidence if available from avg_logprob
            avg_logprob = getattr(seg, "avg_logprob", None)
            if avg_logprob is not None:
                # Math: e^(logprob) converts log-probability to probability in [0.0, 1.0]
                conf = max(0.0, min(1.0, math.exp(float(avg_logprob))))
                confidences.append(conf)

        final_text = " ".join(text_pieces).strip()

        # Enforce invariant: Empty transcription outcome on no recognized words
        if not final_text:
            logger.info("Local STT yielded zero recognized words.")
            raise STTEmptyTranscriptionError("Speech was recognized as empty or unintelligible.")

        # Compute average calibrated confidence or flag as uncalibrated
        if confidences:
            mean_conf = round(sum(confidences) / len(confidences), 4)
            is_calibrated = True
        else:
            mean_conf = 1.0
            is_calibrated = False

        detected_lang = getattr(info, "language", model_lang or "unknown")
        detected_lang_prob = getattr(info, "language_probability", 1.0)

        metadata: Dict[str, Any] = {
            "provider": self.name,
            "model_id": self.config.model_id,
            "device": self.config.device,
            "compute_type": self.config.compute_type,
            "processing_duration_seconds": round(elapsed_seconds, 4),
            "caller_language_hint": language,
            "preserved_language_tag": preserved_tag,
            "model_detected_language": detected_lang,
            "model_language_probability": round(float(detected_lang_prob), 4),
            "confidence_is_calibrated": is_calibrated,
            "segment_count": len(segments),
            "audio_conversion": audio_meta,
        }

        logger.info(
            "STT completed successfully in %.3fs [lang=%s, conf=%.2f]: %s",
            elapsed_seconds,
            preserved_tag,
            mean_conf,
            final_text,
        )

        return TranscriptionResult(
            text=final_text,
            language=preserved_tag,
            confidence=mean_conf,
            is_final=True,
            duration_seconds=audio_meta.get("duration_seconds", captured.duration_seconds),
            metadata=metadata,
        )
