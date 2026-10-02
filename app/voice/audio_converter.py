"""KAISER — Audio Preprocessing & Validation Pipeline for STT.

Provides deterministic audio validation, PCM sample unpacking, channel downmixing,
and resampling for offline Speech-to-Text inference engines.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple

import numpy as np
import scipy.signal  # type: ignore

from app.core.logging_config import get_logger
from app.voice.audio_models import CapturedAudio
from app.voice.stt_models import STTAudioConversionError, STTAudioValidationError

logger = get_logger("voice.audio_converter")


def validate_audio_payload(
    audio: Any,
    max_buffer_bytes: int = 5 * 1024 * 1024,
    min_duration_seconds: float = 0.1,
    max_duration_seconds: float = 60.0,
) -> CapturedAudio:
    """Validate incoming audio payload against strict structural and memory bounds.

    Args:
        audio: Expected CapturedAudio instance or raw 16-bit PCM bytes.
        max_buffer_bytes: Upper memory boundary for audio payload.
        min_duration_seconds: Minimum acceptable recording duration.
        max_duration_seconds: Maximum acceptable recording duration.

    Returns:
        CapturedAudio: Validated captured audio container.

    Raises:
        STTAudioValidationError: If payload is missing, malformed, or exceeds bounds.
    """
    if audio is None:
        raise STTAudioValidationError("Audio data cannot be None.")

    # Wrap raw bytes in a default 16kHz mono container if provided
    if isinstance(audio, bytes):
        if len(audio) == 0:
            raise STTAudioValidationError("Audio buffer is empty (0 bytes).")
        if len(audio) > max_buffer_bytes:
            raise STTAudioValidationError(
                f"Audio payload size {len(audio)} bytes exceeds maximum buffer limit of {max_buffer_bytes} bytes."
            )
        duration = len(audio) / 32000.0  # 16000 Hz * 1 channel * 2 bytes/sample
        audio = CapturedAudio(
            audio_bytes=audio,
            sample_rate=16000,
            channels=1,
            sample_width_bytes=2,
            duration_seconds=duration,
        )

    if not isinstance(audio, CapturedAudio):
        raise STTAudioValidationError(
            f"Expected CapturedAudio instance or raw bytes, got '{type(audio).__name__}'."
        )

    # Validate byte count bounds
    if audio.byte_count == 0:
        raise STTAudioValidationError("Audio buffer is empty (0 bytes).")

    if audio.byte_count > max_buffer_bytes:
        raise STTAudioValidationError(
            f"Audio buffer size {audio.byte_count} bytes exceeds maximum buffer limit of {max_buffer_bytes} bytes."
        )

    # Validate duration bounds
    if audio.duration_seconds < min_duration_seconds:
        raise STTAudioValidationError(
            f"Audio duration {audio.duration_seconds:.2f}s is below minimum duration limit of {min_duration_seconds}s."
        )

    if audio.duration_seconds > max_duration_seconds:
        raise STTAudioValidationError(
            f"Audio duration {audio.duration_seconds:.2f}s exceeds maximum duration limit of {max_duration_seconds}s."
        )

    # Validate sample rate and channel bounds
    if not (8000 <= audio.sample_rate <= 96000):
        raise STTAudioValidationError(
            f"Audio sample rate {audio.sample_rate} Hz is outside supported range [8000, 96000]."
        )

    if not (1 <= audio.channels <= 8):
        raise STTAudioValidationError(
            f"Audio channel count {audio.channels} is outside supported range [1, 8]."
        )

    if audio.sample_width_bytes not in (1, 2, 4):
        raise STTAudioValidationError(
            f"Unsupported sample width {audio.sample_width_bytes} bytes (only 1, 2, 4 bytes supported)."
        )

    return audio


def calculate_rms_amplitude(samples: np.ndarray) -> float:
    """Calculate the Root-Mean-Square (RMS) amplitude of normalized audio samples.

    Args:
        samples: 1-D numpy float32 array in range [-1.0, 1.0].

    Returns:
        float: RMS amplitude in range [0.0, 1.0].
    """
    if len(samples) == 0:
        return 0.0
    return float(np.sqrt(np.mean(samples.astype(np.float64) ** 2)))


def is_silence(samples: np.ndarray, threshold_rms: float = 0.005) -> bool:
    """Check whether normalized audio samples represent silence or low-energy noise."""
    return calculate_rms_amplitude(samples) < threshold_rms


def convert_to_whisper_pcm_float32(
    captured: CapturedAudio,
    target_sample_rate: int = 16000,
    silence_threshold_rms: float = 0.005,
) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Convert CapturedAudio into a normalized 1-D float32 numpy array suitable for Whisper.

    Conversion steps:
    1. Unpack binary PCM bytes to signed float32 array normalized to [-1.0, 1.0].
       - 16-bit PCM (int16): divided by 32768.0
       - 8-bit unsigned PCM (uint8): centered by -128.0 and divided by 128.0
       - 32-bit PCM (int32): divided by 2147483648.0
    2. Multi-channel downmixing:
       - Reshape array across channels and average across columns to produce mono.
    3. Resampling:
       - If sample_rate != target_sample_rate (default 16000 Hz), resample using
         scipy.signal.resample to preserve waveform frequency components.
    4. Clipping & Silence analysis:
       - Clamp values to [-1.0, 1.0].
       - Calculate RMS amplitude to detect pure silence/flatline audio.

    Args:
        captured: Validated CapturedAudio container.
        target_sample_rate: Target sample rate expected by the model (default 16000 Hz).
        silence_threshold_rms: Threshold below which audio is flagged as silence.

    Returns:
        Tuple[np.ndarray, Dict[str, Any]]:
            - Normalized 1-D float32 array sampled at target_sample_rate.
            - Conversion diagnostic metadata dictionary.

    Raises:
        STTAudioConversionError: If conversion, downmixing, or resampling fails.
    """
    try:
        raw_bytes = captured.audio_bytes
        sample_width = captured.sample_width_bytes
        channels = captured.channels
        sample_rate = captured.sample_rate

        # 1. Unpack to float32
        if sample_width == 2:
            # 16-bit signed PCM
            samples = np.frombuffer(raw_bytes, dtype=np.int16).astype(np.float32) / 32768.0
        elif sample_width == 1:
            # 8-bit unsigned PCM
            samples = (np.frombuffer(raw_bytes, dtype=np.uint8).astype(np.float32) - 128.0) / 128.0
        elif sample_width == 4:
            # 32-bit signed PCM
            samples = np.frombuffer(raw_bytes, dtype=np.int32).astype(np.float32) / 2147483648.0
        else:
            raise STTAudioConversionError(f"Unsupported sample width {sample_width} bytes.")

        if len(samples) == 0:
            raise STTAudioConversionError("Unpacked sample buffer is empty.")

        # 2. Multi-channel downmixing
        downmixed = False
        if channels > 1:
            # Truncate any incomplete trailing multi-channel frame
            remainder = len(samples) % channels
            if remainder != 0:
                samples = samples[:-remainder]
            samples = samples.reshape(-1, channels).mean(axis=1)
            downmixed = True

        # 3. Resampling to target rate (16 kHz for Whisper)
        resampled = False
        if sample_rate != target_sample_rate:
            target_num_samples = int(round(len(samples) * target_sample_rate / sample_rate))
            if target_num_samples > 0:
                samples = scipy.signal.resample(samples, target_num_samples).astype(np.float32)
                resampled = True

        # 4. Normalization clamp & RMS computation
        samples = np.clip(samples, -1.0, 1.0)
        rms = calculate_rms_amplitude(samples)
        is_silent = rms < silence_threshold_rms

        metadata = {
            "original_sample_rate": sample_rate,
            "target_sample_rate": target_sample_rate,
            "original_channels": channels,
            "downmixed": downmixed,
            "resampled": resampled,
            "total_samples": len(samples),
            "duration_seconds": float(len(samples)) / float(target_sample_rate),
            "rms_amplitude": round(rms, 6),
            "is_silence": is_silent,
        }

        return samples, metadata

    except Exception as exc:
        if isinstance(exc, (STTAudioConversionError, STTAudioValidationError)):
            raise
        logger.error("Audio conversion failed: %s", exc)
        raise STTAudioConversionError(f"Audio conversion failed: {exc}") from exc
