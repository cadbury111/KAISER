"""KAISER — Voice Interface Package.

Provides modular voice, audio, and Speech-to-Text foundation, including:
- Strongly-typed VoiceInput & VoiceOutput models
- Audio device discovery & management (AudioDeviceInfo, AudioDeviceManager)
- Bounded audio recording & models (AudioCaptureConfig, CapturedAudio, AudioCaptureService)
- Stateful VoiceSession management and lifecycle validation
- SpeechToTextProvider & TextToSpeechProvider abstract interfaces
- Deterministic MockSTTProvider, MockTTSProvider, & MockAudioCaptureProvider
- Local Whisper offline STT provider adapter (LocalWhisperSTTProvider)
- Audio preprocessing pipeline (validate_audio_payload, convert_to_whisper_pcm_float32)
- Safe VoiceService routing to MasterOrchestrator
"""

from app.voice.audio_capture import (
    AudioCaptureBackend,
    AudioCaptureHandle,
    AudioCaptureService,
    MockAudioCaptureBackend,
    SoundDeviceCaptureBackend,
)
from app.voice.audio_converter import (
    calculate_rms_amplitude,
    convert_to_whisper_pcm_float32,
    is_silence,
    validate_audio_payload,
)
from app.voice.audio_devices import (
    AudioDeviceBackend,
    AudioDeviceManager,
    MockAudioDeviceBackend,
    SoundDeviceBackend,
)
from app.voice.audio_models import (
    AudioBufferOverflowError,
    AudioCaptureConfig,
    AudioCaptureError,
    AudioCaptureStatus,
    AudioCaptureTimeoutError,
    AudioConfigurationError,
    AudioDeviceError,
    AudioDeviceInfo,
    AudioDeviceNotFoundError,
    AudioError,
    AudioPermissionError,
    AudioPlaybackCancelledError,
    AudioPlaybackConcurrencyError,
    AudioPlaybackDeviceNotFoundError,
    AudioPlaybackError,
    AudioPlaybackFormatError,
    AudioPlaybackResult,
    AudioPlaybackStatus,
    AudioPlaybackTimeoutError,
    CapturedAudio,
)
from app.voice.audio_playback import (
    AudioPlaybackBackend,
    AudioPlaybackService,
    MockAudioPlaybackBackend,
    SoundDevicePlaybackBackend,
)
from app.voice.interfaces import (
    SpeechToTextProvider,
    TextToSpeechProvider,
    VoiceProviderError,
)
from app.voice.models import (
    SynthesisResult,
    TranscriptionResult,
    VoiceInput,
    VoiceOutput,
    VoiceOutputStatus,
    VoiceSessionStatus,
    VoiceTurnResult,
    VoiceTurnStatus,
)
from app.voice.providers import (
    LocalWhisperSTTProvider,
    MockAudioCaptureProvider,
    MockSTTProvider,
    MockTTSProvider,
    Pyttsx3TTSProvider,
)
from app.voice.service import VoiceService
from app.voice.session import (
    VoiceSession,
    VoiceSessionError,
    VoiceSessionManager,
)
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

__all__ = [
    # Voice Models
    "VoiceInput",
    "VoiceOutput",
    "VoiceOutputStatus",
    "VoiceSessionStatus",
    "VoiceTurnStatus",
    "VoiceTurnResult",
    "TranscriptionResult",
    "SynthesisResult",
    # Audio Models & Configs
    "AudioCaptureStatus",
    "AudioPlaybackStatus",
    "AudioPlaybackResult",
    "AudioDeviceInfo",
    "AudioCaptureConfig",
    "CapturedAudio",
    # Audio Exceptions
    "AudioError",
    "AudioDeviceError",
    "AudioDeviceNotFoundError",
    "AudioCaptureError",
    "AudioConfigurationError",
    "AudioBufferOverflowError",
    "AudioCaptureTimeoutError",
    "AudioPermissionError",
    "AudioPlaybackError",
    "AudioPlaybackDeviceNotFoundError",
    "AudioPlaybackFormatError",
    "AudioPlaybackConcurrencyError",
    "AudioPlaybackTimeoutError",
    "AudioPlaybackCancelledError",
    # Audio Devices
    "AudioDeviceBackend",
    "SoundDeviceBackend",
    "MockAudioDeviceBackend",
    "AudioDeviceManager",
    # Audio Capture
    "AudioCaptureBackend",
    "SoundDeviceCaptureBackend",
    "MockAudioCaptureBackend",
    "AudioCaptureHandle",
    "AudioCaptureService",
    # Audio Playback
    "AudioPlaybackBackend",
    "SoundDevicePlaybackBackend",
    "MockAudioPlaybackBackend",
    "AudioPlaybackService",
    # Audio Preprocessing
    "validate_audio_payload",
    "convert_to_whisper_pcm_float32",
    "calculate_rms_amplitude",
    "is_silence",
    # STT Models & Config
    "LocalSTTConfig",
    "STTError",
    "STTDependencyError",
    "STTModelNotFoundError",
    "STTAudioValidationError",
    "STTAudioConversionError",
    "STTInferenceError",
    "STTTimeoutError",
    "STTEmptyTranscriptionError",
    # TTS Models & Config
    "Pyttsx3Config",
    "TTSError",
    "TTSDependencyError",
    "TTSVoiceNotFoundError",
    "TTSSynthesisError",
    "TTSTimeoutError",
    "TTSEmptyTextError",
    "TTSUnsupportedLanguageError",
    "TTSTextTooLongError",
    # Interfaces & Providers
    "SpeechToTextProvider",
    "TextToSpeechProvider",
    "VoiceProviderError",
    "LocalWhisperSTTProvider",
    "Pyttsx3TTSProvider",
    "MockAudioCaptureProvider",
    "MockSTTProvider",
    "MockTTSProvider",
    # Session & Service
    "VoiceSession",
    "VoiceSessionError",
    "VoiceSessionManager",
    "VoiceService",
]
