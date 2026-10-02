"""KAISER — Voice Service.

Coordinates voice sessions, speech-to-text transcription, Master Orchestrator
command routing, text-to-speech synthesis, and lifecycle event dispatch.
"""

from __future__ import annotations

import copy
import threading
import time
import uuid
from typing import Any, Dict, Mapping, Optional, Union

from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ServiceError, ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.orchestrator import MasterOrchestrator, OrchestratorRequest, OrchestratorResponse, RequestStatus
from app.orchestrator.lifecycle import mask_sensitive_text, sanitize_container
from app.voice.audio_capture import AudioCaptureService
from app.voice.audio_models import (
    AudioBufferOverflowError,
    AudioCaptureConfig,
    AudioCaptureError,
    AudioCaptureTimeoutError,
    AudioConfigurationError,
    AudioDeviceNotFoundError,
    AudioPermissionError,
    AudioPlaybackError,
    AudioPlaybackResult,
    AudioPlaybackStatus,
    CapturedAudio,
)
from app.voice.audio_playback import AudioPlaybackService
from app.voice.interfaces import SpeechToTextProvider, TextToSpeechProvider, VoiceProviderError
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
from app.voice.stt_models import (
    STTAudioConversionError,
    STTAudioValidationError,
    STTDependencyError,
    STTEmptyTranscriptionError,
    STTError,
    STTInferenceError,
    STTModelNotFoundError,
    STTTimeoutError,
)
from app.voice.session import VoiceSession, VoiceSessionError, VoiceSessionManager
from app.voice.tts_models import TTSError, TTSUnsupportedLanguageError

logger = get_logger("voice.service")


class VoiceService:
    """Safe, decoupled coordinator for voice interactions in KAISER.

    Enforces:
    1. Zero live trading execution (routes only to registered orchestrator handlers).
    2. Zero external hardware or network dependencies (modular provider interfaces).
    3. Strict session state transition validation.
    4. Bounded operational telemetry in StateStore without unbounded audio/text history.
    5. Subscriber failure isolation across Event Bus dispatches.
    6. Audio capture isolation (explicit capture returning typed CapturedAudio).
    """

    def __init__(
        self,
        orchestrator: MasterOrchestrator,
        stt_provider: Optional[SpeechToTextProvider] = None,
        tts_provider: Optional[TextToSpeechProvider] = None,
        audio_capture: Optional[AudioCaptureService] = None,
        audio_playback: Optional[AudioPlaybackService] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        session_manager: Optional[VoiceSessionManager] = None,
        name: str = "voice_service",
    ) -> None:
        if orchestrator is None:
            raise ValidationError("MasterOrchestrator instance is required for VoiceService.")

        self.name = name
        self.orchestrator = orchestrator
        self.stt_provider = stt_provider
        self.tts_provider = tts_provider
        self.audio_playback = audio_playback
        self.event_bus = event_bus
        self.state_store = state_store
        self.session_manager = session_manager or VoiceSessionManager(max_sessions=100)

        # Initialize audio capture layer (modular and isolated)
        self.audio_capture = audio_capture or AudioCaptureService(
            event_bus=event_bus,
            state_store=None,
        )

        self._lock = threading.RLock()
        self._turn_lock = threading.Lock()
        self._last_stt_duration: float = 0.0
        self._last_tts_duration: float = 0.0
        self._last_turn_summary: Optional[Dict[str, Any]] = None
        self._counters: Dict[str, int] = {
            "total_inputs": 0,
            "processed": 0,
            "failed": 0,
            "synthesized": 0,
            "played": 0,
            "turns_total": 0,
            "turns_completed": 0,
            "turns_failed": 0,
            "turns_cancelled": 0,
        }

        self._sync_state_store()

        logger.info(
            "VoiceService '%s' initialized (stt=%s, tts=%s, audio_cap=%s, playback=%s, event_bus=%s, state_store=%s)",
            self.name,
            getattr(stt_provider, "name", "none"),
            getattr(tts_provider, "name", "none"),
            self.audio_capture.backend.name if self.audio_capture else "none",
            self.audio_playback.backend.name if self.audio_playback else "none",
            "connected" if event_bus else "none",
            "connected" if state_store else "none",
        )

    # --------------------------------------------------------------------------
    # Audio Capture Delegates
    # --------------------------------------------------------------------------

    def capture_audio(
        self,
        duration_seconds: Optional[float] = None,
        device_id: Optional[int] = None,
        config: Optional[AudioCaptureConfig] = None,
        correlation_id: Optional[str] = None,
    ) -> CapturedAudio:
        """Capture bounded audio from the configured audio input service.

        Safety:
        - Initiated ONLY upon explicit request.
        - Returns a typed CapturedAudio result for future STT consumption.
        - Does NOT pass raw audio directly to MasterOrchestrator.
        - Does NOT fabricate transcription text.
        """
        if self.audio_capture is None:
            raise ServiceError("Audio capture service is not configured on VoiceService.")
        return self.audio_capture.capture_audio(
            duration_seconds=duration_seconds,
            device_id=device_id,
            config=config,
            correlation_id=correlation_id,
        )
    # Session Management Delegates
    # --------------------------------------------------------------------------

    def create_session(
        self,
        session_id: Optional[str] = None,
        language: str = "en",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceSession:
        """Create and track a new VoiceSession."""
        session = self.session_manager.create_session(
            session_id=session_id,
            language=language,
            metadata=metadata,
        )
        self._publish_event(
            EventType.VOICE_SESSION_UPDATED,
            correlation_id=session.session_id,
            payload={
                "session_id": session.session_id,
                "status": session.status.value,
                "language": session.language,
                "action": "created",
            },
        )
        self._sync_state_store(active_session=session)
        return session

    def get_session(self, session_id: str) -> Optional[VoiceSession]:
        """Retrieve an existing session by ID."""
        return self.session_manager.get_session(session_id)

    def close_session(self, session_id: str, reason: str = "User request") -> bool:
        """Close an active voice session."""
        session = self.session_manager.get_session(session_id)
        if session and not session.is_closed:
            closed = self.session_manager.close_session(session_id, reason=reason)
            if closed:
                self._publish_event(
                    EventType.VOICE_SESSION_UPDATED,
                    correlation_id=session_id,
                    payload={
                        "session_id": session_id,
                        "status": VoiceSessionStatus.CLOSED.value,
                        "reason": reason,
                        "action": "closed",
                    },
                )
                self._sync_state_store()
                return True
        return False

    # --------------------------------------------------------------------------
    # Audio & Voice Processing Pipeline
    # --------------------------------------------------------------------------

    def execute_voice_turn(
        self,
        duration_seconds: Optional[float] = None,
        audio_data: Optional[Any] = None,
        session_id: Optional[str] = None,
        language: Optional[str] = None,
        device_id: Optional[int] = None,
        output_device_id: Optional[int] = None,
        config: Optional[AudioCaptureConfig] = None,
        synthesize_output: bool = False,
        play_output: bool = False,
        correlation_id: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> VoiceTurnResult:
        """Execute a safe, single-turn voice command pipeline.

        Pipeline Stages:
        1. Validate request, session, and capture configuration.
        2. Enforce non-overlapping turn protection (rejects concurrent turns).
        3. Acquire bounded audio: explicitly capture from hardware or use provided payload.
        4. Validate transcription provider availability (checks dependencies/weights).
        5. Execute STT inference (preserves multilingual metadata, handles empty/silence).
        6. Validate transcription result and normalize surrounding whitespace.
        7. Route recognized speech text to MasterOrchestrator and registered handlers.
        8. Validate orchestrator response and generate VoiceOutput.
        9. Publish lifecycle events, record bounded telemetry, and clean up all resources.

        Args:
            duration_seconds: Optional capture duration in seconds (0.1 to 60.0).
            audio_data: Optional pre-captured audio payload (CapturedAudio or bytes) for test isolation.
            session_id: Optional existing session ID (creates a new session if omitted).
            language: Optional expected language hint ('en', 'ta', 'tanglish', 'mixed').
            device_id: Optional microphone device index.
            config: Optional custom AudioCaptureConfig.
            synthesize_output: Whether to synthesize response via TTS provider.
            correlation_id: Optional distributed tracing correlation identifier.
            stop_event: Optional threading.Event to signal cancellation.

        Returns:
            VoiceTurnResult: Immutable, typed result of the voice turn execution.
        """
        resolved_corr_id = (correlation_id or uuid.uuid4().hex).strip()
        turn_id = f"turn_{uuid.uuid4().hex[:12]}"

        # Concurrency safety: Prevent overlapping microphone captures / voice turns
        if not self._turn_lock.acquire(blocking=False):
            err_msg = "Another voice turn is currently in progress. Overlapping turns are not permitted."
            logger.warning(err_msg)
            self._publish_event(
                EventType.VOICE_TURN_REJECTED,
                correlation_id=resolved_corr_id,
                payload={
                    "turn_id": turn_id,
                    "session_id": session_id or "unknown",
                    "reason": "concurrency_lock_held",
                },
            )
            return VoiceTurnResult.failure(
                turn_id=turn_id,
                session_id=session_id or "unknown",
                correlation_id=resolved_corr_id,
                error=err_msg,
                status=VoiceTurnStatus.CONCURRENCY_ERROR,
                transcription_status="skipped",
                command_status="skipped",
                synthesis_status="skipped",
                playback_status="skipped",
            )

        turn_start_time = time.monotonic()
        captured_audio_ref: Optional[Any] = None

        try:
            with self._lock:
                self._counters["turns_total"] += 1

            # 1. Resolve or create voice session
            session: VoiceSession
            if session_id:
                existing_sess = self.session_manager.get_session(session_id)
                if existing_sess is None:
                    session = self.session_manager.create_session(
                        session_id=session_id,
                        language=language or "en",
                    )
                elif existing_sess.is_closed:
                    err_msg = f"Voice session '{session_id}' is closed."
                    logger.warning(err_msg)
                    self._publish_event(
                        EventType.VOICE_TURN_REJECTED,
                        correlation_id=resolved_corr_id,
                        payload={
                            "turn_id": turn_id,
                            "session_id": session_id,
                            "reason": "session_closed",
                        },
                    )
                    with self._lock:
                        self._counters["turns_failed"] += 1
                    res = VoiceTurnResult.failure(
                        turn_id=turn_id,
                        session_id=session_id,
                        correlation_id=resolved_corr_id,
                        error=err_msg,
                        status=VoiceTurnStatus.FAILED,
                        transcription_status="skipped",
                        command_status="skipped",
                        synthesis_status="skipped",
                        playback_status="skipped",
                    )
                    self._sync_state_store(last_turn=res)
                    return res
                else:
                    session = existing_sess
            else:
                session = self.session_manager.create_session(language=language or "en")

            # 2. Publish VOICE_TURN_STARTED event
            self._publish_event(
                EventType.VOICE_TURN_STARTED,
                correlation_id=resolved_corr_id,
                payload={
                    "turn_id": turn_id,
                    "session_id": session.session_id,
                    "language_hint": language or session.language,
                    "has_audio_data": audio_data is not None,
                    "duration_seconds": duration_seconds,
                    "device_id": device_id,
                },
            )

            # 3. Audio Acquisition (Explicit Capture or Injected Payload)
            if audio_data is not None:
                captured_audio_ref = audio_data
            else:
                if self.audio_capture is None:
                    err_msg = "Audio capture service is not configured on VoiceService."
                    logger.error(err_msg)
                    with self._lock:
                        self._counters["turns_failed"] += 1
                    res = VoiceTurnResult.failure(
                        turn_id=turn_id,
                        session_id=session.session_id,
                        correlation_id=resolved_corr_id,
                        error=err_msg,
                        status=VoiceTurnStatus.CAPTURE_FAILED,
                        transcription_status="skipped",
                    )
                    self._publish_event(
                        EventType.VOICE_TURN_FAILED,
                        correlation_id=resolved_corr_id,
                        payload={"turn_id": turn_id, "session_id": session.session_id, "error": err_msg},
                    )
                    self._sync_state_store(active_session=session, last_turn=res)
                    return res

                try:
                    captured_audio_ref = self.audio_capture.capture_audio(
                        duration_seconds=duration_seconds,
                        device_id=device_id,
                        config=config,
                        correlation_id=resolved_corr_id,
                    )
                except AudioCaptureTimeoutError as exc:
                    err_msg = f"Audio capture timed out: {exc}"
                    logger.error(err_msg)
                    with self._lock:
                        self._counters["turns_failed"] += 1
                    res = VoiceTurnResult.failure(
                        turn_id=turn_id,
                        session_id=session.session_id,
                        correlation_id=resolved_corr_id,
                        error=err_msg,
                        status=VoiceTurnStatus.CAPTURE_FAILED,
                        transcription_status="skipped",
                    )
                    self._publish_event(
                        EventType.VOICE_TURN_FAILED,
                        correlation_id=resolved_corr_id,
                        payload={"turn_id": turn_id, "session_id": session.session_id, "error": err_msg},
                    )
                    self._sync_state_store(active_session=session, last_turn=res)
                    return res
                except (
                    AudioCaptureError,
                    AudioConfigurationError,
                    AudioDeviceNotFoundError,
                    AudioPermissionError,
                    AudioBufferOverflowError,
                ) as exc:
                    err_msg = f"Audio capture failed: {exc}"
                    logger.error(err_msg)
                    with self._lock:
                        self._counters["turns_failed"] += 1
                    res = VoiceTurnResult.failure(
                        turn_id=turn_id,
                        session_id=session.session_id,
                        correlation_id=resolved_corr_id,
                        error=err_msg,
                        status=VoiceTurnStatus.CAPTURE_FAILED,
                        transcription_status="skipped",
                    )
                    self._publish_event(
                        EventType.VOICE_TURN_FAILED,
                        correlation_id=resolved_corr_id,
                        payload={"turn_id": turn_id, "session_id": session.session_id, "error": err_msg},
                    )
                    self._sync_state_store(active_session=session, last_turn=res)
                    return res
                except Exception as exc:
                    err_msg = f"Unexpected audio capture error: {exc}"
                    logger.error(err_msg)
                    with self._lock:
                        self._counters["turns_failed"] += 1
                    res = VoiceTurnResult.failure(
                        turn_id=turn_id,
                        session_id=session.session_id,
                        correlation_id=resolved_corr_id,
                        error=err_msg,
                        status=VoiceTurnStatus.CAPTURE_FAILED,
                        transcription_status="skipped",
                    )
                    self._publish_event(
                        EventType.VOICE_TURN_FAILED,
                        correlation_id=resolved_corr_id,
                        payload={"turn_id": turn_id, "session_id": session.session_id, "error": err_msg},
                    )
                    self._sync_state_store(active_session=session, last_turn=res)
                    return res

            # Check if capture was cancelled
            if isinstance(captured_audio_ref, CapturedAudio) and captured_audio_ref.metadata.get("cancelled", False):
                logger.info("Audio capture cancelled for turn '%s'", turn_id)
                with self._lock:
                    self._counters["turns_cancelled"] += 1
                res = VoiceTurnResult.cancelled(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    duration_ms=round((time.monotonic() - turn_start_time) * 1000, 2),
                )
                self._publish_event(
                    EventType.VOICE_TURN_CANCELLED,
                    correlation_id=resolved_corr_id,
                    payload={"turn_id": turn_id, "session_id": session.session_id},
                )
                self._sync_state_store(active_session=session, last_turn=res)
                return res

            # 4. Speech-to-Text Provider Verification
            if self.stt_provider is None:
                err_msg = "STT provider is not configured on VoiceService."
                logger.error(err_msg)
                self._publish_provider_error("stt", err_msg, resolved_corr_id)
                with self._lock:
                    self._counters["turns_failed"] += 1
                res = VoiceTurnResult.failure(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    error=err_msg,
                    status=VoiceTurnStatus.STT_UNAVAILABLE,
                    transcription_status="unavailable",
                )
                self._publish_event(
                    EventType.VOICE_TURN_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={"turn_id": turn_id, "session_id": session.session_id, "error": err_msg},
                )
                self._sync_state_store(active_session=session, last_turn=res)
                return res

            if not self.stt_provider.is_available:
                err_msg = (
                    f"STT provider '{getattr(self.stt_provider, 'name', 'stt')}' is unavailable "
                    "(missing dependencies or local model weights)."
                )
                logger.warning(err_msg)
                self._publish_provider_error("stt", err_msg, resolved_corr_id)
                with self._lock:
                    self._counters["turns_failed"] += 1
                res = VoiceTurnResult.failure(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    error=err_msg,
                    status=VoiceTurnStatus.STT_UNAVAILABLE,
                    transcription_status="unavailable",
                )
                self._publish_event(
                    EventType.VOICE_TURN_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={"turn_id": turn_id, "session_id": session.session_id, "error": err_msg},
                )
                self._sync_state_store(active_session=session, last_turn=res)
                return res

            # 5. Execute STT Inference
            self._publish_event(
                EventType.VOICE_TRANSCRIPTION_STARTED,
                correlation_id=resolved_corr_id,
                payload={
                    "turn_id": turn_id,
                    "session_id": session.session_id,
                    "provider": getattr(self.stt_provider, "name", "none"),
                    "language_hint": language or session.language,
                },
            )

            stt_start_time = time.monotonic()
            try:
                transcription: TranscriptionResult = self.stt_provider.transcribe(
                    audio_data=captured_audio_ref,
                    language=language or session.language,
                )
                stt_dur = time.monotonic() - stt_start_time
                with self._lock:
                    self._last_stt_duration = round(stt_dur, 4)
            except STTEmptyTranscriptionError as exc:
                err_msg = str(exc)
                logger.info("Empty speech detected during STT in turn '%s': %s", turn_id, err_msg)
                self._publish_event(
                    EventType.VOICE_TRANSCRIPTION_EMPTY,
                    correlation_id=resolved_corr_id,
                    payload={"turn_id": turn_id, "session_id": session.session_id, "reason": err_msg},
                )
                with self._lock:
                    self._counters["turns_failed"] += 1
                res = VoiceTurnResult.empty_speech(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    language=session.language,
                    language_hint=language,
                    duration_ms=round((time.monotonic() - turn_start_time) * 1000, 2),
                    error=err_msg,
                )
                self._publish_event(
                    EventType.VOICE_TURN_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={
                        "turn_id": turn_id,
                        "session_id": session.session_id,
                        "status": res.status.value,
                        "error": err_msg,
                    },
                )
                self._sync_state_store(active_session=session, last_turn=res)
                return res
            except STTTimeoutError as exc:
                err_msg = f"STT transcription timed out: {exc}"
                logger.error(err_msg)
                self._publish_event(
                    EventType.VOICE_TRANSCRIPTION_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={"turn_id": turn_id, "error": err_msg},
                )
                with self._lock:
                    self._counters["turns_failed"] += 1
                res = VoiceTurnResult.failure(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    error=err_msg,
                    status=VoiceTurnStatus.STT_TIMEOUT,
                    transcription_status="timeout",
                    language=session.language,
                    language_hint=language,
                    duration_ms=round((time.monotonic() - turn_start_time) * 1000, 2),
                )
                self._publish_event(
                    EventType.VOICE_TURN_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={
                        "turn_id": turn_id,
                        "session_id": session.session_id,
                        "status": res.status.value,
                        "error": err_msg,
                    },
                )
                self._sync_state_store(active_session=session, last_turn=res)
                return res
            except (STTDependencyError, STTModelNotFoundError) as exc:
                err_msg = f"STT model/dependency unavailable: {exc}"
                logger.error(err_msg)
                self._publish_event(
                    EventType.VOICE_TRANSCRIPTION_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={"turn_id": turn_id, "error": err_msg},
                )
                with self._lock:
                    self._counters["turns_failed"] += 1
                res = VoiceTurnResult.failure(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    error=err_msg,
                    status=VoiceTurnStatus.STT_UNAVAILABLE,
                    transcription_status="unavailable",
                    language=session.language,
                    language_hint=language,
                    duration_ms=round((time.monotonic() - turn_start_time) * 1000, 2),
                )
                self._publish_event(
                    EventType.VOICE_TURN_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={
                        "turn_id": turn_id,
                        "session_id": session.session_id,
                        "status": res.status.value,
                        "error": err_msg,
                    },
                )
                self._sync_state_store(active_session=session, last_turn=res)
                return res
            except Exception as exc:
                err_msg = f"STT inference failed: {exc}"
                logger.error(err_msg)
                self._publish_event(
                    EventType.VOICE_TRANSCRIPTION_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={"turn_id": turn_id, "error": err_msg},
                )
                self._publish_provider_error("stt", str(exc), resolved_corr_id)
                with self._lock:
                    self._counters["turns_failed"] += 1
                res = VoiceTurnResult.failure(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    error=err_msg,
                    status=VoiceTurnStatus.STT_FAILED,
                    transcription_status="failed",
                    language=session.language,
                    language_hint=language,
                    duration_ms=round((time.monotonic() - turn_start_time) * 1000, 2),
                )
                self._publish_event(
                    EventType.VOICE_TURN_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={
                        "turn_id": turn_id,
                        "session_id": session.session_id,
                        "status": res.status.value,
                        "error": err_msg,
                    },
                )
                self._sync_state_store(active_session=session, last_turn=res)
                return res
            finally:
                # Resource Safety Invariant: Drop raw PCM buffers immediately
                captured_audio_ref = None

            # 6. Validate and Normalize Transcription Text
            if not transcription.text or not transcription.text.strip():
                empty_msg = "Transcription returned empty text (no speech detected)."
                logger.info("Empty transcription for turn '%s'", turn_id)
                self._publish_event(
                    EventType.VOICE_TRANSCRIPTION_EMPTY,
                    correlation_id=resolved_corr_id,
                    payload={"turn_id": turn_id, "session_id": session.session_id, "reason": empty_msg},
                )
                with self._lock:
                    self._counters["turns_failed"] += 1
                res = VoiceTurnResult.empty_speech(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    language=transcription.language,
                    language_hint=language,
                    duration_ms=round((time.monotonic() - turn_start_time) * 1000, 2),
                    error=empty_msg,
                )
                self._publish_event(
                    EventType.VOICE_TURN_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={
                        "turn_id": turn_id,
                        "session_id": session.session_id,
                        "status": res.status.value,
                        "error": empty_msg,
                    },
                )
                self._sync_state_store(active_session=session, last_turn=res)
                return res

            # Normalize surrounding whitespace; preserve user words and language tag
            normalized_text = transcription.text.strip()

            self._publish_event(
                EventType.VOICE_TRANSCRIPTION_COMPLETED,
                correlation_id=resolved_corr_id,
                payload={
                    "turn_id": turn_id,
                    "session_id": session.session_id,
                    "text": mask_sensitive_text(normalized_text),
                    "language": transcription.language,
                    "confidence": transcription.confidence,
                    "duration_seconds": transcription.duration_seconds or getattr(self, "_last_stt_duration", 0.0),
                },
            )

            # 7. Construct VoiceInput and Route through MasterOrchestrator (Text-first command execution)
            voice_input = VoiceInput.create(
                text=normalized_text,
                session_id=session.session_id,
                language=transcription.language,
                source=getattr(self.stt_provider, "name", "voice_turn"),
                metadata=dict(transcription.metadata or {}),
            )

            # Route command execution without synthesis first to preserve response on synthesis failure
            voice_output: VoiceOutput = self.process_voice_input(
                voice_input=voice_input,
                synthesize_output=False,
                correlation_id=resolved_corr_id,
            )

            turn_elapsed_ms = round((time.monotonic() - turn_start_time) * 1000, 2)

            # 8. Command Response Handling, Optional Synthesis, and Optional Playback
            if voice_output.is_success:
                command_status = "success"
                final_voice_output = voice_output
                synthesis_status = "skipped"
                synthesis_error: Optional[str] = None
                synthesis_result: Optional[SynthesisResult] = None
                playback_status = "skipped"
                playback_error: Optional[str] = None
                playback_result: Optional[AudioPlaybackResult] = None

                # Explicit Synthesis Control: callers requesting synthesis or playback
                should_synthesize = synthesize_output or play_output
                if should_synthesize:
                    if self.tts_provider is None:
                        synthesis_status = "failed"
                        synthesis_error = "TTS provider is not configured on VoiceService."
                        logger.error(synthesis_error)
                        self._publish_provider_error("tts", synthesis_error, resolved_corr_id)
                    elif not self.tts_provider.is_available:
                        synthesis_status = "failed"
                        synthesis_error = f"TTS provider '{self.tts_provider.name}' is currently unavailable."
                        logger.warning(synthesis_error)
                        self._publish_provider_error("tts", synthesis_error, resolved_corr_id)
                    else:
                        try:
                            synthesis_result = self.synthesize_text(
                                text=voice_output.text,
                                language=transcription.language,
                                correlation_id=resolved_corr_id,
                            )
                            synthesis_status = "completed"
                            final_voice_output = VoiceOutput.synthesized(
                                text=voice_output.text,
                                session_id=session.session_id,
                                audio_payload=synthesis_result.audio_bytes,
                                audio_format=synthesis_result.audio_format,
                                duration_ms=synthesis_result.duration_ms,
                                language=transcription.language,
                                metadata=dict(voice_output.metadata),
                            )
                        except TTSUnsupportedLanguageError as unsup_exc:
                            synthesis_status = "unsupported"
                            synthesis_error = str(unsup_exc)
                            logger.warning(
                                "TTS synthesis unsupported for language '%s': %s",
                                transcription.language,
                                unsup_exc,
                            )
                            self._publish_event(
                                EventType.VOICE_SYNTHESIS_UNSUPPORTED,
                                correlation_id=resolved_corr_id,
                                payload={
                                    "turn_id": turn_id,
                                    "session_id": session.session_id,
                                    "language": transcription.language,
                                    "reason": str(unsup_exc),
                                },
                            )
                        except Exception as tts_exc:
                            synthesis_status = "failed"
                            synthesis_error = str(tts_exc)
                            logger.error("TTS synthesis failed for turn '%s': %s", turn_id, tts_exc)

                # Explicit Playback Control: caller requesting playback
                if play_output:
                    if synthesis_status == "completed" and final_voice_output.has_audio:
                        if self.audio_playback is None:
                            playback_status = "failed"
                            playback_error = "Audio playback service is not configured on VoiceService."
                            logger.error(playback_error)
                        else:
                            try:
                                playback_result = self.play_audio(
                                    audio_data=final_voice_output,
                                    device_id=output_device_id,
                                    correlation_id=resolved_corr_id,
                                    stop_event=stop_event,
                                )
                                if playback_result.status == AudioPlaybackStatus.COMPLETED:
                                    playback_status = "completed"
                                elif playback_result.status == AudioPlaybackStatus.CANCELLED:
                                    playback_status = "cancelled"
                                else:
                                    playback_status = "failed"
                                    playback_error = playback_result.error or "Audio playback failed."
                            except Exception as play_exc:
                                playback_status = "failed"
                                playback_error = str(play_exc)
                                logger.error("Turn audio playback encountered error: %s", play_exc)
                    else:
                        playback_status = "skipped"

                turn_elapsed_ms = round((time.monotonic() - turn_start_time) * 1000, 2)
                with self._lock:
                    self._counters["turns_completed"] += 1

                res = VoiceTurnResult.completed(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    recognized_text=normalized_text,
                    command_response=final_voice_output,
                    language=transcription.language,
                    language_hint=language,
                    confidence=transcription.confidence,
                    duration_ms=turn_elapsed_ms,
                    command_status="success",
                    synthesis_status=synthesis_status,
                    playback_status=playback_status,
                    synthesis_error=synthesis_error,
                    playback_error=playback_error,
                    metadata={
                        "command": final_voice_output.metadata.get("command"),
                        "orchestrator_request_id": final_voice_output.metadata.get("orchestrator_request_id"),
                        "stt_duration_seconds": getattr(self, "_last_stt_duration", 0.0),
                        "command_status": "success",
                        "synthesis_status": synthesis_status,
                        "playback_status": playback_result.status.value if playback_result else None,
                        "playback_outcome": playback_status,
                        "synthesis_error": synthesis_error,
                        "playback_error": playback_error,
                        "playback_device_id": playback_result.device_id if playback_result else output_device_id,
                    },
                )
                self._publish_event(
                    EventType.VOICE_TURN_COMPLETED,
                    correlation_id=resolved_corr_id,
                    payload={
                        "turn_id": turn_id,
                        "session_id": session.session_id,
                        "status": res.status.value,
                        "command": final_voice_output.metadata.get("command"),
                        "command_status": "success",
                        "synthesis_status": synthesis_status,
                        "playback_status": playback_status,
                        "duration_ms": turn_elapsed_ms,
                    },
                )
                self._sync_state_store(active_session=session, last_output=final_voice_output, last_turn=res)
                return res
            else:
                with self._lock:
                    self._counters["turns_failed"] += 1
                err_str = voice_output.error or "Voice command execution failed."
                turn_status = (
                    VoiceTurnStatus.COMMAND_REJECTED
                    if "Unknown command" in err_str
                    else VoiceTurnStatus.FAILED
                )
                cmd_status = "rejected" if turn_status == VoiceTurnStatus.COMMAND_REJECTED else "failed"
                res = VoiceTurnResult.failure(
                    turn_id=turn_id,
                    session_id=session.session_id,
                    correlation_id=resolved_corr_id,
                    error=err_str,
                    status=turn_status,
                    transcription_status="success",
                    command_status=cmd_status,
                    synthesis_status="skipped",
                    playback_status="skipped",
                    synthesis_error=None,
                    playback_error=None,
                    recognized_text=normalized_text,
                    command_response=voice_output,
                    language=transcription.language,
                    language_hint=language,
                    duration_ms=turn_elapsed_ms,
                    metadata={
                        "command": voice_output.metadata.get("command"),
                        "orchestrator_request_id": voice_output.metadata.get("orchestrator_request_id"),
                        "stt_duration_seconds": getattr(self, "_last_stt_duration", 0.0),
                        "command_status": cmd_status,
                        "synthesis_status": "skipped",
                        "playback_status": None,
                        "playback_outcome": "skipped",
                    },
                )
                self._publish_event(
                    EventType.VOICE_TURN_FAILED,
                    correlation_id=resolved_corr_id,
                    payload={
                        "turn_id": turn_id,
                        "session_id": session.session_id,
                        "status": res.status.value,
                        "command_status": cmd_status,
                        "error": err_str,
                        "duration_ms": turn_elapsed_ms,
                    },
                )
                self._sync_state_store(active_session=session, last_output=voice_output, last_turn=res)
                return res

        finally:
            # Resource Safety Invariants: Always drop raw audio and release concurrency lock
            captured_audio_ref = None
            self._turn_lock.release()

    def process_audio(
        self,
        audio_data: Any,
        session_id: Optional[str] = None,
        language: Optional[str] = None,
        synthesize_output: bool = False,
        correlation_id: Optional[str] = None,
    ) -> VoiceOutput:
        """Transcribe audio input via STT provider and route through the orchestrator.

        Args:
            audio_data: Audio buffer, token, or fixture key.
            session_id: Optional voice session ID (created if omitted).
            language: Optional expected language hint.
            synthesize_output: Whether to synthesize speech response using TTS.
            correlation_id: Optional correlation ID for tracing.

        Returns:
            VoiceOutput: Structured text or synthesized response.
        """
        resolved_corr_id = (correlation_id or uuid.uuid4().hex).strip()

        if self.stt_provider is None:
            err_msg = "STT provider is not configured on VoiceService."
            logger.error(err_msg)
            self._publish_provider_error("stt", err_msg, resolved_corr_id)
            return VoiceOutput.failure(
                error=err_msg,
                session_id=session_id or "unknown_session",
            )

        # 1. Publish transcription started event
        self._publish_event(
            EventType.VOICE_TRANSCRIPTION_STARTED,
            correlation_id=resolved_corr_id,
            payload={
                "session_id": session_id or "pending",
                "provider": getattr(self.stt_provider, "name", "none"),
                "language_hint": language or "en",
                "audio_type": type(audio_data).__name__,
            },
        )

        stt_start_time = time.monotonic()

        # 2. Execute Speech-to-Text
        try:
            transcription: TranscriptionResult = self.stt_provider.transcribe(
                audio_data=audio_data,
                language=language,
            )
        except STTEmptyTranscriptionError as exc:
            err_msg = str(exc)
            logger.info("Empty audio/speech detected during STT: %s", err_msg)
            self._publish_event(
                EventType.VOICE_TRANSCRIPTION_EMPTY,
                correlation_id=resolved_corr_id,
                payload={
                    "session_id": session_id or "pending",
                    "provider": getattr(self.stt_provider, "name", "none"),
                    "reason": err_msg,
                },
            )
            with self._lock:
                self._counters["failed"] += 1
            self._sync_state_store()
            return VoiceOutput.failure(
                error=err_msg,
                session_id=session_id or "unknown_session",
                language=language or "unknown",
            )
        except Exception as exc:
            err_msg = f"STT transcription failed: {exc}"
            logger.error(err_msg)
            self._publish_event(
                EventType.VOICE_TRANSCRIPTION_FAILED,
                correlation_id=resolved_corr_id,
                payload={
                    "session_id": session_id or "pending",
                    "provider": getattr(self.stt_provider, "name", "none"),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            self._publish_provider_error("stt", str(exc), resolved_corr_id)
            with self._lock:
                self._counters["failed"] += 1
            self._sync_state_store()
            return VoiceOutput.failure(
                error=err_msg,
                session_id=session_id or "unknown_session",
                language=language or "unknown",
            )

        stt_duration = time.monotonic() - stt_start_time
        with self._lock:
            self._last_stt_duration = round(stt_duration, 4)

        # 3. Check for empty transcription outcome
        if not transcription.text or not transcription.text.strip():
            empty_msg = "Transcription returned empty text (no speech detected)."
            logger.info(empty_msg)
            self._publish_event(
                EventType.VOICE_TRANSCRIPTION_EMPTY,
                correlation_id=resolved_corr_id,
                payload={
                    "session_id": session_id or "pending",
                    "provider": getattr(self.stt_provider, "name", "none"),
                    "duration_seconds": transcription.duration_seconds or stt_duration,
                },
            )
            with self._lock:
                self._counters["failed"] += 1
            self._sync_state_store()
            return VoiceOutput.failure(
                error=empty_msg,
                session_id=session_id or "unknown_session",
                language=transcription.language,
            )

        # 4. Publish transcription completed event
        self._publish_event(
            EventType.VOICE_TRANSCRIPTION_COMPLETED,
            correlation_id=resolved_corr_id,
            payload={
                "session_id": session_id or "pending",
                "text": mask_sensitive_text(transcription.text),
                "language": transcription.language,
                "confidence": transcription.confidence,
                "duration_seconds": transcription.duration_seconds or stt_duration,
            },
        )

        # 5. Construct VoiceInput and process
        voice_input = VoiceInput.create(
            text=transcription.text,
            session_id=session_id,
            language=transcription.language,
            source=getattr(self.stt_provider, "name", "mock_stt"),
            metadata=transcription.metadata,
        )

        return self.process_voice_input(
            voice_input=voice_input,
            synthesize_output=synthesize_output,
            correlation_id=resolved_corr_id,
        )

    def process_voice_input(
        self,
        voice_input: Union[VoiceInput, Dict[str, Any]],
        synthesize_output: bool = False,
        correlation_id: Optional[str] = None,
    ) -> VoiceOutput:
        """Process validated VoiceInput, route through orchestrator, and return VoiceOutput.

        Args:
            voice_input: VoiceInput instance or dictionary representation.
            synthesize_output: Whether to synthesize TTS audio for response.
            correlation_id: Optional distributed correlation tracing ID.

        Returns:
            VoiceOutput: Structured response containing text and optional audio.
        """
        with self._lock:
            self._counters["total_inputs"] += 1

        resolved_corr_id = (correlation_id or uuid.uuid4().hex).strip()

        # 1. Normalize and validate VoiceInput
        inp: VoiceInput
        if isinstance(voice_input, VoiceInput):
            inp = voice_input
        elif isinstance(voice_input, dict):
            try:
                inp = VoiceInput.create(**voice_input)
            except (ValidationError, TypeError, ValueError) as err:
                err_text = f"Malformed voice input: {err}"
                logger.error(err_text)
                with self._lock:
                    self._counters["failed"] += 1
                self._sync_state_store()
                return VoiceOutput.failure(
                    error=err_text,
                    session_id=str(voice_input.get("session_id", "unknown")),
                )
        else:
            err_text = f"Invalid voice input type: {type(voice_input).__name__}"
            with self._lock:
                self._counters["failed"] += 1
            self._sync_state_store()
            return VoiceOutput.failure(error=err_text, session_id="unknown")

        # 2. Validate or create corresponding VoiceSession
        session = self.session_manager.get_session(inp.session_id)
        if session is None:
            session = self.session_manager.create_session(
                session_id=inp.session_id,
                language=inp.language,
            )
        elif session.is_closed:
            err_text = f"Voice session '{session.session_id}' is closed."
            logger.warning(err_text)
            with self._lock:
                self._counters["failed"] += 1
            self._sync_state_store(active_session=session)
            return VoiceOutput.failure(error=err_text, session_id=session.session_id)

        # Transition session to PROCESSING
        try:
            session.transition_to(VoiceSessionStatus.PROCESSING, reason="Processing input")
            self._publish_event(
                EventType.VOICE_SESSION_UPDATED,
                correlation_id=resolved_corr_id,
                payload={
                    "session_id": session.session_id,
                    "status": VoiceSessionStatus.PROCESSING.value,
                    "action": "status_change",
                },
            )
        except VoiceSessionError as s_err:
            logger.error("Session state error: %s", s_err)

        # 3. Publish VOICE_INPUT_RECEIVED event
        self._publish_event(
            EventType.VOICE_INPUT_RECEIVED,
            correlation_id=resolved_corr_id,
            payload={
                "input_id": inp.input_id,
                "session_id": inp.session_id,
                "text": mask_sensitive_text(inp.text),
                "language": inp.language,
                "source": inp.source,
            },
        )

        # 4. Extract command keyword deterministically from text
        # (Converts 'system status' -> 'system_status', leaves 'status' as 'status')
        raw_text = inp.text.strip().lower()
        command = raw_text.replace(" ", "_")

        # 5. Build OrchestratorRequest
        safe_context = sanitize_container(dict(inp.metadata))
        safe_context["session_id"] = inp.session_id
        safe_context["voice_input_id"] = inp.input_id
        safe_context["raw_voice_text"] = inp.text

        orch_request = OrchestratorRequest.create(
            command=command,
            correlation_id=resolved_corr_id,
            input_language=inp.language,
            source="voice",
            context=safe_context,
        )

        # 6. Route through MasterOrchestrator
        logger.info(
            "Routing voice command '%s' [session=%s, lang=%s] to MasterOrchestrator",
            command,
            session.session_id,
            inp.language,
        )
        orch_response: OrchestratorResponse = self.orchestrator.handle_request(orch_request)

        # 7. Convert OrchestratorResponse to VoiceOutput
        output: VoiceOutput
        if orch_response.is_success:
            # Succeeded: reset session to ACTIVE
            try:
                session.transition_to(VoiceSessionStatus.ACTIVE, reason="Request completed successfully")
            except VoiceSessionError:
                pass

            # Check if caller explicitly requested speech synthesis
            if synthesize_output and self.tts_provider is not None:
                try:
                    synthesis = self.synthesize_text(
                        text=orch_response.response_text,
                        language=inp.language,
                        correlation_id=resolved_corr_id,
                    )
                    output = VoiceOutput.synthesized(
                        text=orch_response.response_text,
                        session_id=inp.session_id,
                        audio_payload=synthesis.audio_bytes,
                        audio_format=synthesis.audio_format,
                        duration_ms=synthesis.duration_ms,
                        language=inp.language,
                        metadata={"command": command, "orchestrator_request_id": orch_request.request_id},
                    )
                except Exception as tts_exc:
                    err_msg = f"TTS synthesis failed: {tts_exc}"
                    logger.error(err_msg)
                    # Return structured failure indicating synthesis error
                    output = VoiceOutput.failure(
                        error=err_msg,
                        session_id=inp.session_id,
                        text=orch_response.response_text,
                        language=inp.language,
                    )
                    with self._lock:
                        self._counters["failed"] += 1
            else:
                # Text-only output
                output = VoiceOutput.text_only(
                    text=orch_response.response_text,
                    session_id=inp.session_id,
                    language=inp.language,
                    metadata={"command": command, "orchestrator_request_id": orch_request.request_id},
                )

            with self._lock:
                self._counters["processed"] += 1

        else:
            # Orchestrator reported failure
            try:
                session.transition_to(VoiceSessionStatus.ACTIVE, reason="Orchestrator error handled")
            except VoiceSessionError:
                pass

            err_summary = orch_response.errors[0] if orch_response.errors else "Command failed."
            output = VoiceOutput.failure(
                error=err_summary,
                session_id=inp.session_id,
                text=orch_response.response_text or f"Command failed: {err_summary}",
                language=inp.language,
                metadata={"command": command, "orchestrator_request_id": orch_request.request_id},
            )
            with self._lock:
                self._counters["failed"] += 1

        # 8. Update StateStore and publish completion event
        self._sync_state_store(active_session=session, last_output=output)

        self._publish_event(
            EventType.VOICE_RESPONSE_CREATED,
            correlation_id=resolved_corr_id,
            payload={
                "output_id": output.output_id,
                "session_id": output.session_id,
                "status": output.status.value,
                "text": mask_sensitive_text(output.text),
                "has_audio": output.audio_payload is not None,
                "language": output.language,
            },
        )

        return output

    # --------------------------------------------------------------------------
    # Explicit Synthesis & Playback Operations (Step 10)
    # --------------------------------------------------------------------------

    def synthesize_text(
        self,
        text: str,
        language: str = "en",
        voice_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        context: Optional[Mapping[str, Any]] = None,
    ) -> SynthesisResult:
        """Synthesize text into a structured audio SynthesisResult without playback.

        Args:
            text: Text to synthesize into speech.
            language: Target language tag ('en', 'ta', 'tanglish', 'mixed').
            voice_id: Optional speaker profile/voice ID.
            correlation_id: Optional distributed tracing correlation identifier.
            context: Optional contextual synthesis parameters.

        Returns:
            SynthesisResult: Validated binary audio payload and metadata.

        Raises:
            VoiceProviderError: If TTS provider is missing, unavailable, or fails.
        """
        resolved_corr_id = (correlation_id or uuid.uuid4().hex).strip()

        if self.tts_provider is None:
            err_msg = "TTS provider is not configured on VoiceService."
            logger.error(err_msg)
            self._publish_provider_error("tts", err_msg, resolved_corr_id)
            raise VoiceProviderError(err_msg)

        if not self.tts_provider.is_available:
            err_msg = f"TTS provider '{self.tts_provider.name}' is currently unavailable."
            logger.warning(err_msg)
            self._publish_provider_error("tts", err_msg, resolved_corr_id)
            raise VoiceProviderError(err_msg)

        if not text or not isinstance(text, str) or not text.strip():
            err_msg = "Cannot synthesize empty or whitespace text."
            self._publish_event(
                EventType.VOICE_SYNTHESIS_EMPTY,
                correlation_id=resolved_corr_id,
                payload={"error": err_msg},
            )
            raise VoiceProviderError(err_msg)

        clean_text = text.strip()
        self._publish_event(
            EventType.VOICE_SYNTHESIS_STARTED,
            correlation_id=resolved_corr_id,
            payload={
                "provider": self.tts_provider.name,
                "language": language,
                "text_length": len(clean_text),
                "voice_id": voice_id,
            },
        )

        start_time = time.monotonic()
        try:
            result = self.tts_provider.synthesize(
                text=clean_text,
                language=language,
                voice_id=voice_id,
                context=context,
            )
            elapsed_ms = round((time.monotonic() - start_time) * 1000.0, 2)
            with self._lock:
                self._counters["synthesized"] += 1
                self._last_tts_duration = elapsed_ms

            self._publish_event(
                EventType.VOICE_SYNTHESIS_COMPLETED,
                correlation_id=resolved_corr_id,
                payload={
                    "provider": self.tts_provider.name,
                    "language": result.language,
                    "duration_ms": result.duration_ms,
                    "audio_format": result.audio_format,
                    "audio_bytes_len": len(result.audio_bytes),
                    "synthesis_time_ms": elapsed_ms,
                },
            )
            self._sync_state_store()
            return result

        except Exception as exc:
            err_msg = f"Speech synthesis failed: {exc}"
            logger.error(err_msg)
            self._publish_event(
                EventType.VOICE_SYNTHESIS_FAILED,
                correlation_id=resolved_corr_id,
                payload={"provider": self.tts_provider.name, "error": str(exc)},
            )
            self._publish_provider_error("tts", str(exc), resolved_corr_id)
            if isinstance(exc, VoiceProviderError):
                raise
            raise VoiceProviderError(err_msg) from exc

    def play_audio(
        self,
        audio_data: Any,
        device_id: Optional[int] = None,
        correlation_id: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> AudioPlaybackResult:
        """Play synthesized audio or VoiceOutput through the configured playback service.

        Safety:
        - Initiated ONLY upon explicit request.
        - Audio is never played automatically.
        - Does NOT persist audio to disk or StateStore.

        Args:
            audio_data: SynthesisResult, VoiceOutput, or raw audio bytes.
            device_id: Optional target output device ID.
            correlation_id: Optional tracing identifier.
            stop_event: Optional cancellation event.

        Returns:
            AudioPlaybackResult: Structured playback execution result.

        Raises:
            AudioPlaybackError: If playback service is missing or execution fails.
        """
        if self.audio_playback is None:
            raise AudioPlaybackError("Audio playback service is not configured on VoiceService.")

        result = self.audio_playback.play(
            audio_data=audio_data,
            device_id=device_id,
            correlation_id=correlation_id,
            stop_event=stop_event,
        )
        if result.is_success:
            with self._lock:
                self._counters["played"] += 1
        return result

    def speak_output(
        self,
        voice_output: VoiceOutput,
        device_id: Optional[int] = None,
        correlation_id: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> AudioPlaybackResult:
        """Synthesize and play VoiceOutput speech response.

        Args:
            voice_output: VoiceOutput to speak.
            device_id: Optional target output device ID.
            correlation_id: Optional tracing identifier.
            stop_event: Optional cancellation event.

        Returns:
            AudioPlaybackResult: Structured playback result.
        """
        resolved_corr_id = correlation_id or getattr(voice_output, "output_id", None)
        if not voice_output.has_audio:
            synthesis = self.synthesize_text(
                text=voice_output.text,
                language=voice_output.language,
                correlation_id=resolved_corr_id,
            )
            return self.play_audio(
                audio_data=synthesis,
                device_id=device_id,
                correlation_id=resolved_corr_id,
                stop_event=stop_event,
            )
        else:
            return self.play_audio(
                audio_data=voice_output,
                device_id=device_id,
                correlation_id=resolved_corr_id,
                stop_event=stop_event,
            )

    # --------------------------------------------------------------------------
    # Telemetry & Event Helpers
    # --------------------------------------------------------------------------

    def get_metrics(self) -> Dict[str, Any]:
        """Return operational counters and active session metrics."""
        with self._lock:
            return {
                "counters": dict(self._counters),
                "active_sessions": self.session_manager.active_count,
                "stt_available": self.stt_provider.is_available if self.stt_provider else False,
                "tts_available": self.tts_provider.is_available if self.tts_provider else False,
                "playback_available": self.audio_playback.is_available if self.audio_playback else False,
                "last_turn": dict(self._last_turn_summary) if self._last_turn_summary else None,
            }

    def _publish_event(
        self,
        event_type: EventType | str,
        correlation_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        """Publish lifecycle event with subscriber failure isolation."""
        if self.event_bus is None:
            return

        try:
            safe_payload = sanitize_container(dict(payload))
            event = Event.create(
                event_type=event_type,
                source="voice",
                correlation_id=correlation_id,
                payload=safe_payload,
                metadata={"source": "voice", "service": self.name},
            )
            self.event_bus.publish(event)
        except Exception as exc:
            # Subscriber failure isolation: event bus issues must never crash voice processing
            logger.warning("EventBus dispatch failed for voice event '%s': %s", event_type, exc)

    def _publish_provider_error(self, provider_type: str, error_message: str, correlation_id: str) -> None:
        """Publish VOICE_PROVIDER_ERROR event."""
        self._publish_event(
            EventType.VOICE_PROVIDER_ERROR,
            correlation_id=correlation_id,
            payload={
                "provider_type": provider_type,
                "error": mask_sensitive_text(error_message),
                "timestamp": time.time(),
            },
        )

    def _sync_state_store(
        self,
        active_session: Optional[VoiceSession] = None,
        last_output: Optional[VoiceOutput] = None,
        last_turn: Optional[VoiceTurnResult] = None,
    ) -> None:
        """Synchronize bounded operational state to StateStore."""
        if self.state_store is None:
            return

        try:
            with self._lock:
                counters_copy = dict(self._counters)

            self.state_store.set("voice_request_counters", counters_copy)

            provider_status = {
                "stt_provider": getattr(self.stt_provider, "name", "none"),
                "stt_available": self.stt_provider.is_available if self.stt_provider else False,
                "tts_provider": getattr(self.tts_provider, "name", "none"),
                "tts_available": self.tts_provider.is_available if self.tts_provider else False,
                "playback_available": self.audio_playback.is_available if self.audio_playback else False,
                "last_stt_duration_seconds": getattr(self, "_last_stt_duration", 0.0),
                "last_tts_duration_ms": getattr(self, "_last_tts_duration", 0.0),
            }
            self.state_store.set("voice_provider_status", provider_status)

            if active_session:
                sess_summary = {
                    "session_id": active_session.session_id,
                    "status": active_session.status.value,
                    "language": active_session.language,
                    "updated_at": active_session.updated_at,
                }
                self.state_store.set("voice_active_session", sess_summary)

            if last_output:
                out_summary = {
                    "output_id": last_output.output_id,
                    "session_id": last_output.session_id,
                    "status": last_output.status.value,
                    "duration_ms": last_output.duration_ms,
                    "has_audio": last_output.audio_payload is not None,
                    "error": last_output.error,
                }
                self.state_store.set("voice_last_output", out_summary)

            if last_turn:
                turn_summary = {
                    "turn_id": last_turn.turn_id,
                    "session_id": last_turn.session_id,
                    "status": last_turn.status.value,
                    "transcription_status": last_turn.transcription_status,
                    "language": last_turn.language,
                    "duration_ms": last_turn.duration_ms,
                    "is_success": last_turn.is_success,
                    "error": last_turn.error,
                }
                with self._lock:
                    self._last_turn_summary = turn_summary
                self.state_store.set("voice_last_turn", turn_summary)

        except Exception as exc:
            logger.warning("Failed to synchronize voice state to StateStore: %s", exc)
