"""KAISER — Voice Interaction Controller & Session Management.

Coordinates user-initiated voice interactions through VoiceService, enforces
per-session concurrency boundaries, manages session lifecycle states, facilitates
clean recovery from turn completions and failures, and provides cooperative
cancellation tokens to supported pipeline stages (audio capture and audio playback).

Guarantees:
- Zero always-on or background microphone recording.
- Zero automatic audio playback on startup or import.
- Purely caller-initiated turn processing.
- Strict per-session concurrency protection (overlapping turns within a session rejected).
- Preserves correlation and tracing IDs across session and turn lifecycles.
- Decoupled fault isolation: Presentation (TTS/playback) issues do not corrupt command responses.
- Explicit cancellation semantics: Audio capture and playback can be interrupted mid-flight;
  synchronous stages (STT inference, orchestration, synthesis) execute atomically to completion.
- Thread-safe bounded session retention and cleanup.
- Trading engine remains uninitialized and execution engine locked.
"""

from __future__ import annotations

import copy
import threading
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Union

from app.core.events import Event, EventBus, EventType
from app.core.exceptions import KaiserError, ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.orchestrator.lifecycle import sanitize_container
from app.orchestrator.models import normalize_language
from app.voice.audio_models import AudioCaptureConfig
from app.voice.models import (
    VoiceSessionStatus,
    VoiceTurnResult,
    VoiceTurnStatus,
)
from app.voice.service import VoiceService
from app.voice.session import VoiceSession, VoiceSessionError, VoiceSessionManager

logger = get_logger("voice.controller")


class VoiceControllerError(KaiserError):
    """Raised when a voice interaction controller operation fails invariant checks."""


class VoiceInteractionController:
    """Coordinates session lifecycle and caller-initiated voice turns.

    Manages interaction sessions, coordinates turn execution through the underlying
    VoiceService, provides cooperative cancellation handles, and isolates session
    failures so subsequent interactions can recover cleanly.
    """

    def __init__(
        self,
        voice_service: VoiceService,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        session_manager: Optional[VoiceSessionManager] = None,
        name: str = "voice_interaction_controller",
    ) -> None:
        if voice_service is None:
            raise ValidationError("voice_service must not be None.")

        self.voice_service = voice_service
        self.event_bus = event_bus or voice_service.event_bus
        self.state_store = state_store or voice_service.state_store
        self.session_manager = session_manager or voice_service.session_manager
        self.name = name

        self._lock = threading.RLock()
        # Maps session_id -> {turn_id, stop_event, started_at, correlation_id}
        self._active_turns: Dict[str, Dict[str, Any]] = {}
        self._counters: Dict[str, int] = {
            "sessions_created": 0,
            "sessions_closed": 0,
            "turns_submitted": 0,
            "turns_cancelled": 0,
            "cancellations_requested": 0,
            "turns_recovered": 0,
        }

        logger.info(
            "VoiceInteractionController '%s' initialized (session_mgr=%s, event_bus=%s, state_store=%s)",
            self.name,
            "connected" if self.session_manager else "none",
            "connected" if self.event_bus else "none",
            "connected" if self.state_store else "none",
        )

    # ---------------------------------------------------------------------------
    # Session Management Operations
    # ---------------------------------------------------------------------------

    def create_session(
        self,
        session_id: Optional[str] = None,
        language: str = "en",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceSession:
        """Explicitly create or begin a new voice interaction session.

        Args:
            session_id: Optional unique session identifier. If omitted, a UUID is generated.
            language: Initial conversational language tag ('en', 'ta', 'tanglish', 'mixed').
            metadata: Optional caller-provided operational metadata.

        Returns:
            VoiceSession: The newly created, thread-safe session in ACTIVE state.

        Raises:
            ValidationError: If session_id is invalid.
        """
        sid = (session_id or f"sess_{uuid.uuid4().hex[:12]}").strip()
        if not sid:
            raise ValidationError("session_id must not be empty or whitespace.")

        norm_lang = normalize_language(language)

        with self._lock:
            session = self.session_manager.create_session(
                session_id=sid,
                language=norm_lang,
                metadata=metadata,
            )
            self._counters["sessions_created"] += 1

        self._publish_event(
            EventType.VOICE_SESSION_CREATED,
            correlation_id=sid,
            payload={
                "session_id": session.session_id,
                "language": session.language,
                "created_at": session.created_at,
                "metadata": session.get_metadata(),
            },
        )
        self._sync_state_store(session)
        logger.info("Session '%s' created [lang=%s]", session.session_id, session.language)
        return session

    def get_session(self, session_id: str) -> Optional[VoiceSession]:
        """Retrieve an existing session by identifier.

        Args:
            session_id: Session identifier string.

        Returns:
            Optional[VoiceSession]: The session instance if found, else None.
        """
        if not session_id or not isinstance(session_id, str):
            return None
        return self.session_manager.get_session(session_id.strip())

    def get_session_status(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Inspect the current operational status of a session.

        Args:
            session_id: Session identifier string.

        Returns:
            Optional[Dict[str, Any]]: Point-in-time snapshot of session status and active turns,
            or None if the session does not exist.
        """
        session = self.get_session(session_id)
        if session is None:
            return None

        with self._lock:
            active_turn = self._active_turns.get(session.session_id)
            active_info: Optional[Dict[str, Any]] = None
            if active_turn:
                active_info = {
                    "turn_id": active_turn["turn_id"],
                    "started_at": active_turn["started_at"],
                    "correlation_id": active_turn["correlation_id"],
                    "is_cancellation_requested": active_turn["stop_event"].is_set(),
                }

        snap = session.to_dict()
        snap["has_active_turn"] = active_info is not None
        snap["active_turn"] = active_info
        return snap

    def close_session(self, session_id: str, reason: str = "User request") -> bool:
        """Gracefully close a voice session and release its resources.

        Cancels any active turn for the session and transitions it to CLOSED.
        Repeated calls on an already-closed session are idempotent and safe.

        Args:
            session_id: Session identifier string.
            reason: Optional explanation for session closure.

        Returns:
            bool: True if the session was transitioned or already closed, False if not found.
        """
        if not session_id or not isinstance(session_id, str):
            return False

        sid = session_id.strip()
        with self._lock:
            # Cancel active turn if running
            active = self._active_turns.get(sid)
            if active:
                logger.info("Cancelling active turn '%s' due to session '%s' closure", active["turn_id"], sid)
                active["stop_event"].set()

            session = self.session_manager.get_session(sid)
            if session is None:
                return False

            if session.is_closed:
                # Idempotent re-entry
                return True

            session.close(reason=reason)
            self._counters["sessions_closed"] += 1

        self._publish_event(
            EventType.VOICE_SESSION_CLOSED,
            correlation_id=sid,
            payload={
                "session_id": sid,
                "reason": reason,
                "closed_at": time.time(),
            },
        )
        self._sync_state_store(session)
        logger.info("Session '%s' closed gracefully (reason: %s)", sid, reason)
        return True

    def recover_session(self, session_id: str, reason: str = "Turn recovery") -> bool:
        """Recover a failed or completed session back to ACTIVE status.

        Allows subsequent turns to be accepted cleanly after an error or completed turn.

        Args:
            session_id: Session identifier string.
            reason: Optional explanation for recovery.

        Returns:
            bool: True if session is now ACTIVE, False if session is closed, not found, or active.
        """
        if not session_id or not isinstance(session_id, str):
            return False

        sid = session_id.strip()
        with self._lock:
            session = self.session_manager.get_session(sid)
            if session is None or session.is_closed:
                return False

            if sid in self._active_turns:
                logger.warning("Cannot recover session '%s' while a turn is actively processing", sid)
                return False

            success = self.session_manager.recover_session(sid, reason=reason)
            if success:
                self._counters["turns_recovered"] += 1

        if success:
            self._publish_event(
                EventType.VOICE_SESSION_UPDATED,
                correlation_id=sid,
                payload={
                    "session_id": sid,
                    "status": VoiceSessionStatus.ACTIVE.value,
                    "action": "recovered",
                    "reason": reason,
                },
            )
            self._sync_state_store(session)
            logger.info("Session '%s' successfully recovered to ACTIVE", sid)

        return success

    def list_active_sessions(self) -> List[str]:
        """Return IDs of all currently active sessions."""
        return self.session_manager.list_active_sessions()

    # ---------------------------------------------------------------------------
    # Voice Turn Submission & Cancellation
    # ---------------------------------------------------------------------------

    def submit_turn(
        self,
        session_id: str,
        duration_seconds: Optional[float] = None,
        audio_data: Optional[Any] = None,
        language: Optional[str] = None,
        device_id: Optional[int] = None,
        output_device_id: Optional[int] = None,
        config: Optional[AudioCaptureConfig] = None,
        synthesize_output: bool = True,
        play_output: bool = False,
        correlation_id: Optional[str] = None,
    ) -> VoiceTurnResult:
        """Submit one voice interaction turn for processing.

        Coordinates execution through VoiceService.execute_voice_turn.
        Enforces per-session concurrency safety: rejects conflicting turns in the same session.

        Args:
            session_id: Unique identifier of the session submitting this turn.
            duration_seconds: Optional duration for audio capture.
            audio_data: Optional pre-captured audio payload (for tests/injections).
            language: Optional expected language hint ('en', 'ta', 'tanglish', 'mixed').
            device_id: Optional input microphone device index.
            output_device_id: Optional output playback device index.
            config: Optional custom audio capture configuration.
            synthesize_output: Whether to synthesize response via TTS provider.
            play_output: Whether to play synthesized response via audio playback.
            correlation_id: Optional distributed tracing correlation identifier.

        Returns:
            VoiceTurnResult: Strongly-typed, immutable turn outcome.
        """
        resolved_corr_id = (correlation_id or uuid.uuid4().hex).strip()
        turn_id = f"turn_{uuid.uuid4().hex[:12]}"

        # 1. Validate session_id
        if not session_id or not isinstance(session_id, str) or not session_id.strip():
            err_msg = "session_id must be a non-empty string."
            logger.error(err_msg)
            return VoiceTurnResult.failure(
                turn_id=turn_id,
                session_id="unknown",
                correlation_id=resolved_corr_id,
                error=err_msg,
                status=VoiceTurnStatus.FAILED,
                transcription_status="skipped",
                command_status="skipped",
            )

        sid = session_id.strip()

        # 2. Check session existence and state
        session = self.session_manager.get_session(sid)
        if session is None:
            err_msg = f"Voice session '{sid}' not found."
            logger.warning(err_msg)
            return VoiceTurnResult.failure(
                turn_id=turn_id,
                session_id=sid,
                correlation_id=resolved_corr_id,
                error=err_msg,
                status=VoiceTurnStatus.FAILED,
                transcription_status="skipped",
                command_status="skipped",
            )

        if session.is_closed:
            err_msg = f"Voice session '{sid}' is closed. New turns cannot be submitted to closed sessions."
            logger.warning(err_msg)
            return VoiceTurnResult.failure(
                turn_id=turn_id,
                session_id=sid,
                correlation_id=resolved_corr_id,
                error=err_msg,
                status=VoiceTurnStatus.FAILED,
                transcription_status="skipped",
                command_status="skipped",
            )

        # 3. Enforce per-session concurrency safety
        stop_event = threading.Event()
        with self._lock:
            if sid in self._active_turns:
                active_turn_id = self._active_turns[sid]["turn_id"]
                err_msg = (
                    f"Session '{sid}' already has an active turn '{active_turn_id}' in progress. "
                    "Concurrent turns within the same session are rejected."
                )
                logger.warning(err_msg)
                return VoiceTurnResult.failure(
                    turn_id=turn_id,
                    session_id=sid,
                    correlation_id=resolved_corr_id,
                    error=err_msg,
                    status=VoiceTurnStatus.CONCURRENCY_ERROR,
                    transcription_status="skipped",
                    command_status="skipped",
                )

            # Auto-recover session to ACTIVE if it was in COMPLETED from a prior turn
            if session.status == VoiceSessionStatus.COMPLETED:
                try:
                    session.transition_to(VoiceSessionStatus.ACTIVE, reason="Starting subsequent turn")
                except VoiceSessionError:
                    pass

            self._active_turns[sid] = {
                "turn_id": turn_id,
                "stop_event": stop_event,
                "started_at": time.time(),
                "correlation_id": resolved_corr_id,
            }
            self._counters["turns_submitted"] += 1

        # 4. Execute turn through VoiceService
        try:
            logger.info("Executing turn '%s' for session '%s' [corr=%s]", turn_id, sid, resolved_corr_id)
            result = self.voice_service.execute_voice_turn(
                duration_seconds=duration_seconds,
                audio_data=audio_data,
                session_id=sid,
                language=language or session.language,
                device_id=device_id,
                output_device_id=output_device_id,
                config=config,
                synthesize_output=synthesize_output,
                play_output=play_output,
                correlation_id=resolved_corr_id,
                stop_event=stop_event,
            )
            return result

        finally:
            # 5. Guaranteed cleanup on all paths
            with self._lock:
                self._active_turns.pop(sid, None)

            # Ensure session recovers cleanly to ACTIVE for subsequent turns if still valid
            if not session.is_closed:
                try:
                    session.recover(reason="Clean recovery after turn completion/failure")
                except VoiceSessionError:
                    pass

    def request_cancellation(self, session_id: str) -> bool:
        """Request cooperative cancellation of the active voice turn for a session.

        Signals the underlying stop_event. Stages that support cancellation (audio capture
        and audio playback) will abort immediately. Synchronous atomic stages (STT inference,
        command execution, speech synthesis) will complete their current operation.

        Repeated calls are idempotent.

        Args:
            session_id: Session identifier string.

        Returns:
            bool: True if an active turn was found and signaled, False if no turn was active.
        """
        if not session_id or not isinstance(session_id, str):
            return False

        sid = session_id.strip()
        with self._lock:
            active = self._active_turns.get(sid)
            if active:
                active["stop_event"].set()
                self._counters["cancellations_requested"] += 1
                logger.info(
                    "Cooperative cancellation requested for turn '%s' in session '%s'",
                    active["turn_id"],
                    sid,
                )
                return True
            else:
                logger.debug("Cancellation requested for session '%s', but no turn is active", sid)
                return False

    def cancel_turn(self, session_id: str) -> bool:
        """Alias for request_cancellation."""
        return self.request_cancellation(session_id)

    # ---------------------------------------------------------------------------
    # Diagnostics & Lifecycle
    # ---------------------------------------------------------------------------

    def get_metrics(self) -> Dict[str, Any]:
        """Return point-in-time controller telemetry."""
        with self._lock:
            return {
                "active_sessions_count": self.session_manager.active_count,
                "active_turns_count": len(self._active_turns),
                "counters": copy.deepcopy(self._counters),
            }

    def shutdown(self) -> None:
        """Gracefully shut down the controller.

        Signals cancellation to all in-flight turns and closes all active sessions.
        """
        logger.info("Shutting down VoiceInteractionController '%s'...", self.name)
        with self._lock:
            # Signal cancellation on all active turns
            for sid, active in list(self._active_turns.items()):
                logger.debug("Signaling shutdown cancellation for session '%s'", sid)
                active["stop_event"].set()

            # Close all active sessions
            active_sids = self.session_manager.list_active_sessions()
            for sid in active_sids:
                try:
                    self.session_manager.close_session(sid, reason="Controller shutdown")
                    self._counters["sessions_closed"] += 1
                except Exception as exc:
                    logger.debug("Error closing session '%s' during shutdown: %s", sid, exc)

            self._active_turns.clear()

        logger.info("VoiceInteractionController '%s' shutdown complete", self.name)

    # ---------------------------------------------------------------------------
    # Internal Helpers
    # ---------------------------------------------------------------------------

    def _publish_event(
        self,
        event_type: EventType,
        correlation_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        """Publish lifecycle event safely without bubbling subscriber exceptions."""
        if not self.event_bus:
            return

        try:
            event = Event.create(
                event_type=event_type,
                source=self.name,
                payload=sanitize_container(dict(payload)),
                correlation_id=correlation_id,
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.warning("Failed to publish controller event '%s': %s", event_type.value, exc)

    def _sync_state_store(self, session: VoiceSession) -> None:
        """Update bounded StateStore with session telemetry."""
        if not self.state_store:
            return

        try:
            self.state_store.update_subsystem(
                "voice_controller",
                status="ready",
                metrics={
                    "active_sessions": self.session_manager.active_count,
                    "last_session_id": session.session_id,
                    "last_session_status": session.status.value,
                    "sessions_created": self._counters["sessions_created"],
                    "sessions_closed": self._counters["sessions_closed"],
                    "turns_submitted": self._counters["turns_submitted"],
                },
            )
        except Exception as exc:
            logger.debug("StateStore update failed in controller: %s", exc)
