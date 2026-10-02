"""KAISER — Voice Session Lifecycle Management.

Maintains stateful, thread-safe conversation sessions with strict state
transition validation, bounded memory guarantees, and language metadata tracking.
"""

from __future__ import annotations

import copy
import threading
import time
import uuid
from typing import Any, Dict, List, Mapping, Optional, Set

from app.core.exceptions import KaiserError, ValidationError
from app.core.logging_config import get_logger
from app.orchestrator.models import normalize_language
from app.voice.models import VoiceSessionStatus

logger = get_logger("voice.session")


class VoiceSessionError(KaiserError):
    """Raised when a voice session state transition or operation violates invariants."""


# Explicit allowed transitions between session states
VALID_SESSION_TRANSITIONS: Dict[VoiceSessionStatus, Set[VoiceSessionStatus]] = {
    VoiceSessionStatus.ACTIVE: {
        VoiceSessionStatus.PROCESSING,
        VoiceSessionStatus.IDLE,
        VoiceSessionStatus.COMPLETED,
        VoiceSessionStatus.CLOSED,
        VoiceSessionStatus.FAILED,
    },
    VoiceSessionStatus.PROCESSING: {
        VoiceSessionStatus.ACTIVE,
        VoiceSessionStatus.COMPLETED,
        VoiceSessionStatus.FAILED,
        VoiceSessionStatus.CLOSED,
    },
    VoiceSessionStatus.IDLE: {
        VoiceSessionStatus.ACTIVE,
        VoiceSessionStatus.CLOSED,
    },
    VoiceSessionStatus.FAILED: {
        VoiceSessionStatus.ACTIVE,  # Recovery
        VoiceSessionStatus.CLOSED,
    },
    VoiceSessionStatus.COMPLETED: {
        VoiceSessionStatus.ACTIVE,  # Recovery / consecutive turns
        VoiceSessionStatus.PROCESSING,
        VoiceSessionStatus.CLOSED,
    },
    VoiceSessionStatus.CLOSED: set(),  # Terminal state: cannot transition out
}


class VoiceSession:
    """Thread-safe, stateful voice interaction session.

    Attributes:
        session_id: Unique session identifier.
        status: Current VoiceSessionStatus.
        language: Selected or negotiated language tag.
        created_at: POSIX UTC session creation timestamp.
        updated_at: POSIX UTC timestamp of last activity.
        metadata: Bounded contextual and operational metadata.
    """

    def __init__(
        self,
        session_id: str,
        language: str = "en",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if not session_id or not isinstance(session_id, str) or not session_id.strip():
            raise ValidationError("session_id must be a non-empty string.")

        self.session_id = session_id.strip()
        self._lock = threading.RLock()
        self._status = VoiceSessionStatus.ACTIVE
        self._language = normalize_language(language)
        self._created_at = time.time()
        self._updated_at = self._created_at
        self._metadata: Dict[str, Any] = copy.deepcopy(dict(metadata or {}))

    @property
    def status(self) -> VoiceSessionStatus:
        """Current session operational state."""
        with self._lock:
            return self._status

    @property
    def language(self) -> str:
        """Configured session language tag."""
        with self._lock:
            return self._language

    @property
    def created_at(self) -> float:
        """POSIX UTC creation timestamp."""
        return self._created_at

    @property
    def updated_at(self) -> float:
        """POSIX UTC timestamp of most recent activity."""
        with self._lock:
            return self._updated_at

    @property
    def is_active(self) -> bool:
        """Check if session is currently active or processing."""
        with self._lock:
            return self._status in (VoiceSessionStatus.ACTIVE, VoiceSessionStatus.PROCESSING)

    @property
    def is_closed(self) -> bool:
        """Check if session has reached terminal CLOSED state."""
        with self._lock:
            return self._status == VoiceSessionStatus.CLOSED

    def transition_to(self, new_status: VoiceSessionStatus, reason: str = "") -> None:
        """Transition session to a new state with strict transition validation.

        Args:
            new_status: Target VoiceSessionStatus.
            reason: Optional explanation for audit logging.

        Raises:
            VoiceSessionError: If the transition is illegal from current status.
        """
        if not isinstance(new_status, VoiceSessionStatus):
            new_status = VoiceSessionStatus(str(new_status))

        with self._lock:
            if new_status == self._status:
                return  # No-op re-entry

            allowed = VALID_SESSION_TRANSITIONS.get(self._status, set())
            if new_status not in allowed:
                err_msg = (
                    f"Illegal session transition: cannot transition from "
                    f"'{self._status.value}' to '{new_status.value}' [session_id={self.session_id}]."
                )
                logger.error(err_msg)
                raise VoiceSessionError(err_msg)

            old_status = self._status
            self._status = new_status
            self._updated_at = time.time()

            logger.info(
                "VoiceSession '%s' transitioned: %s -> %s (reason: %s)",
                self.session_id,
                old_status.value,
                new_status.value,
                reason or "normal lifecycle",
            )

    def set_language(self, new_language: str) -> None:
        """Update session language metadata.

        Args:
            new_language: Valid language string.
        """
        norm = normalize_language(new_language)
        with self._lock:
            self._language = norm
            self._updated_at = time.time()
            logger.debug("VoiceSession '%s' language updated to '%s'", self.session_id, norm)

    def update_metadata(self, key: str, value: Any) -> None:
        """Set or update a bounded metadata entry under session lock."""
        with self._lock:
            self._metadata[key] = copy.deepcopy(value)
            self._updated_at = time.time()

    def get_metadata(self) -> Dict[str, Any]:
        """Return a deep copy of current session metadata."""
        with self._lock:
            return copy.deepcopy(self._metadata)

    def close(self, reason: str = "Session closed") -> None:
        """Convenience method to close the session gracefully."""
        self.transition_to(VoiceSessionStatus.CLOSED, reason=reason)

    def recover(self, reason: str = "Session recovered") -> None:
        """Recover a failed or completed session back to ACTIVE status."""
        with self._lock:
            if self._status in (VoiceSessionStatus.FAILED, VoiceSessionStatus.COMPLETED):
                self.transition_to(VoiceSessionStatus.ACTIVE, reason=reason)
            elif self._status in (VoiceSessionStatus.ACTIVE, VoiceSessionStatus.IDLE):
                pass
            elif self._status == VoiceSessionStatus.CLOSED:
                raise VoiceSessionError(f"Cannot recover closed session '{self.session_id}'.")
            elif self._status == VoiceSessionStatus.PROCESSING:
                raise VoiceSessionError(f"Cannot recover session '{self.session_id}' while actively processing.")

    def to_dict(self) -> Dict[str, Any]:
        """Return a point-in-time dictionary snapshot of session state."""
        with self._lock:
            return {
                "session_id": self.session_id,
                "status": self._status.value,
                "language": self._language,
                "created_at": self._created_at,
                "updated_at": self._updated_at,
                "is_active": self.is_active,
                "is_closed": self.is_closed,
                "metadata": copy.deepcopy(self._metadata),
            }


class VoiceSessionManager:
    """Thread-safe manager enforcing bounded active session retention."""

    def __init__(self, max_sessions: int = 100) -> None:
        self.max_sessions = max_sessions
        self._lock = threading.RLock()
        self._sessions: Dict[str, VoiceSession] = {}

    def create_session(
        self,
        session_id: Optional[str] = None,
        language: str = "en",
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> VoiceSession:
        """Create and track a new VoiceSession.

        Args:
            session_id: Optional session identifier.
            language: Initial language tag.
            metadata: Initial operational metadata.

        Returns:
            VoiceSession: The newly initialized session.
        """
        sid = (session_id or uuid.uuid4().hex).strip()

        with self._lock:
            # If session already exists, return existing if active
            if sid in self._sessions:
                existing = self._sessions[sid]
                if not existing.is_closed:
                    return existing

            # Enforce bounded session retention: purge oldest closed sessions if at capacity
            if len(self._sessions) >= self.max_sessions:
                self._purge_stale_sessions()

            session = VoiceSession(session_id=sid, language=language, metadata=metadata)
            self._sessions[sid] = session
            logger.info("Created VoiceSession '%s' [lang=%s]", sid, session.language)
            return session

    def get_session(self, session_id: str) -> Optional[VoiceSession]:
        """Retrieve an active or existing session by ID."""
        with self._lock:
            return self._sessions.get(session_id.strip())

    def close_session(self, session_id: str, reason: str = "User request") -> bool:
        """Close an active session."""
        with self._lock:
            session = self._sessions.get(session_id.strip())
            if session and not session.is_closed:
                session.close(reason=reason)
                return True
            return False

    def recover_session(self, session_id: str, reason: str = "Session recovered") -> bool:
        """Recover a failed or completed session back to ACTIVE status."""
        with self._lock:
            session = self._sessions.get(session_id.strip())
            if session:
                try:
                    session.recover(reason=reason)
                    return True
                except VoiceSessionError:
                    return False
            return False

    def list_active_sessions(self) -> List[str]:
        """Return IDs of all currently active sessions."""
        with self._lock:
            return [sid for sid, s in self._sessions.items() if s.is_active]

    @property
    def active_count(self) -> int:
        """Count of active sessions."""
        with self._lock:
            return sum(1 for s in self._sessions.values() if s.is_active)

    def _purge_stale_sessions(self) -> None:
        """Remove closed sessions to free capacity."""
        closed_sids = [sid for sid, s in self._sessions.items() if s.is_closed]
        for sid in closed_sids:
            del self._sessions[sid]
        # If still over capacity, remove oldest sessions
        if len(self._sessions) >= self.max_sessions:
            sorted_sids = sorted(self._sessions.keys(), key=lambda k: self._sessions[k].updated_at)
            remove_count = len(self._sessions) - self.max_sessions + 1
            for sid in sorted_sids[:remove_count]:
                del self._sessions[sid]

    def clear(self) -> None:
        """Clear all session tracking."""
        with self._lock:
            self._sessions.clear()
