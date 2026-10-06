"""KAISER — Human Investigation Session & Evidence Review Workflow Models.

Provides strongly-typed, immutable data structures for structured human
investigation sessions, evidence reviews, observation recording, analytical
conclusions, and audit events.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support & investigation only:
   Never outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE recommendations.
2. Absolutely no order execution fields: Prohibits action, side, order, entry/exit price,
   position size, leverage, stop-loss, take-profit, broker orders, expected profit, or trade probability.
3. No profitability claims: No numeric probability of profit or trade success metrics.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
5. Strict separation between Machine Evidence, Human Review, and Derived Analysis.
6. Strict separation between empirical FACT (must cite verified evidence refs) and INTERPRETATION.
7. Terminal session immutability: Completed sessions cannot be silently mutated; follow-up
   reviews must be opened as linked revisions preserving audit lineage.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import math
import re
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.comparison.metrics import safe_float
from app.analysis.opportunities.prioritization.models import (
    FORBIDDEN_VOICE_WORDS,
    PROHIBITED_RECOMMENDATION_FIELDS,
    validate_no_prohibited_fields,
)
from app.core.exceptions import ValidationError
from app.market.provenance.models import (
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)

MAX_STATEMENT_LENGTH: int = 2000
MAX_NOTE_LENGTH: int = 5000
MAX_RATIONALE_LENGTH: int = 2000
MAX_REVIEWER_ID_LENGTH: int = 64
MIN_REVIEWER_ID_LENGTH: int = 2
DEFAULT_MAX_SESSION_HISTORY: int = 100

SAFE_REVIEWER_ID_PATTERN: re.Pattern = re.compile(r"^[a-zA-Z0-9_\-\.]{2,64}$")


def sanitize_reviewer_id(reviewer_id: Optional[str]) -> str:
    """Validate and sanitize a human reviewer identifier.

    Enforces length bounds, prohibits path traversal, null bytes, control characters,
    and credential patterns.

    Args:
        reviewer_id: Raw input reviewer identifier string.

    Returns:
        Sanitized reviewer ID string.

    Raises:
        ValidationError: If the reviewer ID is malformed, invalid, or violates safety bounds.
    """
    if reviewer_id is None:
        return "analyst_unassigned"
    if not isinstance(reviewer_id, str):
        raise ValidationError(f"Reviewer ID must be a string, got {type(reviewer_id).__name__}.")
    
    clean = reviewer_id.strip()
    if not clean:
        return "analyst_unassigned"

    if len(clean) < MIN_REVIEWER_ID_LENGTH or len(clean) > MAX_REVIEWER_ID_LENGTH:
        raise ValidationError(
            f"Reviewer ID length must be between {MIN_REVIEWER_ID_LENGTH} and {MAX_REVIEWER_ID_LENGTH} characters."
        )

    # Rejection of control chars, null bytes, path traversal
    if "\0" in clean or "\n" in clean or "\r" in clean or "\t" in clean:
        raise ValidationError("Reviewer ID contains illegal control characters or null bytes.")
    if ".." in clean or "/" in clean or "\\" in clean:
        raise ValidationError("Reviewer ID contains illegal path traversal characters ('..', '/', '\\').")

    if not SAFE_REVIEWER_ID_PATTERN.match(clean):
        raise ValidationError(
            f"Reviewer ID '{clean}' contains invalid characters. Only alphanumeric, underscores, hyphens, and dots allowed."
        )

    # Rejection of credential-like patterns
    lower = clean.lower()
    for forbidden in ("secret", "token", "password", "api_key", "bearer"):
        if forbidden in lower:
            raise ValidationError(f"Reviewer ID must not contain credential terminology ('{forbidden}').")

    return clean


class SessionState(str, Enum):
    """Deterministic lifecycle state of a human investigation session."""

    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    REQUIRES_MORE_EVIDENCE = "REQUIRES_MORE_EVIDENCE"
    REVIEW_COMPLETE = "REVIEW_COMPLETE"
    INVALIDATED = "INVALIDATED"
    CANCELLED = "CANCELLED"

    @classmethod
    def from_string(cls, val: Union[str, SessionState]) -> SessionState:
        """Parse session state safely from string or enum."""
        if isinstance(val, SessionState):
            return val
        if not val or not isinstance(val, str):
            return cls.CREATED
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.CREATED


VALID_SESSION_TRANSITIONS: Dict[SessionState, Set[SessionState]] = {
    SessionState.CREATED: {SessionState.ACTIVE, SessionState.CANCELLED},
    SessionState.ACTIVE: {
        SessionState.PAUSED,
        SessionState.REQUIRES_MORE_EVIDENCE,
        SessionState.REVIEW_COMPLETE,
        SessionState.INVALIDATED,
        SessionState.CANCELLED,
    },
    SessionState.PAUSED: {
        SessionState.ACTIVE,
        SessionState.CANCELLED,
        SessionState.INVALIDATED,
    },
    SessionState.REQUIRES_MORE_EVIDENCE: {
        SessionState.ACTIVE,
        SessionState.CANCELLED,
        SessionState.INVALIDATED,
    },
    SessionState.REVIEW_COMPLETE: set(),  # Terminal: Completed sessions are immutable
    SessionState.INVALIDATED: set(),      # Terminal
    SessionState.CANCELLED: set(),        # Terminal
}


class ObservationType(str, Enum):
    """Classification of an analyst observation during evidence review.
    
    Maintains strict epistemological separation between empirical FACT and subjective INTERPRETATION.
    """

    FACT = "FACT"
    INTERPRETATION = "INTERPRETATION"
    UNCERTAINTY = "UNCERTAINTY"
    LIMITATION = "LIMITATION"
    QUESTION = "QUESTION"

    @classmethod
    def from_string(cls, val: Union[str, ObservationType]) -> ObservationType:
        """Parse observation type safely."""
        if isinstance(val, ObservationType):
            return val
        if not val or not isinstance(val, str):
            return cls.INTERPRETATION
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.INTERPRETATION


class EvidenceReviewDisposition(str, Enum):
    """Analyst determination for a specific evaluated evidence item."""

    REVIEWED = "REVIEWED"
    SUPPORTING = "SUPPORTING"
    CONFLICTING = "CONFLICTING"
    INCONCLUSIVE = "INCONCLUSIVE"
    MISSING = "MISSING"
    STALE = "STALE"
    INVALID = "INVALID"

    @classmethod
    def from_string(cls, val: Union[str, EvidenceReviewDisposition]) -> EvidenceReviewDisposition:
        """Parse evidence review disposition safely."""
        if isinstance(val, EvidenceReviewDisposition):
            return val
        if not val or not isinstance(val, str):
            return cls.REVIEWED
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.REVIEWED


class SourceType(str, Enum):
    """Provenance origin of investigation components."""

    MACHINE_EVIDENCE = "MACHINE_EVIDENCE"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    DERIVED_ANALYSIS = "DERIVED_ANALYSIS"


class ConclusionState(str, Enum):
    """Non-prescriptive analytical conclusion reached at end of investigation.
    
    Reflects only the sufficiency and integrity of analytical evidence.
    NEVER indicates a trading recommendation or execution order.
    """

    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    EVIDENCE_REQUIRES_FURTHER_REVIEW = "EVIDENCE_REQUIRES_FURTHER_REVIEW"
    EVIDENCE_REMAINS_CONFLICTING = "EVIDENCE_REMAINS_CONFLICTING"
    EVIDENCE_SUPPORTS_FURTHER_ANALYSIS = "EVIDENCE_SUPPORTS_FURTHER_ANALYSIS"
    REVIEW_COMPLETED_WITH_LIMITATIONS = "REVIEW_COMPLETED_WITH_LIMITATIONS"
    INVALIDATED_BY_EVIDENCE = "INVALIDATED_BY_EVIDENCE"

    @classmethod
    def from_string(cls, val: Union[str, ConclusionState]) -> ConclusionState:
        """Parse conclusion state safely."""
        if isinstance(val, ConclusionState):
            return val
        if not val or not isinstance(val, str):
            return cls.INSUFFICIENT_EVIDENCE
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.INSUFFICIENT_EVIDENCE


@dataclass(frozen=True)
class InvestigationVoiceSummary:
    """Concise, deterministic structured speech text for voice synthesis without trading directives."""

    headline: str
    status_speech: str
    conclusion_speech: str
    caveats_speech: str
    full_speech_script: str

    def __post_init__(self) -> None:
        """Enforce strict speech safety: absolutely no trading directives or profitability claims."""
        script_lower = self.full_speech_script.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", script_lower):
                raise ValidationError(
                    f"Forbidden trading instruction or claim '{forbidden}' detected in voice summary script. "
                    f"KAISER investigation workflow provides analytical decision support only."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to dictionary."""
        return {
            "headline": self.headline,
            "status_speech": self.status_speech,
            "conclusion_speech": self.conclusion_speech,
            "caveats_speech": self.caveats_speech,
            "full_speech_script": self.full_speech_script,
        }


@dataclass(frozen=True)
class InvestigationObservation:
    """Immutable record of an analyst observation recorded during an active investigation session.
    
    Strictly distinguishes FACT from INTERPRETATION. If marked as FACT, verified evidence
    references MUST be cited.
    """

    observation_id: str
    session_id: str
    observation_type: ObservationType
    statement: str
    evidence_refs: Tuple[str, ...]
    source_type: SourceType = SourceType.HUMAN_REVIEW
    reviewer_id: str = "analyst_unassigned"
    created_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate observation invariants and safety rules."""
        if not self.observation_id or not isinstance(self.observation_id, str):
            raise ValidationError("InvestigationObservation observation_id must be a non-empty string.")
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("InvestigationObservation session_id must be a non-empty string.")
        if not self.statement or not isinstance(self.statement, str):
            raise ValidationError("InvestigationObservation statement must be a non-empty string.")
        
        clean_statement = self.statement.strip()
        if len(clean_statement) > MAX_STATEMENT_LENGTH:
            raise ValidationError(
                f"InvestigationObservation statement exceeds maximum length of {MAX_STATEMENT_LENGTH} characters."
            )

        # Prohibit trading directives in observation statement
        stmt_lower = clean_statement.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", stmt_lower):
                raise ValidationError(
                    f"Forbidden trading or execution directive '{forbidden}' detected in observation statement."
                )

        # Epistemological safeguard: FACT requires non-empty evidence citations
        if self.observation_type == ObservationType.FACT:
            if not self.evidence_refs:
                raise ValidationError(
                    "Epistemological rule violation: ObservationType.FACT requires at least one verified evidence reference. "
                    "Unsupported analyst observations must be designated as INTERPRETATION, QUESTION, or UNCERTAINTY."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize observation to dictionary."""
        return {
            "observation_id": self.observation_id,
            "session_id": self.session_id,
            "observation_type": self.observation_type.value,
            "statement": self.statement,
            "evidence_refs": list(self.evidence_refs),
            "source_type": self.source_type.value,
            "reviewer_id": self.reviewer_id,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class EvidenceReviewRecord:
    """Immutable determination by an analyst on an individual piece of evidence."""

    review_id: str
    session_id: str
    evidence_id: str
    disposition: EvidenceReviewDisposition
    notes: str
    reviewer_id: str = "analyst_unassigned"
    source_type: SourceType = SourceType.HUMAN_REVIEW
    created_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate review record invariants."""
        if not self.review_id or not isinstance(self.review_id, str):
            raise ValidationError("EvidenceReviewRecord review_id must be a non-empty string.")
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("EvidenceReviewRecord session_id must be a non-empty string.")
        if not self.evidence_id or not isinstance(self.evidence_id, str):
            raise ValidationError("EvidenceReviewRecord evidence_id must be a non-empty string.")
        if len(self.notes) > MAX_NOTE_LENGTH:
            raise ValidationError(
                f"EvidenceReviewRecord notes exceed maximum length of {MAX_NOTE_LENGTH} characters."
            )

        # Prohibit trading directives in notes
        notes_lower = self.notes.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", notes_lower):
                raise ValidationError(
                    f"Forbidden trading or execution directive '{forbidden}' detected in evidence review notes."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize review record to dictionary."""
        return {
            "review_id": self.review_id,
            "session_id": self.session_id,
            "evidence_id": self.evidence_id,
            "disposition": self.disposition.value,
            "notes": self.notes,
            "reviewer_id": self.reviewer_id,
            "source_type": self.source_type.value,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class InvestigationUncertainty:
    """Explicitly documented epistemic uncertainty identified during investigation."""

    uncertainty_id: str
    session_id: str
    description: str
    severity: str
    evidence_refs: Tuple[str, ...] = ()
    source_type: SourceType = SourceType.HUMAN_REVIEW
    reviewer_id: str = "analyst_unassigned"
    created_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate uncertainty record."""
        if not self.uncertainty_id or not isinstance(self.uncertainty_id, str):
            raise ValidationError("InvestigationUncertainty uncertainty_id must be a non-empty string.")
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("InvestigationUncertainty session_id must be a non-empty string.")
        if not self.description or not isinstance(self.description, str):
            raise ValidationError("InvestigationUncertainty description must be a non-empty string.")
        if len(self.description) > MAX_STATEMENT_LENGTH:
            raise ValidationError(
                f"InvestigationUncertainty description exceeds maximum length of {MAX_STATEMENT_LENGTH} characters."
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize uncertainty record to dictionary."""
        return {
            "uncertainty_id": self.uncertainty_id,
            "session_id": self.session_id,
            "description": self.description,
            "severity": self.severity,
            "evidence_refs": list(self.evidence_refs),
            "source_type": self.source_type.value,
            "reviewer_id": self.reviewer_id,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class InvestigationLimitation:
    """Explicitly documented analytical constraint or scope limitation."""

    limitation_id: str
    session_id: str
    description: str
    category: str
    source_type: SourceType = SourceType.HUMAN_REVIEW
    reviewer_id: str = "analyst_unassigned"
    created_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate limitation record."""
        if not self.limitation_id or not isinstance(self.limitation_id, str):
            raise ValidationError("InvestigationLimitation limitation_id must be a non-empty string.")
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("InvestigationLimitation session_id must be a non-empty string.")
        if not self.description or not isinstance(self.description, str):
            raise ValidationError("InvestigationLimitation description must be a non-empty string.")
        if len(self.description) > MAX_STATEMENT_LENGTH:
            raise ValidationError(
                f"InvestigationLimitation description exceeds maximum length of {MAX_STATEMENT_LENGTH} characters."
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize limitation record to dictionary."""
        return {
            "limitation_id": self.limitation_id,
            "session_id": self.session_id,
            "description": self.description,
            "category": self.category,
            "source_type": self.source_type.value,
            "reviewer_id": self.reviewer_id,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class InvestigationReviewNote:
    """Structured analyst commentary recorded during an investigation."""

    note_id: str
    session_id: str
    reviewer_id: str
    content: str
    created_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate review note."""
        if not self.note_id or not isinstance(self.note_id, str):
            raise ValidationError("InvestigationReviewNote note_id must be a non-empty string.")
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("InvestigationReviewNote session_id must be a non-empty string.")
        if not self.content or not isinstance(self.content, str):
            raise ValidationError("InvestigationReviewNote content must be a non-empty string.")
        if len(self.content) > MAX_NOTE_LENGTH:
            raise ValidationError(
                f"InvestigationReviewNote content exceeds maximum length of {MAX_NOTE_LENGTH} characters."
            )

        # Prohibit trading directives in notes
        cnt_lower = self.content.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", cnt_lower):
                raise ValidationError(
                    f"Forbidden trading or execution directive '{forbidden}' detected in review note."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize review note to dictionary."""
        return {
            "note_id": self.note_id,
            "session_id": self.session_id,
            "reviewer_id": self.reviewer_id,
            "content": self.content,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class AnalyticalConclusion:
    """Immutable analytical conclusion terminating or pausing an investigation session.
    
    ABSOLUTELY NOT A TRADING RECOMMENDATION OR EXECUTION DIRECTIVE.
    """

    conclusion_id: str
    session_id: str
    conclusion_state: ConclusionState
    rationale: str
    supporting_evidence_count: int
    conflicting_evidence_count: int
    uncertainty_count: int
    limitation_count: int
    reviewer_id: str = "analyst_unassigned"
    source_type: SourceType = SourceType.HUMAN_REVIEW
    created_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate analytical conclusion invariants."""
        if not self.conclusion_id or not isinstance(self.conclusion_id, str):
            raise ValidationError("AnalyticalConclusion conclusion_id must be a non-empty string.")
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("AnalyticalConclusion session_id must be a non-empty string.")
        if not self.rationale or not isinstance(self.rationale, str):
            raise ValidationError("AnalyticalConclusion rationale must be a non-empty string.")
        if len(self.rationale) > MAX_RATIONALE_LENGTH:
            raise ValidationError(
                f"AnalyticalConclusion rationale exceeds maximum length of {MAX_RATIONALE_LENGTH} characters."
            )

        # Numerical safety: Counts cannot be negative, NaN or Inf
        for count_name in (
            "supporting_evidence_count",
            "conflicting_evidence_count",
            "uncertainty_count",
            "limitation_count",
        ):
            val = getattr(self, count_name)
            if not isinstance(val, int) or val < 0:
                raise ValidationError(f"AnalyticalConclusion {count_name} must be a non-negative integer.")

        # Prohibit trading directives in rationale
        rat_lower = self.rationale.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", rat_lower):
                raise ValidationError(
                    f"Forbidden trading or execution directive '{forbidden}' detected in conclusion rationale."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize conclusion to dictionary."""
        return {
            "conclusion_id": self.conclusion_id,
            "session_id": self.session_id,
            "conclusion_state": self.conclusion_state.value,
            "rationale": self.rationale,
            "supporting_evidence_count": self.supporting_evidence_count,
            "conflicting_evidence_count": self.conflicting_evidence_count,
            "uncertainty_count": self.uncertainty_count,
            "limitation_count": self.limitation_count,
            "reviewer_id": self.reviewer_id,
            "source_type": self.source_type.value,
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class InvestigationAuditRecord:
    """Immutable audit trail entry recording an atomic mutation on an investigation session."""

    audit_id: str
    session_id: str
    event_type: str
    timestamp: float
    reviewer_id: Optional[str]
    previous_state: Optional[str]
    new_state: Optional[str]
    action_summary: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize audit record to dictionary."""
        return {
            "audit_id": self.audit_id,
            "session_id": self.session_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "reviewer_id": self.reviewer_id,
            "previous_state": self.previous_state,
            "new_state": self.new_state,
            "action_summary": self.action_summary,
            "metadata": sanitize_provenance_metadata(self.metadata),
        }


@dataclass(frozen=True)
class InvestigationSession:
    """Immutable domain model of a Human Investigation Session.
    
    Provides complete traceability, lifecycle auditability, and structured human review
    for an OpportunityCandidate.
    
    ABSOLUTELY NOT A TRADING RECOMMENDATION OR EXECUTION DIRECTIVE.
    """

    session_id: str
    candidate_id: str
    priority_id: Optional[str]
    status: SessionState
    created_at: float
    started_at: Optional[float] = None
    updated_at: float = 0.0
    closed_at: Optional[float] = None
    reviewer_id: Optional[str] = None
    evidence_reviews: Tuple[EvidenceReviewRecord, ...] = ()
    observations: Tuple[InvestigationObservation, ...] = ()
    uncertainties: Tuple[InvestigationUncertainty, ...] = ()
    limitations: Tuple[InvestigationLimitation, ...] = ()
    review_notes: Tuple[InvestigationReviewNote, ...] = ()
    analytical_conclusion: Optional[AnalyticalConclusion] = None
    known_evidence_ids: Tuple[str, ...] = ()
    source_report_id: Optional[str] = None
    source_analysis_run_ids: Tuple[str, ...] = ()
    reproducibility_id: Optional[str] = None
    parent_session_id: Optional[str] = None
    revision: int = 1
    voice_summary: Optional[InvestigationVoiceSummary] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    disclaimer: str = (
        "ANALYTICAL INVESTIGATION ONLY: This session records human evidence review and analytical "
        "conclusions only. It does NOT provide trading recommendations, execution instructions, "
        "or profitability predictions. Settings.execution_locked remains True."
    )

    def __post_init__(self) -> None:
        """Validate core invariants and safety rules."""
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("InvestigationSession session_id must be a non-empty string.")
        if not self.candidate_id or not isinstance(self.candidate_id, str):
            raise ValidationError("InvestigationSession candidate_id must be a non-empty string.")

        # Rejection of prohibited recommendation fields anywhere in metadata
        validate_no_prohibited_fields(self.metadata, path="metadata")

        # Numerical safety on timestamps
        for ts_name in ("created_at", "started_at", "updated_at", "closed_at"):
            ts_val = getattr(self, ts_name)
            if ts_val is not None:
                if not isinstance(ts_val, (int, float)):
                    raise ValidationError(f"Timestamp '{ts_name}' must be a float or int.")
                if math.isnan(ts_val) or math.isinf(ts_val):
                    raise ValidationError(f"Timestamp '{ts_name}' cannot be NaN or Infinity.")
                if ts_val < 0.0:
                    raise ValidationError(f"Timestamp '{ts_name}' cannot be negative.")

        if self.revision < 1:
            raise ValidationError("InvestigationSession revision must be an integer >= 1.")

    def is_terminal(self) -> bool:
        """Return True if the session has reached a terminal, immutable state."""
        return self.status in (
            SessionState.REVIEW_COMPLETE,
            SessionState.INVALIDATED,
            SessionState.CANCELLED,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize session to dictionary."""
        return {
            "session_id": self.session_id,
            "candidate_id": self.candidate_id,
            "priority_id": self.priority_id,
            "status": self.status.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "closed_at": self.closed_at,
            "reviewer_id": self.reviewer_id,
            "evidence_reviews": [r.to_dict() for r in self.evidence_reviews],
            "observations": [o.to_dict() for o in self.observations],
            "uncertainties": [u.to_dict() for u in self.uncertainties],
            "limitations": [l.to_dict() for l in self.limitations],
            "review_notes": [n.to_dict() for n in self.review_notes],
            "analytical_conclusion": self.analytical_conclusion.to_dict() if self.analytical_conclusion else None,
            "known_evidence_ids": list(self.known_evidence_ids),
            "source_report_id": self.source_report_id,
            "source_analysis_run_ids": list(self.source_analysis_run_ids),
            "reproducibility_id": self.reproducibility_id,
            "parent_session_id": self.parent_session_id,
            "revision": self.revision,
            "is_terminal": self.is_terminal(),
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "metadata": sanitize_provenance_metadata(self.metadata),
            "disclaimer": self.disclaimer,
        }
