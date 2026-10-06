"""KAISER — Investigation Reporting & Evidence Review Audit Models.

Provides strongly-typed, immutable data structures for structured, reproducible
analytical investigation reports, evidence matrices, observation matrices,
audit summaries, and reproducibility records.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support & analytical investigation only:
   Never outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE recommendations.
2. Absolutely no order execution fields: Prohibits action, side, order, entry/exit price,
   position size, leverage, stop-loss, take-profit, broker orders, expected profit, or trade probability.
3. No profitability claims: No numeric probability of profit or trade success metrics.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values (0, 0.0) are preserved.
5. Strict separation between Machine Evidence, Human Review, and Derived Analysis.
6. Immutability & Reproducibility: Every report retains comprehensive lineage to source session,
   opportunity candidate, priority record, evidence synthesis, and analysis runs.
7. Report versioning: Historical reports remain traceable; updates create linked revisions
   and mark predecessor reports as SUPERSEDED.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import math
import re
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.comparison.metrics import safe_float
from app.analysis.opportunities.investigation.models import (
    AnalyticalConclusion,
    ConclusionState,
    EvidenceReviewDisposition,
    EvidenceReviewRecord,
    InvestigationAuditRecord,
    InvestigationLimitation,
    InvestigationObservation,
    InvestigationReviewNote,
    InvestigationSession,
    InvestigationUncertainty,
    ObservationType,
    SessionState,
    SourceType,
    sanitize_reviewer_id,
)
from app.analysis.opportunities.models import (
    FORBIDDEN_VOICE_WORDS,
)
from app.core.exceptions import ValidationError
from app.market.provenance.models import (
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)

MAX_REPORT_ID_LENGTH: int = 128
MAX_TEXT_FIELD_LENGTH: int = 5000
MAX_DISCLAIMER_LENGTH: int = 2000
DEFAULT_MAX_REPORT_HISTORY: int = 100

PROHIBITED_REPORT_FIELDS: frozenset[str] = frozenset({
    "action",
    "side",
    "order",
    "entry",
    "exit",
    "quantity",
    "position_size",
    "leverage",
    "stop_loss",
    "take_profit",
    "broker_order",
    "execution_instruction",
    "target_price",
    "entry_price",
    "exit_price",
    "order_type",
    "trade_recommendation",
    "trade_signal",
    "buy_signal",
    "sell_signal",
    "expected_profit",
    "profit_probability",
    "trade_probability",
    "predicted_pnl",
    "win_rate",
})


def validate_no_prohibited_fields(data: Any, path: str = "") -> None:
    """Recursively validate that no trading or execution recommendation fields exist in a payload.

    Raises:
        ValidationError: If any prohibited field or recommendation keyword is detected.
    """
    if isinstance(data, Mapping):
        for k, v in data.items():
            field_name = str(k).strip().lower()
            current_path = f"{path}.{field_name}" if path else field_name
            if field_name in PROHIBITED_REPORT_FIELDS:
                raise ValidationError(
                    f"Prohibited trade recommendation field detected at '{current_path}'. "
                    f"KAISER is an evidence-grounded decision support system only and prohibits "
                    f"trade recommendations, execution instructions, or order fields."
                )
            validate_no_prohibited_fields(v, current_path)
    elif isinstance(data, (list, tuple, set, frozenset)):
        for idx, item in enumerate(data):
            current_path = f"{path}[{idx}]"
            validate_no_prohibited_fields(item, current_path)


class ReportStatus(str, Enum):
    """Deterministic status of an analytical investigation report."""

    GENERATED = "GENERATED"
    PARTIAL = "PARTIAL"
    INCOMPLETE = "INCOMPLETE"
    INVALID = "INVALID"
    SUPERSEDED = "SUPERSEDED"

    @classmethod
    def from_string(cls, val: Union[str, ReportStatus]) -> ReportStatus:
        """Parse report status safely from string or enum."""
        if isinstance(val, ReportStatus):
            return val
        if not val or not isinstance(val, str):
            return cls.INCOMPLETE
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.INCOMPLETE


class AttributionType(str, Enum):
    """Epistemological attribution distinguishing evidence origins."""

    MACHINE_GENERATED_EVIDENCE = "MACHINE_GENERATED_EVIDENCE"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    DERIVED_ANALYSIS = "DERIVED_ANALYSIS"

    @classmethod
    def from_string(cls, val: Union[str, AttributionType]) -> AttributionType:
        """Parse attribution type safely."""
        if isinstance(val, AttributionType):
            return val
        if not val or not isinstance(val, str):
            return cls.DERIVED_ANALYSIS
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.DERIVED_ANALYSIS


def validate_numerical_integrity(value: Any, field_name: str) -> None:
    """Validate numerical integrity following Phase 2 Step 18 rules.

    Rejects NaN, Infinity, and -Infinity. Preserves genuine 0 and 0.0.

    Args:
        value: Value to validate.
        field_name: Descriptive name for error messages.

    Raises:
        ValidationError: If value is NaN or infinite.
    """
    if isinstance(value, float):
        if math.isnan(value):
            raise ValidationError(f"Numerical integrity violation: '{field_name}' cannot be NaN.")
        if math.isinf(value):
            raise ValidationError(f"Numerical integrity violation: '{field_name}' cannot be Infinity.")


def compute_deterministic_report_id(
    session_id: str,
    candidate_id: str,
    priority_id: Optional[str] = None,
    source_report_id: Optional[str] = None,
    reproducibility_id: Optional[str] = None,
    version: int = 1,
) -> str:
    """Compute a deterministic, collision-resistant report identifier.

    Uses canonical JSON serialization and SHA-256. Does not rely on random UUIDs.

    Args:
        session_id: Source investigation session ID.
        candidate_id: Opportunity candidate ID.
        priority_id: Optional priority record ID.
        source_report_id: Optional source evidence synthesis report ID.
        reproducibility_id: Optional reproducibility fingerprint.
        version: Integer report version.

    Returns:
        Deterministic report ID string prefixed with 'rep_inv_'.
    """
    payload = {
        "candidate_id": str(candidate_id).strip(),
        "priority_id": str(priority_id).strip() if priority_id else "",
        "reproducibility_id": str(reproducibility_id).strip() if reproducibility_id else "",
        "session_id": str(session_id).strip(),
        "source_report_id": str(source_report_id).strip() if source_report_id else "",
        "version": int(version),
    }
    raw = json.dumps(payload, sort_keys=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"rep_inv_{digest}"


@dataclass(frozen=True)
class AuditSummary:
    """Immutable audit summary evaluating the integrity and completeness of the session trail."""

    total_events: int
    evidence_review_events: int
    observations_added: int
    conclusion_changes: int
    session_state_transitions: int
    reviewer_identifiers: Tuple[str, ...]
    first_event_timestamp: Optional[float] = None
    last_event_timestamp: Optional[float] = None
    sequence_integrity: bool = True
    audit_completeness: bool = True
    integrity_issues: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate audit summary fields and numerical safety."""
        for count_name in (
            "total_events",
            "evidence_review_events",
            "observations_added",
            "conclusion_changes",
            "session_state_transitions",
        ):
            val = getattr(self, count_name)
            if not isinstance(val, int) or val < 0:
                raise ValidationError(f"AuditSummary {count_name} must be a non-negative integer.")

        for ts_name in ("first_event_timestamp", "last_event_timestamp"):
            ts = getattr(self, ts_name)
            if ts is not None:
                validate_numerical_integrity(ts, ts_name)
                if ts < 0.0:
                    raise ValidationError(f"AuditSummary {ts_name} cannot be negative.")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize audit summary to dictionary."""
        return {
            "total_events": self.total_events,
            "evidence_review_events": self.evidence_review_events,
            "observations_added": self.observations_added,
            "conclusion_changes": self.conclusion_changes,
            "session_state_transitions": self.session_state_transitions,
            "reviewer_identifiers": list(self.reviewer_identifiers),
            "first_event_timestamp": self.first_event_timestamp,
            "last_event_timestamp": self.last_event_timestamp,
            "sequence_integrity": self.sequence_integrity,
            "audit_completeness": self.audit_completeness,
            "integrity_issues": list(self.integrity_issues),
        }


@dataclass(frozen=True)
class ReproducibilityRecord:
    """Immutable reproducibility metadata enabling another reviewer to reproduce the evidence trail."""

    source_analysis_run_ids: Tuple[str, ...]
    reproducibility_id: Optional[str]
    source_report_id: Optional[str]
    candidate_id: str
    priority_id: Optional[str]
    session_id: str
    generated_at: float
    config_fingerprint: Optional[str] = None
    is_reproducible: bool = True
    reproducibility_notes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate reproducibility parameters."""
        validate_numerical_integrity(self.generated_at, "generated_at")
        if self.generated_at < 0.0:
            raise ValidationError("ReproducibilityRecord generated_at cannot be negative.")
        if not self.candidate_id or not isinstance(self.candidate_id, str):
            raise ValidationError("ReproducibilityRecord candidate_id must be a non-empty string.")
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("ReproducibilityRecord session_id must be a non-empty string.")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize reproducibility record to dictionary."""
        return {
            "source_analysis_run_ids": list(self.source_analysis_run_ids),
            "reproducibility_id": self.reproducibility_id,
            "source_report_id": self.source_report_id,
            "candidate_id": self.candidate_id,
            "priority_id": self.priority_id,
            "session_id": self.session_id,
            "generated_at": self.generated_at,
            "config_fingerprint": self.config_fingerprint,
            "is_reproducible": self.is_reproducible,
            "reproducibility_notes": list(self.reproducibility_notes),
        }


@dataclass(frozen=True)
class EvidenceMatrixEntry:
    """Structured matrix row capturing machine evidence and human disposition."""

    evidence_id: str
    evidence_type: str
    status: str
    source: str
    reviewer_disposition: str
    observation_refs: Tuple[str, ...] = ()
    conflict_refs: Tuple[str, ...] = ()
    provenance_ref: Optional[str] = None
    quality_ref: Optional[str] = None
    notes: Optional[str] = None
    attribution: str = AttributionType.MACHINE_GENERATED_EVIDENCE.value

    def __post_init__(self) -> None:
        """Validate matrix entry."""
        if not self.evidence_id or not isinstance(self.evidence_id, str):
            raise ValidationError("EvidenceMatrixEntry evidence_id must be a non-empty string.")
        if not self.evidence_type or not isinstance(self.evidence_type, str):
            raise ValidationError("EvidenceMatrixEntry evidence_type must be a non-empty string.")

        # Prohibit trading directives in notes
        if self.notes:
            notes_lower = self.notes.lower()
            for forbidden in FORBIDDEN_VOICE_WORDS:
                if re.search(r"\b" + re.escape(forbidden) + r"\b", notes_lower):
                    raise ValidationError(
                        f"Forbidden trading or execution directive '{forbidden}' detected in evidence matrix notes."
                    )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize evidence matrix entry to dictionary."""
        return {
            "evidence_id": self.evidence_id,
            "evidence_type": self.evidence_type,
            "status": self.status,
            "source": self.source,
            "reviewer_disposition": self.reviewer_disposition,
            "observation_refs": list(self.observation_refs),
            "conflict_refs": list(self.conflict_refs),
            "provenance_ref": self.provenance_ref,
            "quality_ref": self.quality_ref,
            "notes": self.notes,
            "attribution": self.attribution,
        }


@dataclass(frozen=True)
class ObservationMatrixEntry:
    """Structured matrix row preserving human observations and strict citations."""

    observation_id: str
    observation_type: str
    statement: str
    evidence_refs: Tuple[str, ...]
    author_id: str
    source_type: str = AttributionType.HUMAN_REVIEW.value
    timestamp: float = 0.0

    def __post_init__(self) -> None:
        """Validate observation entry."""
        if not self.observation_id or not isinstance(self.observation_id, str):
            raise ValidationError("ObservationMatrixEntry observation_id must be a non-empty string.")
        if not self.statement or not isinstance(self.statement, str):
            raise ValidationError("ObservationMatrixEntry statement must be a non-empty string.")
        validate_numerical_integrity(self.timestamp, "timestamp")

        # Prohibit trading directives in statement
        stmt_lower = self.statement.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", stmt_lower):
                raise ValidationError(
                    f"Forbidden trading or execution directive '{forbidden}' detected in observation matrix statement."
                )

        # Epistemological safeguard: FACT requires non-empty evidence citations
        if self.observation_type == ObservationType.FACT.value:
            if not self.evidence_refs:
                raise ValidationError(
                    "ObservationMatrixEntry fact statement must cite verified evidence references."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize observation matrix entry to dictionary."""
        return {
            "observation_id": self.observation_id,
            "observation_type": self.observation_type,
            "statement": self.statement,
            "evidence_refs": list(self.evidence_refs),
            "author_id": self.author_id,
            "source_type": self.source_type,
            "timestamp": self.timestamp,
        }


@dataclass(frozen=True)
class InvestigationReportVoiceSummary:
    """Concise, deterministic structured speech text for voice synthesis without trading directives."""

    headline: str
    status_speech: str
    candidate_speech: str
    evidence_speech: str
    conclusion_speech: str
    caveats_speech: str
    full_speech_script: str

    def __post_init__(self) -> None:
        """Enforce strict speech safety: absolutely no trading directives or profitability claims."""
        script_lower = self.full_speech_script.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", script_lower):
                raise ValidationError(
                    f"Forbidden trading instruction or claim '{forbidden}' detected in report voice summary script. "
                    f"KAISER investigation report provides analytical decision support only."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to dictionary."""
        return {
            "headline": self.headline,
            "status_speech": self.status_speech,
            "candidate_speech": self.candidate_speech,
            "evidence_speech": self.evidence_speech,
            "conclusion_speech": self.conclusion_speech,
            "caveats_speech": self.caveats_speech,
            "full_speech_script": self.full_speech_script,
        }


@dataclass(frozen=True)
class InvestigationReport:
    """Immutable domain model of an Analytical Investigation Report.

    Provides comprehensive, auditable, reproducible answers to:
    1. What candidate was investigated?
    2. Why was it prioritized?
    3. What evidence was reviewed?
    4. Which evidence supported the analysis?
    5. Which evidence conflicted?
    6. What evidence was missing?
    7. What uncertainty remained?
    8. What limitations remained?
    9. What did the human reviewer conclude analytically?
    10. Can another reviewer reproduce the evidence trail?

    ABSOLUTELY NOT A TRADING REPORT OR EXECUTION INSTRUCTION.
    """

    report_id: str
    session_id: str
    candidate_id: str
    priority_id: Optional[str]
    report_status: ReportStatus
    generated_at: float
    reviewer_id: str
    candidate_summary: Dict[str, Any]
    priority_summary: Optional[Dict[str, Any]]
    evidence_summary: Dict[str, Any]
    supporting_evidence: Tuple[str, ...]
    conflicting_evidence: Tuple[str, ...]
    missing_evidence: Tuple[str, ...]
    uncertainty_summary: Tuple[str, ...]
    limitation_summary: Tuple[str, ...] = ()
    observations: Tuple[ObservationMatrixEntry, ...] = ()
    evidence_matrix: Tuple[EvidenceMatrixEntry, ...] = ()
    analytical_conclusion: Optional[Dict[str, Any]] = None
    audit_summary: Optional[AuditSummary] = None
    reproducibility_record: Optional[ReproducibilityRecord] = None
    source_report_id: Optional[str] = None
    source_analysis_run_ids: Tuple[str, ...] = ()
    reproducibility_id: Optional[str] = None
    report_provenance: Dict[str, Any] = field(default_factory=dict)
    version: int = 1
    parent_report_id: Optional[str] = None
    parent_session_id: Optional[str] = None
    voice_summary: Optional[InvestigationReportVoiceSummary] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    disclaimer: str = (
        "ANALYTICAL INVESTIGATION REPORT ONLY: This report records structured evidence review, "
        "audit verification, and analytical conclusions for decision support. It does NOT "
        "constitute a trading recommendation, execution directive, or profitability prediction. "
        "Settings.execution_locked remains True."
    )

    def __post_init__(self) -> None:
        """Validate core invariants, numerical safety, and prohibited fields."""
        if not self.report_id or not isinstance(self.report_id, str):
            raise ValidationError("InvestigationReport report_id must be a non-empty string.")
        if not self.session_id or not isinstance(self.session_id, str):
            raise ValidationError("InvestigationReport session_id must be a non-empty string.")
        if not self.candidate_id or not isinstance(self.candidate_id, str):
            raise ValidationError("InvestigationReport candidate_id must be a non-empty string.")

        validate_numerical_integrity(self.generated_at, "generated_at")
        if self.generated_at < 0.0:
            raise ValidationError("InvestigationReport generated_at cannot be negative.")

        if self.version < 1:
            raise ValidationError("InvestigationReport version must be an integer >= 1.")

        # Prohibit trading and execution fields recursively
        validate_no_prohibited_fields(self.candidate_summary, path="candidate_summary")
        if self.priority_summary:
            validate_no_prohibited_fields(self.priority_summary, path="priority_summary")
        validate_no_prohibited_fields(self.evidence_summary, path="evidence_summary")
        validate_no_prohibited_fields(self.metadata, path="metadata")
        validate_no_prohibited_fields(self.report_provenance, path="report_provenance")
        if self.analytical_conclusion:
            validate_no_prohibited_fields(self.analytical_conclusion, path="analytical_conclusion")

        # Freeze collections
        object.__setattr__(
            self,
            "supporting_evidence",
            tuple(str(e).strip() for e in self.supporting_evidence if str(e).strip()),
        )
        object.__setattr__(
            self,
            "conflicting_evidence",
            tuple(str(e).strip() for e in self.conflicting_evidence if str(e).strip()),
        )
        object.__setattr__(
            self,
            "missing_evidence",
            tuple(str(e).strip() for e in self.missing_evidence if str(e).strip()),
        )
        object.__setattr__(
            self,
            "uncertainty_summary",
            tuple(str(e).strip() for e in self.uncertainty_summary if str(e).strip()),
        )
        object.__setattr__(
            self,
            "limitation_summary",
            tuple(str(e).strip() for e in self.limitation_summary if str(e).strip()),
        )
        object.__setattr__(
            self,
            "source_analysis_run_ids",
            tuple(str(rid).strip() for rid in self.source_analysis_run_ids if str(rid).strip()),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize report to dictionary."""
        return {
            "report_id": self.report_id,
            "session_id": self.session_id,
            "candidate_id": self.candidate_id,
            "priority_id": self.priority_id,
            "report_status": self.report_status.value,
            "generated_at": self.generated_at,
            "reviewer_id": self.reviewer_id,
            "candidate_summary": sanitize_provenance_metadata(self.candidate_summary),
            "priority_summary": sanitize_provenance_metadata(self.priority_summary) if self.priority_summary else None,
            "evidence_summary": sanitize_provenance_metadata(self.evidence_summary),
            "supporting_evidence": list(self.supporting_evidence),
            "conflicting_evidence": list(self.conflicting_evidence),
            "missing_evidence": list(self.missing_evidence),
            "uncertainty_summary": list(self.uncertainty_summary),
            "limitation_summary": list(self.limitation_summary),
            "observations": [o.to_dict() for o in self.observations],
            "evidence_matrix": [e.to_dict() for e in self.evidence_matrix],
            "analytical_conclusion": sanitize_provenance_metadata(self.analytical_conclusion) if self.analytical_conclusion else None,
            "audit_summary": self.audit_summary.to_dict() if self.audit_summary else None,
            "reproducibility_record": self.reproducibility_record.to_dict() if self.reproducibility_record else None,
            "source_report_id": self.source_report_id,
            "source_analysis_run_ids": list(self.source_analysis_run_ids),
            "reproducibility_id": self.reproducibility_id,
            "report_provenance": sanitize_provenance_metadata(self.report_provenance),
            "version": self.version,
            "parent_report_id": self.parent_report_id,
            "parent_session_id": self.parent_session_id,
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "metadata": sanitize_provenance_metadata(self.metadata),
            "disclaimer": self.disclaimer,
        }
