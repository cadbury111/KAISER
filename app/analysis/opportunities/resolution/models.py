"""KAISER — Investigation Discrepancy Resolution & Evidence Reconciliation Models.

Phase 2, Step 29: Deterministic, immutable domain models for human-driven
discrepancy resolution, evidence reconciliation records, and state transitions.

CRITICAL INVARIANTS:
- KAISER remains an evidence-grounded analytical decision-support system.
- The system NEVER automatically selects a winning report or resolves discrepancies.
- All resolutions are explicitly human-reviewed decisions.
- Strictly prohibited: BUY/SELL/HOLD, trade execution, broker integration,
  position sizing, leverage, profitability prediction, or trade ranking.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from app.core.exceptions import ValidationError

# Regexp pattern for safe identifier validation (prevents path traversal and injection)
SAFE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.:]+$")

# Prohibited recommendation and execution keywords
PROHIBITED_RECOMMENDATION_FIELDS: Tuple[str, ...] = (
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
    "expected_profit",
    "profit_probability",
    "trade_probability",
    "predicted_pnl",
    "trade_signal",
    "trade_ranking",
    "profitability_ranking",
)

DEFAULT_MAX_RESOLUTION_HISTORY: int = 100


class ResolutionState(str, Enum):
    """Explicit state machine for investigation discrepancy resolution."""

    OPEN = "OPEN"
    UNDER_REVIEW = "UNDER_REVIEW"
    EVIDENCE_REQUESTED = "EVIDENCE_REQUESTED"
    RECONCILIATION_RECORDED = "RECONCILIATION_RECORDED"
    RESOLVED = "RESOLVED"
    REJECTED = "REJECTED"
    DEFERRED = "DEFERRED"
    INVALIDATED = "INVALIDATED"


TERMINAL_RESOLUTION_STATES: Set[ResolutionState] = {
    ResolutionState.RESOLVED,
    ResolutionState.REJECTED,
    ResolutionState.INVALIDATED,
}

VALID_RESOLUTION_TRANSITIONS: Mapping[ResolutionState, Tuple[ResolutionState, ...]] = {
    ResolutionState.OPEN: (
        ResolutionState.UNDER_REVIEW,
        ResolutionState.EVIDENCE_REQUESTED,
        ResolutionState.DEFERRED,
        ResolutionState.INVALIDATED,
    ),
    ResolutionState.UNDER_REVIEW: (
        ResolutionState.EVIDENCE_REQUESTED,
        ResolutionState.RECONCILIATION_RECORDED,
        ResolutionState.DEFERRED,
        ResolutionState.INVALIDATED,
    ),
    ResolutionState.EVIDENCE_REQUESTED: (
        ResolutionState.UNDER_REVIEW,
        ResolutionState.RECONCILIATION_RECORDED,
        ResolutionState.DEFERRED,
        ResolutionState.INVALIDATED,
    ),
    ResolutionState.RECONCILIATION_RECORDED: (
        ResolutionState.RESOLVED,
        ResolutionState.REJECTED,
        ResolutionState.UNDER_REVIEW,
        ResolutionState.DEFERRED,
        ResolutionState.INVALIDATED,
    ),
    ResolutionState.DEFERRED: (
        ResolutionState.OPEN,
        ResolutionState.UNDER_REVIEW,
        ResolutionState.INVALIDATED,
    ),
    ResolutionState.RESOLVED: (),      # Terminal
    ResolutionState.REJECTED: (),      # Terminal
    ResolutionState.INVALIDATED: (),   # Terminal
}


class EvidenceRelationshipType(str, Enum):
    """Deterministic relationship classifications between compared evidence."""

    SAME_EVIDENCE = "SAME_EVIDENCE"
    SUPPORTING_EVIDENCE = "SUPPORTING_EVIDENCE"
    CONTRADICTORY_EVIDENCE = "CONTRADICTORY_EVIDENCE"
    COMPLEMENTARY_EVIDENCE = "COMPLEMENTARY_EVIDENCE"
    OUTDATED_EVIDENCE = "OUTDATED_EVIDENCE"
    DIFFERENT_SCOPE = "DIFFERENT_SCOPE"
    DIFFERENT_DATA_QUALITY = "DIFFERENT_DATA_QUALITY"
    DIFFERENT_METHODOLOGY = "DIFFERENT_METHODOLOGY"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    UNAVAILABLE_EVIDENCE = "UNAVAILABLE_EVIDENCE"


class ReconciliationOutcome(str, Enum):
    """Human-reviewed reconciliation outcome for conflicting or differing evidence."""

    BOTH_PRESERVED_VALID = "BOTH_PRESERVED_VALID"
    ONE_SUPERSEDES_OTHER = "ONE_SUPERSEDES_OTHER"
    DIFFERING_CONTEXT_EXPLAINED = "DIFFERING_CONTEXT_EXPLAINED"
    METHODOLOGICAL_DIFFERENCE_ACKNOWLEDGED = "METHODOLOGICAL_DIFFERENCE_ACKNOWLEDGED"
    QUALITY_DEGRADATION_CONFIRMED = "QUALITY_DEGRADATION_CONFIRMED"
    INSUFFICIENT_DATA_FOR_SYNTHESIS = "INSUFFICIENT_DATA_FOR_SYNTHESIS"
    EVIDENCE_INVALIDATED = "EVIDENCE_INVALIDATED"


class DecisionClaimType(str, Enum):
    """Explicit epistemological claim type distinguishing human decisions from facts."""

    FACT = "FACT"
    INTERPRETATION = "INTERPRETATION"
    HUMAN_REVIEW_DECISION = "HUMAN_REVIEW_DECISION"


def validate_no_prohibited_fields(payload: Any) -> None:
    """Recursively enforce that no prohibited trade, order, or profitability fields exist."""
    if isinstance(payload, dict):
        for k, v in payload.items():
            k_lower = str(k).lower().strip()
            if any(forbidden in k_lower for forbidden in PROHIBITED_RECOMMENDATION_FIELDS):
                raise ValidationError(
                    f"Prohibited field detected in payload: '{k}'. "
                    f"KAISER discrepancy resolution does not permit trade execution, "
                    f"ranking, or profitability fields."
                )
            validate_no_prohibited_fields(v)
    elif isinstance(payload, (list, tuple, set)):
        for item in payload:
            validate_no_prohibited_fields(item)


def validate_numerical_integrity(value: Any, name: str = "value") -> Optional[float]:
    """Validate numerical integrity: rejects NaN, Infinity, -Infinity; preserves 0 and 0.0."""
    if value is None:
        return None
    try:
        val_float = float(value)
    except (ValueError, TypeError) as exc:
        raise ValidationError(f"Invalid numeric value for '{name}': {value}") from exc

    if math.isnan(val_float):
        raise ValidationError(f"Numerical integrity violation: '{name}' cannot be NaN.")
    if math.isinf(val_float):
        raise ValidationError(f"Numerical integrity violation: '{name}' cannot be Infinity.")
    return val_float


def compute_deterministic_case_id(comparison_id: str, discrepancy_id: str) -> str:
    """Compute deterministic case ID via SHA-256 fingerprinting."""
    canonical = f"{comparison_id.strip()}:{discrepancy_id.strip()}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"res_case_{digest}"


def compute_deterministic_reconciliation_id(
    case_id: str,
    evidence_refs: Sequence[str],
    relationship_type: str,
) -> str:
    """Compute deterministic reconciliation record ID."""
    sorted_refs = ",".join(sorted(evidence_refs))
    canonical = f"{case_id.strip()}:{sorted_refs}:{relationship_type.strip()}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"reconcile_{digest}"


def compute_deterministic_decision_id(case_id: str, reviewer_id: str, timestamp: float) -> str:
    """Compute deterministic decision ID."""
    canonical = f"{case_id.strip()}:{reviewer_id.strip()}:{timestamp}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"decision_{digest}"


def compute_deterministic_audit_id(
    case_id: str,
    prev_state: str,
    new_state: str,
    timestamp: float,
) -> str:
    """Compute deterministic audit record ID."""
    canonical = f"{case_id.strip()}:{prev_state}:{new_state}:{timestamp}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"audit_res_{digest}"


@dataclass(frozen=True)
class EvidenceReconciliationRecord:
    """Immutable record of a human-reviewed reconciliation between conflicting evidence.

    Preserves both conflicting sources without fabricating a single truth.
    """

    reconciliation_id: str
    resolution_case_id: str
    evidence_refs: Tuple[str, ...]
    evidence_classifications: Mapping[str, str]
    relationship_type: EvidenceRelationshipType
    reconciliation_outcome: ReconciliationOutcome
    reviewer_notes: str
    rationale: str
    created_at: float
    reviewer_id: str
    source_provenance_refs: Tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate reconciliation record integrity."""
        if not self.reconciliation_id or not isinstance(self.reconciliation_id, str):
            raise ValidationError("EvidenceReconciliationRecord reconciliation_id must be non-empty string.")
        if not self.resolution_case_id or not isinstance(self.resolution_case_id, str):
            raise ValidationError("EvidenceReconciliationRecord resolution_case_id must be non-empty string.")
        if not self.evidence_refs:
            raise ValidationError("EvidenceReconciliationRecord evidence_refs must not be empty.")
        if not self.reviewer_id or not isinstance(self.reviewer_id, str):
            raise ValidationError("EvidenceReconciliationRecord reviewer_id must be non-empty string.")
        if not self.rationale or not isinstance(self.rationale, str):
            raise ValidationError("EvidenceReconciliationRecord rationale must be non-empty string.")
        validate_numerical_integrity(self.created_at, "created_at")
        validate_no_prohibited_fields(self.metadata)

    def to_dict(self) -> Dict[str, Any]:
        """Convert reconciliation record to serializable dictionary."""
        return {
            "reconciliation_id": self.reconciliation_id,
            "resolution_case_id": self.resolution_case_id,
            "evidence_refs": list(self.evidence_refs),
            "evidence_classifications": dict(self.evidence_classifications),
            "relationship_type": self.relationship_type.value,
            "reconciliation_outcome": self.reconciliation_outcome.value,
            "reviewer_notes": self.reviewer_notes,
            "rationale": self.rationale,
            "created_at": self.created_at,
            "reviewer_id": self.reviewer_id,
            "source_provenance_refs": list(self.source_provenance_refs),
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class ResolutionDecision:
    """Immutable audit record of a HUMAN decision resolving or rejecting a discrepancy.

    Clearly distinguishes HUMAN_REVIEW_DECISION from empirical FACT or INTERPRETATION.
    Never presents a human decision as an automatically derived fact.
    """

    decision_id: str
    resolution_case_id: str
    claim_type: DecisionClaimType = DecisionClaimType.HUMAN_REVIEW_DECISION
    summary: str = ""
    rationale: str = ""
    reviewer_id: str = "human_reviewer"
    decided_at: float = 0.0
    accepted_evidence_ids: Tuple[str, ...] = ()
    rejected_evidence_ids: Tuple[str, ...] = ()
    residual_uncertainty: Optional[str] = None
    action_required: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate resolution decision integrity."""
        if not self.decision_id or not isinstance(self.decision_id, str):
            raise ValidationError("ResolutionDecision decision_id must be non-empty string.")
        if not self.resolution_case_id or not isinstance(self.resolution_case_id, str):
            raise ValidationError("ResolutionDecision resolution_case_id must be non-empty string.")
        if not self.reviewer_id or not isinstance(self.reviewer_id, str):
            raise ValidationError("ResolutionDecision reviewer_id must be non-empty string.")
        if not self.rationale or not isinstance(self.rationale, str):
            raise ValidationError("ResolutionDecision rationale must be non-empty string.")
        validate_numerical_integrity(self.decided_at, "decided_at")
        validate_no_prohibited_fields(self.metadata)

    def to_dict(self) -> Dict[str, Any]:
        """Convert resolution decision to serializable dictionary."""
        return {
            "decision_id": self.decision_id,
            "resolution_case_id": self.resolution_case_id,
            "claim_type": self.claim_type.value,
            "summary": self.summary,
            "rationale": self.rationale,
            "reviewer_id": self.reviewer_id,
            "decided_at": self.decided_at,
            "accepted_evidence_ids": list(self.accepted_evidence_ids),
            "rejected_evidence_ids": list(self.rejected_evidence_ids),
            "residual_uncertainty": self.residual_uncertainty,
            "action_required": self.action_required,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class ResolutionAuditRecord:
    """Immutable lifecycle audit record for deterministic state transitions."""

    audit_id: str
    resolution_case_id: str
    previous_state: str
    new_state: str
    actor: str
    timestamp: float
    reason: str
    referenced_evidence: Tuple[str, ...] = ()
    correlation_id: str = ""

    def __post_init__(self) -> None:
        """Validate audit record integrity."""
        if not self.audit_id or not isinstance(self.audit_id, str):
            raise ValidationError("ResolutionAuditRecord audit_id must be non-empty string.")
        if not self.resolution_case_id or not isinstance(self.resolution_case_id, str):
            raise ValidationError("ResolutionAuditRecord resolution_case_id must be non-empty string.")
        if not self.actor or not isinstance(self.actor, str):
            raise ValidationError("ResolutionAuditRecord actor must be non-empty string.")
        validate_numerical_integrity(self.timestamp, "timestamp")

    def to_dict(self) -> Dict[str, Any]:
        """Convert audit record to serializable dictionary."""
        return {
            "audit_id": self.audit_id,
            "resolution_case_id": self.resolution_case_id,
            "previous_state": self.previous_state,
            "new_state": self.new_state,
            "actor": self.actor,
            "timestamp": self.timestamp,
            "reason": self.reason,
            "referenced_evidence": list(self.referenced_evidence),
            "correlation_id": self.correlation_id,
        }


@dataclass(frozen=True)
class DiscrepancyResolutionCase:
    """Immutable domain model representing a human-driven discrepancy resolution case.

    Binds a detected investigation discrepancy with reviewer reconciliation records,
    structured decisions, and an audit trail.
    """

    resolution_case_id: str
    comparison_id: str
    discrepancy_id: str
    created_at: float
    updated_at: float
    state: ResolutionState
    scope: str
    discrepancy_category: str
    discrepancy_severity: str
    discrepancy_description: str
    report_ids: Tuple[str, ...]
    notes: Tuple[str, ...] = ()
    resolved_by: Optional[str] = None
    resolution_reason: Optional[str] = None
    supporting_evidence_ids: Tuple[str, ...] = ()
    reconciliation_records: Tuple[EvidenceReconciliationRecord, ...] = ()
    decision: Optional[ResolutionDecision] = None
    audit_trail: Tuple[ResolutionAuditRecord, ...] = ()
    supersedes_case_id: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Enforce immutability and structural validation on resolution case."""
        if not self.resolution_case_id or not isinstance(self.resolution_case_id, str):
            raise ValidationError("DiscrepancyResolutionCase resolution_case_id must be non-empty string.")
        if not self.comparison_id or not isinstance(self.comparison_id, str):
            raise ValidationError("DiscrepancyResolutionCase comparison_id must be non-empty string.")
        if not self.discrepancy_id or not isinstance(self.discrepancy_id, str):
            raise ValidationError("DiscrepancyResolutionCase discrepancy_id must be non-empty string.")
        if not self.scope or not isinstance(self.scope, str):
            raise ValidationError("DiscrepancyResolutionCase scope must be non-empty string.")

        validate_numerical_integrity(self.created_at, "created_at")
        validate_numerical_integrity(self.updated_at, "updated_at")
        validate_no_prohibited_fields(self.metadata)

    def is_terminal(self) -> bool:
        """Check whether the case is in an immutable terminal state."""
        return self.state in TERMINAL_RESOLUTION_STATES

    def to_dict(self) -> Dict[str, Any]:
        """Convert resolution case to serializable dictionary."""
        return {
            "resolution_case_id": self.resolution_case_id,
            "comparison_id": self.comparison_id,
            "discrepancy_id": self.discrepancy_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "state": self.state.value,
            "scope": self.scope,
            "discrepancy_category": self.discrepancy_category,
            "discrepancy_severity": self.discrepancy_severity,
            "discrepancy_description": self.discrepancy_description,
            "report_ids": list(self.report_ids),
            "notes": list(self.notes),
            "resolved_by": self.resolved_by,
            "resolution_reason": self.resolution_reason,
            "supporting_evidence_ids": list(self.supporting_evidence_ids),
            "reconciliation_records": [r.to_dict() for r in self.reconciliation_records],
            "decision": self.decision.to_dict() if self.decision else None,
            "audit_trail": [a.to_dict() for a in self.audit_trail],
            "supersedes_case_id": self.supersedes_case_id,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class ResolutionVoiceSummary:
    """Speech-ready, non-directional summary of a discrepancy resolution case.

    Guaranteed never to contain trade execution terms or profitability statements.
    """

    case_id: str
    state: str
    summary_text: str
    voice_safe: bool = True

    def __post_init__(self) -> None:
        """Enforce voice safety invariant."""
        lower_text = self.summary_text.lower()
        forbidden_words = (
            "buy", "sell", "hold", "long", "short", "order", "execute", "profit", "win rate"
        )
        if any(w in lower_text for w in forbidden_words):
            raise ValidationError(
                f"Voice summary contains prohibited trading terms: {self.summary_text}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to serializable dictionary."""
        return {
            "case_id": self.case_id,
            "state": self.state,
            "summary_text": self.summary_text,
            "voice_safe": self.voice_safe,
        }
