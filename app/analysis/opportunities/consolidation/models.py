"""KAISER — Investigation Evidence Consolidation & Review Outcome Models.

Phase 2, Step 30: Deterministic, immutable domain models for consolidating
human-reviewed investigation evidence, preserving facts, interpretations,
human decisions, unresolved discrepancies, active conflicts, and limitations.

CORE ARCHITECTURAL STATEMENT:
"This consolidation layer produces an auditable representation of the current
human-reviewed evidence state. It does not decide what to trade, predict
profitability, rank strategies, or authorize execution."

SAFETY INVARIANTS:
1. Strictly analytical and decision-support: NEVER outputs BUY/SELL signals,
   expected return predictions, win probabilities, or trade rankings.
2. Absolutely no order execution fields or broker integration.
3. Numerical safety: NaN and Infinity values are strictly rejected; 0.0 is preserved.
4. Epistemological integrity: Strict separation between FACTS, INTERPRETATIONS,
   and HUMAN_REVIEW_DECISIONS. Never converts a human decision into an empirical FACT.
5. Evidence preservation: Conflicting evidence is preserved, never overwritten or merged.
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

from app.analysis.opportunities.resolution.models import (
    DecisionClaimType,
    EvidenceRelationshipType,
    ReconciliationOutcome,
)
from app.core.exceptions import ValidationError

SAFE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.:]+$")

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
    "win_rate",
    "win_probability",
)

FORBIDDEN_VOICE_WORDS: Tuple[str, ...] = (
    "buy",
    "sell",
    "hold",
    "long",
    "short",
    "order",
    "execute",
    "profit",
    "win rate",
    "win probability",
)

DEFAULT_MAX_CONSOLIDATION_HISTORY: int = 100


class ConsolidationState(str, Enum):
    """Deterministic states for investigation evidence consolidation."""

    INCOMPLETE = "INCOMPLETE"
    PARTIALLY_RECONCILED = "PARTIALLY_RECONCILED"
    RECONCILED = "RECONCILED"
    CONFLICTING = "CONFLICTING"
    INVALID = "INVALID"
    UNAVAILABLE = "UNAVAILABLE"


class ConsolidationLimitation(str, Enum):
    """Explicit limitation categories for consolidated evidence outcomes."""

    UNRESOLVED_DISCREPANCY = "UNRESOLVED_DISCREPANCY"
    CONFLICTING_EVIDENCE = "CONFLICTING_EVIDENCE"
    INCOMPLETE_EVIDENCE = "INCOMPLETE_EVIDENCE"
    STALE_DATA = "STALE_DATA"
    DEGRADED_DATA_QUALITY = "DEGRADED_DATA_QUALITY"
    MISSING_PROVENANCE = "MISSING_PROVENANCE"
    NON_REPRODUCIBLE_SOURCE = "NON_REPRODUCIBLE_SOURCE"
    DIFFERENT_ANALYTICAL_SCOPE = "DIFFERENT_ANALYTICAL_SCOPE"
    INSUFFICIENT_COVERAGE = "INSUFFICIENT_COVERAGE"
    UNAVAILABLE_SOURCE = "UNAVAILABLE_SOURCE"


class ReproducibilityConsolidationState(str, Enum):
    """Categorical evaluation of outcome reproducibility."""

    REPRODUCIBLE = "REPRODUCIBLE"
    PARTIALLY_REPRODUCIBLE = "PARTIALLY_REPRODUCIBLE"
    NON_REPRODUCIBLE = "NON_REPRODUCIBLE"
    UNKNOWN = "UNKNOWN"


def validate_no_prohibited_fields(payload: Any) -> None:
    """Recursively enforce that no prohibited trade, order, or profitability fields exist."""
    if isinstance(payload, dict):
        for k, v in payload.items():
            k_lower = str(k).lower().strip()
            if any(forbidden in k_lower for forbidden in PROHIBITED_RECOMMENDATION_FIELDS):
                raise ValidationError(
                    f"Prohibited field detected in payload: '{k}'. "
                    f"KAISER evidence consolidation does not permit trade execution, "
                    f"ranking, or profitability prediction fields."
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


def compute_deterministic_outcome_id(
    investigation_id: str,
    report_ids: Sequence[str],
    resolution_case_ids: Sequence[str],
    timestamp: float,
) -> str:
    """Compute deterministic outcome ID via SHA-256 fingerprinting."""
    canonical = {
        "investigation_id": str(investigation_id).strip(),
        "report_ids": sorted(str(r).strip() for r in report_ids),
        "resolution_case_ids": sorted(str(c).strip() for c in resolution_case_ids),
        "timestamp": float(timestamp),
    }
    raw = json.dumps(canonical, sort_keys=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"outcome_inv_{digest}"


def compute_deterministic_conflict_id(
    discrepancy_id: str,
    conflicting_evidence_ids: Sequence[str],
) -> str:
    """Compute deterministic conflict item ID."""
    sorted_ev = ",".join(sorted(conflicting_evidence_ids))
    canonical = f"{discrepancy_id.strip()}:{sorted_ev}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"conflict_{digest}"


@dataclass(frozen=True)
class ConsolidatedEvidenceItem:
    """Immutable representation of an evidence item in the consolidated review.

    Preserves source lineage, attribution, and empirical claim distinctions.
    """

    evidence_id: str
    source_id: str
    claim_type: DecisionClaimType
    evidence_relationship: Optional[str] = None
    review_status: str = "UNREVIEWED"
    provenance_reference: Optional[str] = None
    quality_status: str = "VALID"
    timestamp: float = 0.0
    scope: str = ""
    notes: Tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate evidence item invariants."""
        if not self.evidence_id or not isinstance(self.evidence_id, str):
            raise ValidationError("ConsolidatedEvidenceItem evidence_id must be a non-empty string.")
        if not self.source_id or not isinstance(self.source_id, str):
            raise ValidationError("ConsolidatedEvidenceItem source_id must be a non-empty string.")
        validate_numerical_integrity(self.timestamp, "timestamp")
        validate_no_prohibited_fields(self.metadata)

    def to_dict(self) -> Dict[str, Any]:
        """Convert evidence item to serializable dictionary."""
        return {
            "evidence_id": self.evidence_id,
            "source_id": self.source_id,
            "claim_type": self.claim_type.value if hasattr(self.claim_type, "value") else str(self.claim_type),
            "evidence_relationship": self.evidence_relationship,
            "review_status": self.review_status,
            "provenance_reference": self.provenance_reference,
            "quality_status": self.quality_status,
            "timestamp": self.timestamp,
            "scope": self.scope,
            "notes": list(self.notes),
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class ConsolidatedReviewDecision:
    """Immutable record of an explicit human-reviewed conclusion.

    Distinguishes HUMAN_REVIEW_DECISION from empirical FACTS and hypotheses.
    Never silently converts a human decision into an empirical FACT.
    """

    decision_id: str
    source_resolution_case_id: str
    claim_type: DecisionClaimType = DecisionClaimType.HUMAN_REVIEW_DECISION
    decision_text: str = ""
    rationale: str = ""
    supporting_evidence_ids: Tuple[str, ...] = ()
    reviewer_id: str = "human_reviewer"
    timestamp: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate review decision invariants."""
        if not self.decision_id or not isinstance(self.decision_id, str):
            raise ValidationError("ConsolidatedReviewDecision decision_id must be a non-empty string.")
        if not self.source_resolution_case_id or not isinstance(self.source_resolution_case_id, str):
            raise ValidationError("ConsolidatedReviewDecision source_resolution_case_id must be a non-empty string.")
        if not self.reviewer_id or not isinstance(self.reviewer_id, str):
            raise ValidationError("ConsolidatedReviewDecision reviewer_id must be a non-empty string.")
        validate_numerical_integrity(self.timestamp, "timestamp")
        validate_no_prohibited_fields(self.metadata)

    def to_dict(self) -> Dict[str, Any]:
        """Convert review decision to serializable dictionary."""
        return {
            "decision_id": self.decision_id,
            "source_resolution_case_id": self.source_resolution_case_id,
            "claim_type": self.claim_type.value if hasattr(self.claim_type, "value") else str(self.claim_type),
            "decision_text": self.decision_text,
            "rationale": self.rationale,
            "supporting_evidence_ids": list(self.supporting_evidence_ids),
            "reviewer_id": self.reviewer_id,
            "timestamp": self.timestamp,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class ConsolidatedConflictItem:
    """Immutable representation of a preserved evidence conflict."""

    conflict_id: str
    discrepancy_id: str
    conflicting_evidence_ids: Tuple[str, ...] = ()
    conflicting_report_ids: Tuple[str, ...] = ()
    description: str = ""
    conflict_nature: str = ""
    resolution_case_id: Optional[str] = None
    is_active: bool = True
    reconciliation_outcome: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate conflict item invariants."""
        if not self.conflict_id or not isinstance(self.conflict_id, str):
            raise ValidationError("ConsolidatedConflictItem conflict_id must be a non-empty string.")
        if not self.discrepancy_id or not isinstance(self.discrepancy_id, str):
            raise ValidationError("ConsolidatedConflictItem discrepancy_id must be a non-empty string.")
        validate_no_prohibited_fields(self.metadata)

    def to_dict(self) -> Dict[str, Any]:
        """Convert conflict item to serializable dictionary."""
        return {
            "conflict_id": self.conflict_id,
            "discrepancy_id": self.discrepancy_id,
            "conflicting_evidence_ids": list(self.conflicting_evidence_ids),
            "conflicting_report_ids": list(self.conflicting_report_ids),
            "description": self.description,
            "conflict_nature": self.conflict_nature,
            "resolution_case_id": self.resolution_case_id,
            "is_active": self.is_active,
            "reconciliation_outcome": self.reconciliation_outcome,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class ConsolidationLimitationItem:
    """Immutable representation of a known analytical or data limitation."""

    limitation_type: ConsolidationLimitation
    description: str
    affected_items: Tuple[str, ...] = ()
    severity: str = "WARNING"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate limitation invariants."""
        if not self.description or not isinstance(self.description, str):
            raise ValidationError("ConsolidationLimitationItem description must be a non-empty string.")
        validate_no_prohibited_fields(self.metadata)

    def to_dict(self) -> Dict[str, Any]:
        """Convert limitation item to serializable dictionary."""
        return {
            "limitation_type": self.limitation_type.value if hasattr(self.limitation_type, "value") else str(self.limitation_type),
            "description": self.description,
            "affected_items": list(self.affected_items),
            "severity": self.severity,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True)
class QualityAggregationSummary:
    """Immutable categorical summary of data quality without a deceptive single score."""

    overall_quality_state: str = "VALID"
    source_quality_statuses: Mapping[str, str] = field(default_factory=dict)
    stale_sources: Tuple[str, ...] = ()
    missing_sources: Tuple[str, ...] = ()
    degraded_sources: Tuple[str, ...] = ()
    invalid_sources: Tuple[str, ...] = ()
    coverage_limitations: Tuple[str, ...] = ()
    notes: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        """Convert quality aggregation summary to serializable dictionary."""
        return {
            "overall_quality_state": self.overall_quality_state,
            "source_quality_statuses": dict(self.source_quality_statuses),
            "stale_sources": list(self.stale_sources),
            "missing_sources": list(self.missing_sources),
            "degraded_sources": list(self.degraded_sources),
            "invalid_sources": list(self.invalid_sources),
            "coverage_limitations": list(self.coverage_limitations),
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class ReproducibilitySummary:
    """Immutable summary of outcome reproducibility without speculative scoring."""

    reproducibility_state: ReproducibilityConsolidationState = ReproducibilityConsolidationState.UNKNOWN
    reproducible_sources: Tuple[str, ...] = ()
    non_reproducible_sources: Tuple[str, ...] = ()
    reproducibility_identifiers: Mapping[str, Optional[str]] = field(default_factory=dict)
    config_fingerprints: Mapping[str, Optional[str]] = field(default_factory=dict)
    notes: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        """Convert reproducibility summary to serializable dictionary."""
        return {
            "reproducibility_state": (
                self.reproducibility_state.value
                if hasattr(self.reproducibility_state, "value")
                else str(self.reproducibility_state)
            ),
            "reproducible_sources": list(self.reproducible_sources),
            "non_reproducible_sources": list(self.non_reproducible_sources),
            "reproducibility_identifiers": dict(self.reproducibility_identifiers),
            "config_fingerprints": dict(self.config_fingerprints),
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class ConsolidationVoiceSummary:
    """Speech-ready, non-directional summary of the consolidated review outcome.

    Strictly devoid of trading directives, price targets, or profit claims.
    """

    outcome_id: str
    outcome_state: str
    summary_text: str
    voice_safe: bool = True

    def __post_init__(self) -> None:
        """Enforce strict voice safety."""
        lower_text = self.summary_text.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", lower_text):
                raise ValidationError(
                    f"Voice summary contains prohibited trading terms '{forbidden}': {self.summary_text}"
                )

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to serializable dictionary."""
        return {
            "outcome_id": self.outcome_id,
            "outcome_state": self.outcome_state,
            "summary_text": self.summary_text,
            "voice_safe": self.voice_safe,
        }


@dataclass(frozen=True)
class ConsolidatedInvestigationOutcome:
    """Immutable domain model representing the consolidated human-reviewed investigation outcome.

    Assembles evidence, resolutions, discrepancies, provenance, reproducibility,
    and data quality without manufacturing truth or recommending trades.
    """

    outcome_id: str
    investigation_id: str
    source_report_ids: Tuple[str, ...]
    source_comparison_ids: Tuple[str, ...] = ()
    source_resolution_case_ids: Tuple[str, ...] = ()
    generated_at: float = 0.0
    outcome_state: ConsolidationState = ConsolidationState.INCOMPLETE
    evidence_summary: Mapping[str, Any] = field(default_factory=dict)
    resolved_items: Tuple[str, ...] = ()
    unresolved_items: Tuple[str, ...] = ()
    conflicts: Tuple[ConsolidatedConflictItem, ...] = ()
    facts: Tuple[ConsolidatedEvidenceItem, ...] = ()
    interpretations: Tuple[ConsolidatedEvidenceItem, ...] = ()
    human_review_decisions: Tuple[ConsolidatedReviewDecision, ...] = ()
    quality_summary: QualityAggregationSummary = field(default_factory=QualityAggregationSummary)
    provenance_summary: Mapping[str, Any] = field(default_factory=dict)
    reproducibility_summary: ReproducibilitySummary = field(default_factory=ReproducibilitySummary)
    limitations: Tuple[ConsolidationLimitationItem, ...] = ()
    traceability: Mapping[str, Any] = field(default_factory=dict)
    voice_summary: Optional[ConsolidationVoiceSummary] = None
    disclaimer: str = (
        "CONSOLIDATED REVIEW OUTCOME ONLY: This consolidation layer produces an auditable "
        "representation of the current human-reviewed evidence state. It does not decide what "
        "to trade, predict profitability, rank strategies, or authorize execution. "
        "Settings.execution_locked remains True."
    )
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate consolidated outcome integrity."""
        if not self.outcome_id or not isinstance(self.outcome_id, str):
            raise ValidationError("ConsolidatedInvestigationOutcome outcome_id must be a non-empty string.")
        if not self.investigation_id or not isinstance(self.investigation_id, str):
            raise ValidationError("ConsolidatedInvestigationOutcome investigation_id must be a non-empty string.")
        validate_numerical_integrity(self.generated_at, "generated_at")
        if self.generated_at < 0.0:
            raise ValidationError("generated_at cannot be negative.")
        validate_no_prohibited_fields(self.metadata)
        validate_no_prohibited_fields(self.traceability)
        validate_no_prohibited_fields(self.provenance_summary)

    def to_dict(self) -> Dict[str, Any]:
        """Convert consolidated outcome to serializable dictionary."""
        return {
            "outcome_id": self.outcome_id,
            "investigation_id": self.investigation_id,
            "source_report_ids": list(self.source_report_ids),
            "source_comparison_ids": list(self.source_comparison_ids),
            "source_resolution_case_ids": list(self.source_resolution_case_ids),
            "generated_at": self.generated_at,
            "outcome_state": (
                self.outcome_state.value
                if hasattr(self.outcome_state, "value")
                else str(self.outcome_state)
            ),
            "evidence_summary": copy.deepcopy(dict(self.evidence_summary)),
            "resolved_items": list(self.resolved_items),
            "unresolved_items": list(self.unresolved_items),
            "conflicts": [c.to_dict() for c in self.conflicts],
            "facts": [f.to_dict() for f in self.facts],
            "interpretations": [i.to_dict() for i in self.interpretations],
            "human_review_decisions": [d.to_dict() for d in self.human_review_decisions],
            "quality_summary": self.quality_summary.to_dict(),
            "provenance_summary": copy.deepcopy(dict(self.provenance_summary)),
            "reproducibility_summary": self.reproducibility_summary.to_dict(),
            "limitations": [lim.to_dict() for lim in self.limitations],
            "traceability": copy.deepcopy(dict(self.traceability)),
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "disclaimer": self.disclaimer,
            "metadata": copy.deepcopy(dict(self.metadata)),
        }
