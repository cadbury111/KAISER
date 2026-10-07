"""KAISER — Cross-Investigation Consolidated Outcome Consistency Domain Models.

Phase 2, Step 31: Deterministic analytical layer for comparing multiple
ConsolidatedInvestigationOutcome records.

CORE ARCHITECTURAL INVARIANT:
This consistency layer identifies structural similarities, differences,
review coverage gaps, unresolved conflicts, and evidence-state divergence
across consolidated investigations. It does NOT determine which
investigation is correct, rank investigations, predict profitability,
or recommend trading actions.
Settings.execution_locked remains True.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.core.exceptions import ValidationError

# Bounded limits
MIN_COMPARISON_OUTCOMES = 2
MAX_COMPARISON_OUTCOMES = 20
DEFAULT_MAX_CONSISTENCY_HISTORY = 100
MAX_ITEMS_PER_DIMENSION = 200
MAX_STRING_LENGTH = 1000

SAFE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_\-:]+$")

PROHIBITED_RECOMMENDATION_FIELDS = frozenset({
    "action",
    "buy",
    "sell",
    "order",
    "order_type",
    "position_size",
    "target_price",
    "stop_loss",
    "take_profit",
    "leverage",
    "allocation",
    "trade_recommendation",
    "execution_command",
    "execution_type",
    "expected_return",
    "win_probability",
    "profit_factor",
    "strategy_ranking",
    "winner",
    "winning_investigation",
    "winning_strategy",
    "best_investigation",
    "best_strategy",
    "confidence_score",
    "subjective_score",
    "signal",
})

FORBIDDEN_VOICE_WORDS = frozenset({
    "buy",
    "sell",
    "long",
    "short",
    "execute",
    "order",
    "trade",
    "invest",
    "allocate",
    "profit",
    "profitable",
    "winner",
    "winning",
    "best",
    "superior",
    "ranked",
    "ranking",
    "target",
})


def validate_no_prohibited_fields(data: Any, path: str = "root") -> None:
    """Recursively enforce that no trade execution or recommendation fields exist."""
    if isinstance(data, dict):
        for k, v in data.items():
            k_lower = str(k).lower().strip()
            if k_lower in PROHIBITED_RECOMMENDATION_FIELDS:
                raise ValidationError(
                    f"Prohibited field '{k}' detected at '{path}'. "
                    f"KAISER Step 31 forbids trade recommendations, strategy rankings, and execution directives."
                )
            validate_no_prohibited_fields(v, path=f"{path}.{k}")
    elif isinstance(data, (list, tuple, set)):
        for idx, item in enumerate(data):
            validate_no_prohibited_fields(item, path=f"{path}[{idx}]")


def validate_numerical_integrity(value: Any, name: str = "value") -> None:
    """Ensure numerical fields are finite, non-NaN, and non-infinite."""
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            raise ValidationError(
                f"Numerical field '{name}' must be finite (got {value}). Rejection of NaN/Infinity enforced."
            )


# ==============================================================================
# Enumerations
# ==============================================================================

class ConsolidatedOutcomeComparability(str, Enum):
    """Categorical comparability state across consolidated outcomes."""
    COMPARABLE = "COMPARABLE"
    PARTIALLY_COMPARABLE = "PARTIALLY_COMPARABLE"
    NOT_COMPARABLE = "NOT_COMPARABLE"
    INVALID_INPUT = "INVALID_INPUT"
    INCOMPLETE_EVIDENCE = "INCOMPLETE_EVIDENCE"


class DimensionMatchStatus(str, Enum):
    """Status of comparison across an explicit analytical dimension."""
    MATCH = "MATCH"
    PARTIAL_MATCH = "PARTIAL_MATCH"
    MISMATCH = "MISMATCH"
    UNAVAILABLE = "UNAVAILABLE"


class ComparisonDimensionName(str, Enum):
    """Explicit dimensions for comparing consolidated outcomes."""
    SYMBOL_SUBJECT = "symbol_subject"
    TIMEFRAME = "timeframe"
    OBSERVATION_PERIOD = "observation_period"
    ANALYTICAL_SCOPE = "analytical_scope"
    EVIDENCE_SCHEMA = "evidence_schema"
    REPORT_VERSION = "report_version"
    PROVENANCE = "provenance"
    DATA_QUALITY = "data_quality"
    REPRODUCIBILITY = "reproducibility"
    RESOLUTION_STATE = "resolution_state"
    REVIEW_COVERAGE = "review_coverage"
    LIMITATION_PROFILE = "limitation_profile"


class EvidenceConsistencyStatus(str, Enum):
    """Classification of evidence across multiple consolidated outcomes."""
    SHARED = "SHARED"
    UNIQUE_TO_OUTCOME = "UNIQUE_TO_OUTCOME"
    MISSING = "MISSING"
    CONFLICTING = "CONFLICTING"
    MODIFIED = "MODIFIED"
    UNAVAILABLE = "UNAVAILABLE"


class FactConsistencyStatus(str, Enum):
    """Classification of FACT claims across consolidated outcomes."""
    CONSISTENT = "CONSISTENT"
    INCONSISTENT = "INCONSISTENT"
    PARTIALLY_SUPPORTED = "PARTIALLY_SUPPORTED"
    UNAVAILABLE = "UNAVAILABLE"


class InterpretationConsistencyStatus(str, Enum):
    """Classification of analytical INTERPRETATIONS across outcomes."""
    SAME_INTERPRETATION = "SAME_INTERPRETATION"
    DIFFERENT_INTERPRETATION = "DIFFERENT_INTERPRETATION"
    PARTIALLY_ALIGNED = "PARTIALLY_ALIGNED"
    UNAVAILABLE = "UNAVAILABLE"


class HumanReviewConsistencyStatus(str, Enum):
    """Classification of explicit human review decisions across outcomes."""
    SAME_REVIEW_OUTCOME = "SAME_REVIEW_OUTCOME"
    DIFFERENT_REVIEW_OUTCOME = "DIFFERENT_REVIEW_OUTCOME"
    PARTIALLY_ALIGNED = "PARTIALLY_ALIGNED"
    NO_REVIEW = "NO_REVIEW"
    UNAVAILABLE = "UNAVAILABLE"


class CoverageStatus(str, Enum):
    """Categorical review coverage state."""
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    MINIMAL = "MINIMAL"
    NONE = "NONE"
    UNKNOWN = "UNKNOWN"


class DivergenceSeverity(str, Enum):
    """Descriptive severity of divergence across outcomes."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DivergenceStatus(str, Enum):
    """Status of an identified divergence item."""
    ACTIVE = "ACTIVE"
    RESOLVED = "RESOLVED"
    DEFERRED = "DEFERRED"
    INVALIDATED = "INVALIDATED"


class QualityComparisonStatus(str, Enum):
    """Aggregate data quality relationship across outcomes."""
    SAME = "SAME"
    DIFFERENT = "DIFFERENT"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"
    UNAVAILABLE = "UNAVAILABLE"


class ProvenanceComparisonStatus(str, Enum):
    """Provenance relationship across outcomes."""
    CONSISTENT = "CONSISTENT"
    PARTIALLY_TRACEABLE = "PARTIALLY_TRACEABLE"
    DIVERGENT = "DIVERGENT"
    MISSING = "MISSING"


class ReproducibilityComparisonStatus(str, Enum):
    """Reproducibility relationship across outcomes."""
    CONSISTENT = "CONSISTENT"
    PARTIALLY_REPRODUCIBLE = "PARTIALLY_REPRODUCIBLE"
    DIVERGENT = "DIVERGENT"
    UNKNOWN = "UNKNOWN"


class ComparisonLifecycleState(str, Enum):
    """Finite state machine for outcome comparison lifecycle."""
    CREATED = "CREATED"
    ANALYZING = "ANALYZING"
    COMPLETED = "COMPLETED"
    INCOMPLETE = "INCOMPLETE"
    FAILED = "FAILED"
    INVALID = "INVALID"


VALID_COMPARISON_LIFECYCLE_TRANSITIONS = {
    ComparisonLifecycleState.CREATED: {
        ComparisonLifecycleState.ANALYZING,
        ComparisonLifecycleState.FAILED,
        ComparisonLifecycleState.INVALID,
    },
    ComparisonLifecycleState.ANALYZING: {
        ComparisonLifecycleState.COMPLETED,
        ComparisonLifecycleState.INCOMPLETE,
        ComparisonLifecycleState.FAILED,
        ComparisonLifecycleState.INVALID,
    },
    ComparisonLifecycleState.COMPLETED: set(),  # Terminal immutable
    ComparisonLifecycleState.INCOMPLETE: set(), # Terminal immutable
    ComparisonLifecycleState.FAILED: set(),     # Terminal immutable
    ComparisonLifecycleState.INVALID: set(),    # Terminal immutable
}


# ==============================================================================
# Helper ID Computation
# ==============================================================================

def compute_deterministic_comparison_id(
    outcome_ids: Sequence[str],
    comparison_scope: str,
    schema_version: str = "v1",
) -> str:
    """Compute deterministic SHA-256 ID for a consolidated outcome comparison."""
    clean_outcomes = sorted(list({str(o).strip() for o in outcome_ids if str(o).strip()}))
    payload = {
        "outcome_ids": clean_outcomes,
        "comparison_scope": str(comparison_scope).strip().upper(),
        "schema_version": schema_version,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
    return f"coc_{digest}"


def compute_deterministic_divergence_id(
    divergence_type: str,
    outcome_ids: Sequence[str],
    entity_refs: Sequence[str],
) -> str:
    """Compute deterministic SHA-256 ID for an outcome divergence item."""
    clean_outcomes = sorted(list({str(o).strip() for o in outcome_ids if str(o).strip()}))
    clean_refs = sorted(list({str(r).strip() for r in entity_refs if str(r).strip()}))
    payload = {
        "divergence_type": str(divergence_type).strip(),
        "outcome_ids": clean_outcomes,
        "entity_refs": clean_refs,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
    return f"div_{digest}"


# ==============================================================================
# Domain Models
# ==============================================================================

@dataclass(frozen=True)
class OutcomeComparisonDimensionRecord:
    """Individual dimension comparison record."""

    dimension_name: ComparisonDimensionName
    status: DimensionMatchStatus
    description: str
    details_by_outcome: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.description:
            raise ValidationError("Dimension record description must be non-empty.")
        validate_no_prohibited_fields(self.details_by_outcome, path="details_by_outcome")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension_name": self.dimension_name.value,
            "status": self.status.value,
            "description": self.description,
            "details_by_outcome": dict(self.details_by_outcome),
        }


@dataclass(frozen=True)
class OutcomeDivergenceItem:
    """Represents an explicit divergence identified across consolidated outcomes."""

    divergence_id: str
    source_outcome_ids: Tuple[str, ...]
    source_evidence_ids: Tuple[str, ...]
    discrepancy_ids: Tuple[str, ...]
    resolution_ids: Tuple[str, ...]
    severity: DivergenceSeverity
    status: DivergenceStatus
    explanation: str
    provenance: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.divergence_id or not SAFE_ID_PATTERN.match(self.divergence_id):
            raise ValidationError(f"Invalid divergence_id: {self.divergence_id}")
        if not self.explanation:
            raise ValidationError("Divergence explanation must be non-empty.")
        validate_no_prohibited_fields(self.provenance, path="provenance")

        object.__setattr__(self, "source_outcome_ids", tuple(self.source_outcome_ids))
        object.__setattr__(self, "source_evidence_ids", tuple(self.source_evidence_ids))
        object.__setattr__(self, "discrepancy_ids", tuple(self.discrepancy_ids))
        object.__setattr__(self, "resolution_ids", tuple(self.resolution_ids))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "divergence_id": self.divergence_id,
            "source_outcome_ids": list(self.source_outcome_ids),
            "source_evidence_ids": list(self.source_evidence_ids),
            "discrepancy_ids": list(self.discrepancy_ids),
            "resolution_ids": list(self.resolution_ids),
            "severity": self.severity.value,
            "status": self.status.value,
            "explanation": self.explanation,
            "provenance": dict(self.provenance),
        }


@dataclass(frozen=True)
class EvidenceConsistencyItem:
    """Represents comparative consistency of an evidence item across outcomes."""

    evidence_id: str
    status: EvidenceConsistencyStatus
    present_in_outcome_ids: Tuple[str, ...]
    missing_in_outcome_ids: Tuple[str, ...]
    claim_type: str
    provenance_reference: str
    notes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.evidence_id:
            raise ValidationError("EvidenceConsistencyItem evidence_id cannot be empty.")
        object.__setattr__(self, "present_in_outcome_ids", tuple(self.present_in_outcome_ids))
        object.__setattr__(self, "missing_in_outcome_ids", tuple(self.missing_in_outcome_ids))
        object.__setattr__(self, "notes", tuple(self.notes))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "evidence_id": self.evidence_id,
            "status": self.status.value,
            "present_in_outcome_ids": list(self.present_in_outcome_ids),
            "missing_in_outcome_ids": list(self.missing_in_outcome_ids),
            "claim_type": self.claim_type,
            "provenance_reference": self.provenance_reference,
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class FactConsistencyItem:
    """Represents comparative consistency of a FACT statement across outcomes."""

    fact_statement: str
    status: FactConsistencyStatus
    supporting_outcome_ids: Tuple[str, ...]
    opposing_outcome_ids: Tuple[str, ...]
    evidence_refs: Tuple[str, ...]
    notes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.fact_statement:
            raise ValidationError("Fact statement cannot be empty.")
        object.__setattr__(self, "supporting_outcome_ids", tuple(self.supporting_outcome_ids))
        object.__setattr__(self, "opposing_outcome_ids", tuple(self.opposing_outcome_ids))
        object.__setattr__(self, "evidence_refs", tuple(self.evidence_refs))
        object.__setattr__(self, "notes", tuple(self.notes))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fact_statement": self.fact_statement,
            "status": self.status.value,
            "supporting_outcome_ids": list(self.supporting_outcome_ids),
            "opposing_outcome_ids": list(self.opposing_outcome_ids),
            "evidence_refs": list(self.evidence_refs),
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class InterpretationConsistencyItem:
    """Represents comparative consistency of an analytical INTERPRETATION."""

    interpretation_topic: str
    status: InterpretationConsistencyStatus
    outcome_interpretations: Dict[str, str] = field(default_factory=dict)
    notes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.interpretation_topic:
            raise ValidationError("Interpretation topic cannot be empty.")
        object.__setattr__(self, "notes", tuple(self.notes))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "interpretation_topic": self.interpretation_topic,
            "status": self.status.value,
            "outcome_interpretations": dict(self.outcome_interpretations),
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class HumanReviewConsistencyItem:
    """Represents comparative consistency of human review decisions."""

    decision_topic: str
    status: HumanReviewConsistencyStatus
    outcome_decisions: Dict[str, str] = field(default_factory=dict)
    supporting_evidence_ids: Tuple[str, ...] = ()
    reviewer_ids: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.decision_topic:
            raise ValidationError("Decision topic cannot be empty.")
        object.__setattr__(self, "supporting_evidence_ids", tuple(self.supporting_evidence_ids))
        object.__setattr__(self, "reviewer_ids", tuple(self.reviewer_ids))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "decision_topic": self.decision_topic,
            "status": self.status.value,
            "outcome_decisions": dict(self.outcome_decisions),
            "supporting_evidence_ids": list(self.supporting_evidence_ids),
            "reviewer_ids": list(self.reviewer_ids),
        }


@dataclass(frozen=True)
class ReviewCoverageSummary:
    """Deterministic summary of review coverage across compared outcomes."""

    overall_coverage: CoverageStatus
    evidence_reviewed_count: int
    evidence_unreviewed_count: int
    discrepancies_reviewed_count: int
    discrepancies_unresolved_count: int
    limitations_acknowledged_count: int
    provenance_verified: bool
    reproducibility_checked: bool
    quality_checked: bool
    coverage_by_outcome: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_numerical_integrity(self.evidence_reviewed_count, "evidence_reviewed_count")
        validate_numerical_integrity(self.evidence_unreviewed_count, "evidence_unreviewed_count")
        validate_numerical_integrity(self.discrepancies_reviewed_count, "discrepancies_reviewed_count")
        validate_numerical_integrity(self.discrepancies_unresolved_count, "discrepancies_unresolved_count")
        validate_numerical_integrity(self.limitations_acknowledged_count, "limitations_acknowledged_count")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_coverage": self.overall_coverage.value,
            "evidence_reviewed_count": self.evidence_reviewed_count,
            "evidence_unreviewed_count": self.evidence_unreviewed_count,
            "discrepancies_reviewed_count": self.discrepancies_reviewed_count,
            "discrepancies_unresolved_count": self.discrepancies_unresolved_count,
            "limitations_acknowledged_count": self.limitations_acknowledged_count,
            "provenance_verified": self.provenance_verified,
            "reproducibility_checked": self.reproducibility_checked,
            "quality_checked": self.quality_checked,
            "coverage_by_outcome": dict(self.coverage_by_outcome),
        }


@dataclass(frozen=True)
class OutcomeComparisonLimitationItem:
    """Explicit limitation affecting the outcome comparison."""

    limitation_type: str
    description: str
    affected_outcomes: Tuple[str, ...]
    severity: str = "WARNING"

    def __post_init__(self) -> None:
        if not self.limitation_type or not self.description:
            raise ValidationError("Limitation type and description must be non-empty.")
        object.__setattr__(self, "affected_outcomes", tuple(self.affected_outcomes))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "limitation_type": self.limitation_type,
            "description": self.description,
            "affected_outcomes": list(self.affected_outcomes),
            "severity": self.severity,
        }


@dataclass(frozen=True)
class OutcomeComparisonVoiceSummary:
    """Voice-safe structured summary of outcome comparison."""

    comparison_id: str
    headline: str
    script: str
    voice_safe: bool = True

    def __post_init__(self) -> None:
        if not self.comparison_id:
            raise ValidationError("Voice summary comparison_id must be non-empty.")
        script_lower = self.script.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", script_lower):
                raise ValidationError(
                    f"Forbidden trading or ranking term '{forbidden}' detected in voice summary script."
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "comparison_id": self.comparison_id,
            "headline": self.headline,
            "script": self.script,
            "voice_safe": self.voice_safe,
        }


@dataclass(frozen=True)
class ConsolidatedOutcomeComparison:
    """Immutable domain model for cross-investigation consolidated outcome comparison.

    Represents an auditable consistency and coverage evaluation across multiple
    ConsolidatedInvestigationOutcome records.
    ABSOLUTELY NOT A TRADING DIRECTIVE, STRATEGY RANKING, OR WINNER SELECTION.
    """

    comparison_id: str
    outcome_ids: Tuple[str, ...]
    investigation_ids: Tuple[str, ...]
    generated_at: float
    comparison_scope: str
    lifecycle_state: ComparisonLifecycleState
    comparability_state: ConsolidatedOutcomeComparability
    dimensions: Tuple[OutcomeComparisonDimensionRecord, ...]
    evidence_consistency: Tuple[EvidenceConsistencyItem, ...]
    fact_consistency: Tuple[FactConsistencyItem, ...]
    interpretation_consistency: Tuple[InterpretationConsistencyItem, ...]
    human_review_consistency: Tuple[HumanReviewConsistencyItem, ...]
    review_coverage: ReviewCoverageSummary
    divergence_items: Tuple[OutcomeDivergenceItem, ...]
    conflict_summary: Dict[str, Any]
    quality_comparison: Dict[str, Any]
    provenance_comparison: Dict[str, Any]
    reproducibility_comparison: Dict[str, Any]
    limitations: Tuple[OutcomeComparisonLimitationItem, ...]
    traceability: Dict[str, Any]
    voice_summary: Optional[OutcomeComparisonVoiceSummary] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    disclaimer: str = (
        "ANALYTICAL CONSISTENCY COMPARISON ONLY: This comparison evaluates analytical consistency, "
        "divergences, and review coverage across consolidated investigations. It does NOT select a "
        "winner, rank strategies, predict profitability, or recommend trade execution. "
        "Settings.execution_locked remains True."
    )

    def __post_init__(self) -> None:
        """Validate invariants, numerical bounds, and prohibited fields."""
        if not self.comparison_id or not SAFE_ID_PATTERN.match(self.comparison_id):
            raise ValidationError(f"Invalid comparison_id: {self.comparison_id}")
        if len(self.outcome_ids) < MIN_COMPARISON_OUTCOMES:
            raise ValidationError(
                f"ConsolidatedOutcomeComparison requires at least {MIN_COMPARISON_OUTCOMES} outcomes (got {len(self.outcome_ids)})."
            )
        if len(self.outcome_ids) > MAX_COMPARISON_OUTCOMES:
            raise ValidationError(
                f"ConsolidatedOutcomeComparison exceeds maximum of {MAX_COMPARISON_OUTCOMES} outcomes (got {len(self.outcome_ids)})."
            )

        validate_numerical_integrity(self.generated_at, "generated_at")
        if self.generated_at < 0.0:
            raise ValidationError("generated_at cannot be negative.")

        validate_no_prohibited_fields(self.conflict_summary, path="conflict_summary")
        validate_no_prohibited_fields(self.quality_comparison, path="quality_comparison")
        validate_no_prohibited_fields(self.provenance_comparison, path="provenance_comparison")
        validate_no_prohibited_fields(self.reproducibility_comparison, path="reproducibility_comparison")
        validate_no_prohibited_fields(self.traceability, path="traceability")
        validate_no_prohibited_fields(self.metadata, path="metadata")

        # Freeze collections
        object.__setattr__(self, "outcome_ids", tuple(sorted(list(self.outcome_ids))))
        object.__setattr__(self, "investigation_ids", tuple(sorted(list(set(self.investigation_ids)))))
        object.__setattr__(self, "dimensions", tuple(self.dimensions))
        object.__setattr__(self, "evidence_consistency", tuple(self.evidence_consistency))
        object.__setattr__(self, "fact_consistency", tuple(self.fact_consistency))
        object.__setattr__(self, "interpretation_consistency", tuple(self.interpretation_consistency))
        object.__setattr__(self, "human_review_consistency", tuple(self.human_review_consistency))
        object.__setattr__(self, "divergence_items", tuple(self.divergence_items))
        object.__setattr__(self, "limitations", tuple(self.limitations))

    def to_dict(self) -> Dict[str, Any]:
        """Convert complete comparison to dictionary."""
        return {
            "comparison_id": self.comparison_id,
            "outcome_ids": list(self.outcome_ids),
            "investigation_ids": list(self.investigation_ids),
            "generated_at": self.generated_at,
            "comparison_scope": self.comparison_scope,
            "lifecycle_state": self.lifecycle_state.value,
            "comparability_state": self.comparability_state.value,
            "dimensions": [d.to_dict() for d in self.dimensions],
            "evidence_consistency": [e.to_dict() for e in self.evidence_consistency],
            "fact_consistency": [f.to_dict() for f in self.fact_consistency],
            "interpretation_consistency": [i.to_dict() for i in self.interpretation_consistency],
            "human_review_consistency": [h.to_dict() for h in self.human_review_consistency],
            "review_coverage": self.review_coverage.to_dict(),
            "divergence_items": [div.to_dict() for div in self.divergence_items],
            "conflict_summary": dict(self.conflict_summary),
            "quality_comparison": dict(self.quality_comparison),
            "provenance_comparison": dict(self.provenance_comparison),
            "reproducibility_comparison": dict(self.reproducibility_comparison),
            "limitations": [lim.to_dict() for lim in self.limitations],
            "traceability": dict(self.traceability),
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "metadata": dict(self.metadata),
            "disclaimer": self.disclaimer,
        }
