"""KAISER — Consolidated Investigation Review Evolution & Historical Consistency Domain Models.

Phase 2, Step 32: Deterministic historical tracking layer for how investigation evidence,
reconciliation, review coverage, and analytical consistency change over time.

CORE ARCHITECTURAL INVARIANT:
"This historical review layer records how investigation evidence,
reconciliation, review coverage, and analytical consistency change over
time. It describes historical evolution only. It does not determine
correctness, rank investigations, predict profitability, or recommend
trading actions."
Settings.execution_locked remains True.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.core.exceptions import ValidationError

# Bounded limits
MAX_SNAPSHOTS_PER_INVESTIGATION = 100
MAX_EVOLUTION_PER_INVESTIGATION = 100
MAX_GLOBAL_HISTORY = 500
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
    if isinstance(data, (dict, MappingProxyType)):
        for k, v in data.items():
            k_lower = str(k).lower().strip()
            if k_lower in PROHIBITED_RECOMMENDATION_FIELDS:
                raise ValidationError(
                    f"Prohibited field '{k}' detected at '{path}'. "
                    f"KAISER Step 32 forbids trade recommendations, strategy rankings, and execution directives."
                )
            validate_no_prohibited_fields(v, path=f"{path}.{k}")
    elif isinstance(data, (list, tuple, set, frozenset)):
        for idx, item in enumerate(data):
            validate_no_prohibited_fields(item, path=f"{path}[{idx}]")


def validate_numerical_integrity(value: Any, name: str = "value") -> None:
    """Ensure numerical fields are finite, non-NaN, and non-infinite."""
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            raise ValidationError(
                f"Numerical field '{name}' must be finite (got {value}). Rejection of NaN/Infinity enforced."
            )


def _freeze(val: Any) -> Any:
    """Recursively freeze dictionaries and lists into immutable containers."""
    if isinstance(val, (dict, MappingProxyType)):
        return MappingProxyType({k: _freeze(v) for k, v in val.items()})
    elif isinstance(val, (list, tuple)):
        return tuple(_freeze(item) for item in val)
    elif isinstance(val, (set, frozenset)):
        return frozenset(_freeze(item) for item in val)
    return val


def _unfreeze(val: Any) -> Any:
    """Recursively convert frozen containers into standard serializable dicts/lists."""
    if isinstance(val, (dict, MappingProxyType)):
        return {str(k): _unfreeze(v) for k, v in val.items()}
    elif isinstance(val, (list, tuple)):
        return [_unfreeze(item) for item in val]
    elif isinstance(val, (set, frozenset)):
        return sorted([_unfreeze(item) for item in val], key=lambda x: str(x))
    return val


# ==============================================================================
# Enumerations
# ==============================================================================

class HistoricalSnapshotLifecycleState(str, Enum):
    """Lifecycle state of a historical review snapshot."""
    CAPTURED = "CAPTURED"
    VALIDATED = "VALIDATED"
    STORED = "STORED"
    FAILED = "FAILED"
    INVALID = "INVALID"

    @property
    def is_terminal(self) -> bool:
        """Return True if the lifecycle state is terminal and immutable."""
        return self in TERMINAL_SNAPSHOT_LIFECYCLE_STATES


TERMINAL_SNAPSHOT_LIFECYCLE_STATES = frozenset({
    HistoricalSnapshotLifecycleState.STORED,
    HistoricalSnapshotLifecycleState.FAILED,
    HistoricalSnapshotLifecycleState.INVALID,
})


class ChangeClassification(str, Enum):
    """Deterministic, objective classification of changes between reviewed states."""
    UNCHANGED = "UNCHANGED"
    ADDED = "ADDED"
    REMOVED = "REMOVED"
    MODIFIED = "MODIFIED"
    RESOLVED = "RESOLVED"
    REOPENED = "REOPENED"
    DEFERRED = "DEFERRED"
    INVALIDATED = "INVALIDATED"
    IMPROVED_DATA_AVAILABILITY = "IMPROVED_DATA_AVAILABILITY"
    DEGRADED_DATA_AVAILABILITY = "DEGRADED_DATA_AVAILABILITY"
    PROVENANCE_CHANGED = "PROVENANCE_CHANGED"
    REPRODUCIBILITY_CHANGED = "REPRODUCIBILITY_CHANGED"
    REVIEW_COVERAGE_CHANGED = "REVIEW_COVERAGE_CHANGED"
    CONFLICT_STATE_CHANGED = "CONFLICT_STATE_CHANGED"


class EvolutionDimensionName(str, Enum):
    """The 12 independent tracking dimensions across historical review states."""
    EVIDENCE = "EVIDENCE"
    FACTS = "FACTS"
    INTERPRETATIONS = "INTERPRETATIONS"
    HUMAN_REVIEW_DECISIONS = "HUMAN_REVIEW_DECISIONS"
    DISCREPANCIES = "DISCREPANCIES"
    RECONCILIATIONS = "RECONCILIATIONS"
    CONFLICTS = "CONFLICTS"
    DATA_QUALITY = "DATA_QUALITY"
    PROVENANCE = "PROVENANCE"
    REPRODUCIBILITY = "REPRODUCIBILITY"
    REVIEW_COVERAGE = "REVIEW_COVERAGE"
    LIMITATIONS = "LIMITATIONS"


class ReviewCoverageEvolutionState(str, Enum):
    """Categorical review coverage states without arbitrary metrics."""
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    MINIMAL = "MINIMAL"
    NONE = "NONE"
    UNKNOWN = "UNKNOWN"


class DataQualityEvolutionState(str, Enum):
    """Data quality categories at source level."""
    VALID = "VALID"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    INVALID = "INVALID"
    MISSING = "MISSING"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    UNAVAILABLE = "UNAVAILABLE"


class ReproducibilityEvolutionState(str, Enum):
    """Reproducibility state supported by evidence and run records."""
    REPRODUCIBLE = "REPRODUCIBLE"
    PARTIALLY_REPRODUCIBLE = "PARTIALLY_REPRODUCIBLE"
    NON_REPRODUCIBLE = "NON_REPRODUCIBLE"
    UNKNOWN = "UNKNOWN"


class ConflictLifecycleState(str, Enum):
    """Conflict lifecycle classification across review history."""
    NEWLY_DETECTED = "NEWLY_DETECTED"
    ACTIVE = "ACTIVE"
    RECONCILED = "RECONCILED"
    DEFERRED = "DEFERRED"
    REOPENED = "REOPENED"
    INVALIDATED = "INVALIDATED"


class EvidenceClaimType(str, Enum):
    """Explicit distinction among claim types."""
    FACT = "FACT"
    INTERPRETATION = "INTERPRETATION"
    HUMAN_REVIEW_DECISION = "HUMAN_REVIEW_DECISION"


# ==============================================================================
# Deterministic Identity Helpers
# ==============================================================================

def compute_deterministic_snapshot_id(
    investigation_id: str,
    outcome_id: str,
    comparison_id: Optional[str],
    snapshot_version: int,
    source_state_fingerprint: str,
) -> str:
    """Generate deterministic snapshot ID using canonicalized input fields and SHA-256."""
    canonical = {
        "comparison_id": str(comparison_id or ""),
        "investigation_id": str(investigation_id),
        "outcome_id": str(outcome_id),
        "snapshot_version": int(snapshot_version),
        "source_state_fingerprint": str(source_state_fingerprint),
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:16]
    return f"revsnap_{digest}"


def compute_deterministic_evolution_id(
    previous_snapshot_id: str,
    current_snapshot_id: str,
) -> str:
    """Generate deterministic evolution ID using canonical snapshot references and SHA-256."""
    canonical = {
        "current_snapshot_id": str(current_snapshot_id),
        "previous_snapshot_id": str(previous_snapshot_id),
    }
    raw = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:16]
    return f"revevol_{digest}"


# ==============================================================================
# Domain Models
# ==============================================================================

@dataclass(frozen=True)
class EvolutionDimensionRecord:
    """Change record for an individual analytical dimension between two snapshots."""

    dimension_name: EvolutionDimensionName
    previous_state: Any
    current_state: Any
    change_classification: ChangeClassification
    source_references: Tuple[str, ...] = ()
    explanation: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "previous_state", _freeze(self.previous_state))
        object.__setattr__(self, "current_state", _freeze(self.current_state))
        object.__setattr__(self, "source_references", tuple(self.source_references))
        validate_no_prohibited_fields(self.previous_state)
        validate_no_prohibited_fields(self.current_state)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension_name": self.dimension_name.value,
            "previous_state": _unfreeze(self.previous_state),
            "current_state": _unfreeze(self.current_state),
            "change_classification": self.change_classification.value,
            "source_references": list(self.source_references),
            "explanation": self.explanation,
        }


@dataclass(frozen=True)
class RecurringReviewConflict:
    """Descriptive tracking of a conflict that recurred across multiple review snapshots."""

    conflict_id: str
    original_discrepancy_id: Optional[str]
    evidence_lineage: Tuple[str, ...]
    occurrence_count: int
    observed_snapshot_ids: Tuple[str, ...]
    lifecycle_status: ConflictLifecycleState
    descriptive_note: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence_lineage", tuple(self.evidence_lineage))
        object.__setattr__(self, "observed_snapshot_ids", tuple(self.observed_snapshot_ids))
        validate_numerical_integrity(self.occurrence_count, "occurrence_count")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "conflict_id": self.conflict_id,
            "original_discrepancy_id": self.original_discrepancy_id,
            "evidence_lineage": list(self.evidence_lineage),
            "occurrence_count": self.occurrence_count,
            "observed_snapshot_ids": list(self.observed_snapshot_ids),
            "lifecycle_status": self.lifecycle_status.value,
            "descriptive_note": self.descriptive_note,
        }


@dataclass(frozen=True)
class ReviewEvolutionVoiceSummary:
    """Voice-safe summary of historical review evolution."""

    evolution_id: str
    previous_snapshot_id: str
    current_snapshot_id: str
    summary_text: str
    voice_safe: bool = True

    def __post_init__(self) -> None:
        if not self.summary_text:
            raise ValidationError("Voice summary summary_text cannot be empty.")
        lower_text = self.summary_text.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", lower_text):
                raise ValidationError(
                    f"Forbidden trading or ranking term '{forbidden}' detected in voice summary: {self.summary_text}"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "evolution_id": self.evolution_id,
            "previous_snapshot_id": self.previous_snapshot_id,
            "current_snapshot_id": self.current_snapshot_id,
            "summary_text": self.summary_text,
            "voice_safe": self.voice_safe,
        }


@dataclass(frozen=True)
class HistoricalReviewSnapshot:
    """Immutable domain model capturing historical state of reviewed evidence.

    Captures ConsolidatedInvestigationOutcome and ConsolidatedOutcomeComparison states
    without mutating past snapshots, ranking strategies, or forecasting financial outcomes.
    """

    snapshot_id: str
    investigation_id: str
    outcome_id: str
    comparison_id: Optional[str] = None
    snapshot_version: int = 1
    captured_at: float = 0.0
    lifecycle_state: HistoricalSnapshotLifecycleState = HistoricalSnapshotLifecycleState.STORED
    source_state: Mapping[str, Any] = field(default_factory=dict)
    evidence_state: Mapping[str, Any] = field(default_factory=dict)
    review_state: Mapping[str, Any] = field(default_factory=dict)
    conflict_state: Mapping[str, Any] = field(default_factory=dict)
    quality_state: Mapping[str, Any] = field(default_factory=dict)
    provenance_state: Mapping[str, Any] = field(default_factory=dict)
    reproducibility_state: Mapping[str, Any] = field(default_factory=dict)
    coverage_state: Mapping[str, Any] = field(default_factory=dict)
    limitation_state: Tuple[Mapping[str, Any], ...] = ()
    source_references: Tuple[str, ...] = ()
    generated_from: str = "CONSOLIDATED_INVESTIGATION_OUTCOME"
    schema_version: str = "1.0.0"
    disclaimer: str = (
        "HISTORICAL REVIEW SNAPSHOT ONLY: This record captures historical review evidence and coverage state. "
        "It does not evaluate correctness, rank strategies, or recommend trades."
    )

    def __post_init__(self) -> None:
        # Enforce valid and non-empty IDs
        if not self.snapshot_id or not SAFE_ID_PATTERN.match(self.snapshot_id):
            raise ValidationError(f"Invalid snapshot_id format: '{self.snapshot_id}'")
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id format: '{self.investigation_id}'")
        if not self.outcome_id or not SAFE_ID_PATTERN.match(self.outcome_id):
            raise ValidationError(f"Invalid outcome_id format: '{self.outcome_id}'")
        if self.comparison_id and not SAFE_ID_PATTERN.match(self.comparison_id):
            raise ValidationError(f"Invalid comparison_id format: '{self.comparison_id}'")
        if self.snapshot_version < 1:
            raise ValidationError(f"snapshot_version must be >= 1 (got {self.snapshot_version})")

        validate_numerical_integrity(self.captured_at, "captured_at")
        validate_numerical_integrity(self.snapshot_version, "snapshot_version")

        # Freeze all collection fields into immutable representations
        object.__setattr__(self, "source_state", _freeze(self.source_state))
        object.__setattr__(self, "evidence_state", _freeze(self.evidence_state))
        object.__setattr__(self, "review_state", _freeze(self.review_state))
        object.__setattr__(self, "conflict_state", _freeze(self.conflict_state))
        object.__setattr__(self, "quality_state", _freeze(self.quality_state))
        object.__setattr__(self, "provenance_state", _freeze(self.provenance_state))
        object.__setattr__(self, "reproducibility_state", _freeze(self.reproducibility_state))
        object.__setattr__(self, "coverage_state", _freeze(self.coverage_state))
        object.__setattr__(self, "limitation_state", tuple(_freeze(item) for item in self.limitation_state))
        object.__setattr__(self, "source_references", tuple(self.source_references))

        # Enforce zero-prohibited-fields invariant
        validate_no_prohibited_fields(self.source_state, "source_state")
        validate_no_prohibited_fields(self.evidence_state, "evidence_state")
        validate_no_prohibited_fields(self.review_state, "review_state")
        validate_no_prohibited_fields(self.conflict_state, "conflict_state")
        validate_no_prohibited_fields(self.quality_state, "quality_state")
        validate_no_prohibited_fields(self.provenance_state, "provenance_state")
        validate_no_prohibited_fields(self.reproducibility_state, "reproducibility_state")
        validate_no_prohibited_fields(self.coverage_state, "coverage_state")
        validate_no_prohibited_fields(self.limitation_state, "limitation_state")

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to standard serializable dictionary."""
        return {
            "snapshot_id": self.snapshot_id,
            "investigation_id": self.investigation_id,
            "outcome_id": self.outcome_id,
            "comparison_id": self.comparison_id,
            "snapshot_version": self.snapshot_version,
            "captured_at": self.captured_at,
            "lifecycle_state": self.lifecycle_state.value,
            "source_state": _unfreeze(self.source_state),
            "evidence_state": _unfreeze(self.evidence_state),
            "review_state": _unfreeze(self.review_state),
            "conflict_state": _unfreeze(self.conflict_state),
            "quality_state": _unfreeze(self.quality_state),
            "provenance_state": _unfreeze(self.provenance_state),
            "reproducibility_state": _unfreeze(self.reproducibility_state),
            "coverage_state": _unfreeze(self.coverage_state),
            "limitation_state": [_unfreeze(item) for item in self.limitation_state],
            "source_references": list(self.source_references),
            "generated_from": self.generated_from,
            "schema_version": self.schema_version,
            "disclaimer": self.disclaimer,
        }


@dataclass(frozen=True)
class ReviewEvolutionRecord:
    """Immutable domain model representing deterministic changes between two historical snapshots.

    Answers "What changed between two reviewed evidence states?" without answering
    "Which state is better?", "Which strategy should be used?", or "Will this make money?".
    """

    evolution_id: str
    previous_snapshot_id: str
    current_snapshot_id: str
    investigation_id: str
    generated_at: float
    change_state: ChangeClassification
    changed_dimensions: Tuple[EvolutionDimensionRecord, ...] = ()
    added_evidence: Tuple[str, ...] = ()
    removed_evidence: Tuple[str, ...] = ()
    modified_evidence: Tuple[str, ...] = ()
    newly_resolved_discrepancies: Tuple[str, ...] = ()
    newly_open_discrepancies: Tuple[str, ...] = ()
    changed_review_decisions: Tuple[str, ...] = ()
    changed_quality: Tuple[str, ...] = ()
    changed_provenance: Tuple[str, ...] = ()
    changed_reproducibility: Tuple[str, ...] = ()
    changed_coverage: Tuple[str, ...] = ()
    changed_limitations: Tuple[str, ...] = ()
    traceability: Dict[str, Any] = field(default_factory=dict)
    voice_summary: Optional[ReviewEvolutionVoiceSummary] = None
    disclaimer: str = (
        "REVIEW EVOLUTION RECORD ONLY: Describes historical changes across reviewed evidence states. "
        "Does not determine superiority, predict return, or recommend trading execution."
    )

    def __post_init__(self) -> None:
        if not self.evolution_id or not SAFE_ID_PATTERN.match(self.evolution_id):
            raise ValidationError(f"Invalid evolution_id format: '{self.evolution_id}'")
        if not self.previous_snapshot_id or not SAFE_ID_PATTERN.match(self.previous_snapshot_id):
            raise ValidationError(f"Invalid previous_snapshot_id format: '{self.previous_snapshot_id}'")
        if not self.current_snapshot_id or not SAFE_ID_PATTERN.match(self.current_snapshot_id):
            raise ValidationError(f"Invalid current_snapshot_id format: '{self.current_snapshot_id}'")
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id format: '{self.investigation_id}'")

        validate_numerical_integrity(self.generated_at, "generated_at")

        object.__setattr__(self, "changed_dimensions", tuple(self.changed_dimensions))
        object.__setattr__(self, "added_evidence", tuple(self.added_evidence))
        object.__setattr__(self, "removed_evidence", tuple(self.removed_evidence))
        object.__setattr__(self, "modified_evidence", tuple(self.modified_evidence))
        object.__setattr__(self, "newly_resolved_discrepancies", tuple(self.newly_resolved_discrepancies))
        object.__setattr__(self, "newly_open_discrepancies", tuple(self.newly_open_discrepancies))
        object.__setattr__(self, "changed_review_decisions", tuple(self.changed_review_decisions))
        object.__setattr__(self, "changed_quality", tuple(self.changed_quality))
        object.__setattr__(self, "changed_provenance", tuple(self.changed_provenance))
        object.__setattr__(self, "changed_reproducibility", tuple(self.changed_reproducibility))
        object.__setattr__(self, "changed_coverage", tuple(self.changed_coverage))
        object.__setattr__(self, "changed_limitations", tuple(self.changed_limitations))
        object.__setattr__(self, "traceability", _freeze(self.traceability))

        validate_no_prohibited_fields(self.traceability, "traceability")

    def to_dict(self) -> Dict[str, Any]:
        """Convert evolution record to standard serializable dictionary."""
        return {
            "evolution_id": self.evolution_id,
            "previous_snapshot_id": self.previous_snapshot_id,
            "current_snapshot_id": self.current_snapshot_id,
            "investigation_id": self.investigation_id,
            "generated_at": self.generated_at,
            "change_state": self.change_state.value,
            "changed_dimensions": [d.to_dict() for d in self.changed_dimensions],
            "added_evidence": list(self.added_evidence),
            "removed_evidence": list(self.removed_evidence),
            "modified_evidence": list(self.modified_evidence),
            "newly_resolved_discrepancies": list(self.newly_resolved_discrepancies),
            "newly_open_discrepancies": list(self.newly_open_discrepancies),
            "changed_review_decisions": list(self.changed_review_decisions),
            "changed_quality": list(self.changed_quality),
            "changed_provenance": list(self.changed_provenance),
            "changed_reproducibility": list(self.changed_reproducibility),
            "changed_coverage": list(self.changed_coverage),
            "changed_limitations": list(self.changed_limitations),
            "traceability": _unfreeze(self.traceability),
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "disclaimer": self.disclaimer,
        }
