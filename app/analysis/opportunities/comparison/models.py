"""KAISER — Cross-Investigation Evidence Comparison & Review Consistency Domain Models.

Provides strongly-typed, immutable data structures for evaluating comparability,
evidence diffing, observation diffing, review consistency, and discrepancy detection
across completed analytical investigation reports.

SAFETY INVARIANTS:
1. Analytical and decision-support comparison only: NEVER ranks trades or asserts profitability.
2. Neutral comparison semantics: Reports differences, conflicts, and consistency objectively.
3. Strict numerical safety: Rejects NaN/Infinity, safely handles zero-baseline relative deltas.
4. Secret sanitization: Redacts credentials, tokens, and keys from all comparison artifacts.
5. Execution lock invariant: System execution remains locked.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
import re
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from app.analysis.opportunities.investigation.reporting.models import (
    AttributionType,
    EvidenceMatrixEntry,
    InvestigationReport,
    ObservationMatrixEntry,
    ReportStatus,
)
from app.core.exceptions import ValidationError

PROHIBITED_COMPARISON_FIELDS = frozenset({
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
})

FORBIDDEN_VOICE_WORDS = frozenset({
    "buy",
    "sell",
    "hold",
    "long",
    "short",
    "enter",
    "exit",
    "execute",
    "order",
    "leverage",
    "profit",
})


def validate_no_prohibited_fields(data: Any, path: str = "") -> None:
    """Recursively validate that no prohibited trade recommendation fields exist."""
    if isinstance(data, dict):
        for k, v in data.items():
            field_name = str(k).strip().lower()
            current_path = f"{path}.{field_name}" if path else field_name
            if field_name in PROHIBITED_COMPARISON_FIELDS:
                raise ValidationError(
                    f"Prohibited trade recommendation field detected at '{current_path}'. "
                    f"KAISER cross-investigation comparison is an analytical decision support layer only."
                )
            validate_no_prohibited_fields(v, current_path)
    elif isinstance(data, (list, tuple, set, frozenset)):
        for idx, item in enumerate(data):
            current_path = f"{path}[{idx}]"
            validate_no_prohibited_fields(item, current_path)


def validate_numerical_integrity(value: Any, field_name: str) -> None:
    """Validate numerical integrity following Phase 2 Step 18 rules."""
    if value is None:
        return
    if isinstance(value, (int, float)):
        if isinstance(value, float):
            if math.isnan(value):
                raise ValidationError(f"Invalid NaN encountered in '{field_name}'.")
            if math.isinf(value):
                raise ValidationError(f"Invalid Infinity encountered in '{field_name}'.")
    elif isinstance(value, (list, tuple)):
        for idx, item in enumerate(value):
            validate_numerical_integrity(item, f"{field_name}[{idx}]")
    elif isinstance(value, dict):
        for k, v in value.items():
            validate_numerical_integrity(v, f"{field_name}.{k}")


def safe_percentage_delta(left: Optional[float], right: Optional[float]) -> Tuple[Optional[float], Optional[str]]:
    """Compute relative percentage delta safely protecting against zero baseline and invalid floats.

    Returns:
        (delta_pct, reason): delta_pct is None if baseline is zero or values missing.
    """
    if left is None or right is None:
        return None, "One or both values unavailable."
    validate_numerical_integrity(left, "left")
    validate_numerical_integrity(right, "right")

    if abs(left) < 1e-12:
        if abs(right) < 1e-12:
            return 0.0, None
        return None, "Baseline value is zero; relative percentage change is mathematically undefined."

    delta = ((right - left) / abs(left)) * 100.0
    if math.isnan(delta) or math.isinf(delta):
        return None, "Non-finite delta encountered."
    return delta, None


class ComparabilityState(str, Enum):
    """Deterministic classification of whether investigation reports are comparable."""

    COMPARABLE = "COMPARABLE"
    PARTIALLY_COMPARABLE = "PARTIALLY_COMPARABLE"
    NOT_COMPARABLE = "NOT_COMPARABLE"
    INVALID_INPUT = "INVALID_INPUT"
    INCOMPLETE_EVIDENCE = "INCOMPLETE_EVIDENCE"

    @classmethod
    def from_string(cls, val: Union[str, ComparabilityState]) -> ComparabilityState:
        """Parse comparability state safely."""
        if isinstance(val, ComparabilityState):
            return val
        if not val or not isinstance(val, str):
            return cls.NOT_COMPARABLE
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.NOT_COMPARABLE


class EvidenceDiffCategory(str, Enum):
    """Classification of an evidence item across compared investigation reports."""

    UNCHANGED = "UNCHANGED"
    ADDED = "ADDED"
    REMOVED = "REMOVED"
    MODIFIED = "MODIFIED"
    CONFLICTING = "CONFLICTING"
    UNAVAILABLE = "UNAVAILABLE"

    @classmethod
    def from_string(cls, val: Union[str, EvidenceDiffCategory]) -> EvidenceDiffCategory:
        """Parse evidence diff category safely."""
        if isinstance(val, EvidenceDiffCategory):
            return val
        if not val or not isinstance(val, str):
            return cls.UNAVAILABLE
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.UNAVAILABLE


class ObservationDiffType(str, Enum):
    """Classification of observation continuity or variation across reports."""

    SHARED = "SHARED"
    CHANGED = "CHANGED"
    NEW = "NEW"
    RESOLVED = "RESOLVED"
    CLASSIFICATION_CHANGE = "CLASSIFICATION_CHANGE"


class DiscrepancyCategory(str, Enum):
    """Category of review or evidence discrepancy detected across reports."""

    EVIDENCE_DISPOSITION = "EVIDENCE_DISPOSITION"
    FACT_INTERPRETATION_MISMATCH = "FACT_INTERPRETATION_MISMATCH"
    PROVENANCE_MISMATCH = "PROVENANCE_MISMATCH"
    QUALITY_DEGRADATION = "QUALITY_DEGRADATION"
    CONCLUSION_DIVERGENCE = "CONCLUSION_DIVERGENCE"
    TIMEFRAME_MISMATCH = "TIMEFRAME_MISMATCH"
    SYMBOL_MISMATCH = "SYMBOL_MISMATCH"
    STALE_EVIDENCE = "STALE_EVIDENCE"
    UNRESOLVED_UNCERTAINTY = "UNRESOLVED_UNCERTAINTY"


class DiscrepancySeverity(str, Enum):
    """Severity rating of an investigation discrepancy."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DiscrepancyResolutionState(str, Enum):
    """Resolution state of an investigation discrepancy."""

    UNRESOLVED = "UNRESOLVED"
    EXPLAINED = "EXPLAINED"
    RESOLVED = "RESOLVED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True)
class EvidenceDiffItem:
    """Immutable diff record for a specific evidence item across compared reports."""

    evidence_id: str
    diff_category: EvidenceDiffCategory
    report_dispositions: Mapping[str, str]
    source_references: Tuple[str, ...]
    reason: str
    previous_state: Optional[Dict[str, Any]] = None
    current_state: Optional[Dict[str, Any]] = None
    evidence_type: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate evidence diff item."""
        if not self.evidence_id or not isinstance(self.evidence_id, str):
            raise ValidationError("EvidenceDiffItem evidence_id must be a non-empty string.")
        if not self.reason or not isinstance(self.reason, str):
            raise ValidationError("EvidenceDiffItem reason must be a non-empty string.")
        validate_no_prohibited_fields(self.previous_state or {})
        validate_no_prohibited_fields(self.current_state or {})

    def to_dict(self) -> Dict[str, Any]:
        """Convert evidence diff item to dictionary."""
        return {
            "evidence_id": self.evidence_id,
            "diff_category": self.diff_category.value,
            "report_dispositions": dict(self.report_dispositions),
            "source_references": list(self.source_references),
            "reason": self.reason,
            "previous_state": copy.deepcopy(self.previous_state) if self.previous_state else None,
            "current_state": copy.deepcopy(self.current_state) if self.current_state else None,
            "evidence_type": self.evidence_type,
        }


@dataclass(frozen=True)
class ObservationDiffItem:
    """Immutable diff record for human observations across compared reports.

    Never equates a FACT with an INTERPRETATION. Explicitly tracks classification changes.
    """

    observation_id: str
    diff_type: ObservationDiffType
    statement: str
    classification_change: bool
    old_type: Optional[str]
    new_type: Optional[str]
    report_presence: Tuple[str, ...]
    evidence_refs: Tuple[str, ...]
    reason: str

    def __post_init__(self) -> None:
        """Validate observation diff item."""
        if not self.observation_id or not isinstance(self.observation_id, str):
            raise ValidationError("ObservationDiffItem observation_id must be a non-empty string.")
        if not self.statement or not isinstance(self.statement, str):
            raise ValidationError("ObservationDiffItem statement must be a non-empty string.")

    def to_dict(self) -> Dict[str, Any]:
        """Convert observation diff item to dictionary."""
        return {
            "observation_id": self.observation_id,
            "diff_type": self.diff_type.value,
            "statement": self.statement,
            "classification_change": self.classification_change,
            "old_type": self.old_type,
            "new_type": self.new_type,
            "report_presence": list(self.report_presence),
            "evidence_refs": list(self.evidence_refs),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ProvenanceComparisonRecord:
    """Immutable comparison of data sources and provenance across reports."""

    is_identical: bool
    source_types: Mapping[str, Tuple[str, ...]]
    provenance_ids: Mapping[str, Tuple[str, ...]]
    shared_provenance_count: int
    differing_provenance_count: int
    notes: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        """Convert provenance comparison record to dictionary."""
        return {
            "is_identical": self.is_identical,
            "source_types": {k: list(v) for k, v in self.source_types.items()},
            "provenance_ids": {k: list(v) for k, v in self.provenance_ids.items()},
            "shared_provenance_count": self.shared_provenance_count,
            "differing_provenance_count": self.differing_provenance_count,
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class QualityComparisonRecord:
    """Immutable comparison of market data quality metrics across reports."""

    quality_statuses: Mapping[str, str]
    all_valid: bool
    status_divergence: bool
    freshness_seconds: Mapping[str, float]
    completeness_ratios: Mapping[str, float]
    anomaly_counts: Mapping[str, int]
    relative_completeness_change_pct: Optional[float] = None
    notes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate numerical integrity."""
        validate_numerical_integrity(dict(self.freshness_seconds), "freshness_seconds")
        validate_numerical_integrity(dict(self.completeness_ratios), "completeness_ratios")
        validate_numerical_integrity(dict(self.anomaly_counts), "anomaly_counts")
        validate_numerical_integrity(self.relative_completeness_change_pct, "relative_completeness_change_pct")

    def to_dict(self) -> Dict[str, Any]:
        """Convert quality comparison record to dictionary."""
        return {
            "quality_statuses": dict(self.quality_statuses),
            "all_valid": self.all_valid,
            "status_divergence": self.status_divergence,
            "freshness_seconds": dict(self.freshness_seconds),
            "completeness_ratios": dict(self.completeness_ratios),
            "anomaly_counts": dict(self.anomaly_counts),
            "relative_completeness_change_pct": self.relative_completeness_change_pct,
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class ReproducibilityComparisonRecord:
    """Immutable comparison of reproducibility hashes and runs across reports."""

    is_fully_reproducible: bool
    reproducibility_ids: Mapping[str, Optional[str]]
    shared_source_runs: Tuple[str, ...]
    differing_source_runs: Tuple[str, ...]
    config_fingerprints_match: bool
    unavailable_reasons: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        """Convert reproducibility comparison record to dictionary."""
        return {
            "is_fully_reproducible": self.is_fully_reproducible,
            "reproducibility_ids": dict(self.reproducibility_ids),
            "shared_source_runs": list(self.shared_source_runs),
            "differing_source_runs": list(self.differing_source_runs),
            "config_fingerprints_match": self.config_fingerprints_match,
            "unavailable_reasons": list(self.unavailable_reasons),
        }


@dataclass(frozen=True)
class ConclusionComparisonRecord:
    """Immutable comparison of analytical conclusions across reports without trading preferences."""

    conclusions_agree: bool
    conclusion_states: Mapping[str, str]
    rationales: Mapping[str, str]
    divergence_summary: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert conclusion comparison record to dictionary."""
        return {
            "conclusions_agree": self.conclusions_agree,
            "conclusion_states": dict(self.conclusion_states),
            "rationales": dict(self.rationales),
            "divergence_summary": self.divergence_summary,
        }


@dataclass(frozen=True)
class InvestigationDiscrepancy:
    """Immutable discrepancy record detected across compared reports."""

    discrepancy_id: str
    category: DiscrepancyCategory
    severity: DiscrepancySeverity
    report_ids: Tuple[str, ...]
    evidence_refs: Tuple[str, ...]
    description: str
    resolution_state: DiscrepancyResolutionState = DiscrepancyResolutionState.UNRESOLVED
    source_refs: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate discrepancy integrity."""
        if not self.discrepancy_id or not isinstance(self.discrepancy_id, str):
            raise ValidationError("InvestigationDiscrepancy discrepancy_id must be a non-empty string.")
        if not self.description or not isinstance(self.description, str):
            raise ValidationError("InvestigationDiscrepancy description must be a non-empty string.")

    def to_dict(self) -> Dict[str, Any]:
        """Convert discrepancy to dictionary."""
        return {
            "discrepancy_id": self.discrepancy_id,
            "category": self.category.value,
            "severity": self.severity.value,
            "report_ids": list(self.report_ids),
            "evidence_refs": list(self.evidence_refs),
            "description": self.description,
            "resolution_state": self.resolution_state.value,
            "source_refs": list(self.source_refs),
        }


@dataclass(frozen=True)
class ConsistencySummary:
    """Structured summary of review consistency across compared reports."""

    total_reports_compared: int
    comparable_reports: int
    partially_comparable_reports: int
    incompatible_reports: int
    shared_evidence_count: int
    changed_evidence_count: int
    conflicting_evidence_count: int
    unresolved_discrepancy_count: int
    new_uncertainty_count: int
    resolved_uncertainty_count: int
    common_limitation_count: int

    def __post_init__(self) -> None:
        """Validate counts."""
        for field_name in [
            "total_reports_compared",
            "comparable_reports",
            "partially_comparable_reports",
            "incompatible_reports",
            "shared_evidence_count",
            "changed_evidence_count",
            "conflicting_evidence_count",
            "unresolved_discrepancy_count",
            "new_uncertainty_count",
            "resolved_uncertainty_count",
            "common_limitation_count",
        ]:
            val = getattr(self, field_name)
            validate_numerical_integrity(val, field_name)
            if val < 0:
                raise ValidationError(f"ConsistencySummary {field_name} cannot be negative.")

    def to_dict(self) -> Dict[str, Any]:
        """Convert consistency summary to dictionary."""
        return {
            "total_reports_compared": self.total_reports_compared,
            "comparable_reports": self.comparable_reports,
            "partially_comparable_reports": self.partially_comparable_reports,
            "incompatible_reports": self.incompatible_reports,
            "shared_evidence_count": self.shared_evidence_count,
            "changed_evidence_count": self.changed_evidence_count,
            "conflicting_evidence_count": self.conflicting_evidence_count,
            "unresolved_discrepancy_count": self.unresolved_discrepancy_count,
            "new_uncertainty_count": self.new_uncertainty_count,
            "resolved_uncertainty_count": self.resolved_uncertainty_count,
            "common_limitation_count": self.common_limitation_count,
        }


@dataclass(frozen=True)
class InvestigationComparisonVoiceSummary:
    """Speech-ready comparison summary text strictly devoid of trading directives."""

    headline: str
    comparability_speech: str
    evidence_speech: str
    discrepancy_speech: str
    full_speech_script: str

    def __post_init__(self) -> None:
        """Enforce strict speech safety."""
        script_lower = self.full_speech_script.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", script_lower):
                raise ValidationError(
                    f"Forbidden trading directive or profit claim '{forbidden}' detected in voice summary script."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to dictionary."""
        return {
            "headline": self.headline,
            "comparability_speech": self.comparability_speech,
            "evidence_speech": self.evidence_speech,
            "discrepancy_speech": self.discrepancy_speech,
            "full_speech_script": self.full_speech_script,
        }


@dataclass(frozen=True)
class InvestigationComparison:
    """Immutable domain model of a Cross-Investigation Evidence Comparison.

    Provides a comprehensive, deterministic, auditable comparison across multiple
    completed investigation reports.

    ABSOLUTELY NOT A TRADING OR STRATEGY RANKING REPORT.
    """

    comparison_id: str
    report_ids: Tuple[str, ...]
    candidate_ids: Tuple[str, ...]
    comparison_status: str
    comparability_state: ComparabilityState
    generated_at: float
    shared_evidence: Tuple[EvidenceDiffItem, ...]
    changed_evidence: Tuple[EvidenceDiffItem, ...]
    conflicting_evidence: Tuple[EvidenceDiffItem, ...]
    missing_evidence: Tuple[EvidenceDiffItem, ...]
    observation_differences: Tuple[ObservationDiffItem, ...]
    uncertainty_differences: Tuple[Dict[str, Any], ...]
    limitation_differences: Tuple[Dict[str, Any], ...]
    provenance_comparison: ProvenanceComparisonRecord
    quality_comparison: QualityComparisonRecord
    reproducibility_comparison: ReproducibilityComparisonRecord
    conclusion_comparison: ConclusionComparisonRecord
    consistency_summary: ConsistencySummary
    discrepancy_summary: Tuple[InvestigationDiscrepancy, ...]
    traceability: Dict[str, Any]
    voice_summary: InvestigationComparisonVoiceSummary
    disclaimer: str = (
        "ANALYTICAL INVESTIGATION COMPARISON ONLY: This comparison layer reports analytical differences "
        "and review consistency only. It does not rank trades, predict profitability, or provide execution "
        "recommendations. Settings.execution_locked remains True."
    )

    def __post_init__(self) -> None:
        """Validate core comparison invariants, numerical safety, and prohibited fields."""
        if not self.comparison_id or not isinstance(self.comparison_id, str):
            raise ValidationError("InvestigationComparison comparison_id must be a non-empty string.")
        if len(self.report_ids) < 2:
            raise ValidationError("InvestigationComparison requires at least 2 report_ids.")

        validate_numerical_integrity(self.generated_at, "generated_at")
        if self.generated_at < 0.0:
            raise ValidationError("generated_at cannot be negative.")

        validate_no_prohibited_fields(self.traceability, path="traceability")
        for u in self.uncertainty_differences:
            validate_no_prohibited_fields(u, path="uncertainty_differences")
        for l in self.limitation_differences:
            validate_no_prohibited_fields(l, path="limitation_differences")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize comparison to dictionary."""
        return {
            "comparison_id": self.comparison_id,
            "report_ids": list(self.report_ids),
            "candidate_ids": list(self.candidate_ids),
            "comparison_status": self.comparison_status,
            "comparability_state": self.comparability_state.value,
            "generated_at": self.generated_at,
            "shared_evidence": [e.to_dict() for e in self.shared_evidence],
            "changed_evidence": [e.to_dict() for e in self.changed_evidence],
            "conflicting_evidence": [e.to_dict() for e in self.conflicting_evidence],
            "missing_evidence": [e.to_dict() for e in self.missing_evidence],
            "observation_differences": [o.to_dict() for o in self.observation_differences],
            "uncertainty_differences": copy.deepcopy(list(self.uncertainty_differences)),
            "limitation_differences": copy.deepcopy(list(self.limitation_differences)),
            "provenance_comparison": self.provenance_comparison.to_dict(),
            "quality_comparison": self.quality_comparison.to_dict(),
            "reproducibility_comparison": self.reproducibility_comparison.to_dict(),
            "conclusion_comparison": self.conclusion_comparison.to_dict(),
            "consistency_summary": self.consistency_summary.to_dict(),
            "discrepancy_summary": [d.to_dict() for d in self.discrepancy_summary],
            "traceability": copy.deepcopy(self.traceability),
            "voice_summary": self.voice_summary.to_dict(),
            "disclaimer": self.disclaimer,
        }


def compute_deterministic_comparison_id(
    report_ids: Sequence[str],
    report_versions: Optional[Sequence[int]] = None,
    reproducibility_ids: Optional[Sequence[Optional[str]]] = None,
    config_salt: str = "v1",
) -> str:
    """Generate a deterministic comparison ID using canonical SHA-256 fingerprinting.

    Sorts report IDs alphabetically so order of arguments does not affect the comparison ID.
    """
    sorted_ids = sorted([str(r).strip() for r in report_ids if r])
    if not sorted_ids:
        raise ValidationError("Cannot compute comparison_id with empty report_ids.")

    canonical_payload = {
        "report_ids": sorted_ids,
        "report_versions": list(report_versions) if report_versions else [],
        "reproducibility_ids": [str(r) for r in reproducibility_ids if r] if reproducibility_ids else [],
        "config_salt": config_salt,
    }
    encoded = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"cmp_{digest[:16]}"
