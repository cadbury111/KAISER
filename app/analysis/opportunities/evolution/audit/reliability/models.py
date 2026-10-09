"""KAISER — Historical Review Audit Coverage, Regression Monitoring & Reliability Models.

Phase 2, Step 34: Reliability validation models for review audit coverage, repeatability,
regression drift monitoring, and long-term analytical reproducibility.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support & analytical review audit only:
   Never outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE directives.
2. Absolutely no order execution fields: Prohibits action, order, target_price,
   position_size, stop_loss, win_probability, expected_return, or strategy_ranking.
3. No profitability claims: No numeric probability of profit or ranking claims.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
5. Strict append-only immutability: Records are frozen and never mutated or silently overwritten.
6. Execution engine remains locked: Settings.execution_locked == True.
7. Trading engine remains uninitialized.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from app.analysis.opportunities.evolution.audit.models import (
    RecordValidationStatus,
)
from app.analysis.opportunities.evolution.models import (
    FORBIDDEN_VOICE_WORDS,
    MAX_STRING_LENGTH,
    SAFE_ID_PATTERN,
    _freeze,
    _unfreeze,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.core.exceptions import ValidationError

MAX_RELIABILITY_EVALUATIONS_PER_INVESTIGATION = 100


# ==============================================================================
# Enumerations
# ==============================================================================

class AuditCoverageState(str, Enum):
    """Classification of audit coverage over available historical review entities."""
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    EMPTY = "EMPTY"
    UNAVAILABLE = "UNAVAILABLE"


class AuditRepeatabilityState(str, Enum):
    """Repeatability consistency outcome when re-auditing identical inputs."""
    STABLE = "STABLE"
    DIVERGENT = "DIVERGENT"
    UNVERIFIABLE = "UNVERIFIABLE"
    FAILED = "FAILED"


class AuditRegressionState(str, Enum):
    """Regression drift classification between successive historical review audits."""
    NO_REGRESSION = "NO_REGRESSION"
    REGRESSION_DETECTED = "REGRESSION_DETECTED"
    IMPROVEMENT_DETECTED = "IMPROVEMENT_DETECTED"
    BASELINE_ESTABLISHED = "BASELINE_ESTABLISHED"
    UNVERIFIABLE = "UNVERIFIABLE"


class ReliabilityEvaluationStatus(str, Enum):
    """Overall reliability status of the historical review audit subsystem."""
    RELIABLE = "RELIABLE"
    DEGRADED = "DEGRADED"
    UNRELIABLE = "UNRELIABLE"
    UNVERIFIABLE = "UNVERIFIABLE"
    FAILED = "FAILED"


# ==============================================================================
# Deterministic Identity Helpers
# ==============================================================================

def compute_deterministic_reliability_id(
    investigation_id: str,
    coverage_state: str,
    reliability_status: str,
    fingerprint: str,
) -> str:
    """Compute a deterministic SHA-256 ID for a review audit reliability evaluation."""
    clean_inv = (investigation_id or "").strip()
    clean_cov = (coverage_state or "").strip()
    clean_status = (reliability_status or "").strip()
    clean_fp = (fingerprint or "").strip()

    raw_data = json.dumps(
        {
            "investigation_id": clean_inv,
            "coverage_state": clean_cov,
            "reliability_status": clean_status,
            "fingerprint": clean_fp,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    digest = hashlib.sha256(raw_data).hexdigest()[:24]
    return f"revrel_{digest}"


# ==============================================================================
# Domain Models (Frozen Dataclasses)
# ==============================================================================

@dataclass(frozen=True)
class AuditCoverageSummary:
    """Immutable summary of audit coverage across historical review snapshots and evolutions."""

    investigation_id: str
    coverage_state: AuditCoverageState
    total_snapshots: int
    audited_snapshots: int
    snapshot_coverage_ratio: float
    total_evolutions: int
    audited_evolutions: int
    evolution_coverage_ratio: float
    overall_coverage_ratio: float
    unaudited_snapshot_ids: Tuple[str, ...] = ()
    unaudited_evolution_ids: Tuple[str, ...] = ()
    missing_history_detected: bool = False
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id format: '{self.investigation_id}'")

        validate_numerical_integrity(self.total_snapshots, "total_snapshots")
        validate_numerical_integrity(self.audited_snapshots, "audited_snapshots")
        validate_numerical_integrity(self.snapshot_coverage_ratio, "snapshot_coverage_ratio")
        validate_numerical_integrity(self.total_evolutions, "total_evolutions")
        validate_numerical_integrity(self.audited_evolutions, "audited_evolutions")
        validate_numerical_integrity(self.evolution_coverage_ratio, "evolution_coverage_ratio")
        validate_numerical_integrity(self.overall_coverage_ratio, "overall_coverage_ratio")

        if not (0.0 <= self.snapshot_coverage_ratio <= 1.0):
            raise ValidationError(
                f"snapshot_coverage_ratio must be in [0.0, 1.0], got {self.snapshot_coverage_ratio}"
            )
        if not (0.0 <= self.evolution_coverage_ratio <= 1.0):
            raise ValidationError(
                f"evolution_coverage_ratio must be in [0.0, 1.0], got {self.evolution_coverage_ratio}"
            )
        if not (0.0 <= self.overall_coverage_ratio <= 1.0):
            raise ValidationError(
                f"overall_coverage_ratio must be in [0.0, 1.0], got {self.overall_coverage_ratio}"
            )

        object.__setattr__(self, "unaudited_snapshot_ids", tuple(self.unaudited_snapshot_ids))
        object.__setattr__(self, "unaudited_evolution_ids", tuple(self.unaudited_evolution_ids))
        object.__setattr__(self, "details", _freeze(self.details))

        validate_no_prohibited_fields(self.details, "details")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "investigation_id": self.investigation_id,
            "coverage_state": self.coverage_state.value,
            "total_snapshots": self.total_snapshots,
            "audited_snapshots": self.audited_snapshots,
            "snapshot_coverage_ratio": self.snapshot_coverage_ratio,
            "total_evolutions": self.total_evolutions,
            "audited_evolutions": self.audited_evolutions,
            "evolution_coverage_ratio": self.evolution_coverage_ratio,
            "overall_coverage_ratio": self.overall_coverage_ratio,
            "unaudited_snapshot_ids": list(self.unaudited_snapshot_ids),
            "unaudited_evolution_ids": list(self.unaudited_evolution_ids),
            "missing_history_detected": self.missing_history_detected,
            "details": _unfreeze(self.details),
        }


@dataclass(frozen=True)
class AuditRepeatabilityResult:
    """Validation result assessing repeatability of audit findings on identical inputs."""

    investigation_id: str
    repeatability_state: AuditRepeatabilityState
    repeatability_score: float
    is_repeatable: bool
    report_id_1: str
    report_id_2: str
    report_ids_match: bool
    findings_count_1: int
    findings_count_2: int
    findings_match: bool
    status_1: RecordValidationStatus
    status_2: RecordValidationStatus
    status_match: bool
    divergent_fields: Tuple[str, ...] = ()
    divergence_reasons: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id format: '{self.investigation_id}'")
        if not self.report_id_1 or not SAFE_ID_PATTERN.match(self.report_id_1):
            raise ValidationError(f"Invalid report_id_1 format: '{self.report_id_1}'")
        if not self.report_id_2 or not SAFE_ID_PATTERN.match(self.report_id_2):
            raise ValidationError(f"Invalid report_id_2 format: '{self.report_id_2}'")

        validate_numerical_integrity(self.repeatability_score, "repeatability_score")
        validate_numerical_integrity(self.findings_count_1, "findings_count_1")
        validate_numerical_integrity(self.findings_count_2, "findings_count_2")

        if not (0.0 <= self.repeatability_score <= 1.0):
            raise ValidationError(
                f"repeatability_score must be in [0.0, 1.0], got {self.repeatability_score}"
            )

        object.__setattr__(self, "divergent_fields", tuple(self.divergent_fields))
        object.__setattr__(self, "divergence_reasons", tuple(self.divergence_reasons))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "investigation_id": self.investigation_id,
            "repeatability_state": self.repeatability_state.value,
            "repeatability_score": self.repeatability_score,
            "is_repeatable": self.is_repeatable,
            "report_id_1": self.report_id_1,
            "report_id_2": self.report_id_2,
            "report_ids_match": self.report_ids_match,
            "findings_count_1": self.findings_count_1,
            "findings_count_2": self.findings_count_2,
            "findings_match": self.findings_match,
            "status_1": self.status_1.value,
            "status_2": self.status_2.value,
            "status_match": self.status_match,
            "divergent_fields": list(self.divergent_fields),
            "divergence_reasons": list(self.divergence_reasons),
        }


@dataclass(frozen=True)
class AuditRegressionResult:
    """Validation result assessing finding drift and regressions across successive audits."""

    investigation_id: str
    regression_state: AuditRegressionState
    baseline_report_id: Optional[str]
    current_report_id: str
    new_findings_count: int
    resolved_findings_count: int
    persistent_findings_count: int
    new_finding_ids: Tuple[str, ...] = ()
    resolved_finding_ids: Tuple[str, ...] = ()
    persistent_finding_ids: Tuple[str, ...] = ()
    regression_severities: Tuple[str, ...] = ()
    conflict_preservation_maintained: bool = True
    regression_summary: str = ""

    def __post_init__(self) -> None:
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id format: '{self.investigation_id}'")
        if self.baseline_report_id and not SAFE_ID_PATTERN.match(self.baseline_report_id):
            raise ValidationError(f"Invalid baseline_report_id format: '{self.baseline_report_id}'")
        if not self.current_report_id or not SAFE_ID_PATTERN.match(self.current_report_id):
            raise ValidationError(f"Invalid current_report_id format: '{self.current_report_id}'")

        validate_numerical_integrity(self.new_findings_count, "new_findings_count")
        validate_numerical_integrity(self.resolved_findings_count, "resolved_findings_count")
        validate_numerical_integrity(self.persistent_findings_count, "persistent_findings_count")

        object.__setattr__(self, "new_finding_ids", tuple(self.new_finding_ids))
        object.__setattr__(self, "resolved_finding_ids", tuple(self.resolved_finding_ids))
        object.__setattr__(self, "persistent_finding_ids", tuple(self.persistent_finding_ids))
        object.__setattr__(self, "regression_severities", tuple(self.regression_severities))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "investigation_id": self.investigation_id,
            "regression_state": self.regression_state.value,
            "baseline_report_id": self.baseline_report_id,
            "current_report_id": self.current_report_id,
            "new_findings_count": self.new_findings_count,
            "resolved_findings_count": self.resolved_findings_count,
            "persistent_findings_count": self.persistent_findings_count,
            "new_finding_ids": list(self.new_finding_ids),
            "resolved_finding_ids": list(self.resolved_finding_ids),
            "persistent_finding_ids": list(self.persistent_finding_ids),
            "regression_severities": list(self.regression_severities),
            "conflict_preservation_maintained": self.conflict_preservation_maintained,
            "regression_summary": self.regression_summary,
        }


@dataclass(frozen=True)
class AuditReliabilityVoiceSummary:
    """Voice-safe summary of historical review audit reliability evaluation."""

    evaluation_id: str
    investigation_id: str
    reliability_status: ReliabilityEvaluationStatus
    summary_text: str
    voice_safe: bool = True

    def __post_init__(self) -> None:
        if not self.summary_text or not self.summary_text.strip():
            raise ValidationError("AuditReliabilityVoiceSummary summary_text cannot be empty.")

        lower_text = self.summary_text.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", lower_text):
                raise ValidationError(
                    f"Forbidden trading or ranking term '{forbidden}' detected in voice summary: {self.summary_text}"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "evaluation_id": self.evaluation_id,
            "investigation_id": self.investigation_id,
            "reliability_status": self.reliability_status.value,
            "summary_text": self.summary_text,
            "voice_safe": self.voice_safe,
        }


@dataclass(frozen=True)
class HistoricalReviewReliabilityResult:
    """Comprehensive reliability evaluation for historical review audit subsystem."""

    evaluation_id: str
    investigation_id: str
    evaluated_at: float
    overall_reliability_status: ReliabilityEvaluationStatus
    coverage_summary: AuditCoverageSummary
    repeatability_result: Optional[AuditRepeatabilityResult] = None
    regression_result: Optional[AuditRegressionResult] = None
    facts_count: int = 0
    interpretations_count: int = 0
    human_decisions_count: int = 0
    active_conflicts_preserved_count: int = 0
    controlled_failure_recovered: bool = False
    recovery_notes: Tuple[str, ...] = ()
    voice_summary: Optional[AuditReliabilityVoiceSummary] = None
    schema_version: str = "1.0.0"
    disclaimer: str = (
        "HISTORICAL REVIEW AUDIT RELIABILITY EVALUATION: Assesses audit coverage, repeatability, "
        "deterministic stability, and finding regression over review history. "
        "Does not produce trading signals, profitability estimates, or strategy execution recommendations."
    )

    def __post_init__(self) -> None:
        if not self.evaluation_id or not SAFE_ID_PATTERN.match(self.evaluation_id):
            raise ValidationError(f"Invalid evaluation_id format: '{self.evaluation_id}'")
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id format: '{self.investigation_id}'")

        validate_numerical_integrity(self.evaluated_at, "evaluated_at")
        validate_numerical_integrity(self.facts_count, "facts_count")
        validate_numerical_integrity(self.interpretations_count, "interpretations_count")
        validate_numerical_integrity(self.human_decisions_count, "human_decisions_count")
        validate_numerical_integrity(self.active_conflicts_preserved_count, "active_conflicts_preserved_count")

        object.__setattr__(self, "recovery_notes", tuple(self.recovery_notes))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "evaluation_id": self.evaluation_id,
            "investigation_id": self.investigation_id,
            "evaluated_at": self.evaluated_at,
            "overall_reliability_status": self.overall_reliability_status.value,
            "coverage_summary": self.coverage_summary.to_dict(),
            "repeatability_result": self.repeatability_result.to_dict() if self.repeatability_result else None,
            "regression_result": self.regression_result.to_dict() if self.regression_result else None,
            "facts_count": self.facts_count,
            "interpretations_count": self.interpretations_count,
            "human_decisions_count": self.human_decisions_count,
            "active_conflicts_preserved_count": self.active_conflicts_preserved_count,
            "controlled_failure_recovered": self.controlled_failure_recovered,
            "recovery_notes": list(self.recovery_notes),
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "schema_version": self.schema_version,
            "disclaimer": self.disclaimer,
        }
