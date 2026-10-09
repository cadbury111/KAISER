"""KAISER — Historical Review Evolution Audit, Integrity & Reproducibility Domain Models.

Phase 2, Step 33: Auditing foundation for historical review snapshots, evolution deltas,
chain continuity, deterministic identity verification, and reproducibility metadata.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support & analytical review audit only:
   Never outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE directives.
2. Absolutely no order execution fields: Prohibits action, order, target_price,
   position_size, stop_loss, win_probability, expected_return, or strategy_ranking.
3. No profitability claims: No numeric probability of profit or ranking claims.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
5. Strict append-only immutability: Snapshots and audit records are never mutated or silently overwritten.
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

from app.analysis.opportunities.evolution.models import (
    FORBIDDEN_VOICE_WORDS,
    MAX_STRING_LENGTH,
    PROHIBITED_RECOMMENDATION_FIELDS,
    SAFE_ID_PATTERN,
    ReproducibilityEvolutionState,
    _freeze,
    _unfreeze,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.core.exceptions import ValidationError

MAX_AUDIT_REPORTS_PER_INVESTIGATION = 100
MAX_AUDIT_FINDINGS_PER_REPORT = 500


# ==============================================================================
# Enumerations
# ==============================================================================

class AuditFindingSeverity(str, Enum):
    """Severity classification for review evolution audit findings."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AuditFindingCategory(str, Enum):
    """Categorical classification of audit validation findings."""
    STRUCTURAL_INTEGRITY = "STRUCTURAL_INTEGRITY"
    IDENTITY_DETERMINISM = "IDENTITY_DETERMINISM"
    CHRONOLOGICAL_ORDERING = "CHRONOLOGICAL_ORDERING"
    HISTORICAL_GAP = "HISTORICAL_GAP"
    DUPLICATE_IDENTIFIER = "DUPLICATE_IDENTIFIER"
    BROKEN_REFERENCE = "BROKEN_REFERENCE"
    INCONSISTENT_REFERENCE = "INCONSISTENT_REFERENCE"
    REPRODUCIBILITY_METADATA = "REPRODUCIBILITY_METADATA"
    CONFLICT_PRESERVATION = "CONFLICT_PRESERVATION"
    CLAIM_SEPARATION = "CLAIM_SEPARATION"
    DATA_QUALITY_DEGRADATION = "DATA_QUALITY_DEGRADATION"
    PROHIBITED_CONTENT = "PROHIBITED_CONTENT"


class RecordValidationStatus(str, Enum):
    """Validation outcome for a historical review record, chain, or audit report."""
    VALID = "VALID"
    INCONSISTENT = "INCONSISTENT"
    INCOMPLETE = "INCOMPLETE"
    UNVERIFIABLE = "UNVERIFIABLE"
    FAILED = "FAILED"


class AuditReportLifecycleState(str, Enum):
    """Lifecycle state of an audit report."""
    GENERATED = "GENERATED"
    STORED = "STORED"
    FAILED = "FAILED"

    @property
    def is_terminal(self) -> bool:
        """Return True if the audit report lifecycle state is permanently sealed."""
        return self in (AuditReportLifecycleState.STORED, AuditReportLifecycleState.FAILED)


# ==============================================================================
# Deterministic Identity Helpers
# ==============================================================================

def compute_deterministic_audit_report_id(
    investigation_id: str,
    snapshot_ids: Sequence[str],
    evolution_ids: Sequence[str],
    overall_status: str,
) -> str:
    """Compute a canonical deterministic SHA-256 ID for a review audit report."""
    clean_inv = (investigation_id or "").strip()
    sorted_snaps = sorted(str(s).strip() for s in snapshot_ids)
    sorted_evols = sorted(str(e).strip() for e in evolution_ids)
    clean_status = (overall_status or "").strip()

    raw_data = json.dumps(
        {
            "investigation_id": clean_inv,
            "snapshot_ids": sorted_snaps,
            "evolution_ids": sorted_evols,
            "overall_status": clean_status,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    digest = hashlib.sha256(raw_data).hexdigest()[:24]
    return f"revaudit_{digest}"


# ==============================================================================
# Domain Models (Frozen Dataclasses)
# ==============================================================================

@dataclass(frozen=True)
class EvolutionAuditFinding:
    """An individual validation finding identified during review evolution audit."""

    finding_id: str
    category: AuditFindingCategory
    severity: AuditFindingSeverity
    target_type: str  # SNAPSHOT, EVOLUTION, CHAIN, INVESTIGATION
    target_id: str
    field_path: str
    message: str
    observed_value: Optional[str] = None
    expected_value: Optional[str] = None
    source_reference: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.finding_id or not SAFE_ID_PATTERN.match(self.finding_id):
            raise ValidationError(f"Invalid finding_id format: '{self.finding_id}'")
        if not self.target_id or not SAFE_ID_PATTERN.match(self.target_id):
            raise ValidationError(f"Invalid target_id format: '{self.target_id}'")
        if not self.message or not self.message.strip():
            raise ValidationError("EvolutionAuditFinding message cannot be empty.")
        if len(self.message) > MAX_STRING_LENGTH:
            raise ValidationError(f"EvolutionAuditFinding message exceeds max length of {MAX_STRING_LENGTH}.")

        validate_no_prohibited_fields(self.to_dict(), path="finding")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "category": self.category.value,
            "severity": self.severity.value,
            "target_type": self.target_type,
            "target_id": self.target_id,
            "field_path": self.field_path,
            "message": self.message,
            "observed_value": self.observed_value,
            "expected_value": self.expected_value,
            "source_reference": self.source_reference,
        }


@dataclass(frozen=True)
class SnapshotIntegrityResult:
    """Validation result for an individual historical review snapshot."""

    snapshot_id: str
    investigation_id: str
    status: RecordValidationStatus
    hash_verified: bool
    recalculated_id: Optional[str] = None
    chronology_valid: bool = True
    has_prohibited_terms: bool = False
    facts_count: int = 0
    interpretations_count: int = 0
    human_decisions_count: int = 0
    active_conflicts_count: int = 0
    findings: Tuple[EvolutionAuditFinding, ...] = ()

    def __post_init__(self) -> None:
        if not self.snapshot_id or not SAFE_ID_PATTERN.match(self.snapshot_id):
            raise ValidationError(f"Invalid snapshot_id: '{self.snapshot_id}'")
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id: '{self.investigation_id}'")

        validate_numerical_integrity(self.facts_count, "facts_count")
        validate_numerical_integrity(self.interpretations_count, "interpretations_count")
        validate_numerical_integrity(self.human_decisions_count, "human_decisions_count")
        validate_numerical_integrity(self.active_conflicts_count, "active_conflicts_count")

        object.__setattr__(self, "findings", tuple(self.findings))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "investigation_id": self.investigation_id,
            "status": self.status.value,
            "hash_verified": self.hash_verified,
            "recalculated_id": self.recalculated_id,
            "chronology_valid": self.chronology_valid,
            "has_prohibited_terms": self.has_prohibited_terms,
            "facts_count": self.facts_count,
            "interpretations_count": self.interpretations_count,
            "human_decisions_count": self.human_decisions_count,
            "active_conflicts_count": self.active_conflicts_count,
            "findings": [f.to_dict() for f in self.findings],
        }


@dataclass(frozen=True)
class EvolutionRecordIntegrityResult:
    """Validation result for an individual review evolution delta record."""

    evolution_id: str
    previous_snapshot_id: str
    current_snapshot_id: str
    investigation_id: str
    status: RecordValidationStatus
    hash_verified: bool
    recalculated_id: Optional[str] = None
    references_exist: bool = True
    dimensions_evaluated_count: int = 0
    findings: Tuple[EvolutionAuditFinding, ...] = ()

    def __post_init__(self) -> None:
        if not self.evolution_id or not SAFE_ID_PATTERN.match(self.evolution_id):
            raise ValidationError(f"Invalid evolution_id: '{self.evolution_id}'")
        if not self.previous_snapshot_id or not SAFE_ID_PATTERN.match(self.previous_snapshot_id):
            raise ValidationError(f"Invalid previous_snapshot_id: '{self.previous_snapshot_id}'")
        if not self.current_snapshot_id or not SAFE_ID_PATTERN.match(self.current_snapshot_id):
            raise ValidationError(f"Invalid current_snapshot_id: '{self.current_snapshot_id}'")
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id: '{self.investigation_id}'")

        validate_numerical_integrity(self.dimensions_evaluated_count, "dimensions_evaluated_count")
        object.__setattr__(self, "findings", tuple(self.findings))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "evolution_id": self.evolution_id,
            "previous_snapshot_id": self.previous_snapshot_id,
            "current_snapshot_id": self.current_snapshot_id,
            "investigation_id": self.investigation_id,
            "status": self.status.value,
            "hash_verified": self.hash_verified,
            "recalculated_id": self.recalculated_id,
            "references_exist": self.references_exist,
            "dimensions_evaluated_count": self.dimensions_evaluated_count,
            "findings": [f.to_dict() for f in self.findings],
        }


@dataclass(frozen=True)
class HistoricalChainIntegrityResult:
    """Validation result for the sequence of historical review snapshots and evolutions."""

    investigation_id: str
    chain_length: int
    status: RecordValidationStatus
    version_sequence_intact: bool
    chronology_intact: bool
    detected_gaps: Tuple[str, ...] = ()
    duplicate_ids_detected: Tuple[str, ...] = ()
    broken_links_detected: Tuple[str, ...] = ()
    recurring_conflicts_preserved: bool = True
    findings: Tuple[EvolutionAuditFinding, ...] = ()

    def __post_init__(self) -> None:
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id: '{self.investigation_id}'")

        validate_numerical_integrity(self.chain_length, "chain_length")
        object.__setattr__(self, "detected_gaps", tuple(self.detected_gaps))
        object.__setattr__(self, "duplicate_ids_detected", tuple(self.duplicate_ids_detected))
        object.__setattr__(self, "broken_links_detected", tuple(self.broken_links_detected))
        object.__setattr__(self, "findings", tuple(self.findings))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "investigation_id": self.investigation_id,
            "chain_length": self.chain_length,
            "status": self.status.value,
            "version_sequence_intact": self.version_sequence_intact,
            "chronology_intact": self.chronology_intact,
            "detected_gaps": list(self.detected_gaps),
            "duplicate_ids_detected": list(self.duplicate_ids_detected),
            "broken_links_detected": list(self.broken_links_detected),
            "recurring_conflicts_preserved": self.recurring_conflicts_preserved,
            "findings": [f.to_dict() for f in self.findings],
        }


@dataclass(frozen=True)
class EvolutionReproducibilityVerificationResult:
    """Reproducibility metadata validation result for review evolution entities."""

    target_id: str
    target_type: str
    reproducibility_status: ReproducibilityEvolutionState
    is_verifiable: bool
    lineage_linked: bool
    provenance_sources_count: int
    analysis_runs_linked: Tuple[str, ...] = ()
    unverifiable_reasons: Tuple[str, ...] = ()
    findings: Tuple[EvolutionAuditFinding, ...] = ()

    def __post_init__(self) -> None:
        if not self.target_id or not SAFE_ID_PATTERN.match(self.target_id):
            raise ValidationError(f"Invalid target_id: '{self.target_id}'")

        validate_numerical_integrity(self.provenance_sources_count, "provenance_sources_count")
        object.__setattr__(self, "analysis_runs_linked", tuple(self.analysis_runs_linked))
        object.__setattr__(self, "unverifiable_reasons", tuple(self.unverifiable_reasons))
        object.__setattr__(self, "findings", tuple(self.findings))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target_id": self.target_id,
            "target_type": self.target_type,
            "reproducibility_status": self.reproducibility_status.value,
            "is_verifiable": self.is_verifiable,
            "lineage_linked": self.lineage_linked,
            "provenance_sources_count": self.provenance_sources_count,
            "analysis_runs_linked": list(self.analysis_runs_linked),
            "unverifiable_reasons": list(self.unverifiable_reasons),
            "findings": [f.to_dict() for f in self.findings],
        }


@dataclass(frozen=True)
class EvolutionAuditVoiceSummary:
    """Voice-safe summary of review evolution audit findings."""

    report_id: str
    investigation_id: str
    overall_status: RecordValidationStatus
    summary_text: str
    voice_safe: bool = True

    def __post_init__(self) -> None:
        if not self.summary_text or not self.summary_text.strip():
            raise ValidationError("EvolutionAuditVoiceSummary summary_text cannot be empty.")

        lower_text = self.summary_text.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", lower_text):
                raise ValidationError(
                    f"Forbidden trading or ranking term '{forbidden}' detected in voice summary: {self.summary_text}"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_id": self.report_id,
            "investigation_id": self.investigation_id,
            "overall_status": self.overall_status.value,
            "summary_text": self.summary_text,
            "voice_safe": self.voice_safe,
        }


@dataclass(frozen=True)
class HistoricalReviewAuditReport:
    """Immutable root domain model capturing full review evolution audit results."""

    report_id: str
    investigation_id: str
    generated_at: float
    lifecycle_state: AuditReportLifecycleState = AuditReportLifecycleState.STORED
    overall_status: RecordValidationStatus = RecordValidationStatus.VALID
    snapshots_audited_count: int = 0
    evolutions_audited_count: int = 0
    findings_summary: Mapping[str, int] = field(default_factory=dict)
    snapshot_results: Tuple[SnapshotIntegrityResult, ...] = ()
    evolution_results: Tuple[EvolutionRecordIntegrityResult, ...] = ()
    chain_result: Optional[HistoricalChainIntegrityResult] = None
    reproducibility_result: Optional[EvolutionReproducibilityVerificationResult] = None
    findings: Tuple[EvolutionAuditFinding, ...] = ()
    voice_summary: Optional[EvolutionAuditVoiceSummary] = None
    traceability: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = "1.0.0"
    disclaimer: str = (
        "HISTORICAL REVIEW AUDIT REPORT ONLY: Validates structural integrity, chronological consistency, "
        "deterministic hashes, and reproducibility metadata of historical review records. "
        "Does not determine correctness, rank strategies, or recommend trading execution."
    )

    def __post_init__(self) -> None:
        if not self.report_id or not SAFE_ID_PATTERN.match(self.report_id):
            raise ValidationError(f"Invalid report_id format: '{self.report_id}'")
        if not self.investigation_id or not SAFE_ID_PATTERN.match(self.investigation_id):
            raise ValidationError(f"Invalid investigation_id format: '{self.investigation_id}'")

        validate_numerical_integrity(self.generated_at, "generated_at")
        validate_numerical_integrity(self.snapshots_audited_count, "snapshots_audited_count")
        validate_numerical_integrity(self.evolutions_audited_count, "evolutions_audited_count")

        object.__setattr__(self, "findings_summary", _freeze(self.findings_summary))
        object.__setattr__(self, "snapshot_results", tuple(self.snapshot_results))
        object.__setattr__(self, "evolution_results", tuple(self.evolution_results))
        object.__setattr__(self, "findings", tuple(self.findings))
        object.__setattr__(self, "traceability", _freeze(self.traceability))

        validate_no_prohibited_fields(self.findings_summary, "findings_summary")
        validate_no_prohibited_fields(self.traceability, "traceability")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_id": self.report_id,
            "investigation_id": self.investigation_id,
            "generated_at": self.generated_at,
            "lifecycle_state": self.lifecycle_state.value,
            "overall_status": self.overall_status.value,
            "snapshots_audited_count": self.snapshots_audited_count,
            "evolutions_audited_count": self.evolutions_audited_count,
            "findings_summary": _unfreeze(self.findings_summary),
            "snapshot_results": [s.to_dict() for s in self.snapshot_results],
            "evolution_results": [e.to_dict() for e in self.evolution_results],
            "chain_result": self.chain_result.to_dict() if self.chain_result else None,
            "reproducibility_result": self.reproducibility_result.to_dict() if self.reproducibility_result else None,
            "findings": [f.to_dict() for f in self.findings],
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "traceability": _unfreeze(self.traceability),
            "schema_version": self.schema_version,
            "disclaimer": self.disclaimer,
        }
