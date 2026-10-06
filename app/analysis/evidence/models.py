"""KAISER — Evidence Synthesis & Decision-Support Reporting Domain Models.

Provides strongly-typed, immutable data structures for evidence synthesis,
facts vs. interpretation categorization, deterministic conflict detection,
uncertainty handling, and decision-support reporting.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Evidence synthesis only: Strictly separates facts, interpretations, uncertainties, and limitations.
2. Absolutely no trading recommendations: Never outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE.
3. No automatic winners: Comparative and analytical evidence uses neutral direction semantics.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
5. Immutability & Traceability: Every observation references originating run, provenance, or quality IDs.
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

from app.analysis.comparison.metrics import safe_float
from app.core.exceptions import ValidationError
from app.market.provenance.models import (
    CREDENTIAL_KEY_PATTERNS,
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)
from app.market.quality.status import DataQualityStatus

DEFAULT_MAX_EVIDENCE_ITEMS = 1000
DEFAULT_MAX_SOURCE_RUNS = 20
DEFAULT_MAX_OBSERVATIONS = 200
DEFAULT_MAX_CONFLICTS = 100
DEFAULT_MAX_LIMITATIONS = 50


class EvidenceType(str, Enum):
    """Categorical classification of analytical and operational evidence sources."""

    DATA_QUALITY = "DATA_QUALITY"
    DATA_ANOMALY = "DATA_ANOMALY"
    PROVENANCE = "PROVENANCE"
    TECHNICAL_ANALYSIS = "TECHNICAL_ANALYSIS"
    BACKTEST = "BACKTEST"
    WALK_FORWARD = "WALK_FORWARD"
    ROBUSTNESS = "ROBUSTNESS"
    PORTFOLIO_PERFORMANCE = "PORTFOLIO_PERFORMANCE"
    RISK = "RISK"
    ATTRIBUTION = "ATTRIBUTION"
    COMPARISON = "COMPARISON"
    SYSTEM_DIAGNOSTIC = "SYSTEM_DIAGNOSTIC"

    @classmethod
    def from_string(cls, val: Union[str, EvidenceType]) -> EvidenceType:
        """Parse evidence type safely from string or enum."""
        if isinstance(val, EvidenceType):
            return val
        if not val or not isinstance(val, str):
            return cls.SYSTEM_DIAGNOSTIC
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        if "QUALITY" in clean:
            return cls.DATA_QUALITY
        if "ANOMAL" in clean:
            return cls.DATA_ANOMALY
        if "PROV" in clean or "LINEAGE" in clean:
            return cls.PROVENANCE
        if "TECH" in clean or "INDICATOR" in clean:
            return cls.TECHNICAL_ANALYSIS
        if "WALK" in clean or "FORWARD" in clean:
            return cls.WALK_FORWARD
        if "ROBUST" in clean or "STRESS" in clean:
            return cls.ROBUSTNESS
        if "ATTRIB" in clean:
            return cls.ATTRIBUTION
        if "PORTFOLIO" in clean:
            return cls.PORTFOLIO_PERFORMANCE
        if "RISK" in clean:
            return cls.RISK
        if "COMPARE" in clean or "COMPARISON" in clean:
            return cls.COMPARISON
        if "BACKTEST" in clean or "SIMULAT" in clean:
            return cls.BACKTEST
        if "DIAG" in clean:
            return cls.SYSTEM_DIAGNOSTIC
        return cls.SYSTEM_DIAGNOSTIC


class EvidenceStatus(str, Enum):
    """Explicit availability and reliability state of an evidence item."""

    AVAILABLE = "AVAILABLE"
    PARTIAL = "PARTIAL"
    UNAVAILABLE = "UNAVAILABLE"
    STALE = "STALE"
    INVALID = "INVALID"
    CONFLICTING = "CONFLICTING"

    @classmethod
    def from_string(cls, val: Union[str, EvidenceStatus]) -> EvidenceStatus:
        """Parse evidence status safely from string or enum."""
        if isinstance(val, EvidenceStatus):
            return val
        if not val or not isinstance(val, str):
            return cls.AVAILABLE
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.AVAILABLE


class ObservationType(str, Enum):
    """Strict categorical separation between facts, interpretations, and boundaries."""

    FACT = "FACT"
    INTERPRETATION = "INTERPRETATION"
    UNCERTAINTY = "UNCERTAINTY"
    LIMITATION = "LIMITATION"


class ConflictType(str, Enum):
    """Classification of contradictory signals detected between evidence items."""

    TECHNICAL_DIRECTION_CONFLICT = "TECHNICAL_DIRECTION_CONFLICT"
    RISK_PERFORMANCE_TENSION = "RISK_PERFORMANCE_TENSION"
    QUALITY_DEGRADATION = "QUALITY_DEGRADATION"
    PROVENANCE_MISMATCH = "PROVENANCE_MISMATCH"
    METRIC_DISAGREEMENT = "METRIC_DISAGREEMENT"
    SAMPLE_COVERAGE_MISMATCH = "SAMPLE_COVERAGE_MISMATCH"
    OTHER = "OTHER"


class ConflictSeverity(str, Enum):
    """Severity level of an evidence conflict."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ConflictResolutionStatus(str, Enum):
    """Status of conflict resolution. Never resolves automatically without deterministic rule."""

    UNRESOLVED = "UNRESOLVED"
    RESOLVED_BY_RULE = "RESOLVED_BY_RULE"
    DISMISSED = "DISMISSED"


class UncertaintySource(str, Enum):
    """Origin of uncertainty affecting analytical confidence or completeness."""

    STALE_DATA = "STALE_DATA"
    MISSING_DATA = "MISSING_DATA"
    INCOMPLETE_PROVENANCE = "INCOMPLETE_PROVENANCE"
    DEGRADED_QUALITY = "DEGRADED_QUALITY"
    CONFLICTING_EVIDENCE = "CONFLICTING_EVIDENCE"
    INSUFFICIENT_SAMPLE_SIZE = "INSUFFICIENT_SAMPLE_SIZE"
    UNAVAILABLE_METRICS = "UNAVAILABLE_METRICS"
    DATASET_DIFFERENCE = "DATASET_DIFFERENCE"
    CONFIGURATION_DIFFERENCE = "CONFIGURATION_DIFFERENCE"
    STRATEGY_VERSION_DIFFERENCE = "STRATEGY_VERSION_DIFFERENCE"
    OTHER = "OTHER"


@dataclass(frozen=True)
class EvidenceItem:
    """Immutable atomic unit of evidence derived from KAISER analytical subsystems.

    Guarantees full auditable linkage to source runs, provenance records,
    quality assessments, and metric observations.
    """

    evidence_id: str
    evidence_type: EvidenceType
    source_reference: str
    observation: str
    timestamp: float
    status: EvidenceStatus = EvidenceStatus.AVAILABLE
    source_run_id: Optional[str] = None
    provenance_id: Optional[str] = None
    quality_assessment_id: Optional[str] = None
    comparison_id: Optional[str] = None
    observed_value: Optional[Any] = None
    expected_value: Optional[Any] = None
    units: Optional[str] = None
    limitations: Tuple[str, ...] = ()
    supporting_references: Tuple[str, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate evidence invariants and freeze collections."""
        if not self.evidence_id or not isinstance(self.evidence_id, str):
            raise ValidationError("EvidenceItem evidence_id must be a non-empty string.")
        if not self.source_reference or not isinstance(self.source_reference, str):
            raise ValidationError("EvidenceItem source_reference must be a non-empty string.")
        if not self.observation or not isinstance(self.observation, str):
            raise ValidationError("EvidenceItem observation must be a non-empty string.")
        if self.timestamp < 0:
            raise ValidationError(f"EvidenceItem timestamp cannot be negative: {self.timestamp}")

        if not isinstance(self.evidence_type, EvidenceType):
            object.__setattr__(self, "evidence_type", EvidenceType.from_string(self.evidence_type))

        if not isinstance(self.status, EvidenceStatus):
            object.__setattr__(self, "status", EvidenceStatus.from_string(self.status))

        # Check numeric observed/expected values for NaN/Inf
        for val_name, val in [("observed_value", self.observed_value), ("expected_value", self.expected_value)]:
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                if math.isnan(val) or math.isinf(val):
                    raise ValidationError(f"EvidenceItem {val_name} cannot be NaN or Infinity, got {val}")

        # Freeze collections
        object.__setattr__(self, "limitations", tuple(str(lim).strip() for lim in self.limitations if str(lim).strip()))
        object.__setattr__(
            self,
            "supporting_references",
            tuple(str(ref).strip() for ref in self.supporting_references if str(ref).strip()),
        )

        # Sanitize metadata
        sanitized_meta = sanitize_provenance_metadata(self.metadata or {})
        object.__setattr__(self, "metadata", sanitized_meta)

    def to_dict(self) -> Dict[str, Any]:
        """Convert evidence item to dictionary."""
        return {
            "evidence_id": self.evidence_id,
            "evidence_type": self.evidence_type.value,
            "source_reference": sanitize_provenance_string(self.source_reference),
            "source_run_id": self.source_run_id,
            "provenance_id": self.provenance_id,
            "quality_assessment_id": self.quality_assessment_id,
            "comparison_id": self.comparison_id,
            "observation": sanitize_provenance_string(self.observation),
            "observed_value": self.observed_value,
            "expected_value": self.expected_value,
            "units": self.units,
            "timestamp": self.timestamp,
            "status": self.status.value,
            "limitations": list(self.limitations),
            "supporting_references": list(self.supporting_references),
            "metadata": dict(self.metadata),
        }


def evidence_priority_key(item: EvidenceItem) -> Tuple[int, float, str]:
    """Deterministic ordering key for prioritizing evidence.

    ORDERING RATIONALE:
    1. INVALID / CRITICAL data quality issues: Fundamental data corruption takes precedence.
    2. STALE / DEGRADED data evidence: Degraded or stale data affects all downstream interpretations.
    3. Structural / Provenance / Comparability limitations: Incompatible or missing lineage.
    4. Risk evidence: Risk boundaries and drawdowns must be observed before return/performance.
    5. Performance evidence: Backtest, walk-forward, simulation, attribution metrics.
    6. Supporting technical analysis and diagnostics.

    This ordering ensures decision-makers see data reliability barriers before performance figures.
    It does NOT rank trading importance or predict future outcomes.
    """
    # Determine tier
    if item.status == EvidenceStatus.INVALID or (
        item.evidence_type in (EvidenceType.DATA_QUALITY, EvidenceType.DATA_ANOMALY)
        and item.status in (EvidenceStatus.INVALID, EvidenceStatus.CONFLICTING)
    ):
        tier = 1
    elif item.status in (EvidenceStatus.STALE, EvidenceStatus.PARTIAL, EvidenceStatus.UNAVAILABLE) or (
        item.evidence_type in (EvidenceType.DATA_QUALITY, EvidenceType.DATA_ANOMALY)
    ):
        tier = 2
    elif item.evidence_type in (EvidenceType.PROVENANCE, EvidenceType.COMPARISON):
        tier = 3
    elif item.evidence_type == EvidenceType.RISK:
        tier = 4
    elif item.evidence_type in (
        EvidenceType.BACKTEST,
        EvidenceType.WALK_FORWARD,
        EvidenceType.ROBUSTNESS,
        EvidenceType.PORTFOLIO_PERFORMANCE,
        EvidenceType.ATTRIBUTION,
    ):
        tier = 5
    else:
        tier = 6

    return (tier, item.timestamp, item.evidence_id)


@dataclass(frozen=True)
class KeyObservation:
    """Immutable structured observation clearly categorized as FACT or INTERPRETATION.

    Guarantees strict traceability to source references and metric identifiers.
    """

    observation_id: str
    observation_type: ObservationType
    statement: str
    source_references: Tuple[str, ...]
    metric_name: Optional[str] = None
    observed_value: Optional[Any] = None
    confidence_or_status: EvidenceStatus = EvidenceStatus.AVAILABLE
    rationale: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate observation invariants."""
        if not self.observation_id or not isinstance(self.observation_id, str):
            raise ValidationError("KeyObservation observation_id must be a non-empty string.")
        if not self.statement or not isinstance(self.statement, str):
            raise ValidationError("KeyObservation statement must be a non-empty string.")
        if not self.source_references:
            raise ValidationError(
                f"Traceability violation: KeyObservation '{self.observation_id}' must reference at least one source."
            )

        if not isinstance(self.observation_type, ObservationType):
            object.__setattr__(self, "observation_type", ObservationType(str(self.observation_type)))

        if not isinstance(self.confidence_or_status, EvidenceStatus):
            object.__setattr__(self, "confidence_or_status", EvidenceStatus.from_string(self.confidence_or_status))

        if isinstance(self.observed_value, (int, float)) and not isinstance(self.observed_value, bool):
            if math.isnan(self.observed_value) or math.isinf(self.observed_value):
                raise ValidationError(f"KeyObservation observed_value cannot be NaN or Infinity, got {self.observed_value}")

        object.__setattr__(
            self,
            "source_references",
            tuple(str(r).strip() for r in self.source_references if str(r).strip()),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert observation to dictionary."""
        return {
            "observation_id": self.observation_id,
            "observation_type": self.observation_type.value,
            "statement": sanitize_provenance_string(self.statement),
            "source_references": list(self.source_references),
            "metric_name": self.metric_name,
            "observed_value": self.observed_value,
            "confidence_or_status": self.confidence_or_status.value,
            "rationale": sanitize_provenance_string(self.rationale) if self.rationale else None,
        }


@dataclass(frozen=True)
class EvidenceConflict:
    """Immutable representation of conflicting signals between evidence sources.

    Represents contradictory signals without declaring an arbitrary winner.
    """

    conflict_id: str
    evidence_references: Tuple[str, ...]
    conflict_type: ConflictType
    description: str
    severity: ConflictSeverity = ConflictSeverity.MEDIUM
    resolution_status: ConflictResolutionStatus = ConflictResolutionStatus.UNRESOLVED
    resolution_notes: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate conflict invariants."""
        if not self.conflict_id or not isinstance(self.conflict_id, str):
            raise ValidationError("EvidenceConflict conflict_id must be a non-empty string.")
        if not self.evidence_references or len(self.evidence_references) < 2:
            raise ValidationError("EvidenceConflict must reference at least 2 conflicting evidence items.")
        if not self.description or not isinstance(self.description, str):
            raise ValidationError("EvidenceConflict description must be a non-empty string.")

        if not isinstance(self.conflict_type, ConflictType):
            object.__setattr__(self, "conflict_type", ConflictType(str(self.conflict_type)))

        if not isinstance(self.severity, ConflictSeverity):
            object.__setattr__(self, "severity", ConflictSeverity(str(self.severity)))

        if not isinstance(self.resolution_status, ConflictResolutionStatus):
            object.__setattr__(self, "resolution_status", ConflictResolutionStatus(str(self.resolution_status)))

        object.__setattr__(
            self,
            "evidence_references",
            tuple(str(r).strip() for r in self.evidence_references if str(r).strip()),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert conflict to dictionary."""
        return {
            "conflict_id": self.conflict_id,
            "evidence_references": list(self.evidence_references),
            "conflict_type": self.conflict_type.value,
            "description": sanitize_provenance_string(self.description),
            "severity": self.severity.value,
            "resolution_status": self.resolution_status.value,
            "resolution_notes": sanitize_provenance_string(self.resolution_notes) if self.resolution_notes else None,
        }


@dataclass(frozen=True)
class EvidenceUncertainty:
    """Immutable structured record of uncertainty affecting evidence synthesis."""

    uncertainty_id: str
    source: UncertaintySource
    description: str
    impact: str
    affected_evidence_ids: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate uncertainty invariants."""
        if not self.uncertainty_id or not isinstance(self.uncertainty_id, str):
            raise ValidationError("EvidenceUncertainty uncertainty_id must be a non-empty string.")
        if not self.description or not isinstance(self.description, str):
            raise ValidationError("EvidenceUncertainty description must be a non-empty string.")
        if not self.impact or not isinstance(self.impact, str):
            raise ValidationError("EvidenceUncertainty impact must be a non-empty string.")

        if not isinstance(self.source, UncertaintySource):
            object.__setattr__(self, "source", UncertaintySource(str(self.source)))

        object.__setattr__(
            self,
            "affected_evidence_ids",
            tuple(str(i).strip() for i in self.affected_evidence_ids if str(i).strip()),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert uncertainty to dictionary."""
        return {
            "uncertainty_id": self.uncertainty_id,
            "source": self.source.value,
            "description": sanitize_provenance_string(self.description),
            "impact": sanitize_provenance_string(self.impact),
            "affected_evidence_ids": list(self.affected_evidence_ids),
        }


@dataclass(frozen=True)
class MissingEvidence:
    """Immutable record of expected evidence that was absent during synthesis."""

    missing_id: str
    evidence_type: EvidenceType
    description: str
    impact: str
    expected_source: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate missing evidence invariants."""
        if not self.missing_id or not isinstance(self.missing_id, str):
            raise ValidationError("MissingEvidence missing_id must be a non-empty string.")
        if not self.description or not isinstance(self.description, str):
            raise ValidationError("MissingEvidence description must be a non-empty string.")
        if not self.impact or not isinstance(self.impact, str):
            raise ValidationError("MissingEvidence impact must be a non-empty string.")

        if not isinstance(self.evidence_type, EvidenceType):
            object.__setattr__(self, "evidence_type", EvidenceType.from_string(self.evidence_type))

    def to_dict(self) -> Dict[str, Any]:
        """Convert missing evidence to dictionary."""
        return {
            "missing_id": self.missing_id,
            "evidence_type": self.evidence_type.value,
            "description": sanitize_provenance_string(self.description),
            "impact": sanitize_provenance_string(self.impact),
            "expected_source": sanitize_provenance_string(self.expected_source) if self.expected_source else None,
        }


@dataclass(frozen=True)
class SampleCoverage:
    """Preserves observed sample information without inventing arbitrary validity thresholds."""

    record_count: Optional[int] = None
    trade_count: Optional[int] = None
    window_count: Optional[int] = None
    oos_window_count: Optional[int] = None
    duration_seconds: Optional[float] = None
    duration_days: Optional[float] = None
    symbol_count: Optional[int] = None
    timeframe_count: Optional[int] = None
    notes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate non-negative counts and finite duration."""
        for name, val in [
            ("record_count", self.record_count),
            ("trade_count", self.trade_count),
            ("window_count", self.window_count),
            ("oos_window_count", self.oos_window_count),
            ("symbol_count", self.symbol_count),
            ("timeframe_count", self.timeframe_count),
        ]:
            if val is not None and val < 0:
                raise ValidationError(f"SampleCoverage {name} cannot be negative: {val}")

        for name, val in [("duration_seconds", self.duration_seconds), ("duration_days", self.duration_days)]:
            if val is not None:
                if math.isnan(val) or math.isinf(val) or val < 0:
                    raise ValidationError(f"SampleCoverage {name} must be a non-negative finite number: {val}")

        object.__setattr__(self, "notes", tuple(str(n).strip() for n in self.notes if str(n).strip()))

    def to_dict(self) -> Dict[str, Any]:
        """Convert sample coverage to dictionary."""
        return {
            "record_count": self.record_count,
            "trade_count": self.trade_count,
            "window_count": self.window_count,
            "oos_window_count": self.oos_window_count,
            "duration_seconds": self.duration_seconds,
            "duration_days": self.duration_days,
            "symbol_count": self.symbol_count,
            "timeframe_count": self.timeframe_count,
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class EvidenceVoiceSummary:
    """Concise, deterministic structured speech text for voice synthesis without trading instructions."""

    headline: str
    data_quality_speech: str
    comparison_speech: str
    risk_speech: str
    limitations_speech: str
    full_speech_script: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to dictionary."""
        return {
            "headline": self.headline,
            "data_quality_speech": self.data_quality_speech,
            "comparison_speech": self.comparison_speech,
            "risk_speech": self.risk_speech,
            "limitations_speech": self.limitations_speech,
            "full_speech_script": self.full_speech_script,
        }


@dataclass(frozen=True)
class EvidenceSynthesisReport:
    """Immutable, auditable synthesis report consolidating all analytical evidence.

    Combines data quality, provenance, execution runs, comparison diffs,
    and portfolio risk into an objective decision-support contract.
    """

    report_id: str
    generated_at: float
    subject: str
    scope: str
    source_run_ids: Tuple[str, ...]
    evidence_items: Tuple[EvidenceItem, ...]
    key_observations: Tuple[KeyObservation, ...]
    supporting_metrics: Dict[str, Any] = field(default_factory=dict)
    data_quality_summary: Dict[str, Any] = field(default_factory=dict)
    provenance_summary: Dict[str, Any] = field(default_factory=dict)
    comparison_summary: Dict[str, Any] = field(default_factory=dict)
    risk_summary: Dict[str, Any] = field(default_factory=dict)
    conflicts: Tuple[EvidenceConflict, ...] = ()
    missing_evidence: Tuple[MissingEvidence, ...] = ()
    uncertainties: Tuple[EvidenceUncertainty, ...] = ()
    sample_coverage: Optional[SampleCoverage] = None
    limitations: Tuple[str, ...] = ()
    methodology: str = "KAISER Phase 2 Step 23 Deterministic Evidence Synthesis"
    reproducibility_references: Tuple[str, ...] = ()
    report_version: str = "1.0.0"
    voice_summary: Optional[EvidenceVoiceSummary] = None
    disclaimer: str = (
        "EVIDENCE SYNTHESIS ONLY: This report provides structured decision-support evidence synthesis. "
        "It does NOT recommend trading actions, position sizes, or strategy execution. "
        "Settings.execution_locked remains True."
    )

    def __post_init__(self) -> None:
        """Validate core invariants, enforce bounds, and freeze collections."""
        if not self.report_id or not isinstance(self.report_id, str):
            raise ValidationError("report_id must be a non-empty string.")
        if not self.subject or not isinstance(self.subject, str):
            raise ValidationError("subject must be a non-empty string.")
        if not self.scope or not isinstance(self.scope, str):
            raise ValidationError("scope must be a non-empty string.")
        if self.generated_at < 0:
            raise ValidationError(f"generated_at timestamp cannot be negative: {self.generated_at}")

        # Enforce bounds
        if len(self.source_run_ids) > DEFAULT_MAX_SOURCE_RUNS:
            raise ValidationError(
                f"Source run count {len(self.source_run_ids)} exceeds maximum bounded capacity of {DEFAULT_MAX_SOURCE_RUNS}."
            )
        if len(self.evidence_items) > DEFAULT_MAX_EVIDENCE_ITEMS:
            raise ValidationError(
                f"Evidence items count {len(self.evidence_items)} exceeds maximum bounded capacity of {DEFAULT_MAX_EVIDENCE_ITEMS}."
            )
        if len(self.key_observations) > DEFAULT_MAX_OBSERVATIONS:
            raise ValidationError(
                f"Key observations count {len(self.key_observations)} exceeds maximum bounded capacity of {DEFAULT_MAX_OBSERVATIONS}."
            )
        if len(self.conflicts) > DEFAULT_MAX_CONFLICTS:
            raise ValidationError(
                f"Conflicts count {len(self.conflicts)} exceeds maximum bounded capacity of {DEFAULT_MAX_CONFLICTS}."
            )

        # Freeze collections
        object.__setattr__(self, "source_run_ids", tuple(str(r).strip() for r in self.source_run_ids if str(r).strip()))
        object.__setattr__(self, "evidence_items", tuple(self.evidence_items or ()))
        object.__setattr__(self, "key_observations", tuple(self.key_observations or ()))
        object.__setattr__(self, "conflicts", tuple(self.conflicts or ()))
        object.__setattr__(self, "missing_evidence", tuple(self.missing_evidence or ()))
        object.__setattr__(self, "uncertainties", tuple(self.uncertainties or ()))
        object.__setattr__(self, "limitations", tuple(str(lim).strip() for lim in self.limitations if str(lim).strip()))
        object.__setattr__(
            self,
            "reproducibility_references",
            tuple(str(ref).strip() for ref in self.reproducibility_references if str(ref).strip()),
        )

        # Sanitize summaries
        clean_metrics = sanitize_provenance_metadata(self.supporting_metrics or {})
        clean_dq = sanitize_provenance_metadata(self.data_quality_summary or {})
        clean_prov = sanitize_provenance_metadata(self.provenance_summary or {})
        clean_comp = sanitize_provenance_metadata(self.comparison_summary or {})
        clean_risk = sanitize_provenance_metadata(self.risk_summary or {})

        object.__setattr__(self, "supporting_metrics", clean_metrics)
        object.__setattr__(self, "data_quality_summary", clean_dq)
        object.__setattr__(self, "provenance_summary", clean_prov)
        object.__setattr__(self, "comparison_summary", clean_comp)
        object.__setattr__(self, "risk_summary", clean_risk)

    @property
    def facts(self) -> Tuple[KeyObservation, ...]:
        """Retrieve only observations categorized as FACT."""
        return tuple(obs for obs in self.key_observations if obs.observation_type == ObservationType.FACT)

    @property
    def interpretations(self) -> Tuple[KeyObservation, ...]:
        """Retrieve only observations categorized as INTERPRETATION."""
        return tuple(obs for obs in self.key_observations if obs.observation_type == ObservationType.INTERPRETATION)

    @property
    def has_conflicts(self) -> bool:
        """True if any unresolved conflicts exist."""
        return any(c.resolution_status == ConflictResolutionStatus.UNRESOLVED for c in self.conflicts)

    @property
    def overall_evidence_status(self) -> EvidenceStatus:
        """Compute primary synthesized evidence availability status.

        Returns INVALID if any critical evidence is invalid,
        CONFLICTING if unresolved conflicts exist,
        STALE if stale evidence exists,
        PARTIAL if missing evidence exists,
        else AVAILABLE.
        """
        statuses = set(item.status for item in self.evidence_items)
        if EvidenceStatus.INVALID in statuses:
            return EvidenceStatus.INVALID
        if self.has_conflicts or EvidenceStatus.CONFLICTING in statuses:
            return EvidenceStatus.CONFLICTING
        if EvidenceStatus.STALE in statuses:
            return EvidenceStatus.STALE
        if EvidenceStatus.UNAVAILABLE in statuses or len(self.missing_evidence) > 0 or EvidenceStatus.PARTIAL in statuses:
            return EvidenceStatus.PARTIAL
        return EvidenceStatus.AVAILABLE

    def to_dict(self) -> Dict[str, Any]:
        """Convert evidence synthesis report to dictionary."""
        return {
            "report_id": self.report_id,
            "generated_at": self.generated_at,
            "subject": sanitize_provenance_string(self.subject),
            "scope": self.scope,
            "source_run_ids": list(self.source_run_ids),
            "overall_status": self.overall_evidence_status.value,
            "evidence_items_count": len(self.evidence_items),
            "evidence_items": [item.to_dict() for item in self.evidence_items],
            "key_observations": [obs.to_dict() for obs in self.key_observations],
            "facts": [f.to_dict() for f in self.facts],
            "interpretations": [i.to_dict() for i in self.interpretations],
            "supporting_metrics": dict(self.supporting_metrics),
            "data_quality_summary": dict(self.data_quality_summary),
            "provenance_summary": dict(self.provenance_summary),
            "comparison_summary": dict(self.comparison_summary),
            "risk_summary": dict(self.risk_summary),
            "conflicts": [c.to_dict() for c in self.conflicts],
            "missing_evidence": [m.to_dict() for m in self.missing_evidence],
            "uncertainties": [u.to_dict() for u in self.uncertainties],
            "sample_coverage": self.sample_coverage.to_dict() if self.sample_coverage else None,
            "limitations": list(self.limitations),
            "methodology": self.methodology,
            "reproducibility_references": list(self.reproducibility_references),
            "report_version": self.report_version,
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "disclaimer": self.disclaimer,
        }
