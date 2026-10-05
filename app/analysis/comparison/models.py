"""KAISER — Experiment Comparison & Run Comparison Domain Models.

Provides strongly-typed, immutable data structures for evaluating analytical run
comparability, configuration diffing, provenance diffing, quality diffing,
and numerical metric comparison with strict mathematical safety.

SAFETY INVARIANTS:
1. Research and evaluation only: Never declares a strategy profitable or superior.
2. No automatic winners: Uses neutral direction semantics (INCREASED, DECREASED, UNCHANGED, UNAVAILABLE).
3. Numerical safety: Rejects NaN/Infinity, explicitly marks undefined relative changes (e.g. zero baseline).
4. Secret sanitization: Redacts credentials, tokens, and keys from all comparisons.
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

from app.analysis.models import normalize_timeframe
from app.analysis.runs.models import AnalysisRun, OperationType, RunStatus
from app.core.exceptions import ValidationError
from app.market.provenance.models import CREDENTIAL_KEY_PATTERNS
from app.market.quality.status import DataQualityStatus

DEFAULT_MAX_COMPARISON_RUNS = 20


class ComparabilityStatus(str, Enum):
    """Classification of whether two or more analytical runs are directly comparable."""

    COMPARABLE = "COMPARABLE"
    PARTIALLY_COMPARABLE = "PARTIALLY_COMPARABLE"
    NOT_COMPARABLE = "NOT_COMPARABLE"


class ComparisonDimension(str, Enum):
    """Categorical dimensions evaluated during run comparability and diffing."""

    OPERATION_TYPE = "operation_type"
    SYMBOL_UNIVERSE = "symbol_universe"
    TIMEFRAME_UNIVERSE = "timeframe_universe"
    DATA_RANGE = "data_range"
    DATA_PROVENANCE = "data_provenance"
    DATA_QUALITY = "data_quality"
    CONFIGURATION = "configuration"
    STRATEGY = "strategy"
    ANALYSIS_VERSION = "analysis_version"
    SOFTWARE_VERSION = "software_version"
    RESULT_TYPE = "result_type"


class DifferenceType(str, Enum):
    """Classification of a difference in a parameter, dimension, or attribute."""

    ADDED = "ADDED"
    REMOVED = "REMOVED"
    CHANGED = "CHANGED"
    UNCHANGED = "UNCHANGED"


class MetricDirection(str, Enum):
    """Neutral directional change of a numeric metric without subjective valuation."""

    INCREASED = "INCREASED"
    DECREASED = "DECREASED"
    UNCHANGED = "UNCHANGED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class MetricDifference:
    """Immutable record of difference between two numeric metric observations.

    Guarantees that NaN/Infinity values are never emitted and that zero-baseline
    relative deltas are represented explicitly as undefined with a reason.
    """

    metric_name: str
    left_value: Optional[float]
    right_value: Optional[float]
    absolute_difference: Optional[float]
    relative_difference: Optional[float]
    direction: MetricDirection
    is_available: bool
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        """Enforce strict numerical invariants."""
        if not self.metric_name or not isinstance(self.metric_name, str):
            raise ValidationError("metric_name must be a non-empty string.")

        for val_name, val in [
            ("left_value", self.left_value),
            ("right_value", self.right_value),
            ("absolute_difference", self.absolute_difference),
            ("relative_difference", self.relative_difference),
        ]:
            if val is not None:
                if math.isnan(val) or math.isinf(val):
                    raise ValidationError(f"{val_name} cannot be NaN or Infinity, got {val}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert metric difference to serializable dictionary."""
        return {
            "metric_name": self.metric_name,
            "left_value": self.left_value,
            "right_value": self.right_value,
            "absolute_difference": self.absolute_difference,
            "relative_difference": self.relative_difference,
            "direction": self.direction.value,
            "is_available": self.is_available,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ConfigurationDifference:
    """Immutable record of a parameter difference between two run configurations."""

    parameter_path: str
    left_value: Any
    right_value: Any
    diff_type: DifferenceType
    is_secret: bool = False

    def __post_init__(self) -> None:
        """Validate required fields."""
        if not self.parameter_path or not isinstance(self.parameter_path, str):
            raise ValidationError("parameter_path must be a non-empty string.")

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration difference to dictionary."""
        return {
            "parameter_path": self.parameter_path,
            "left_value": "[REDACTED]" if self.is_secret else self.left_value,
            "right_value": "[REDACTED]" if self.is_secret else self.right_value,
            "diff_type": self.diff_type.value,
            "is_secret": self.is_secret,
        }


@dataclass(frozen=True)
class ProvenanceDifference:
    """Immutable record of data provenance difference between two runs."""

    dimension: str
    left_value: Any
    right_value: Any
    diff_type: DifferenceType
    description: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert provenance difference to dictionary."""
        return {
            "dimension": self.dimension,
            "left_value": self.left_value,
            "right_value": self.right_value,
            "diff_type": self.diff_type.value,
            "description": self.description,
        }


@dataclass(frozen=True)
class QualityDifference:
    """Immutable record of data quality condition difference between two runs."""

    dimension: str
    left_value: Any
    right_value: Any
    diff_type: DifferenceType
    description: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert quality difference to dictionary."""
        return {
            "dimension": self.dimension,
            "left_value": self.left_value,
            "right_value": self.right_value,
            "diff_type": self.diff_type.value,
            "description": self.description,
        }


@dataclass(frozen=True)
class RunComparison:
    """Immutable comparative evaluation between two analytical execution runs.

    Encapsulates comparability status, explicit reasons for incompatibility,
    detected configuration diffs, provenance changes, quality differences, and metric deltas.
    """

    comparison_id: str
    generated_at: float
    left_run_id: str
    right_run_id: str
    comparability_status: ComparabilityStatus
    comparability_reasons: Tuple[str, ...]
    changed_dimensions: Tuple[ComparisonDimension, ...]
    metric_differences: Tuple[MetricDifference, ...] = ()
    configuration_differences: Tuple[ConfigurationDifference, ...] = ()
    provenance_differences: Tuple[ProvenanceDifference, ...] = ()
    quality_differences: Tuple[QualityDifference, ...] = ()
    summary: Dict[str, Any] = field(default_factory=dict)
    limitations: Tuple[str, ...] = ()
    disclaimer: str = (
        "RESEARCH ONLY: Comparative analytics evaluate hypothetical historical runs. "
        "Directional metric differences do not indicate future profitability, strategy superiority, "
        "or an automatic winner."
    )

    def __post_init__(self) -> None:
        """Validate core invariants."""
        if not self.comparison_id or not isinstance(self.comparison_id, str):
            raise ValidationError("comparison_id must be a non-empty string.")
        if not self.left_run_id or not isinstance(self.left_run_id, str):
            raise ValidationError("left_run_id must be a non-empty string.")
        if not self.right_run_id or not isinstance(self.right_run_id, str):
            raise ValidationError("right_run_id must be a non-empty string.")
        if self.generated_at < 0:
            raise ValidationError(f"generated_at timestamp cannot be negative: {self.generated_at}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert run comparison to dictionary."""
        return {
            "comparison_id": self.comparison_id,
            "generated_at": self.generated_at,
            "left_run_id": self.left_run_id,
            "right_run_id": self.right_run_id,
            "comparability_status": self.comparability_status.value,
            "comparability_reasons": list(self.comparability_reasons),
            "changed_dimensions": [d.value for d in self.changed_dimensions],
            "metric_differences": [m.to_dict() for m in self.metric_differences],
            "configuration_differences": [c.to_dict() for c in self.configuration_differences],
            "provenance_differences": [p.to_dict() for p in self.provenance_differences],
            "quality_differences": [q.to_dict() for q in self.quality_differences],
            "summary": dict(self.summary),
            "limitations": list(self.limitations),
            "disclaimer": self.disclaimer,
        }


@dataclass(frozen=True)
class MultiRunComparison:
    """Immutable comparative evaluation across a bounded group of analytical execution runs."""

    comparison_id: str
    generated_at: float
    run_ids: Tuple[str, ...]
    baseline_run_id: Optional[str]
    comparisons: Tuple[RunComparison, ...]
    aligned_metrics: Dict[str, Dict[str, Optional[float]]]
    summary: Dict[str, Any] = field(default_factory=dict)
    limitations: Tuple[str, ...] = ()
    disclaimer: str = (
        "RESEARCH ONLY: Multi-run comparative analytics evaluate hypothetical historical experiments. "
        "They do not rank strategies as winners or prove market profitability."
    )

    def __post_init__(self) -> None:
        """Validate multi-run bounds and requirements."""
        if not self.comparison_id:
            raise ValidationError("comparison_id must be a non-empty string.")
        if len(self.run_ids) < 2:
            raise ValidationError("MultiRunComparison requires at least 2 run_ids.")
        if len(self.run_ids) > DEFAULT_MAX_COMPARISON_RUNS:
            raise ValidationError(
                f"MultiRunComparison exceeds maximum bounded capacity of {DEFAULT_MAX_COMPARISON_RUNS} runs."
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert multi-run comparison to dictionary."""
        return {
            "comparison_id": self.comparison_id,
            "generated_at": self.generated_at,
            "run_ids": list(self.run_ids),
            "baseline_run_id": self.baseline_run_id,
            "runs_count": len(self.run_ids),
            "comparisons": [c.to_dict() for c in self.comparisons],
            "aligned_metrics": {k: dict(v) for k, v in self.aligned_metrics.items()},
            "summary": dict(self.summary),
            "limitations": list(self.limitations),
            "disclaimer": self.disclaimer,
        }
