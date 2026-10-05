"""KAISER — Experiment Comparison & Run Comparison Foundation.

Exports comparability models, difference engines, numerical metric comparison,
and the RunComparisonEngine.
"""

from app.analysis.comparison.diff import (
    diff_configurations,
    diff_provenance,
    diff_quality,
)
from app.analysis.comparison.engine import RunComparisonEngine
from app.analysis.comparison.handlers import register_comparison_handlers
from app.analysis.comparison.metrics import (
    compare_metrics_dicts,
    compute_metric_difference,
    extract_metrics_from_result,
    safe_float,
)
from app.analysis.comparison.models import (
    ComparabilityStatus,
    ComparisonDimension,
    ConfigurationDifference,
    DifferenceType,
    MetricDifference,
    MetricDirection,
    MultiRunComparison,
    ProvenanceDifference,
    QualityDifference,
    RunComparison,
)

__all__ = [
    "ComparabilityStatus",
    "ComparisonDimension",
    "DifferenceType",
    "MetricDirection",
    "MetricDifference",
    "ConfigurationDifference",
    "ProvenanceDifference",
    "QualityDifference",
    "RunComparison",
    "MultiRunComparison",
    "RunComparisonEngine",
    "diff_configurations",
    "diff_provenance",
    "diff_quality",
    "compute_metric_difference",
    "extract_metrics_from_result",
    "compare_metrics_dicts",
    "safe_float",
    "register_comparison_handlers",
]
