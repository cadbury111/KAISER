"""KAISER — Cross-Investigation Evidence Comparison & Review Consistency Foundation (Phase 2, Step 28).

Provides strongly-typed, immutable analytical comparisons across multiple completed
InvestigationReports, detecting shared/changed/conflicting evidence, observation shifts,
review consistency, and discrepancies with strict analytical neutrality.
"""

from __future__ import annotations

from app.analysis.opportunities.comparison.engine import InvestigationComparisonEngine
from app.analysis.opportunities.comparison.handlers import (
    register_investigation_comparison_handlers,
)
from app.analysis.opportunities.comparison.models import (
    PROHIBITED_COMPARISON_FIELDS,
    ComparabilityState,
    ConclusionComparisonRecord,
    ConsistencySummary,
    DiscrepancyCategory,
    DiscrepancyResolutionState,
    DiscrepancySeverity,
    EvidenceDiffCategory,
    EvidenceDiffItem,
    InvestigationComparison,
    InvestigationComparisonVoiceSummary,
    InvestigationDiscrepancy,
    ObservationDiffItem,
    ObservationDiffType,
    ProvenanceComparisonRecord,
    QualityComparisonRecord,
    ReproducibilityComparisonRecord,
    compute_deterministic_comparison_id,
    safe_percentage_delta,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)

__all__ = [
    "PROHIBITED_COMPARISON_FIELDS",
    "ComparabilityState",
    "ConclusionComparisonRecord",
    "ConsistencySummary",
    "DiscrepancyCategory",
    "DiscrepancyResolutionState",
    "DiscrepancySeverity",
    "EvidenceDiffCategory",
    "EvidenceDiffItem",
    "InvestigationComparison",
    "InvestigationComparisonEngine",
    "InvestigationComparisonVoiceSummary",
    "InvestigationDiscrepancy",
    "ObservationDiffItem",
    "ObservationDiffType",
    "ProvenanceComparisonRecord",
    "QualityComparisonRecord",
    "ReproducibilityComparisonRecord",
    "compute_deterministic_comparison_id",
    "register_investigation_comparison_handlers",
    "safe_percentage_delta",
    "validate_no_prohibited_fields",
    "validate_numerical_integrity",
]
