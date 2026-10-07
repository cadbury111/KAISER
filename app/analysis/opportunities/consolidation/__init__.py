"""KAISER — Investigation Evidence Consolidation & Review Outcome Package.

Phase 2, Step 30: Deterministic foundation for producing an auditable representation
of the current human-reviewed investigation evidence state.
"""

from app.analysis.opportunities.consolidation.handlers import (
    register_consolidation_handlers,
)
from app.analysis.opportunities.consolidation.models import (
    DEFAULT_MAX_CONSOLIDATION_HISTORY,
    FORBIDDEN_VOICE_WORDS,
    PROHIBITED_RECOMMENDATION_FIELDS,
    SAFE_ID_PATTERN,
    ConsolidatedConflictItem,
    ConsolidatedEvidenceItem,
    ConsolidatedInvestigationOutcome,
    ConsolidatedReviewDecision,
    ConsolidationLimitation,
    ConsolidationLimitationItem,
    ConsolidationState,
    ConsolidationVoiceSummary,
    DecisionClaimType,
    QualityAggregationSummary,
    ReproducibilityConsolidationState,
    ReproducibilitySummary,
    compute_deterministic_conflict_id,
    compute_deterministic_outcome_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.consolidation.service import (
    InvestigationOutcomeConsolidationService,
)

__all__ = [
    "ConsolidatedConflictItem",
    "ConsolidatedEvidenceItem",
    "ConsolidatedInvestigationOutcome",
    "ConsolidatedReviewDecision",
    "ConsolidationLimitation",
    "ConsolidationLimitationItem",
    "ConsolidationState",
    "ConsolidationVoiceSummary",
    "DEFAULT_MAX_CONSOLIDATION_HISTORY",
    "DecisionClaimType",
    "FORBIDDEN_VOICE_WORDS",
    "InvestigationOutcomeConsolidationService",
    "PROHIBITED_RECOMMENDATION_FIELDS",
    "QualityAggregationSummary",
    "ReproducibilityConsolidationState",
    "ReproducibilitySummary",
    "SAFE_ID_PATTERN",
    "compute_deterministic_conflict_id",
    "compute_deterministic_outcome_id",
    "register_consolidation_handlers",
    "validate_no_prohibited_fields",
    "validate_numerical_integrity",
]
