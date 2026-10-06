"""KAISER — Opportunity Assessment & Decision Support Package.

Exposes deterministic domain models, safety validation guards, state models,
and the OpportunityAssessmentEngine.
"""

from app.analysis.opportunities.engine import OpportunityAssessmentEngine
from app.analysis.opportunities.handlers import register_opportunity_handlers
from app.analysis.opportunities.models import (
    DEFAULT_MAX_OPPORTUNITY_HISTORY,
    FORBIDDEN_VOICE_WORDS,
    PROHIBITED_RECOMMENDATION_FIELDS,
    ConfidenceState,
    ObservationWindow,
    OpportunityCandidate,
    OpportunityState,
    OpportunityVoiceSummary,
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.prioritization import (
    DEFAULT_MAX_PRIORITY_HISTORY,
    DEFAULT_MAX_QUEUE_CAPACITY,
    STATE_RANK,
    OpportunityInvestigationQueue,
    OpportunityPrioritizationEngine,
    OpportunityPriorityRecord,
    PriorityState,
    PriorityVoiceSummary,
    register_prioritization_handlers,
)

__all__ = [
    "OpportunityAssessmentEngine",
    "register_opportunity_handlers",
    "OpportunityCandidate",
    "OpportunityState",
    "ConfidenceState",
    "ObservationWindow",
    "OpportunityVoiceSummary",
    "PROHIBITED_RECOMMENDATION_FIELDS",
    "FORBIDDEN_VOICE_WORDS",
    "DEFAULT_MAX_OPPORTUNITY_HISTORY",
    "validate_no_prohibited_fields",
    "OpportunityPrioritizationEngine",
    "OpportunityInvestigationQueue",
    "OpportunityPriorityRecord",
    "PriorityState",
    "PriorityVoiceSummary",
    "register_prioritization_handlers",
    "DEFAULT_MAX_PRIORITY_HISTORY",
    "DEFAULT_MAX_QUEUE_CAPACITY",
    "STATE_RANK",
]

