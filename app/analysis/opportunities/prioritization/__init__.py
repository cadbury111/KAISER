"""KAISER — Opportunity Prioritization & Investigation Queue Package.

Provides evidence-based prioritization of opportunity candidates and a thread-safe
investigation queue for decision support.
"""

from app.analysis.opportunities.prioritization.engine import (
    STATE_RANK,
    OpportunityPrioritizationEngine,
)
from app.analysis.opportunities.prioritization.handlers import (
    register_prioritization_handlers,
)
from app.analysis.opportunities.prioritization.models import (
    DEFAULT_MAX_PRIORITY_HISTORY,
    DEFAULT_MAX_QUEUE_CAPACITY,
    FORBIDDEN_VOICE_WORDS,
    PROHIBITED_RECOMMENDATION_FIELDS,
    OpportunityPriorityRecord,
    PriorityState,
    PriorityVoiceSummary,
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.prioritization.queue import (
    OpportunityInvestigationQueue,
)

__all__ = [
    "OpportunityPriorityRecord",
    "PriorityState",
    "PriorityVoiceSummary",
    "OpportunityPrioritizationEngine",
    "OpportunityInvestigationQueue",
    "register_prioritization_handlers",
    "validate_no_prohibited_fields",
    "PROHIBITED_RECOMMENDATION_FIELDS",
    "FORBIDDEN_VOICE_WORDS",
    "DEFAULT_MAX_PRIORITY_HISTORY",
    "DEFAULT_MAX_QUEUE_CAPACITY",
    "STATE_RANK",
]
