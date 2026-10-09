"""KAISER — Historical Review Audit Coverage, Regression Monitoring & Reliability Module.

Phase 2, Step 34: Long-term reliability validation foundation for historical review audits.
"""

from app.analysis.opportunities.evolution.audit.reliability.handlers import (
    register_reliability_handlers,
)
from app.analysis.opportunities.evolution.audit.reliability.models import (
    MAX_RELIABILITY_EVALUATIONS_PER_INVESTIGATION,
    AuditCoverageState,
    AuditCoverageSummary,
    AuditRegressionResult,
    AuditRegressionState,
    AuditRepeatabilityResult,
    AuditRepeatabilityState,
    AuditReliabilityVoiceSummary,
    HistoricalReviewReliabilityResult,
    ReliabilityEvaluationStatus,
    compute_deterministic_reliability_id,
)
from app.analysis.opportunities.evolution.audit.reliability.service import (
    HistoricalReviewAuditReliabilityService,
)

__all__ = [
    "AuditCoverageState",
    "AuditRepeatabilityState",
    "AuditRegressionState",
    "ReliabilityEvaluationStatus",
    "AuditCoverageSummary",
    "AuditRepeatabilityResult",
    "AuditRegressionResult",
    "AuditReliabilityVoiceSummary",
    "HistoricalReviewReliabilityResult",
    "HistoricalReviewAuditReliabilityService",
    "compute_deterministic_reliability_id",
    "register_reliability_handlers",
    "MAX_RELIABILITY_EVALUATIONS_PER_INVESTIGATION",
]
