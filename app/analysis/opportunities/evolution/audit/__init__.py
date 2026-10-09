"""KAISER — Historical Review Evolution Audit, Integrity & Reproducibility Package.

Phase 2, Step 33: Auditing foundation for historical review snapshots, evolution deltas,
chain continuity, deterministic identity verification, and reproducibility metadata.
"""

from __future__ import annotations

from app.analysis.opportunities.evolution.audit.handlers import (
    register_audit_handlers,
)
from app.analysis.opportunities.evolution.audit.models import (
    AuditFindingCategory,
    AuditFindingSeverity,
    AuditReportLifecycleState,
    EvolutionAuditFinding,
    EvolutionAuditVoiceSummary,
    EvolutionRecordIntegrityResult,
    EvolutionReproducibilityVerificationResult,
    HistoricalChainIntegrityResult,
    HistoricalReviewAuditReport,
    RecordValidationStatus,
    SnapshotIntegrityResult,
    compute_deterministic_audit_report_id,
)
from app.analysis.opportunities.evolution.audit.service import (
    HistoricalReviewAuditService,
)
from app.analysis.opportunities.evolution.audit.reliability import (
    AuditCoverageState,
    AuditCoverageSummary,
    AuditRegressionResult,
    AuditRegressionState,
    AuditRepeatabilityResult,
    AuditRepeatabilityState,
    AuditReliabilityVoiceSummary,
    HistoricalReviewReliabilityResult,
    HistoricalReviewAuditReliabilityService,
    compute_deterministic_reliability_id,
    register_reliability_handlers,
    MAX_RELIABILITY_EVALUATIONS_PER_INVESTIGATION,
)

__all__ = [
    # Models (Step 33)
    "AuditFindingSeverity",
    "AuditFindingCategory",
    "RecordValidationStatus",
    "AuditReportLifecycleState",
    "EvolutionAuditFinding",
    "SnapshotIntegrityResult",
    "EvolutionRecordIntegrityResult",
    "HistoricalChainIntegrityResult",
    "EvolutionReproducibilityVerificationResult",
    "EvolutionAuditVoiceSummary",
    "HistoricalReviewAuditReport",
    "compute_deterministic_audit_report_id",
    # Service (Step 33)
    "HistoricalReviewAuditService",
    # Handlers (Step 33)
    "register_audit_handlers",
    # Reliability (Step 34)
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
