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

__all__ = [
    # Models
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
    # Service
    "HistoricalReviewAuditService",
    # Handlers
    "register_audit_handlers",
]
