"""KAISER — Historical Review Audit Observability, Alerting & Operational Diagnostics Package.

Phase 2, Step 36.
"""

from app.analysis.opportunities.evolution.audit.diagnostics.handlers import (
    register_audit_diagnostics_handlers,
)
from app.analysis.opportunities.evolution.audit.diagnostics.models import (
    AuditAlert,
    AuditAlertStatus,
    AuditDiagnosticCategory,
    AuditDiagnosticRecord,
    AuditDiagnosticSeverity,
    AuditDiagnosticSummary,
    AuditEvidenceNature,
    DiagnosticsExportRequest,
    DiagnosticsExportResult,
    DiagnosticsReconciliationDiscrepancy,
    DiagnosticsReconciliationReport,
    DiagnosticsReconciliationStatus,
    compute_deterministic_alert_id,
    compute_deterministic_diagnostic_id,
    compute_deterministic_export_id,
    compute_deterministic_reconciliation_id,
    compute_export_canonical_dict,
    compute_export_content_fingerprint,
)
from app.analysis.opportunities.evolution.audit.diagnostics.service import (
    HistoricalReviewAuditDiagnosticsService,
)

__all__ = [
    "AuditAlert",
    "AuditAlertStatus",
    "AuditDiagnosticCategory",
    "AuditDiagnosticRecord",
    "AuditDiagnosticSeverity",
    "AuditDiagnosticSummary",
    "AuditEvidenceNature",
    "DiagnosticsExportRequest",
    "DiagnosticsExportResult",
    "DiagnosticsReconciliationDiscrepancy",
    "DiagnosticsReconciliationReport",
    "DiagnosticsReconciliationStatus",
    "HistoricalReviewAuditDiagnosticsService",
    "compute_deterministic_alert_id",
    "compute_deterministic_diagnostic_id",
    "compute_deterministic_export_id",
    "compute_deterministic_reconciliation_id",
    "compute_export_canonical_dict",
    "compute_export_content_fingerprint",
    "register_audit_diagnostics_handlers",
]
