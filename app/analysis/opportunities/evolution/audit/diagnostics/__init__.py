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
    compute_deterministic_alert_id,
    compute_deterministic_diagnostic_id,
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
    "HistoricalReviewAuditDiagnosticsService",
    "compute_deterministic_alert_id",
    "compute_deterministic_diagnostic_id",
    "register_audit_diagnostics_handlers",
]
