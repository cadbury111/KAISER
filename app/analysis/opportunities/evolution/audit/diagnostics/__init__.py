"""KAISER — Historical Review Audit Observability, Alerting & Operational Diagnostics Package.

Phase 2, Steps 36–43.
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
from app.analysis.opportunities.evolution.audit.diagnostics.schema import (
    CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
    SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS,
    LEGACY_UNVERSIONED,
    SchemaVersionCompatibilityStatus,
    SchemaVersionError,
    IncompatibleSchemaVersionError,
    MalformedSchemaVersionError,
    detect_and_validate_schema_version,
    migrate_alert_payload,
    migrate_diagnostic_payload,
    migrate_export_payload,
    migrate_reconciliation_payload,
    migrate_retention_stats_payload,
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
    "CURRENT_DIAGNOSTICS_SCHEMA_VERSION",
    "DiagnosticsExportRequest",
    "DiagnosticsExportResult",
    "DiagnosticsReconciliationDiscrepancy",
    "DiagnosticsReconciliationReport",
    "DiagnosticsReconciliationStatus",
    "HistoricalReviewAuditDiagnosticsService",
    "IncompatibleSchemaVersionError",
    "LEGACY_UNVERSIONED",
    "MalformedSchemaVersionError",
    "SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS",
    "SchemaVersionCompatibilityStatus",
    "SchemaVersionError",
    "compute_deterministic_alert_id",
    "compute_deterministic_diagnostic_id",
    "compute_deterministic_export_id",
    "compute_deterministic_reconciliation_id",
    "compute_export_canonical_dict",
    "compute_export_content_fingerprint",
    "detect_and_validate_schema_version",
    "migrate_alert_payload",
    "migrate_diagnostic_payload",
    "migrate_export_payload",
    "migrate_reconciliation_payload",
    "migrate_retention_stats_payload",
    "register_audit_diagnostics_handlers",
]
