"""KAISER — Investigation Reporting & Evidence Review Audit Foundation.

This package provides deterministic, traceable, reproducible analytical investigation
reporting and evidence review audit logs on top of human investigation sessions.

SAFETY INVARIANT:
Provides analytical investigation reporting and decision support only.
Never outputs BUY/SELL/HOLD recommendations, trade executions, order routing,
or profitability predictions.
"""

from app.analysis.opportunities.investigation.reporting.handlers import (
    register_investigation_reporting_handlers,
)
from app.analysis.opportunities.investigation.reporting.models import (
    DEFAULT_MAX_REPORT_HISTORY,
    AttributionType,
    AuditSummary,
    EvidenceMatrixEntry,
    InvestigationReport,
    InvestigationReportVoiceSummary,
    ObservationMatrixEntry,
    ReportStatus,
    ReproducibilityRecord,
    compute_deterministic_report_id,
    validate_numerical_integrity,
)
from app.analysis.opportunities.investigation.reporting.service import (
    InvestigationReportingService,
)

__all__ = [
    "DEFAULT_MAX_REPORT_HISTORY",
    "AttributionType",
    "AuditSummary",
    "EvidenceMatrixEntry",
    "InvestigationReport",
    "InvestigationReportVoiceSummary",
    "InvestigationReportingService",
    "ObservationMatrixEntry",
    "ReportStatus",
    "ReproducibilityRecord",
    "compute_deterministic_report_id",
    "register_investigation_reporting_handlers",
    "validate_numerical_integrity",
]
