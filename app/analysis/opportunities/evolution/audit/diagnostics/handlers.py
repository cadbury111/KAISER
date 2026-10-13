"""KAISER — Historical Review Audit Observability & Diagnostics Command Handlers.

Phase 2, Step 36: Orchestrator command handlers for recording and querying diagnostics and alerts:
- `record_historical_review_diagnostic`
- `get_historical_review_diagnostic`
- `list_historical_review_diagnostics`
- `get_historical_review_alert`
- `list_historical_review_alerts`
- `get_historical_review_diagnostic_summary`

SAFETY INVARIANTS:
1. Rejects prohibited execution, trade recommendations, buy/sell signals, and profitability fields.
2. Read-only decision support; execution engine remains locked.
3. Errors are sanitized and internal exceptions are never leaked directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.opportunities.evolution.audit.diagnostics.models import (
    AuditDiagnosticCategory,
    AuditDiagnosticSeverity,
    AuditEvidenceNature,
    DiagnosticsExportRequest,
    SAFE_ID_PATTERN,
    validate_safe_id,
)
from app.analysis.opportunities.evolution.audit.diagnostics.service import (
    HistoricalReviewAuditDiagnosticsService,
)
from app.analysis.opportunities.evolution.models import validate_no_prohibited_fields
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.evolution.audit.diagnostics.handlers")


def register_audit_diagnostics_handlers(
    orchestrator: MasterOrchestrator,
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
) -> None:
    """Register review audit diagnostics command handlers with MasterOrchestrator."""

    def _extract_params(req: OrchestratorRequest) -> Dict[str, Any]:
        """Extract and validate parameters from OrchestratorRequest."""
        p = dict(
            getattr(req, "context", {})
            or getattr(req, "parameters", {})
            or getattr(req, "payload", {})
            or {}
        )
        validate_no_prohibited_fields(p)
        return p

    def handle_record_historical_review_diagnostic(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'record_historical_review_diagnostic'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        if not inv_id or not isinstance(inv_id, str):
            raise ValidationError("Missing or invalid 'investigation_id' parameter.")

        cat_raw = params.get("category", AuditDiagnosticCategory.GENERAL_DIAGNOSTIC.value)
        sev_raw = params.get("severity", AuditDiagnosticSeverity.INFO.value)
        nat_raw = params.get("evidence_nature", AuditEvidenceNature.OBSERVED_FAILURE.value)
        msg = params.get("message", "Operational diagnostic recorded.")

        rec = diagnostics_service.record_diagnostic(
            investigation_id=inv_id,
            category=cat_raw,
            severity=sev_raw,
            message=msg,
            evidence_nature=nat_raw,
            correlation_id=params.get("correlation_id"),
            report_id=params.get("report_id"),
            evaluation_id=params.get("evaluation_id"),
            source_references=params.get("source_references", ()),
            details=params.get("details", {}),
            error_category=params.get("error_category"),
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "diagnostic": rec.to_dict(),
        }

    def handle_get_historical_review_diagnostic(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_historical_review_diagnostic'."""
        params = _extract_params(req)
        diag_id = params.get("diagnostic_id")
        if not diag_id or not isinstance(diag_id, str):
            raise ValidationError("Missing or invalid 'diagnostic_id' parameter.")

        rec = diagnostics_service.get_diagnostic(diag_id)
        if rec is None:
            return {
                "status": "not_found",
                "read_only": True,
                "execution_locked": True,
                "message": f"Diagnostic record '{diag_id}' not found.",
            }

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "diagnostic": rec.to_dict(),
        }

    def handle_list_historical_review_diagnostics(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_historical_review_diagnostics'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        cat = params.get("category")
        sev = params.get("severity")
        limit = int(params.get("limit", 50))
        offset = int(params.get("offset", 0))

        records = diagnostics_service.list_diagnostics(
            investigation_id=inv_id,
            category=cat,
            severity=sev,
            limit=limit,
            offset=offset,
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "count": len(records),
            "diagnostics": [r.to_dict() for r in records],
        }

    def handle_get_historical_review_alert(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_historical_review_alert'."""
        params = _extract_params(req)
        alert_id = params.get("alert_id")
        if not alert_id or not isinstance(alert_id, str):
            raise ValidationError("Missing or invalid 'alert_id' parameter.")

        alert = diagnostics_service.get_alert(alert_id)
        if alert is None:
            return {
                "status": "not_found",
                "read_only": True,
                "execution_locked": True,
                "message": f"Operational alert '{alert_id}' not found.",
            }

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "alert": alert.to_dict(),
        }

    def handle_list_historical_review_alerts(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_historical_review_alerts'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        cat = params.get("category")
        sev = params.get("severity")
        active_only = bool(params.get("active_only", False))
        include_suppressed = bool(params.get("include_suppressed", False))
        limit = int(params.get("limit", 50))
        offset = int(params.get("offset", 0))

        alerts = diagnostics_service.list_alerts(
            investigation_id=inv_id,
            category=cat,
            severity=sev,
            active_only=active_only,
            limit=limit,
            offset=offset,
            include_suppressed=include_suppressed,
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "count": len(alerts),
            "alerts": [a.to_dict() for a in alerts],
        }

    def handle_get_historical_review_diagnostic_summary(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_historical_review_diagnostic_summary'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        if not inv_id or not isinstance(inv_id, str):
            raise ValidationError("Missing or invalid 'investigation_id' parameter.")

        summary = diagnostics_service.get_diagnostic_summary(inv_id)
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "summary": summary.to_dict(),
        }

    def handle_acknowledge_historical_review_alert(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'acknowledge_historical_review_alert'."""
        params = _extract_params(req)
        alert_id = params.get("alert_id")
        if not alert_id or not isinstance(alert_id, str):
            raise ValidationError("Missing or invalid 'alert_id' parameter.")

        alert = diagnostics_service.acknowledge_alert(
            alert_id=alert_id,
            acknowledged_by=params.get("acknowledged_by"),
        )
        if alert is None:
            return {
                "status": "not_found",
                "read_only": True,
                "execution_locked": True,
                "message": f"Operational alert '{alert_id}' not found.",
            }

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "alert": alert.to_dict(),
        }

    def handle_resolve_historical_review_alert(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'resolve_historical_review_alert'."""
        params = _extract_params(req)
        alert_id = params.get("alert_id")
        if not alert_id or not isinstance(alert_id, str):
            raise ValidationError("Missing or invalid 'alert_id' parameter.")

        alert = diagnostics_service.resolve_alert(
            alert_id=alert_id,
            reason=params.get("reason"),
        )
        if alert is None:
            return {
                "status": "not_found",
                "read_only": True,
                "execution_locked": True,
                "message": f"Operational alert '{alert_id}' not found.",
            }

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "alert": alert.to_dict(),
        }

    def handle_get_historical_review_retention_status(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_historical_review_retention_status'."""
        _extract_params(req)
        status_info = diagnostics_service.get_retention_status()
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "retention": status_info,
        }

    def handle_export_historical_review_diagnostics(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'export_historical_review_diagnostics'."""
        params = _extract_params(req)
        export_req = DiagnosticsExportRequest.from_dict(params)
        result = diagnostics_service.export_diagnostics(
            request=export_req,
            correlation_id=params.get("correlation_id"),
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "export": result.to_dict(),
        }

    def handle_get_historical_review_diagnostics_export(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_historical_review_diagnostics_export'."""
        params = _extract_params(req)
        export_id = params.get("export_id")
        if not export_id or not isinstance(export_id, str):
            raise ValidationError("Missing or invalid 'export_id' parameter.")

        validate_safe_id(export_id, "export_id")
        result = diagnostics_service.get_export(export_id)
        if result is None:
            return {
                "status": "not_found",
                "read_only": True,
                "execution_locked": True,
                "message": f"Diagnostics export '{export_id}' not found.",
            }

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "export": result.to_dict(),
        }

    def handle_list_historical_review_diagnostics_exports(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_historical_review_diagnostics_exports'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        limit = int(params.get("limit", 50))
        offset = int(params.get("offset", 0))

        exports = diagnostics_service.list_exports(
            investigation_id=inv_id,
            limit=limit,
            offset=offset,
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "count": len(exports),
            "exports": [e.to_dict() for e in exports],
        }

    def handle_reconcile_historical_review_diagnostics_export(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'reconcile_historical_review_diagnostics_export'."""
        params = _extract_params(req)
        export_id = params.get("export_id")
        if not export_id or not isinstance(export_id, str):
            raise ValidationError("Missing or invalid 'export_id' parameter.")

        validate_safe_id(export_id, "export_id")
        report = diagnostics_service.reconcile_export(
            export_id=export_id,
            correlation_id=params.get("correlation_id"),
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "reconciliation": report.to_dict(),
        }

    handlers = {
        "record_historical_review_diagnostic": handle_record_historical_review_diagnostic,
        "get_historical_review_diagnostic": handle_get_historical_review_diagnostic,
        "list_historical_review_diagnostics": handle_list_historical_review_diagnostics,
        "get_historical_review_alert": handle_get_historical_review_alert,
        "list_historical_review_alerts": handle_list_historical_review_alerts,
        "get_historical_review_diagnostic_summary": handle_get_historical_review_diagnostic_summary,
        "acknowledge_historical_review_alert": handle_acknowledge_historical_review_alert,
        "resolve_historical_review_alert": handle_resolve_historical_review_alert,
        "get_historical_review_retention_status": handle_get_historical_review_retention_status,
        "export_historical_review_diagnostics": handle_export_historical_review_diagnostics,
        "get_historical_review_diagnostics_export": handle_get_historical_review_diagnostics_export,
        "list_historical_review_diagnostics_exports": handle_list_historical_review_diagnostics_exports,
        "reconcile_historical_review_diagnostics_export": handle_reconcile_historical_review_diagnostics_export,
    }

    for cmd, handler_func in handlers.items():
        orchestrator.register_handler(cmd, handler_func)

    logger.info("Historical review audit diagnostics command handlers registered successfully (%d handlers).", len(handlers))
