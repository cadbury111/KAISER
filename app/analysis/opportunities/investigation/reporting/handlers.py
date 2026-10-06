"""KAISER — Investigation Reporting Orchestrator Command Handlers.

Provides MasterOrchestrator command handlers for analytical investigation reports:
- `generate_investigation_report`: Generate immutable report from completed session.
- `get_investigation_report`: Retrieve report by ID.
- `list_investigation_reports`: Query reports with filtering and pagination.
- `get_investigation_report_audit`: Retrieve audit summary and sequence verification.
- `get_investigation_report_evidence`: Retrieve structured evidence matrix.
- `validate_investigation_report`: Validate report integrity, audit, and reproducibility.

SAFETY INVARIANTS:
1. Rejects any prohibited execution, trading, or recommendation parameters.
2. Strictly read-only decision support; execution engine remains locked.
3. Redacts secrets and sanitizes errors before responding.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from app.analysis.opportunities.investigation.reporting.service import InvestigationReportingService
from app.analysis.opportunities.investigation.session import InvestigationSessionManager
from app.analysis.opportunities.models import validate_no_prohibited_fields
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.investigation.reporting.handlers")

SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")


def register_investigation_reporting_handlers(
    orchestrator: MasterOrchestrator,
    reporting_service: InvestigationReportingService,
    session_manager: Optional[InvestigationSessionManager] = None,
) -> None:
    """Register investigation reporting commands with MasterOrchestrator."""

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

    def handle_generate_investigation_report(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'generate_investigation_report' command."""
        p = _extract_params(req)

        session = p.get("session")
        session_id = p.get("session_id")

        if session is None and session_id and session_manager is not None:
            session = session_manager.get_session(session_id)

        if session is None:
            return {
                "status": "invalid_request",
                "message": "Missing 'session' or 'session_id' parameter.",
                "read_only": True,
            }

        candidate = p.get("candidate")
        if candidate is None:
            return {
                "status": "invalid_request",
                "message": "Missing 'candidate' parameter.",
                "read_only": True,
            }

        priority_record = p.get("priority_record")
        evidence_report = p.get("evidence_report")
        audit_trail = p.get("audit_trail")
        if audit_trail is None and session_manager is not None and getattr(session, "session_id", None):
            audit_trail = session_manager.get_audit_trail(session.session_id)

        reviewer_id = p.get("reviewer_id")
        metadata = p.get("metadata", {})

        try:
            report = reporting_service.generate_report(
                session=session,
                candidate=candidate,
                priority_record=priority_record,
                evidence_report=evidence_report,
                audit_trail=audit_trail,
                reviewer_id=reviewer_id,
                metadata=metadata,
            )
            return {
                "status": "success",
                "report_id": report.report_id,
                "session_id": report.session_id,
                "candidate_id": report.candidate_id,
                "report_status": report.report_status.value,
                "report": report.to_dict(),
                "read_only": True,
            }
        except ValidationError as err:
            return {
                "status": "invalid_request",
                "message": str(err),
                "read_only": True,
            }
        except Exception as exc:
            logger.error("Failed to generate investigation report: %s", exc)
            return {
                "status": "error",
                "message": f"Error generating investigation report: {exc}",
                "read_only": True,
            }

    def handle_get_investigation_report(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_report' command."""
        p = _extract_params(req)
        report_id = p.get("report_id")
        if not report_id or not isinstance(report_id, str) or not SAFE_ID_PATTERN.match(report_id):
            return {
                "status": "invalid_request",
                "message": "Valid 'report_id' parameter is required.",
                "read_only": True,
            }

        report = reporting_service.get_report(report_id)
        if not report:
            return {
                "status": "not_found",
                "message": f"Investigation report '{report_id}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "report_id": report.report_id,
            "report": report.to_dict(),
            "read_only": True,
        }

    def handle_list_investigation_reports(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_investigation_reports' command."""
        p = _extract_params(req)
        candidate_id = p.get("candidate_id")
        session_id = p.get("session_id")
        status = p.get("status")
        limit = int(p.get("limit", 50))
        offset = int(p.get("offset", 0))

        reports = reporting_service.list_reports(
            candidate_id=candidate_id,
            session_id=session_id,
            status=status,
            limit=limit,
            offset=offset,
        )

        return {
            "status": "success",
            "count": len(reports),
            "reports": [r.to_dict() for r in reports],
            "limit": limit,
            "offset": offset,
            "read_only": True,
        }

    def handle_get_investigation_report_audit(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_report_audit' command."""
        p = _extract_params(req)
        report_id = p.get("report_id")
        if not report_id or not isinstance(report_id, str) or not SAFE_ID_PATTERN.match(report_id):
            return {
                "status": "invalid_request",
                "message": "Valid 'report_id' parameter is required.",
                "read_only": True,
            }

        audit = reporting_service.get_report_audit(report_id)
        if audit is None:
            return {
                "status": "not_found",
                "message": f"Audit summary for report '{report_id}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "report_id": report_id,
            "audit_summary": audit,
            "read_only": True,
        }

    def handle_get_investigation_report_evidence(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_report_evidence' command."""
        p = _extract_params(req)
        report_id = p.get("report_id")
        if not report_id or not isinstance(report_id, str) or not SAFE_ID_PATTERN.match(report_id):
            return {
                "status": "invalid_request",
                "message": "Valid 'report_id' parameter is required.",
                "read_only": True,
            }

        evidence = reporting_service.get_report_evidence(report_id)
        if evidence is None:
            return {
                "status": "not_found",
                "message": f"Evidence matrix for report '{report_id}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "report_id": report_id,
            "count": len(evidence),
            "evidence_matrix": evidence,
            "read_only": True,
        }

    def handle_validate_investigation_report(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'validate_investigation_report' command."""
        p = _extract_params(req)
        report_id = p.get("report_id")
        if not report_id or not isinstance(report_id, str) or not SAFE_ID_PATTERN.match(report_id):
            return {
                "status": "invalid_request",
                "message": "Valid 'report_id' parameter is required.",
                "read_only": True,
            }

        validation_result = reporting_service.validate_report(report_id)
        return {
            "status": "success",
            "validation": validation_result,
            "read_only": True,
        }

    handlers = {
        "generate_investigation_report": handle_generate_investigation_report,
        "get_investigation_report": handle_get_investigation_report,
        "list_investigation_reports": handle_list_investigation_reports,
        "get_investigation_report_audit": handle_get_investigation_report_audit,
        "get_investigation_report_evidence": handle_get_investigation_report_evidence,
        "validate_investigation_report": handle_validate_investigation_report,
    }

    for cmd, handler_func in handlers.items():
        orchestrator.register_handler(
            command=cmd,
            handler=handler_func,
            capability="investigation_reporting",
            allow_override=True,
        )

    logger.info("Registered investigation reporting handlers with MasterOrchestrator.")
