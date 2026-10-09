"""KAISER — Historical Review Evolution Audit Command Handlers.

Phase 2, Step 33: Orchestrator command handlers for auditing review snapshots, evolutions,
chain continuity, deterministic identity verification, and reproducibility:
- `audit_historical_review_snapshot`
- `audit_review_evolution`
- `audit_investigation_review_chain`
- `verify_review_evolution_reproducibility`
- `generate_historical_review_audit_report`
- `get_historical_review_audit_report`
- `list_historical_review_audit_reports`
- `get_latest_historical_review_audit_report`

SAFETY INVARIANTS:
1. Rejects prohibited execution, trade recommendations, buy/sell signals, and profitability fields.
2. Read-only decision support; execution engine remains locked.
3. Errors are sanitized and internal exceptions are never leaked directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.opportunities.evolution.audit.service import HistoricalReviewAuditService
from app.analysis.opportunities.evolution.models import (
    SAFE_ID_PATTERN,
    validate_no_prohibited_fields,
)
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.evolution.audit.handlers")


def register_audit_handlers(
    orchestrator: MasterOrchestrator,
    audit_service: HistoricalReviewAuditService,
) -> None:
    """Register review evolution audit command handlers with MasterOrchestrator."""

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

    def handle_audit_historical_review_snapshot(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'audit_historical_review_snapshot'."""
        params = _extract_params(req)
        snapshot_id = params.get("snapshot_id")
        if not snapshot_id or not isinstance(snapshot_id, str):
            raise ValidationError("Parameter 'snapshot_id' is required.")

        res = audit_service.audit_snapshot(snapshot_id)
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "result": res.to_dict(),
        }

    def handle_audit_review_evolution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'audit_review_evolution'."""
        params = _extract_params(req)
        evolution_id = params.get("evolution_id")
        if not evolution_id or not isinstance(evolution_id, str):
            raise ValidationError("Parameter 'evolution_id' is required.")

        res = audit_service.audit_evolution(evolution_id)
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "result": res.to_dict(),
        }

    def handle_audit_investigation_review_chain(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'audit_investigation_review_chain'."""
        params = _extract_params(req)
        investigation_id = params.get("investigation_id")
        if not investigation_id or not isinstance(investigation_id, str):
            raise ValidationError("Parameter 'investigation_id' is required.")

        res = audit_service.audit_investigation_chain(investigation_id)
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "result": res.to_dict(),
        }

    def handle_verify_review_evolution_reproducibility(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'verify_review_evolution_reproducibility'."""
        params = _extract_params(req)
        target_id = params.get("target_id") or params.get("snapshot_id")
        if not target_id or not isinstance(target_id, str):
            raise ValidationError("Parameter 'target_id' is required.")

        target_type = params.get("target_type", "SNAPSHOT")
        res = audit_service.verify_reproducibility(target_id=target_id, target_type=target_type)
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "result": res.to_dict(),
        }

    def handle_generate_historical_review_audit_report(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'generate_historical_review_audit_report'."""
        params = _extract_params(req)
        investigation_id = params.get("investigation_id")
        if not investigation_id or not isinstance(investigation_id, str):
            raise ValidationError("Parameter 'investigation_id' is required.")

        include_rep = bool(params.get("include_reproducibility", True))
        report = audit_service.generate_audit_report(
            investigation_id=investigation_id,
            include_reproducibility=include_rep,
            correlation_id=getattr(req, "correlation_id", None),
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "report_id": report.report_id,
            "overall_status": report.overall_status.value,
            "report": report.to_dict(),
        }

    def handle_get_historical_review_audit_report(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_historical_review_audit_report'."""
        params = _extract_params(req)
        report_id = params.get("report_id")
        if not report_id or not isinstance(report_id, str):
            raise ValidationError("Parameter 'report_id' is required.")

        report = audit_service.get_audit_report(report_id)
        if report is None:
            raise ValidationError(f"Audit report '{report_id}' not found.")

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "report": report.to_dict(),
        }

    def handle_list_historical_review_audit_reports(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_historical_review_audit_reports'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        try:
            limit = int(params.get("limit", 50))
            offset = int(params.get("offset", 0))
        except (ValueError, TypeError):
            limit = 50
            offset = 0

        reports = audit_service.list_audit_reports(
            investigation_id=inv_id,
            limit=limit,
            offset=offset,
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "count": len(reports),
            "reports": [r.to_dict() for r in reports],
        }

    def handle_get_latest_historical_review_audit_report(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_latest_historical_review_audit_report'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        report = audit_service.get_latest_audit_report(investigation_id=inv_id)
        if report is None:
            return {
                "status": "not_found",
                "read_only": True,
                "execution_locked": True,
                "report": None,
            }

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "report": report.to_dict(),
        }

    # Register with orchestrator
    commands = {
        "audit_historical_review_snapshot": handle_audit_historical_review_snapshot,
        "audit_review_evolution": handle_audit_review_evolution,
        "audit_investigation_review_chain": handle_audit_investigation_review_chain,
        "verify_review_evolution_reproducibility": handle_verify_review_evolution_reproducibility,
        "generate_historical_review_audit_report": handle_generate_historical_review_audit_report,
        "get_historical_review_audit_report": handle_get_historical_review_audit_report,
        "list_historical_review_audit_reports": handle_list_historical_review_audit_reports,
        "get_latest_historical_review_audit_report": handle_get_latest_historical_review_audit_report,
    }

    for cmd, fn in commands.items():
        orchestrator.register_handler(cmd, fn)

    logger.info("Historical review audit command handlers registered successfully (%d handlers).", len(commands))
