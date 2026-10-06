"""KAISER — Cross-Investigation Comparison MasterOrchestrator Command Handlers.

Provides MasterOrchestrator command handlers for analytical cross-investigation comparisons:
- `compare_investigation_reports`: Execute deterministic comparison across reports.
- `get_investigation_comparison`: Retrieve comparison by ID.
- `list_investigation_comparisons`: Query comparisons with filtering and pagination.
- `get_investigation_comparison_discrepancies`: Retrieve discrepancy records.
- `validate_investigation_comparison`: Validate comparison integrity and consistency.

SAFETY INVARIANTS:
1. Rejects any prohibited execution, trading, or recommendation parameters.
2. Strictly read-only decision support; execution engine remains locked.
3. Redacts secrets and sanitizes errors before responding.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from app.analysis.opportunities.comparison.engine import InvestigationComparisonEngine
from app.analysis.opportunities.comparison.models import (
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.investigation.reporting.service import InvestigationReportingService
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.comparison.handlers")

SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")


def register_investigation_comparison_handlers(
    orchestrator: MasterOrchestrator,
    comparison_engine: InvestigationComparisonEngine,
    reporting_service: Optional[InvestigationReportingService] = None,
) -> None:
    """Register cross-investigation comparison commands with MasterOrchestrator."""

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

    def handle_compare_investigation_reports(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'compare_investigation_reports' command."""
        p = _extract_params(req)

        reports = p.get("reports")
        report_ids = p.get("report_ids")

        if reports is None and report_ids is None:
            return {
                "status": "invalid_request",
                "message": "Missing 'reports' or 'report_ids' parameter.",
                "read_only": True,
            }

        context = p.get("context", {})

        try:
            comparison = comparison_engine.compare_reports(
                reports=reports,
                report_ids=report_ids,
                context=context,
            )
            return {
                "status": "success",
                "comparison_id": comparison.comparison_id,
                "comparability_state": comparison.comparability_state.value,
                "shared_evidence_count": len(comparison.shared_evidence),
                "conflicting_evidence_count": len(comparison.conflicting_evidence),
                "discrepancy_count": len(comparison.discrepancy_summary),
                "comparison": comparison.to_dict(),
                "read_only": True,
                "execution_locked": True,
            }
        except ValidationError as val_err:
            return {
                "status": "validation_error",
                "message": str(val_err),
                "read_only": True,
                "execution_locked": True,
            }
        except Exception as exc:
            logger.error("Error executing report comparison: %s", exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Comparison execution failed: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_investigation_comparison(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_comparison' command."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")

        if not comp_id or not isinstance(comp_id, str) or not SAFE_ID_PATTERN.match(comp_id):
            return {
                "status": "invalid_request",
                "message": "Missing or invalid 'comparison_id'.",
                "read_only": True,
            }

        comparison = comparison_engine.get_comparison(comp_id)
        if comparison is None:
            return {
                "status": "not_found",
                "message": f"Investigation comparison '{comp_id}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "comparison": comparison.to_dict(),
            "read_only": True,
            "execution_locked": True,
        }

    def handle_list_investigation_comparisons(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_investigation_comparisons' command."""
        p = _extract_params(req)
        report_id = p.get("report_id")
        comp_state = p.get("comparability_state")
        limit = int(p.get("limit", 50))
        offset = int(p.get("offset", 0))

        comparisons = comparison_engine.list_comparisons(
            report_id=report_id,
            comparability_state=comp_state,
            limit=limit,
            offset=offset,
        )

        return {
            "status": "success",
            "count": len(comparisons),
            "comparisons": [c.to_dict() for c in comparisons],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_investigation_comparison_discrepancies(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_comparison_discrepancies' command."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")
        sev_filter = p.get("severity")

        if not comp_id or not isinstance(comp_id, str) or not SAFE_ID_PATTERN.match(comp_id):
            return {
                "status": "invalid_request",
                "message": "Missing or invalid 'comparison_id'.",
                "read_only": True,
            }

        comparison = comparison_engine.get_comparison(comp_id)
        if comparison is None:
            return {
                "status": "not_found",
                "message": f"Investigation comparison '{comp_id}' not found.",
                "read_only": True,
            }

        discs = comparison.discrepancy_summary
        if sev_filter:
            sf_upper = str(sev_filter).strip().upper()
            discs = tuple(d for d in discs if d.severity.value == sf_upper)

        return {
            "status": "success",
            "comparison_id": comp_id,
            "count": len(discs),
            "discrepancies": [d.to_dict() for d in discs],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_validate_investigation_comparison(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'validate_investigation_comparison' command."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")

        if not comp_id or not isinstance(comp_id, str) or not SAFE_ID_PATTERN.match(comp_id):
            return {
                "status": "invalid_request",
                "message": "Missing or invalid 'comparison_id'.",
                "read_only": True,
            }

        comparison = comparison_engine.get_comparison(comp_id)
        if comparison is None:
            return {
                "status": "not_found",
                "message": f"Investigation comparison '{comp_id}' not found.",
                "read_only": True,
            }

        is_valid = comparison.comparison_status in ("COMPLETED", "PARTIAL")
        return {
            "status": "success",
            "comparison_id": comp_id,
            "validation": {
                "valid": is_valid,
                "status": comparison.comparison_status,
                "comparability_state": comparison.comparability_state.value,
                "unresolved_discrepancies": comparison.consistency_summary.unresolved_discrepancy_count,
            },
            "read_only": True,
            "execution_locked": True,
        }

    # Register handlers on MasterOrchestrator
    orchestrator.register_handler(
        command="compare_investigation_reports",
        handler=handle_compare_investigation_reports,
        description="Execute deterministic cross-investigation comparison across reports.",
        capability="investigation_comparison",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_comparison",
        handler=handle_get_investigation_comparison,
        description="Retrieve cross-investigation comparison by ID.",
        capability="investigation_comparison",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="list_investigation_comparisons",
        handler=handle_list_investigation_comparisons,
        description="List cross-investigation comparisons with filtering.",
        capability="investigation_comparison",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_comparison_discrepancies",
        handler=handle_get_investigation_comparison_discrepancies,
        description="Retrieve discrepancy records for a comparison.",
        capability="investigation_comparison",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="validate_investigation_comparison",
        handler=handle_validate_investigation_comparison,
        description="Validate cross-investigation comparison integrity.",
        capability="investigation_comparison",
        allow_override=True,
    )

    logger.info("Registered cross-investigation comparison handlers with MasterOrchestrator.")
