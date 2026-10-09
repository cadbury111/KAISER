"""KAISER — Historical Review Audit Reliability Command Handlers.

Phase 2, Step 34: Orchestrator command handlers for evaluating audit coverage,
verifying deterministic repeatability, monitoring regressions, and assessing long-term reliability:
- `evaluate_historical_review_audit_coverage`
- `verify_historical_review_audit_repeatability`
- `check_historical_review_audit_regression`
- `evaluate_historical_review_audit_reliability`
- `get_historical_review_reliability_result`
- `list_historical_review_reliability_results`
- `get_latest_historical_review_reliability_result`

SAFETY INVARIANTS:
1. Rejects prohibited execution, trade recommendations, buy/sell signals, and profitability fields.
2. Read-only decision support; execution engine remains locked.
3. Errors are sanitized and internal exceptions are never leaked directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.opportunities.evolution.audit.reliability.service import (
    HistoricalReviewAuditReliabilityService,
)
from app.analysis.opportunities.evolution.models import (
    SAFE_ID_PATTERN,
    validate_no_prohibited_fields,
)
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.evolution.audit.reliability.handlers")


def register_reliability_handlers(
    orchestrator: MasterOrchestrator,
    reliability_service: HistoricalReviewAuditReliabilityService,
) -> None:
    """Register review audit reliability command handlers with MasterOrchestrator."""

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

    def handle_evaluate_historical_review_audit_coverage(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'evaluate_historical_review_audit_coverage'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        if not inv_id or not isinstance(inv_id, str):
            raise ValidationError("Missing or invalid 'investigation_id' parameter.")

        summary = reliability_service.evaluate_coverage(inv_id)
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "coverage_summary": summary.to_dict(),
        }

    def handle_verify_historical_review_audit_repeatability(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'verify_historical_review_audit_repeatability'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        if not inv_id or not isinstance(inv_id, str):
            raise ValidationError("Missing or invalid 'investigation_id' parameter.")

        runs = int(params.get("runs", 2))
        res = reliability_service.verify_repeatability(inv_id, runs=runs)
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "repeatability_result": res.to_dict(),
        }

    def handle_check_historical_review_audit_regression(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'check_historical_review_audit_regression'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        if not inv_id or not isinstance(inv_id, str):
            raise ValidationError("Missing or invalid 'investigation_id' parameter.")

        base_id = params.get("baseline_report_id")
        curr_id = params.get("current_report_id")
        res = reliability_service.monitor_regression(
            inv_id,
            baseline_report_id=base_id,
            current_report_id=curr_id,
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "regression_result": res.to_dict(),
        }

    def handle_evaluate_historical_review_audit_reliability(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'evaluate_historical_review_audit_reliability'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        if not inv_id or not isinstance(inv_id, str):
            raise ValidationError("Missing or invalid 'investigation_id' parameter.")

        runs = int(params.get("runs_for_repeatability", 2))
        res = reliability_service.evaluate_reliability(inv_id, runs_for_repeatability=runs)
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "reliability_result": res.to_dict(),
        }

    def handle_get_historical_review_reliability_result(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_historical_review_reliability_result'."""
        params = _extract_params(req)
        eval_id = params.get("evaluation_id")
        if not eval_id or not isinstance(eval_id, str):
            raise ValidationError("Missing or invalid 'evaluation_id' parameter.")

        res = reliability_service.get_reliability_result(eval_id)
        if res is None:
            return {
                "status": "not_found",
                "read_only": True,
                "execution_locked": True,
                "evaluation": None,
            }

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "evaluation": res.to_dict(),
        }

    def handle_list_historical_review_reliability_results(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_historical_review_reliability_results'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        limit = int(params.get("limit", 50))
        offset = int(params.get("offset", 0))

        results = reliability_service.list_reliability_results(
            investigation_id=inv_id,
            limit=limit,
            offset=offset,
        )
        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "count": len(results),
            "evaluations": [r.to_dict() for r in results],
        }

    def handle_get_latest_historical_review_reliability_result(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_latest_historical_review_reliability_result'."""
        params = _extract_params(req)
        inv_id = params.get("investigation_id")
        res = reliability_service.get_latest_reliability_result(investigation_id=inv_id)
        if res is None:
            return {
                "status": "not_found",
                "read_only": True,
                "execution_locked": True,
                "evaluation": None,
            }

        return {
            "status": "success",
            "read_only": True,
            "execution_locked": True,
            "evaluation": res.to_dict(),
        }

    # Register with orchestrator
    commands = {
        "evaluate_historical_review_audit_coverage": handle_evaluate_historical_review_audit_coverage,
        "verify_historical_review_audit_repeatability": handle_verify_historical_review_audit_repeatability,
        "check_historical_review_audit_regression": handle_check_historical_review_audit_regression,
        "evaluate_historical_review_audit_reliability": handle_evaluate_historical_review_audit_reliability,
        "get_historical_review_reliability_result": handle_get_historical_review_reliability_result,
        "list_historical_review_reliability_results": handle_list_historical_review_reliability_results,
        "get_latest_historical_review_reliability_result": handle_get_latest_historical_review_reliability_result,
    }

    for cmd, fn in commands.items():
        orchestrator.register_handler(cmd, fn)

    logger.info("Historical review audit reliability command handlers registered successfully (%d handlers).", len(commands))
