"""KAISER — Cross-Investigation Consolidated Outcome Consistency Command Handlers.

Phase 2, Step 31: Command handlers for comparing consolidated investigation outcomes:
- `compare_consolidated_outcomes`
- `get_consolidated_outcome_comparison`
- `list_consolidated_outcome_comparisons`
- `get_consolidated_outcome_comparison_dimensions`
- `get_consolidated_outcome_comparison_evidence`
- `get_consolidated_outcome_comparison_review_coverage`
- `get_consolidated_outcome_comparison_divergences`
- `get_consolidated_outcome_comparison_provenance`

SAFETY INVARIANTS:
1. Rejects prohibited execution, trade recommendations, buy/sell signals, and profitability fields.
2. Read-only decision support; execution engine remains locked.
3. Errors are sanitized and internal exceptions are never leaked directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.opportunities.consistency.models import (
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.consistency.service import (
    ConsolidatedOutcomeConsistencyService,
)
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.consistency.handlers")


def register_consistency_handlers(
    orchestrator: MasterOrchestrator,
    consistency_service: ConsolidatedOutcomeConsistencyService,
) -> None:
    """Register consolidated outcome consistency command handlers with MasterOrchestrator."""

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

    def handle_compare_consolidated_outcomes(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'compare_consolidated_outcomes'."""
        p = _extract_params(req)
        outcome_ids = p.get("outcome_ids")

        if not outcome_ids:
            return {
                "status": "invalid_request",
                "message": "Parameter 'outcome_ids' is required (minimum 2 outcomes).",
                "read_only": True,
                "execution_locked": True,
            }

        try:
            comparison = consistency_service.compare_consolidated_outcomes(
                outcome_ids=outcome_ids,
                comparison_scope=p.get("comparison_scope", "STANDARD"),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "comparison_id": comparison.comparison_id,
                "comparability_state": comparison.comparability_state.value,
                "lifecycle_state": comparison.lifecycle_state.value,
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
            logger.error("Error comparing consolidated outcomes: %s", exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to compare consolidated outcomes: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_consolidated_outcome_comparison(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_consolidated_outcome_comparison'."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")

        if not comp_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'comparison_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        comparison = consistency_service.get_comparison(str(comp_id))
        if comparison is None:
            return {
                "status": "not_found",
                "message": f"Consolidated outcome comparison '{comp_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "comparison": comparison.to_dict(),
            "read_only": True,
            "execution_locked": True,
        }

    def handle_list_consolidated_outcome_comparisons(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_consolidated_outcome_comparisons'."""
        p = _extract_params(req)
        try:
            limit = int(p.get("limit", 50))
            offset = int(p.get("offset", 0))
        except (ValueError, TypeError):
            limit = 50
            offset = 0

        comparisons = consistency_service.list_comparisons(
            comparability_state=p.get("comparability_state"),
            limit=limit,
            offset=offset,
        )
        return {
            "status": "success",
            "count": len(comparisons),
            "limit": limit,
            "offset": offset,
            "comparisons": [c.to_dict() for c in comparisons],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_consolidated_outcome_comparison_dimensions(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_consolidated_outcome_comparison_dimensions'."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")

        if not comp_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'comparison_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        dims = consistency_service.get_comparison_dimensions(str(comp_id))
        if dims is None:
            return {
                "status": "not_found",
                "message": f"Consolidated outcome comparison '{comp_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "comparison_id": str(comp_id),
            "dimensions": [d.to_dict() for d in dims],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_consolidated_outcome_comparison_evidence(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_consolidated_outcome_comparison_evidence'."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")

        if not comp_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'comparison_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        ev = consistency_service.get_comparison_evidence(str(comp_id))
        if ev is None:
            return {
                "status": "not_found",
                "message": f"Consolidated outcome comparison '{comp_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "comparison_id": str(comp_id),
            "count": len(ev),
            "evidence_consistency": [e.to_dict() for e in ev],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_consolidated_outcome_comparison_review_coverage(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_consolidated_outcome_comparison_review_coverage'."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")

        if not comp_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'comparison_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        cov = consistency_service.get_comparison_review_coverage(str(comp_id))
        if cov is None:
            return {
                "status": "not_found",
                "message": f"Consolidated outcome comparison '{comp_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "comparison_id": str(comp_id),
            "review_coverage": cov.to_dict(),
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_consolidated_outcome_comparison_divergences(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_consolidated_outcome_comparison_divergences'."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")

        if not comp_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'comparison_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        divs = consistency_service.get_comparison_divergences(str(comp_id))
        if divs is None:
            return {
                "status": "not_found",
                "message": f"Consolidated outcome comparison '{comp_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "comparison_id": str(comp_id),
            "count": len(divs),
            "divergences": [d.to_dict() for d in divs],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_consolidated_outcome_comparison_provenance(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_consolidated_outcome_comparison_provenance'."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")

        if not comp_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'comparison_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        prov = consistency_service.get_comparison_provenance(str(comp_id))
        if prov is None:
            return {
                "status": "not_found",
                "message": f"Consolidated outcome comparison '{comp_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "comparison_id": str(comp_id),
            **prov,
            "read_only": True,
            "execution_locked": True,
        }

    # Register handlers with MasterOrchestrator
    capability = "consolidated_outcome_consistency"
    orchestrator.register_handler(
        "compare_consolidated_outcomes",
        handle_compare_consolidated_outcomes,
        capability=capability,
    )
    orchestrator.register_handler(
        "get_consolidated_outcome_comparison",
        handle_get_consolidated_outcome_comparison,
        capability=capability,
    )
    orchestrator.register_handler(
        "list_consolidated_outcome_comparisons",
        handle_list_consolidated_outcome_comparisons,
        capability=capability,
    )
    orchestrator.register_handler(
        "get_consolidated_outcome_comparison_dimensions",
        handle_get_consolidated_outcome_comparison_dimensions,
        capability=capability,
    )
    orchestrator.register_handler(
        "get_consolidated_outcome_comparison_evidence",
        handle_get_consolidated_outcome_comparison_evidence,
        capability=capability,
    )
    orchestrator.register_handler(
        "get_consolidated_outcome_comparison_review_coverage",
        handle_get_consolidated_outcome_comparison_review_coverage,
        capability=capability,
    )
    orchestrator.register_handler(
        "get_consolidated_outcome_comparison_divergences",
        handle_get_consolidated_outcome_comparison_divergences,
        capability=capability,
    )
    orchestrator.register_handler(
        "get_consolidated_outcome_comparison_provenance",
        handle_get_consolidated_outcome_comparison_provenance,
        capability=capability,
    )

    logger.info("Registered cross-investigation consolidated outcome consistency handlers with MasterOrchestrator.")
