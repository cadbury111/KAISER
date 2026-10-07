"""KAISER — Investigation Evidence Consolidation MasterOrchestrator Command Handlers.

Phase 2, Step 30: Command handlers for consolidated investigation outcomes:
- `create_investigation_outcome_consolidation`
- `get_investigation_outcome`
- `list_investigation_outcomes`
- `refresh_investigation_outcome`
- `get_investigation_outcome_evidence`
- `get_investigation_outcome_conflicts`
- `get_investigation_outcome_limitations`
- `get_investigation_outcome_provenance`

SAFETY INVARIANTS:
1. Rejects prohibited execution, trade recommendations, buy/sell signals, and profitability fields.
2. Read-only decision support; execution engine remains locked.
3. Errors are sanitized and internal exceptions are never leaked directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.opportunities.consolidation.models import (
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.consolidation.service import (
    InvestigationOutcomeConsolidationService,
)
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.consolidation.handlers")


def register_consolidation_handlers(
    orchestrator: MasterOrchestrator,
    consolidation_service: InvestigationOutcomeConsolidationService,
) -> None:
    """Register investigation outcome consolidation command handlers with MasterOrchestrator."""

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

    def handle_create_investigation_outcome_consolidation(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'create_investigation_outcome_consolidation'."""
        p = _extract_params(req)
        inv_id = p.get("investigation_id") or p.get("session_id")

        if not inv_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'investigation_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        try:
            outcome = consolidation_service.consolidate_investigation_outcome(
                investigation_id=str(inv_id),
                report_ids=p.get("report_ids"),
                comparison_ids=p.get("comparison_ids"),
                resolution_case_ids=p.get("resolution_case_ids"),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "outcome_id": outcome.outcome_id,
                "investigation_id": outcome.investigation_id,
                "outcome_state": outcome.outcome_state.value,
                "outcome": outcome.to_dict(),
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
            logger.error("Error creating investigation consolidation outcome: %s", exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to consolidate investigation outcome: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_investigation_outcome(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_outcome'."""
        p = _extract_params(req)
        outcome_id = p.get("outcome_id") or p.get("id")

        if not outcome_id:
            return {
                "status": "invalid_request",
                "message": "Missing 'outcome_id' parameter.",
                "read_only": True,
                "execution_locked": True,
            }

        outcome = consolidation_service.get_outcome(str(outcome_id))
        if not outcome:
            return {
                "status": "not_found",
                "message": f"Outcome '{outcome_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "outcome_id": outcome.outcome_id,
            "outcome": outcome.to_dict(),
            "read_only": True,
            "execution_locked": True,
        }

    def handle_list_investigation_outcomes(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_investigation_outcomes'."""
        p = _extract_params(req)
        inv_id = p.get("investigation_id")
        state_filter = p.get("state")
        limit = p.get("limit", 50)
        offset = p.get("offset", 0)

        outcomes = consolidation_service.list_outcomes(
            investigation_id=str(inv_id) if inv_id else None,
            state=str(state_filter) if state_filter else None,
            limit=limit,
            offset=offset,
        )

        return {
            "status": "success",
            "count": len(outcomes),
            "outcomes": [o.to_dict() for o in outcomes],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_refresh_investigation_outcome(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'refresh_investigation_outcome'."""
        p = _extract_params(req)
        outcome_id = p.get("outcome_id") or p.get("id")

        if not outcome_id:
            return {
                "status": "invalid_request",
                "message": "Missing 'outcome_id' parameter.",
                "read_only": True,
                "execution_locked": True,
            }

        try:
            refreshed = consolidation_service.refresh_outcome(
                outcome_id=str(outcome_id),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "outcome_id": refreshed.outcome_id,
                "previous_outcome_id": outcome_id,
                "outcome_state": refreshed.outcome_state.value,
                "outcome": refreshed.to_dict(),
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
            logger.error("Error refreshing outcome '%s': %s", outcome_id, exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to refresh outcome: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_investigation_outcome_evidence(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_outcome_evidence'."""
        p = _extract_params(req)
        outcome_id = p.get("outcome_id") or p.get("id")

        if not outcome_id:
            return {
                "status": "invalid_request",
                "message": "Missing 'outcome_id' parameter.",
                "read_only": True,
                "execution_locked": True,
            }

        outcome = consolidation_service.get_outcome(str(outcome_id))
        if not outcome:
            return {
                "status": "not_found",
                "message": f"Outcome '{outcome_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "outcome_id": outcome.outcome_id,
            "facts": [f.to_dict() for f in outcome.facts],
            "interpretations": [i.to_dict() for i in outcome.interpretations],
            "human_review_decisions": [d.to_dict() for d in outcome.human_review_decisions],
            "evidence_summary": outcome.evidence_summary,
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_investigation_outcome_conflicts(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_outcome_conflicts'."""
        p = _extract_params(req)
        outcome_id = p.get("outcome_id") or p.get("id")

        if not outcome_id:
            return {
                "status": "invalid_request",
                "message": "Missing 'outcome_id' parameter.",
                "read_only": True,
                "execution_locked": True,
            }

        outcome = consolidation_service.get_outcome(str(outcome_id))
        if not outcome:
            return {
                "status": "not_found",
                "message": f"Outcome '{outcome_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "outcome_id": outcome.outcome_id,
            "conflicts": [c.to_dict() for c in outcome.conflicts],
            "unresolved_items": list(outcome.unresolved_items),
            "resolved_items": list(outcome.resolved_items),
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_investigation_outcome_limitations(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_outcome_limitations'."""
        p = _extract_params(req)
        outcome_id = p.get("outcome_id") or p.get("id")

        if not outcome_id:
            return {
                "status": "invalid_request",
                "message": "Missing 'outcome_id' parameter.",
                "read_only": True,
                "execution_locked": True,
            }

        outcome = consolidation_service.get_outcome(str(outcome_id))
        if not outcome:
            return {
                "status": "not_found",
                "message": f"Outcome '{outcome_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "outcome_id": outcome.outcome_id,
            "limitations": [lim.to_dict() for lim in outcome.limitations],
            "quality_summary": outcome.quality_summary.to_dict(),
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_investigation_outcome_provenance(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_outcome_provenance'."""
        p = _extract_params(req)
        outcome_id = p.get("outcome_id") or p.get("id")

        if not outcome_id:
            return {
                "status": "invalid_request",
                "message": "Missing 'outcome_id' parameter.",
                "read_only": True,
                "execution_locked": True,
            }

        outcome = consolidation_service.get_outcome(str(outcome_id))
        if not outcome:
            return {
                "status": "not_found",
                "message": f"Outcome '{outcome_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "outcome_id": outcome.outcome_id,
            "traceability": outcome.traceability,
            "provenance_summary": outcome.provenance_summary,
            "reproducibility_summary": outcome.reproducibility_summary.to_dict(),
            "read_only": True,
            "execution_locked": True,
        }

    # Register handlers with MasterOrchestrator
    orchestrator.register_handler(
        command="create_investigation_outcome_consolidation",
        handler=handle_create_investigation_outcome_consolidation,
        capability="investigation_consolidation",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_outcome",
        handler=handle_get_investigation_outcome,
        capability="investigation_consolidation",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="list_investigation_outcomes",
        handler=handle_list_investigation_outcomes,
        capability="investigation_consolidation",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="refresh_investigation_outcome",
        handler=handle_refresh_investigation_outcome,
        capability="investigation_consolidation",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_outcome_evidence",
        handler=handle_get_investigation_outcome_evidence,
        capability="investigation_consolidation",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_outcome_conflicts",
        handler=handle_get_investigation_outcome_conflicts,
        capability="investigation_consolidation",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_outcome_limitations",
        handler=handle_get_investigation_outcome_limitations,
        capability="investigation_consolidation",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_outcome_provenance",
        handler=handle_get_investigation_outcome_provenance,
        capability="investigation_consolidation",
        allow_override=True,
    )

    logger.info("Registered investigation outcome consolidation handlers with MasterOrchestrator.")
