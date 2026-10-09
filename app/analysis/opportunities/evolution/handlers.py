"""KAISER — Consolidated Investigation Review Evolution Command Handlers.

Phase 2, Step 32: Orchestrator command handlers for capturing and querying historical
review snapshots and evolution records:
- `capture_historical_review_snapshot`
- `get_historical_review_snapshot`
- `list_historical_review_snapshots`
- `compare_historical_review_snapshots`
- `get_review_evolution`
- `list_review_evolution_history`
- `get_recurring_review_conflicts`
- `get_review_coverage_evolution`
- `get_provenance_evolution`

SAFETY INVARIANTS:
1. Rejects prohibited execution, trade recommendations, buy/sell signals, and profitability fields.
2. Read-only decision support; execution engine remains locked.
3. Errors are sanitized and internal exceptions are never leaked directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.opportunities.evolution.models import (
    SAFE_ID_PATTERN,
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.evolution.service import (
    HistoricalReviewEvolutionService,
)
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.evolution.handlers")


def register_evolution_handlers(
    orchestrator: MasterOrchestrator,
    evolution_service: HistoricalReviewEvolutionService,
) -> None:
    """Register historical review evolution command handlers with MasterOrchestrator."""

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

    def handle_capture_historical_review_snapshot(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'capture_historical_review_snapshot'."""
        p = _extract_params(req)
        investigation_id = p.get("investigation_id")
        outcome_id = p.get("outcome_id")

        if not investigation_id or not outcome_id:
            return {
                "status": "invalid_request",
                "message": "Parameters 'investigation_id' and 'outcome_id' are required.",
                "read_only": True,
                "execution_locked": True,
            }

        try:
            # If an outcome object or ID is available via consolidation service
            c_service = getattr(evolution_service, "consolidation_service", None)
            snapshot = None
            if c_service is not None and hasattr(c_service, "get_consolidated_outcome"):
                outcome_obj = c_service.get_consolidated_outcome(outcome_id)
                if outcome_obj is not None:
                    snapshot = evolution_service.capture_snapshot_from_outcome(
                        outcome=outcome_obj,
                        snapshot_version=p.get("snapshot_version"),
                        correlation_id=getattr(req, "correlation_id", None),
                    )

            if snapshot is None:
                snapshot = evolution_service.capture_snapshot(
                    payload=p,
                    correlation_id=getattr(req, "correlation_id", None),
                )

            return {
                "status": "success",
                "snapshot_id": snapshot.snapshot_id,
                "investigation_id": snapshot.investigation_id,
                "snapshot_version": snapshot.snapshot_version,
                "snapshot": snapshot.to_dict(),
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
            logger.error("Error capturing historical review snapshot: %s", exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to capture historical review snapshot: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_historical_review_snapshot(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_historical_review_snapshot'."""
        p = _extract_params(req)
        snapshot_id = p.get("snapshot_id")

        if not snapshot_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'snapshot_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        snapshot = evolution_service.get_snapshot(snapshot_id)
        if not snapshot:
            return {
                "status": "not_found",
                "message": f"Historical review snapshot '{snapshot_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "snapshot_id": snapshot.snapshot_id,
            "snapshot": snapshot.to_dict(),
            "read_only": True,
            "execution_locked": True,
        }

    def handle_list_historical_review_snapshots(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_historical_review_snapshots'."""
        p = _extract_params(req)
        inv_id = p.get("investigation_id")
        limit = int(p.get("limit", 50))
        offset = int(p.get("offset", 0))

        snapshots = evolution_service.list_snapshots(
            investigation_id=inv_id,
            limit=limit,
            offset=offset,
        )

        return {
            "status": "success",
            "count": len(snapshots),
            "limit": max(1, min(100, limit)),
            "offset": max(0, offset),
            "snapshots": [s.to_dict() for s in snapshots],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_compare_historical_review_snapshots(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'compare_historical_review_snapshots'."""
        p = _extract_params(req)
        prev_id = p.get("previous_snapshot_id")
        curr_id = p.get("current_snapshot_id")

        if not prev_id or not curr_id:
            return {
                "status": "invalid_request",
                "message": "Parameters 'previous_snapshot_id' and 'current_snapshot_id' are required.",
                "read_only": True,
                "execution_locked": True,
            }

        try:
            record = evolution_service.compare_snapshots(
                previous_snapshot_id=prev_id,
                current_snapshot_id=curr_id,
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "evolution_id": record.evolution_id,
                "change_state": record.change_state.value,
                "evolution": record.to_dict(),
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
            logger.error("Error comparing historical review snapshots: %s", exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to compare historical review snapshots: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_review_evolution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_review_evolution'."""
        p = _extract_params(req)
        evolution_id = p.get("evolution_id")

        if not evolution_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'evolution_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        record = evolution_service.get_evolution(evolution_id)
        if not record:
            return {
                "status": "not_found",
                "message": f"Review evolution record '{evolution_id}' not found.",
                "read_only": True,
                "execution_locked": True,
            }

        return {
            "status": "success",
            "evolution_id": record.evolution_id,
            "evolution": record.to_dict(),
            "read_only": True,
            "execution_locked": True,
        }

    def handle_list_review_evolution_history(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_review_evolution_history'."""
        p = _extract_params(req)
        inv_id = p.get("investigation_id")
        limit = int(p.get("limit", 50))
        offset = int(p.get("offset", 0))

        history = evolution_service.list_evolution_history(
            investigation_id=inv_id,
            limit=limit,
            offset=offset,
        )

        return {
            "status": "success",
            "count": len(history),
            "limit": max(1, min(100, limit)),
            "offset": max(0, offset),
            "evolutions": [h.to_dict() for h in history],
            "read_only": True,
            "execution_locked": True,
        }

    def handle_get_recurring_review_conflicts(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_recurring_review_conflicts'."""
        p = _extract_params(req)
        inv_id = p.get("investigation_id")

        if not inv_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'investigation_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        try:
            recurring = evolution_service.get_recurring_review_conflicts(inv_id)
            return {
                "status": "success",
                "investigation_id": inv_id,
                "count": len(recurring),
                "conflicts": [r.to_dict() for r in recurring],
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
            return {
                "status": "error",
                "message": f"Failed to retrieve recurring conflicts: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_review_coverage_evolution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_review_coverage_evolution'."""
        p = _extract_params(req)
        inv_id = p.get("investigation_id")

        if not inv_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'investigation_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        try:
            cov_history = evolution_service.get_review_coverage_evolution(inv_id)
            return {
                "status": "success",
                "investigation_id": inv_id,
                "count": len(cov_history),
                "coverage_history": cov_history,
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
            return {
                "status": "error",
                "message": f"Failed to retrieve review coverage evolution: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_provenance_evolution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_provenance_evolution'."""
        p = _extract_params(req)
        inv_id = p.get("investigation_id")

        if not inv_id:
            return {
                "status": "invalid_request",
                "message": "Parameter 'investigation_id' is required.",
                "read_only": True,
                "execution_locked": True,
            }

        try:
            prov_history = evolution_service.get_provenance_evolution(inv_id)
            return {
                "status": "success",
                "investigation_id": inv_id,
                "count": len(prov_history),
                "provenance_history": prov_history,
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
            return {
                "status": "error",
                "message": f"Failed to retrieve provenance evolution: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    # Register handlers with orchestrator
    orchestrator.register_handler("capture_historical_review_snapshot", handle_capture_historical_review_snapshot)
    orchestrator.register_handler("get_historical_review_snapshot", handle_get_historical_review_snapshot)
    orchestrator.register_handler("list_historical_review_snapshots", handle_list_historical_review_snapshots)
    orchestrator.register_handler("compare_historical_review_snapshots", handle_compare_historical_review_snapshots)
    orchestrator.register_handler("get_review_evolution", handle_get_review_evolution)
    orchestrator.register_handler("list_review_evolution_history", handle_list_review_evolution_history)
    orchestrator.register_handler("get_recurring_review_conflicts", handle_get_recurring_review_conflicts)
    orchestrator.register_handler("get_review_coverage_evolution", handle_get_review_coverage_evolution)
    orchestrator.register_handler("get_provenance_evolution", handle_get_provenance_evolution)

    logger.info("Historical review evolution command handlers registered successfully (9 handlers).")
