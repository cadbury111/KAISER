"""KAISER — Investigation Discrepancy Resolution MasterOrchestrator Command Handlers.

Phase 2, Step 29: Handlers for human-driven discrepancy resolution workflows:
- `create_discrepancy_resolution_case`: Open a resolution case from a discrepancy.
- `get_discrepancy_resolution_case`: Fetch case details.
- `list_discrepancy_resolution_cases`: List cases with pagination and filters.
- `transition_discrepancy_resolution`: Move case through valid lifecycle states.
- `attach_resolution_evidence`: Associate evidence IDs and analyst notes.
- `record_evidence_reconciliation`: Record human-reviewed evidence reconciliation.
- `record_human_resolution`: Record human terminal resolution decision.
- `defer_discrepancy_resolution`: Defer resolution pending future evidence.
- `invalidate_discrepancy_resolution`: Mark discrepancy invalid or spurious.

SAFETY INVARIANTS:
1. Prohibited execution, trading, or recommendation parameters are strictly rejected.
2. Read-only decision support; execution engine remains locked.
3. Errors are sanitized and internal exceptions are never leaked directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.opportunities.resolution.models import (
    EvidenceRelationshipType,
    ReconciliationOutcome,
    ResolutionState,
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.resolution.service import DiscrepancyResolutionService
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.resolution.handlers")


def register_discrepancy_resolution_handlers(
    orchestrator: MasterOrchestrator,
    resolution_service: DiscrepancyResolutionService,
) -> None:
    """Register discrepancy resolution command handlers with MasterOrchestrator."""

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

    def handle_create_discrepancy_resolution_case(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'create_discrepancy_resolution_case'."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")
        disc_id = p.get("discrepancy_id")

        if not comp_id or not disc_id:
            return {
                "status": "invalid_request",
                "message": "Both 'comparison_id' and 'discrepancy_id' are required.",
                "read_only": True,
            }

        try:
            case = resolution_service.create_resolution_case(
                comparison_id=str(comp_id),
                discrepancy_id=str(disc_id),
                scope=p.get("scope"),
                discrepancy_category=p.get("discrepancy_category"),
                discrepancy_severity=p.get("discrepancy_severity"),
                discrepancy_description=p.get("discrepancy_description"),
                report_ids=p.get("report_ids"),
                notes=p.get("notes"),
                actor=p.get("actor", "human_reviewer"),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "resolution_case_id": case.resolution_case_id,
                "comparison_id": case.comparison_id,
                "discrepancy_id": case.discrepancy_id,
                "state": case.state.value,
                "case": case.to_dict(),
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
            logger.error("Error creating resolution case: %s", exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to create resolution case: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_get_discrepancy_resolution_case(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_discrepancy_resolution_case'."""
        p = _extract_params(req)
        case_id = p.get("resolution_case_id") or p.get("case_id")

        if not case_id:
            return {
                "status": "invalid_request",
                "message": "Missing 'resolution_case_id' parameter.",
                "read_only": True,
            }

        try:
            case = resolution_service.get_case(str(case_id))
            if not case:
                return {
                    "status": "not_found",
                    "message": f"Resolution case '{case_id}' not found.",
                    "read_only": True,
                    "execution_locked": True,
                }
            return {
                "status": "success",
                "resolution_case_id": case.resolution_case_id,
                "state": case.state.value,
                "case": case.to_dict(),
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
            logger.error("Error fetching resolution case '%s': %s", case_id, exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to retrieve resolution case: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_list_discrepancy_resolution_cases(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_discrepancy_resolution_cases'."""
        p = _extract_params(req)
        comp_id = p.get("comparison_id")
        disc_id = p.get("discrepancy_id")
        state = p.get("state")
        limit = int(p.get("limit", 50))
        offset = int(p.get("offset", 0))

        try:
            cases = resolution_service.list_cases(
                comparison_id=comp_id,
                discrepancy_id=disc_id,
                state=state,
                limit=limit,
                offset=offset,
            )
            return {
                "status": "success",
                "count": len(cases),
                "limit": limit,
                "offset": offset,
                "cases": [c.to_dict() for c in cases],
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
            logger.error("Error listing resolution cases: %s", exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to list resolution cases: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_transition_discrepancy_resolution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'transition_discrepancy_resolution'."""
        p = _extract_params(req)
        case_id = p.get("resolution_case_id") or p.get("case_id")
        target_state = p.get("target_state") or p.get("state")
        reason = p.get("reason")
        actor = p.get("actor", "human_reviewer")

        if not case_id or not target_state or not reason:
            return {
                "status": "invalid_request",
                "message": "'resolution_case_id', 'target_state', and 'reason' are required.",
                "read_only": True,
            }

        try:
            case = resolution_service.transition_case(
                case_id=str(case_id),
                target_state=str(target_state),
                reason=str(reason),
                actor=str(actor),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "resolution_case_id": case.resolution_case_id,
                "state": case.state.value,
                "case": case.to_dict(),
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
            logger.error("Error transitioning case '%s': %s", case_id, exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to transition case: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_attach_resolution_evidence(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'attach_resolution_evidence'."""
        p = _extract_params(req)
        case_id = p.get("resolution_case_id") or p.get("case_id")
        evidence_ids = p.get("evidence_ids", [])
        reviewer_id = p.get("reviewer_id", "human_reviewer")
        notes = p.get("notes")

        if not case_id or not evidence_ids:
            return {
                "status": "invalid_request",
                "message": "'resolution_case_id' and non-empty 'evidence_ids' are required.",
                "read_only": True,
            }

        try:
            case = resolution_service.attach_evidence(
                case_id=str(case_id),
                evidence_ids=list(evidence_ids),
                reviewer_id=str(reviewer_id),
                notes=notes,
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "resolution_case_id": case.resolution_case_id,
                "supporting_evidence_ids": list(case.supporting_evidence_ids),
                "case": case.to_dict(),
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
            logger.error("Error attaching evidence to case '%s': %s", case_id, exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to attach evidence: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_record_evidence_reconciliation(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'record_evidence_reconciliation'."""
        p = _extract_params(req)
        case_id = p.get("resolution_case_id") or p.get("case_id")
        evidence_refs = p.get("evidence_refs", [])
        relationship_type = p.get("relationship_type")
        reconciliation_outcome = p.get("reconciliation_outcome")
        reviewer_notes = p.get("reviewer_notes", "")
        rationale = p.get("rationale")
        reviewer_id = p.get("reviewer_id", "human_reviewer")

        if not case_id or not evidence_refs or not relationship_type or not reconciliation_outcome or not rationale:
            return {
                "status": "invalid_request",
                "message": (
                    "'resolution_case_id', 'evidence_refs', 'relationship_type', "
                    "'reconciliation_outcome', and 'rationale' are required."
                ),
                "read_only": True,
            }

        try:
            case = resolution_service.record_reconciliation(
                case_id=str(case_id),
                evidence_refs=list(evidence_refs),
                relationship_type=relationship_type,
                reconciliation_outcome=reconciliation_outcome,
                reviewer_notes=reviewer_notes,
                rationale=str(rationale),
                reviewer_id=str(reviewer_id),
                evidence_classifications=p.get("evidence_classifications"),
                source_provenance_refs=p.get("source_provenance_refs"),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "resolution_case_id": case.resolution_case_id,
                "reconciliation_count": len(case.reconciliation_records),
                "case": case.to_dict(),
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
            logger.error("Error recording reconciliation on case '%s': %s", case_id, exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to record reconciliation: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_record_human_resolution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'record_human_resolution'."""
        p = _extract_params(req)
        case_id = p.get("resolution_case_id") or p.get("case_id")
        summary = p.get("summary", "")
        rationale = p.get("rationale")
        reviewer_id = p.get("reviewer_id")
        res_state = p.get("resolution_state", "RESOLVED")

        if not case_id or not rationale or not reviewer_id:
            return {
                "status": "invalid_request",
                "message": "'resolution_case_id', 'rationale', and human 'reviewer_id' are required.",
                "read_only": True,
            }

        try:
            case = resolution_service.record_human_resolution(
                case_id=str(case_id),
                summary=str(summary),
                rationale=str(rationale),
                reviewer_id=str(reviewer_id),
                accepted_evidence_ids=p.get("accepted_evidence_ids", ()),
                rejected_evidence_ids=p.get("rejected_evidence_ids", ()),
                residual_uncertainty=p.get("residual_uncertainty"),
                resolution_state=res_state,
                action_required=bool(p.get("action_required", False)),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "resolution_case_id": case.resolution_case_id,
                "state": case.state.value,
                "resolved_by": case.resolved_by,
                "decision": case.decision.to_dict() if case.decision else None,
                "case": case.to_dict(),
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
            logger.error("Error recording human resolution on case '%s': %s", case_id, exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to record human resolution: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_defer_discrepancy_resolution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'defer_discrepancy_resolution'."""
        p = _extract_params(req)
        case_id = p.get("resolution_case_id") or p.get("case_id")
        reason = p.get("reason")
        reviewer_id = p.get("reviewer_id", "human_reviewer")

        if not case_id or not reason:
            return {
                "status": "invalid_request",
                "message": "'resolution_case_id' and 'reason' are required.",
                "read_only": True,
            }

        try:
            case = resolution_service.defer_resolution(
                case_id=str(case_id),
                reason=str(reason),
                reviewer_id=str(reviewer_id),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "resolution_case_id": case.resolution_case_id,
                "state": case.state.value,
                "case": case.to_dict(),
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
            logger.error("Error deferring resolution on case '%s': %s", case_id, exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to defer resolution: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    def handle_invalidate_discrepancy_resolution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'invalidate_discrepancy_resolution'."""
        p = _extract_params(req)
        case_id = p.get("resolution_case_id") or p.get("case_id")
        reason = p.get("reason")
        reviewer_id = p.get("reviewer_id", "human_reviewer")

        if not case_id or not reason:
            return {
                "status": "invalid_request",
                "message": "'resolution_case_id' and 'reason' are required.",
                "read_only": True,
            }

        try:
            case = resolution_service.invalidate_resolution(
                case_id=str(case_id),
                reason=str(reason),
                reviewer_id=str(reviewer_id),
                correlation_id=getattr(req, "correlation_id", None),
            )
            return {
                "status": "success",
                "resolution_case_id": case.resolution_case_id,
                "state": case.state.value,
                "case": case.to_dict(),
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
            logger.error("Error invalidating resolution on case '%s': %s", case_id, exc, exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to invalidate resolution: {exc}",
                "read_only": True,
                "execution_locked": True,
            }

    # Register all handlers
    orchestrator.register_handler(
        command="create_discrepancy_resolution_case",
        handler=handle_create_discrepancy_resolution_case,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_discrepancy_resolution_case",
        handler=handle_get_discrepancy_resolution_case,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="list_discrepancy_resolution_cases",
        handler=handle_list_discrepancy_resolution_cases,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="transition_discrepancy_resolution",
        handler=handle_transition_discrepancy_resolution,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="attach_resolution_evidence",
        handler=handle_attach_resolution_evidence,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="record_evidence_reconciliation",
        handler=handle_record_evidence_reconciliation,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="record_human_resolution",
        handler=handle_record_human_resolution,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="defer_discrepancy_resolution",
        handler=handle_defer_discrepancy_resolution,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="invalidate_discrepancy_resolution",
        handler=handle_invalidate_discrepancy_resolution,
        capability="discrepancy_resolution",
        allow_override=True,
    )
    logger.info("Registered investigation discrepancy resolution handlers with MasterOrchestrator.")
