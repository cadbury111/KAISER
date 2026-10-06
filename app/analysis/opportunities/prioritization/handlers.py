"""KAISER — Opportunity Prioritization Orchestrator Command Handlers.

Provides MasterOrchestrator command handlers for:
- `prioritize_opportunity`: Prioritize a single OpportunityCandidate.
- `prioritize_opportunity_group`: Deterministically rank multiple candidates.
- `get_opportunity_priority`: Retrieve priority record by ID.
- `list_opportunity_priorities`: Query historical priority records with pagination.
- `get_investigation_queue`: Retrieve current investigation queue in priority order.
- `validate_opportunity_priority`: Check eligibility and invalidation conditions of priority record.

SAFETY INVARIANTS:
1. Rejects any prohibited execution, trading, or recommendation parameters.
2. Strictly read-only decision support; execution engine remains locked.
3. Redacts secrets and sanitizes errors before responding.
"""

from __future__ import annotations

import collections
from collections.abc import Mapping
import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from app.analysis.opportunities.models import OpportunityCandidate
from app.analysis.opportunities.engine import OpportunityAssessmentEngine
from app.analysis.opportunities.prioritization.engine import (
    OpportunityPrioritizationEngine,
)
from app.analysis.opportunities.prioritization.models import (
    OpportunityPriorityRecord,
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.prioritization.queue import (
    OpportunityInvestigationQueue,
)
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.prioritization.handlers")

SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")


def register_prioritization_handlers(
    orchestrator: MasterOrchestrator,
    prioritization_engine: OpportunityPrioritizationEngine,
    investigation_queue: OpportunityInvestigationQueue,
    opportunity_assessment_engine: Optional[OpportunityAssessmentEngine] = None,
) -> None:
    """Register opportunity prioritization commands with MasterOrchestrator."""

    def handle_prioritize_opportunity(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'prioritize_opportunity' command."""
        p = dict(
            getattr(req, "context", {})
            or getattr(req, "parameters", {})
            or getattr(req, "payload", {})
            or {}
        )

        # SAFETY GUARD: Check for prohibited fields in input
        validate_no_prohibited_fields(p, path="request_parameters")

        candidate_id = p.get("candidate_id")
        candidate_dict = p.get("candidate")
        enqueue = bool(p.get("enqueue", True))
        metadata = p.get("metadata")

        target_candidate: Any = None
        if candidate_id:
            cid = str(candidate_id).strip()
            if not SAFE_ID_PATTERN.match(cid):
                raise ValidationError(f"Invalid candidate_id format: '{cid}'.")
            if opportunity_assessment_engine:
                target_candidate = opportunity_assessment_engine.get_assessment(cid)
            if target_candidate is None and not candidate_dict:
                raise ValidationError(f"Opportunity candidate '{cid}' not found.")

        if target_candidate is None and candidate_dict:
            if isinstance(candidate_dict, OpportunityCandidate):
                target_candidate = candidate_dict
            elif isinstance(candidate_dict, (dict, collections.abc.Mapping)):
                c_dict = dict(candidate_dict)
                validate_no_prohibited_fields(c_dict, path="candidate")
                target_candidate = c_dict
            else:
                raise ValidationError("Parameter 'candidate' must be a dictionary or OpportunityCandidate.")

        if target_candidate is None:
            raise ValidationError("Either 'candidate_id' or 'candidate' must be provided.")

        try:
            if enqueue:
                record = investigation_queue.add_candidate(
                    candidate=target_candidate,
                    metadata=metadata,
                )
            else:
                record = prioritization_engine.prioritize_candidate(
                    candidate=target_candidate,
                    metadata=metadata,
                )

            return {
                "status": "success",
                "priority_id": record.priority_id,
                "candidate_id": record.candidate_id,
                "investigation_priority": record.investigation_priority,
                "priority_state": record.priority_state.value,
                "evidence_priority_score": record.evidence_priority_score,
                "enqueued": enqueue,
                "priority_record": record.to_dict(),
                "read_only": True,
            }
        except ValidationError:
            raise
        except Exception as exc:
            logger.error("Error executing prioritize_opportunity: %s", exc, exc_info=True)
            raise ValidationError(f"Opportunity prioritization failed: {str(exc)}")

    def handle_prioritize_opportunity_group(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'prioritize_opportunity_group' command."""
        p = dict(
            getattr(req, "context", {})
            or getattr(req, "parameters", {})
            or getattr(req, "payload", {})
            or {}
        )

        validate_no_prohibited_fields(p, path="request_parameters")

        candidate_ids = p.get("candidate_ids", [])
        candidates_raw = p.get("candidates", [])
        enqueue = bool(p.get("enqueue", False))
        metadata = p.get("metadata")

        candidates_to_process: List[Any] = []

        if candidate_ids and isinstance(candidate_ids, (list, tuple)):
            for cid in candidate_ids:
                clean_id = str(cid).strip()
                if not SAFE_ID_PATTERN.match(clean_id):
                    raise ValidationError(f"Invalid candidate_id format: '{clean_id}'.")
                cand = None
                if opportunity_assessment_engine:
                    cand = opportunity_assessment_engine.get_assessment(clean_id)
                if cand is None:
                    raise ValidationError(f"Opportunity candidate '{clean_id}' not found.")
                candidates_to_process.append(cand)

        if candidates_raw and isinstance(candidates_raw, (list, tuple)):
            for c_raw in candidates_raw:
                if isinstance(c_raw, (dict, collections.abc.Mapping)):
                    c_dict = dict(c_raw)
                    validate_no_prohibited_fields(c_dict, path="candidates")
                    candidates_to_process.append(c_dict)
                else:
                    candidates_to_process.append(c_raw)

        if not candidates_to_process:
            raise ValidationError("Either 'candidate_ids' or 'candidates' list must be provided.")

        try:
            ranked_records = prioritization_engine.prioritize_candidates(
                candidates=candidates_to_process,
                metadata=metadata,
            )

            if enqueue:
                for cand in candidates_to_process:
                    investigation_queue.add_candidate(cand, metadata=metadata)

            return {
                "status": "success",
                "count": len(ranked_records),
                "priorities": [r.to_dict() for r in ranked_records],
                "enqueued": enqueue,
                "read_only": True,
            }
        except ValidationError:
            raise
        except Exception as exc:
            logger.error("Error executing prioritize_opportunity_group: %s", exc, exc_info=True)
            raise ValidationError(f"Opportunity group prioritization failed: {str(exc)}")

    def handle_get_opportunity_priority(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_opportunity_priority' command."""
        p = dict(
            getattr(req, "context", {})
            or getattr(req, "parameters", {})
            or getattr(req, "payload", {})
            or {}
        )

        pid = p.get("priority_id") or p.get("candidate_id")
        if not pid or not isinstance(pid, str) or not pid.strip():
            raise ValidationError("Missing required parameter 'priority_id' or 'candidate_id'.")

        clean_id = pid.strip()
        if not SAFE_ID_PATTERN.match(clean_id):
            raise ValidationError(f"Invalid identifier format: '{clean_id}'.")

        record = prioritization_engine.get_priority(clean_id)
        if record is None:
            record = investigation_queue.get(clean_id)

        if record is None:
            return {
                "status": "not_found",
                "identifier": clean_id,
                "message": f"Opportunity priority record '{clean_id}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "priority_record": record.to_dict(),
            "read_only": True,
        }

    def handle_list_opportunity_priorities(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_opportunity_priorities' command."""
        p = dict(
            getattr(req, "context", {})
            or getattr(req, "parameters", {})
            or getattr(req, "payload", {})
            or {}
        )

        try:
            limit = int(p.get("limit", 50))
            offset = int(p.get("offset", 0))
        except (ValueError, TypeError):
            limit = 50
            offset = 0

        state = p.get("state")
        records = prioritization_engine.list_priorities(limit=limit, offset=offset, state=state)

        return {
            "status": "success",
            "count": len(records),
            "limit": limit,
            "offset": offset,
            "priorities": [r.to_dict() for r in records],
            "read_only": True,
        }

    def handle_get_investigation_queue(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_queue' command."""
        p = dict(
            getattr(req, "context", {})
            or getattr(req, "parameters", {})
            or getattr(req, "payload", {})
            or {}
        )

        try:
            limit = int(p.get("limit", 50)) if "limit" in p else None
            offset = int(p.get("offset", 0))
        except (ValueError, TypeError):
            limit = None
            offset = 0

        items = investigation_queue.list_queue(limit=limit, offset=offset)

        return {
            "status": "success",
            "queue_size": investigation_queue.size(),
            "count": len(items),
            "offset": offset,
            "queue": [r.to_dict() for r in items],
            "read_only": True,
        }

    def handle_validate_opportunity_priority(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'validate_opportunity_priority' command."""
        p = dict(
            getattr(req, "context", {})
            or getattr(req, "parameters", {})
            or getattr(req, "payload", {})
            or {}
        )

        pid = p.get("priority_id") or p.get("candidate_id")
        if not pid or not isinstance(pid, str) or not pid.strip():
            raise ValidationError("Missing required parameter 'priority_id' or 'candidate_id'.")

        clean_id = pid.strip()
        if not SAFE_ID_PATTERN.match(clean_id):
            raise ValidationError(f"Invalid identifier format: '{clean_id}'.")

        record = prioritization_engine.get_priority(clean_id)
        if record is None:
            record = investigation_queue.get(clean_id)

        if record is None:
            return {
                "status": "not_found",
                "identifier": clean_id,
                "message": f"Opportunity priority record '{clean_id}' not found.",
                "is_valid": False,
                "read_only": True,
            }

        is_eligible = record.is_eligible
        invalidation_reasons = list(record.deprioritization_reasons)

        return {
            "status": "success",
            "priority_id": record.priority_id,
            "candidate_id": record.candidate_id,
            "is_eligible": is_eligible,
            "priority_state": record.priority_state.value,
            "investigation_priority": record.investigation_priority,
            "evidence_priority_score": record.evidence_priority_score,
            "deprioritization_reasons": invalidation_reasons,
            "read_only": True,
        }

    # Register handlers with MasterOrchestrator
    orchestrator.register_handler(
        command="prioritize_opportunity",
        handler=handle_prioritize_opportunity,
        capability="opportunity_prioritization",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="prioritize_opportunity_group",
        handler=handle_prioritize_opportunity_group,
        capability="opportunity_prioritization",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_opportunity_priority",
        handler=handle_get_opportunity_priority,
        capability="opportunity_prioritization",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="list_opportunity_priorities",
        handler=handle_list_opportunity_priorities,
        capability="opportunity_prioritization",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_queue",
        handler=handle_get_investigation_queue,
        capability="opportunity_prioritization",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="validate_opportunity_priority",
        handler=handle_validate_opportunity_priority,
        capability="opportunity_prioritization",
        allow_override=True,
    )

    logger.info("Registered opportunity prioritization handlers with MasterOrchestrator.")
