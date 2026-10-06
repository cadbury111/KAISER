"""KAISER — Opportunity Assessment Orchestrator Command Handlers.

Provides MasterOrchestrator command handlers for:
- `generate_opportunity_assessment`: Transform an evidence synthesis report into an OpportunityCandidate.
- `get_opportunity_assessment`: Retrieve historical opportunity candidate by ID.
- `list_opportunity_assessments`: Query historical opportunity candidates with pagination.
- `validate_opportunity_assessment`: Check validity and invalidation conditions of an opportunity candidate.

SAFETY INVARIANTS:
1. Inputs must reference existing structured evidence reports.
2. Rejects any prohibited execution, trading, or recommendation parameters.
3. Strictly read-only analysis; execution engine remains locked.
4. Redacts secrets and sanitizes errors before responding.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.opportunities.engine import OpportunityAssessmentEngine
from app.analysis.opportunities.models import (
    PROHIBITED_RECOMMENDATION_FIELDS,
    validate_no_prohibited_fields,
)
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.handlers")

SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")


def register_opportunity_handlers(
    orchestrator: MasterOrchestrator,
    opportunity_engine: OpportunityAssessmentEngine,
) -> None:
    """Register opportunity assessment commands with MasterOrchestrator."""

    def handle_generate_opportunity_assessment(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'generate_opportunity_assessment' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        # SAFETY GUARD: Check for prohibited fields in input
        validate_no_prohibited_fields(p, path="request_parameters")

        report_id = p.get("report_id")
        report_dict = p.get("report")

        if not report_id and not report_dict:
            raise ValidationError("Either 'report_id' or 'report' must be provided for opportunity assessment.")

        target_report: Any = None
        if report_id:
            rid = str(report_id).strip()
            if not SAFE_ID_PATTERN.match(rid):
                raise ValidationError(f"Invalid report_id format: '{rid}'.")
            if opportunity_engine.evidence_engine:
                target_report = opportunity_engine.evidence_engine.get_synthesis(rid)
            if target_report is None and not report_dict:
                raise ValidationError(f"Evidence synthesis report '{rid}' not found.")
        
        if target_report is None and report_dict:
            if not isinstance(report_dict, dict):
                raise ValidationError("Parameter 'report' must be a dictionary.")
            validate_no_prohibited_fields(report_dict, path="report")
            target_report = report_dict

        symbol = p.get("symbol")
        if symbol:
            symbol = str(symbol).strip()

        timeframe = p.get("timeframe")
        if timeframe:
            timeframe = str(timeframe).strip()

        metadata = p.get("metadata")
        if metadata and not isinstance(metadata, dict):
            metadata = None

        try:
            candidate = opportunity_engine.assess(
                report=target_report,
                symbol=symbol,
                timeframe=timeframe,
                metadata=metadata,
            )
            return {
                "status": "success",
                "candidate_id": candidate.candidate_id,
                "assessment_state": candidate.assessment_state.value,
                "evidence_completeness_score": candidate.evidence_completeness_score,
                "confidence_state": candidate.confidence_state.value,
                "assessment": candidate.to_dict(),
                "read_only": True,
            }
        except ValidationError:
            raise
        except Exception as exc:
            logger.error("Error executing generate_opportunity_assessment: %s", exc, exc_info=True)
            raise ValidationError(f"Opportunity assessment generation failed: {str(exc)}")

    def handle_get_opportunity_assessment(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_opportunity_assessment' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        cid = p.get("candidate_id") or p.get("assessment_id")
        if not cid or not isinstance(cid, str) or not cid.strip():
            raise ValidationError("Missing required parameter 'candidate_id'.")

        clean_id = cid.strip()
        if not SAFE_ID_PATTERN.match(clean_id):
            raise ValidationError(f"Invalid candidate_id format: '{clean_id}'.")

        cand = opportunity_engine.get_assessment(clean_id)
        if cand is None:
            return {
                "status": "not_found",
                "candidate_id": clean_id,
                "message": f"Opportunity assessment candidate '{clean_id}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "candidate_id": clean_id,
            "assessment": cand.to_dict(),
            "read_only": True,
        }

    def handle_list_opportunity_assessments(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_opportunity_assessments' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        try:
            limit = int(p.get("limit", 50))
            offset = int(p.get("offset", 0))
        except (ValueError, TypeError):
            limit = 50
            offset = 0

        assessments = opportunity_engine.list_assessments(limit=limit, offset=offset)

        return {
            "status": "success",
            "count": len(assessments),
            "limit": max(1, min(100, limit)),
            "offset": max(0, offset),
            "assessments": [a.to_dict() for a in assessments],
            "read_only": True,
        }

    def handle_validate_opportunity_assessment(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'validate_opportunity_assessment' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        # Check for prohibited fields
        validate_no_prohibited_fields(p, path="request_parameters")

        cid = p.get("candidate_id") or p.get("assessment_id")
        cand = None
        if cid and isinstance(cid, str):
            cand = opportunity_engine.get_assessment(cid.strip())

        if cand is None:
            raw_cand = p.get("assessment") or p.get("candidate")
            if isinstance(raw_cand, dict):
                validate_no_prohibited_fields(raw_cand, path="candidate")
                cand = opportunity_engine._dict_to_candidate(raw_cand)

        if cand is None:
            raise ValidationError("Target candidate could not be found or parsed for validation.")

        is_valid = not cand.is_invalidated
        findings: List[str] = []

        if cand.is_invalidated:
            findings.append("Candidate is in INVALIDATED state.")
        if cand.has_conflicts:
            findings.append("Unresolved evidence conflicts are present.")
        if cand.evidence_completeness_score < 0.50:
            findings.append("Evidence completeness score is below 0.50 threshold.")

        return {
            "status": "success",
            "candidate_id": cand.candidate_id,
            "is_valid": is_valid,
            "assessment_state": cand.assessment_state.value,
            "findings": findings,
            "read_only": True,
        }

    orchestrator.register_handler(
        command="generate_opportunity_assessment",
        handler=handle_generate_opportunity_assessment,
        description="Transform evidence synthesis report into an OpportunityCandidate",
        capability="opportunity_assessment",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_opportunity_assessment",
        handler=handle_get_opportunity_assessment,
        description="Retrieve historical opportunity candidate by ID",
        capability="opportunity_assessment",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="list_opportunity_assessments",
        handler=handle_list_opportunity_assessments,
        description="Query historical opportunity candidates with pagination",
        capability="opportunity_assessment",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="validate_opportunity_assessment",
        handler=handle_validate_opportunity_assessment,
        description="Verify validity and invalidation status of an opportunity candidate",
        capability="opportunity_assessment",
        allow_override=True,
    )

    logger.info("Registered opportunity assessment handlers with MasterOrchestrator.")
