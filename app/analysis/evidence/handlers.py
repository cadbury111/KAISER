"""KAISER — Evidence Synthesis Orchestrator Command Handlers.

Provides MasterOrchestrator command handlers for:
- `generate_evidence_synthesis`: Compile structured multi-source evidence reports.
- `get_evidence_synthesis`: Retrieve historical synthesis report by ID.
- `list_evidence_syntheses`: Query historical synthesis reports with pagination.

SAFETY INVARIANTS:
1. Inputs must reference existing structured KAISER objects (runs, comparisons).
2. Never accept arbitrary prose as trusted evidence.
3. Strictly read-only analysis; execution engine remains locked.
4. Redacts secrets and sanitizes errors before responding.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.evidence.engine import EvidenceSynthesisEngine
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.evidence.handlers")

SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")


def register_evidence_handlers(
    orchestrator: MasterOrchestrator,
    evidence_engine: EvidenceSynthesisEngine,
) -> None:
    """Register evidence synthesis commands with MasterOrchestrator."""

    def handle_generate_evidence_synthesis(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'generate_evidence_synthesis' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        subject = p.get("subject")
        if not subject or not isinstance(subject, str) or not subject.strip():
            raise ValidationError("Missing required parameter 'subject'.")

        raw_runs = p.get("run_ids") or p.get("runs") or []
        if isinstance(raw_runs, str):
            raw_runs = [raw_runs]
        elif not isinstance(raw_runs, (list, tuple)):
            raise ValidationError("Parameter 'run_ids' must be a list or tuple of run IDs.")

        cleaned_runs = [str(r).strip() for r in raw_runs if str(r).strip()]
        for rid in cleaned_runs:
            if not SAFE_ID_PATTERN.match(rid):
                raise ValidationError(f"Invalid run_id format: '{rid}'.")

        comp_id = p.get("comparison_id")
        if comp_id:
            comp_id = str(comp_id).strip()
            if not SAFE_ID_PATTERN.match(comp_id):
                raise ValidationError(f"Invalid comparison_id format: '{comp_id}'.")

        if not cleaned_runs and not comp_id:
            raise ValidationError("Evidence synthesis requires at least one 'run_id' or a 'comparison_id'.")

        scope = p.get("scope")
        if scope:
            scope = str(scope).strip()

        limitations = p.get("limitations")
        if limitations and not isinstance(limitations, (list, tuple)):
            limitations = None

        try:
            report = evidence_engine.synthesize(
                subject=subject.strip(),
                runs=cleaned_runs,
                comparison=comp_id,
                scope=scope,
                additional_limitations=limitations,
            )
            return {
                "status": "success",
                "report_id": report.report_id,
                "overall_status": report.overall_evidence_status.value,
                "evidence_count": len(report.evidence_items),
                "report": report.to_dict(),
                "read_only": True,
            }
        except ValidationError:
            raise
        except Exception as exc:
            logger.error("Error executing generate_evidence_synthesis: %s", exc, exc_info=True)
            raise ValidationError(f"Evidence synthesis failed: {str(exc)}")

    def handle_get_evidence_synthesis(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_evidence_synthesis' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        report_id = p.get("report_id")
        if not report_id or not isinstance(report_id, str) or not report_id.strip():
            raise ValidationError("Missing required parameter 'report_id'.")

        r_id = report_id.strip()
        if not SAFE_ID_PATTERN.match(r_id):
            raise ValidationError(f"Invalid report_id format: '{r_id}'.")

        rep = evidence_engine.get_synthesis(r_id)
        if rep is None:
            return {
                "status": "not_found",
                "report_id": r_id,
                "message": f"Evidence synthesis report '{r_id}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "report_id": r_id,
            "report": rep.to_dict(),
            "read_only": True,
        }

    def handle_list_evidence_syntheses(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_evidence_syntheses' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        try:
            limit = int(p.get("limit", 50))
            offset = int(p.get("offset", 0))
        except (ValueError, TypeError):
            limit = 50
            offset = 0

        subject_filter = p.get("subject")
        if subject_filter:
            subject_filter = str(subject_filter).strip()

        reports = evidence_engine.list_syntheses(
            limit=limit,
            offset=offset,
            subject=subject_filter,
        )

        return {
            "status": "success",
            "count": len(reports),
            "limit": max(1, min(100, limit)),
            "offset": max(0, offset),
            "reports": [r.to_dict() for r in reports],
            "read_only": True,
        }

    orchestrator.register_handler(
        command="generate_evidence_synthesis",
        handler=handle_generate_evidence_synthesis,
        description="Compile structured multi-source decision-support evidence reports",
        capability="evidence_synthesis",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_evidence_synthesis",
        handler=handle_get_evidence_synthesis,
        description="Retrieve historical evidence synthesis report by ID",
        capability="evidence_synthesis",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="list_evidence_syntheses",
        handler=handle_list_evidence_syntheses,
        description="Query historical evidence synthesis reports with pagination",
        capability="evidence_synthesis",
        allow_override=True,
    )

    logger.info("Registered evidence synthesis handlers with MasterOrchestrator.")
