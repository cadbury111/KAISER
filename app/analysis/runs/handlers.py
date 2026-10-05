"""KAISER — Reproducible Analysis Run & Experiment Tracking Handlers.

Provides read-only and registration command handlers for MasterOrchestrator:
- `create_analysis_run`: Register a new analytical execution in CREATED state.
- `get_analysis_run`: Look up an existing analytical execution by run_id.
- `list_analysis_runs`: Query bounded runs with filtering by operation, status, symbol, strategy, and time.
- `get_reproducibility_record`: Query all historical executions matching a reproducibility fingerprint.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.models import normalize_timeframe
from app.analysis.runs.models import OperationType, RunStatus
from app.analysis.runs.registry import AnalysisRunRegistry
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.runs.handlers")


def register_run_handlers(
    orchestrator: MasterOrchestrator,
    run_registry: AnalysisRunRegistry,
) -> None:
    """Register analysis run tracking commands with MasterOrchestrator."""

    def handle_create_analysis_run(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'create_analysis_run' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        op_str = p.get("operation_type")
        if not op_str:
            raise ValidationError("Missing required parameter 'operation_type'.")

        cfg = p.get("configuration")
        if cfg is None:
            raise ValidationError("Missing required parameter 'configuration'.")

        try:
            run = run_registry.create_run(
                operation_type=op_str,
                configuration=cfg,
                run_id=p.get("run_id"),
                input_provenance_ids=p.get("input_provenance_ids", ()),
                parent_run_ids=p.get("parent_run_ids", ()),
                strategy_id=p.get("strategy_id"),
                strategy_version=p.get("strategy_version"),
                symbol_universe=p.get("symbol_universe", ()),
                timeframe_universe=p.get("timeframe_universe", ()),
                data_start=p.get("data_start"),
                data_end=p.get("data_end"),
                input_record_count=int(p.get("input_record_count", 0)),
                quality_assessment_ids=p.get("quality_assessment_ids", ()),
                analysis_version=p.get("analysis_version", "1.0.0"),
                schema_version=p.get("schema_version", "1.0.0"),
                software_version=p.get("software_version", "1.0.0"),
                execution_metadata=p.get("execution_metadata"),
            )
            return {
                "status": "success",
                "run": run.to_dict(),
                "read_only": False,
            }
        except ValidationError as val_err:
            return {
                "status": "validation_error",
                "error": str(val_err),
                "read_only": False,
            }
        except Exception as exc:
            logger.error("Failed to create analysis run: %s", exc)
            return {
                "status": "error",
                "error": f"Failed to create analysis run: {exc}",
                "read_only": False,
            }

    def handle_get_analysis_run(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_analysis_run' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        run_id = p.get("run_id")
        if not run_id or not isinstance(run_id, str) or not run_id.strip():
            raise ValidationError("Missing required parameter 'run_id'.")

        run = run_registry.get_run(run_id.strip())
        if not run:
            return {
                "status": "not_found",
                "run_id": run_id.strip(),
                "message": f"Analysis run with ID '{run_id.strip()}' was not found in registry.",
                "read_only": True,
            }

        return {
            "status": "success",
            "run": run.to_dict(),
            "read_only": True,
        }

    def handle_list_analysis_runs(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_analysis_runs' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        try:
            limit = int(p.get("limit", 50))
            offset = int(p.get("offset", 0))
        except (ValueError, TypeError):
            limit = 50
            offset = 0

        runs = run_registry.query_runs(
            operation_type=p.get("operation_type"),
            status=p.get("status"),
            symbol=p.get("symbol"),
            strategy_id=p.get("strategy_id"),
            parent_run_id=p.get("parent_run_id"),
            start_time=p.get("start_time"),
            end_time=p.get("end_time"),
            limit=limit,
            offset=offset,
        )

        return {
            "status": "success",
            "count": len(runs),
            "limit": limit,
            "offset": offset,
            "runs": [r.to_dict() for r in runs],
            "read_only": True,
        }

    def handle_get_reproducibility_record(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_reproducibility_record' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        repro_id = p.get("reproducibility_id")
        if not repro_id or not isinstance(repro_id, str) or not repro_id.strip():
            raise ValidationError("Missing required parameter 'reproducibility_id'.")

        try:
            limit = int(p.get("limit", 50))
            offset = int(p.get("offset", 0))
        except (ValueError, TypeError):
            limit = 50
            offset = 0

        runs = run_registry.get_runs_by_reproducibility(
            reproducibility_id=repro_id.strip(),
            limit=limit,
            offset=offset,
        )

        return {
            "status": "success",
            "reproducibility_id": repro_id.strip(),
            "matched_runs_count": len(runs),
            "runs": [r.to_dict() for r in runs],
            "read_only": True,
        }

    orchestrator.register_handler(
        command="create_analysis_run",
        handler=handle_create_analysis_run,
        description="Register a new analytical execution in CREATED state",
        capability="analysis_runs",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_analysis_run",
        handler=handle_get_analysis_run,
        description="Look up an analytical execution run by run_id",
        capability="analysis_runs",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="list_analysis_runs",
        handler=handle_list_analysis_runs,
        description="List and filter analytical runs with pagination",
        capability="analysis_runs",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_reproducibility_record",
        handler=handle_get_reproducibility_record,
        description="Query all historical executions matching a reproducibility identity",
        capability="analysis_runs",
        allow_override=True,
    )
