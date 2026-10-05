"""KAISER — Run Comparison & Experiment Tracking Orchestrator Handlers.

Provides MasterOrchestrator command handlers for:
- `compare_analysis_runs`: Compare two completed analytical runs.
- `compare_analysis_run_group`: Compare a bounded group of 2 to 20 runs.
- `get_analysis_comparison`: Retrieve a cached or stored comparison by ID.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.comparison.engine import RunComparisonEngine
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.comparison.handlers")


def register_comparison_handlers(
    orchestrator: MasterOrchestrator,
    comparison_engine: RunComparisonEngine,
) -> None:
    """Register run comparison commands with MasterOrchestrator."""

    def handle_compare_analysis_runs(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'compare_analysis_runs' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        left_id = p.get("left_run_id") or p.get("run_id_a") or p.get("baseline_run_id")
        right_id = p.get("right_run_id") or p.get("run_id_b") or p.get("candidate_run_id")

        if not left_id or not isinstance(left_id, str) or not left_id.strip():
            raise ValidationError("Missing or invalid required parameter 'left_run_id'.")
        if not right_id or not isinstance(right_id, str) or not right_id.strip():
            raise ValidationError("Missing or invalid required parameter 'right_run_id'.")

        try:
            comparison = comparison_engine.compare_runs(
                left_run_or_id=left_id.strip(),
                right_run_or_id=right_id.strip(),
                bypass_cache=bool(p.get("bypass_cache", False)),
            )
            return {
                "status": "success",
                "comparison_id": comparison.comparison_id,
                "comparison": comparison.to_dict(),
            }
        except ValidationError as val_err:
            logger.warning("Validation error in compare_analysis_runs: %s", val_err)
            raise
        except Exception as exc:
            logger.error("Error executing compare_analysis_runs: %s", exc, exc_info=True)
            raise ValidationError(f"Comparison execution failed: {str(exc)}")

    def handle_compare_analysis_run_group(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'compare_analysis_run_group' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        run_ids = p.get("run_ids")
        if not run_ids or not isinstance(run_ids, (list, tuple)):
            raise ValidationError("Parameter 'run_ids' must be a list of at least 2 run IDs.")

        cleaned_ids = [str(r).strip() for r in run_ids if str(r).strip()]
        if len(cleaned_ids) < 2:
            raise ValidationError("At least 2 distinct run_ids are required for group comparison.")

        # Check for duplicates
        if len(cleaned_ids) != len(set(cleaned_ids)):
            raise ValidationError("Duplicate run_ids detected in comparison group request.")

        if len(cleaned_ids) > comparison_engine.max_group_runs:
            raise ValidationError(
                f"Requested {len(cleaned_ids)} runs exceeds maximum allowable group bound of "
                f"{comparison_engine.max_group_runs} runs."
            )

        baseline_id = p.get("baseline_run_id")
        if baseline_id and str(baseline_id).strip():
            baseline_id = str(baseline_id).strip()
        else:
            baseline_id = None

        try:
            multi_comp = comparison_engine.compare_run_group(
                runs_or_ids=cleaned_ids,
                baseline_run_id=baseline_id,
            )
            return {
                "status": "success",
                "comparison_id": multi_comp.comparison_id,
                "multi_run_comparison": multi_comp.to_dict(),
            }
        except ValidationError:
            raise
        except Exception as exc:
            logger.error("Error executing compare_analysis_run_group: %s", exc, exc_info=True)
            raise ValidationError(f"Group comparison failed: {str(exc)}")

    def handle_get_analysis_comparison(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_analysis_comparison' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        comp_id = p.get("comparison_id")
        if not comp_id or not isinstance(comp_id, str) or not comp_id.strip():
            raise ValidationError("Missing required parameter 'comparison_id'.")

        res = comparison_engine.get_comparison(comp_id.strip())
        if res is None:
            return {
                "status": "not_found",
                "message": f"Comparison '{comp_id.strip()}' not found.",
            }

        return {
            "status": "success",
            "comparison": res.to_dict() if hasattr(res, "to_dict") else res,
        }

    orchestrator.register_handler(
        command="compare_analysis_runs",
        handler=handle_compare_analysis_runs,
        description="Compare two analytical execution runs deterministically",
        capability="analysis_comparison",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="compare_analysis_run_group",
        handler=handle_compare_analysis_run_group,
        description="Compare a bounded group of 2 to 20 analytical execution runs",
        capability="analysis_comparison",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_analysis_comparison",
        handler=handle_get_analysis_comparison,
        description="Retrieve a cached or stored analysis comparison by comparison_id",
        capability="analysis_comparison",
        allow_override=True,
    )
    logger.info("Registered analysis comparison orchestrator handlers.")
