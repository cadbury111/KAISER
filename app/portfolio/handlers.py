"""KAISER — Portfolio Attribution & Risk-Adjusted Analytics Orchestrator Handlers.

Registers the following commands with MasterOrchestrator:
- `calculate_portfolio_attribution`: Generates detailed performance attribution and risk-adjusted metrics.
- `get_portfolio_attribution`: Retrieves stored attribution reports by ID or latest.
- `compare_performance_runs`: Compares simulation runs, walk-forward windows, or robustness stress scenarios.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.backtesting.models import BacktestResult
from app.backtesting.portfolio_simulation.models import PortfolioSimulationResult
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.models import AttributionConfig

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator


def register_portfolio_handlers(
    orchestrator: MasterOrchestrator,
    service: PortfolioAttributionService,
) -> None:
    """Register portfolio performance attribution command handlers with the Master Orchestrator.

    Args:
        orchestrator: MasterOrchestrator instance.
        service: PortfolioAttributionService instance.
    """

    def handle_calculate_portfolio_attribution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'calculate_portfolio_attribution' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        ann_factor = float(p.get("annualization_factor", 252.0))
        rf_rate = float(p.get("risk_free_rate", 0.0))
        tolerance = float(p.get("reconciliation_tolerance", 0.05))
        config = AttributionConfig(
            annualization_factor=ann_factor,
            risk_free_rate=rf_rate,
            reconciliation_tolerance=tolerance,
        )

        # 1. Check for directly supplied simulation_result
        raw_sim = p.get("simulation_result")
        if isinstance(raw_sim, PortfolioSimulationResult):
            report = service.analyze_simulation_result(raw_sim, config=config, correlation_id=req.correlation_id)
            return {
                "status": "success",
                "report": report.to_dict(),
                "summary": report.summary_text,
                "disclaimer": report.disclaimer,
            }

        # 2. Check for simulation_id in StateStore
        sim_id = p.get("simulation_id")
        if sim_id and service.state_store:
            sim_data = service.state_store.get(f"portfolio_simulation:{sim_id}")
            if sim_data:
                # If stored as object or reconstructed
                if isinstance(sim_data, PortfolioSimulationResult):
                    report = service.analyze_simulation_result(sim_data, config=config, correlation_id=req.correlation_id)
                    return {
                        "status": "success",
                        "report": report.to_dict(),
                        "summary": report.summary_text,
                        "disclaimer": report.disclaimer,
                    }

        # 3. Check for directly supplied backtest_result
        raw_bt = p.get("backtest_result")
        if isinstance(raw_bt, BacktestResult):
            report = service.analyze_backtest_result(raw_bt, config=config, correlation_id=req.correlation_id)
            return {
                "status": "success",
                "report": report.to_dict(),
                "summary": report.summary_text,
                "disclaimer": report.disclaimer,
            }

        # 4. Check for backtest_id in StateStore
        bt_id = p.get("backtest_id")
        if bt_id and service.state_store:
            bt_data = service.state_store.get(f"backtest:{bt_id}")
            if bt_data and isinstance(bt_data, BacktestResult):
                report = service.analyze_backtest_result(bt_data, config=config, correlation_id=req.correlation_id)
                return {
                    "status": "success",
                    "report": report.to_dict(),
                    "summary": report.summary_text,
                    "disclaimer": report.disclaimer,
                }

        raise ValidationError(
            "calculate_portfolio_attribution requires a valid 'simulation_result', 'simulation_id', "
            "'backtest_result', or 'backtest_id'."
        )

    def handle_get_portfolio_attribution(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_portfolio_attribution' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        report_id = p.get("report_id")

        if report_id:
            report = service.get_report(str(report_id))
        else:
            report = service.get_latest_report()

        if not report:
            return {
                "status": "not_found",
                "message": f"No portfolio attribution report found{' for id ' + str(report_id) if report_id else ''}.",
            }

        return {
            "status": "success",
            "report": report.to_dict(),
            "summary": report.summary_text,
            "disclaimer": report.disclaimer,
        }

    def handle_compare_performance_runs(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'compare_performance_runs' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        comp_type = str(p.get("comparison_type", "simulation_runs")).lower()

        if comp_type in ("simulation_runs", "simulations"):
            sim_results = p.get("simulation_results")
            if not sim_results and p.get("simulation_ids") and service.state_store:
                sim_ids = p["simulation_ids"]
                if isinstance(sim_ids, (list, tuple)):
                    sim_results = [
                        service.state_store.get(f"portfolio_simulation:{sid}")
                        for sid in sim_ids
                    ]
                    sim_results = [r for r in sim_results if isinstance(r, PortfolioSimulationResult)]

            if not sim_results or not isinstance(sim_results, (list, tuple)):
                raise ValidationError("compare_performance_runs requires a non-empty sequence of simulation_results.")

            report = service.compare_simulation_runs(sim_results)
            return {
                "status": "success",
                "comparison": report.to_dict(),
                "summary": report.summary_text,
                "disclaimer": report.disclaimer,
            }

        elif comp_type in ("walk_forward", "wf"):
            wf_result = p.get("walk_forward_result")
            if not wf_result and p.get("validation_id") and service.state_store:
                wf_result = service.state_store.get(f"walk_forward:{p['validation_id']}")

            if not wf_result:
                raise ValidationError("compare_performance_runs requires a valid walk_forward_result or validation_id.")

            report = service.compare_walk_forward_windows(wf_result)
            return {
                "status": "success",
                "comparison": report.to_dict(),
                "summary": report.summary_text,
                "disclaimer": report.disclaimer,
            }

        elif comp_type in ("robustness", "stress"):
            rob_result = p.get("robustness_result")
            test_id = p.get("test_id") or p.get("session_id")
            if not rob_result and test_id and service.state_store:
                rob_result = service.state_store.get(f"robustness:{test_id}")

            if not rob_result:
                raise ValidationError("compare_performance_runs requires a valid robustness_result, test_id, or session_id.")

            report = service.compare_robustness_runs(rob_result)
            return {
                "status": "success",
                "comparison": report.to_dict(),
                "summary": report.summary_text,
                "disclaimer": report.disclaimer,
            }

        raise ValidationError(
            f"Unsupported comparison_type '{comp_type}'. Supported: 'simulation_runs', 'walk_forward', 'robustness'."
        )

    orchestrator.register_handler(
        command="calculate_portfolio_attribution",
        handler=handle_calculate_portfolio_attribution,
        capability="portfolio_analytics",
        description="Calculate multi-level performance attribution and risk-adjusted metrics",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="get_portfolio_attribution",
        handler=handle_get_portfolio_attribution,
        capability="portfolio_analytics",
        description="Retrieve stored portfolio performance attribution reports",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="compare_performance_runs",
        handler=handle_compare_performance_runs,
        capability="portfolio_analytics",
        description="Compare multiple simulation runs, walk-forward windows, or robustness scenarios",
        allow_override=True,
    )
