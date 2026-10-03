"""KAISER — Portfolio Reporting & Dashboard Data API Orchestrator Handlers.

Registers orchestrator command handlers under the 'portfolio_analytics' capability:
- `generate_unified_portfolio_report`: Generates a UnifiedPortfolioReport.
- `get_latest_portfolio_report`: Retrieves the newest generated unified report.
- `get_portfolio_report_by_id`: Retrieves a report by report_id or run_id.
- `list_portfolio_reports`: Retrieves bounded, paginated historical report summaries.
- `get_portfolio_dashboard_series`: Retrieves bounded timeline series for charting.
- `compare_portfolio_runs`: Compares multiple simulation runs.
- `get_portfolio_voice_summary`: Retrieves concise TTS summary fields.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.backtesting.models import BacktestResult
from app.backtesting.portfolio_simulation.models import PortfolioSimulationResult
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.reporting import PortfolioReportingService

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator


def register_reporting_handlers(
    orchestrator: MasterOrchestrator,
    service: PortfolioReportingService,
) -> None:
    """Register portfolio analytics reporting command handlers with the Master Orchestrator.

    Args:
        orchestrator: MasterOrchestrator instance.
        service: PortfolioReportingService instance.
    """

    def handle_generate_unified_portfolio_report(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'generate_unified_portfolio_report' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        title = p.get("title")
        prov_overrides = p.get("provenance_overrides")
        wf_summary = p.get("walk_forward_summary")
        rob_summary = p.get("robustness_summary")

        # 1. Directly provided simulation_result
        raw_sim = p.get("simulation_result")
        if isinstance(raw_sim, PortfolioSimulationResult):
            report = service.generate_unified_report(
                source_result=raw_sim,
                title=title,
                provenance_overrides=prov_overrides,
                walk_forward_summary=wf_summary,
                robustness_summary=rob_summary,
            )
            return {
                "status": "success",
                "report": report.to_dict(),
                "summary": report.summary_text,
                "voice_summary": report.voice_summary.to_dict() if report.voice_summary else None,
            }

        # 2. Directly provided backtest_result
        raw_bt = p.get("backtest_result")
        if isinstance(raw_bt, BacktestResult):
            report = service.generate_unified_report(
                source_result=raw_bt,
                title=title,
                provenance_overrides=prov_overrides,
                walk_forward_summary=wf_summary,
                robustness_summary=rob_summary,
            )
            return {
                "status": "success",
                "report": report.to_dict(),
                "summary": report.summary_text,
                "voice_summary": report.voice_summary.to_dict() if report.voice_summary else None,
            }

        # 3. Source dict directly provided
        if "source_dict" in p and isinstance(p["source_dict"], dict):
            report = service.generate_unified_report(
                source_result=p["source_dict"],
                title=title,
                provenance_overrides=prov_overrides,
                walk_forward_summary=wf_summary,
                robustness_summary=rob_summary,
            )
            return {
                "status": "success",
                "report": report.to_dict(),
                "summary": report.summary_text,
                "voice_summary": report.voice_summary.to_dict() if report.voice_summary else None,
            }

        # 4. Check for simulation_id in StateStore
        sim_id = p.get("simulation_id")
        if sim_id and service.state_store:
            sim_data = service.state_store.get(f"portfolio_simulation:{sim_id}")
            if sim_data and isinstance(sim_data, PortfolioSimulationResult):
                report = service.generate_unified_report(
                    source_result=sim_data,
                    title=title,
                    provenance_overrides=prov_overrides,
                    walk_forward_summary=wf_summary,
                    robustness_summary=rob_summary,
                )
                return {
                    "status": "success",
                    "report": report.to_dict(),
                    "summary": report.summary_text,
                    "voice_summary": report.voice_summary.to_dict() if report.voice_summary else None,
                }

        raise ValidationError(
            "generate_unified_portfolio_report requires a valid 'simulation_result', 'backtest_result', "
            "'source_dict', or stored 'simulation_id'."
        )

    def handle_get_latest_portfolio_report(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_latest_portfolio_report' command."""
        report = service.get_latest_unified_report()
        if report is None:
            return {
                "status": "not_found",
                "message": "No portfolio reports generated yet.",
            }
        return {
            "status": "success",
            "report": report.to_dict(),
            "summary": report.summary_text,
        }

    def handle_get_portfolio_report_by_id(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_portfolio_report_by_id' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        report_id = p.get("report_id")
        run_id = p.get("run_id")

        if report_id:
            report = service.get_report_by_id(str(report_id))
        elif run_id:
            report = service.get_report_by_run_id(str(run_id))
        else:
            raise ValidationError("get_portfolio_report_by_id requires 'report_id' or 'run_id'.")

        if report is None:
            target = report_id or run_id
            return {
                "status": "not_found",
                "message": f"Portfolio report for '{target}' not found.",
            }

        return {
            "status": "success",
            "report": report.to_dict(),
            "summary": report.summary_text,
        }

    def handle_list_portfolio_reports(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_portfolio_reports' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        limit = int(p.get("limit", 20))
        offset = int(p.get("offset", 0))
        sort_desc = bool(p.get("sort_desc", True))

        summaries = service.list_historical_reports(limit=limit, offset=offset, sort_desc=sort_desc)
        return {
            "status": "success",
            "count": len(summaries),
            "offset": offset,
            "limit": limit,
            "reports": [s.to_dict() for s in summaries],
        }

    def handle_get_portfolio_dashboard_series(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_portfolio_dashboard_series' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        report_id = p.get("report_id")
        if not report_id:
            # Fall back to latest report
            latest = service.get_latest_unified_report()
            if latest:
                report_id = latest.report_id
            else:
                raise ValidationError("get_portfolio_dashboard_series requires 'report_id' or an existing report.")

        limit = int(p["limit"]) if "limit" in p and p["limit"] is not None else None
        offset = int(p.get("offset", 0))

        series = service.get_equity_and_drawdown_series(str(report_id), limit=limit, offset=offset)
        if series is None:
            return {
                "status": "not_found",
                "message": f"Report '{report_id}' not found.",
            }

        return {
            "status": "success",
            "series": series.to_dict(),
        }

    def handle_compare_portfolio_runs(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'compare_portfolio_runs' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        run_ids = p.get("run_ids")
        if not run_ids or not isinstance(run_ids, (list, tuple)):
            raise ValidationError("compare_portfolio_runs requires a list of 'run_ids'.")

        report = service.compare_simulation_runs([str(r) for r in run_ids])
        return {
            "status": "success",
            "comparison": report.to_dict(),
            "summary": report.summary_text,
        }

    def handle_get_portfolio_voice_summary(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_portfolio_voice_summary' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        report_id = p.get("report_id")

        if report_id:
            rep = service.get_report_by_id(str(report_id))
        else:
            rep = service.get_latest_unified_report()

        if rep is None:
            return {
                "status": "not_found",
                "message": "No portfolio report available to voice.",
            }

        return {
            "status": "success",
            "report_id": rep.report_id,
            "voice_summary": rep.voice_summary.to_dict() if rep.voice_summary else None,
        }

    # Register handlers with MasterOrchestrator
    orchestrator.register_handler(
        command="generate_unified_portfolio_report",
        handler=handle_generate_unified_portfolio_report,
        capability="portfolio_analytics",
        description="Generate a unified portfolio report consolidating performance, risk, and attribution",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="get_latest_portfolio_report",
        handler=handle_get_latest_portfolio_report,
        capability="portfolio_analytics",
        description="Retrieve the latest unified portfolio analytics report",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="get_portfolio_report_by_id",
        handler=handle_get_portfolio_report_by_id,
        capability="portfolio_analytics",
        description="Retrieve a portfolio report by report_id or source run_id",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="list_portfolio_reports",
        handler=handle_list_portfolio_reports,
        capability="portfolio_analytics",
        description="List bounded historical portfolio report summaries",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="get_portfolio_dashboard_series",
        handler=handle_get_portfolio_dashboard_series,
        capability="portfolio_analytics",
        description="Retrieve bounded timeline series of equity and drawdown for charting",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="compare_portfolio_runs",
        handler=handle_compare_portfolio_runs,
        capability="portfolio_analytics",
        description="Compare multiple portfolio simulation runs side-by-side",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="get_portfolio_voice_summary",
        handler=handle_get_portfolio_voice_summary,
        capability="portfolio_analytics",
        description="Retrieve concise structured speech summary fields for voice synthesis",
        allow_override=True,
    )
