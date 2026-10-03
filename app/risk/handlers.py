"""KAISER — Portfolio Risk Monitoring Orchestrator Command Handlers.

Registers `evaluate_portfolio_risk` and `get_portfolio_risk_status` commands with
the MasterOrchestrator, providing voice and CLI access to portfolio risk monitoring.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Dict, Optional

from app.backtesting.portfolio_simulation.models import PortfolioRiskSnapshot
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest
from app.risk.monitor import PortfolioRiskMonitor

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator


def register_risk_handlers(
    orchestrator: MasterOrchestrator,
    monitor: PortfolioRiskMonitor,
) -> None:
    """Register portfolio risk monitoring commands with the Master Orchestrator.

    Commands registered:
    - `evaluate_portfolio_risk`: Evaluate a portfolio snapshot or simulation against risk thresholds.
    - `get_portfolio_risk_status`: Retrieve currently active alerts and overall portfolio risk health.
    """

    def handle_evaluate_portfolio_risk(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'evaluate_portfolio_risk' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        snapshot: Optional[PortfolioRiskSnapshot] = None
        source_ref: str = str(p.get("source_ref", "orchestrator_request"))

        # 1. Direct snapshot dictionary passed in context
        raw_snapshot = p.get("snapshot")
        if raw_snapshot and isinstance(raw_snapshot, Mapping):
            snapshot = PortfolioRiskSnapshot(
                timestamp=float(raw_snapshot.get("timestamp", 0.0)),
                total_equity=float(raw_snapshot.get("total_equity", 100_000.0)),
                cash=float(raw_snapshot.get("cash", 100_000.0)),
                cash_reserve_pct=float(raw_snapshot.get("cash_reserve_pct", 100.0)),
                gross_exposure=float(raw_snapshot.get("gross_exposure", 0.0)),
                gross_exposure_pct=float(raw_snapshot.get("gross_exposure_pct", 0.0)),
                net_exposure=float(raw_snapshot.get("net_exposure", 0.0)),
                net_exposure_pct=float(raw_snapshot.get("net_exposure_pct", 0.0)),
                strategy_exposures=dict(raw_snapshot.get("strategy_exposures", {})),
                instrument_exposures=dict(raw_snapshot.get("instrument_exposures", {})),
                max_instrument_concentration_pct=float(raw_snapshot.get("max_instrument_concentration_pct", 0.0)),
                herfindahl_index=float(raw_snapshot.get("herfindahl_index", 0.0)),
                drawdown_pct=float(raw_snapshot.get("drawdown_pct", 0.0)),
                circuit_breaker_active=bool(raw_snapshot.get("circuit_breaker_active", False)),
            )
        elif p.get("simulation_id") and monitor.state_store:
            # 2. Retrieve snapshot from stored simulation
            sim_id = str(p["simulation_id"]).strip()
            sim_state = monitor.state_store.get(f"portfolio_simulation:{sim_id}")
            if sim_state and isinstance(sim_state, Mapping):
                snapshots = sim_state.get("risk_snapshots", [])
                if snapshots and isinstance(snapshots, (list, tuple)):
                    last_snap = snapshots[-1]
                    if isinstance(last_snap, Mapping):
                        snapshot = PortfolioRiskSnapshot(
                            timestamp=float(last_snap.get("timestamp", 0.0)),
                            total_equity=float(last_snap.get("total_equity", 100_000.0)),
                            cash=float(last_snap.get("cash", 100_000.0)),
                            cash_reserve_pct=float(last_snap.get("cash_reserve_pct", 100.0)),
                            gross_exposure=float(last_snap.get("gross_exposure", 0.0)),
                            gross_exposure_pct=float(last_snap.get("gross_exposure_pct", 0.0)),
                            net_exposure=float(last_snap.get("net_exposure", 0.0)),
                            net_exposure_pct=float(last_snap.get("net_exposure_pct", 0.0)),
                            strategy_exposures=dict(last_snap.get("strategy_exposures", {})),
                            instrument_exposures=dict(last_snap.get("instrument_exposures", {})),
                            max_instrument_concentration_pct=float(last_snap.get("max_instrument_concentration_pct", 0.0)),
                            herfindahl_index=float(last_snap.get("herfindahl_index", 0.0)),
                            drawdown_pct=float(last_snap.get("drawdown_pct", 0.0)),
                            circuit_breaker_active=bool(last_snap.get("circuit_breaker_active", False)),
                        )
                        source_ref = f"simulation:{sim_id}"

        report = monitor.evaluate_snapshot(
            snapshot=snapshot,
            source_ref=source_ref,
            correlation_id=req.correlation_id,
        )

        return {
            "status": "success",
            "overall_state": report.overall_state.value,
            "is_healthy": report.is_healthy,
            "active_alerts_count": len(report.active_alerts),
            "cleared_alerts_count": len(report.cleared_alerts),
            "summary": report.summary_text,
            "active_alerts": [a.to_dict() for a in report.active_alerts],
            "cleared_alerts": [a.to_dict() for a in report.cleared_alerts],
            "metrics_evaluated": report.metrics_evaluated,
            "report": report.to_dict(),
            "disclaimer": report.disclaimer,
        }

    def handle_get_portfolio_risk_status(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_portfolio_risk_status' command."""
        active = monitor.get_active_alerts()
        latest = monitor.get_latest_report()

        overall_state = latest.overall_state.value if latest else "UNKNOWN"
        summary = latest.summary_text if latest else "No portfolio risk evaluations recorded yet."

        return {
            "status": "success",
            "overall_state": overall_state,
            "is_healthy": (latest.is_healthy if latest else False),
            "active_alerts_count": len(active),
            "active_alerts": [a.to_dict() for a in active],
            "summary": summary,
            "latest_report": latest.to_dict() if latest else None,
            "disclaimer": (
                "RESEARCH ONLY: Portfolio risk monitoring is purely observational and informational. "
                "It does NOT place orders, connect to a broker, or automatically liquidate positions."
            ),
        }

    orchestrator.register_handler(
        command="evaluate_portfolio_risk",
        handler=handle_evaluate_portfolio_risk,
        capability="risk_management",
        description="Evaluate portfolio exposure, cash reserves, and drawdown against risk thresholds",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="get_portfolio_risk_status",
        handler=handle_get_portfolio_risk_status,
        capability="risk_management",
        description="Retrieve current active portfolio risk alerts and health status",
        allow_override=True,
    )
