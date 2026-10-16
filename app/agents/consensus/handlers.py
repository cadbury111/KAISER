"""KAISER — Multi-Agent Consensus Command Handlers.

Registers MasterOrchestrator command handlers for multi-agent specialist coordination
and deterministic consensus evaluation.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from app.agents.consensus.service import MultiAgentCoordinationService
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest


def register_consensus_handlers(
    orchestrator: Any,
    service: MultiAgentCoordinationService,
) -> None:
    """Register multi-agent consensus commands on MasterOrchestrator."""

    def handle_coordinate_agent_consensus(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handle coordinate_agent_consensus command across registered specialist agents."""
        params = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or {})
        symbol = params.get("symbol")
        timeframes_param = params.get("timeframes")
        allow_partial_param = params.get("allow_partial", True)
        reference_time = params.get("reference_time")

        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("Missing or invalid 'symbol' parameter for coordinate_agent_consensus.")

        parsed_tfs: Optional[List[str]] = None
        if timeframes_param:
            if isinstance(timeframes_param, (list, tuple)):
                parsed_tfs = [str(t) for t in timeframes_param]
            elif isinstance(timeframes_param, str):
                parsed_tfs = [t.strip() for t in timeframes_param.split(",") if t.strip()]

        allow_partial = True
        if isinstance(allow_partial_param, bool):
            allow_partial = allow_partial_param
        elif isinstance(allow_partial_param, str):
            allow_partial = allow_partial_param.strip().lower() in ("true", "1", "yes")

        ref_t: Optional[float] = None
        if reference_time is not None:
            try:
                ref_t = float(reference_time)
            except (ValueError, TypeError):
                ref_t = None

        report = service.coordinate_consensus(
            symbol=symbol.strip().upper(),
            timeframes=parsed_tfs,
            allow_partial=allow_partial,
            reference_time=ref_t,
            correlation_id=req.correlation_id,
        )

        return {
            "status": "success",
            "message": report.voice_summary,
            "consensus_id": report.consensus_id,
            "symbol": report.symbol,
            "consensus_status": report.status.value,
            "synthesized_posture": report.synthesized_posture,
            "agreement_points": list(report.agreement_points),
            "disagreement_points": list(report.disagreement_points),
            "data": report.to_dict(),
        }

    orchestrator.register_handler(
        command="coordinate_agent_consensus",
        handler=handle_coordinate_agent_consensus,
        capability="multi_agent_coordination",
        allow_override=True,
    )

    # Register convenient alias
    orchestrator.register_handler(
        command="agent_consensus",
        handler=handle_coordinate_agent_consensus,
        capability="multi_agent_coordination",
        allow_override=True,
    )
