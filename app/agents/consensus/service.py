"""KAISER — Deterministic Multi-Agent Coordination Service.

Coordinates specialist intelligence agents (TechnicalAnalysisAgent, MarketRegimeAgent),
evaluates deterministic consensus rules, generates structured consensus reports,
and emits auditable lifecycle events.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Analytical only: Zero trade execution, order routing, or broker connections.
2. Failure containment: Individual agent exceptions never crash the service or orchestrator.
3. Explicit classification: Strictly distinguishes AGREEMENT, DISAGREEMENT, INSUFFICIENT_EVIDENCE,
   PARTIAL_RESULT, and FAILED.
4. Non-prescriptive: Never outputs BUY, SELL, position sizing, or target prices.
5. Deterministic replay: Given identical inputs and reference time, generates identical outputs.
"""

from __future__ import annotations

import collections
import copy
from datetime import datetime, timezone
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid

from app.agents.consensus.models import (
    AgentConsensusReport,
    AgentFinding,
    ConsensusStatus,
    compute_deterministic_consensus_id,
)
from app.agents.regime import MarketRegime, MarketRegimeAgent, MarketRegimeReport, RegimePosture
from app.agents.technical import SignalCondition, TechnicalAnalysisAgent, TechnicalIntelligenceReport
from app.analysis.opportunities.models import validate_no_prohibited_fields
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore

logger = get_logger("agents.consensus.service")

DEFAULT_MAX_CONSENSUS_REPORTS = 100


class MultiAgentCoordinationService:
    """Coordinates specialist agents and resolves multi-agent consensus."""

    def __init__(
        self,
        technical_agent: TechnicalAnalysisAgent,
        regime_agent: MarketRegimeAgent,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        max_history: int = DEFAULT_MAX_CONSENSUS_REPORTS,
        name: str = "multi_agent_coordination_service",
    ) -> None:
        self.name = name
        self.technical_agent = technical_agent
        self.regime_agent = regime_agent
        self.event_bus = event_bus
        self.state_store = state_store
        self.max_history = max_history
        self._lock = threading.RLock()
        self._reports_history: collections.deque[AgentConsensusReport] = collections.deque(maxlen=max_history)
        self._coordination_count = 0

    @property
    def coordination_count(self) -> int:
        with self._lock:
            return self._coordination_count

    def get_status(self) -> Dict[str, Any]:
        """Return operational telemetry."""
        with self._lock:
            return {
                "name": self.name,
                "coordination_count": self._coordination_count,
                "history_length": len(self._reports_history),
                "technical_agent": self.technical_agent.name,
                "regime_agent": self.regime_agent.name,
                "max_history": self.max_history,
            }

    def coordinate_consensus(
        self,
        symbol: str,
        timeframes: Optional[Sequence[str]] = None,
        allow_partial: bool = True,
        reference_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> AgentConsensusReport:
        """Execute deterministic multi-agent specialist coordination across technical and regime specialists.

        Args:
            symbol: Ticker symbol to evaluate.
            timeframes: Timeframes to assess across agents.
            allow_partial: Whether a single agent failure returns PARTIAL_RESULT or FAILED.
            reference_time: Optional deterministic clock.
            correlation_id: Tracing correlation ID.

        Returns:
            AgentConsensusReport: Structured outcome.
        """
        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("MultiAgentCoordinationService requires a non-empty symbol string.")

        clean_symbol = symbol.strip().upper()
        now = time.time() if reference_time is None else float(reference_time)
        cid = correlation_id or uuid.uuid4().hex

        with self._lock:
            self._coordination_count += 1

        # 1. Dispatch coordination started event
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.MULTI_AGENT_COORDINATION_STARTED,
                    source=self.name,
                    correlation_id=cid,
                    payload={
                        "symbol": clean_symbol,
                        "timeframes": list(timeframes) if timeframes else None,
                        "allow_partial": allow_partial,
                    },
                )
            )

        # 2. Invoke Participating Agents with Exception Containment
        findings: Dict[str, AgentFinding] = {}
        participating_agents = [self.technical_agent.name, self.regime_agent.name]

        # Agent 1: Technical Analysis Specialist
        tech_report: Optional[TechnicalIntelligenceReport] = None
        try:
            tech_report = self.technical_agent.generate_report(
                symbol=clean_symbol,
                timeframes=timeframes,
                reference_time=now,
                correlation_id=cid,
            )
            findings[self.technical_agent.name] = AgentFinding(
                agent_name=self.technical_agent.name,
                agent_type=self.technical_agent.agent_type,
                posture=tech_report.overall_condition.value,
                confidence=tech_report.confidence.value,
                summary=tech_report.voice_summary,
                facts=tech_report.facts,
                limitations=tech_report.uncertainty_caveats,
                is_valid=True,
            )
        except Exception as exc:
            logger.error("TechnicalAnalysisAgent failed during coordination for %s: %s", clean_symbol, exc)
            findings[self.technical_agent.name] = AgentFinding(
                agent_name=self.technical_agent.name,
                agent_type=self.technical_agent.agent_type,
                posture="FAILED",
                confidence="NONE",
                summary=f"Technical analysis execution error: {exc}",
                is_valid=False,
                error=str(exc),
            )

        # Agent 2: Market Regime Specialist
        regime_report: Optional[MarketRegimeReport] = None
        try:
            regime_report = self.regime_agent.generate_report(
                symbol=clean_symbol,
                timeframes=timeframes,
                reference_time=now,
                correlation_id=cid,
            )
            findings[self.regime_agent.name] = AgentFinding(
                agent_name=self.regime_agent.name,
                agent_type=self.regime_agent.agent_type,
                posture=regime_report.overall_posture.value,
                confidence=regime_report.overall_trend_strength.value,
                summary=regime_report.voice_summary,
                facts=regime_report.facts,
                limitations=regime_report.uncertainty_caveats,
                is_valid=True,
            )
        except Exception as exc:
            logger.error("MarketRegimeAgent failed during coordination for %s: %s", clean_symbol, exc)
            findings[self.regime_agent.name] = AgentFinding(
                agent_name=self.regime_agent.name,
                agent_type=self.regime_agent.agent_type,
                posture="FAILED",
                confidence="NONE",
                summary=f"Market regime execution error: {exc}",
                is_valid=False,
                error=str(exc),
            )

        # 3. Apply Deterministic Consensus Matrix
        status: ConsensusStatus
        synthesized_posture: Optional[str] = None
        agreement_points: List[str] = []
        disagreement_points: List[str] = []
        limitations: List[str] = []

        valid_findings = {k: v for k, v in findings.items() if v.is_valid}
        failed_findings = {k: v for k, v in findings.items() if not v.is_valid}

        # Case A: Both agents failed
        if len(failed_findings) == len(findings):
            status = ConsensusStatus.FAILED
            limitations.append("All participating specialist agents failed execution.")
            voice_summary = f"Multi-agent coordination failed for {clean_symbol}; all specialist agents encountered errors."

        # Case B: Exactly one agent failed
        elif len(failed_findings) > 0:
            if allow_partial:
                status = ConsensusStatus.PARTIAL_RESULT
                failed_names = ", ".join(failed_findings.keys())
                succ_names = ", ".join(valid_findings.keys())
                limitations.append(f"Partial analytical evidence: agent(s) [{failed_names}] failed while [{succ_names}] succeeded.")
                voice_summary = f"Partial specialist result for {clean_symbol}. Technical analysis succeeded but regime analysis encountered an issue." if self.technical_agent.name in valid_findings else f"Partial specialist result for {clean_symbol}. Market regime succeeded but technical analysis encountered an issue."
            else:
                status = ConsensusStatus.FAILED
                limitations.append(f"Coordination failed because partial results are disallowed and agent(s) [{', '.join(failed_findings.keys())}] failed.")
                voice_summary = f"Multi-agent coordination failed for {clean_symbol} due to specialist agent failure under strict all-agent consensus policy."

        # Case C: Both agents returned findings successfully
        else:
            tech_finding = findings[self.technical_agent.name]
            regime_finding = findings[self.regime_agent.name]

            # Collect caveats from both agents
            limitations.extend(tech_finding.limitations)
            limitations.extend(regime_finding.limitations)

            # Check for INSUFFICIENT_DATA in either agent
            if tech_finding.posture == "INSUFFICIENT_DATA" or regime_finding.posture == "INSUFFICIENT_DATA":
                status = ConsensusStatus.INSUFFICIENT_EVIDENCE
                limitations.append("One or more specialist agents reported insufficient market bar history or data gaps.")
                voice_summary = f"Insufficient evidence to establish multi-agent consensus for {clean_symbol} due to data history limitations."

            # Check AGREEMENT vs DISAGREEMENT
            elif tech_finding.posture == regime_finding.posture:
                status = ConsensusStatus.AGREEMENT
                synthesized_posture = tech_finding.posture
                agreement_points.append(
                    f"Both technical analysis and market regime confirm a {synthesized_posture} posture."
                )
                voice_summary = f"Specialist consensus reached for {clean_symbol}: Both technical indicators and market regime agree on a {synthesized_posture.lower()} posture."

            else:
                status = ConsensusStatus.DISAGREEMENT
                synthesized_posture = "CONFLICTING"
                disagreement_points.append(
                    f"Technical analysis indicates {tech_finding.posture} posture while market regime indicates {regime_finding.posture}."
                )
                voice_summary = f"Specialist divergence for {clean_symbol}: Technical analysis indicates {tech_finding.posture.lower()} posture while market regime indicates {regime_finding.posture.lower()}."

        # 4. Construct Immutable Consensus Report
        iso_str = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
        consensus_id = compute_deterministic_consensus_id(clean_symbol, now, findings)

        report = AgentConsensusReport(
            consensus_id=consensus_id,
            symbol=clean_symbol,
            timestamp=now,
            iso_timestamp=iso_str,
            status=status,
            synthesized_posture=synthesized_posture,
            participating_agents=participating_agents,
            agent_findings=findings,
            agreement_points=agreement_points,
            disagreement_points=disagreement_points,
            limitations=limitations,
            voice_summary=voice_summary,
            provenance={
                "correlation_id": cid,
                "allow_partial": allow_partial,
                "technical_agent": self.technical_agent.name,
                "regime_agent": self.regime_agent.name,
            },
        )

        # 5. Bounded StateStore and History Retention
        with self._lock:
            self._reports_history.append(report)

        if self.state_store:
            self.state_store.update(
                "agent_consensus_reports",
                {clean_symbol: report.to_dict()},
                default={},
            )

        # 6. Publish Deterministic Consensus Lifecycle Event
        if self.event_bus:
            event_type = EventType.MULTI_AGENT_COORDINATION_FAILED
            if status == ConsensusStatus.AGREEMENT:
                event_type = EventType.MULTI_AGENT_CONSENSUS_REACHED
            elif status == ConsensusStatus.DISAGREEMENT:
                event_type = EventType.MULTI_AGENT_CONSENSUS_DISAGREEMENT
            elif status == ConsensusStatus.INSUFFICIENT_EVIDENCE:
                event_type = EventType.MULTI_AGENT_CONSENSUS_INSUFFICIENT_EVIDENCE
            elif status == ConsensusStatus.PARTIAL_RESULT:
                event_type = EventType.MULTI_AGENT_CONSENSUS_PARTIAL

            self.event_bus.publish(
                Event.create(
                    event_type=event_type,
                    source=self.name,
                    correlation_id=cid,
                    payload={
                        "consensus_id": consensus_id,
                        "symbol": clean_symbol,
                        "status": status.value,
                        "synthesized_posture": synthesized_posture,
                    },
                )
            )

        return report

    def get_latest_report(self, symbol: Optional[str] = None) -> Optional[AgentConsensusReport]:
        """Retrieve the most recent consensus report, optionally filtered by symbol."""
        with self._lock:
            if not self._reports_history:
                return None
            if symbol is None:
                return self._reports_history[-1]
            clean = symbol.strip().upper()
            for r in reversed(self._reports_history):
                if r.symbol == clean:
                    return r
            return None
