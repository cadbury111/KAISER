"""KAISER Specialist Agents Package.

Hosts autonomous intelligence and specialist trading agents:
- BaseAgent (foundational abstract agent interface)
- TechnicalAnalysisAgent (multi-timeframe technical specialist)
"""

from app.agents.base import BaseAgent
from app.agents.consensus import (
    AgentConsensusReport,
    AgentFinding,
    ConsensusStatus,
    MultiAgentCoordinationService,
    compute_deterministic_consensus_id,
    register_consensus_handlers,
)
from app.agents.regime import (
    MarketRegime,
    MarketRegimeAgent,
    MarketRegimeReport,
    RegimePosture,
    TimeframeRegimeEvaluation,
    TrendStrength,
    VolatilityState,
    register_regime_agent_handlers,
)
from app.agents.technical import (
    ConfidenceLevel,
    CrossTimeframeAgreement,
    SignalCondition,
    TechnicalAnalysisAgent,
    TechnicalIntelligenceReport,
    TimeframeSignalReport,
    register_technical_agent_handlers,
)

__all__ = [
    "BaseAgent",
    "TechnicalAnalysisAgent",
    "SignalCondition",
    "CrossTimeframeAgreement",
    "ConfidenceLevel",
    "TimeframeSignalReport",
    "TechnicalIntelligenceReport",
    "register_technical_agent_handlers",
    # Step 49: Market Regime Specialist
    "MarketRegime",
    "RegimePosture",
    "VolatilityState",
    "TrendStrength",
    "TimeframeRegimeEvaluation",
    "MarketRegimeReport",
    "MarketRegimeAgent",
    "register_regime_agent_handlers",
    # Step 49: Multi-Agent Consensus
    "ConsensusStatus",
    "AgentFinding",
    "AgentConsensusReport",
    "compute_deterministic_consensus_id",
    "MultiAgentCoordinationService",
    "register_consensus_handlers",
]
