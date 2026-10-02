"""KAISER Specialist Agents Package.

Hosts autonomous intelligence and specialist trading agents:
- BaseAgent (foundational abstract agent interface)
- TechnicalAnalysisAgent (multi-timeframe technical specialist)
"""

from app.agents.base import BaseAgent
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
]
