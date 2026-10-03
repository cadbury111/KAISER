"""KAISER Risk Management Package.

Enforces deterministic risk controls, position limits, drawdown guardrails,
and pre-trade verification gates.

SAFETY INVARIANTS:
1. KAISER analysis must never directly execute a trade without passing through
   an independent deterministic risk gate.
2. Risk monitoring is strictly observational and informational.
"""

from app.risk.handlers import register_risk_handlers
from app.risk.models import (
    AlertSeverity,
    AlertState,
    PortfolioRiskAlert,
    PortfolioRiskMonitorConfig,
    PortfolioRiskReport,
    RiskRuleId,
)
from app.risk.monitor import PortfolioRiskMonitor

__all__ = [
    "PortfolioRiskMonitor",
    "PortfolioRiskMonitorConfig",
    "PortfolioRiskAlert",
    "PortfolioRiskReport",
    "RiskRuleId",
    "AlertSeverity",
    "AlertState",
    "register_risk_handlers",
]
