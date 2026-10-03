"""KAISER — Portfolio-Level Risk Analytics & Multi-Strategy Simulation Package.

Provides a research-only framework for evaluating multiple strategies operating together
inside a shared virtual portfolio, enforcing capital limits, position concentration bounds,
deterministic simultaneous-signal resolution, and pairwise return correlation analytics.
"""

from app.backtesting.portfolio_simulation.analytics import (
    calculate_pearson_correlation,
    compute_diversification_metrics,
)
from app.backtesting.portfolio_simulation.engine import PortfolioSimulationEngine
from app.backtesting.portfolio_simulation.models import (
    DiversificationMetrics,
    PortfolioPosition,
    PortfolioRiskSnapshot,
    PortfolioRiskWarning,
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
    StrategyContribution,
)

__all__ = [
    "PortfolioSimulationEngine",
    "PortfolioSimulationConfig",
    "PortfolioSimulationResult",
    "PortfolioSimulationStatus",
    "StrategyAllocation",
    "PortfolioPosition",
    "PortfolioRiskSnapshot",
    "PortfolioRiskWarning",
    "StrategyContribution",
    "DiversificationMetrics",
    "calculate_pearson_correlation",
    "compute_diversification_metrics",
]
