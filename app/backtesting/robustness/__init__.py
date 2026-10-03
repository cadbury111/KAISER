"""KAISER — Strategy Robustness & Stress Testing Package.

Provides research-only stress testing against transaction-cost variations,
parameter sensitivity perturbations, and historical time partitions.
"""

from __future__ import annotations

from app.backtesting.robustness.engine import RobustnessTestEngine
from app.backtesting.robustness.models import (
    HistoricalWindowComparison,
    ParameterSensitivityResult,
    RobustnessStatus,
    RobustnessSummary,
    RobustnessTestConfig,
    RobustnessTestResult,
    RobustnessWarning,
    ScenarioType,
    StressScenario,
    StressScenarioResult,
)

__all__ = [
    "RobustnessTestEngine",
    "RobustnessStatus",
    "ScenarioType",
    "RobustnessWarning",
    "StressScenario",
    "StressScenarioResult",
    "ParameterSensitivityResult",
    "HistoricalWindowComparison",
    "RobustnessSummary",
    "RobustnessTestConfig",
    "RobustnessTestResult",
]
