"""KAISER Execution Engine Package.

Manages simulated paper trading execution, risk gating, and portfolio simulation.

SAFETY INVARIANTS:
1. The execution engine is locked by default.
2. Real-money execution strictly requires explicit human approval.
3. No live broker execution is enabled during foundation phase.
4. Paper trading is fully simulated and isolated from all live order paths.
"""

from __future__ import annotations

from app.execution.engine import PaperExecutionEngine
from app.execution.models import (
    PaperExecutionResult,
    PaperFill,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectionCode,
    PaperRiskConfig,
)
from app.execution.portfolio import PaperPosition, PaperPortfolio
from app.execution.risk import PaperRiskGate
from app.execution.simulated_broker import SimulatedBrokerAdapter

__all__ = [
    "PaperOrderSide",
    "PaperOrderType",
    "PaperOrderStatus",
    "PaperRejectionCode",
    "PaperRiskConfig",
    "PaperOrderRequest",
    "PaperOrder",
    "PaperFill",
    "PaperExecutionResult",
    "PaperPosition",
    "PaperPortfolio",
    "PaperRiskGate",
    "SimulatedBrokerAdapter",
    "PaperExecutionEngine",
]
