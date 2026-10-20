"""KAISER Execution Engine Package.

Manages simulated paper trading execution, risk gating, and portfolio simulation.

SAFETY INVARIANTS:
1. The execution engine is locked by default.
2. Real-money execution strictly requires explicit human approval.
3. No live broker execution is enabled during foundation phase.
4. Paper trading is fully simulated and isolated from all live order paths.
"""

from __future__ import annotations

from app.execution.calibration import (
    HistoricalSpreadCalibrator,
    ObservationRejectionReason,
    SpreadCalibrationReport,
    SpreadObservation,
    SpreadStats,
)
from app.execution.engine import PaperExecutionEngine
from app.execution.metrics import (
    ExecutionLatencyProfiler,
    LatencyAggregate,
    LatencyStage,
    OrderLifecycleEvent,
    OrderLifecycleStage,
    OrderLifecycleTracker,
    calculate_quantiles,
)
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
from app.execution.portfolio import PaperPosition, PaperPortfolio, compute_financial_digest
from app.execution.risk import PaperRiskGate
from app.execution.simulated_broker import SimulatedBrokerAdapter
from app.execution.workflow import (
    PaperSimulationSession,
    PaperTradingWorkflowCoordinator,
    PaperWorkflowOutcome,
    PaperWorkflowRequest,
    WorkflowStage,
    WorkflowStatus,
)

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
    "compute_financial_digest",
    "PaperRiskGate",
    "SimulatedBrokerAdapter",
    "PaperExecutionEngine",
    "WorkflowStage",
    "WorkflowStatus",
    "PaperWorkflowRequest",
    "PaperWorkflowOutcome",
    "PaperSimulationSession",
    "PaperTradingWorkflowCoordinator",
    "OrderLifecycleStage",
    "OrderLifecycleEvent",
    "LatencyStage",
    "LatencyAggregate",
    "ExecutionLatencyProfiler",
    "OrderLifecycleTracker",
    "calculate_quantiles",
    "ObservationRejectionReason",
    "SpreadObservation",
    "SpreadStats",
    "SpreadCalibrationReport",
    "HistoricalSpreadCalibrator",
]

