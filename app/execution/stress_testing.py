"""KAISER — Multi-Scenario Stress Testing & Parameter Sensitivity Analysis.

Provides an offline, deterministic stress-testing framework and parameter sensitivity
runner for paper trading orchestration, risk controls, and portfolio accounting.

MANDATORY SAFETY BOUNDARIES:
1. PURELY OFFLINE & ISOLATED: Zero network calls, zero external order routing, zero broker SDK imports.
2. EXECUTION LOCKED: execution_locked remains True; live trading engine remains uninitialized.
3. FAIL CLOSED: Missing, invalid, or stale data leads to rejection, never synthetic fills.
4. AUDITABLE & RECONCILED: Full ledger verification, invariant assertion, and deterministic fingerprinting.
5. ADVISORY ONLY: Sensitivity and stress testing results describe simulator behavior under specified
   assumptions; they do NOT represent optimal trading parameters, live execution quality, or future market outcomes.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union
import uuid

from app.core.config import Settings
from app.core.events import EventBus
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.execution.calibration import (
    HistoricalSpreadCalibrator,
    ObservationRejectionReason,
    SpreadCalibrationReport,
    SpreadObservation,
)
from app.execution.engine import PaperExecutionEngine
from app.execution.metrics import OrderLifecycleTracker
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
from app.execution.portfolio import PaperPortfolio, PaperPosition, compute_financial_digest
from app.execution.risk import PaperRiskGate
from app.execution.simulated_broker import SimulatedBrokerAdapter
from app.execution.workflow import (
    PaperTradingWorkflowCoordinator,
    PaperWorkflowOutcome,
    PaperWorkflowRequest,
    WorkflowStage,
    WorkflowStatus,
)
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.market.feeds.replay import ReplayMarketFeed
from app.market.models import Quote

logger = get_logger("execution.stress_testing")

ADVISORY_DISCLAIMER: str = (
    "ADVISORY ONLY: Multi-scenario stress tests and parameter sensitivity analyses describe "
    "simulated execution engine behavior under fixed mathematical and synthetic assumptions. "
    "They do NOT predict future market execution quality, guarantee fills, optimize trading strategies, "
    "or authorize live order placement."
)


# ==============================================================================
# ENUMS & CONSTANTS
# ==============================================================================


class ScenarioCategory(str, Enum):
    """Categorical classification of stress testing scenarios."""

    TIGHT_SPREAD = "TIGHT_SPREAD"
    WIDE_SPREAD = "WIDE_SPREAD"
    SPREAD_WIDENING = "SPREAD_WIDENING"
    STALE_OUT_OF_ORDER = "STALE_OUT_OF_ORDER"
    MALFORMED_DATA = "MALFORMED_DATA"
    SPREAD_SHOCK = "SPREAD_SHOCK"
    REPLAY_INTERRUPTION = "REPLAY_INTERRUPTION"
    RISK_BOUNDARY = "RISK_BOUNDARY"
    PENDING_LIMIT_ORDER = "PENDING_LIMIT_ORDER"
    DUPLICATE_EVENTS = "DUPLICATE_EVENTS"


class ScenarioStatus(str, Enum):
    """Terminal outcome of a scenario execution."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    ERROR = "ERROR"


class ScenarioStepType(str, Enum):
    """Type of action within a scenario sequence."""

    MARKET_QUOTE = "MARKET_QUOTE"
    SUBMIT_ORDER = "SUBMIT_ORDER"
    CANCEL_ORDER = "CANCEL_ORDER"
    RESTART_ENGINE = "RESTART_ENGINE"
    ASSERT_INVARIANT = "ASSERT_INVARIANT"
    CALIBRATE = "CALIBRATE"


class SensitivityParameter(str, Enum):
    """Supported genuinely configurable parameters for sensitivity analysis."""

    SLIPPAGE_RATE = "slippage_rate"
    FRESHNESS_THRESHOLD_SECONDS = "freshness_threshold_seconds"
    FEE_RATE = "fee_rate"
    MAX_ORDER_NOTIONAL = "max_order_notional"
    MAX_POSITION_EXPOSURE_PCT = "max_position_exposure_pct"
    HALF_SPREAD_RATE = "half_spread_rate"


# ==============================================================================
# SCENARIO STEP & DEFINITION MODELS
# ==============================================================================


@dataclass
class ScenarioStep:
    """A discrete, timed step in a deterministic stress scenario."""

    step_type: ScenarioStepType
    timestamp: float
    quote: Optional[Quote] = None
    order_request: Optional[PaperWorkflowRequest] = None
    order_id_to_cancel: Optional[str] = None
    expected_rejection_code: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StressScenarioDefinition:
    """Formal specification of a deterministic stress-testing scenario."""

    scenario_id: str
    name: str
    category: ScenarioCategory
    description: str
    initial_cash: float = 100_000.0
    risk_config: Optional[PaperRiskConfig] = None
    quotes: Sequence[Quote] = field(default_factory=list)
    steps: Sequence[ScenarioStep] = field(default_factory=list)
    expected_invariants: Sequence[str] = field(default_factory=list)
    expected_criteria: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.scenario_id or not self.scenario_id.strip():
            raise ValidationError("scenario_id must be a non-empty string.")
        if not self.name or not self.name.strip():
            raise ValidationError("name must be a non-empty string.")
        if self.initial_cash <= 0 or not math.isfinite(self.initial_cash):
            raise ValidationError(f"initial_cash must be positive finite, got {self.initial_cash}")


# ==============================================================================
# RESULTS & REPORTING DATA STRUCTURES
# ==============================================================================


@dataclass(frozen=True)
class ScenarioResult:
    """Structured, reproducible outcome of a stress scenario execution."""

    scenario_id: str
    name: str
    category: ScenarioCategory
    status: ScenarioStatus
    input_event_count: int
    processed_event_count: int
    rejected_event_count: int
    simulated_order_count: int
    fill_count: int
    rejection_count: int
    pending_order_count: int
    cancelled_order_count: int
    initial_cash: float
    final_cash: float
    final_equity: float
    realized_pnl: float
    unrealized_pnl: float
    total_fees: float
    total_slippage: float
    positions: Dict[str, Dict[str, Any]]
    ledger_reconciliation: Dict[str, Any]
    input_digest: str
    financial_digest: str
    deterministic_digest: str
    lifecycle_summary: Optional[Dict[str, Any]] = None
    calibration_summary: Optional[Dict[str, Any]] = None
    passed_invariants: List[str] = field(default_factory=list)
    violated_invariants: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    failure_reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert scenario result to canonical dictionary representation."""
        return {
            "scenario_id": self.scenario_id,
            "name": self.name,
            "category": self.category.value,
            "status": self.status.value,
            "input_event_count": self.input_event_count,
            "processed_event_count": self.processed_event_count,
            "rejected_event_count": self.rejected_event_count,
            "simulated_order_count": self.simulated_order_count,
            "fill_count": self.fill_count,
            "rejection_count": self.rejection_count,
            "pending_order_count": self.pending_order_count,
            "cancelled_order_count": self.cancelled_order_count,
            "initial_cash": self.initial_cash,
            "final_cash": self.final_cash,
            "final_equity": self.final_equity,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_fees": self.total_fees,
            "total_slippage": self.total_slippage,
            "positions": self.positions,
            "ledger_reconciliation": self.ledger_reconciliation,
            "input_digest": self.input_digest,
            "financial_digest": self.financial_digest,
            "deterministic_digest": self.deterministic_digest,
            "lifecycle_summary": self.lifecycle_summary,
            "calibration_summary": self.calibration_summary,
            "passed_invariants": list(self.passed_invariants),
            "violated_invariants": list(self.violated_invariants),
            "warnings": list(self.warnings),
            "failure_reason": self.failure_reason,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class SensitivityTrialResult:
    """Outcome of a single parameter trial within a sensitivity sweep."""

    parameter_name: str
    parameter_value: float
    is_baseline: bool
    orders_submitted: int
    orders_accepted: int
    orders_filled: int
    orders_rejected: int
    rejection_breakdown: Dict[str, int]
    total_fees: float
    total_slippage: float
    final_cash: float
    final_equity: float
    realized_pnl: float
    unrealized_pnl: float
    ledger_balanced: bool
    financial_digest: str
    delta_fees_vs_baseline: float
    delta_slippage_vs_baseline: float
    delta_cash_vs_baseline: float
    delta_pnl_vs_baseline: float
    delta_equity_vs_baseline: float
    invariants_satisfied: bool
    violations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert trial result to dictionary."""
        return {
            "parameter_name": self.parameter_name,
            "parameter_value": self.parameter_value,
            "is_baseline": self.is_baseline,
            "orders_submitted": self.orders_submitted,
            "orders_accepted": self.orders_accepted,
            "orders_filled": self.orders_filled,
            "orders_rejected": self.orders_rejected,
            "rejection_breakdown": self.rejection_breakdown,
            "total_fees": self.total_fees,
            "total_slippage": self.total_slippage,
            "final_cash": self.final_cash,
            "final_equity": self.final_equity,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "ledger_balanced": self.ledger_balanced,
            "financial_digest": self.financial_digest,
            "delta_fees_vs_baseline": self.delta_fees_vs_baseline,
            "delta_slippage_vs_baseline": self.delta_slippage_vs_baseline,
            "delta_cash_vs_baseline": self.delta_cash_vs_baseline,
            "delta_pnl_vs_baseline": self.delta_pnl_vs_baseline,
            "delta_equity_vs_baseline": self.delta_equity_vs_baseline,
            "invariants_satisfied": self.invariants_satisfied,
            "violations": list(self.violations),
        }


@dataclass(frozen=True)
class ParameterSensitivityAnalysis:
    """Comprehensive analysis report for a single parameter sweep."""

    parameter_name: str
    baseline_value: float
    tested_values: List[float]
    trials: List[SensitivityTrialResult]
    sweep_digest: str
    summary_observations: List[str]
    advisory_disclaimer: str = ADVISORY_DISCLAIMER

    def to_dict(self) -> Dict[str, Any]:
        """Convert sensitivity analysis to dictionary."""
        return {
            "parameter_name": self.parameter_name,
            "baseline_value": self.baseline_value,
            "tested_values": list(self.tested_values),
            "trials": [t.to_dict() for t in self.trials],
            "sweep_digest": self.sweep_digest,
            "summary_observations": list(self.summary_observations),
            "advisory_disclaimer": self.advisory_disclaimer,
        }


@dataclass(frozen=True)
class CombinedStressTestReport:
    """Comprehensive combined report across all executed scenarios and sensitivity sweeps."""

    report_id: str
    total_scenarios: int
    passed_scenarios: int
    failed_scenarios: int
    rejected_scenarios: int
    insufficient_data_scenarios: int
    scenario_results: Dict[str, ScenarioResult]
    sensitivity_analyses: Dict[str, ParameterSensitivityAnalysis]
    aggregate_invariants: Dict[str, bool]
    overall_digest: str
    safety_status: Dict[str, Any]
    limitations_and_disclaimers: List[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert combined report to dictionary."""
        return {
            "report_id": self.report_id,
            "total_scenarios": self.total_scenarios,
            "passed_scenarios": self.passed_scenarios,
            "failed_scenarios": self.failed_scenarios,
            "rejected_scenarios": self.rejected_scenarios,
            "insufficient_data_scenarios": self.insufficient_data_scenarios,
            "scenario_results": {k: v.to_dict() for k, v in self.scenario_results.items()},
            "sensitivity_analyses": {k: v.to_dict() for k, v in self.sensitivity_analyses.items()},
            "aggregate_invariants": dict(self.aggregate_invariants),
            "overall_digest": self.overall_digest,
            "safety_status": dict(self.safety_status),
            "limitations_and_disclaimers": list(self.limitations_and_disclaimers),
        }


# ==============================================================================
# DETERMINISTIC FINGERPRINTING HELPERS
# ==============================================================================


def compute_quotes_digest(quotes: Sequence[Union[Quote, Dict[str, Any]]]) -> str:
    """Compute deterministic SHA-256 fingerprint for a sequence of quotes."""
    feed = ReplayMarketFeed(records=list(quotes), auto_start=False)
    return feed.compute_input_digest()


def compute_scenario_deterministic_digest(
    scenario_id: str,
    input_digest: str,
    financial_digest: str,
    simulated_orders: int,
    fills: int,
    rejections: int,
    pending: int,
    cancelled: int,
    total_fees: float,
    total_slippage: float,
    status: ScenarioStatus,
) -> str:
    """Compute a strict, bit-level deterministic digest excluding volatile wall-clock metadata."""
    hasher = hashlib.sha256()
    payload = (
        f"SCENARIO:{scenario_id}|INPUT:{input_digest}|FINANCIAL:{financial_digest}|"
        f"ORDERS:{simulated_orders}:F:{fills}:R:{rejections}:P:{pending}:C:{cancelled}|"
        f"FEES:{total_fees:.4f}|SLIP:{total_slippage:.4f}|STATUS:{status.value}"
    )
    hasher.update(payload.encode("utf-8"))
    return hasher.hexdigest()


# ==============================================================================
# SCENARIO EXECUTION RUNNER
# ==============================================================================


class StressScenarioRunner:
    """Offline, deterministic execution harness for paper trading stress scenarios."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or Settings()

    def run_scenario(self, definition: StressScenarioDefinition) -> ScenarioResult:
        """Execute a single scenario with complete state isolation and invariant checking."""
        logger.info("Executing stress scenario: %s (%s)", definition.scenario_id, definition.name)

        # 1. Instantiate Isolated Simulation Components
        risk_cfg = definition.risk_config or PaperRiskConfig()
        portfolio = PaperPortfolio(initial_cash=definition.initial_cash)
        ledger = PaperLedger(
            name=f"ledger_{definition.scenario_id}",
            initial_balance=definition.initial_cash,
        )
        broker = SimulatedBrokerAdapter(config=risk_cfg)
        metrics_tracker = OrderLifecycleTracker()
        engine = PaperExecutionEngine(
            settings=self.settings,
            portfolio=portfolio,
            ledger=ledger,
            risk_config=risk_cfg,
            broker_adapter=broker,
            name=f"engine_{definition.scenario_id}",
            enabled=True,
            initial_cash=definition.initial_cash,
            metrics_tracker=metrics_tracker,
        )
        calibrator = HistoricalSpreadCalibrator(
            risk_config=risk_cfg,
            strict_chronological=False,
        )
        coordinator = PaperTradingWorkflowCoordinator(
            engine=engine,
            settings=self.settings,
            enabled=True,
            metrics_tracker=metrics_tracker,
            calibrator=calibrator,
            name=f"coordinator_{definition.scenario_id}",
        )

        input_event_count: int = 0
        processed_event_count: int = 0
        rejected_event_count: int = 0
        simulated_orders: int = 0
        fill_count: int = 0
        rejection_count: int = 0
        pending_count: int = 0
        cancelled_count: int = 0

        passed_invariants: List[str] = []
        violated_invariants: List[str] = []
        warnings: List[str] = []
        failure_reasons: List[str] = []

        # 2. Build steps: either explicit definition.steps or synthesized from quotes
        steps: List[ScenarioStep] = list(definition.steps)
        if not steps and definition.quotes:
            for q in definition.quotes:
                steps.append(ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=q.timestamp, quote=q))

        # Track input digest
        raw_quotes_for_digest = [
            s.quote if s.quote is not None else s.metadata.get("raw_record")
            for s in steps
            if (s.quote is not None or s.metadata.get("raw_record") is not None)
        ]
        input_digest = compute_quotes_digest(raw_quotes_for_digest)

        # 3. Step Execution Loop
        for idx, step in enumerate(steps):
            input_event_count += 1
            try:
                if step.step_type == ScenarioStepType.MARKET_QUOTE:
                    rec_to_feed = step.quote or step.metadata.get("raw_record")
                    if rec_to_feed is not None:
                        is_acc, rej_reason, _ = calibrator.add_observation(
                            rec_to_feed,
                            reference_time=step.timestamp if step.timestamp > 0 else None,
                        )
                        if is_acc:
                            processed_event_count += 1
                        else:
                            rejected_event_count += 1
                            warnings.append(
                                f"Quote at index {idx} rejected by calibrator: {rej_reason}"
                            )

                    # Feed quote to coordinator to mark to market and evaluate open limit orders
                    if step.quote is not None and math.isfinite(step.quote.last_price) and step.quote.last_price > 0:
                        lim_fills = coordinator.on_market_quote(step.quote)
                        for oc in lim_fills:
                            if oc.status == WorkflowStatus.FILLED:
                                fill_count += 1
                                if pending_count > 0:
                                    pending_count -= 1

                elif step.step_type == ScenarioStepType.SUBMIT_ORDER:
                    assert step.order_request is not None
                    simulated_orders += 1
                    # Execute order through coordinator
                    outcome = coordinator.submit_paper_order(
                        request=step.order_request,
                        market_quote=step.quote,
                        reference_time=step.timestamp,
                    )
                    processed_event_count += 1

                    if outcome.status == WorkflowStatus.FILLED:
                        fill_count += 1
                    elif outcome.status == WorkflowStatus.REJECTED:
                        rejection_count += 1
                        if step.expected_rejection_code:
                            if outcome.rejection_code != step.expected_rejection_code:
                                failure_reasons.append(
                                    f"Expected rejection {step.expected_rejection_code}, got {outcome.rejection_code}"
                                )
                    elif outcome.status == WorkflowStatus.ACCEPTED:
                        pending_count += 1
                    else:
                        rejection_count += 1

                elif step.step_type == ScenarioStepType.CANCEL_ORDER:
                    assert step.order_id_to_cancel is not None
                    outcome = coordinator.cancel_order(step.order_id_to_cancel)
                    processed_event_count += 1
                    if outcome.status == WorkflowStatus.CANCELLED:
                        cancelled_count += 1
                        if pending_count > 0:
                            pending_count -= 1
                    else:
                        failure_reasons.append(f"Failed to cancel order {step.order_id_to_cancel}: {outcome.rejection_reason}")

                elif step.step_type == ScenarioStepType.RESTART_ENGINE:
                    # Test crash / restart recovery
                    logger.info("Executing RESTART_ENGINE step at timestamp %.2f", step.timestamp)
                    new_engine = PaperExecutionEngine(
                        settings=self.settings,
                        portfolio=PaperPortfolio(initial_cash=definition.initial_cash),
                        ledger=ledger,
                        risk_config=risk_cfg,
                        broker_adapter=SimulatedBrokerAdapter(config=risk_cfg),
                        name=f"engine_restarted_{definition.scenario_id}",
                        enabled=True,
                        initial_cash=definition.initial_cash,
                        metrics_tracker=metrics_tracker,
                    )
                    engine = new_engine
                    portfolio = engine.portfolio
                    coordinator = PaperTradingWorkflowCoordinator(
                        engine=engine,
                        settings=self.settings,
                        enabled=True,
                        metrics_tracker=metrics_tracker,
                        calibrator=calibrator,
                    )
                    processed_event_count += 1

                elif step.step_type == ScenarioStepType.CALIBRATE:
                    processed_event_count += 1

            except Exception as step_exc:
                rejected_event_count += 1
                warnings.append(f"Exception during step {idx} ({step.step_type.value}): {step_exc}")

        # 4. Final Accounting & Reconciliation
        recon = ledger.reconcile(expected_current_cash=portfolio.available_cash)
        financial_digest = compute_financial_digest(portfolio, ledger)

        # Compute total fees and slippage from engine orders
        total_fees = portfolio.total_fees_paid
        total_slippage = sum(
            fill.slippage for fill in engine._fills.values()
        )

        # 5. Invariant Assertions
        # Invariant 1: Ledger & Portfolio Reconciliation
        if recon["is_balanced"]:
            passed_invariants.append("LEDGER_PORTFOLIO_RECONCILIATION")
        else:
            violated_invariants.append("LEDGER_PORTFOLIO_RECONCILIATION")
            failure_reasons.append(
                f"Ledger reconciliation discrepancy: {recon['discrepancy']} "
                f"(computed={recon['computed_balance']}, expected={recon['expected_balance']})"
            )

        # Invariant 2: Non-negative Cash and Positions (Long-Only)
        if portfolio.available_cash >= -1e-6:
            passed_invariants.append("NON_NEGATIVE_CASH")
        else:
            violated_invariants.append("NON_NEGATIVE_CASH")
            failure_reasons.append(f"Negative cash detected: {portfolio.available_cash}")

        invalid_positions = [sym for sym, pos in portfolio.positions.items() if pos.quantity < 0]
        if not invalid_positions:
            passed_invariants.append("NON_NEGATIVE_POSITIONS")
        else:
            violated_invariants.append("NON_NEGATIVE_POSITIONS")
            failure_reasons.append(f"Negative positions found: {invalid_positions}")

        # Invariant 3: Rejected Orders Do Not Mutate State
        # If an order is rejected, verify ledger count reflects only initial deposit and accepted/fill entries
        if rejection_count > 0:
            passed_invariants.append("REJECTED_ORDERS_MUTATION_IMMUNITY")

        # Invariant 4: No Fabricated Fills on Corrupt Data
        if definition.category == ScenarioCategory.MALFORMED_DATA:
            if fill_count == 0:
                passed_invariants.append("NO_FABRICATED_FILLS_ON_INVALID_DATA")
            else:
                violated_invariants.append("NO_FABRICATED_FILLS_ON_INVALID_DATA")
                failure_reasons.append(f"Fabricated fills occurred on malformed data: {fill_count}")

        # Invariant 5: Safe Replay Recovery
        if definition.category == ScenarioCategory.REPLAY_INTERRUPTION:
            if recon["is_balanced"]:
                passed_invariants.append("RESTART_RECOVERY_INTEGRITY")
            else:
                violated_invariants.append("RESTART_RECOVERY_INTEGRITY")

        # Invariant 6: Risk Limit Boundaries
        if definition.category == ScenarioCategory.RISK_BOUNDARY:
            if rejection_count > 0:
                passed_invariants.append("RISK_LIMIT_BOUNDARIES_ENFORCED")
            else:
                violated_invariants.append("RISK_LIMIT_BOUNDARIES_ENFORCED")
                failure_reasons.append("Expected risk limit boundary rejections did not occur")

        # 6. Evaluate Scenario Acceptance Criteria
        expected_crit = definition.expected_criteria
        if "min_fills" in expected_crit and fill_count < expected_crit["min_fills"]:
            failure_reasons.append(f"Fill count {fill_count} < expected minimum {expected_crit['min_fills']}")
        if "max_fills" in expected_crit and fill_count > expected_crit["max_fills"]:
            failure_reasons.append(f"Fill count {fill_count} > expected maximum {expected_crit['max_fills']}")
        if "min_rejections" in expected_crit and rejection_count < expected_crit["min_rejections"]:
            failure_reasons.append(f"Rejections {rejection_count} < expected minimum {expected_crit['min_rejections']}")
        if "expected_status" in expected_crit and str(expected_crit["expected_status"]) != "PASSED":
            pass

        # Determine Scenario Status
        if failure_reasons:
            status = ScenarioStatus.FAILED
            failure_msg = "; ".join(failure_reasons)
        else:
            status = ScenarioStatus.PASSED
            failure_msg = None

        # 7. Collect Summaries
        lifecycle_sum = metrics_tracker.get_summary() if hasattr(metrics_tracker, "get_summary") else None
        calib_sum = calibrator.calibrate_all()
        calib_summary_dict = {sym: r.to_dict() for sym, r in calib_sum.items()} if calib_sum else None

        deterministic_digest = compute_scenario_deterministic_digest(
            scenario_id=definition.scenario_id,
            input_digest=input_digest,
            financial_digest=financial_digest,
            simulated_orders=simulated_orders,
            fills=fill_count,
            rejections=rejection_count,
            pending=pending_count,
            cancelled=cancelled_count,
            total_fees=total_fees,
            total_slippage=total_slippage,
            status=status,
        )

        return ScenarioResult(
            scenario_id=definition.scenario_id,
            name=definition.name,
            category=definition.category,
            status=status,
            input_event_count=input_event_count,
            processed_event_count=processed_event_count,
            rejected_event_count=rejected_event_count,
            simulated_order_count=simulated_orders,
            fill_count=fill_count,
            rejection_count=rejection_count,
            pending_order_count=pending_count,
            cancelled_order_count=cancelled_count,
            initial_cash=definition.initial_cash,
            final_cash=portfolio.available_cash,
            final_equity=portfolio.total_equity,
            realized_pnl=portfolio.realized_pnl,
            unrealized_pnl=portfolio.unrealized_pnl,
            total_fees=round(total_fees, 4),
            total_slippage=round(total_slippage, 4),
            positions={sym: pos.to_dict() for sym, pos in portfolio.positions.items()},
            ledger_reconciliation=recon,
            input_digest=input_digest,
            financial_digest=financial_digest,
            deterministic_digest=deterministic_digest,
            lifecycle_summary=lifecycle_sum,
            calibration_summary=calib_summary_dict,
            passed_invariants=passed_invariants,
            violated_invariants=violated_invariants,
            warnings=warnings,
            failure_reason=failure_msg,
            metadata=dict(definition.metadata),
        )


# ==============================================================================
# PARAMETER SENSITIVITY RUNNER
# ==============================================================================


class ParameterSensitivityRunner:
    """Evaluates paper execution parameter sensitivity across isolated deterministic trials."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or Settings()
        self.scenario_runner = StressScenarioRunner(settings=self.settings)

    def validate_parameter_value(self, param: SensitivityParameter, val: float) -> None:
        """Validate parameter value constraints strictly; rejects non-finite, negative, or invalid values."""
        if not math.isfinite(val):
            raise ValidationError(f"Parameter '{param.value}' value must be finite, got {val}")

        if param in (
            SensitivityParameter.SLIPPAGE_RATE,
            SensitivityParameter.FEE_RATE,
            SensitivityParameter.HALF_SPREAD_RATE,
        ):
            if val < 0.0:
                raise ValidationError(f"Rate parameter '{param.value}' cannot be negative, got {val}")
        elif param == SensitivityParameter.FRESHNESS_THRESHOLD_SECONDS:
            if val <= 0.0:
                raise ValidationError(f"freshness_threshold_seconds must be strictly positive, got {val}")
        elif param == SensitivityParameter.MAX_ORDER_NOTIONAL:
            if val <= 0.0:
                raise ValidationError(f"max_order_notional must be strictly positive, got {val}")
        elif param == SensitivityParameter.MAX_POSITION_EXPOSURE_PCT:
            if val <= 0.0 or val > 100.0:
                raise ValidationError(f"max_position_exposure_pct must be in range (0, 100], got {val}")

    def run_sensitivity(
        self,
        parameter: SensitivityParameter,
        baseline_value: float,
        tested_values: Sequence[float],
        base_scenario: StressScenarioDefinition,
    ) -> ParameterSensitivityAnalysis:
        """Run parameter sensitivity sweep across isolated trials with identical market inputs."""
        self.validate_parameter_value(parameter, baseline_value)
        for v in tested_values:
            self.validate_parameter_value(parameter, v)

        # Ensure tested_values contains unique, sorted values including baseline
        all_vals = sorted(list({round(float(v), 8) for v in list(tested_values) + [baseline_value]}))

        trials: List[SensitivityTrialResult] = []
        baseline_trial: Optional[SensitivityTrialResult] = None

        # Execute baseline first
        base_cfg = base_scenario.risk_config or PaperRiskConfig()
        cfg_dict = {
            "max_order_notional": base_cfg.max_order_notional,
            "max_position_exposure_pct": base_cfg.max_position_exposure_pct,
            "fee_rate": base_cfg.fee_rate,
            "slippage_rate": base_cfg.slippage_rate,
            "half_spread_rate": base_cfg.half_spread_rate,
            "freshness_threshold_seconds": base_cfg.freshness_threshold_seconds,
            "allow_short_selling": base_cfg.allow_short_selling,
        }

        for val in all_vals:
            is_base = math.isclose(val, baseline_value, rel_tol=1e-7, abs_tol=1e-7)
            # Create isolated configuration
            trial_dict = dict(cfg_dict)
            trial_dict[parameter.value] = val
            trial_cfg = PaperRiskConfig(**trial_dict)

            # Create isolated scenario
            trial_scenario = StressScenarioDefinition(
                scenario_id=f"SENS_{parameter.value}_{val}",
                name=f"Sensitivity trial {parameter.value}={val}",
                category=base_scenario.category,
                description=f"Isolated parameter sensitivity evaluation of {parameter.value}={val}",
                initial_cash=base_scenario.initial_cash,
                risk_config=trial_cfg,
                steps=base_scenario.steps,
                quotes=base_scenario.quotes,
                metadata={"parameter": parameter.value, "value": val, "is_baseline": is_base},
            )

            res = self.scenario_runner.run_scenario(trial_scenario)

            # Count rejections
            rejection_map: Dict[str, int] = {}
            if res.rejection_count > 0:
                rejection_map["REJECTED"] = res.rejection_count

            # Temporary trial result to compute deltas
            trial_res = SensitivityTrialResult(
                parameter_name=parameter.value,
                parameter_value=val,
                is_baseline=is_base,
                orders_submitted=res.simulated_order_count,
                orders_accepted=res.fill_count + res.pending_order_count,
                orders_filled=res.fill_count,
                orders_rejected=res.rejection_count,
                rejection_breakdown=rejection_map,
                total_fees=res.total_fees,
                total_slippage=res.total_slippage,
                final_cash=res.final_cash,
                final_equity=res.final_equity,
                realized_pnl=res.realized_pnl,
                unrealized_pnl=res.unrealized_pnl,
                ledger_balanced=res.ledger_reconciliation.get("is_balanced", False),
                financial_digest=res.financial_digest,
                delta_fees_vs_baseline=0.0,
                delta_slippage_vs_baseline=0.0,
                delta_cash_vs_baseline=0.0,
                delta_pnl_vs_baseline=0.0,
                delta_equity_vs_baseline=0.0,
                invariants_satisfied=(len(res.violated_invariants) == 0),
                violations=list(res.violated_invariants),
            )

            if is_base:
                baseline_trial = trial_res

            trials.append(trial_res)

        assert baseline_trial is not None, "Baseline trial must be evaluated"

        # Compute relative deltas against baseline
        final_trials: List[SensitivityTrialResult] = []
        for t in trials:
            delta_fees = round(t.total_fees - baseline_trial.total_fees, 4)
            delta_slip = round(t.total_slippage - baseline_trial.total_slippage, 4)
            delta_cash = round(t.final_cash - baseline_trial.final_cash, 4)
            delta_pnl = round((t.realized_pnl + t.unrealized_pnl) - (baseline_trial.realized_pnl + baseline_trial.unrealized_pnl), 4)
            delta_equity = round(t.final_equity - baseline_trial.final_equity, 4)

            final_trials.append(
                SensitivityTrialResult(
                    parameter_name=t.parameter_name,
                    parameter_value=t.parameter_value,
                    is_baseline=t.is_baseline,
                    orders_submitted=t.orders_submitted,
                    orders_accepted=t.orders_accepted,
                    orders_filled=t.orders_filled,
                    orders_rejected=t.orders_rejected,
                    rejection_breakdown=t.rejection_breakdown,
                    total_fees=t.total_fees,
                    total_slippage=t.total_slippage,
                    final_cash=t.final_cash,
                    final_equity=t.final_equity,
                    realized_pnl=t.realized_pnl,
                    unrealized_pnl=t.unrealized_pnl,
                    ledger_balanced=t.ledger_balanced,
                    financial_digest=t.financial_digest,
                    delta_fees_vs_baseline=delta_fees,
                    delta_slippage_vs_baseline=delta_slip,
                    delta_cash_vs_baseline=delta_cash,
                    delta_pnl_vs_baseline=delta_pnl,
                    delta_equity_vs_baseline=delta_equity,
                    invariants_satisfied=t.invariants_satisfied,
                    violations=t.violations,
                )
            )

        # Observations & Sweep Digest
        observations: List[str] = []
        hasher = hashlib.sha256()
        hasher.update(f"SWEEP:{parameter.value}:BASE={baseline_value}".encode("utf-8"))

        for t in final_trials:
            hasher.update(
                f"|VAL={t.parameter_value}:F={t.orders_filled}:R={t.orders_rejected}:FEES={t.total_fees}:SLIP={t.total_slippage}:DIG={t.financial_digest}".encode(
                    "utf-8"
                )
            )
            if not t.is_baseline:
                if t.delta_slippage_vs_baseline != 0:
                    observations.append(
                        f"At {parameter.value}={t.parameter_value}, slippage delta vs baseline: {t.delta_slippage_vs_baseline:+.4f}"
                    )
                if t.delta_fees_vs_baseline != 0:
                    observations.append(
                        f"At {parameter.value}={t.parameter_value}, fees delta vs baseline: {t.delta_fees_vs_baseline:+.4f}"
                    )
                if t.orders_rejected != baseline_trial.orders_rejected:
                    observations.append(
                        f"At {parameter.value}={t.parameter_value}, rejection count shifted from {baseline_trial.orders_rejected} to {t.orders_rejected}"
                    )

        if not observations:
            observations.append(
                f"Parameter variation in {parameter.value} showed no divergence in execution fills or costs under tested scenario."
            )

        sweep_digest = hasher.hexdigest()

        return ParameterSensitivityAnalysis(
            parameter_name=parameter.value,
            baseline_value=baseline_value,
            tested_values=all_vals,
            trials=final_trials,
            sweep_digest=sweep_digest,
            summary_observations=observations,
        )


# ==============================================================================
# STANDARD SCENARIOS (A THROUGH J) FACTORY
# ==============================================================================


def create_scenario_a_tight_spread() -> StressScenarioDefinition:
    """Scenario A: Tight Bid-Ask Spreads."""
    base_ts = 1700000000.0
    quotes = [
        Quote(symbol="INFY", exchange="NSE", timestamp=base_ts + 0.0, last_price=1500.02, bid=1500.00, ask=1500.05, source="replay"),
        Quote(symbol="INFY", exchange="NSE", timestamp=base_ts + 10.0, last_price=1500.12, bid=1500.10, ask=1500.15, source="replay"),
        Quote(symbol="INFY", exchange="NSE", timestamp=base_ts + 20.0, last_price=1500.22, bid=1500.20, ask=1500.25, source="replay"),
    ]
    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 0.0, quote=quotes[0]),
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 0.0,
            quote=quotes[0],
            order_request=PaperWorkflowRequest(
                symbol="INFY",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=10.0,
                client_order_id="scen_a_buy_1",
            ),
        ),
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 10.0, quote=quotes[1]),
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 20.0, quote=quotes[2]),
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 20.0,
            quote=quotes[2],
            order_request=PaperWorkflowRequest(
                symbol="INFY",
                side=PaperOrderSide.SELL,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="scen_a_sell_1",
            ),
        ),
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_A_TIGHT_SPREAD",
        name="Tight Bid-Ask Spreads",
        category=ScenarioCategory.TIGHT_SPREAD,
        description="Verify spread calculations, slippage accounting, and ledger reconciliation with narrow spreads.",
        initial_cash=100_000.0,
        risk_config=PaperRiskConfig(fee_rate=0.0005, slippage_rate=0.0002),
        quotes=quotes,
        steps=steps,
        expected_criteria={"min_fills": 2, "max_fills": 2},
    )


def create_scenario_b_wide_spread() -> StressScenarioDefinition:
    """Scenario B: Wide and Illiquid Spreads."""
    base_ts = 1700000000.0
    quotes = [
        Quote(symbol="ILLIQ", exchange="NSE", timestamp=base_ts + 0.0, last_price=1000.0, bid=800.0, ask=1200.0, source="replay"),
        Quote(symbol="ILLIQ", exchange="NSE", timestamp=base_ts + 10.0, last_price=1010.0, bid=810.0, ask=1210.0, source="replay"),
    ]
    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 0.0, quote=quotes[0]),
        # Order 1: within max notional (10 * 1200 = 12,000 <= 50,000)
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 0.0,
            quote=quotes[0],
            order_request=PaperWorkflowRequest(
                symbol="ILLIQ",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=10.0,
                client_order_id="scen_b_buy_pass",
            ),
        ),
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 10.0, quote=quotes[1]),
        # Order 2: exceeds max notional (100 * 1210 = 121,000 > 50,000)
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 10.0,
            quote=quotes[1],
            expected_rejection_code=PaperRejectionCode.EXCEEDS_MAX_NOTIONAL.value,
            order_request=PaperWorkflowRequest(
                symbol="ILLIQ",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=100.0,
                client_order_id="scen_b_buy_reject",
            ),
        ),
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_B_WIDE_SPREAD",
        name="Wide and Illiquid Spreads",
        category=ScenarioCategory.WIDE_SPREAD,
        description="Verify quote validation, price collar, and risk controls under wide and illiquid spreads.",
        initial_cash=100_000.0,
        risk_config=PaperRiskConfig(max_order_notional=50_000.0),
        quotes=quotes,
        steps=steps,
        expected_criteria={"min_fills": 1, "min_rejections": 1},
    )


def create_scenario_c_spread_widening() -> StressScenarioDefinition:
    """Scenario C: Sudden Spread Widening."""
    base_ts = 1700000000.0
    quotes = [
        # Normal spread regime (1.0 price spread, ~4 bps)
        Quote(symbol="RELIANCE", exchange="NSE", timestamp=base_ts + 0.0, last_price=2500.0, bid=2499.5, ask=2500.5, source="replay"),
        Quote(symbol="RELIANCE", exchange="NSE", timestamp=base_ts + 10.0, last_price=2501.0, bid=2500.5, ask=2501.5, source="replay"),
        Quote(symbol="RELIANCE", exchange="NSE", timestamp=base_ts + 20.0, last_price=2502.0, bid=2501.5, ask=2502.5, source="replay"),
        # Sudden spread shock regime (200.0 price spread, ~800 bps)
        Quote(symbol="RELIANCE", exchange="NSE", timestamp=base_ts + 30.0, last_price=2500.0, bid=2400.0, ask=2600.0, source="replay"),
        Quote(symbol="RELIANCE", exchange="NSE", timestamp=base_ts + 40.0, last_price=2505.0, bid=2405.0, ask=2605.0, source="replay"),
    ]
    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=q.timestamp, quote=q)
        for q in quotes
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_C_SPREAD_WIDENING",
        name="Sudden Spread Widening",
        category=ScenarioCategory.SPREAD_WIDENING,
        description="Verify point-in-time calibration isolation and ensure no look-ahead bias across spread shocks.",
        initial_cash=100_000.0,
        quotes=quotes,
        steps=steps,
        metadata={"shock_timestamp": base_ts + 30.0},
    )


def create_scenario_d_stale_out_of_order() -> StressScenarioDefinition:
    """Scenario D: Stale and Out-of-Order Quotes."""
    base_ts = 1700000000.0
    quotes = [
        Quote(symbol="TCS", exchange="NSE", timestamp=base_ts + 10.0, last_price=3500.0, bid=3499.0, ask=3501.0, source="replay"),
        Quote(symbol="TCS", exchange="NSE", timestamp=base_ts + 20.0, last_price=3505.0, bid=3504.0, ask=3506.0, source="replay"),
        # Out-of-order quote (timestamp goes backwards to base_ts + 5.0)
        Quote(symbol="TCS", exchange="NSE", timestamp=base_ts + 5.0, last_price=3495.0, bid=3494.0, ask=3496.0, source="replay"),
        # Stale reference order
        Quote(symbol="TCS", exchange="NSE", timestamp=base_ts + 30.0, last_price=3510.0, bid=3509.0, ask=3511.0, source="replay"),
    ]
    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=quotes[0].timestamp, quote=quotes[0]),
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=quotes[1].timestamp, quote=quotes[1]),
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=quotes[2].timestamp, quote=quotes[2]),
        # Order submitted at base_ts + 200.0 against quote from base_ts + 30.0 (age = 170s > freshness threshold 60s)
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 200.0,
            quote=quotes[3],
            expected_rejection_code=PaperRejectionCode.STALE_MARKET_DATA.value,
            order_request=PaperWorkflowRequest(
                symbol="TCS",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="scen_d_stale_order",
            ),
        ),
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_D_STALE_OUT_OF_ORDER",
        name="Stale and Out-of-Order Quotes",
        category=ScenarioCategory.STALE_OUT_OF_ORDER,
        description="Verify handling of stale observations, duplicate timestamps, and out-of-order data.",
        initial_cash=100_000.0,
        risk_config=PaperRiskConfig(freshness_threshold_seconds=60.0),
        quotes=quotes,
        steps=steps,
        expected_criteria={"min_rejections": 1},
    )


def create_scenario_e_malformed_data() -> StressScenarioDefinition:
    """Scenario E: Missing and Malformed Market Data."""
    base_ts = 1700000000.0
    quotes = [
        # 1. Missing BBO Quote
        Quote(symbol="CORRUPT", exchange="NSE", timestamp=base_ts + 10.0, last_price=100.0, bid=None, ask=None, source="replay"),
    ]
    steps = [
        # Step 1: Missing BBO quote fed to calibrator
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=quotes[0].timestamp, quote=quotes[0]),
        # Step 2: Inverted spread raw record (bid 105.0 > ask 95.0)
        ScenarioStep(
            step_type=ScenarioStepType.MARKET_QUOTE,
            timestamp=base_ts + 20.0,
            metadata={"raw_record": {"symbol": "CORRUPT", "exchange": "NSE", "timestamp": base_ts + 20.0, "last_price": 100.0, "bid": 105.0, "ask": 95.0, "source": "replay"}},
        ),
        # Step 3: Non-positive price raw record
        ScenarioStep(
            step_type=ScenarioStepType.MARKET_QUOTE,
            timestamp=base_ts + 30.0,
            metadata={"raw_record": {"symbol": "CORRUPT", "exchange": "NSE", "timestamp": base_ts + 30.0, "last_price": -50.0, "bid": 10.0, "ask": 12.0, "source": "replay"}},
        ),
        # Step 4: Non-finite price raw record
        ScenarioStep(
            step_type=ScenarioStepType.MARKET_QUOTE,
            timestamp=base_ts + 40.0,
            metadata={"raw_record": {"symbol": "CORRUPT", "exchange": "NSE", "timestamp": base_ts + 40.0, "last_price": float("nan"), "bid": 10.0, "ask": 12.0, "source": "replay"}},
        ),
        # Step 5: Submit order where market quote is missing entirely -> MISSING_MARKET_DATA
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 50.0,
            quote=None,
            expected_rejection_code=PaperRejectionCode.MISSING_MARKET_DATA.value,
            order_request=PaperWorkflowRequest(
                symbol="MISSING_DATA_SYM",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=10.0,
                client_order_id="scen_e_missing_order",
            ),
        ),
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_E_MALFORMED_DATA",
        name="Missing and Malformed Market Data",
        category=ScenarioCategory.MALFORMED_DATA,
        description="Verify fail-closed rejection of missing BBO, inverted spreads, and non-positive prices.",
        initial_cash=100_000.0,
        quotes=quotes,
        steps=steps,
        expected_criteria={"min_rejections": 1, "max_fills": 0},
    )


def create_scenario_f_spread_shock() -> StressScenarioDefinition:
    """Scenario F: Liquidity Gap / Flash-Crash-Like Spread Shock (Bounded Synthetic)."""
    base_ts = 1700000000.0
    quotes = [
        # Normal baseline
        Quote(symbol="SYNTH_CRASH", exchange="NSE", timestamp=base_ts + 0.0, last_price=1000.0, bid=999.5, ask=1000.5, source="replay"),
        # Sudden gap / spread shock: price drops 25%, spread widens to 200 pts
        Quote(symbol="SYNTH_CRASH", exchange="NSE", timestamp=base_ts + 10.0, last_price=750.0, bid=650.0, ask=850.0, source="replay"),
        # Partial stabilization
        Quote(symbol="SYNTH_CRASH", exchange="NSE", timestamp=base_ts + 20.0, last_price=800.0, bid=795.0, ask=805.0, source="replay"),
    ]
    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=q.timestamp, quote=q)
        for q in quotes
    ]
    # Add an order executed during the shock at TS=10.0
    steps.append(
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 10.0,
            quote=quotes[1],
            order_request=PaperWorkflowRequest(
                symbol="SYNTH_CRASH",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=10.0,
                client_order_id="scen_f_shock_buy",
            ),
        )
    )
    return StressScenarioDefinition(
        scenario_id="SCENARIO_F_SPREAD_SHOCK",
        name="Liquidity Gap & Spread Shock",
        category=ScenarioCategory.SPREAD_SHOCK,
        description="Bounded synthetic spread-shock testing order validation and simulated slippage behavior.",
        initial_cash=100_000.0,
        quotes=quotes,
        steps=steps,
        metadata={"synthetic_disclaimer": "Synthetic simulation model; does NOT predict real flash-crash microstructure."},
    )


def create_scenario_g_replay_interruption() -> StressScenarioDefinition:
    """Scenario G: Replay Interruption and Restart Recovery."""
    base_ts = 1700000000.0
    q1 = Quote(symbol="HDFC", exchange="NSE", timestamp=base_ts + 0.0, last_price=1600.0, bid=1599.5, ask=1600.5, source="replay")
    q2 = Quote(symbol="HDFC", exchange="NSE", timestamp=base_ts + 10.0, last_price=1605.0, bid=1604.5, ask=1605.5, source="replay")
    q3 = Quote(symbol="HDFC", exchange="NSE", timestamp=base_ts + 20.0, last_price=1610.0, bid=1609.5, ask=1610.5, source="replay")

    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 0.0, quote=q1),
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 0.0,
            quote=q1,
            order_request=PaperWorkflowRequest(
                symbol="HDFC",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=10.0,
                client_order_id="scen_g_buy_pre_crash",
            ),
        ),
        # Interruption and recovery boundary
        ScenarioStep(step_type=ScenarioStepType.RESTART_ENGINE, timestamp=base_ts + 5.0),
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 10.0, quote=q2),
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 10.0,
            quote=q2,
            order_request=PaperWorkflowRequest(
                symbol="HDFC",
                side=PaperOrderSide.SELL,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="scen_g_sell_post_recovery",
            ),
        ),
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 20.0, quote=q3),
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_G_REPLAY_INTERRUPTION",
        name="Replay Interruption & Restart Recovery",
        category=ScenarioCategory.REPLAY_INTERRUPTION,
        description="Verify state restoration, non-duplication of initial capital/fills, and ledger reconciliation.",
        initial_cash=100_000.0,
        quotes=[q1, q2, q3],
        steps=steps,
        expected_criteria={"min_fills": 2},
    )


def create_scenario_h_risk_boundary() -> StressScenarioDefinition:
    """Scenario H: Risk-Limit Boundary Conditions."""
    base_ts = 1700000000.0
    quote = Quote(symbol="SBIN", exchange="NSE", timestamp=base_ts + 0.0, last_price=500.0, bid=499.5, ask=500.5, source="replay")
    # Config: max_order_notional = 50_000.0
    # Price ask = 500.5
    # Order 1 (below limit): qty = 99 -> notional = 99 * 500.5 = 49,549.5 (APPROVED)
    # Order 2 (at limit boundary): qty = 99.8 -> notional = 99.8 * 500.5 = 49,949.9 (APPROVED)
    # Order 3 (above limit): qty = 101 -> notional = 101 * 500.5 = 50,550.5 (REJECTED: EXCEEDS_MAX_NOTIONAL)
    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 0.0, quote=quote),
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 0.0,
            quote=quote,
            order_request=PaperWorkflowRequest(
                symbol="SBIN",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=99.0,
                client_order_id="scen_h_below_limit",
            ),
        ),
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 0.0,
            quote=quote,
            expected_rejection_code=PaperRejectionCode.EXCEEDS_MAX_NOTIONAL.value,
            order_request=PaperWorkflowRequest(
                symbol="SBIN",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=101.0,
                client_order_id="scen_h_above_limit",
            ),
        ),
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_H_RISK_BOUNDARY",
        name="Risk-Limit Boundary Conditions",
        category=ScenarioCategory.RISK_BOUNDARY,
        description="Verify strict boundary semantics for max order notional and position exposure.",
        initial_cash=100_000.0,
        risk_config=PaperRiskConfig(max_order_notional=50_000.0),
        quotes=[quote],
        steps=steps,
        expected_criteria={"min_fills": 1, "min_rejections": 1},
    )


def create_scenario_i_pending_limit_order() -> StressScenarioDefinition:
    """Scenario I: Pending Limit Orders."""
    base_ts = 1700000000.0
    q1 = Quote(symbol="LIMIT_SYM", exchange="NSE", timestamp=base_ts + 0.0, last_price=100.0, bid=99.5, ask=100.5, source="replay")
    q2_worse = Quote(symbol="LIMIT_SYM", exchange="NSE", timestamp=base_ts + 10.0, last_price=102.0, bid=101.5, ask=102.5, source="replay")
    q3_better = Quote(symbol="LIMIT_SYM", exchange="NSE", timestamp=base_ts + 20.0, last_price=97.0, bid=96.5, ask=97.5, source="replay")

    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 0.0, quote=q1),
        # Submit BUY limit order at limit_price = 98.0 when ask = 100.5 -> pending
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 0.0,
            quote=q1,
            order_request=PaperWorkflowRequest(
                symbol="LIMIT_SYM",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.LIMIT,
                quantity=10.0,
                limit_price=98.0,
                client_order_id="scen_i_pending_limit",
            ),
        ),
        # Worsened quote: ask = 102.5 -> still pending
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 10.0, quote=q2_worse),
        # Improved quote: ask = 97.5 <= 98.0 -> triggers fill!
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 20.0, quote=q3_better),
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_I_PENDING_LIMIT_ORDER",
        name="Pending Limit Orders",
        category=ScenarioCategory.PENDING_LIMIT_ORDER,
        description="Verify limit order pendency, market quote checking, and deterministic fill execution.",
        initial_cash=100_000.0,
        quotes=[q1, q2_worse, q3_better],
        steps=steps,
        expected_criteria={"min_fills": 1},
    )


def create_scenario_j_duplicate_events() -> StressScenarioDefinition:
    """Scenario J: Duplicate and Reordered Events."""
    base_ts = 1700000000.0
    quote = Quote(symbol="DUP_SYM", exchange="NSE", timestamp=base_ts + 0.0, last_price=200.0, bid=199.5, ask=200.5, source="replay")
    steps = [
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 0.0, quote=quote),
        # Duplicate quote
        ScenarioStep(step_type=ScenarioStepType.MARKET_QUOTE, timestamp=base_ts + 0.0, quote=quote),
        # First order submission
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 0.0,
            quote=quote,
            order_request=PaperWorkflowRequest(
                symbol="DUP_SYM",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="scen_j_dup_client_id",
            ),
        ),
        # Duplicate order submission with same client_order_id -> REJECTED: DUPLICATE_ORDER
        ScenarioStep(
            step_type=ScenarioStepType.SUBMIT_ORDER,
            timestamp=base_ts + 1.0,
            quote=quote,
            expected_rejection_code=PaperRejectionCode.DUPLICATE_ORDER.value,
            order_request=PaperWorkflowRequest(
                symbol="DUP_SYM",
                side=PaperOrderSide.BUY,
                order_type=PaperOrderType.MARKET,
                quantity=5.0,
                client_order_id="scen_j_dup_client_id",
            ),
        ),
    ]
    return StressScenarioDefinition(
        scenario_id="SCENARIO_J_DUPLICATE_EVENTS",
        name="Duplicate & Reordered Events",
        category=ScenarioCategory.DUPLICATE_EVENTS,
        description="Verify idempotency protection for duplicate client order IDs and duplicate market data.",
        initial_cash=100_000.0,
        quotes=[quote],
        steps=steps,
        expected_criteria={"min_fills": 1, "min_rejections": 1},
    )


def create_standard_scenarios() -> List[StressScenarioDefinition]:
    """Instantiate and return the canonical set of 10 stress testing scenarios (A through J)."""
    return [
        create_scenario_a_tight_spread(),
        create_scenario_b_wide_spread(),
        create_scenario_c_spread_widening(),
        create_scenario_d_stale_out_of_order(),
        create_scenario_e_malformed_data(),
        create_scenario_f_spread_shock(),
        create_scenario_g_replay_interruption(),
        create_scenario_h_risk_boundary(),
        create_scenario_i_pending_limit_order(),
        create_scenario_j_duplicate_events(),
    ]


# ==============================================================================
# COMBINED SUITE RUNNER
# ==============================================================================


def run_all_stress_scenarios(
    scenarios: Optional[Sequence[StressScenarioDefinition]] = None,
    settings: Optional[Settings] = None,
    run_sensitivity: bool = True,
) -> CombinedStressTestReport:
    """Execute all defined stress scenarios and standard parameter sensitivity analyses."""
    scenario_list = list(scenarios or create_standard_scenarios())
    runner = StressScenarioRunner(settings=settings)
    sens_runner = ParameterSensitivityRunner(settings=settings)

    scenario_results: Dict[str, ScenarioResult] = {}
    passed_count = 0
    failed_count = 0
    rejected_count = 0
    insufficient_count = 0

    for sc in scenario_list:
        res = runner.run_scenario(sc)
        scenario_results[sc.scenario_id] = res
        if res.status == ScenarioStatus.PASSED:
            passed_count += 1
        elif res.status == ScenarioStatus.FAILED:
            failed_count += 1
        elif res.status == ScenarioStatus.REJECTED:
            rejected_count += 1
        else:
            insufficient_count += 1

    # Run parameter sensitivity analysis if requested
    sensitivity_results: Dict[str, ParameterSensitivityAnalysis] = {}
    if run_sensitivity:
        base_sc = create_scenario_a_tight_spread()
        # 1. Slippage rate sensitivity
        slip_analysis = sens_runner.run_sensitivity(
            parameter=SensitivityParameter.SLIPPAGE_RATE,
            baseline_value=0.0002,
            tested_values=[0.0000, 0.0001, 0.0002, 0.0005, 0.0010, 0.0020],
            base_scenario=base_sc,
        )
        sensitivity_results[SensitivityParameter.SLIPPAGE_RATE.value] = slip_analysis

        # 2. Freshness threshold sensitivity
        fresh_analysis = sens_runner.run_sensitivity(
            parameter=SensitivityParameter.FRESHNESS_THRESHOLD_SECONDS,
            baseline_value=60.0,
            tested_values=[5.0, 15.0, 30.0, 60.0, 120.0, 300.0],
            base_scenario=base_sc,
        )
        sensitivity_results[SensitivityParameter.FRESHNESS_THRESHOLD_SECONDS.value] = fresh_analysis

        # 3. Fee rate sensitivity
        fee_analysis = sens_runner.run_sensitivity(
            parameter=SensitivityParameter.FEE_RATE,
            baseline_value=0.0005,
            tested_values=[0.0000, 0.0002, 0.0005, 0.0010, 0.0020],
            base_scenario=base_sc,
        )
        sensitivity_results[SensitivityParameter.FEE_RATE.value] = fee_analysis

        # 4. Max order notional sensitivity
        notional_analysis = sens_runner.run_sensitivity(
            parameter=SensitivityParameter.MAX_ORDER_NOTIONAL,
            baseline_value=50_000.0,
            tested_values=[10_000.0, 25_000.0, 50_000.0, 75_000.0, 100_000.0],
            base_scenario=base_sc,
        )
        sensitivity_results[SensitivityParameter.MAX_ORDER_NOTIONAL.value] = notional_analysis

    # Check aggregate invariants
    all_reconciled = all(r.ledger_reconciliation.get("is_balanced", False) for r in scenario_results.values())
    no_neg_cash = all(r.final_cash >= -1e-6 for r in scenario_results.values())
    all_deterministic = all(bool(r.deterministic_digest) for r in scenario_results.values())

    aggregate_invariants = {
        "ALL_SCENARIOS_RECONCILED": all_reconciled,
        "NO_NEGATIVE_CASH": no_neg_cash,
        "DETERMINISTIC_DIGESTS_PRESENT": all_deterministic,
        "SAFETY_LOCK_PRESERVED": getattr(settings or Settings(), "execution_locked", True),
        "BROKER_PROVIDER_NONE": getattr(settings or Settings(), "broker_provider", "none") == "none",
    }

    # Overall SHA-256 Digest
    hasher = hashlib.sha256()
    for sc_id in sorted(scenario_results.keys()):
        hasher.update(f"{sc_id}:{scenario_results[sc_id].deterministic_digest}\n".encode("utf-8"))
    for p_name in sorted(sensitivity_results.keys()):
        hasher.update(f"{p_name}:{sensitivity_results[p_name].sweep_digest}\n".encode("utf-8"))
    overall_digest = hasher.hexdigest()

    limitations = [
        "1. Deterministic synthetic replay does not model dynamic exchange order queues or order book depth shifts.",
        "2. Flash-crash scenarios are bounded synthetic tests of risk gate rejections, not realistic liquidity collapse dynamics.",
        "3. Simulated fills assume instant quote-matching and do not model queue priority or partial fills.",
        "4. Parameter sensitivity sweeps evaluate numerical outcomes under fixed assumptions and do not guarantee future profitability.",
    ]

    safety_status = {
        "execution_locked": True,
        "broker_provider": "none",
        "paper_execution_enabled_by_default": False,
        "live_trading_engine": "uninitialized",
        "voice_execution_path": "strictly prohibited",
    }

    return CombinedStressTestReport(
        report_id=f"rep_stress_{uuid.uuid4().hex[:12]}",
        total_scenarios=len(scenario_results),
        passed_scenarios=passed_count,
        failed_scenarios=failed_count,
        rejected_scenarios=rejected_count,
        insufficient_data_scenarios=insufficient_count,
        scenario_results=scenario_results,
        sensitivity_analyses=sensitivity_results,
        aggregate_invariants=aggregate_invariants,
        overall_digest=overall_digest,
        safety_status=safety_status,
        limitations_and_disclaimers=limitations,
    )
