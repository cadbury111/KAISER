"""KAISER — Strategy Robustness & Stress Testing Domain Models.

Provides strongly typed, immutable data structures for evaluating strategy sensitivity,
transaction-cost stress testing, parameter perturbation analysis, historical window stability,
and comparative metric degradation under adverse market conditions.

SAFETY INVARIANTS:
1. Research and evaluation only: Evaluates hypothetical historical performance under stress.
   Never executes real trades or connects to a live broker.
2. Anti-leakage and anti-look-ahead: Stress scenarios use only permitted historical data.
3. No profitability claims: Stress testing demonstrates sensitivity and fragility;
   it cannot prove future profitability or eliminate trading risks.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from app.analysis.models import normalize_timeframe
from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    ExecutionModel,
    FeeModel,
    PerformanceMetrics,
    PositionSizeType,
)
from app.core.exceptions import ValidationError


class RobustnessStatus(str, Enum):
    """Lifecycle status of a robustness / stress-testing session."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class ScenarioType(str, Enum):
    """Classification of stress scenario."""

    BASELINE = "baseline"
    COMMISSION_STRESS = "commission_stress"
    SPREAD_STRESS = "spread_stress"
    SLIPPAGE_STRESS = "slippage_stress"
    COMBINED_COST_STRESS = "combined_cost_stress"
    PARAMETER_SENSITIVITY = "parameter_sensitivity"
    HISTORICAL_WINDOW = "historical_window"
    CUSTOM = "custom"


@dataclass(frozen=True)
class RobustnessWarning:
    """Structured warning emitted during robustness analysis.

    Attributes:
        code: Machine-readable warning category.
        message: Explanatory diagnostic text.
        scenario_name: Optional name of the scenario where warning occurred.
        severity: Severity classification ('INFO', 'WARNING', 'CRITICAL').
    """

    code: str
    message: str
    scenario_name: Optional[str] = None
    severity: str = "WARNING"

    def to_dict(self) -> Dict[str, Any]:
        """Convert warning to serializable dictionary."""
        return {
            "code": self.code,
            "message": self.message,
            "scenario_name": self.scenario_name,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class StressScenario:
    """Explicitly configured hypothetical stress scenario.

    Attributes:
        name: Unique identifier for this scenario (e.g. '2x_commission', 'wide_spread_3x').
        description: Human-readable explanation of the adverse assumption.
        scenario_type: Type classification from ScenarioType.
        fee_rate: Optional commission rate override.
        fixed_fee: Optional fixed dollar fee override.
        slippage_bps: Optional slippage basis points override.
        half_spread_bps: Optional half-spread basis points override.
        parameter_overrides: Strategy parameter overrides (for parameter sensitivity).
        window_start: Optional historical window start timestamp.
        window_end: Optional historical window end timestamp.
        metadata: Additional scenario context or tags.
    """

    name: str
    description: str
    scenario_type: str = ScenarioType.COST_STRESS.value if hasattr(ScenarioType, "COST_STRESS") else "cost_stress"
    fee_rate: Optional[float] = None
    fixed_fee: Optional[float] = None
    slippage_bps: Optional[float] = None
    half_spread_bps: Optional[float] = None
    parameter_overrides: Dict[str, Any] = field(default_factory=dict)
    window_start: Optional[float] = None
    window_end: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name or not isinstance(self.name, str) or not self.name.strip():
            raise ValidationError("StressScenario name must be a non-empty string.")
        object.__setattr__(self, "name", self.name.strip())

        if self.fee_rate is not None and self.fee_rate < 0:
            raise ValidationError(f"StressScenario fee_rate cannot be negative: {self.fee_rate}")
        if self.fixed_fee is not None and self.fixed_fee < 0:
            raise ValidationError(f"StressScenario fixed_fee cannot be negative: {self.fixed_fee}")
        if self.slippage_bps is not None and self.slippage_bps < 0:
            raise ValidationError(f"StressScenario slippage_bps cannot be negative: {self.slippage_bps}")
        if self.half_spread_bps is not None and self.half_spread_bps < 0:
            raise ValidationError(f"StressScenario half_spread_bps cannot be negative: {self.half_spread_bps}")

        if self.window_start is not None and self.window_end is not None:
            if self.window_start >= self.window_end:
                raise ValidationError("window_start must be strictly less than window_end.")

        object.__setattr__(self, "parameter_overrides", dict(self.parameter_overrides or {}))
        object.__setattr__(self, "metadata", dict(self.metadata or {}))

    def to_dict(self) -> Dict[str, Any]:
        """Convert scenario to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "scenario_type": self.scenario_type,
            "fee_rate": self.fee_rate,
            "fixed_fee": self.fixed_fee,
            "slippage_bps": self.slippage_bps,
            "half_spread_bps": self.half_spread_bps,
            "parameter_overrides": dict(self.parameter_overrides),
            "window_start": self.window_start,
            "window_end": self.window_end,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class StressScenarioResult:
    """Outcome report for an individual stress scenario run compared to baseline.

    Attributes:
        scenario: The StressScenario evaluated.
        status: Run status ('COMPLETED', 'FAILED', 'SKIPPED').
        backtest_result: Full BacktestResult artifact from the run.
        delta_net_pnl: Change in net PnL versus baseline (scenario - baseline).
        delta_return_pct: Change in percentage return versus baseline (scenario - baseline).
        delta_max_drawdown_pct: Change in maximum drawdown versus baseline.
        delta_trades: Change in total trades versus baseline.
        delta_win_rate: Change in win rate versus baseline.
        delta_transaction_costs: Change in total fees/slippage versus baseline.
        return_degradation_pct: Relative percentage drop in returns vs baseline.
        drawdown_increase_pct: Relative percentage increase in maximum drawdown vs baseline.
        warnings: Structured warnings generated for this scenario.
        duration_ms: Execution duration in milliseconds.
        error_message: Optional error message if the scenario failed.
    """

    scenario: StressScenario
    status: str
    backtest_result: Optional[BacktestResult] = None
    delta_net_pnl: float = 0.0
    delta_return_pct: float = 0.0
    delta_max_drawdown_pct: float = 0.0
    delta_trades: int = 0
    delta_win_rate: float = 0.0
    delta_transaction_costs: float = 0.0
    return_degradation_pct: Optional[float] = None
    drawdown_increase_pct: Optional[float] = None
    warnings: Tuple[RobustnessWarning, ...] = ()
    duration_ms: float = 0.0
    error_message: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "warnings", tuple(self.warnings or ()))

    @property
    def is_completed(self) -> bool:
        """True if the scenario evaluated successfully."""
        return self.status == "COMPLETED" and self.backtest_result is not None

    @property
    def metrics(self) -> Optional[PerformanceMetrics]:
        """Convenience accessor to performance metrics."""
        return self.backtest_result.metrics if self.backtest_result else None

    def to_dict(self) -> Dict[str, Any]:
        """Convert scenario result to dictionary."""
        return {
            "scenario": self.scenario.to_dict(),
            "status": self.status,
            "metrics": self.metrics.to_dict() if self.metrics else None,
            "delta_net_pnl": round(self.delta_net_pnl, 2),
            "delta_return_pct": round(self.delta_return_pct, 2),
            "delta_max_drawdown_pct": round(self.delta_max_drawdown_pct, 2),
            "delta_trades": self.delta_trades,
            "delta_win_rate": round(self.delta_win_rate, 2),
            "delta_transaction_costs": round(self.delta_transaction_costs, 4),
            "return_degradation_pct": (
                round(self.return_degradation_pct, 2) if self.return_degradation_pct is not None else None
            ),
            "drawdown_increase_pct": (
                round(self.drawdown_increase_pct, 2) if self.drawdown_increase_pct is not None else None
            ),
            "warnings": [w.to_dict() for w in self.warnings],
            "duration_ms": round(self.duration_ms, 2),
            "error_message": self.error_message,
        }


@dataclass(frozen=True)
class ParameterSensitivityResult:
    """Evaluation summary for a specific strategy parameter variant."""

    variant_name: str
    parameters: Dict[str, Any]
    status: str
    metrics: Optional[PerformanceMetrics] = None
    delta_return_pct: float = 0.0
    delta_max_drawdown_pct: float = 0.0
    delta_trades: int = 0
    delta_win_rate: float = 0.0
    duration_ms: float = 0.0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert parameter sensitivity result to dictionary."""
        return {
            "variant_name": self.variant_name,
            "parameters": dict(self.parameters),
            "status": self.status,
            "metrics": self.metrics.to_dict() if self.metrics else None,
            "delta_return_pct": round(self.delta_return_pct, 2),
            "delta_max_drawdown_pct": round(self.delta_max_drawdown_pct, 2),
            "delta_trades": self.delta_trades,
            "delta_win_rate": round(self.delta_win_rate, 2),
            "duration_ms": round(self.duration_ms, 2),
            "error_message": self.error_message,
        }


@dataclass(frozen=True)
class HistoricalWindowComparison:
    """Comparative outcome across distinct historical time windows."""

    window_name: str
    start_time: float
    end_time: float
    bars_count: int
    status: str
    metrics: Optional[PerformanceMetrics] = None
    delta_return_pct: float = 0.0
    delta_max_drawdown_pct: float = 0.0
    warnings: Tuple[RobustnessWarning, ...] = ()
    duration_ms: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "warnings", tuple(self.warnings or ()))

    def to_dict(self) -> Dict[str, Any]:
        """Convert window comparison to dictionary."""
        return {
            "window_name": self.window_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "bars_count": self.bars_count,
            "status": self.status,
            "metrics": self.metrics.to_dict() if self.metrics else None,
            "delta_return_pct": round(self.delta_return_pct, 2),
            "delta_max_drawdown_pct": round(self.delta_max_drawdown_pct, 2),
            "warnings": [w.to_dict() for w in self.warnings],
            "duration_ms": round(self.duration_ms, 2),
        }


@dataclass(frozen=True)
class RobustnessSummary:
    """Synthesized analysis across all stress scenarios and parameter perturbations.

    STATISTICAL INTEGRITY GUARANTEE:
    - Does not report an arbitrary composite 'robustness score'.
    - Reports explicit empirical degradation deltas and worst-case outcomes.
    - Division by zero and undefined metrics are handled explicitly without NaN or Inf.
    """

    total_scenarios_evaluated: int
    completed_scenarios: int
    failed_scenarios: int
    skipped_scenarios: int
    baseline_return_pct: float
    baseline_max_drawdown_pct: float
    baseline_net_pnl: float
    baseline_trades: int
    worst_case_return_pct: float
    worst_case_scenario_name: str
    worst_case_max_drawdown_pct: float
    worst_case_drawdown_scenario_name: str
    mean_scenario_return_pct: float
    max_cost_impact_pnl: float
    max_cost_scenario_name: str
    profitable_scenarios_count: int
    unprofitable_scenarios_count: int
    scenario_pass_rate_pct: float
    parameter_variants_count: int = 0
    historical_windows_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary representation."""
        return {
            "total_scenarios_evaluated": self.total_scenarios_evaluated,
            "completed_scenarios": self.completed_scenarios,
            "failed_scenarios": self.failed_scenarios,
            "skipped_scenarios": self.skipped_scenarios,
            "baseline_return_pct": round(self.baseline_return_pct, 2),
            "baseline_max_drawdown_pct": round(self.baseline_max_drawdown_pct, 2),
            "baseline_net_pnl": round(self.baseline_net_pnl, 2),
            "baseline_trades": self.baseline_trades,
            "worst_case_return_pct": round(self.worst_case_return_pct, 2),
            "worst_case_scenario_name": self.worst_case_scenario_name,
            "worst_case_max_drawdown_pct": round(self.worst_case_max_drawdown_pct, 2),
            "worst_case_drawdown_scenario_name": self.worst_case_drawdown_scenario_name,
            "mean_scenario_return_pct": round(self.mean_scenario_return_pct, 2),
            "max_cost_impact_pnl": round(self.max_cost_impact_pnl, 2),
            "max_cost_scenario_name": self.max_cost_scenario_name,
            "profitable_scenarios_count": self.profitable_scenarios_count,
            "unprofitable_scenarios_count": self.unprofitable_scenarios_count,
            "scenario_pass_rate_pct": round(self.scenario_pass_rate_pct, 2),
            "parameter_variants_count": self.parameter_variants_count,
            "historical_windows_count": self.historical_windows_count,
        }


@dataclass(frozen=True)
class RobustnessTestConfig:
    """Validated configuration controlling a robustness and stress-testing session.

    Attributes:
        symbol: Ticker symbol.
        timeframe: Bar timeframe ('1m', '5m', '15m', '1h', '1D').
        start_time: Absolute start timestamp.
        end_time: Absolute end timestamp.
        strategy_name: Target strategy name in StrategyRegistry.
        initial_cash: Starting virtual cash for backtests.
        position_size_type: 'fixed_quantity', 'fixed_cash', or 'percent_equity'.
        position_size_value: Size value.
        fee_model: 'percentage' or 'fixed'.
        fee_rate: Baseline percentage commission.
        fixed_fee: Baseline dollar commission.
        slippage_bps: Baseline slippage in basis points.
        half_spread_bps: Baseline half-spread in basis points.
        execution_model: 'next_bar_open' (anti-look-ahead default) or 'current_bar_close'.
        include_cost_stress: If True, auto-generate standard cost stress scenarios.
        include_parameter_sensitivity: If True, evaluate parameter variants.
        include_historical_windows: If True, evaluate distinct time partitions.
        custom_scenarios: Additional user-defined StressScenario instances.
        parameter_variants: Explicit parameter override dictionaries (max bounded).
        historical_windows: Explicit (start_time, end_time, label) window partitions.
        commission_multipliers: Commission stress scaling factors (e.g. 1.5, 2.0, 3.0).
        spread_multipliers: Spread stress scaling factors (e.g. 2.0, 3.0, 5.0).
        slippage_multipliers: Slippage stress scaling factors (e.g. 2.0, 3.0, 5.0).
        max_total_scenarios: Maximum scenarios allowed to execute in a single run (bound safety).
        test_id: Optional unique run identifier.
        correlation_id: Tracing correlation ID.
    """

    symbol: str
    timeframe: str
    start_time: float
    end_time: float
    strategy_name: str = "sma_crossover"
    initial_cash: float = 100_000.0
    position_size_type: str = "fixed_quantity"
    position_size_value: float = 10.0
    fee_model: str = "percentage"
    fee_rate: float = 0.0005
    fixed_fee: float = 0.0
    slippage_bps: float = 2.0
    half_spread_bps: float = 1.0
    execution_model: str = "next_bar_open"
    include_cost_stress: bool = True
    include_parameter_sensitivity: bool = True
    include_historical_windows: bool = False
    custom_scenarios: Sequence[StressScenario] = ()
    parameter_variants: Sequence[Dict[str, Any]] = ()
    historical_windows: Sequence[Tuple[float, float, str]] = ()
    commission_multipliers: Tuple[float, ...] = (1.5, 2.0, 3.0)
    spread_multipliers: Tuple[float, ...] = (2.0, 3.0, 5.0)
    slippage_multipliers: Tuple[float, ...] = (2.0, 3.0, 5.0)
    max_total_scenarios: int = 30
    test_id: Optional[str] = None
    correlation_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("RobustnessTestConfig symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        tf = normalize_timeframe(self.timeframe)
        object.__setattr__(self, "timeframe", tf)

        if self.start_time <= 0 or self.end_time <= 0:
            raise ValidationError("start_time and end_time must be positive timestamps.")
        if self.end_time <= self.start_time:
            raise ValidationError(
                f"end_time ({self.end_time}) must be strictly greater than start_time ({self.start_time})"
            )

        if not self.strategy_name or not isinstance(self.strategy_name, str) or not self.strategy_name.strip():
            raise ValidationError("strategy_name must be a non-empty string.")
        object.__setattr__(self, "strategy_name", self.strategy_name.strip().lower())

        if self.initial_cash <= 0:
            raise ValidationError("initial_cash must be strictly positive.")

        pst = self.position_size_type.strip().lower()
        if pst not in [p.value for p in PositionSizeType]:
            raise ValidationError(f"Invalid position_size_type: {self.position_size_type}")
        object.__setattr__(self, "position_size_type", pst)

        fm = self.fee_model.strip().lower()
        if fm not in [f.value for f in FeeModel]:
            raise ValidationError(f"Invalid fee_model: {self.fee_model}")
        object.__setattr__(self, "fee_model", fm)

        em = self.execution_model.strip().lower()
        if em not in [e.value for e in ExecutionModel]:
            raise ValidationError(f"Invalid execution_model: {self.execution_model}")
        object.__setattr__(self, "execution_model", em)

        # Enforce bounded scenario count
        if self.max_total_scenarios < 1 or self.max_total_scenarios > 100:
            raise ValidationError("max_total_scenarios must be between 1 and 100.")

        # Bound parameter variants
        if len(self.parameter_variants) > 20:
            raise ValidationError(
                f"Maximum 20 parameter variants allowed per robustness test (got {len(self.parameter_variants)})."
            )

        object.__setattr__(self, "custom_scenarios", tuple(self.custom_scenarios or ()))
        object.__setattr__(self, "parameter_variants", tuple(self.parameter_variants or ()))
        object.__setattr__(self, "historical_windows", tuple(self.historical_windows or ()))
        object.__setattr__(self, "commission_multipliers", tuple(self.commission_multipliers or ()))
        object.__setattr__(self, "spread_multipliers", tuple(self.spread_multipliers or ()))
        object.__setattr__(self, "slippage_multipliers", tuple(self.slippage_multipliers or ()))

        if not self.test_id:
            object.__setattr__(self, "test_id", f"rob_{uuid.uuid4().hex[:12]}")
        if not self.correlation_id:
            object.__setattr__(self, "correlation_id", uuid.uuid4().hex)

    def create_baseline_backtest_config(self) -> BacktestConfig:
        """Construct the reference BacktestConfig corresponding to baseline assumptions."""
        return BacktestConfig(
            symbol=self.symbol,
            timeframe=self.timeframe,
            start_time=self.start_time,
            end_time=self.end_time,
            initial_cash=self.initial_cash,
            position_size_type=self.position_size_type,
            position_size_value=self.position_size_value,
            fee_model=self.fee_model,
            fee_rate=self.fee_rate,
            fixed_fee=self.fixed_fee,
            slippage_bps=self.slippage_bps,
            half_spread_bps=self.half_spread_bps,
            execution_model=self.execution_model,
            reset_state_between_runs=True,
            correlation_id=self.correlation_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "test_id": self.test_id,
            "correlation_id": self.correlation_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "strategy_name": self.strategy_name,
            "initial_cash": self.initial_cash,
            "position_size_type": self.position_size_type,
            "position_size_value": self.position_size_value,
            "fee_model": self.fee_model,
            "fee_rate": self.fee_rate,
            "fixed_fee": self.fixed_fee,
            "slippage_bps": self.slippage_bps,
            "half_spread_bps": self.half_spread_bps,
            "execution_model": self.execution_model,
            "include_cost_stress": self.include_cost_stress,
            "include_parameter_sensitivity": self.include_parameter_sensitivity,
            "include_historical_windows": self.include_historical_windows,
            "custom_scenarios_count": len(self.custom_scenarios),
            "parameter_variants_count": len(self.parameter_variants),
            "historical_windows_count": len(self.historical_windows),
            "max_total_scenarios": self.max_total_scenarios,
        }


@dataclass(frozen=True)
class RobustnessTestResult:
    """Comprehensive artifact bundling baseline performance, stress outcomes, and sensitivity analysis."""

    test_id: str
    status: RobustnessStatus
    strategy_name: str
    strategy_version: str
    config: RobustnessTestConfig
    baseline_result: Optional[BacktestResult]
    summary: RobustnessSummary
    scenario_results: Tuple[StressScenarioResult, ...] = ()
    parameter_results: Tuple[ParameterSensitivityResult, ...] = ()
    window_results: Tuple[HistoricalWindowComparison, ...] = ()
    warnings: Tuple[RobustnessWarning, ...] = ()
    duration_ms: float = 0.0
    error_message: Optional[str] = None
    disclaimer: str = (
        "RESEARCH ONLY: Strategy robustness and stress testing evaluates hypothetical performance "
        "under adverse transaction costs, parameter perturbations, and data partitions. "
        "It does NOT guarantee future market profitability, predict actual future costs, "
        "or execute live orders."
    )
    run_id: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "scenario_results", tuple(self.scenario_results or ()))
        object.__setattr__(self, "parameter_results", tuple(self.parameter_results or ()))
        object.__setattr__(self, "window_results", tuple(self.window_results or ()))
        object.__setattr__(self, "warnings", tuple(self.warnings or ()))

    @property
    def is_success(self) -> bool:
        """True if the robustness testing session completed without fatal failure."""
        return self.status == RobustnessStatus.COMPLETED

    @property
    def summary_text(self) -> str:
        """Concise human/voice readable robustness summary."""
        return (
            f"Robustness Test [{self.test_id}] for {self.config.symbol} ({self.config.timeframe}) "
            f"Strategy={self.strategy_name}: Status={self.status.value}, "
            f"Scenarios={self.summary.completed_scenarios}/{self.summary.total_scenarios_evaluated} "
            f"(PassRate={self.summary.scenario_pass_rate_pct:.1f}%), "
            f"BaselineReturn={self.summary.baseline_return_pct:+.2f}%, "
            f"WorstCaseReturn={self.summary.worst_case_return_pct:+.2f}% ({self.summary.worst_case_scenario_name}), "
            f"BaselineMaxDD={self.summary.baseline_max_drawdown_pct:.2f}%, "
            f"WorstCaseMaxDD={self.summary.worst_case_max_drawdown_pct:.2f}% ({self.summary.worst_case_drawdown_scenario_name}), "
            f"MaxCostImpact=-${abs(self.summary.max_cost_impact_pnl):,.2f}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert complete result to serializable dictionary."""
        return {
            "test_id": self.test_id,
            "status": self.status.value,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "config": self.config.to_dict(),
            "baseline_result": self.baseline_result.to_dict() if self.baseline_result else None,
            "summary": self.summary.to_dict(),
            "scenario_results_count": len(self.scenario_results),
            "scenario_results": [r.to_dict() for r in self.scenario_results],
            "parameter_results_count": len(self.parameter_results),
            "parameter_results": [p.to_dict() for p in self.parameter_results],
            "window_results_count": len(self.window_results),
            "window_results": [w.to_dict() for w in self.window_results],
            "warnings_count": len(self.warnings),
            "warnings": [w.to_dict() for w in self.warnings],
            "duration_ms": round(self.duration_ms, 2),
            "error_message": self.error_message,
            "summary_text": self.summary_text,
            "disclaimer": self.disclaimer,
            "run_id": self.run_id,
        }
