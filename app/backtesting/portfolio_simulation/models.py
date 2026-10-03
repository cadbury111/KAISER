"""KAISER — Portfolio-Level Risk Analytics & Multi-Strategy Domain Models.

Provides strongly typed, immutable dataclasses for multi-strategy virtual portfolio
simulations, risk control configurations, strategy allocations, position tracking,
risk snapshots, diversification metrics, and audit results.

SAFETY INVARIANTS:
1. Virtual simulation only: strictly operates within simulated portfolios without real broker connections.
2. Complete balance integrity: cash + total_positions == total_equity at every simulation step.
3. Central risk controls: enforces cash limits, concentration, and exposure bounds deterministically.
4. Research-only disclaimer: backtested multi-strategy results never guarantee future performance or diversification.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.analysis.models import normalize_timeframe
from app.backtesting.models import (
    EquityPoint,
    ExecutionModel,
    FeeModel,
    PositionSizeType,
    SimulatedFill,
    SimulatedOrder,
    TradeRecord,
)
from app.core.exceptions import ValidationError


class PortfolioSimulationStatus(str, Enum):
    """Execution status of a multi-strategy portfolio simulation."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class StrategyAllocation:
    """Configured allocation and sizing parameters for a specific strategy instance.

    Attributes:
        strategy_name: Registered strategy identifier in StrategyRegistry.
        symbol: Ticker symbol assigned to this strategy instance.
        weight: Fractional capital allocation weight (0.0 < weight <= 1.0).
        parameters: Strategy configuration parameters (e.g. fast_period, slow_period).
        allocation_id: Unique identifier for this strategy instance.
        max_exposure_pct: Maximum allowed position exposure for this strategy as % of portfolio equity.
        position_size_type: Sizing model ('percent_equity', 'fixed_quantity', 'fixed_cash').
        position_size_value: Numerical sizing parameter.
    """

    strategy_name: str
    symbol: str
    weight: float = 1.0
    parameters: Dict[str, Any] = field(default_factory=dict)
    allocation_id: str = ""
    max_exposure_pct: Optional[float] = None
    position_size_type: str = PositionSizeType.PERCENT_EQUITY.value
    position_size_value: float = 1.0

    def __post_init__(self) -> None:
        if not self.strategy_name or not isinstance(self.strategy_name, str):
            raise ValidationError("StrategyAllocation strategy_name must be a non-empty string.")
        object.__setattr__(self, "strategy_name", self.strategy_name.strip().lower())

        if not self.symbol or not isinstance(self.symbol, str):
            raise ValidationError("StrategyAllocation symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if self.weight <= 0.0 or self.weight > 1.0001:
            raise ValidationError(
                f"StrategyAllocation weight must be strictly positive and <= 1.0, got {self.weight}"
            )

        if not self.allocation_id:
            object.__setattr__(self, "allocation_id", f"{self.strategy_name}_{self.symbol.replace('/', '_')}")

        if self.max_exposure_pct is not None and (self.max_exposure_pct <= 0.0 or self.max_exposure_pct > 1.0001):
            raise ValidationError(
                f"StrategyAllocation max_exposure_pct must be in (0.0, 1.0], got {self.max_exposure_pct}"
            )

        if self.position_size_value <= 0.0:
            raise ValidationError(
                f"StrategyAllocation position_size_value must be positive, got {self.position_size_value}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert allocation configuration to dictionary."""
        return {
            "allocation_id": self.allocation_id,
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "weight": round(self.weight, 4),
            "parameters": dict(self.parameters),
            "max_exposure_pct": self.max_exposure_pct,
            "position_size_type": self.position_size_type,
            "position_size_value": self.position_size_value,
        }


@dataclass(frozen=True)
class PortfolioPosition:
    """Holding representation for a specific strategy instance inside the shared virtual portfolio.

    Attributes:
        strategy_id: Strategy instance allocation_id that owns this position.
        symbol: Ticker symbol.
        quantity: Share or contract quantity.
        avg_cost: Weighted average execution cost per unit.
        current_price: Latest mark-to-market price.
        entry_timestamp: Timestamp of initial entry fill.
        accumulated_fees: Total transaction fees accrued on this position.
    """

    strategy_id: str
    symbol: str
    quantity: float
    avg_cost: float
    current_price: float
    entry_timestamp: float
    accumulated_fees: float = 0.0

    @property
    def market_value(self) -> float:
        """Current mark-to-market value."""
        return self.quantity * self.current_price

    @property
    def cost_basis(self) -> float:
        """Total capital invested into the position."""
        return self.quantity * self.avg_cost

    @property
    def unrealized_pnl(self) -> float:
        """Unrealized dollar profit or loss."""
        return self.market_value - self.cost_basis

    @property
    def unrealized_return_pct(self) -> float:
        """Unrealized percentage return on cost basis."""
        if self.cost_basis <= 0:
            return 0.0
        return (self.unrealized_pnl / self.cost_basis) * 100.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary."""
        return {
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "quantity": round(self.quantity, 6),
            "avg_cost": round(self.avg_cost, 4),
            "current_price": round(self.current_price, 4),
            "market_value": round(self.market_value, 2),
            "cost_basis": round(self.cost_basis, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "unrealized_return_pct": round(self.unrealized_return_pct, 4),
            "entry_timestamp": self.entry_timestamp,
            "accumulated_fees": round(self.accumulated_fees, 4),
        }


@dataclass(frozen=True)
class PortfolioRiskSnapshot:
    """Instantaneous snapshot of portfolio-level exposure, capital, and risk metrics.

    Attributes:
        timestamp: Simulation timestamp of observation.
        total_equity: Total virtual equity (cash + mark-to-market positions).
        cash: Free virtual cash balance.
        cash_reserve_pct: Free cash expressed as a percentage of total equity.
        gross_exposure: Sum of absolute market values of all open positions.
        gross_exposure_pct: Gross exposure divided by total equity * 100.
        net_exposure: Net sum of position market values (longs - shorts).
        net_exposure_pct: Net exposure divided by total equity * 100.
        strategy_exposures: Mapping of strategy_id to absolute position market value.
        instrument_exposures: Mapping of symbol to absolute position market value.
        max_instrument_concentration_pct: Highest single-instrument market value as % of total equity.
        herfindahl_index: Herfindahl-Hirschman concentration index of instrument weights.
        drawdown_pct: Current percentage drawdown from peak equity.
        circuit_breaker_active: Whether portfolio drawdown circuit breaker is triggered.
    """

    timestamp: float
    total_equity: float
    cash: float
    cash_reserve_pct: float
    gross_exposure: float
    gross_exposure_pct: float
    net_exposure: float
    net_exposure_pct: float
    strategy_exposures: Dict[str, float] = field(default_factory=dict)
    instrument_exposures: Dict[str, float] = field(default_factory=dict)
    max_instrument_concentration_pct: float = 0.0
    herfindahl_index: float = 0.0
    drawdown_pct: float = 0.0
    circuit_breaker_active: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "timestamp": self.timestamp,
            "total_equity": round(self.total_equity, 2),
            "cash": round(self.cash, 2),
            "cash_reserve_pct": round(self.cash_reserve_pct, 2),
            "gross_exposure": round(self.gross_exposure, 2),
            "gross_exposure_pct": round(self.gross_exposure_pct, 2),
            "net_exposure": round(self.net_exposure, 2),
            "net_exposure_pct": round(self.net_exposure_pct, 2),
            "strategy_exposures": {k: round(v, 2) for k, v in self.strategy_exposures.items()},
            "instrument_exposures": {k: round(v, 2) for k, v in self.instrument_exposures.items()},
            "max_instrument_concentration_pct": round(self.max_instrument_concentration_pct, 2),
            "herfindahl_index": round(self.herfindahl_index, 4),
            "drawdown_pct": round(self.drawdown_pct, 2),
            "circuit_breaker_active": self.circuit_breaker_active,
        }


@dataclass(frozen=True)
class PortfolioRiskWarning:
    """Structured audit warning logged during portfolio simulation.

    Attributes:
        timestamp: Simulation timestamp when warning was triggered.
        warning_type: Categorical warning identifier.
        message: Human-readable description.
        details: Additional contextual parameters.
    """

    timestamp: float
    warning_type: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert warning to dictionary."""
        return {
            "timestamp": self.timestamp,
            "warning_type": self.warning_type,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class StrategyContribution:
    """Individual strategy's contribution and standalone performance inside the combined portfolio.

    Attributes:
        strategy_id: Unique strategy allocation ID.
        strategy_name: Registered strategy name.
        symbol: Traded instrument.
        weight: Allocated capital weight.
        allocated_capital: Starting virtual cash allocated to this strategy.
        gross_pnl: Gross profit or loss from completed trades and open positions.
        net_pnl: Net profit or loss after fees and slippage.
        net_return_pct: Percentage return on allocated capital.
        portfolio_pnl_share_pct: Strategy's contribution as a percentage of total portfolio net PnL.
        total_trades: Number of round-trip trades completed.
        winning_trades: Count of profitable trades.
        losing_trades: Count of unprofitable trades.
        win_rate: Percentage of winning trades.
        profit_factor: Gross profit divided by gross loss.
        profit_factor_description: Safe human-readable descriptor.
        total_fees: Cumulative transaction fees charged to this strategy.
        total_slippage: Cumulative slippage cost incurred by this strategy.
        orders_submitted: Total simulated orders submitted.
        orders_accepted: Total orders accepted and queued for execution.
        orders_rejected: Total orders rejected by risk or cash gates.
        trades: Tuple of completed trade records for this strategy.
    """

    strategy_id: str
    strategy_name: str
    symbol: str
    weight: float
    allocated_capital: float
    gross_pnl: float
    net_pnl: float
    net_return_pct: float
    portfolio_pnl_share_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    profit_factor: Optional[float]
    profit_factor_description: str
    total_fees: float
    total_slippage: float
    orders_submitted: int
    orders_accepted: int
    orders_rejected: int
    trades: Tuple[TradeRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy contribution to dictionary."""
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "weight": round(self.weight, 4),
            "allocated_capital": round(self.allocated_capital, 2),
            "gross_pnl": round(self.gross_pnl, 2),
            "net_pnl": round(self.net_pnl, 2),
            "net_return_pct": round(self.net_return_pct, 4),
            "portfolio_pnl_share_pct": round(self.portfolio_pnl_share_pct, 2),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": round(self.win_rate, 2),
            "profit_factor": self.profit_factor,
            "profit_factor_description": self.profit_factor_description,
            "total_fees": round(self.total_fees, 4),
            "total_slippage": round(self.total_slippage, 4),
            "orders_submitted": self.orders_submitted,
            "orders_accepted": self.orders_accepted,
            "orders_rejected": self.orders_rejected,
            "trades_count": len(self.trades),
        }


@dataclass(frozen=True)
class DiversificationMetrics:
    """Historical return correlation and portfolio diversification analytics.

    Attributes:
        strategy_correlations: Pairwise correlation matrix between strategy returns.
        instrument_correlations: Pairwise correlation matrix between instrument bar returns.
        observation_count: Number of aligned historical observation intervals analyzed.
        is_valid: True if sufficient observations existed to calculate correlations safely.
        warning: Diagnostic message if correlations could not be calculated.
        herfindahl_index: Capital concentration HHI across strategies (sum of squared weights).
        effective_strategies_count: Effective number of uncorrelated strategies (1 / HHI).
        disclaimer: Clear statement that historical correlations do not guarantee future diversification.
    """

    strategy_correlations: Dict[str, Dict[str, Optional[float]]] = field(default_factory=dict)
    instrument_correlations: Dict[str, Dict[str, Optional[float]]] = field(default_factory=dict)
    observation_count: int = 0
    is_valid: bool = True
    warning: Optional[str] = None
    herfindahl_index: float = 1.0
    effective_strategies_count: float = 1.0
    disclaimer: str = (
        "RESEARCH ONLY: Historical correlations and diversification metrics are descriptive. "
        "They do NOT guarantee future diversification, risk reduction, or downside protection."
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert diversification metrics to dictionary."""
        return {
            "strategy_correlations": self.strategy_correlations,
            "instrument_correlations": self.instrument_correlations,
            "observation_count": self.observation_count,
            "is_valid": self.is_valid,
            "warning": self.warning,
            "herfindahl_index": round(self.herfindahl_index, 4),
            "effective_strategies_count": round(self.effective_strategies_count, 2),
            "disclaimer": self.disclaimer,
        }


@dataclass(frozen=True)
class PortfolioSimulationConfig:
    """Validated configuration for multi-strategy virtual portfolio simulation.

    Attributes:
        allocations: Non-empty list of StrategyAllocation instances.
        start_time: Evaluation window start epoch timestamp.
        end_time: Evaluation window end epoch timestamp.
        timeframe: Bar timeframe (e.g. '15m', '1h', '1d').
        initial_cash: Starting virtual capital in base currency.
        simulation_id: Unique simulation run identifier.
        max_portfolio_exposure_pct: Maximum allowed portfolio gross exposure (1.0 = 100%).
        max_strategy_exposure_pct: Default maximum allowed exposure per strategy.
        max_instrument_concentration_pct: Maximum allowed exposure in any single instrument.
        min_cash_reserve_pct: Minimum required free cash buffer as % of initial equity.
        max_drawdown_limit_pct: Portfolio circuit-breaker threshold (e.g. 20.0%).
        fee_model: Transaction fee model ('percentage' or 'fixed').
        fee_rate: Percentage fee applied per trade value.
        fixed_fee: Fixed dollar fee applied per trade.
        slippage_bps: Slippage penalty in basis points.
        half_spread_bps: Half-spread penalty in basis points.
        execution_model: Order execution timing model ('next_bar_open').
        allow_short: Whether short positions are permitted.
        min_correlation_observations: Minimum aligned return observations required for correlation matrix.
        correlation_id: Tracing correlation ID.
        reset_state_between_runs: Whether to reset strategy/engine states before simulation.
        pacing_seconds: Optional sleep interval per bar for demonstration/pacing.
    """

    allocations: Tuple[StrategyAllocation, ...]
    start_time: float
    end_time: float
    timeframe: str = "15m"
    initial_cash: float = 100_000.0
    simulation_id: str = ""
    max_portfolio_exposure_pct: float = 1.0
    max_strategy_exposure_pct: float = 1.0
    max_instrument_concentration_pct: float = 1.0
    min_cash_reserve_pct: float = 0.0
    max_drawdown_limit_pct: Optional[float] = None
    fee_model: str = FeeModel.PERCENTAGE.value
    fee_rate: float = 0.0005
    fixed_fee: float = 0.0
    slippage_bps: float = 2.0
    half_spread_bps: float = 1.0
    execution_model: str = ExecutionModel.NEXT_BAR_OPEN.value
    allow_short: bool = False
    min_correlation_observations: int = 10
    correlation_id: Optional[str] = None
    reset_state_between_runs: bool = True
    pacing_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not self.allocations:
            raise ValidationError("PortfolioSimulationConfig requires at least one StrategyAllocation.")

        # Ensure allocations is a tuple
        if isinstance(self.allocations, list):
            object.__setattr__(self, "allocations", tuple(self.allocations))

        # Check total allocation weights
        total_weight = sum(alloc.weight for alloc in self.allocations)
        if total_weight > 1.0001:
            raise ValidationError(
                f"Sum of allocation weights ({total_weight:.4f}) cannot exceed 1.0 (100%)."
            )

        if self.initial_cash <= 0:
            raise ValidationError(
                f"initial_cash must be strictly positive, got {self.initial_cash}"
            )

        if self.start_time <= 0 or self.end_time <= 0:
            raise ValidationError("start_time and end_time must be positive timestamps.")

        if self.end_time <= self.start_time:
            raise ValidationError(
                f"end_time ({self.end_time}) must be strictly greater than start_time ({self.start_time})"
            )

        normalized_tf = normalize_timeframe(self.timeframe)
        object.__setattr__(self, "timeframe", normalized_tf)

        if not self.simulation_id:
            object.__setattr__(self, "simulation_id", f"portsim_{uuid.uuid4().hex[:12]}")

        if self.max_portfolio_exposure_pct <= 0.0 or self.max_portfolio_exposure_pct > 2.0:
            raise ValidationError(
                f"max_portfolio_exposure_pct must be in (0.0, 2.0], got {self.max_portfolio_exposure_pct}"
            )

        if self.max_strategy_exposure_pct <= 0.0 or self.max_strategy_exposure_pct > 1.0001:
            raise ValidationError(
                f"max_strategy_exposure_pct must be in (0.0, 1.0], got {self.max_strategy_exposure_pct}"
            )

        if self.max_instrument_concentration_pct <= 0.0 or self.max_instrument_concentration_pct > 1.0001:
            raise ValidationError(
                f"max_instrument_concentration_pct must be in (0.0, 1.0], got {self.max_instrument_concentration_pct}"
            )

        if self.min_cash_reserve_pct < 0.0 or self.min_cash_reserve_pct >= 1.0:
            raise ValidationError(
                f"min_cash_reserve_pct must be in [0.0, 1.0), got {self.min_cash_reserve_pct}"
            )

        if self.max_drawdown_limit_pct is not None and (self.max_drawdown_limit_pct <= 0.0 or self.max_drawdown_limit_pct > 100.0):
            raise ValidationError(
                f"max_drawdown_limit_pct must be in (0.0, 100.0], got {self.max_drawdown_limit_pct}"
            )

        if self.fee_model not in (FeeModel.PERCENTAGE.value, FeeModel.FIXED.value):
            raise ValidationError(f"Invalid fee_model '{self.fee_model}'.")

        if self.execution_model != ExecutionModel.NEXT_BAR_OPEN.value:
            raise ValidationError(
                f"Unsupported execution_model '{self.execution_model}'. Must be '{ExecutionModel.NEXT_BAR_OPEN.value}'."
            )

    @property
    def symbols(self) -> Tuple[str, ...]:
        """Unique set of traded instruments across all allocations."""
        unique_syms = sorted(list({alloc.symbol for alloc in self.allocations}))
        return tuple(unique_syms)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "simulation_id": self.simulation_id,
            "allocations": [alloc.to_dict() for alloc in self.allocations],
            "symbols": list(self.symbols),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "timeframe": self.timeframe,
            "initial_cash": self.initial_cash,
            "max_portfolio_exposure_pct": self.max_portfolio_exposure_pct,
            "max_strategy_exposure_pct": self.max_strategy_exposure_pct,
            "max_instrument_concentration_pct": self.max_instrument_concentration_pct,
            "min_cash_reserve_pct": self.min_cash_reserve_pct,
            "max_drawdown_limit_pct": self.max_drawdown_limit_pct,
            "fee_model": self.fee_model,
            "fee_rate": self.fee_rate,
            "fixed_fee": self.fixed_fee,
            "slippage_bps": self.slippage_bps,
            "half_spread_bps": self.half_spread_bps,
            "execution_model": self.execution_model,
            "allow_short": self.allow_short,
            "min_correlation_observations": self.min_correlation_observations,
            "correlation_id": self.correlation_id,
        }


@dataclass(frozen=True)
class PortfolioSimulationResult:
    """Comprehensive, immutable audit result of a multi-strategy portfolio simulation.

    Contains full portfolio-level performance metrics, equity curves, individual strategy
    contributions, risk snapshots, correlation matrices, and risk limit warnings.
    """

    simulation_id: str
    status: PortfolioSimulationStatus
    config: PortfolioSimulationConfig
    initial_equity: float
    final_equity: float
    gross_pnl: float
    net_pnl: float
    net_return_pct: float
    max_drawdown_pct: float
    max_drawdown_amount: float
    max_drawdown_duration_bars: int
    total_fees: float
    total_slippage: float
    total_transaction_costs: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    profit_factor: Optional[float]
    profit_factor_description: str
    equity_curve: Tuple[EquityPoint, ...] = field(default_factory=tuple)
    risk_snapshots: Tuple[PortfolioRiskSnapshot, ...] = field(default_factory=tuple)
    strategy_contributions: Tuple[StrategyContribution, ...] = field(default_factory=tuple)
    diversification: Optional[DiversificationMetrics] = None
    orders_submitted: int = 0
    orders_accepted: int = 0
    orders_rejected: int = 0
    rejected_orders: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)
    warnings: Tuple[PortfolioRiskWarning, ...] = field(default_factory=tuple)
    bars_processed: int = 0
    start_timestamp: float = 0.0
    end_timestamp: float = 0.0
    duration_ms: float = 0.0
    error_message: Optional[str] = None
    disclaimer: str = (
        "RESEARCH ONLY: Multi-strategy portfolio simulation results are hypothetical. "
        "They do NOT guarantee profitability, protect against loss, or predict actual future performance."
    )

    @property
    def is_success(self) -> bool:
        """True if the simulation completed successfully."""
        return self.status == PortfolioSimulationStatus.SUCCESS

    @property
    def summary_text(self) -> str:
        """Formatted human-readable portfolio simulation performance summary."""
        sign = "+" if self.net_pnl >= 0 else ""
        return (
            f"Portfolio Simulation [{self.simulation_id}] ({self.status.value}): "
            f"Initial=${self.initial_equity:,.2f} -> Final=${self.final_equity:,.2f} | "
            f"Net PnL={sign}${self.net_pnl:,.2f} ({sign}{self.net_return_pct:.2f}%) | "
            f"MaxDD={self.max_drawdown_pct:.2f}% (${self.max_drawdown_amount:,.2f}) | "
            f"Trades={self.total_trades} (WinRate={self.win_rate:.1f}%, PF={self.profit_factor_description}) | "
            f"Costs=${self.total_transaction_costs:,.2f} | "
            f"Strategies={len(self.strategy_contributions)} | "
            f"Warnings={len(self.warnings)}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize complete portfolio simulation result to dictionary."""
        return {
            "simulation_id": self.simulation_id,
            "status": self.status.value,
            "config": self.config.to_dict(),
            "initial_equity": round(self.initial_equity, 2),
            "final_equity": round(self.final_equity, 2),
            "gross_pnl": round(self.gross_pnl, 2),
            "net_pnl": round(self.net_pnl, 2),
            "net_return_pct": round(self.net_return_pct, 4),
            "max_drawdown_pct": round(self.max_drawdown_pct, 2),
            "max_drawdown_amount": round(self.max_drawdown_amount, 2),
            "max_drawdown_duration_bars": self.max_drawdown_duration_bars,
            "total_fees": round(self.total_fees, 4),
            "total_slippage": round(self.total_slippage, 4),
            "total_transaction_costs": round(self.total_transaction_costs, 4),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": round(self.win_rate, 2),
            "profit_factor": self.profit_factor,
            "profit_factor_description": self.profit_factor_description,
            "equity_points_count": len(self.equity_curve),
            "risk_snapshots_count": len(self.risk_snapshots),
            "strategy_contributions": [c.to_dict() for c in self.strategy_contributions],
            "diversification": self.diversification.to_dict() if self.diversification else None,
            "orders_submitted": self.orders_submitted,
            "orders_accepted": self.orders_accepted,
            "orders_rejected": self.orders_rejected,
            "rejected_orders_sample": list(self.rejected_orders[:20]),
            "warnings": [w.to_dict() for w in self.warnings],
            "bars_processed": self.bars_processed,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "duration_ms": round(self.duration_ms, 2),
            "error_message": self.error_message,
            "summary_text": self.summary_text,
            "disclaimer": self.disclaimer,
        }
