"""KAISER — Portfolio Performance Attribution & Risk-Adjusted Metrics Models.

Provides strongly typed, immutable dataclasses for:
- MetricValue container with explicit availability, unit, and reason
- RiskAdjustedMetrics (annualized volatility, Sharpe, Sortino, Calmar, MaxDD, Win/Loss)
- StrategyAttribution and InstrumentAttribution
- TransactionCostAttribution
- ReconciliationReport (verifying portfolio vs strategy PnL and gross vs net accounting)
- PortfolioAttributionReport
- ComparisonReport (multi-strategy, simulation runs, walk-forward, robustness)

SAFETY & RESEARCH INVARIANTS:
1. Purely informational, research-only analytics.
2. Never places trades, modifies orders, or connects to live brokers.
3. Does not infer or fabricate unavailable values; missing data yields explicit unavailable status.
4. Historical risk-adjusted metrics do not guarantee future performance or risk mitigation.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.exceptions import ValidationError


class AttributionLevel(str, Enum):
    """Granularity level for performance attribution."""

    PORTFOLIO = "PORTFOLIO"
    STRATEGY = "STRATEGY"
    INSTRUMENT = "INSTRUMENT"


class ReconciliationStatus(str, Enum):
    """Integrity and reconciliation status of accounting records."""

    EXACT = "EXACT"
    RECONCILED_WITHIN_TOLERANCE = "RECONCILED_WITHIN_TOLERANCE"
    DISCREPANCY_DETECTED = "DISCREPANCY_DETECTED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class ComparisonType(str, Enum):
    """Category of multi-entity performance comparison."""

    SIMULATION_RUNS = "SIMULATION_RUNS"
    WALK_FORWARD = "WALK_FORWARD"
    ROBUSTNESS = "ROBUSTNESS"
    STRATEGY_COMPARISON = "STRATEGY_COMPARISON"


@dataclass(frozen=True)
class MetricValue:
    """Strongly typed metric value wrapper with availability, units, and diagnostics.

    Prevents silent fallback to zero or undefined infinity. If a metric cannot be
    calculated (e.g. division by zero, insufficient observations), `value` is `None`,
    `is_available` is `False`, and `unavailable_reason` explains why.
    """

    value: Optional[float]
    unit: str
    is_available: bool = True
    unavailable_reason: Optional[str] = None
    description: str = ""

    def __post_init__(self) -> None:
        if self.value is not None:
            if math.isnan(self.value) or math.isinf(self.value):
                object.__setattr__(self, "value", None)
                object.__setattr__(self, "is_available", False)
                if not self.unavailable_reason:
                    object.__setattr__(self, "unavailable_reason", "Value is non-finite (NaN or Inf).")

        if self.value is None and self.is_available:
            object.__setattr__(self, "is_available", False)
            if not self.unavailable_reason:
                object.__setattr__(self, "unavailable_reason", "Metric value is None.")

    def to_dict(self) -> Dict[str, Any]:
        """Convert metric value to serializable dictionary."""
        return {
            "value": round(self.value, 4) if self.value is not None else None,
            "unit": self.unit,
            "is_available": self.is_available,
            "unavailable_reason": self.unavailable_reason,
            "description": self.description,
        }


@dataclass(frozen=True)
class AttributionConfig:
    """Configuration parameters for portfolio attribution and risk-adjusted metrics.

    Attributes:
        annualization_factor: Number of observation periods per year (e.g. 252 for daily).
        risk_free_rate: Annualized benchmark risk-free rate as a decimal (e.g. 0.04 = 4.0%).
        reconciliation_tolerance: Acceptable absolute difference in currency units for accounting reconciliation.
        sampling_frequency: Label for bar or snapshot frequency ('daily', '15m', '1h', etc.).
    """

    annualization_factor: float = 252.0
    risk_free_rate: float = 0.0
    reconciliation_tolerance: float = 0.05
    sampling_frequency: str = "daily"

    def __post_init__(self) -> None:
        if self.annualization_factor <= 0:
            raise ValidationError(
                f"annualization_factor must be strictly positive, got {self.annualization_factor}"
            )
        if self.reconciliation_tolerance < 0:
            raise ValidationError(
                f"reconciliation_tolerance cannot be negative, got {self.reconciliation_tolerance}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "annualization_factor": self.annualization_factor,
            "risk_free_rate": self.risk_free_rate,
            "reconciliation_tolerance": self.reconciliation_tolerance,
            "sampling_frequency": self.sampling_frequency,
        }


@dataclass(frozen=True)
class RiskAdjustedMetrics:
    """Comprehensive risk-adjusted and trade distribution performance metrics."""

    annualized_return_pct: MetricValue
    annualized_volatility_pct: MetricValue
    sharpe_ratio: MetricValue
    sortino_ratio: MetricValue
    max_drawdown_pct: MetricValue
    max_drawdown_amount: MetricValue
    max_drawdown_duration_bars: MetricValue
    calmar_ratio: MetricValue
    profit_factor: MetricValue
    win_rate: MetricValue
    win_loss_ratio: MetricValue
    average_win: MetricValue
    average_loss: MetricValue
    total_trades: MetricValue
    market_exposure_pct: MetricValue
    annualization_factor: float
    risk_free_rate: float

    def to_dict(self) -> Dict[str, Any]:
        """Serialize metrics to dictionary."""
        return {
            "annualized_return_pct": self.annualized_return_pct.to_dict(),
            "annualized_volatility_pct": self.annualized_volatility_pct.to_dict(),
            "sharpe_ratio": self.sharpe_ratio.to_dict(),
            "sortino_ratio": self.sortino_ratio.to_dict(),
            "max_drawdown_pct": self.max_drawdown_pct.to_dict(),
            "max_drawdown_amount": self.max_drawdown_amount.to_dict(),
            "max_drawdown_duration_bars": self.max_drawdown_duration_bars.to_dict(),
            "calmar_ratio": self.calmar_ratio.to_dict(),
            "profit_factor": self.profit_factor.to_dict(),
            "win_rate": self.win_rate.to_dict(),
            "win_loss_ratio": self.win_loss_ratio.to_dict(),
            "average_win": self.average_win.to_dict(),
            "average_loss": self.average_loss.to_dict(),
            "total_trades": self.total_trades.to_dict(),
            "market_exposure_pct": self.market_exposure_pct.to_dict(),
            "annualization_factor": self.annualization_factor,
            "risk_free_rate": self.risk_free_rate,
        }


@dataclass(frozen=True)
class TransactionCostAttribution:
    """Attribution breakdown of transaction friction costs."""

    total_costs: float
    commissions_fees: float
    slippage_costs: float
    spread_costs: float = 0.0
    pct_of_gross_pnl: Optional[float] = None
    pct_of_initial_equity: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert transaction costs to dictionary."""
        return {
            "total_costs": round(self.total_costs, 4),
            "commissions_fees": round(self.commissions_fees, 4),
            "slippage_costs": round(self.slippage_costs, 4),
            "spread_costs": round(self.spread_costs, 4),
            "pct_of_gross_pnl": round(self.pct_of_gross_pnl, 2) if self.pct_of_gross_pnl is not None else None,
            "pct_of_initial_equity": round(self.pct_of_initial_equity, 4),
        }


@dataclass(frozen=True)
class StrategyAttribution:
    """Individual strategy's performance attribution inside the combined portfolio."""

    strategy_id: str
    strategy_name: str
    symbol: str
    weight: float
    allocated_capital: float
    gross_pnl: float
    net_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    total_costs: float
    gross_return_pct: float
    net_return_pct: float
    portfolio_pnl_share_pct: float
    trades_count: int
    win_rate: float
    profit_factor: Optional[float] = None
    risk_adjusted_metrics: Optional[RiskAdjustedMetrics] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert strategy attribution to dictionary."""
        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "weight": round(self.weight, 4),
            "allocated_capital": round(self.allocated_capital, 2),
            "gross_pnl": round(self.gross_pnl, 2),
            "net_pnl": round(self.net_pnl, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "total_costs": round(self.total_costs, 4),
            "gross_return_pct": round(self.gross_return_pct, 4),
            "net_return_pct": round(self.net_return_pct, 4),
            "portfolio_pnl_share_pct": round(self.portfolio_pnl_share_pct, 2),
            "trades_count": self.trades_count,
            "win_rate": round(self.win_rate, 2),
            "profit_factor": round(self.profit_factor, 2) if self.profit_factor is not None else None,
            "risk_adjusted_metrics": self.risk_adjusted_metrics.to_dict() if self.risk_adjusted_metrics else None,
        }


@dataclass(frozen=True)
class InstrumentAttribution:
    """Attribution breakdown aggregated by traded instrument/symbol."""

    symbol: str
    gross_pnl: float
    net_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    total_costs: float
    trades_count: int
    portfolio_pnl_share_pct: float
    is_available: bool = True
    unavailable_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert instrument attribution to dictionary."""
        return {
            "symbol": self.symbol,
            "gross_pnl": round(self.gross_pnl, 2),
            "net_pnl": round(self.net_pnl, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "total_costs": round(self.total_costs, 4),
            "trades_count": self.trades_count,
            "portfolio_pnl_share_pct": round(self.portfolio_pnl_share_pct, 2),
            "is_available": self.is_available,
            "unavailable_reason": self.unavailable_reason,
        }


@dataclass(frozen=True)
class ReconciliationReport:
    """Formal audit report reconciling strategy contributions and transaction costs against portfolio totals."""

    status: ReconciliationStatus
    portfolio_net_pnl: float
    sum_strategy_net_pnl: float
    strategy_discrepancy: float
    portfolio_gross_pnl: float
    portfolio_total_costs: float
    gross_minus_costs: float
    gross_to_net_discrepancy: float
    is_reconciled: bool
    tolerance: float
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert reconciliation report to dictionary."""
        return {
            "status": self.status.value,
            "portfolio_net_pnl": round(self.portfolio_net_pnl, 2),
            "sum_strategy_net_pnl": round(self.sum_strategy_net_pnl, 2),
            "strategy_discrepancy": round(self.strategy_discrepancy, 4),
            "portfolio_gross_pnl": round(self.portfolio_gross_pnl, 2),
            "portfolio_total_costs": round(self.portfolio_total_costs, 4),
            "gross_minus_costs": round(self.gross_minus_costs, 2),
            "gross_to_net_discrepancy": round(self.gross_to_net_discrepancy, 4),
            "is_reconciled": self.is_reconciled,
            "tolerance": self.tolerance,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class PortfolioAttributionReport:
    """Complete, immutable performance attribution and risk-adjusted analytics report."""

    report_id: str
    source_type: str
    source_id: str
    created_at: float
    initial_equity: float
    final_equity: float
    gross_pnl: float
    net_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    gross_return_pct: float
    net_return_pct: float
    cash_balance: float
    total_costs: TransactionCostAttribution
    risk_adjusted_metrics: RiskAdjustedMetrics
    strategy_attributions: Tuple[StrategyAttribution, ...] = field(default_factory=tuple)
    instrument_attributions: Tuple[InstrumentAttribution, ...] = field(default_factory=tuple)
    reconciliation: Optional[ReconciliationReport] = None
    disclaimer: str = (
        "RESEARCH ONLY: Performance attribution and risk-adjusted metrics are purely descriptive "
        "and evaluate hypothetical historical simulations. They do NOT guarantee future profitability, "
        "protect against drawdown, or predict live execution outcomes."
    )
    run_id: Optional[str] = None

    @property
    def summary_text(self) -> str:
        """Concise human-readable performance attribution summary."""
        sign = "+" if self.net_pnl >= 0 else ""
        sharpe_str = (
            f"{self.risk_adjusted_metrics.sharpe_ratio.value:.2f}"
            if self.risk_adjusted_metrics.sharpe_ratio.is_available
            else "N/A"
        )
        sortino_str = (
            f"{self.risk_adjusted_metrics.sortino_ratio.value:.2f}"
            if self.risk_adjusted_metrics.sortino_ratio.is_available
            else "N/A"
        )
        vol_str = (
            f"{self.risk_adjusted_metrics.annualized_volatility_pct.value:.2f}%"
            if self.risk_adjusted_metrics.annualized_volatility_pct.is_available
            else "N/A"
        )
        recon_status = self.reconciliation.status.value if self.reconciliation else "N/A"
        return (
            f"Attribution [{self.report_id}] ({self.source_type}:{self.source_id}): "
            f"Equity: ${self.initial_equity:,.2f} -> ${self.final_equity:,.2f} | "
            f"Net PnL: {sign}${self.net_pnl:,.2f} ({sign}{self.net_return_pct:.2f}%) | "
            f"Sharpe={sharpe_str}, Sortino={sortino_str}, Vol={vol_str} | "
            f"Costs: ${self.total_costs.total_costs:,.2f} | "
            f"Strategies={len(self.strategy_attributions)}, Instruments={len(self.instrument_attributions)} | "
            f"Reconciliation={recon_status}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize complete report to dictionary."""
        return {
            "report_id": self.report_id,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "created_at": self.created_at,
            "initial_equity": round(self.initial_equity, 2),
            "final_equity": round(self.final_equity, 2),
            "gross_pnl": round(self.gross_pnl, 2),
            "net_pnl": round(self.net_pnl, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "gross_return_pct": round(self.gross_return_pct, 4),
            "net_return_pct": round(self.net_return_pct, 4),
            "cash_balance": round(self.cash_balance, 2),
            "total_costs": self.total_costs.to_dict(),
            "risk_adjusted_metrics": self.risk_adjusted_metrics.to_dict(),
            "strategy_attributions": [s.to_dict() for s in self.strategy_attributions],
            "instrument_attributions": [i.to_dict() for i in self.instrument_attributions],
            "reconciliation": self.reconciliation.to_dict() if self.reconciliation else None,
            "summary_text": self.summary_text,
            "disclaimer": self.disclaimer,
            "run_id": self.run_id,
        }


@dataclass(frozen=True)
class ComparisonEntity:
    """Individual entity entry inside a multi-run or multi-strategy comparison report."""

    entity_id: str
    entity_name: str
    entity_type: str
    metrics: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert comparison entity to dictionary."""
        return {
            "entity_id": self.entity_id,
            "entity_name": self.entity_name,
            "entity_type": self.entity_type,
            "metrics": dict(self.metrics),
        }


@dataclass(frozen=True)
class ComparisonReport:
    """Structured report comparing performance across runs, strategies, windows, or scenarios."""

    comparison_id: str
    comparison_type: ComparisonType
    created_at: float
    entities: Tuple[ComparisonEntity, ...] = field(default_factory=tuple)
    summary: Dict[str, Any] = field(default_factory=dict)
    disclaimer: str = (
        "RESEARCH ONLY: Comparative performance analytics evaluate simulated historical variations. "
        "Differences do NOT prove that an outperforming strategy or scenario will persist in live markets."
    )

    @property
    def summary_text(self) -> str:
        """Formatted comparison summary text."""
        return (
            f"Comparison [{self.comparison_id}] ({self.comparison_type.value}): "
            f"Compared {len(self.entities)} entities. {self.summary.get('headline', '')}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert comparison report to dictionary."""
        return {
            "comparison_id": self.comparison_id,
            "comparison_type": self.comparison_type.value,
            "created_at": self.created_at,
            "entities_count": len(self.entities),
            "entities": [e.to_dict() for e in self.entities],
            "summary": dict(self.summary),
            "summary_text": self.summary_text,
            "disclaimer": self.disclaimer,
        }
