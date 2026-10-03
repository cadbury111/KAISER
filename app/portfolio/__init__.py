"""KAISER Portfolio Management & Attribution Package.

Tracks current holdings, asset allocation, cash balances, unrealized/realized PnL,
multi-strategy performance attribution, and risk-adjusted metrics.
"""

from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.handlers import register_portfolio_handlers
from app.portfolio.metrics import (
    calculate_annualized_volatility,
    calculate_calmar_ratio,
    calculate_equity_returns,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
    calculate_sortino_ratio,
    calculate_trade_metrics,
    compute_risk_adjusted_metrics,
)
from app.portfolio.models import (
    AttributionConfig,
    AttributionLevel,
    ComparisonEntity,
    ComparisonReport,
    ComparisonType,
    InstrumentAttribution,
    MetricValue,
    PortfolioAttributionReport,
    ReconciliationReport,
    ReconciliationStatus,
    RiskAdjustedMetrics,
    StrategyAttribution,
    TransactionCostAttribution,
)

__all__ = [
    # Enums
    "AttributionLevel",
    "ReconciliationStatus",
    "ComparisonType",
    # Models
    "MetricValue",
    "AttributionConfig",
    "RiskAdjustedMetrics",
    "TransactionCostAttribution",
    "StrategyAttribution",
    "InstrumentAttribution",
    "ReconciliationReport",
    "PortfolioAttributionReport",
    "ComparisonEntity",
    "ComparisonReport",
    # Metrics
    "calculate_equity_returns",
    "calculate_annualized_volatility",
    "calculate_sharpe_ratio",
    "calculate_sortino_ratio",
    "calculate_max_drawdown",
    "calculate_calmar_ratio",
    "calculate_trade_metrics",
    "compute_risk_adjusted_metrics",
    # Service & Handlers
    "PortfolioAttributionService",
    "register_portfolio_handlers",
]
