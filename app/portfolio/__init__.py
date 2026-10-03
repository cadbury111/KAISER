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
from app.portfolio.report_models import (
    DashboardSeriesPoint,
    DashboardTimeSeries,
    HistoricalReportSummary,
    ReportDataProvenance,
    ReportDataStatus,
    UnifiedPortfolioReport,
    VoiceSummary,
)
from app.portfolio.dashboard import PortfolioDashboardServer, get_dashboard_html
from app.portfolio.reporting import PortfolioReportingService
from app.portfolio.reporting_handlers import register_reporting_handlers

__all__ = [
    # Enums
    "AttributionLevel",
    "ReconciliationStatus",
    "ComparisonType",
    "ReportDataStatus",
    # Attribution Models
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
    # Reporting Models
    "ReportDataProvenance",
    "DashboardSeriesPoint",
    "DashboardTimeSeries",
    "VoiceSummary",
    "HistoricalReportSummary",
    "UnifiedPortfolioReport",
    # Metrics
    "calculate_equity_returns",
    "calculate_annualized_volatility",
    "calculate_sharpe_ratio",
    "calculate_sortino_ratio",
    "calculate_max_drawdown",
    "calculate_calmar_ratio",
    "calculate_trade_metrics",
    "compute_risk_adjusted_metrics",
    # Services, Server & Handlers
    "PortfolioAttributionService",
    "PortfolioReportingService",
    "PortfolioDashboardServer",
    "get_dashboard_html",
    "register_portfolio_handlers",
    "register_reporting_handlers",
]


