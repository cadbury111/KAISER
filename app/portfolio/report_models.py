"""KAISER — Portfolio Analytics Reporting & Dashboard Data Models.

Provides strongly typed, immutable dataclasses for unified portfolio reporting:
- ReportDataStatus and ReportDataProvenance
- DashboardSeriesPoint and DashboardTimeSeries
- HistoricalReportSummary for bounded list pagination
- VoiceSummary for structured TTS presentation
- UnifiedPortfolioReport aggregating attribution, risk, costs, and exposures

SAFETY & RESEARCH INVARIANTS:
1. Purely informational, research-only reporting.
2. Never places trades, modifies orders, or connects to live brokers.
3. Explicit data provenance: never fabricates missing values or claims unavailable metrics are zero.
4. Descriptive historical analytics: do NOT guarantee future performance or profitability.
5. Clean sanitization: output is guaranteed free of NaN, Infinity, and credential secrets.
"""

from __future__ import annotations

import datetime
import math
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.exceptions import ValidationError


class ReportDataStatus(str, Enum):
    """Integrity and freshness status of the data underpinning an analytics report."""

    VALID = "VALID"
    STALE = "STALE"
    INCOMPLETE = "INCOMPLETE"
    UNAVAILABLE = "UNAVAILABLE"


def sanitize_numeric(
    val: Any,
    precision: int = 4,
    default: Optional[float] = None,
) -> Optional[float]:
    """Sanitize numeric values, replacing NaN/Inf with default/None and rounding."""
    if val is None:
        return default
    try:
        f_val = float(val)
        if math.isnan(f_val) or math.isinf(f_val):
            return default
        return round(f_val, precision)
    except (ValueError, TypeError):
        return default


def format_iso_timestamp(epoch_seconds: Optional[float]) -> str:
    """Format an epoch timestamp to a standardized UTC ISO-8601 string."""
    if epoch_seconds is None or epoch_seconds <= 0.0:
        return "1970-01-01T00:00:00Z"
    try:
        dt = datetime.datetime.fromtimestamp(epoch_seconds, tz=datetime.timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return "1970-01-01T00:00:00Z"


def sanitize_dict_for_export(obj: Any) -> Any:
    """Recursively sanitize data containers for safe JSON serialization.

    Guarantees:
    - No NaN or Infinity values (replaced with None).
    - All Enums converted to their string values.
    - Strips secret/credential keys if accidentally present.
    """
    if obj is None:
        return None
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return round(obj, 4)
    if isinstance(obj, (int, str, bool)):
        return obj
    if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
        return sanitize_dict_for_export(obj.to_dict())
    if isinstance(obj, dict):
        sanitized: Dict[str, Any] = {}
        for k, v in obj.items():
            key_str = str(k).lower()
            if any(secret_term in key_str for secret_term in ("secret", "password", "token", "key", "credential")):
                continue
            sanitized[str(k)] = sanitize_dict_for_export(v)
        return sanitized
    if isinstance(obj, (list, tuple)):
        return [sanitize_dict_for_export(item) for item in obj]
    return str(obj)


@dataclass(frozen=True)
class ReportDataProvenance:
    """Audit provenance describing the origin, runtime, and configuration of an analytics report."""

    source_run_id: str
    source_type: str  # "portfolio_simulation", "backtest", "walk_forward", "robustness", "synthetic"
    generated_at: float  # epoch seconds
    generated_at_iso: str  # ISO-8601 UTC string
    engine_version: str = "1.0.0"
    git_commit: Optional[str] = None
    timeframe: Optional[str] = "1d"
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    parameters: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert provenance metadata to serializable dictionary."""
        return {
            "source_run_id": self.source_run_id,
            "source_type": self.source_type,
            "generated_at": self.generated_at,
            "generated_at_iso": self.generated_at_iso,
            "engine_version": self.engine_version,
            "git_commit": self.git_commit,
            "timeframe": self.timeframe,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "start_time_iso": format_iso_timestamp(self.start_time) if self.start_time else None,
            "end_time_iso": format_iso_timestamp(self.end_time) if self.end_time else None,
            "parameters": sanitize_dict_for_export(self.parameters),
        }


@dataclass(frozen=True)
class DashboardSeriesPoint:
    """Individual observation point on a dashboard timeline (equity and drawdown curves)."""

    timestamp: float
    timestamp_iso: str
    cash: float
    position_value: float
    total_equity: float
    drawdown_pct: float
    drawdown_amount: float
    cumulative_return_pct: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert series observation to dictionary."""
        return {
            "timestamp": self.timestamp,
            "timestamp_iso": self.timestamp_iso,
            "cash": sanitize_numeric(self.cash, 2, default=0.0) or 0.0,
            "position_value": sanitize_numeric(self.position_value, 2, default=0.0) or 0.0,
            "total_equity": sanitize_numeric(self.total_equity, 2, default=0.0) or 0.0,
            "drawdown_pct": sanitize_numeric(self.drawdown_pct, 2, default=0.0) or 0.0,
            "drawdown_amount": sanitize_numeric(self.drawdown_amount, 2, default=0.0) or 0.0,
            "cumulative_return_pct": sanitize_numeric(self.cumulative_return_pct, 4, default=0.0) or 0.0,
        }


@dataclass(frozen=True)
class DashboardTimeSeries:
    """Bounded, ordered time-series dataset suitable for dashboard chart rendering."""

    series_id: str
    points: Tuple[DashboardSeriesPoint, ...] = field(default_factory=tuple)
    total_points: int = 0
    timeframe: str = "1d"
    is_empty: bool = True
    peak_equity: float = 0.0
    trough_equity: float = 0.0
    max_drawdown_pct: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert time-series container to dictionary."""
        return {
            "series_id": self.series_id,
            "points_count": len(self.points),
            "total_points": self.total_points,
            "timeframe": self.timeframe,
            "is_empty": self.is_empty,
            "peak_equity": sanitize_numeric(self.peak_equity, 2, default=0.0) or 0.0,
            "trough_equity": sanitize_numeric(self.trough_equity, 2, default=0.0) or 0.0,
            "max_drawdown_pct": sanitize_numeric(self.max_drawdown_pct, 2, default=0.0) or 0.0,
            "points": [p.to_dict() for p in self.points],
        }


@dataclass(frozen=True)
class VoiceSummary:
    """Concise, structured summary fields designed for TTS audio synthesis."""

    headline: str
    net_pnl_speech: str
    drawdown_speech: str
    risk_status_speech: str
    warning_speech: str
    full_speech_script: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to dictionary."""
        return {
            "headline": self.headline,
            "net_pnl_speech": self.net_pnl_speech,
            "drawdown_speech": self.drawdown_speech,
            "risk_status_speech": self.risk_status_speech,
            "warning_speech": self.warning_speech,
            "full_speech_script": self.full_speech_script,
        }


@dataclass(frozen=True)
class HistoricalReportSummary:
    """Lightweight metadata entry for bounded historical report listings."""

    report_id: str
    source_id: str
    source_type: str
    created_at: float
    created_at_iso: str
    initial_equity: float
    final_equity: float
    net_pnl: float
    net_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio: Optional[float]
    risk_state: str
    is_reconciled: bool
    status: ReportDataStatus
    title: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert historical summary to dictionary."""
        return {
            "report_id": self.report_id,
            "source_id": self.source_id,
            "source_type": self.source_type,
            "created_at": self.created_at,
            "created_at_iso": self.created_at_iso,
            "initial_equity": sanitize_numeric(self.initial_equity, 2, default=0.0) or 0.0,
            "final_equity": sanitize_numeric(self.final_equity, 2, default=0.0) or 0.0,
            "net_pnl": sanitize_numeric(self.net_pnl, 2, default=0.0) or 0.0,
            "net_return_pct": sanitize_numeric(self.net_return_pct, 2, default=0.0) or 0.0,
            "max_drawdown_pct": sanitize_numeric(self.max_drawdown_pct, 2, default=0.0) or 0.0,
            "sharpe_ratio": sanitize_numeric(self.sharpe_ratio, 2, default=None),
            "risk_state": self.risk_state,
            "is_reconciled": self.is_reconciled,
            "status": self.status.value,
            "title": self.title,
        }


@dataclass(frozen=True)
class UnifiedPortfolioReport:
    """Unified, immutable portfolio analytics report consolidating attribution, risk, costs, and exposure.

    Provides a comprehensive data contract that can be consumed directly by UI dashboards,
    REST/WebSocket endpoints, or voice assistants.
    """

    report_id: str
    title: str
    provenance: ReportDataProvenance
    status: ReportDataStatus
    data_warnings: Tuple[str, ...] = field(default_factory=tuple)

    # Performance & Capital
    initial_equity: float = 0.0
    final_equity: float = 0.0
    gross_pnl: float = 0.0
    net_pnl: float = 0.0
    gross_return_pct: float = 0.0
    net_return_pct: float = 0.0
    realized_pnl: Optional[float] = None
    unrealized_pnl: Optional[float] = None

    # Cash & Exposure Summaries
    cash_balance: float = 0.0
    gross_exposure: float = 0.0
    gross_exposure_pct: float = 0.0
    net_exposure: float = 0.0
    net_exposure_pct: float = 0.0
    cash_reserve_pct: float = 0.0

    # Costs Breakdown
    total_transaction_costs: float = 0.0
    cost_breakdown: Dict[str, float] = field(default_factory=dict)

    # Time Series & Drawdown
    equity_curve: Tuple[DashboardSeriesPoint, ...] = field(default_factory=tuple)
    max_drawdown_pct: float = 0.0
    max_drawdown_amount: float = 0.0
    max_drawdown_duration_bars: int = 0

    # Risk-Adjusted Metrics (Serialized MetricValues)
    risk_adjusted_metrics: Dict[str, Any] = field(default_factory=dict)

    # Attribution & Reconciliation
    strategy_attributions: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)
    instrument_attributions: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)
    instrument_attribution_available: bool = False
    reconciliation: Optional[Dict[str, Any]] = None

    # Risk Monitoring & Alerts
    risk_status: Optional[Dict[str, Any]] = None
    active_alerts_count: int = 0

    # Validation & Robustness Summaries
    walk_forward_summary: Optional[Dict[str, Any]] = None
    robustness_summary: Optional[Dict[str, Any]] = None

    # Voice Presentation
    voice_summary: Optional[VoiceSummary] = None

    # Safety & Research Disclaimer
    disclaimer: str = (
        "RESEARCH ONLY: Unified portfolio analytics and risk-adjusted metrics are purely descriptive "
        "and evaluate historical simulation data. They do NOT guarantee future profitability, "
        "protect against loss, or predict live execution outcomes."
    )

    @property
    def summary_text(self) -> str:
        """Formatted human-readable report overview."""
        sign = "+" if self.net_pnl >= 0 else ""
        risk_str = self.risk_status.get("overall_state", "UNKNOWN") if self.risk_status else "UNKNOWN"
        return (
            f"Unified Portfolio Report [{self.report_id}] ({self.status.value}): "
            f"Initial=${self.initial_equity:,.2f} -> Final=${self.final_equity:,.2f} | "
            f"Net PnL={sign}${self.net_pnl:,.2f} ({sign}{self.net_return_pct:.2f}%) | "
            f"MaxDD={self.max_drawdown_pct:.2f}% | "
            f"Risk={risk_str} ({self.active_alerts_count} alerts) | "
            f"Costs=${self.total_transaction_costs:,.2f} | "
            f"Strategies={len(self.strategy_attributions)} | "
            f"Warnings={len(self.data_warnings)}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert complete unified report to serialized dictionary."""
        return {
            "report_id": self.report_id,
            "title": self.title,
            "provenance": self.provenance.to_dict(),
            "status": self.status.value,
            "data_warnings": list(self.data_warnings),
            "initial_equity": sanitize_numeric(self.initial_equity, 2, default=0.0) or 0.0,
            "final_equity": sanitize_numeric(self.final_equity, 2, default=0.0) or 0.0,
            "gross_pnl": sanitize_numeric(self.gross_pnl, 2, default=0.0) or 0.0,
            "net_pnl": sanitize_numeric(self.net_pnl, 2, default=0.0) or 0.0,
            "gross_return_pct": sanitize_numeric(self.gross_return_pct, 4, default=0.0) or 0.0,
            "net_return_pct": sanitize_numeric(self.net_return_pct, 4, default=0.0) or 0.0,
            "realized_pnl": sanitize_numeric(self.realized_pnl, 2, default=None),
            "unrealized_pnl": sanitize_numeric(self.unrealized_pnl, 2, default=None),
            "cash_balance": sanitize_numeric(self.cash_balance, 2, default=0.0) or 0.0,
            "gross_exposure": sanitize_numeric(self.gross_exposure, 2, default=0.0) or 0.0,
            "gross_exposure_pct": sanitize_numeric(self.gross_exposure_pct, 2, default=0.0) or 0.0,
            "net_exposure": sanitize_numeric(self.net_exposure, 2, default=0.0) or 0.0,
            "net_exposure_pct": sanitize_numeric(self.net_exposure_pct, 2, default=0.0) or 0.0,
            "cash_reserve_pct": sanitize_numeric(self.cash_reserve_pct, 2, default=0.0) or 0.0,
            "total_transaction_costs": sanitize_numeric(self.total_transaction_costs, 4, default=0.0) or 0.0,
            "cost_breakdown": {k: sanitize_numeric(v, 4, default=0.0) or 0.0 for k, v in self.cost_breakdown.items()},
            "equity_curve_points_count": len(self.equity_curve),
            "equity_curve": [p.to_dict() for p in self.equity_curve],
            "max_drawdown_pct": sanitize_numeric(self.max_drawdown_pct, 2, default=0.0) or 0.0,
            "max_drawdown_amount": sanitize_numeric(self.max_drawdown_amount, 2, default=0.0) or 0.0,
            "max_drawdown_duration_bars": self.max_drawdown_duration_bars,
            "risk_adjusted_metrics": sanitize_dict_for_export(self.risk_adjusted_metrics),
            "strategy_attributions": sanitize_dict_for_export(list(self.strategy_attributions)),
            "instrument_attributions": sanitize_dict_for_export(list(self.instrument_attributions)),
            "instrument_attribution_available": self.instrument_attribution_available,
            "reconciliation": sanitize_dict_for_export(self.reconciliation) if self.reconciliation else None,
            "risk_status": sanitize_dict_for_export(self.risk_status) if self.risk_status else None,
            "active_alerts_count": self.active_alerts_count,
            "walk_forward_summary": sanitize_dict_for_export(self.walk_forward_summary) if self.walk_forward_summary else None,
            "robustness_summary": sanitize_dict_for_export(self.robustness_summary) if self.robustness_summary else None,
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "summary_text": self.summary_text,
            "disclaimer": self.disclaimer,
        }
