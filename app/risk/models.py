"""KAISER — Portfolio Risk Monitoring & Alerting Domain Models.

Provides strongly typed, immutable dataclasses and enumerations for risk rules,
severity levels, alert lifecycle states, configurable monitoring thresholds,
structured risk alerts, and comprehensive risk status reports.

SAFETY INVARIANTS:
1. Purely informational: risk alerts report metrics and breaches, but NEVER place trades or modify positions.
2. Complete balance & data integrity: missing, stale, or corrupt data is flagged explicitly, never faked as healthy.
3. Deterministic state transitions: alert lifecycle moves through explicit states (NORMAL, WARNING, CRITICAL, RECOVERED, UNKNOWN).
4. Cooldown and hysteresis: prevents alert storming while ensuring new critical breaches are immediately broadcast.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.exceptions import ValidationError


class RiskRuleId(str, Enum):
    """Categorical identifier for portfolio risk monitoring rules."""

    GROSS_EXPOSURE = "GROSS_EXPOSURE"
    NET_EXPOSURE = "NET_EXPOSURE"
    CASH_RESERVE = "CASH_RESERVE"
    STRATEGY_EXPOSURE = "STRATEGY_EXPOSURE"
    INSTRUMENT_EXPOSURE = "INSTRUMENT_EXPOSURE"
    INSTRUMENT_CONCENTRATION = "INSTRUMENT_CONCENTRATION"
    DRAWDOWN = "DRAWDOWN"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"
    SNAPSHOT_FRESHNESS = "SNAPSHOT_FRESHNESS"
    DATA_COMPLETENESS = "DATA_COMPLETENESS"


class AlertSeverity(str, Enum):
    """Severity classification of a portfolio risk alert."""

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertState(str, Enum):
    """Lifecycle transition state of a monitored risk rule or overall portfolio."""

    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    RECOVERED = "RECOVERED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class PortfolioRiskMonitorConfig:
    """Configurable threshold parameters for deterministic portfolio risk monitoring.

    Attributes:
        max_gross_exposure_warning_pct: Warning threshold for gross exposure as % of equity.
        max_gross_exposure_critical_pct: Critical threshold for gross exposure as % of equity.
        max_net_exposure_warning_pct: Warning threshold for net exposure as % of equity.
        max_net_exposure_critical_pct: Critical threshold for net exposure as % of equity.
        min_cash_reserve_warning_pct: Warning threshold when cash reserve falls below this %.
        min_cash_reserve_critical_pct: Critical threshold when cash reserve falls below this %.
        max_strategy_exposure_warning_pct: Warning threshold for single-strategy exposure as % of equity.
        max_strategy_exposure_critical_pct: Critical threshold for single-strategy exposure as % of equity.
        max_instrument_concentration_warning_pct: Warning threshold for single-instrument concentration.
        max_instrument_concentration_critical_pct: Critical threshold for single-instrument concentration.
        max_drawdown_warning_pct: Warning threshold for peak-to-trough drawdown %.
        max_drawdown_critical_pct: Critical threshold for peak-to-trough drawdown %.
        max_snapshot_age_seconds: Maximum tolerable age of a portfolio snapshot before it is marked STALE.
        deduplication_cooldown_seconds: Cooldown interval to suppress duplicate alerts for unchanged conditions.
        hysteresis_pct: Buffer percentage required for a recovering metric to prevent boundary oscillation.
    """

    max_gross_exposure_warning_pct: float = 80.0
    max_gross_exposure_critical_pct: float = 100.0
    max_net_exposure_warning_pct: float = 70.0
    max_net_exposure_critical_pct: float = 90.0
    min_cash_reserve_warning_pct: float = 15.0
    min_cash_reserve_critical_pct: float = 5.0
    max_strategy_exposure_warning_pct: float = 40.0
    max_strategy_exposure_critical_pct: float = 50.0
    max_instrument_concentration_warning_pct: float = 25.0
    max_instrument_concentration_critical_pct: float = 35.0
    max_drawdown_warning_pct: float = 10.0
    max_drawdown_critical_pct: float = 15.0
    max_snapshot_age_seconds: float = 300.0
    deduplication_cooldown_seconds: float = 60.0
    hysteresis_pct: float = 1.0

    def __post_init__(self) -> None:
        """Validate threshold ordering and reasonable ranges."""
        # 1. Gross exposure
        if self.max_gross_exposure_warning_pct <= 0:
            raise ValidationError("max_gross_exposure_warning_pct must be strictly positive.")
        if self.max_gross_exposure_warning_pct >= self.max_gross_exposure_critical_pct:
            raise ValidationError(
                f"Gross exposure warning ({self.max_gross_exposure_warning_pct}%) must be strictly "
                f"less than critical ({self.max_gross_exposure_critical_pct}%)."
            )

        # 2. Net exposure
        if self.max_net_exposure_warning_pct <= 0:
            raise ValidationError("max_net_exposure_warning_pct must be strictly positive.")
        if self.max_net_exposure_warning_pct >= self.max_net_exposure_critical_pct:
            raise ValidationError(
                f"Net exposure warning ({self.max_net_exposure_warning_pct}%) must be strictly "
                f"less than critical ({self.max_net_exposure_critical_pct}%)."
            )

        # 3. Cash reserve (lower is worse)
        if self.min_cash_reserve_critical_pct < 0:
            raise ValidationError("min_cash_reserve_critical_pct cannot be negative.")
        if self.min_cash_reserve_critical_pct >= self.min_cash_reserve_warning_pct:
            raise ValidationError(
                f"Cash reserve critical ({self.min_cash_reserve_critical_pct}%) must be strictly "
                f"less than warning ({self.min_cash_reserve_warning_pct}%)."
            )

        # 4. Strategy exposure
        if self.max_strategy_exposure_warning_pct <= 0:
            raise ValidationError("max_strategy_exposure_warning_pct must be strictly positive.")
        if self.max_strategy_exposure_warning_pct >= self.max_strategy_exposure_critical_pct:
            raise ValidationError(
                f"Strategy exposure warning ({self.max_strategy_exposure_warning_pct}%) must be strictly "
                f"less than critical ({self.max_strategy_exposure_critical_pct}%)."
            )

        # 5. Instrument concentration
        if self.max_instrument_concentration_warning_pct <= 0:
            raise ValidationError("max_instrument_concentration_warning_pct must be strictly positive.")
        if self.max_instrument_concentration_warning_pct >= self.max_instrument_concentration_critical_pct:
            raise ValidationError(
                f"Instrument concentration warning ({self.max_instrument_concentration_warning_pct}%) must be strictly "
                f"less than critical ({self.max_instrument_concentration_critical_pct}%)."
            )

        # 6. Drawdown
        if self.max_drawdown_warning_pct <= 0:
            raise ValidationError("max_drawdown_warning_pct must be strictly positive.")
        if self.max_drawdown_warning_pct >= self.max_drawdown_critical_pct:
            raise ValidationError(
                f"Drawdown warning ({self.max_drawdown_warning_pct}%) must be strictly "
                f"less than critical ({self.max_drawdown_critical_pct}%)."
            )

        # 7. Time and cooldown parameters
        if self.max_snapshot_age_seconds <= 0:
            raise ValidationError("max_snapshot_age_seconds must be strictly positive.")
        if self.deduplication_cooldown_seconds < 0:
            raise ValidationError("deduplication_cooldown_seconds cannot be negative.")
        if self.hysteresis_pct < 0:
            raise ValidationError("hysteresis_pct cannot be negative.")

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "max_gross_exposure_warning_pct": self.max_gross_exposure_warning_pct,
            "max_gross_exposure_critical_pct": self.max_gross_exposure_critical_pct,
            "max_net_exposure_warning_pct": self.max_net_exposure_warning_pct,
            "max_net_exposure_critical_pct": self.max_net_exposure_critical_pct,
            "min_cash_reserve_warning_pct": self.min_cash_reserve_warning_pct,
            "min_cash_reserve_critical_pct": self.min_cash_reserve_critical_pct,
            "max_strategy_exposure_warning_pct": self.max_strategy_exposure_warning_pct,
            "max_strategy_exposure_critical_pct": self.max_strategy_exposure_critical_pct,
            "max_instrument_concentration_warning_pct": self.max_instrument_concentration_warning_pct,
            "max_instrument_concentration_critical_pct": self.max_instrument_concentration_critical_pct,
            "max_drawdown_warning_pct": self.max_drawdown_warning_pct,
            "max_drawdown_critical_pct": self.max_drawdown_critical_pct,
            "max_snapshot_age_seconds": self.max_snapshot_age_seconds,
            "deduplication_cooldown_seconds": self.deduplication_cooldown_seconds,
            "hysteresis_pct": self.hysteresis_pct,
        }


@dataclass(frozen=True)
class PortfolioRiskAlert:
    """Immutable, typed risk alert emitted when a portfolio rule is breached or recovered.

    Attributes:
        alert_id: Unique alert event identifier.
        rule_id: Categorical RiskRuleId string.
        severity: AlertSeverity (INFO, WARNING, CRITICAL).
        state: AlertState (WARNING, CRITICAL, RECOVERED, UNKNOWN).
        metric_name: Monitored metric name (e.g. 'gross_exposure_pct').
        observed_value: Value observed during monitoring evaluation.
        threshold_value: Target threshold value that was breached.
        unit: Measurement unit (e.g. '%', 'USD', 's').
        timestamp: Time of alert evaluation.
        target: Affected entity ('portfolio', 'strategy:name', 'instrument:symbol').
        message: Concise, human-readable explanatory message.
        evidence: Contextual dictionary supporting the alert determination.
        source_ref: Optional identifier of the source snapshot or simulation.
        recovery_details: Details recorded if this alert represents a recovery.
    """

    alert_id: str
    rule_id: str
    severity: AlertSeverity
    state: AlertState
    metric_name: str
    observed_value: Optional[float]
    threshold_value: Optional[float]
    unit: str = "%"
    timestamp: float = 0.0
    target: str = "portfolio"
    message: str = ""
    evidence: Dict[str, Any] = field(default_factory=dict)
    source_ref: Optional[str] = None
    recovery_details: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if not self.alert_id:
            object.__setattr__(self, "alert_id", f"alert_{uuid.uuid4().hex[:12]}")
        if self.timestamp <= 0.0:
            object.__setattr__(self, "timestamp", time.time())

    @property
    def key(self) -> str:
        """Deterministic unique deduplication key for this rule and target."""
        return f"{self.rule_id}:{self.target}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "rule_id": self.rule_id if isinstance(self.rule_id, str) else self.rule_id.value,
            "severity": self.severity.value,
            "state": self.state.value,
            "metric_name": self.metric_name,
            "observed_value": round(self.observed_value, 4) if self.observed_value is not None else None,
            "threshold_value": round(self.threshold_value, 4) if self.threshold_value is not None else None,
            "unit": self.unit,
            "timestamp": self.timestamp,
            "target": self.target,
            "message": self.message,
            "evidence": dict(self.evidence),
            "source_ref": self.source_ref,
            "recovery_details": dict(self.recovery_details) if self.recovery_details else None,
        }


@dataclass(frozen=True)
class PortfolioRiskReport:
    """Comprehensive, immutable status report synthesized across all evaluated risk rules.

    Attributes:
        report_id: Unique report identifier.
        timestamp: Time of evaluation.
        overall_state: Synthesized portfolio risk state (NORMAL, WARNING, CRITICAL, UNKNOWN).
        active_alerts: Tuple of currently active un-cleared alerts.
        cleared_alerts: Tuple of alerts that recovered during this evaluation cycle.
        metrics_evaluated: Key-value dictionary of evaluated metric values.
        snapshot_timestamp: Original timestamp of the underlying portfolio snapshot.
        snapshot_age_seconds: Age in seconds of the underlying snapshot at evaluation time.
        is_snapshot_stale: True if snapshot age exceeded max_snapshot_age_seconds.
        summary_text: Human/voice readable risk overview.
        disclaimer: Safety reminder that monitoring is informational only.
    """

    report_id: str
    timestamp: float
    overall_state: AlertState
    active_alerts: Tuple[PortfolioRiskAlert, ...] = field(default_factory=tuple)
    cleared_alerts: Tuple[PortfolioRiskAlert, ...] = field(default_factory=tuple)
    metrics_evaluated: Dict[str, Any] = field(default_factory=dict)
    snapshot_timestamp: Optional[float] = None
    snapshot_age_seconds: Optional[float] = None
    is_snapshot_stale: bool = False
    summary_text: str = ""
    disclaimer: str = (
        "RESEARCH ONLY: Portfolio risk monitoring is purely observational and informational. "
        "It does NOT place orders, connect to a broker, or automatically liquidate positions."
    )

    @property
    def is_healthy(self) -> bool:
        """True if the portfolio is in a clean NORMAL state with no active alerts."""
        return self.overall_state == AlertState.NORMAL and len(self.active_alerts) == 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "report_id": self.report_id,
            "timestamp": self.timestamp,
            "overall_state": self.overall_state.value,
            "active_alerts_count": len(self.active_alerts),
            "cleared_alerts_count": len(self.cleared_alerts),
            "active_alerts": [a.to_dict() for a in self.active_alerts],
            "cleared_alerts": [a.to_dict() for a in self.cleared_alerts],
            "metrics_evaluated": dict(self.metrics_evaluated),
            "snapshot_timestamp": self.snapshot_timestamp,
            "snapshot_age_seconds": round(self.snapshot_age_seconds, 2) if self.snapshot_age_seconds is not None else None,
            "is_snapshot_stale": self.is_snapshot_stale,
            "summary_text": self.summary_text,
            "disclaimer": self.disclaimer,
        }
