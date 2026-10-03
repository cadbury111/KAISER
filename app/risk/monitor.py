"""KAISER — Portfolio Risk Monitoring & Alerting Engine.

Implements an independent, deterministic monitoring service that continuously evaluates
portfolio snapshots and simulation results against configured risk thresholds.
Emits structured alerts with hysteresis and cooldown deduplication over EventBus,
and persists real-time risk health in StateStore.

SAFETY INVARIANTS:
1. Strictly observational: never executes orders, modifies holdings, or connects to brokers.
2. Complete data integrity: missing or stale snapshots are reported explicitly as UNKNOWN/CRITICAL.
3. Hysteresis & cooldown: prevents alert storming while ensuring new critical breaches broadcast immediately.
4. Independent failure containment: subscriber or state errors cannot crash risk monitoring.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from app.backtesting.portfolio_simulation.models import (
    PortfolioRiskSnapshot,
    PortfolioSimulationResult,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.logging_config import get_logger
from app.core.state.store import StateStore
from app.risk.models import (
    AlertSeverity,
    AlertState,
    PortfolioRiskAlert,
    PortfolioRiskMonitorConfig,
    PortfolioRiskReport,
    RiskRuleId,
)

logger = get_logger("risk.portfolio_monitor")


class PortfolioRiskMonitor:
    """Dedicated deterministic portfolio risk monitoring and alerting service."""

    def __init__(
        self,
        config: Optional[PortfolioRiskMonitorConfig] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        clock_fn: Optional[Callable[[], float]] = None,
        name: str = "portfolio_risk_monitor",
    ) -> None:
        self.config = config or PortfolioRiskMonitorConfig()
        self.event_bus = event_bus
        self.state_store = state_store
        self.clock_fn: Callable[[], float] = clock_fn or time.time
        self.name = name

        self._lock = threading.RLock()
        # Active alerts mapping key (f"{rule_id}:{target}") -> PortfolioRiskAlert
        self._active_alerts: Dict[str, PortfolioRiskAlert] = {}
        # Last broadcast timestamp per alert key for cooldown deduplication
        self._last_broadcast_times: Dict[str, float] = {}
        self._last_report: Optional[PortfolioRiskReport] = None

    def reset(self) -> None:
        """Reset internal active alert and deduplication caches."""
        with self._lock:
            self._active_alerts.clear()
            self._last_broadcast_times.clear()
            self._last_report = None
            logger.info("PortfolioRiskMonitor state reset.")

    def get_active_alerts(self) -> Tuple[PortfolioRiskAlert, ...]:
        """Return currently active un-cleared alerts."""
        with self._lock:
            return tuple(self._active_alerts.values())

    def get_latest_report(self) -> Optional[PortfolioRiskReport]:
        """Return the most recently evaluated risk report."""
        with self._lock:
            return self._last_report

    def evaluate_snapshot(
        self,
        snapshot: Optional[PortfolioRiskSnapshot],
        source_ref: str = "",
        correlation_id: Optional[str] = None,
    ) -> PortfolioRiskReport:
        """Evaluate a PortfolioRiskSnapshot against configured risk thresholds.

        Handles missing or stale snapshots explicitly, evaluates exposure, cash reserve,
        concentration, and drawdown limits with deterministic hysteresis and cooldown.
        """
        now = self.clock_fn()
        cid = correlation_id or f"risk_{uuid.uuid4().hex[:8]}"

        with self._lock:
            new_triggered_alerts: List[PortfolioRiskAlert] = []
            cleared_alerts: List[PortfolioRiskAlert] = []
            current_cycle_alerts: Dict[str, PortfolioRiskAlert] = {}
            metrics_evaluated: Dict[str, Any] = {}

            # ------------------------------------------------------------------
            # 1. Missing Snapshot Handling
            # ------------------------------------------------------------------
            if snapshot is None:
                missing_key = f"{RiskRuleId.DATA_COMPLETENESS.value}:portfolio"
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.DATA_COMPLETENESS.value,
                    severity=AlertSeverity.CRITICAL,
                    state=AlertState.CRITICAL,
                    metric_name="snapshot_availability",
                    observed_value=None,
                    threshold_value=None,
                    unit="",
                    timestamp=now,
                    target="portfolio",
                    message="CRITICAL: No portfolio risk snapshot available for evaluation.",
                    evidence={"source_ref": source_ref},
                    source_ref=source_ref,
                )
                self._process_alert_transition(
                    key=missing_key,
                    candidate=alert,
                    now=now,
                    new_triggered=new_triggered_alerts,
                    current_cycle=current_cycle_alerts,
                )
                report = PortfolioRiskReport(
                    report_id=f"rep_{uuid.uuid4().hex[:10]}",
                    timestamp=now,
                    overall_state=AlertState.UNKNOWN,
                    active_alerts=tuple(self._active_alerts.values()),
                    cleared_alerts=tuple(cleared_alerts),
                    metrics_evaluated={"error": "SNAPSHOT_UNAVAILABLE"},
                    snapshot_timestamp=None,
                    snapshot_age_seconds=None,
                    is_snapshot_stale=True,
                    summary_text="Portfolio Risk: UNKNOWN (No snapshot data available)",
                )
                self._persist_and_publish(report, new_triggered_alerts, cleared_alerts, cid)
                return report

            # Clear missing snapshot alert if previously active
            missing_key = f"{RiskRuleId.DATA_COMPLETENESS.value}:portfolio"
            if missing_key in self._active_alerts:
                self._recover_alert(missing_key, now, "Snapshot data received.", cleared_alerts)

            # ------------------------------------------------------------------
            # 2. Freshness & Staleness Check
            # ------------------------------------------------------------------
            snapshot_age = max(0.0, now - snapshot.timestamp)
            is_stale = snapshot_age > self.config.max_snapshot_age_seconds
            metrics_evaluated["snapshot_age_seconds"] = snapshot_age
            metrics_evaluated["is_stale"] = is_stale

            freshness_key = f"{RiskRuleId.SNAPSHOT_FRESHNESS.value}:portfolio"
            if is_stale:
                stale_alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.SNAPSHOT_FRESHNESS.value,
                    severity=AlertSeverity.WARNING,
                    state=AlertState.WARNING,
                    metric_name="snapshot_age_seconds",
                    observed_value=snapshot_age,
                    threshold_value=self.config.max_snapshot_age_seconds,
                    unit="s",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"WARNING: Portfolio risk snapshot is stale ({snapshot_age:.1f}s > "
                        f"{self.config.max_snapshot_age_seconds:.1f}s threshold)."
                    ),
                    evidence={"snapshot_timestamp": snapshot.timestamp, "now": now},
                    source_ref=source_ref,
                )
                self._process_alert_transition(
                    key=freshness_key,
                    candidate=stale_alert,
                    now=now,
                    new_triggered=new_triggered_alerts,
                    current_cycle=current_cycle_alerts,
                )
            elif freshness_key in self._active_alerts:
                self._recover_alert(freshness_key, now, "Snapshot freshness recovered.", cleared_alerts)

            # ------------------------------------------------------------------
            # 3. Gross Exposure Monitoring
            # ------------------------------------------------------------------
            gross_val = snapshot.gross_exposure_pct
            metrics_evaluated["gross_exposure_pct"] = gross_val
            gross_key = f"{RiskRuleId.GROSS_EXPOSURE.value}:portfolio"

            if gross_val >= self.config.max_gross_exposure_critical_pct:
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.GROSS_EXPOSURE.value,
                    severity=AlertSeverity.CRITICAL,
                    state=AlertState.CRITICAL,
                    metric_name="gross_exposure_pct",
                    observed_value=gross_val,
                    threshold_value=self.config.max_gross_exposure_critical_pct,
                    unit="%",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"CRITICAL: Portfolio gross exposure {gross_val:.2f}% breached critical limit "
                        f"{self.config.max_gross_exposure_critical_pct:.2f}%."
                    ),
                    evidence={"total_equity": snapshot.total_equity, "gross_exposure": snapshot.gross_exposure},
                    source_ref=source_ref,
                )
                self._process_alert_transition(gross_key, alert, now, new_triggered_alerts, current_cycle_alerts)
            elif gross_val >= self.config.max_gross_exposure_warning_pct:
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.GROSS_EXPOSURE.value,
                    severity=AlertSeverity.WARNING,
                    state=AlertState.WARNING,
                    metric_name="gross_exposure_pct",
                    observed_value=gross_val,
                    threshold_value=self.config.max_gross_exposure_warning_pct,
                    unit="%",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"WARNING: Portfolio gross exposure {gross_val:.2f}% reached warning threshold "
                        f"{self.config.max_gross_exposure_warning_pct:.2f}%."
                    ),
                    evidence={"total_equity": snapshot.total_equity, "gross_exposure": snapshot.gross_exposure},
                    source_ref=source_ref,
                )
                self._process_alert_transition(gross_key, alert, now, new_triggered_alerts, current_cycle_alerts)
            else:
                # Check recovery with hysteresis
                if gross_key in self._active_alerts:
                    recovery_thresh = self.config.max_gross_exposure_warning_pct - self.config.hysteresis_pct
                    if gross_val < recovery_thresh:
                        self._recover_alert(
                            gross_key, now, f"Gross exposure dropped to {gross_val:.2f}% (< {recovery_thresh:.2f}%).", cleared_alerts
                        )

            # ------------------------------------------------------------------
            # 4. Net Exposure Monitoring
            # ------------------------------------------------------------------
            net_val = abs(snapshot.net_exposure_pct)
            metrics_evaluated["net_exposure_pct"] = snapshot.net_exposure_pct
            net_key = f"{RiskRuleId.NET_EXPOSURE.value}:portfolio"

            if net_val >= self.config.max_net_exposure_critical_pct:
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.NET_EXPOSURE.value,
                    severity=AlertSeverity.CRITICAL,
                    state=AlertState.CRITICAL,
                    metric_name="net_exposure_pct",
                    observed_value=net_val,
                    threshold_value=self.config.max_net_exposure_critical_pct,
                    unit="%",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"CRITICAL: Portfolio net directional exposure {net_val:.2f}% breached critical limit "
                        f"{self.config.max_net_exposure_critical_pct:.2f}%."
                    ),
                    evidence={"net_exposure": snapshot.net_exposure, "total_equity": snapshot.total_equity},
                    source_ref=source_ref,
                )
                self._process_alert_transition(net_key, alert, now, new_triggered_alerts, current_cycle_alerts)
            elif net_val >= self.config.max_net_exposure_warning_pct:
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.NET_EXPOSURE.value,
                    severity=AlertSeverity.WARNING,
                    state=AlertState.WARNING,
                    metric_name="net_exposure_pct",
                    observed_value=net_val,
                    threshold_value=self.config.max_net_exposure_warning_pct,
                    unit="%",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"WARNING: Portfolio net directional exposure {net_val:.2f}% reached warning threshold "
                        f"{self.config.max_net_exposure_warning_pct:.2f}%."
                    ),
                    evidence={"net_exposure": snapshot.net_exposure, "total_equity": snapshot.total_equity},
                    source_ref=source_ref,
                )
                self._process_alert_transition(net_key, alert, now, new_triggered_alerts, current_cycle_alerts)
            else:
                if net_key in self._active_alerts:
                    recovery_thresh = self.config.max_net_exposure_warning_pct - self.config.hysteresis_pct
                    if net_val < recovery_thresh:
                        self._recover_alert(
                            net_key, now, f"Net exposure dropped to {net_val:.2f}% (< {recovery_thresh:.2f}%).", cleared_alerts
                        )

            # ------------------------------------------------------------------
            # 5. Cash Reserve Buffer Monitoring (Lower is worse)
            # ------------------------------------------------------------------
            cash_res_val = snapshot.cash_reserve_pct
            metrics_evaluated["cash_reserve_pct"] = cash_res_val
            cash_key = f"{RiskRuleId.CASH_RESERVE.value}:portfolio"

            if cash_res_val <= self.config.min_cash_reserve_critical_pct:
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.CASH_RESERVE.value,
                    severity=AlertSeverity.CRITICAL,
                    state=AlertState.CRITICAL,
                    metric_name="cash_reserve_pct",
                    observed_value=cash_res_val,
                    threshold_value=self.config.min_cash_reserve_critical_pct,
                    unit="%",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"CRITICAL: Free cash reserve depleted to {cash_res_val:.2f}% (<= critical limit "
                        f"{self.config.min_cash_reserve_critical_pct:.2f}%)."
                    ),
                    evidence={"free_cash": snapshot.cash, "total_equity": snapshot.total_equity},
                    source_ref=source_ref,
                )
                self._process_alert_transition(cash_key, alert, now, new_triggered_alerts, current_cycle_alerts)
            elif cash_res_val <= self.config.min_cash_reserve_warning_pct:
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.CASH_RESERVE.value,
                    severity=AlertSeverity.WARNING,
                    state=AlertState.WARNING,
                    metric_name="cash_reserve_pct",
                    observed_value=cash_res_val,
                    threshold_value=self.config.min_cash_reserve_warning_pct,
                    unit="%",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"WARNING: Free cash reserve fell to {cash_res_val:.2f}% (<= warning threshold "
                        f"{self.config.min_cash_reserve_warning_pct:.2f}%)."
                    ),
                    evidence={"free_cash": snapshot.cash, "total_equity": snapshot.total_equity},
                    source_ref=source_ref,
                )
                self._process_alert_transition(cash_key, alert, now, new_triggered_alerts, current_cycle_alerts)
            else:
                if cash_key in self._active_alerts:
                    recovery_thresh = self.config.min_cash_reserve_warning_pct + self.config.hysteresis_pct
                    if cash_res_val > recovery_thresh:
                        self._recover_alert(
                            cash_key, now, f"Cash reserve restored to {cash_res_val:.2f}% (> {recovery_thresh:.2f}%).", cleared_alerts
                        )

            # ------------------------------------------------------------------
            # 6. Per-Strategy Exposure Monitoring
            # ------------------------------------------------------------------
            for strat_id, s_mkt_val in snapshot.strategy_exposures.items():
                s_pct = (s_mkt_val / snapshot.total_equity * 100.0) if snapshot.total_equity > 0 else 0.0
                strat_key = f"{RiskRuleId.STRATEGY_EXPOSURE.value}:strategy:{strat_id}"

                if s_pct >= self.config.max_strategy_exposure_critical_pct:
                    alert = PortfolioRiskAlert(
                        alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                        rule_id=RiskRuleId.STRATEGY_EXPOSURE.value,
                        severity=AlertSeverity.CRITICAL,
                        state=AlertState.CRITICAL,
                        metric_name="strategy_exposure_pct",
                        observed_value=s_pct,
                        threshold_value=self.config.max_strategy_exposure_critical_pct,
                        unit="%",
                        timestamp=now,
                        target=f"strategy:{strat_id}",
                        message=(
                            f"CRITICAL: Strategy '{strat_id}' exposure {s_pct:.2f}% breached critical limit "
                            f"{self.config.max_strategy_exposure_critical_pct:.2f}%."
                        ),
                        evidence={"strategy_id": strat_id, "market_value": s_mkt_val, "total_equity": snapshot.total_equity},
                        source_ref=source_ref,
                    )
                    self._process_alert_transition(strat_key, alert, now, new_triggered_alerts, current_cycle_alerts)
                elif s_pct >= self.config.max_strategy_exposure_warning_pct:
                    alert = PortfolioRiskAlert(
                        alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                        rule_id=RiskRuleId.STRATEGY_EXPOSURE.value,
                        severity=AlertSeverity.WARNING,
                        state=AlertState.WARNING,
                        metric_name="strategy_exposure_pct",
                        observed_value=s_pct,
                        threshold_value=self.config.max_strategy_exposure_warning_pct,
                        unit="%",
                        timestamp=now,
                        target=f"strategy:{strat_id}",
                        message=(
                            f"WARNING: Strategy '{strat_id}' exposure {s_pct:.2f}% reached warning threshold "
                            f"{self.config.max_strategy_exposure_warning_pct:.2f}%."
                        ),
                        evidence={"strategy_id": strat_id, "market_value": s_mkt_val, "total_equity": snapshot.total_equity},
                        source_ref=source_ref,
                    )
                    self._process_alert_transition(strat_key, alert, now, new_triggered_alerts, current_cycle_alerts)
                else:
                    if strat_key in self._active_alerts:
                        rec_thresh = self.config.max_strategy_exposure_warning_pct - self.config.hysteresis_pct
                        if s_pct < rec_thresh:
                            self._recover_alert(
                                strat_key, now, f"Strategy '{strat_id}' exposure fell to {s_pct:.2f}%.", cleared_alerts
                            )

            # ------------------------------------------------------------------
            # 7. Single-Instrument Concentration Monitoring
            # ------------------------------------------------------------------
            for sym, inst_mkt_val in snapshot.instrument_exposures.items():
                inst_pct = (inst_mkt_val / snapshot.total_equity * 100.0) if snapshot.total_equity > 0 else 0.0
                inst_key = f"{RiskRuleId.INSTRUMENT_CONCENTRATION.value}:instrument:{sym}"

                if inst_pct >= self.config.max_instrument_concentration_critical_pct:
                    alert = PortfolioRiskAlert(
                        alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                        rule_id=RiskRuleId.INSTRUMENT_CONCENTRATION.value,
                        severity=AlertSeverity.CRITICAL,
                        state=AlertState.CRITICAL,
                        metric_name="instrument_concentration_pct",
                        observed_value=inst_pct,
                        threshold_value=self.config.max_instrument_concentration_critical_pct,
                        unit="%",
                        timestamp=now,
                        target=f"instrument:{sym}",
                        message=(
                            f"CRITICAL: Instrument '{sym}' concentration {inst_pct:.2f}% breached critical limit "
                            f"{self.config.max_instrument_concentration_critical_pct:.2f}%."
                        ),
                        evidence={"symbol": sym, "market_value": inst_mkt_val, "total_equity": snapshot.total_equity},
                        source_ref=source_ref,
                    )
                    self._process_alert_transition(inst_key, alert, now, new_triggered_alerts, current_cycle_alerts)
                elif inst_pct >= self.config.max_instrument_concentration_warning_pct:
                    alert = PortfolioRiskAlert(
                        alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                        rule_id=RiskRuleId.INSTRUMENT_CONCENTRATION.value,
                        severity=AlertSeverity.WARNING,
                        state=AlertState.WARNING,
                        metric_name="instrument_concentration_pct",
                        observed_value=inst_pct,
                        threshold_value=self.config.max_instrument_concentration_warning_pct,
                        unit="%",
                        timestamp=now,
                        target=f"instrument:{sym}",
                        message=(
                            f"WARNING: Instrument '{sym}' concentration {inst_pct:.2f}% reached warning threshold "
                            f"{self.config.max_instrument_concentration_warning_pct:.2f}%."
                        ),
                        evidence={"symbol": sym, "market_value": inst_mkt_val, "total_equity": snapshot.total_equity},
                        source_ref=source_ref,
                    )
                    self._process_alert_transition(inst_key, alert, now, new_triggered_alerts, current_cycle_alerts)
                else:
                    if inst_key in self._active_alerts:
                        rec_thresh = self.config.max_instrument_concentration_warning_pct - self.config.hysteresis_pct
                        if inst_pct < rec_thresh:
                            self._recover_alert(
                                inst_key, now, f"Instrument '{sym}' concentration fell to {inst_pct:.2f}%.", cleared_alerts
                            )

            # ------------------------------------------------------------------
            # 8. Drawdown Monitoring & Circuit Breaker
            # ------------------------------------------------------------------
            dd_val = snapshot.drawdown_pct
            metrics_evaluated["drawdown_pct"] = dd_val
            dd_key = f"{RiskRuleId.DRAWDOWN.value}:portfolio"

            if dd_val >= self.config.max_drawdown_critical_pct:
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.DRAWDOWN.value,
                    severity=AlertSeverity.CRITICAL,
                    state=AlertState.CRITICAL,
                    metric_name="drawdown_pct",
                    observed_value=dd_val,
                    threshold_value=self.config.max_drawdown_critical_pct,
                    unit="%",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"CRITICAL: Portfolio drawdown {dd_val:.2f}% breached critical limit "
                        f"{self.config.max_drawdown_critical_pct:.2f}%."
                    ),
                    evidence={"drawdown_pct": dd_val, "total_equity": snapshot.total_equity},
                    source_ref=source_ref,
                )
                self._process_alert_transition(dd_key, alert, now, new_triggered_alerts, current_cycle_alerts)
            elif dd_val >= self.config.max_drawdown_warning_pct:
                alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.DRAWDOWN.value,
                    severity=AlertSeverity.WARNING,
                    state=AlertState.WARNING,
                    metric_name="drawdown_pct",
                    observed_value=dd_val,
                    threshold_value=self.config.max_drawdown_warning_pct,
                    unit="%",
                    timestamp=now,
                    target="portfolio",
                    message=(
                        f"WARNING: Portfolio drawdown {dd_val:.2f}% reached warning threshold "
                        f"{self.config.max_drawdown_warning_pct:.2f}%."
                    ),
                    evidence={"drawdown_pct": dd_val, "total_equity": snapshot.total_equity},
                    source_ref=source_ref,
                )
                self._process_alert_transition(dd_key, alert, now, new_triggered_alerts, current_cycle_alerts)
            else:
                if dd_key in self._active_alerts:
                    rec_thresh = self.config.max_drawdown_warning_pct - self.config.hysteresis_pct
                    if dd_val < rec_thresh:
                        self._recover_alert(
                            dd_key, now, f"Drawdown dropped to {dd_val:.2f}% (< {rec_thresh:.2f}%).", cleared_alerts
                        )

            # Circuit breaker check
            cb_key = f"{RiskRuleId.CIRCUIT_BREAKER.value}:portfolio"
            if snapshot.circuit_breaker_active:
                cb_alert = PortfolioRiskAlert(
                    alert_id=f"alt_{uuid.uuid4().hex[:10]}",
                    rule_id=RiskRuleId.CIRCUIT_BREAKER.value,
                    severity=AlertSeverity.CRITICAL,
                    state=AlertState.CRITICAL,
                    metric_name="circuit_breaker_active",
                    observed_value=1.0,
                    threshold_value=1.0,
                    unit="",
                    timestamp=now,
                    target="portfolio",
                    message="CRITICAL: Portfolio circuit breaker is active. New trade entries halted.",
                    evidence={"circuit_breaker_active": True, "drawdown_pct": snapshot.drawdown_pct},
                    source_ref=source_ref,
                )
                self._process_alert_transition(cb_key, cb_alert, now, new_triggered_alerts, current_cycle_alerts)
            elif cb_key in self._active_alerts:
                self._recover_alert(cb_key, now, "Circuit breaker reset to inactive.", cleared_alerts)

            # ------------------------------------------------------------------
            # 9. Synthesize Overall Report State
            # ------------------------------------------------------------------
            active_list = list(self._active_alerts.values())
            if any(a.severity == AlertSeverity.CRITICAL for a in active_list):
                overall_state = AlertState.CRITICAL
            elif any(a.severity == AlertSeverity.WARNING for a in active_list):
                overall_state = AlertState.WARNING
            else:
                overall_state = AlertState.NORMAL

            crit_count = sum(1 for a in active_list if a.severity == AlertSeverity.CRITICAL)
            warn_count = sum(1 for a in active_list if a.severity == AlertSeverity.WARNING)

            if overall_state == AlertState.NORMAL:
                summary = "Portfolio Risk: NORMAL (All exposure, cash, and drawdown metrics within safe bounds)"
            else:
                summary = (
                    f"Portfolio Risk: {overall_state.value} | Active: {len(active_list)} "
                    f"(Critical: {crit_count}, Warning: {warn_count}) | Recovered: {len(cleared_alerts)}"
                )

            report = PortfolioRiskReport(
                report_id=f"rep_{uuid.uuid4().hex[:10]}",
                timestamp=now,
                overall_state=overall_state,
                active_alerts=tuple(active_list),
                cleared_alerts=tuple(cleared_alerts),
                metrics_evaluated=metrics_evaluated,
                snapshot_timestamp=snapshot.timestamp,
                snapshot_age_seconds=snapshot_age,
                is_snapshot_stale=is_stale,
                summary_text=summary,
            )

            self._persist_and_publish(report, new_triggered_alerts, cleared_alerts, cid)
            return report

    def evaluate_simulation_result(
        self,
        result: PortfolioSimulationResult,
        correlation_id: Optional[str] = None,
    ) -> PortfolioRiskReport:
        """Evaluate the overall outcome and final snapshots of a PortfolioSimulationResult."""
        final_snap = result.risk_snapshots[-1] if result.risk_snapshots else None
        return self.evaluate_snapshot(
            snapshot=final_snap,
            source_ref=f"simulation:{result.simulation_id}",
            correlation_id=correlation_id or result.config.correlation_id,
        )

    # --------------------------------------------------------------------------
    # Internal Alert Transition and Deduplication Helpers
    # --------------------------------------------------------------------------

    def _process_alert_transition(
        self,
        key: str,
        candidate: PortfolioRiskAlert,
        now: float,
        new_triggered: List[PortfolioRiskAlert],
        current_cycle: Dict[str, PortfolioRiskAlert],
    ) -> None:
        """Process an alert determination with state transitions, cooldown, and deduplication."""
        existing = self._active_alerts.get(key)
        current_cycle[key] = candidate

        # 1. State transition (e.g. None -> WARNING, WARNING -> CRITICAL, CRITICAL -> WARNING)
        if existing is None or existing.severity != candidate.severity:
            self._active_alerts[key] = candidate
            self._last_broadcast_times[key] = now
            new_triggered.append(candidate)
            logger.warning(
                "Risk Alert Transition [%s -> %s]: %s",
                existing.severity.value if existing else "NONE",
                candidate.severity.value,
                candidate.message,
            )
            return

        # 2. Same severity state - Check deduplication cooldown
        last_broadcast = self._last_broadcast_times.get(key, 0.0)
        cooldown = self.config.deduplication_cooldown_seconds

        if (now - last_broadcast) >= cooldown:
            self._active_alerts[key] = candidate
            self._last_broadcast_times[key] = now
            new_triggered.append(candidate)
            logger.info("Risk Alert Periodic Refresh [%s]: %s", key, candidate.message)
        else:
            # Suppress broadcast on EventBus, but update stored active alert value
            self._active_alerts[key] = candidate

    def _recover_alert(
        self,
        key: str,
        now: float,
        recovery_reason: str,
        cleared_alerts: List[PortfolioRiskAlert],
    ) -> None:
        """Mark an active alert as RECOVERED and remove from active collection."""
        existing = self._active_alerts.pop(key, None)
        self._last_broadcast_times.pop(key, None)

        if existing:
            recovered_alert = PortfolioRiskAlert(
                alert_id=f"alt_rec_{uuid.uuid4().hex[:8]}",
                rule_id=existing.rule_id,
                severity=AlertSeverity.INFO,
                state=AlertState.RECOVERED,
                metric_name=existing.metric_name,
                observed_value=existing.observed_value,
                threshold_value=existing.threshold_value,
                unit=existing.unit,
                timestamp=now,
                target=existing.target,
                message=f"RECOVERED: {existing.metric_name} on {existing.target} returned to normal. {recovery_reason}",
                evidence=dict(existing.evidence),
                source_ref=existing.source_ref,
                recovery_details={"reason": recovery_reason, "recovered_at": now},
            )
            cleared_alerts.append(recovered_alert)
            logger.info("Risk Alert Recovered [%s]: %s", key, recovery_reason)

    def _persist_and_publish(
        self,
        report: PortfolioRiskReport,
        new_triggered: List[PortfolioRiskAlert],
        cleared_alerts: List[PortfolioRiskAlert],
        correlation_id: str,
    ) -> None:
        """Persist latest report to StateStore and broadcast events over EventBus safely."""
        self._last_report = report

        # 1. StateStore Persistence
        if self.state_store:
            try:
                self.state_store.set("portfolio_risk:latest", report.to_dict())
                self.state_store.set("portfolio_risk:active_alerts", [a.to_dict() for a in report.active_alerts])
            except Exception as e:
                logger.warning("Failed to persist portfolio risk in StateStore: %s", e)

        # 2. EventBus Publication
        if self.event_bus:
            # Broadcast new or escalated alerts
            for alert in new_triggered:
                try:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.PORTFOLIO_RISK_ALERT_TRIGGERED,
                            source=self.name,
                            correlation_id=correlation_id,
                            payload=alert.to_dict(),
                        )
                    )
                except Exception as e:
                    logger.error("Failed to publish PORTFOLIO_RISK_ALERT_TRIGGERED event: %s", e)

            # Broadcast cleared/recovered alerts
            for rec in cleared_alerts:
                try:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.PORTFOLIO_RISK_ALERT_CLEARED,
                            source=self.name,
                            correlation_id=correlation_id,
                            payload=rec.to_dict(),
                        )
                    )
                except Exception as e:
                    logger.error("Failed to publish PORTFOLIO_RISK_ALERT_CLEARED event: %s", e)

            # Broadcast overall evaluated report
            try:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.PORTFOLIO_RISK_STATUS_EVALUATED,
                        source=self.name,
                        correlation_id=correlation_id,
                        payload=report.to_dict(),
                    )
                )
            except Exception as e:
                logger.error("Failed to publish PORTFOLIO_RISK_STATUS_EVALUATED event: %s", e)
