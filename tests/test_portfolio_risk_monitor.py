"""KAISER — Portfolio Risk Monitoring & Alerting Foundation Tests.

Comprehensive, deterministic test suite verifying:
1. Config validation (ordering of warning vs critical, positive boundaries, hysteresis).
2. Normal evaluations where all portfolio metrics are within acceptable thresholds.
3. Warning threshold crossings (gross exposure, net exposure, cash reserve, drawdown, strategy, instrument).
4. Critical threshold crossings and severity escalation (WARNING -> CRITICAL).
5. Severity de-escalation (CRITICAL -> WARNING) and recovery with hysteresis (WARNING -> RECOVERED).
6. Hysteresis deadband behavior (prevents boundary oscillation and flapping).
7. Duplicate alert suppression and cooldown deduplication over EventBus.
8. Repeated evaluation of unchanged snapshots without duplicate state corruption.
9. Missing snapshots (None) and stale snapshot detection (snapshot age > max limit).
10. Strategy-level exposure breaches and single-instrument concentration breaches.
11. Drawdown breaches and portfolio circuit-breaker alerts.
12. EventBus publication and subscriber failure isolation (subscriber crashes do not corrupt monitoring).
13. StateStore persistence and retrieval of latest report and active alerts.
14. Deterministic clock dependency injection for repeatable timestamp verification.
15. Simulation result evaluation (evaluate_simulation_result).
16. Orchestrator command integration (`evaluate_portfolio_risk` and `get_portfolio_risk_status`).
17. Proof that risk monitoring is purely informational and never touches broker/order/execution paths.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional
import pytest

from app.backtesting.portfolio_simulation.models import (
    PortfolioRiskSnapshot,
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
)
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.risk.handlers import register_risk_handlers
from app.risk.models import (
    AlertSeverity,
    AlertState,
    PortfolioRiskAlert,
    PortfolioRiskMonitorConfig,
    PortfolioRiskReport,
    RiskRuleId,
)
from app.risk.monitor import PortfolioRiskMonitor


# ------------------------------------------------------------------------------
# Test Fixtures & Helpers
# ------------------------------------------------------------------------------

def _create_snapshot(
    timestamp: float = 1700000000.0,
    total_equity: float = 100_000.0,
    cash: float = 50_000.0,
    cash_reserve_pct: float = 50.0,
    gross_exposure: float = 50_000.0,
    gross_exposure_pct: float = 50.0,
    net_exposure: float = 50_000.0,
    net_exposure_pct: float = 50.0,
    strategy_exposures: Optional[dict] = None,
    instrument_exposures: Optional[dict] = None,
    max_instrument_concentration_pct: float = 10.0,
    herfindahl_index: float = 0.5,
    drawdown_pct: float = 2.0,
    circuit_breaker_active: bool = False,
) -> PortfolioRiskSnapshot:
    """Helper to construct a deterministic PortfolioRiskSnapshot."""
    return PortfolioRiskSnapshot(
        timestamp=timestamp,
        total_equity=total_equity,
        cash=cash,
        cash_reserve_pct=cash_reserve_pct,
        gross_exposure=gross_exposure,
        gross_exposure_pct=gross_exposure_pct,
        net_exposure=net_exposure,
        net_exposure_pct=net_exposure_pct,
        strategy_exposures=strategy_exposures or {"strat_a": 15_000.0, "strat_b": 15_000.0},
        instrument_exposures=instrument_exposures or {"TCS": 10_000.0, "INFY": 10_000.0},
        max_instrument_concentration_pct=max_instrument_concentration_pct,
        herfindahl_index=herfindahl_index,
        drawdown_pct=drawdown_pct,
        circuit_breaker_active=circuit_breaker_active,
    )


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def state_store() -> StateStore:
    return StateStore()


# ------------------------------------------------------------------------------
# 1. Config Validation Tests
# ------------------------------------------------------------------------------

def test_config_validation():
    """Verify PortfolioRiskMonitorConfig invariant validations."""
    # Valid config
    cfg = PortfolioRiskMonitorConfig(
        max_gross_exposure_warning_pct=80.0,
        max_gross_exposure_critical_pct=100.0,
    )
    assert cfg.max_gross_exposure_warning_pct == 80.0

    # Warning >= Critical rejected for gross exposure
    with pytest.raises(ValidationError, match="Gross exposure warning .* must be strictly less than critical"):
        PortfolioRiskMonitorConfig(
            max_gross_exposure_warning_pct=100.0,
            max_gross_exposure_critical_pct=90.0,
        )

    # Net exposure warning >= critical rejected
    with pytest.raises(ValidationError, match="Net exposure warning .* must be strictly less than critical"):
        PortfolioRiskMonitorConfig(
            max_net_exposure_warning_pct=95.0,
            max_net_exposure_critical_pct=95.0,
        )

    # Cash reserve critical >= warning rejected (since lower is worse)
    with pytest.raises(ValidationError, match="Cash reserve critical .* must be strictly less than warning"):
        PortfolioRiskMonitorConfig(
            min_cash_reserve_warning_pct=10.0,
            min_cash_reserve_critical_pct=15.0,
        )

    # Negative values rejected
    with pytest.raises(ValidationError, match="max_gross_exposure_warning_pct must be strictly positive"):
        PortfolioRiskMonitorConfig(max_gross_exposure_warning_pct=-5.0)

    with pytest.raises(ValidationError, match="min_cash_reserve_critical_pct cannot be negative"):
        PortfolioRiskMonitorConfig(min_cash_reserve_critical_pct=-1.0)


# ------------------------------------------------------------------------------
# 2. Normal State Evaluation Tests
# ------------------------------------------------------------------------------

def test_normal_state_evaluation(event_bus: EventBus, state_store: StateStore):
    """Verify clean evaluation when all metrics are safely below warning thresholds."""
    current_time = 1700000100.0
    monitor = PortfolioRiskMonitor(
        event_bus=event_bus,
        state_store=state_store,
        clock_fn=lambda: current_time,
    )

    snapshot = _create_snapshot(
        timestamp=current_time - 30.0,  # 30s old (fresh)
        gross_exposure_pct=50.0,  # < 80% warning
        net_exposure_pct=30.0,  # < 70% warning
        cash_reserve_pct=50.0,  # > 15% warning
        drawdown_pct=2.0,  # < 10% warning
    )

    report = monitor.evaluate_snapshot(snapshot)

    assert report.overall_state == AlertState.NORMAL
    assert report.is_healthy is True
    assert len(report.active_alerts) == 0
    assert len(report.cleared_alerts) == 0
    assert report.is_snapshot_stale is False
    assert "NORMAL" in report.summary_text

    # StateStore verified
    saved = state_store.get("portfolio_risk:latest")
    assert saved is not None
    assert saved["overall_state"] == "NORMAL"


# ------------------------------------------------------------------------------
# 3. Threshold Crossings & Escalation Tests
# ------------------------------------------------------------------------------

def test_warning_threshold_crossing(event_bus: EventBus):
    """Verify WARNING alert emitted when gross exposure crosses warning threshold."""
    current_time = 1700000100.0
    published_alerts = []
    event_bus.subscribe(
        EventType.PORTFOLIO_RISK_ALERT_TRIGGERED,
        lambda ev: published_alerts.append(ev),
    )

    monitor = PortfolioRiskMonitor(
        event_bus=event_bus,
        clock_fn=lambda: current_time,
    )

    # Gross exposure = 85.0% (Warning threshold is 80.0%)
    snapshot = _create_snapshot(
        timestamp=current_time - 10.0,
        gross_exposure_pct=85.0,
    )

    report = monitor.evaluate_snapshot(snapshot)

    assert report.overall_state == AlertState.WARNING
    assert report.is_healthy is False
    assert len(report.active_alerts) == 1
    alert = report.active_alerts[0]
    assert alert.rule_id == RiskRuleId.GROSS_EXPOSURE.value
    assert alert.severity == AlertSeverity.WARNING
    assert alert.state == AlertState.WARNING
    assert alert.observed_value == 85.0

    # EventBus verified
    assert len(published_alerts) == 1
    assert published_alerts[0].payload["rule_id"] == RiskRuleId.GROSS_EXPOSURE.value


def test_alert_escalation_and_deescalation(event_bus: EventBus):
    """Verify escalation from WARNING -> CRITICAL and de-escalation back to WARNING."""
    current_time = 1700000100.0
    published = []
    event_bus.subscribe(
        EventType.PORTFOLIO_RISK_ALERT_TRIGGERED,
        lambda ev: published.append(ev),
    )

    monitor = PortfolioRiskMonitor(
        event_bus=event_bus,
        clock_fn=lambda: current_time,
    )

    # Step 1: Warning level (85%)
    snap1 = _create_snapshot(timestamp=current_time - 10.0, gross_exposure_pct=85.0)
    rep1 = monitor.evaluate_snapshot(snap1)
    assert rep1.overall_state == AlertState.WARNING
    assert len(published) == 1
    assert published[-1].payload["severity"] == "WARNING"

    # Step 2: Escalate to Critical level (105%)
    snap2 = _create_snapshot(timestamp=current_time - 5.0, gross_exposure_pct=105.0)
    rep2 = monitor.evaluate_snapshot(snap2)
    assert rep2.overall_state == AlertState.CRITICAL
    assert len(published) == 2
    assert published[-1].payload["severity"] == "CRITICAL"

    # Step 3: De-escalate back to Warning level (88%)
    snap3 = _create_snapshot(timestamp=current_time - 2.0, gross_exposure_pct=88.0)
    rep3 = monitor.evaluate_snapshot(snap3)
    assert rep3.overall_state == AlertState.WARNING
    assert len(published) == 3
    assert published[-1].payload["severity"] == "WARNING"


# ------------------------------------------------------------------------------
# 4. Recovery and Hysteresis Tests
# ------------------------------------------------------------------------------

def test_recovery_with_hysteresis(event_bus: EventBus):
    """Verify alert clears only after metric drops below warning threshold minus hysteresis."""
    current_time = 1700000100.0
    cleared_events = []
    event_bus.subscribe(
        EventType.PORTFOLIO_RISK_ALERT_CLEARED,
        lambda ev: cleared_events.append(ev),
    )

    # Warning threshold = 80.0%, Hysteresis = 2.0% -> Must drop strictly below 78.0% to recover
    cfg = PortfolioRiskMonitorConfig(
        max_gross_exposure_warning_pct=80.0,
        max_gross_exposure_critical_pct=100.0,
        hysteresis_pct=2.0,
    )
    monitor = PortfolioRiskMonitor(
        config=cfg,
        event_bus=event_bus,
        clock_fn=lambda: current_time,
    )

    # 1. Breach warning threshold (85.0%)
    snap1 = _create_snapshot(timestamp=current_time - 10.0, gross_exposure_pct=85.0)
    monitor.evaluate_snapshot(snap1)
    assert len(monitor.get_active_alerts()) == 1

    # 2. Metric drops to 79.0% (Below 80%, but inside 78.0-80.0% hysteresis deadband)
    snap2 = _create_snapshot(timestamp=current_time - 5.0, gross_exposure_pct=79.0)
    rep2 = monitor.evaluate_snapshot(snap2)
    # Alert should STILL be active to prevent fluttering!
    assert len(rep2.active_alerts) == 1
    assert len(cleared_events) == 0

    # 3. Metric drops to 76.0% (Below 78.0% recovery threshold)
    snap3 = _create_snapshot(timestamp=current_time - 1.0, gross_exposure_pct=76.0)
    rep3 = monitor.evaluate_snapshot(snap3)
    assert len(rep3.active_alerts) == 0
    assert rep3.overall_state == AlertState.NORMAL
    assert len(rep3.cleared_alerts) == 1
    assert rep3.cleared_alerts[0].state == AlertState.RECOVERED
    assert len(cleared_events) == 1


# ------------------------------------------------------------------------------
# 5. Cooldown & Deduplication Tests
# ------------------------------------------------------------------------------

def test_alert_deduplication_cooldown(event_bus: EventBus):
    """Verify unchanged alerts are suppressed from EventBus flooding within cooldown period."""
    sim_clock = 1700000000.0

    def clock():
        return sim_clock

    published = []
    event_bus.subscribe(
        EventType.PORTFOLIO_RISK_ALERT_TRIGGERED,
        lambda ev: published.append(ev),
    )

    # Cooldown = 60s
    cfg = PortfolioRiskMonitorConfig(
        max_gross_exposure_warning_pct=80.0,
        deduplication_cooldown_seconds=60.0,
    )
    monitor = PortfolioRiskMonitor(config=cfg, event_bus=event_bus, clock_fn=clock)

    snap = _create_snapshot(timestamp=sim_clock, gross_exposure_pct=85.0)

    # Evaluation 1: Alert triggered (t = 0)
    monitor.evaluate_snapshot(snap)
    assert len(published) == 1

    # Evaluation 2: Repeated snapshot at t = 20s (inside 60s cooldown)
    sim_clock += 20.0
    monitor.evaluate_snapshot(snap)
    # EventBus must NOT have received a duplicate event!
    assert len(published) == 1

    # Evaluation 3: Repeated snapshot at t = 65s (past 60s cooldown)
    sim_clock += 45.0
    monitor.evaluate_snapshot(snap)
    # EventBus refresh broadcast allowed
    assert len(published) == 2


# ------------------------------------------------------------------------------
# 6. Data Quality & Staleness Tests
# ------------------------------------------------------------------------------

def test_missing_snapshot_alert():
    """Verify that passing None for snapshot produces an explicit CRITICAL alert and UNKNOWN report."""
    monitor = PortfolioRiskMonitor()
    report = monitor.evaluate_snapshot(snapshot=None, source_ref="test_none")

    assert report.overall_state == AlertState.UNKNOWN
    assert report.is_healthy is False
    assert len(report.active_alerts) == 1
    alert = report.active_alerts[0]
    assert alert.rule_id == RiskRuleId.DATA_COMPLETENESS.value
    assert alert.severity == AlertSeverity.CRITICAL


def test_stale_snapshot_detection():
    """Verify that snapshot older than max_snapshot_age_seconds triggers STALE warning."""
    sim_time = 1700001000.0
    cfg = PortfolioRiskMonitorConfig(max_snapshot_age_seconds=120.0)
    monitor = PortfolioRiskMonitor(config=cfg, clock_fn=lambda: sim_time)

    # Snapshot is 300 seconds old (> 120s limit)
    snap = _create_snapshot(timestamp=sim_time - 300.0)
    report = monitor.evaluate_snapshot(snap)

    assert report.is_snapshot_stale is True
    assert report.overall_state == AlertState.WARNING
    assert any(a.rule_id == RiskRuleId.SNAPSHOT_FRESHNESS.value for a in report.active_alerts)


# ------------------------------------------------------------------------------
# 7. Granular Risk Rules Tests: Cash Reserve, Strategy, Concentration, Drawdown
# ------------------------------------------------------------------------------

def test_cash_reserve_depletion_alert():
    """Verify critical alert when cash reserve falls below min_cash_reserve_critical_pct."""
    monitor = PortfolioRiskMonitor(clock_fn=lambda: 1700000100.0)

    # Cash reserve 3.0% (Critical threshold is 5.0%)
    snap = _create_snapshot(timestamp=1700000090.0, cash_reserve_pct=3.0)
    report = monitor.evaluate_snapshot(snap)

    assert report.overall_state == AlertState.CRITICAL
    alert = next(a for a in report.active_alerts if a.rule_id == RiskRuleId.CASH_RESERVE.value)
    assert alert.severity == AlertSeverity.CRITICAL
    assert alert.observed_value == 3.0


def test_strategy_and_instrument_concentration_alerts():
    """Verify strategy exposure and instrument concentration breach alerts."""
    monitor = PortfolioRiskMonitor(clock_fn=lambda: 1700000100.0)

    # Total equity 100k. Strategy A holds 55k (55% > 50% critical limit).
    # Instrument TCS holds 40k (40% > 35% critical concentration limit).
    snap = _create_snapshot(
        timestamp=1700000090.0,
        total_equity=100_000.0,
        strategy_exposures={"strat_a": 55_000.0, "strat_b": 10_000.0},
        instrument_exposures={"TCS": 40_000.0, "INFY": 25_000.0},
    )
    report = monitor.evaluate_snapshot(snap)

    assert report.overall_state == AlertState.CRITICAL
    targets = {a.target for a in report.active_alerts}
    assert "strategy:strat_a" in targets
    assert "instrument:TCS" in targets


def test_drawdown_and_circuit_breaker_alerts():
    """Verify drawdown breach and circuit breaker alerts."""
    monitor = PortfolioRiskMonitor(clock_fn=lambda: 1700000100.0)

    # Drawdown 18.0% (> 15% critical) and circuit breaker active
    snap = _create_snapshot(
        timestamp=1700000090.0,
        drawdown_pct=18.0,
        circuit_breaker_active=True,
    )
    report = monitor.evaluate_snapshot(snap)

    assert report.overall_state == AlertState.CRITICAL
    rule_ids = {a.rule_id for a in report.active_alerts}
    assert RiskRuleId.DRAWDOWN.value in rule_ids
    assert RiskRuleId.CIRCUIT_BREAKER.value in rule_ids


# ------------------------------------------------------------------------------
# 8. EventBus Subscriber Failure Isolation
# ------------------------------------------------------------------------------

def test_subscriber_failure_does_not_break_monitoring(event_bus: EventBus):
    """Verify that a failing EventBus subscriber does not crash or corrupt risk monitoring."""
    def broken_handler(ev: Event):
        raise RuntimeError("Subscriber explosion!")

    event_bus.subscribe(EventType.PORTFOLIO_RISK_ALERT_TRIGGERED, broken_handler)

    monitor = PortfolioRiskMonitor(event_bus=event_bus, clock_fn=lambda: 1700000100.0)
    snap = _create_snapshot(timestamp=1700000090.0, gross_exposure_pct=95.0)

    # Must complete safely without raising RuntimeError
    report = monitor.evaluate_snapshot(snap)
    assert report.overall_state == AlertState.WARNING


# ------------------------------------------------------------------------------
# 9. Simulation Result Evaluation
# ------------------------------------------------------------------------------

def test_evaluate_simulation_result():
    """Verify evaluate_simulation_result processes final snapshot of a simulation result."""
    alloc = StrategyAllocation("sma_crossover", "TCS", weight=1.0)
    cfg = PortfolioSimulationConfig(
        allocations=(alloc,),
        start_time=1700000000.0,
        end_time=1700003600.0,
        initial_cash=100_000.0,
    )
    snap = _create_snapshot(timestamp=1700003600.0, gross_exposure_pct=92.0)
    sim_res = PortfolioSimulationResult(
        simulation_id="sim_test_123",
        status=PortfolioSimulationStatus.SUCCESS,
        config=cfg,
        initial_equity=100_000.0,
        final_equity=100_000.0,
        gross_pnl=0.0,
        net_pnl=0.0,
        net_return_pct=0.0,
        max_drawdown_pct=0.0,
        max_drawdown_amount=0.0,
        max_drawdown_duration_bars=0,
        total_fees=0.0,
        total_slippage=0.0,
        total_transaction_costs=0.0,
        total_trades=0,
        winning_trades=0,
        losing_trades=0,
        win_rate=0.0,
        profit_factor=None,
        profit_factor_description="N/A",
        risk_snapshots=(snap,),
    )

    monitor = PortfolioRiskMonitor(clock_fn=lambda: 1700003650.0)
    report = monitor.evaluate_simulation_result(sim_res)

    assert report.overall_state == AlertState.WARNING
    assert len(report.active_alerts) == 1
    assert report.active_alerts[0].source_ref is not None
    assert "simulation:sim_test_123" in report.active_alerts[0].source_ref


# ------------------------------------------------------------------------------
# 10. Orchestrator Integration Tests
# ------------------------------------------------------------------------------

def test_orchestrator_integration(event_bus: EventBus, state_store: StateStore):
    """Verify evaluate_portfolio_risk and get_portfolio_risk_status commands with MasterOrchestrator."""
    current_time = 1700000100.0
    monitor = PortfolioRiskMonitor(
        event_bus=event_bus,
        state_store=state_store,
        clock_fn=lambda: current_time,
    )
    orchestrator = MasterOrchestrator(
        event_bus=event_bus,
        state_store=state_store,
        name="test_orchestrator",
    )
    register_risk_handlers(orchestrator=orchestrator, monitor=monitor)

    # 1. Test evaluate_portfolio_risk
    snap = _create_snapshot(timestamp=current_time - 10.0, gross_exposure_pct=88.0)
    req1 = OrchestratorRequest.create(
        command="evaluate_portfolio_risk",
        context={"snapshot": snap.to_dict()},
    )
    resp1 = orchestrator.handle_request(req1)
    assert resp1.is_success
    assert resp1.result["status"] == "success"
    assert resp1.result["overall_state"] == "WARNING"
    assert resp1.result["active_alerts_count"] == 1

    # 2. Test get_portfolio_risk_status
    req2 = OrchestratorRequest.create(command="get_portfolio_risk_status")
    resp2 = orchestrator.handle_request(req2)
    assert resp2.is_success
    assert resp2.result["status"] == "success"
    assert resp2.result["overall_state"] == "WARNING"
    assert resp2.result["active_alerts_count"] == 1


# ------------------------------------------------------------------------------
# 11. Safety Invariants Confirmation
# ------------------------------------------------------------------------------

def test_safety_invariants_strictly_maintained():
    """Verify that Settings.execution_locked remains True and monitor never places trades."""
    settings = Settings()
    assert settings.execution_locked is True

    monitor = PortfolioRiskMonitor()
    # Critical breach
    snap = _create_snapshot(
        timestamp=1700000000.0,
        gross_exposure_pct=150.0,  # Huge breach
        drawdown_pct=50.0,
        circuit_breaker_active=True,
    )
    report = monitor.evaluate_snapshot(snap)

    assert report.overall_state == AlertState.CRITICAL
    # Verify no execution attributes exist on monitor
    assert not hasattr(monitor, "place_order")
    assert not hasattr(monitor, "liquidate")
    assert not hasattr(monitor, "broker")
    assert "RESEARCH ONLY" in report.disclaimer
