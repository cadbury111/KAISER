"""KAISER — Phase 2, Step 37 Acceptance Testing & Operational Resilience Suite.

End-to-End Acceptance Tests validating:
1. Audit report generation observes diagnostic event.
2. Reliability degradation generates appropriate diagnostic and alert.
3. Repeatability divergence and regression traceable to sources.
4. Repeated identical failures deduplicated at alert layer while all diagnostics remain.
5. Recovery transitions correctly resolve previously active failures.
6. Unavailable and malformed data produces explicit statuses without fabrication.
7. Event delivery and listener failure isolation (EventBus contract).
8. StateStore persistence and cross-service reconstruction.
9. Concurrent audit, reliability, and diagnostic operations thread safety.
10. Bounded retention under sustained input.
11. Correlation IDs and source references consistency across the pipeline.
12. Dashboard Endpoint 28 security, sanitization, and read-only invariants.
13. Existing Endpoints 26 and 27 compatibility.
14. System safety invariants (execution_locked, uninitialized trading engine).
"""

from __future__ import annotations

import concurrent.futures
import hashlib
from http import HTTPStatus
import io
import json
import math
import re
import socket
import threading
import time
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.parse
import urllib.request

import pytest

from app.analysis.opportunities.evolution.audit.diagnostics.models import (
    AuditAlert,
    AuditAlertStatus,
    AuditDiagnosticCategory,
    AuditDiagnosticRecord,
    AuditDiagnosticSeverity,
    AuditDiagnosticSummary,
    AuditEvidenceNature,
    compute_deterministic_alert_id,
    compute_deterministic_diagnostic_id,
    format_utc_iso,
    sanitize_text,
    validate_safe_id,
)
from app.analysis.opportunities.evolution.audit.diagnostics.service import (
    HistoricalReviewAuditDiagnosticsService,
)
from app.analysis.opportunities.evolution.audit.models import (
    AuditFindingSeverity,
    HistoricalReviewAuditReport,
    RecordValidationStatus,
)
from app.analysis.opportunities.evolution.audit.reliability.models import (
    AuditCoverageState,
    AuditCoverageSummary,
    AuditRepeatabilityResult,
    AuditRepeatabilityState,
    AuditRegressionResult,
    AuditRegressionState,
    HistoricalReviewReliabilityResult,
    ReliabilityEvaluationStatus,
)
from app.analysis.opportunities.evolution.audit.reliability.service import (
    HistoricalReviewAuditReliabilityService,
)
from app.analysis.opportunities.evolution.audit.service import (
    HistoricalReviewAuditService,
)
from app.analysis.opportunities.evolution.models import (
    HistoricalReviewSnapshot,
    HistoricalSnapshotLifecycleState,
    compute_deterministic_snapshot_id,
)
from app.analysis.opportunities.evolution.service import (
    HistoricalReviewEvolutionService,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.portfolio.dashboard.server import PortfolioDashboardServer


# ==============================================================================
# Deterministic Test Fixtures & Injected Clocks
# ==============================================================================

class DeterministicClock:
    """Controllable injected clock for deterministic cooldown and expiry testing."""

    def __init__(self, initial_time: float = 1700000000.0) -> None:
        self._current_time = initial_time
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._current_time

    def advance(self, seconds: float) -> float:
        with self._lock:
            self._current_time += seconds
            return self._current_time


def find_free_port() -> int:
    """Find an available port on loopback interface."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_test_snapshot(
    investigation_id: str = "inv_accept_01",
    snapshot_version: int = 1,
    captured_at: float = 1700000000.0,
    outcome_id: str = "out_accept_01",
) -> HistoricalReviewSnapshot:
    """Create a deterministic test snapshot instance."""
    source_state = {"status": "RECONCILED", "analysis_run_ids": ["run_1"]}
    evidence_state = {"facts": [{"evidence_id": "f_1", "claim_type": "FACT"}]}
    review_state = {"human_review_decisions": []}
    conflict_state = {"active_conflicts": []}
    quality_state = {"status": "VALID"}
    fp = hashlib.sha256(f"{investigation_id}:{snapshot_version}".encode("utf-8")).hexdigest()[:16]
    snap_id = compute_deterministic_snapshot_id(investigation_id, outcome_id, None, snapshot_version, fp)

    return HistoricalReviewSnapshot(
        snapshot_id=snap_id,
        investigation_id=investigation_id,
        outcome_id=outcome_id,
        comparison_id=None,
        snapshot_version=snapshot_version,
        captured_at=captured_at,
        lifecycle_state=HistoricalSnapshotLifecycleState.STORED,
        source_state=source_state,
        evidence_state=evidence_state,
        review_state=review_state,
        conflict_state=conflict_state,
        quality_state=quality_state,
        source_references=(f"ref_{investigation_id}_{snapshot_version}",),
    )


# ==============================================================================
# Scenario 1: Audit Report Generation Observes Diagnostic Event
# ==============================================================================

def test_acceptance_scenario_1_audit_report_generation_observes_diagnostic_event():
    """Verify that an audit report generation produces the expected diagnostic event."""
    bus = EventBus("test_bus_s1")
    store = StateStore()
    clock = DeterministicClock(1700000000.0)

    diag_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        time_provider=clock,
        auto_subscribe=True,
    )

    ev_service = HistoricalReviewEvolutionService(event_bus=bus, state_store=store)
    audit_service = HistoricalReviewAuditService(
        evolution_service=ev_service,
        event_bus=bus,
        state_store=store,
    )

    inv_id = "inv_accept_01"
    snap1 = make_test_snapshot(inv_id, 1, 1700000000.0)
    snap2 = make_test_snapshot(inv_id, 2, 1700000100.0)
    ev_service.store_snapshot(snap1)
    ev_service.store_snapshot(snap2)

    observed_diag_events = []
    bus.subscribe(
        EventType.HISTORICAL_REVIEW_DIAGNOSTIC_RECORDED,
        lambda evt: observed_diag_events.append(evt),
    )

    # Generate audit report
    corr_id = "corr_accept_audit_01"
    report = audit_service.generate_audit_report(
        investigation_id=inv_id,
        correlation_id=corr_id,
    )

    assert report is not None
    assert report.investigation_id == inv_id

    # Verify diagnostic record was emitted and recorded
    assert len(observed_diag_events) >= 1
    last_evt = observed_diag_events[-1]
    assert last_evt.correlation_id == corr_id
    assert last_evt.payload["investigation_id"] == inv_id
    assert last_evt.payload["report_id"] == report.report_id

    # Verify diagnostic is accessible from service
    diags = diag_service.list_diagnostics(investigation_id=inv_id)
    assert len(diags) >= 1
    assert any(d.report_id == report.report_id for d in diags)


# ==============================================================================
# Scenario 2: Reliability Degradation Generates Diagnostic and Informational Alert
# ==============================================================================

def test_acceptance_scenario_2_reliability_degradation_generates_diagnostic_and_alert():
    """Verify that reliability degradation triggers a diagnostic record and operational alert."""
    bus = EventBus("test_bus_s2")
    store = StateStore()
    clock = DeterministicClock(1700000000.0)

    diag_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        time_provider=clock,
        alert_cooldown_seconds=60.0,
        auto_subscribe=True,
    )

    reliability_service = HistoricalReviewAuditReliabilityService(
        event_bus=bus,
        state_store=store,
        audit_service=None,  # Unavailable audit service simulates degradation
    )

    triggered_alerts = []
    bus.subscribe(
        EventType.HISTORICAL_REVIEW_AUDIT_ALERT_TRIGGERED,
        lambda evt: triggered_alerts.append(evt),
    )

    inv_id = "inv_degraded_02"

    # Evaluate coverage on empty/unavailable data
    cov = reliability_service.evaluate_coverage(inv_id)
    assert cov.coverage_state in (AuditCoverageState.EMPTY, AuditCoverageState.UNAVAILABLE)

    # Diagnostic service should have processed the coverage degradation event
    diags = diag_service.list_diagnostics(investigation_id=inv_id)
    assert len(diags) >= 1
    diag = diags[0]
    assert diag.category == AuditDiagnosticCategory.COVERAGE_DEGRADATION
    assert diag.severity in (AuditDiagnosticSeverity.WARNING, AuditDiagnosticSeverity.ERROR)
    assert diag.evidence_nature == AuditEvidenceNature.OBSERVED_FAILURE

    # Verify operational alert was triggered
    assert len(triggered_alerts) >= 1
    alert_evt = triggered_alerts[-1]
    assert alert_evt.payload["investigation_id"] == inv_id
    assert alert_evt.payload["category"] == AuditDiagnosticCategory.COVERAGE_DEGRADATION.value
    assert alert_evt.payload["status"] == AuditAlertStatus.TRIGGERED.value


# ==============================================================================
# Scenario 3: Repeatability Divergence and Regression Traceable to Sources
# ==============================================================================

def test_acceptance_scenario_3_repeatability_divergence_and_regression_traceable_to_sources():
    """Verify that divergence and regression are traceable to their source records."""
    bus = EventBus("test_bus_s3")
    store = StateStore()
    clock = DeterministicClock(1700000000.0)

    diag_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        time_provider=clock,
        auto_subscribe=True,
    )

    inv_id = "inv_traceable_03"
    source_snap_id = "snap_inv_traceable_03_v1"
    source_rep_id = "audit_rep_traceable_001"
    corr_id = "corr_traceable_03"

    # Publish repeatability divergence event
    bus.publish(
        Event.create(
            event_type=EventType.HISTORICAL_REVIEW_AUDIT_REPEATABILITY_VERIFIED,
            source="HistoricalReviewAuditReliabilityService",
            payload={
                "investigation_id": inv_id,
                "is_repeatable": False,
                "repeatability_score": 0.65,
                "source_references": [source_snap_id],
                "report_id": source_rep_id,
            },
            correlation_id=corr_id,
        )
    )

    # Publish regression detected event
    bus.publish(
        Event.create(
            event_type=EventType.HISTORICAL_REVIEW_AUDIT_REGRESSION_DETECTED,
            source="HistoricalReviewAuditReliabilityService",
            payload={
                "investigation_id": inv_id,
                "regression_state": "REGRESSION_DETECTED",
                "new_findings_count": 3,
                "current_report_id": source_rep_id,
                "source_references": [source_snap_id],
            },
            correlation_id=corr_id,
        )
    )

    diags = diag_service.list_diagnostics(investigation_id=inv_id)
    assert len(diags) == 2

    # Check traceability on repeatability divergence
    rep_diags = [d for d in diags if d.category == AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE]
    assert len(rep_diags) == 1
    assert rep_diags[0].correlation_id == corr_id
    assert rep_diags[0].details["repeatability_score"] == 0.65

    # Check traceability on regression
    reg_diags = [d for d in diags if d.category == AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION]
    assert len(reg_diags) == 1
    assert reg_diags[0].correlation_id == corr_id
    assert reg_diags[0].report_id == source_rep_id
    assert reg_diags[0].details["new_findings_count"] == 3


# ==============================================================================
# Scenario 4: Repeated Failures Deduplicated at Alert Layer (Diagnostics Preserved)
# ==============================================================================

def test_acceptance_scenario_4_repeated_identical_failures_deduplicated_at_alert_layer():
    """Verify that identical failure alerts are deduplicated while all diagnostics are preserved."""
    bus = EventBus("test_bus_s4")
    store = StateStore()
    clock = DeterministicClock(1700000000.0)

    diag_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        time_provider=clock,
        alert_cooldown_seconds=120.0,
        auto_subscribe=True,
    )

    triggered_alerts = []
    suppressed_alerts = []
    bus.subscribe(
        EventType.HISTORICAL_REVIEW_AUDIT_ALERT_TRIGGERED,
        lambda evt: triggered_alerts.append(evt),
    )
    bus.subscribe(
        EventType.HISTORICAL_REVIEW_AUDIT_ALERT_SUPPRESSED,
        lambda evt: suppressed_alerts.append(evt),
    )

    inv_id = "inv_dedup_04"
    # Emit 5 identical failure events within cooldown
    for i in range(5):
        diag_service.record_diagnostic(
            investigation_id=inv_id,
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.ERROR,
            evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
            message="Repeated hardware timeout during audit validation.",
            correlation_id=f"corr_dedup_{i}",
        )
        clock.advance(10.0)  # Advance 10s per iteration (within 120s cooldown)

    # All 5 diagnostic records MUST be preserved in diagnostic history
    diags = diag_service.list_diagnostics(investigation_id=inv_id)
    assert len(diags) == 5

    # Alerts: Exactly 1 TRIGGERED alert, and 4 SUPPRESSED alerts
    assert len(triggered_alerts) == 1
    assert len(suppressed_alerts) == 4

    last_suppressed = suppressed_alerts[-1].payload
    assert last_suppressed["suppressed_occurrences"] == 4
    assert last_suppressed["status"] == AuditAlertStatus.SUPPRESSED_COOLDOWN.value


# ==============================================================================
# Scenario 5: Recovery Transitions Correctly Resolve Active Failures
# ==============================================================================

def test_acceptance_scenario_5_recovery_transitions_resolve_active_failures():
    """Verify that recovery events correctly transition active failure states to resolved."""
    bus = EventBus("test_bus_s5")
    store = StateStore()
    clock = DeterministicClock(1700000000.0)

    diag_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        time_provider=clock,
        auto_subscribe=True,
    )

    recovery_events = []
    bus.subscribe(
        EventType.HISTORICAL_REVIEW_AUDIT_RECOVERY_OBSERVED,
        lambda evt: recovery_events.append(evt),
    )

    inv_id = "inv_recovery_05"

    # Step 1: Record an active failure
    diag_service.record_diagnostic(
        investigation_id=inv_id,
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        message="Coverage dropped to zero.",
    )

    summary_before = diag_service.get_diagnostic_summary(inv_id)
    assert summary_before.has_active_failures is True
    assert summary_before.recovery_observed is False

    # Step 2: Observe recovery via reliability completion event
    clock.advance(30.0)
    bus.publish(
        Event.create(
            event_type=EventType.HISTORICAL_REVIEW_RELIABILITY_EVALUATION_COMPLETED,
            source="HistoricalReviewAuditReliabilityService",
            payload={
                "investigation_id": inv_id,
                "overall_reliability_status": "RELIABLE",
                "evaluation_id": "eval_rec_05",
            },
            correlation_id="corr_rec_05",
        )
    )

    # Step 3: Verify recovery transition
    assert len(recovery_events) == 1
    rec_evt = recovery_events[0]
    assert rec_evt.payload["investigation_id"] == inv_id
    assert rec_evt.payload["category"] == AuditDiagnosticCategory.RECOVERY_OBSERVED.value

    # Step 4: Verify active failures cleared in summary
    summary_after = diag_service.get_diagnostic_summary(inv_id)
    assert summary_after.has_active_failures is False
    assert summary_after.recovery_observed is True

    # Active alerts query returns 0 active alerts
    active_alerts = diag_service.list_alerts(investigation_id=inv_id, active_only=True)
    assert len(active_alerts) == 0


# ==============================================================================
# Scenario 6: Unavailable and Malformed Data Produces Explicit Statuses
# ==============================================================================

def test_acceptance_scenario_6_unavailable_and_malformed_data_produces_explicit_statuses():
    """Verify that missing or malformed data produces explicit statuses without fabrication."""
    bus = EventBus("test_bus_s6")
    store = StateStore()
    clock = DeterministicClock(1700000000.0)

    diag_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        time_provider=clock,
        auto_subscribe=True,
    )

    # 1. Non-finite timestamps (nan, inf) handled gracefully without throwing
    diag_id = compute_deterministic_diagnostic_id(
        investigation_id="inv_nan_test",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        timestamp=float("nan"),
        correlation_id="corr_nan",
    )
    assert diag_id.startswith("diag_")

    iso_nan = format_utc_iso(float("nan"))
    assert iso_nan == "1970-01-01T00:00:00Z"

    iso_inf = format_utc_iso(float("inf"))
    assert iso_inf == "1970-01-01T00:00:00Z"

    # 2. Malformed investigation ID in event payload handled via safe extraction without crashing
    bus.publish(
        Event.create(
            event_type=EventType.HISTORICAL_REVIEW_AUDIT_FAILED,
            source="test_source",
            payload={
                "investigation_id": "../malicious/path",
                "error": "Corrupted header in block.",
            },
            correlation_id="corr_malformed_06",
        )
    )

    # Diagnostic record recorded under safe fallback ID
    all_diags = diag_service.list_diagnostics()
    assert len(all_diags) >= 1
    malformed_diag = all_diags[0]
    assert malformed_diag.investigation_id.startswith("inv_malformed_")
    assert malformed_diag.evidence_nature == AuditEvidenceNature.OBSERVED_FAILURE


# ==============================================================================
# Scenario 7: Event Delivery and Listener Failure Isolation (EventBus Contract)
# ==============================================================================

def test_acceptance_scenario_7_event_delivery_and_listener_failure_isolation():
    """Verify EventBus isolates subscriber errors according to contract."""
    bus = EventBus("test_bus_s7")
    store = StateStore()

    diag_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        auto_subscribe=True,
    )

    # Register faulty listener that raises simulated exception
    def failing_listener(event: Event):
        raise RuntimeError("Controlled listener simulation fault")

    bus.subscribe(EventType.HISTORICAL_REVIEW_AUDIT_COMPLETED, failing_listener)

    # Publish audit completed event
    inv_id = "inv_fault_07"
    evt = Event.create(
        event_type=EventType.HISTORICAL_REVIEW_AUDIT_COMPLETED,
        source="test_source",
        payload={
            "investigation_id": inv_id,
            "overall_status": "PASSED",
            "findings_count": 0,
            "report_id": "rep_fault_07",
        },
        correlation_id="corr_fault_07",
    )

    # Publish should succeed and isolate the failing listener without raising
    dispatched = bus.publish(evt)
    assert dispatched >= 1

    # Verify diagnostic service processed the event despite the failing sibling listener
    diags = diag_service.list_diagnostics(investigation_id=inv_id)
    assert len(diags) >= 1
    assert diags[0].report_id == "rep_fault_07"


# ==============================================================================
# Scenario 8: StateStore Persistence and Cross-Service Reconstruction
# ==============================================================================

def test_acceptance_scenario_8_statestore_persistence_and_cross_service_reconstruction():
    """Verify StateStore persistence and retrieval remain consistent across service reconstruction."""
    store = StateStore()
    clock = DeterministicClock(1700000000.0)

    # Service 1 writes records
    svc1 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        time_provider=clock,
        auto_subscribe=False,
    )

    inv_id = "inv_reconstruct_08"
    rec = svc1.record_diagnostic(
        investigation_id=inv_id,
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        message="Persistence test integrity regression.",
        correlation_id="corr_rec_08",
        source_references=("urn:kaiser:snap:v1",),
    )

    alert = svc1.list_alerts(investigation_id=inv_id)[0]

    # Service 2 initialized afresh with the same backing StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        time_provider=clock,
        auto_subscribe=False,
    )

    # 1. Fetch individual diagnostic record
    rec_fetched = svc2.get_diagnostic(rec.diagnostic_id)
    assert rec_fetched is not None
    assert rec_fetched.diagnostic_id == rec.diagnostic_id
    assert rec_fetched.message == rec.message
    assert rec_fetched.source_references == rec.source_references

    # 2. Fetch individual alert
    alert_fetched = svc2.get_alert(alert.alert_id)
    assert alert_fetched is not None
    assert alert_fetched.alert_id == alert.alert_id
    assert alert_fetched.category == alert.category

    # 3. List diagnostics hydrated from StateStore history
    diags_hydrated = svc2.list_diagnostics(investigation_id=inv_id)
    assert len(diags_hydrated) >= 1
    assert diags_hydrated[0].diagnostic_id == rec.diagnostic_id

    # 4. Summary hydrated correctly
    summary_hydrated = svc2.get_diagnostic_summary(inv_id)
    assert summary_hydrated.total_diagnostics >= 1
    assert summary_hydrated.has_active_failures is True


# ==============================================================================
# Scenario 9: Concurrent Operations Thread Safety
# ==============================================================================

def test_acceptance_scenario_9_concurrent_audit_reliability_and_diagnostic_operations():
    """Verify thread-safety across concurrent diagnostic, alert, and query operations."""
    store = StateStore()
    clock = DeterministicClock(1700000000.0)
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        time_provider=clock,
        max_diagnostics=200,
        max_alerts=200,
        auto_subscribe=False,
    )

    num_threads = 8
    ops_per_thread = 25
    inv_ids = [f"inv_concurrent_{i}" for i in range(num_threads)]

    def worker(thread_idx: int):
        inv = inv_ids[thread_idx]
        for op in range(ops_per_thread):
            # Record diagnostic
            svc.record_diagnostic(
                investigation_id=inv,
                category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
                severity=AuditDiagnosticSeverity.WARNING,
                evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
                message=f"Thread {thread_idx} op {op} diagnostic",
                correlation_id=f"corr_t{thread_idx}_{op}",
            )
            # Query
            _ = svc.list_diagnostics(investigation_id=inv)
            _ = svc.get_diagnostic_summary(inv)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, i) for i in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            f.result()  # Will raise if any thread encountered an exception

    total_diags = len(svc.list_diagnostics(limit=100))
    assert total_diags > 0


# ==============================================================================
# Scenario 10: Bounded Retention Under Sustained Input
# ==============================================================================

def test_acceptance_scenario_10_bounded_retention_under_sustained_input():
    """Verify memory remains strictly bounded under sustained input."""
    clock = DeterministicClock(1700000000.0)
    max_d = 15
    max_a = 15

    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=max_d,
        max_alerts=max_a,
        alert_cooldown_seconds=0.0,  # Zero cooldown to allow new alerts
        time_provider=clock,
        auto_subscribe=False,
    )

    # Sustained input of 60 diagnostics across different categories
    for i in range(60):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id=f"inv_sustained_{i}",
            category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
            severity=AuditDiagnosticSeverity.ERROR,
            evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
            message=f"Sustained record {i}",
        )

    # Internal queues must not exceed max bounds
    assert len(svc._diagnostics) == max_d
    assert len(svc._diagnostics_by_id) == max_d
    assert len(svc._alerts) == max_a
    assert len(svc._alerts_by_id) == max_a


# ==============================================================================
# Scenario 11: Correlation IDs and Source References Consistency
# ==============================================================================

def test_acceptance_scenario_11_correlation_ids_and_source_references_consistency():
    """Verify correlation IDs and source references remain consistent across the pipeline."""
    bus = EventBus("test_bus_s11")
    store = StateStore()
    clock = DeterministicClock(1700000000.0)

    svc = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        time_provider=clock,
        auto_subscribe=True,
    )

    observed_diag_events = []
    observed_alert_events = []
    bus.subscribe(
        EventType.HISTORICAL_REVIEW_DIAGNOSTIC_RECORDED,
        lambda evt: observed_diag_events.append(evt),
    )
    bus.subscribe(
        EventType.HISTORICAL_REVIEW_AUDIT_ALERT_TRIGGERED,
        lambda evt: observed_alert_events.append(evt),
    )

    inv_id = "inv_pipeline_11"
    corr_id = "corr_pipe_exact_123"
    refs = ("urn:kaiser:snap:v1", "urn:kaiser:audit_rep:001")

    # Record diagnostic with explicit correlation ID and source references
    rec = svc.record_diagnostic(
        investigation_id=inv_id,
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        message="Integrity regression detected in chain.",
        correlation_id=corr_id,
        source_references=refs,
    )

    assert rec.correlation_id == corr_id
    assert rec.source_references == refs

    # Check diagnostic event
    assert len(observed_diag_events) == 1
    assert observed_diag_events[0].correlation_id == corr_id
    assert tuple(observed_diag_events[0].payload["source_references"]) == refs

    # Check alert event
    assert len(observed_alert_events) == 1
    assert observed_alert_events[0].correlation_id == corr_id
    assert tuple(observed_alert_events[0].payload["source_references"]) == refs


# ==============================================================================
# Scenario 12: Dashboard Endpoint 28 Security and Sanitization
# ==============================================================================

def test_acceptance_scenario_12_dashboard_endpoint_28_security_and_sanitization():
    """Verify Dashboard Endpoint 28 enforces security, read-only guards, and sanitization."""
    store = StateStore()
    clock = DeterministicClock(1700000000.0)
    diag_svc = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        time_provider=clock,
        auto_subscribe=False,
    )

    inv_id = "inv_sec_12"
    rec = diag_svc.record_diagnostic(
        investigation_id=inv_id,
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.WARNING,
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        message="Sensitive test: api_key=ghp_secretpassword12345678901234567890",
        correlation_id="corr_sec_12",
        details={"secret_token": "bearer eyJhbGciOiJIUzI1NiJ9.test"},
    )
    alert = diag_svc.list_alerts(investigation_id=inv_id)[0]

    port = find_free_port()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=port,
        audit_diagnostics_service=diag_svc,
        state_store=store,
    )
    server.start()

    try:
        base_url = server.url

        # 1. Valid GET /api/historical-review-diagnostics
        req = urllib.request.Request(f"{base_url}/api/historical-review-diagnostics?investigation_id={inv_id}")
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["status"] == "success"
            assert body["read_only"] is True
            assert body["execution_locked"] is True
            assert body["count"] == 1
            # Check secret sanitization in response
            diag_data = body["diagnostics"][0]
            assert "ghp_secretpassword" not in json.dumps(diag_data)
            assert "eyJhbGciOiJIUzI1NiJ9" not in json.dumps(diag_data)

        # 2. Valid GET individual diagnostic
        with urllib.request.urlopen(f"{base_url}/api/historical-review-diagnostics/{rec.diagnostic_id}", timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["diagnostic"]["diagnostic_id"] == rec.diagnostic_id

        # 3. Valid GET /alerts and individual alert
        with urllib.request.urlopen(f"{base_url}/api/historical-review-diagnostics/alerts?investigation_id={inv_id}", timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["count"] == 1

        with urllib.request.urlopen(f"{base_url}/api/historical-review-diagnostics/alerts/{alert.alert_id}", timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["alert"]["alert_id"] == alert.alert_id

        # 4. Valid GET /summary
        with urllib.request.urlopen(f"{base_url}/api/historical-review-diagnostics/summary?investigation_id={inv_id}", timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["summary"]["investigation_id"] == inv_id

        # 5. HEAD request support
        req_head = urllib.request.Request(f"{base_url}/api/historical-review-diagnostics", method="HEAD")
        with urllib.request.urlopen(req_head, timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK
            assert resp.read() == b""

        # 6. Rejection of mutation methods (POST, PUT, DELETE) -> HTTP 405
        for m in ("POST", "PUT", "DELETE", "PATCH"):
            req_mut = urllib.request.Request(
                f"{base_url}/api/historical-review-diagnostics",
                data=b"{}",
                method=m,
                headers={"Content-Type": "application/json"},
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req_mut, timeout=3.0)
            assert exc_info.value.code == HTTPStatus.METHOD_NOT_ALLOWED

        # 7. Path traversal rejection -> HTTP 400
        traversal_urls = [
            f"{base_url}/api/historical-review-diagnostics/..%2f..%2fetc%2fpasswd",
            f"{base_url}/api/historical-review-diagnostics?investigation_id=..%2fbad",
        ]
        for t_url in traversal_urls:
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(t_url, timeout=3.0)
            assert exc_info.value.code in (HTTPStatus.BAD_REQUEST, HTTPStatus.NOT_FOUND)

        # 8. Unknown sub-resource -> HTTP 404
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/api/historical-review-diagnostics/nonexistent_sub/path", timeout=3.0)
        assert exc_info.value.code == HTTPStatus.NOT_FOUND

    finally:
        server.stop()


# ==============================================================================
# Scenario 13: Compatibility with Existing Endpoints 26 and 27
# ==============================================================================

def test_acceptance_scenario_13_existing_endpoints_26_and_27_compatibility():
    """Verify Endpoints 26 and 27 remain fully functional and security-hardened."""
    store = StateStore()
    ev_svc = HistoricalReviewEvolutionService(state_store=store)
    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc, state_store=store)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, state_store=store)

    port = find_free_port()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=port,
        audit_service=audit_svc,
        reliability_service=rel_svc,
        state_store=store,
    )
    server.start()

    try:
        base_url = server.url

        # Endpoint 26: /api/historical-review-audit
        with urllib.request.urlopen(f"{base_url}/api/historical-review-audit", timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["read_only"] is True
            assert body["execution_locked"] is True

        # Endpoint 27: /api/historical-review-reliability
        with urllib.request.urlopen(f"{base_url}/api/historical-review-reliability", timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK
            body = json.loads(resp.read().decode("utf-8"))
            assert body["read_only"] is True
            assert body["execution_locked"] is True

        # Endpoint 26 HEAD
        req_head = urllib.request.Request(f"{base_url}/api/historical-review-audit", method="HEAD")
        with urllib.request.urlopen(req_head, timeout=3.0) as resp:
            assert resp.status == HTTPStatus.OK

        # Endpoint 27 mutation rejection -> HTTP 405
        req_mut = urllib.request.Request(f"{base_url}/api/historical-review-reliability", data=b"{}", method="POST")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_mut, timeout=3.0)
        assert exc_info.value.code == HTTPStatus.METHOD_NOT_ALLOWED

    finally:
        server.stop()


# ==============================================================================
# Scenario 14: Safety Invariants & Execution Engine Locked
# ==============================================================================

def test_acceptance_scenario_14_safety_invariants_and_execution_engine_locked():
    """Verify system-wide safety invariants remain intact."""
    settings = Settings()
    assert settings.execution_locked is True

    # No prohibited ranking/trading terms in diagnostic models
    from app.analysis.opportunities.evolution.audit.diagnostics.models import PROHIBITED_WORDS
    assert "buy" in PROHIBITED_WORDS
    assert "sell" in PROHIBITED_WORDS
    assert "win_probability" in PROHIBITED_WORDS
    assert "profit_prediction" in PROHIBITED_WORDS
    assert "strategy_ranking" in PROHIBITED_WORDS
    assert "order_placement" in PROHIBITED_WORDS

    # Ensure prohibited words cannot be recorded in messages
    with pytest.raises(ValidationError):
        from app.analysis.opportunities.evolution.audit.diagnostics.models import check_prohibited_terms
        check_prohibited_terms("Target price recommendation buy now", "test")
