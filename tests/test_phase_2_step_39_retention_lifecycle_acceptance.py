"""KAISER — Phase 2, Step 39 Acceptance Testing Suite.

Historical Review Diagnostics Retention & Alert Lifecycle End-to-End Acceptance Testing Foundation.

TEST SCENARIOS:
1. Diagnostics and alerts remain within configured capacities.
2. Eviction counters and per-investigation retention statistics remain accurate after repeated overflow.
3. Retention status correctly distinguishes COMPLETE, TRUNCATED_TO_CAPACITY, and NO_DATA.
4. Acknowledged alerts remain unresolved and appear in active_only=True results.
5. Resolved alerts are excluded from active-only results.
6. Suppressed alerts remain inspectable when include_suppressed=True.
7. Repeated failures across service reconstruction preserve cooldown and deduplication behavior.
8. StateStore hydration restores active failures, retention counters, and cooldown metadata consistently.
9. Recovery for one target category does not clear unrelated active failure categories.
10. Diagnostic eviction does not incorrectly resolve an active alert.
11. Report and evaluation references remain consistent across diagnostics and alerts.
12. Concurrent reads, writes, acknowledgements, and resolutions remain thread-safe.
13. Dashboard /retention and alert filtering behave correctly.
14. Existing dashboard Endpoints 26 and 27 remain compatible.
15. Malformed or incomplete persisted state is handled safely without fabricating history or crashing startup.
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import threading
import time
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.error

import pytest

from app.analysis.opportunities.evolution.audit.diagnostics.models import (
    AuditAlert,
    AuditAlertStatus,
    AuditDiagnosticCategory,
    AuditDiagnosticRecord,
    AuditDiagnosticSeverity,
    AuditDiagnosticSummary,
    AuditEvidenceNature,
)
from app.analysis.opportunities.evolution.audit.diagnostics.service import (
    HistoricalReviewAuditDiagnosticsService,
)
from app.analysis.opportunities.evolution.audit.service import HistoricalReviewAuditService
from app.analysis.opportunities.evolution.audit.reliability.service import (
    HistoricalReviewAuditReliabilityService,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.state import StateStore
from app.portfolio.dashboard.server import PortfolioDashboardServer


class DeterministicClock:
    """Deterministic injectable clock fixture for cooldown and retention testing."""

    def __init__(self, initial_timestamp: float = 1700000000.0) -> None:
        self._current_time = float(initial_timestamp)

    def time(self) -> float:
        return self._current_time

    def advance(self, seconds: float) -> None:
        self._current_time += float(seconds)


# ==============================================================================
# Scenario 1: Capacity Enforcement Under Overflow
# ==============================================================================

def test_acceptance_scenario_1_capacity_enforcement_under_overflow():
    """Verify diagnostics and alerts strictly remain within their configured capacities."""
    clock = DeterministicClock()
    max_diag = 12
    max_alert = 10

    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=max_diag,
        max_alerts=max_alert,
        time_provider=clock.time,
    )

    # Ingest 30 diagnostics and 25 distinct alerts
    for i in range(30):
        clock.advance(1.0)
        # Alternate severity to create alerts
        sev = AuditDiagnosticSeverity.ERROR if i < 25 else AuditDiagnosticSeverity.INFO
        svc.record_diagnostic(
            investigation_id=f"inv_cap_{i}",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=sev,
            message=f"Capacity test diagnostic {i}",
        )

    # In-memory ring buffer lengths strictly bounded
    assert len(svc._diagnostics) == max_diag
    assert len(svc._alerts) == max_alert
    assert len(svc._diagnostics_by_id) == max_diag
    assert len(svc._alerts_by_id) == max_alert

    # list queries adhere to capacity
    all_diags = svc.list_diagnostics(limit=100)
    all_alerts = svc.list_alerts(limit=100)
    assert len(all_diags) == max_diag
    assert len(all_alerts) == max_alert


# ==============================================================================
# Scenario 2: Eviction Counters and Per-Investigation Retention Statistics
# ==============================================================================

def test_acceptance_scenario_2_eviction_counters_and_per_investigation_retention_stats():
    """Verify eviction counters and per-investigation retention statistics remain accurate after repeated overflow."""
    clock = DeterministicClock()
    max_diag = 10
    max_alert = 10

    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=max_diag,
        max_alerts=max_alert,
        time_provider=clock.time,
    )

    # Investigation A: 8 records (each separated by 65s > 60s cooldown to trigger distinct alerts)
    for i in range(8):
        clock.advance(65.0)
        svc.record_diagnostic(
            investigation_id="inv_alpha",
            category=AuditDiagnosticCategory.INTEGRITY_VIOLATION,
            severity=AuditDiagnosticSeverity.ERROR,
            message=f"Alpha failure {i}",
        )

    # Investigation B: 8 records (pushes 6 of Alpha out of diagnostics & alerts)
    for i in range(8):
        clock.advance(65.0)
        svc.record_diagnostic(
            investigation_id="inv_beta",
            category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
            severity=AuditDiagnosticSeverity.ERROR,
            message=f"Beta failure {i}",
        )

    # Investigation C: 8 records (pushes remaining Alpha and some Beta out)
    for i in range(8):
        clock.advance(65.0)
        svc.record_diagnostic(
            investigation_id="inv_gamma",
            category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
            severity=AuditDiagnosticSeverity.ERROR,
            message=f"Gamma failure {i}",
        )

    # Total recorded: 24. Capacity: 10. Total evictions: 14
    ret_status = svc.get_retention_status()
    assert ret_status["total_diagnostics_evicted"] == 14
    assert ret_status["total_alerts_evicted"] == 14
    assert ret_status["current_diagnostics_count"] == 10
    assert ret_status["current_alerts_count"] == 10

    # Per-investigation eviction counts sum up to total
    sum_diag_evictions = sum(svc._evicted_diagnostics_by_inv.values())
    sum_alert_evictions = sum(svc._evicted_alerts_by_inv.values())
    assert sum_diag_evictions == 14
    assert sum_alert_evictions == 14

    # Alpha was completely evicted (8 recorded, 8 evicted)
    assert svc._evicted_diagnostics_by_inv["inv_alpha"] == 8
    assert svc._evicted_alerts_by_inv["inv_alpha"] == 8


# ==============================================================================
# Scenario 3: Retention Status Distinction: COMPLETE, TRUNCATED_TO_CAPACITY, NO_DATA
# ==============================================================================

def test_acceptance_scenario_3_retention_status_distinction():
    """Verify retention status accurately distinguishes COMPLETE, TRUNCATED_TO_CAPACITY, and NO_DATA."""
    clock = DeterministicClock()
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock.time,
    )

    # 1. NO_DATA: Nonexistent investigation
    summary_empty = svc.get_diagnostic_summary("inv_nonexistent_99")
    assert summary_empty.retention_status == "NO_DATA"
    assert summary_empty.is_truncated is False
    assert summary_empty.total_diagnostics == 0
    assert summary_empty.total_alerts == 0

    # 2. COMPLETE: Records within capacity without evictions
    for i in range(5):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_within_cap",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Record {i}",
        )

    summary_complete = svc.get_diagnostic_summary("inv_within_cap")
    assert summary_complete.retention_status == "COMPLETE"
    assert summary_complete.is_truncated is False
    assert summary_complete.total_diagnostics == 5
    assert summary_complete.evicted_diagnostics_count == 0

    # 3. TRUNCATED_TO_CAPACITY: Overflow causing evictions
    for i in range(12):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_overflow",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Overflow record {i}",
        )

    summary_truncated = svc.get_diagnostic_summary("inv_overflow")
    assert summary_truncated.retention_status == "TRUNCATED_TO_CAPACITY"
    assert summary_truncated.is_truncated is True
    assert summary_truncated.evicted_diagnostics_count > 0


# ==============================================================================
# Scenario 4: Acknowledged Alerts Remain Unresolved in Active-Only Results
# ==============================================================================

def test_acceptance_scenario_4_acknowledged_alerts_remain_unresolved_in_active_only():
    """Verify acknowledged alerts remain unresolved and are returned by active_only=True queries."""
    svc = HistoricalReviewAuditDiagnosticsService()

    rec = svc.record_diagnostic(
        investigation_id="inv_ack_test",
        category=AuditDiagnosticCategory.INTEGRITY_VIOLATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Integrity violation detected.",
    )
    alerts = svc.list_alerts(investigation_id="inv_ack_test")
    assert len(alerts) == 1
    alert_id = alerts[0].alert_id
    assert alerts[0].status == AuditAlertStatus.TRIGGERED

    # Acknowledge the alert
    ack_alert = svc.acknowledge_alert(alert_id, acknowledged_by="lead_sec_auditor")
    assert ack_alert is not None
    assert ack_alert.status == AuditAlertStatus.ACKNOWLEDGED

    # Active-only query must still return this acknowledged alert
    active_alerts = svc.list_alerts(investigation_id="inv_ack_test", active_only=True)
    assert len(active_alerts) == 1
    assert active_alerts[0].alert_id == alert_id
    assert active_alerts[0].status == AuditAlertStatus.ACKNOWLEDGED

    # Diagnostic summary must count it as active failure and active alert
    summary = svc.get_diagnostic_summary("inv_ack_test")
    assert summary.active_alerts_count == 1
    assert summary.has_active_failures is True


# ==============================================================================
# Scenario 5: Resolved Alerts Excluded from Active-Only Results
# ==============================================================================

def test_acceptance_scenario_5_resolved_alerts_excluded_from_active_only():
    """Verify resolved alerts are strictly excluded from active_only results and update failure lifecycle."""
    svc = HistoricalReviewAuditDiagnosticsService()

    rec = svc.record_diagnostic(
        investigation_id="inv_resolve_test",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Coverage degradation failure.",
    )
    alert = svc.list_alerts(investigation_id="inv_resolve_test")[0]

    # Resolve alert
    res_alert = svc.resolve_alert(alert.alert_id, reason="Missing historical data backfilled and verified.")
    assert res_alert is not None
    assert res_alert.status == AuditAlertStatus.RESOLVED
    assert "Resolved: Missing historical data backfilled" in res_alert.reason

    # Active-only query must now exclude the resolved alert
    active_alerts = svc.list_alerts(investigation_id="inv_resolve_test", active_only=True)
    assert len(active_alerts) == 0

    # Summary reflects zero active alerts and no active failure
    summary = svc.get_diagnostic_summary("inv_resolve_test")
    assert summary.active_alerts_count == 0
    assert summary.has_active_failures is False

    # Resolving again is idempotent
    re_resolved = svc.resolve_alert(alert.alert_id, reason="Second resolve")
    assert re_resolved.status == AuditAlertStatus.RESOLVED


# ==============================================================================
# Scenario 6: Suppressed Alerts Inspectable When include_suppressed=True
# ==============================================================================

def test_acceptance_scenario_6_suppressed_alerts_inspectable_when_requested():
    """Verify suppressed alerts are deduplicated by default but inspectable when include_suppressed=True."""
    clock = DeterministicClock()
    svc = HistoricalReviewAuditDiagnosticsService(
        alert_cooldown_seconds=120.0,
        time_provider=clock.time,
    )

    # 1. Initial alert at t=0
    svc.record_diagnostic(
        investigation_id="inv_supp_test",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Divergence in execution findings.",
    )
    assert len(svc.list_alerts(investigation_id="inv_supp_test")) == 1

    # 2. Duplicate failure at t=30 (within 120s cooldown)
    clock.advance(30.0)
    svc.record_diagnostic(
        investigation_id="inv_supp_test",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Divergence in execution findings (repeat).",
    )

    # Default list_alerts does NOT include suppressed alert
    default_alerts = svc.list_alerts(investigation_id="inv_supp_test")
    assert len(default_alerts) == 1
    assert default_alerts[0].status == AuditAlertStatus.TRIGGERED

    # With include_suppressed=True, suppressed alert is visible
    all_alerts = svc.list_alerts(investigation_id="inv_supp_test", include_suppressed=True)
    assert len(all_alerts) == 2
    suppressed = [a for a in all_alerts if a.status == AuditAlertStatus.SUPPRESSED_COOLDOWN]
    assert len(suppressed) == 1
    assert suppressed[0].suppressed_occurrences == 1

    # Direct ID lookup retrieves suppressed alert
    supp_id = suppressed[0].alert_id
    assert svc.get_alert(supp_id) is not None
    assert svc.get_alert(supp_id).status == AuditAlertStatus.SUPPRESSED_COOLDOWN


# ==============================================================================
# Scenario 7: Repeated Failures Across Service Reconstruction Preserve Cooldown
# ==============================================================================

def test_acceptance_scenario_7_repeated_failures_across_service_reconstruction():
    """Verify alert cooldown and suppression state persists across service reconstruction."""
    clock = DeterministicClock(1700000000.0)
    store = StateStore()

    svc1 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        alert_cooldown_seconds=60.0,
        time_provider=clock.time,
    )

    # Initial alert at t=1700000000.0 (cooldown until 1700000060.0)
    svc1.record_diagnostic(
        investigation_id="inv_cd_recon",
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Regression detected.",
    )
    alerts1 = svc1.list_alerts(investigation_id="inv_cd_recon")
    assert len(alerts1) == 1
    assert alerts1[0].status == AuditAlertStatus.TRIGGERED

    # Reconstruct service at t=1700000025.0 (< 1700000060.0)
    clock.advance(25.0)
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        alert_cooldown_seconds=60.0,
        time_provider=clock.time,
        auto_hydrate=True,
    )

    # Ingest duplicate failure under active cooldown: must be SUPPRESSED!
    svc2.record_diagnostic(
        investigation_id="inv_cd_recon",
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Regression detected (duplicate).",
    )
    suppressed = [a for a in svc2.list_alerts(investigation_id="inv_cd_recon", include_suppressed=True) if a.status == AuditAlertStatus.SUPPRESSED_COOLDOWN]
    assert len(suppressed) == 1
    assert suppressed[0].suppressed_occurrences == 1

    # Advance clock past cooldown: t=1700000070.0 (> 1700000060.0)
    clock.advance(45.0)
    svc2.record_diagnostic(
        investigation_id="inv_cd_recon",
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Regression detected (post cooldown).",
    )
    triggered_alerts = [a for a in svc2.list_alerts(investigation_id="inv_cd_recon") if a.status == AuditAlertStatus.TRIGGERED]
    assert len(triggered_alerts) == 2
    assert triggered_alerts[0].suppressed_occurrences == 1


# ==============================================================================
# Scenario 8: StateStore Hydration Restores Failures, Counters, and Metadata
# ==============================================================================

def test_acceptance_scenario_8_statestore_hydration_restores_all_metadata():
    """Verify StateStore hydration restores active failures, eviction counters, and cooldown metadata consistently."""
    clock = DeterministicClock()
    store = StateStore()

    svc1 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock.time,
    )

    # Ingest 15 diagnostics to cause 5 evictions and active failure
    for i in range(15):
        clock.advance(1.0)
        svc1.record_diagnostic(
            investigation_id="inv_hydrate_full",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.ERROR,
            message=f"Evaluation failure #{i}",
        )

    ret1 = svc1.get_retention_status()
    assert ret1["total_diagnostics_evicted"] == 5
    assert ret1["active_failures_tracked"] == 1

    # Reconstruct service with auto_hydrate=True
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock.time,
        auto_hydrate=True,
    )

    ret2 = svc2.get_retention_status()
    assert ret2["total_diagnostics_evicted"] == ret1["total_diagnostics_evicted"]
    assert ret2["total_alerts_evicted"] == ret1["total_alerts_evicted"]
    assert ret2["active_failures_tracked"] == ret1["active_failures_tracked"]
    assert ret2["cooldown_entries_active"] == ret1["cooldown_entries_active"]

    # Summary on reconstructed service matches
    summary2 = svc2.get_diagnostic_summary("inv_hydrate_full")
    assert summary2.has_active_failures is True
    assert summary2.retention_status == "TRUNCATED_TO_CAPACITY"
    assert summary2.is_truncated is True
    assert summary2.evicted_diagnostics_count == 5


# ==============================================================================
# Scenario 9: Target-Category Compatible Recovery
# ==============================================================================

def test_acceptance_scenario_9_target_category_compatible_recovery():
    """Verify recovery for one target category does not clear unrelated active failure categories."""
    svc = HistoricalReviewAuditDiagnosticsService()
    inv_id = "inv_target_recov"

    # Record 2 distinct failure categories
    svc.record_diagnostic(
        investigation_id=inv_id,
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Coverage degraded to PARTIAL.",
    )
    svc.record_diagnostic(
        investigation_id=inv_id,
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Integrity regression discovered.",
    )

    summary_start = svc.get_diagnostic_summary(inv_id)
    assert summary_start.has_active_failures is True
    assert summary_start.active_alerts_count == 2

    # Targeted recovery for COVERAGE_DEGRADATION only
    recov1 = svc.check_and_record_recovery(
        investigation_id=inv_id,
        target_categories=[AuditDiagnosticCategory.COVERAGE_DEGRADATION],
    )
    assert recov1 is not None

    # COVERAGE_DEGRADATION is resolved, but HISTORICAL_INTEGRITY_REGRESSION remains active!
    summary_mid = svc.get_diagnostic_summary(inv_id)
    assert summary_mid.has_active_failures is True
    assert summary_mid.active_alerts_count == 1

    # Targeted recovery for HISTORICAL_INTEGRITY_REGRESSION clears remaining failure
    recov2 = svc.check_and_record_recovery(
        investigation_id=inv_id,
        target_categories=[AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION],
    )
    assert recov2 is not None

    summary_end = svc.get_diagnostic_summary(inv_id)
    assert summary_end.has_active_failures is False
    assert summary_end.active_alerts_count == 0


# ==============================================================================
# Scenario 10: Diagnostic Eviction Does Not Resolve Active Alert
# ==============================================================================

def test_acceptance_scenario_10_diagnostic_eviction_does_not_resolve_active_alert():
    """Verify that an active alert remains active even if its originating diagnostic record is evicted."""
    clock = DeterministicClock()
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock.time,
    )
    inv_id = "inv_evict_keep_alert"

    # Initial critical diagnostic causing an alert
    rec_crit = svc.record_diagnostic(
        investigation_id=inv_id,
        category=AuditDiagnosticCategory.INTEGRITY_VIOLATION,
        severity=AuditDiagnosticSeverity.CRITICAL,
        message="Critical audit integrity violation.",
    )
    alert = svc.list_alerts(investigation_id=inv_id)[0]
    assert alert.status == AuditAlertStatus.TRIGGERED

    # Ingest 15 subsequent informational diagnostics to push rec_crit out of the ring buffer
    for i in range(15):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id=inv_id,
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Pusher record {i}",
        )

    # Diagnostic was evicted from in-memory ring buffer
    assert svc.is_diagnostic_evicted(rec_crit.diagnostic_id) is True

    # Critical invariant: Alert MUST NOT be cleared or resolved due to diagnostic eviction
    alert_check = svc.get_alert(alert.alert_id)
    assert alert_check.status == AuditAlertStatus.TRIGGERED
    summary = svc.get_diagnostic_summary(inv_id)
    assert summary.has_active_failures is True
    assert summary.active_alerts_count == 1


# ==============================================================================
# Scenario 11: Traceability Across Diagnostics and Alerts
# ==============================================================================

def test_acceptance_scenario_11_traceability_report_evaluation_references():
    """Verify report and evaluation references are consistently preserved across diagnostics and alerts."""
    svc = HistoricalReviewAuditDiagnosticsService()
    inv_id = "inv_trace_ref"
    rep_id = "rep_audit_999"
    eval_id = "eval_rel_888"

    rec = svc.record_diagnostic(
        investigation_id=inv_id,
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Evaluation discrepancy.",
        report_id=rep_id,
        evaluation_id=eval_id,
        source_references=("urn:kaiser:evidence:1", "urn:kaiser:evidence:2"),
    )

    alert = svc.list_alerts(investigation_id=inv_id)[0]
    assert alert.report_id == rep_id
    assert alert.evaluation_id == eval_id
    assert alert.diagnostic_id == rec.diagnostic_id
    assert "urn:kaiser:evidence:1" in alert.source_references
    assert "urn:kaiser:evidence:2" in alert.source_references

    # Serialization and deserialization preserves all fields
    alert_dict = alert.to_dict()
    assert alert_dict["report_id"] == rep_id
    assert alert_dict["evaluation_id"] == eval_id

    reconstructed_alert = AuditAlert.from_dict(alert_dict)
    assert reconstructed_alert.report_id == rep_id
    assert reconstructed_alert.evaluation_id == eval_id


# ==============================================================================
# Scenario 12: Concurrent Operations Thread Safety
# ==============================================================================

def test_acceptance_scenario_12_concurrent_reads_writes_acknowledgements_resolutions():
    """Verify thread-safety under heavy concurrent ingestion, queries, acknowledgements, and resolutions."""
    store = StateStore()
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        max_diagnostics=20,
        max_alerts=20,
    )

    exceptions = []

    def concurrent_worker(worker_idx: int):
        try:
            inv = f"inv_worker_{worker_idx}"
            for step in range(25):
                # 1. Write diagnostic
                rec = svc.record_diagnostic(
                    investigation_id=inv,
                    category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC if step % 2 == 0 else AuditDiagnosticCategory.COVERAGE_DEGRADATION,
                    severity=AuditDiagnosticSeverity.INFO if step % 2 == 0 else AuditDiagnosticSeverity.WARNING,
                    message=f"Concurrent record w{worker_idx} s{step}",
                )
                # 2. Read diagnostics & summary
                _ = svc.list_diagnostics(investigation_id=inv)
                _ = svc.get_diagnostic_summary(inv)
                _ = svc.get_retention_status()

                # 3. Read alerts and mutate state
                alerts = svc.list_alerts(investigation_id=inv, active_only=True)
                if alerts:
                    target_id = alerts[0].alert_id
                    if step % 3 == 0:
                        svc.acknowledge_alert(target_id, acknowledged_by=f"w{worker_idx}")
                    elif step % 5 == 0:
                        svc.resolve_alert(target_id, reason="Resolved in concurrent worker")
        except Exception as exc:
            exceptions.append(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(concurrent_worker, i) for i in range(8)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    assert len(exceptions) == 0, f"Encountered thread safety exceptions: {exceptions}"
    assert len(svc._diagnostics) <= 20
    assert len(svc._alerts) <= 20


# ==============================================================================
# Scenario 13: Dashboard /retention and Alert Filtering
# ==============================================================================

def test_acceptance_scenario_13_dashboard_endpoint_28_retention_and_alert_filtering():
    """Verify Dashboard Endpoint 28 /retention route, query filtering, and HTTP method security."""
    clock = DeterministicClock()
    diag_svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=15,
        max_alerts=15,
        time_provider=clock.time,
        alert_cooldown_seconds=60.0,
    )

    # Ingest records with active, acknowledged, and suppressed alerts
    clock.advance(1.0)
    rec1 = diag_svc.record_diagnostic(
        investigation_id="inv_dash_filter",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Coverage degradation.",
    )
    a1 = diag_svc.list_alerts(investigation_id="inv_dash_filter")[0]
    diag_svc.acknowledge_alert(a1.alert_id)

    # Duplicate under cooldown (suppressed)
    clock.advance(10.0)
    diag_svc.record_diagnostic(
        investigation_id="inv_dash_filter",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Coverage degradation (duplicate).",
    )

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        audit_diagnostics_service=diag_svc,
    )
    server.start()
    time.sleep(0.1)

    port = server.port
    base_url = f"http://127.0.0.1:{port}"

    try:
        # 1. GET /api/historical-review-diagnostics/retention
        req_ret = urllib.request.Request(f"{base_url}/api/historical-review-diagnostics/retention")
        with urllib.request.urlopen(req_ret, timeout=3.0) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert "retention" in data
            assert data["retention"]["max_diagnostics_capacity"] == 15

        # 2. HEAD /api/historical-review-diagnostics/retention
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3.0)
        conn.request("HEAD", "/api/historical-review-diagnostics/retention")
        head_resp = conn.getresponse()
        assert head_resp.status == 200
        assert head_resp.read() == b""

        # 3. POST /api/historical-review-diagnostics/retention -> 405 Method Not Allowed
        conn.request("POST", "/api/historical-review-diagnostics/retention", body=b"{}")
        post_resp = conn.getresponse()
        assert post_resp.status == 405
        assert "GET" in post_resp.getheader("Allow", "")

        # 4. GET /api/historical-review-diagnostics/alerts?active_only=true (returns ACKNOWLEDGED alert)
        req_active = urllib.request.Request(f"{base_url}/api/historical-review-diagnostics/alerts?active_only=true")
        with urllib.request.urlopen(req_active, timeout=3.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] == 1
            assert data["alerts"][0]["status"] == AuditAlertStatus.ACKNOWLEDGED.value

        # 5. GET /api/historical-review-diagnostics/alerts?include_suppressed=true
        req_supp = urllib.request.Request(f"{base_url}/api/historical-review-diagnostics/alerts?include_suppressed=true")
        with urllib.request.urlopen(req_supp, timeout=3.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] == 2
            statuses = [a["status"] for a in data["alerts"]]
            assert AuditAlertStatus.SUPPRESSED_COOLDOWN.value in statuses

        conn.close()
    finally:
        server.stop()


# ==============================================================================
# Scenario 14: Existing Dashboard Endpoints 26 and 27 Compatibility
# ==============================================================================

def test_acceptance_scenario_14_existing_dashboard_endpoints_26_and_27_compatibility():
    """Verify Endpoints 26 and 27 continue operating normally without regression."""
    store = StateStore()
    audit_svc = HistoricalReviewAuditService(state_store=store)
    rel_svc = HistoricalReviewAuditReliabilityService(state_store=store)

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        audit_service=audit_svc,
        reliability_service=rel_svc,
    )
    server.start()
    time.sleep(0.1)

    port = server.port
    base_url = f"http://127.0.0.1:{port}"

    try:
        # Endpoint 26: /api/historical-review-audit
        req26 = urllib.request.Request(f"{base_url}/api/historical-review-audit")
        with urllib.request.urlopen(req26, timeout=3.0) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True

        # Endpoint 27: /api/historical-review-reliability
        req27 = urllib.request.Request(f"{base_url}/api/historical-review-reliability")
        with urllib.request.urlopen(req27, timeout=3.0) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True

        # Method lockdown (POST -> 405 on both)
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3.0)
        conn.request("POST", "/api/historical-review-audit", body=b"{}")
        assert conn.getresponse().status == 405

        conn.request("POST", "/api/historical-review-reliability", body=b"{}")
        assert conn.getresponse().status == 405
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# Scenario 15: Malformed or Incomplete Persisted State Handling
# ==============================================================================

def test_acceptance_scenario_15_malformed_or_incomplete_persisted_state_handling():
    """Verify malformed or corrupted StateStore data is safely handled without crashing or fabricating history."""
    corrupted_store = StateStore()

    # Inject corrupted items
    corrupted_store.set(
        "historical_review_diagnostics:history",
        ["not_a_dict", None, 12345, {"missing_required_keys": True}],
    )
    corrupted_store.set(
        "historical_review_alerts:history",
        ["invalid_alert_payload", {"alert_id": "bad/id/traversal"}],
    )
    corrupted_store.set(
        "historical_review_diagnostics:cooldown_state",
        {
            "invalid_key_format": "bad_data",
            "inv_corrupt:NOT_A_CATEGORY:ERROR": {"last_timestamp": "nan", "suppressed": "bad_int"},
        },
    )
    corrupted_store.set(
        "historical_review_diagnostics:active_failures",
        {"inv_bad": ["INVALID_ENUM_STRING", 99999]},
    )
    corrupted_store.set(
        "historical_review_diagnostics:retention_stats",
        {
            "evicted_diagnostics_count": "not_an_int",
            "evicted_alerts_count": -50,
            "evicted_diagnostics_by_inv": "not_a_dict",
        },
    )

    # Initialize service with auto_hydrate=True on corrupted StateStore: MUST NOT CRASH
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=corrupted_store,
        auto_hydrate=True,
    )

    # In-memory history safely ignores corrupted items without fabrication
    assert len(svc._diagnostics) == 0
    assert len(svc._alerts) == 0
    assert len(svc._active_failures) == 0

    # Retention status falls back safely
    ret = svc.get_retention_status()
    assert ret["total_diagnostics_evicted"] == 0
    assert ret["total_alerts_evicted"] == 0
    assert ret["active_failures_tracked"] == 0

    # Clean recording continues without hindrance
    rec = svc.record_diagnostic(
        investigation_id="inv_after_corrupt",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Valid diagnostic after corrupted store load.",
    )
    assert svc.get_diagnostic(rec.diagnostic_id) is not None
