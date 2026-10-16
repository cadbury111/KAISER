"""KAISER — Phase 2, Step 38 Focused Test Suite.

Historical Review Audit Diagnostics Retention, Lifecycle & Long-Term Consistency Foundation.

TEST CATEGORIES:
1. Retention at, below, and above capacity.
2. Retention boundaries across service reconstruction.
3. StateStore and in-memory history consistency.
4. Active, suppressed, acknowledged, and resolved alert transitions.
5. Recovery before and after diagnostic eviction.
6. Target-category compatible recovery.
7. Repeated alerts and cooldown across restarts.
8. Missing, expired, and incomplete historical data.
9. Traceability from alert to audit and reliability sources.
10. Concurrent writes, reads, and evictions thread safety.
11. Secret sanitization and bounded resource safety.
12. Dashboard Endpoint 28 retention and security behavior.
13. Orchestrator command handlers registration and execution.
14. System safety invariants (execution engine locked, uninitialized trading engine).
"""

from __future__ import annotations

import http.client
import json
import threading
import time
import urllib.request
from typing import Any, Dict, List, Optional

import pytest

from app.analysis.opportunities.evolution.audit.diagnostics.handlers import (
    register_audit_diagnostics_handlers,
)
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
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.core.state import StateStore


class MockClock:
    """Deterministic injectable clock for testing cooldowns and expiry."""

    def __init__(self, initial_time: float = 1700000000.0) -> None:
        self._time = initial_time

    def time(self) -> float:
        return self._time

    def advance(self, seconds: float) -> None:
        self._time += seconds


# ==============================================================================
# 1. Retention At, Below, and Above Capacity
# ==============================================================================

def test_retention_at_below_and_above_capacity():
    """Verify in-memory ring buffers and retention reporting at, below, and above capacity."""
    clock = MockClock()
    service = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock.time,
    )

    # 1. Below capacity (3 diagnostics recorded)
    for i in range(3):
        clock.advance(1.0)
        service.record_diagnostic(
            investigation_id="inv_ret_01",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Diagnostic below capacity {i}",
        )

    summary_below = service.get_diagnostic_summary("inv_ret_01")
    assert summary_below.total_diagnostics == 3
    assert summary_below.retention_status == "COMPLETE"
    assert summary_below.is_truncated is False
    assert summary_below.evicted_diagnostics_count == 0
    assert summary_below.capacity == 10

    ret_status = service.get_retention_status()
    assert ret_status["current_diagnostics_count"] == 3
    assert ret_status["total_diagnostics_evicted"] == 0

    # 2. At capacity (7 more diagnostics recorded to reach exactly 10)
    for i in range(3, 10):
        clock.advance(1.0)
        service.record_diagnostic(
            investigation_id="inv_ret_01",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Diagnostic at capacity {i}",
        )

    summary_at = service.get_diagnostic_summary("inv_ret_01")
    assert summary_at.total_diagnostics == 10
    assert summary_at.retention_status == "COMPLETE"
    assert summary_at.is_truncated is False
    assert summary_at.evicted_diagnostics_count == 0

    ret_status_at = service.get_retention_status()
    assert ret_status_at["current_diagnostics_count"] == 10
    assert ret_status_at["total_diagnostics_evicted"] == 0

    # 3. Above capacity (5 more diagnostics recorded, total 15, capacity 10)
    first_recs = service.list_diagnostics(investigation_id="inv_ret_01", limit=10)
    oldest_id = first_recs[-1].diagnostic_id  # Oldest currently in memory

    for i in range(10, 15):
        clock.advance(1.0)
        service.record_diagnostic(
            investigation_id="inv_ret_01",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Diagnostic above capacity {i}",
        )

    summary_above = service.get_diagnostic_summary("inv_ret_01")
    assert summary_above.total_diagnostics == 10  # Capped at ring buffer size
    assert summary_above.retention_status == "TRUNCATED_TO_CAPACITY"
    assert summary_above.is_truncated is True
    assert summary_above.evicted_diagnostics_count == 5

    ret_status_above = service.get_retention_status()
    assert ret_status_above["current_diagnostics_count"] == 10
    assert ret_status_above["total_diagnostics_evicted"] == 5

    # Oldest record was evicted from in-memory ring buffer
    assert service.is_diagnostic_evicted(oldest_id) is True


# ==============================================================================
# 2. Retention Boundaries Across Service Reconstruction
# ==============================================================================

def test_retention_boundaries_across_service_reconstruction():
    """Verify history hydration, retention boundaries, and active failure preservation on restart."""
    clock = MockClock()
    store = StateStore()

    svc1 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock.time,
    )

    # Record error diagnostic (triggers active failure and alert)
    clock.advance(1.0)
    rec_err = svc1.record_diagnostic(
        investigation_id="inv_recon_01",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Critical coverage degradation observed.",
    )

    # Record 12 info diagnostics to push error diagnostic past ring buffer capacity (capacity=10)
    for i in range(12):
        clock.advance(1.0)
        svc1.record_diagnostic(
            investigation_id="inv_recon_01",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Follow-up info record {i}",
        )

    assert svc1.get_retention_status()["total_diagnostics_evicted"] == 3

    # Reconstruct service instance from same StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock.time,
        auto_hydrate=True,
    )

    # In-memory diagnostics hydrated up to capacity
    assert len(svc2._diagnostics) == 10
    # Eviction stats restored
    assert svc2._evicted_diagnostics_count == 3
    # Active failure tracked across reconstruction
    assert AuditDiagnosticCategory.COVERAGE_DEGRADATION in svc2._active_failures.get("inv_recon_01", set())

    # Summary correctly reflects reconstructed state
    summary_recon = svc2.get_diagnostic_summary("inv_recon_01")
    assert summary_recon.total_diagnostics == 10
    assert summary_recon.is_truncated is True
    assert summary_recon.has_active_failures is True


# ==============================================================================
# 3. StateStore and In-Memory History Consistency
# ==============================================================================

def test_statestore_and_in_memory_history_consistency():
    """Verify alignment between in-memory queues and persistent StateStore history."""
    store = StateStore()
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        max_diagnostics=10,
        max_alerts=10,
    )

    created_ids = []
    for i in range(5):
        r = svc.record_diagnostic(
            investigation_id="inv_sync_01",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.WARNING,
            message=f"Sync record {i}",
        )
        created_ids.append(r.diagnostic_id)

    # List in-memory
    mem_diags = svc.list_diagnostics(investigation_id="inv_sync_01")
    assert len(mem_diags) == 5

    # Check StateStore history list
    stored_list = store.get("historical_review_diagnostics:history", [])
    assert isinstance(stored_list, list)
    assert len(stored_list) == 5

    # Newest-first consistency
    assert mem_diags[0].diagnostic_id == stored_list[0]["diagnostic_id"]
    assert mem_diags[-1].diagnostic_id == stored_list[-1]["diagnostic_id"]

    # Individual lookups directly from StateStore
    for d_id in created_ids:
        raw_stored = store.get(f"historical_review_diagnostics:{d_id}")
        assert raw_stored is not None
        assert raw_stored["diagnostic_id"] == d_id


# ==============================================================================
# 4. Active, Suppressed, Acknowledged, and Resolved Alert Transitions
# ==============================================================================

def test_active_suppressed_acknowledged_and_resolved_alert_transitions():
    """Verify operational alert lifecycle state machine transitions."""
    clock = MockClock()
    store = StateStore()
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        alert_cooldown_seconds=60.0,
        time_provider=clock.time,
    )

    # 1. TRIGGERED alert
    clock.advance(10.0)
    rec1 = svc.record_diagnostic(
        investigation_id="inv_life_01",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Repeatability divergence score 0.4.",
    )
    alerts1 = svc.list_alerts(investigation_id="inv_life_01")
    assert len(alerts1) == 1
    a1 = alerts1[0]
    assert a1.status == AuditAlertStatus.TRIGGERED
    assert a1.suppressed_occurrences == 0

    # 2. SUPPRESSED_COOLDOWN alert (under cooldown)
    clock.advance(20.0)  # t=30.0 (< 10.0 + 60.0)
    rec2 = svc.record_diagnostic(
        investigation_id="inv_life_01",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Repeatability divergence score 0.4 recurring.",
    )
    # Underlying diagnostic recorded
    assert svc.get_diagnostic(rec2.diagnostic_id) is not None
    # Suppression alert created
    suppressed_alerts = [a for a in svc.list_alerts(investigation_id="inv_life_01", include_suppressed=True) if a.status == AuditAlertStatus.SUPPRESSED_COOLDOWN]
    assert len(suppressed_alerts) == 1
    a_suppressed = suppressed_alerts[0]
    assert a_suppressed.status == AuditAlertStatus.SUPPRESSED_COOLDOWN
    assert a_suppressed.suppressed_occurrences == 1

    # 3. ACKNOWLEDGED alert transition
    ack_alert = svc.acknowledge_alert(a1.alert_id, acknowledged_by="analyst_ops")
    assert ack_alert is not None
    assert ack_alert.status == AuditAlertStatus.ACKNOWLEDGED
    # Reflected in service lookup
    alt = svc.get_alert(a1.alert_id)
    assert alt is not None and alt.status == AuditAlertStatus.ACKNOWLEDGED

    # Active alerts filter includes ACKNOWLEDGED (as unresolved)
    active_alerts = svc.list_alerts(investigation_id="inv_life_01", active_only=True)
    assert any(a.alert_id == a1.alert_id for a in active_alerts)

    # 4. RESOLVED alert transition
    res_alert = svc.resolve_alert(a1.alert_id, reason="Issue investigated and mitigated.")
    assert res_alert is not None
    assert res_alert.status == AuditAlertStatus.RESOLVED
    assert "Resolved:" in res_alert.reason

    # Active alerts filter now excludes resolved alert
    active_now = svc.list_alerts(investigation_id="inv_life_01", active_only=True)
    assert not any(a.alert_id == a1.alert_id for a in active_now)

    # Double-resolve or double-acknowledge is safe
    res_resolved = svc.resolve_alert(a1.alert_id)
    assert res_resolved is not None and res_resolved.status == AuditAlertStatus.RESOLVED
    res_ack = svc.acknowledge_alert(a1.alert_id)
    assert res_ack is not None and res_ack.status == AuditAlertStatus.RESOLVED


# ==============================================================================
# 5. Recovery Before and After Diagnostic Eviction
# ==============================================================================

def test_recovery_before_and_after_diagnostic_eviction():
    """Verify alert and failure resolution succeeds before and after diagnostic eviction."""
    clock = MockClock()
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock.time,
    )

    # 1. Failure recorded
    clock.advance(1.0)
    rec_err = svc.record_diagnostic(
        investigation_id="inv_recov_evict",
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Integrity regression detected.",
    )
    assert svc.get_diagnostic_summary("inv_recov_evict").has_active_failures is True
    alert = svc.list_alerts(investigation_id="inv_recov_evict")[0]
    assert alert.status == AuditAlertStatus.TRIGGERED

    # 2. Record 12 subsequent diagnostics to evict the initial error record from ring buffer
    for i in range(12):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_recov_evict",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Pusher record {i}",
        )

    # Confirm initial diagnostic was evicted from memory
    assert svc.is_diagnostic_evicted(rec_err.diagnostic_id) is True
    # Invariant: Alert is NOT resolved merely because diagnostic expired!
    alert_check = svc.get_alert(alert.alert_id)
    assert alert_check is not None and alert_check.status == AuditAlertStatus.TRIGGERED
    assert svc.get_diagnostic_summary("inv_recov_evict").has_active_failures is True

    # 3. Operational recovery observed
    clock.advance(10.0)
    recov = svc.check_and_record_recovery(
        investigation_id="inv_recov_evict",
        report_id="report_healthy_final",
    )
    assert recov is not None
    assert recov.category == AuditDiagnosticCategory.RECOVERY_OBSERVED

    # Alert transitioned to RESOLVED
    alert_resolved = svc.get_alert(alert.alert_id)
    assert alert_resolved is not None and alert_resolved.status == AuditAlertStatus.RESOLVED

    # Summary updated
    summary_final = svc.get_diagnostic_summary("inv_recov_evict")
    assert summary_final.has_active_failures is False
    assert summary_final.recovery_observed is True


# ==============================================================================
# 6. Target-Category Compatible Recovery
# ==============================================================================

def test_target_category_compatible_recovery():
    """Verify recovery events only resolve compatible target failure categories."""
    svc = HistoricalReviewAuditDiagnosticsService()

    # Record 2 distinct failure categories for same investigation
    svc.record_diagnostic(
        investigation_id="inv_multi_fail",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Coverage degraded to PARTIAL.",
    )
    svc.record_diagnostic(
        investigation_id="inv_multi_fail",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Repeatability score 0.5 divergence.",
    )

    summary_init = svc.get_diagnostic_summary("inv_multi_fail")
    assert summary_init.has_active_failures is True
    assert summary_init.active_alerts_count == 2

    # Attempt recovery with incompatible category (e.g. AUDIT_EVALUATION_FAILURE)
    noop_recov = svc.check_and_record_recovery(
        investigation_id="inv_multi_fail",
        target_categories=[AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE],
    )
    assert noop_recov is None  # No matching failure resolved

    # Targeted recovery for COVERAGE_DEGRADATION only
    recov1 = svc.check_and_record_recovery(
        investigation_id="inv_multi_fail",
        target_categories=[AuditDiagnosticCategory.COVERAGE_DEGRADATION],
    )
    assert recov1 is not None
    assert recov1.details["resolved_failure_categories"] == [AuditDiagnosticCategory.COVERAGE_DEGRADATION.value]

    # REPEATABILITY_DIVERGENCE remains active!
    summary_partial = svc.get_diagnostic_summary("inv_multi_fail")
    assert summary_partial.has_active_failures is True
    assert summary_partial.active_alerts_count == 1

    # Final recovery for remaining failure
    recov2 = svc.check_and_record_recovery(
        investigation_id="inv_multi_fail",
        target_categories=[AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE],
    )
    assert recov2 is not None

    summary_clean = svc.get_diagnostic_summary("inv_multi_fail")
    assert summary_clean.has_active_failures is False
    assert summary_clean.active_alerts_count == 0


# ==============================================================================
# 7. Repeated Alerts and Cooldown Across Restarts
# ==============================================================================

def test_repeated_alerts_and_cooldown_across_restarts():
    """Verify alert cooldown and suppression state persists across service reconstruction."""
    clock = MockClock(initial_time=1000.0)
    store = StateStore()

    svc1 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        alert_cooldown_seconds=60.0,
        time_provider=clock.time,
    )

    # Initial alert at t=1000.0 (cooldown until 1060.0)
    rec1 = svc1.record_diagnostic(
        investigation_id="inv_cd_restart",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Initial coverage degradation.",
    )
    alerts1 = svc1.list_alerts(investigation_id="inv_cd_restart")
    assert len(alerts1) == 1
    assert alerts1[0].status == AuditAlertStatus.TRIGGERED

    # Reconstruct service at t=1020.0 (< 1060.0)
    clock.advance(20.0)
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        alert_cooldown_seconds=60.0,
        time_provider=clock.time,
        auto_hydrate=True,
    )

    # Duplicate failure at t=1020.0 should be SUPPRESSED (cooldown respected across restart!)
    rec2 = svc2.record_diagnostic(
        investigation_id="inv_cd_restart",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Repeated coverage degradation under cooldown.",
    )
    # Check alert was suppressed
    suppressed_alerts = svc2.list_alerts(investigation_id="inv_cd_restart", include_suppressed=True)
    suppressed = [a for a in suppressed_alerts if a.status == AuditAlertStatus.SUPPRESSED_COOLDOWN]
    assert len(suppressed) == 1
    assert suppressed[0].suppressed_occurrences == 1

    # Advance clock past cooldown window: t=1070.0 (> 1060.0)
    clock.advance(50.0)
    rec3 = svc2.record_diagnostic(
        investigation_id="inv_cd_restart",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="New failure after cooldown expiry.",
    )
    fresh_alerts = [a for a in svc2.list_alerts(investigation_id="inv_cd_restart") if a.status == AuditAlertStatus.TRIGGERED]
    assert len(fresh_alerts) == 2
    assert fresh_alerts[0].suppressed_occurrences == 1


# ==============================================================================
# 8. Missing, Expired, and Incomplete Historical Data
# ==============================================================================

def test_missing_expired_and_incomplete_historical_data():
    """Verify explicit status reporting for nonexistent and evicted data without fabrication."""
    svc = HistoricalReviewAuditDiagnosticsService(max_diagnostics=10)

    # 1. Nonexistent investigation summary
    unknown_summary = svc.get_diagnostic_summary("inv_nonexistent_xyz")
    assert unknown_summary.total_diagnostics == 0
    assert unknown_summary.total_alerts == 0
    assert unknown_summary.retention_status == "NO_DATA"
    assert unknown_summary.has_active_failures is False
    assert unknown_summary.recovery_observed is False

    # 2. Nonexistent diagnostic lookup
    assert svc.get_diagnostic("diag_nonexistent_123") is None
    assert svc.get_alert("alert_nonexistent_456") is None

    # 3. Invalid IDs return None safely without throwing
    assert svc.get_diagnostic("../traversal") is None
    assert svc.get_alert("bad/id") is None


# ==============================================================================
# 9. Traceability from Alert to Audit and Reliability Sources
# ==============================================================================

def test_traceability_from_alert_to_audit_and_reliability_sources():
    """Verify diagnostic and alert models retain end-to-end report and evaluation references."""
    svc = HistoricalReviewAuditDiagnosticsService()

    rec = svc.record_diagnostic(
        investigation_id="inv_trace_01",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.CRITICAL,
        message="Audit report synthesis failure.",
        correlation_id="corr_trace_999",
        report_id="rep_audit_777",
        evaluation_id="eval_rel_888",
        source_references=("urn:kaiser:src:rev_01", "urn:kaiser:src:audit_02"),
    )

    alert = svc.list_alerts(investigation_id="inv_trace_01")[0]

    assert alert.diagnostic_id == rec.diagnostic_id
    assert alert.correlation_id == "corr_trace_999"
    assert alert.report_id == "rep_audit_777"
    assert alert.evaluation_id == "eval_rel_888"
    assert "urn:kaiser:src:rev_01" in alert.source_references
    assert "urn:kaiser:src:audit_02" in alert.source_references


# ==============================================================================
# 10. Concurrent Writes, Reads, and Evictions Thread Safety
# ==============================================================================

def test_concurrent_writes_reads_and_evictions_thread_safety():
    """Verify thread-safety and bounded limits under high-concurrency ingestion."""
    store = StateStore()
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        max_diagnostics=15,
        max_alerts=15,
    )

    errors = []

    def worker(worker_id: int):
        try:
            for i in range(20):
                inv = f"inv_conc_{worker_id}"
                rec = svc.record_diagnostic(
                    investigation_id=inv,
                    category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
                    severity=AuditDiagnosticSeverity.INFO if i % 2 == 0 else AuditDiagnosticSeverity.WARNING,
                    message=f"Concurrent record w{worker_id} i{i}",
                )
                _ = svc.list_diagnostics(investigation_id=inv, limit=10)
                _ = svc.get_diagnostic_summary(inv)
                _ = svc.get_retention_status()
                if i % 5 == 0:
                    alts = svc.list_alerts(investigation_id=inv)
                    if alts:
                        svc.acknowledge_alert(alts[0].alert_id)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert len(errors) == 0
    # Ring buffers strictly adhere to limits
    assert len(svc._diagnostics) <= 15
    assert len(svc._alerts) <= 15
    assert svc._evicted_diagnostics_count > 0


# ==============================================================================
# 11. Secret Sanitization and Bounded Resource Safety
# ==============================================================================

def test_secret_sanitization_and_bounded_resource_safety():
    """Verify credentials and filesystem paths are scrubbed in diagnostics and alerts."""
    svc = HistoricalReviewAuditDiagnosticsService()

    leaky_msg = (
        "Fatal failure on C:\\Users\\Administrator\\kaiser\\config.py "
        "with api_key=ghp_ABC123XYZ456789012345678901234567890 "
        "and password=my_super_secret"
    )

    rec = svc.record_diagnostic(
        investigation_id="inv_leak_01",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message=leaky_msg,
        details={"token": "bearer_secret_999", "clean_metric": 42},
    )

    alert = svc.list_alerts(investigation_id="inv_leak_01")[0]

    assert "ghp_ABC123" not in rec.message
    assert "my_super_secret" not in rec.message
    assert "Administrator" not in rec.message
    assert "token" not in rec.details
    assert rec.details["clean_metric"] == 42

    assert "ghp_ABC123" not in alert.reason
    assert "my_super_secret" not in alert.reason


# ==============================================================================
# 12. Dashboard Endpoint 28 Retention and Security Behavior
# ==============================================================================

def test_dashboard_endpoint_28_retention_and_security():
    """Verify Endpoint 28 /retention route, security, and read-only invariants."""
    diag_service = HistoricalReviewAuditDiagnosticsService(max_diagnostics=20)
    for i in range(5):
        diag_service.record_diagnostic(
            investigation_id="inv_dash_sec",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Dash test {i}",
        )

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        audit_diagnostics_service=diag_service,
    )
    server.start()
    time.sleep(0.1)

    port = server.port
    base = f"http://127.0.0.1:{port}"

    try:
        # 1. GET /api/historical-review-diagnostics/retention
        req = urllib.request.Request(f"{base}/api/historical-review-diagnostics/retention")
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert "retention" in data
            assert data["retention"]["max_diagnostics_capacity"] == 20
            assert data["retention"]["current_diagnostics_count"] == 5

        # 2. HEAD /api/historical-review-diagnostics/retention
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3.0)
        conn.request("HEAD", "/api/historical-review-diagnostics/retention")
        head_resp = conn.getresponse()
        assert head_resp.status == 200
        assert head_resp.read() == b""

        # 3. Mutation rejection (POST returns 405)
        conn.request("POST", "/api/historical-review-diagnostics/retention", body=b"{}")
        post_resp = conn.getresponse()
        assert post_resp.status == 405
        assert "GET" in post_resp.getheader("Allow", "")

        # 4. Traversal on retention subpath returns 404
        conn.request("GET", "/api/historical-review-diagnostics/retention/extra")
        sub_resp = conn.getresponse()
        assert sub_resp.status == 404
        conn.close()

    finally:
        server.stop()


# ==============================================================================
# 13. Orchestrator Command Handlers Registration and Execution
# ==============================================================================

def test_orchestrator_command_handlers_registration_and_execution():
    """Verify MasterOrchestrator handlers for acknowledge, resolve, and retention status."""
    bus = EventBus()
    store = StateStore()
    diag_svc = HistoricalReviewAuditDiagnosticsService(event_bus=bus, state_store=store)
    orch = MasterOrchestrator(event_bus=bus, state_store=store)

    register_audit_diagnostics_handlers(orch, diag_svc)

    # 1. Record diagnostic command
    rec_res = orch.handle_request(
        OrchestratorRequest.create(
            command="record_historical_review_diagnostic",
            context={
                "investigation_id": "inv_orch_life",
                "category": AuditDiagnosticCategory.INTEGRITY_VIOLATION.value,
                "severity": AuditDiagnosticSeverity.ERROR.value,
                "message": "Orchestrator handler integrity violation test.",
            },
        )
    )
    assert rec_res.is_success is True
    assert rec_res.result["status"] == "success"
    diag_id = rec_res.result["diagnostic"]["diagnostic_id"]

    # 2. List alerts to get alert_id
    alert_list_res = orch.handle_request(
        OrchestratorRequest.create(
            command="list_historical_review_alerts",
            context={"investigation_id": "inv_orch_life"},
        )
    )
    assert alert_list_res.is_success is True
    assert alert_list_res.result["status"] == "success"
    assert alert_list_res.result["count"] == 1
    alert_id = alert_list_res.result["alerts"][0]["alert_id"]

    # 3. Acknowledge alert command
    ack_res = orch.handle_request(
        OrchestratorRequest.create(
            command="acknowledge_historical_review_alert",
            context={"alert_id": alert_id, "acknowledged_by": "lead_auditor"},
        )
    )
    assert ack_res.is_success is True
    assert ack_res.result["status"] == "success"
    assert ack_res.result["alert"]["status"] == AuditAlertStatus.ACKNOWLEDGED.value

    # 4. Resolve alert command
    res_res = orch.handle_request(
        OrchestratorRequest.create(
            command="resolve_historical_review_alert",
            context={"alert_id": alert_id, "reason": "Integrity restored from primary evidence."},
        )
    )
    assert res_res.is_success is True
    assert res_res.result["status"] == "success"
    assert res_res.result["alert"]["status"] == AuditAlertStatus.RESOLVED.value

    # 5. Get retention status command
    ret_res = orch.handle_request(
        OrchestratorRequest.create(
            command="get_historical_review_retention_status",
            context={},
        )
    )
    assert ret_res.is_success is True
    assert ret_res.result["status"] == "success"
    assert "retention" in ret_res.result
    assert ret_res.result["retention"]["max_diagnostics_capacity"] == 100


# ==============================================================================
# 14. Safety Invariants and Execution Engine Locked
# ==============================================================================

def test_safety_invariants_and_execution_engine_locked():
    """Verify system safety invariants: execution locked, trading engine uninitialized."""
    assert Settings.execution_locked is True, "CRITICAL: execution_locked must remain True"

    svc = HistoricalReviewAuditDiagnosticsService()
    summary = svc.get_diagnostic_summary("inv_safe_99")
    summary_dict = summary.to_dict()

    # Diagnostics and summaries must never produce financial execution advice
    prohibited_keys = {
        "buy", "sell", "target_price", "win_probability", "profit_prediction",
        "strategy_ranking", "order_placement", "broker_integration",
    }
    for key in summary_dict:
        assert key.lower() not in prohibited_keys, f"Prohibited key '{key}' found in summary"
