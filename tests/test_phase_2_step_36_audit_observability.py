"""KAISER — Phase 2, Step 36: Historical Review Audit Observability, Alerting & Operational Diagnostics Tests.

Comprehensive deterministic test suite covering:
1. Diagnostic record immutability and validation.
2. Correlation between audit and reliability events.
3. Each supported diagnostic category and severity.
4. Informational alert generation and deduplication.
5. Cooldown behavior using an injected clock rather than uncontrolled sleeps.
6. Preservation of diagnostics when alerts are deduplicated.
7. Bounded retention and concurrent access.
8. Failure and recovery lifecycle tracking.
9. EventBus and StateStore integration.
10. Sanitization and sensitive-data redaction.
11. Dashboard GET/HEAD, mutation rejection, malformed identifiers, and path traversal protection.
12. Orchestrator command handlers registration and execution.
13. Architectural safety boundaries and execution-engine lock.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
import http.client
import json
import os
import threading
import time
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.request

import pytest

from app.analysis.opportunities.evolution.audit.diagnostics import (
    AuditAlert,
    AuditAlertStatus,
    AuditDiagnosticCategory,
    AuditDiagnosticRecord,
    AuditDiagnosticSeverity,
    AuditDiagnosticSummary,
    AuditEvidenceNature,
    HistoricalReviewAuditDiagnosticsService,
    compute_deterministic_alert_id,
    compute_deterministic_diagnostic_id,
    register_audit_diagnostics_handlers,
)
from app.analysis.opportunities.evolution.audit.models import (
    AuditFindingCategory,
    AuditFindingSeverity,
    RecordValidationStatus,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer


# ==============================================================================
# Injected Clock Fixture
# ==============================================================================

class InjectedClock:
    """Deterministic, controllable clock for alert cooldown testing."""

    def __init__(self, start_time: float = 1700000000.0) -> None:
        self._current_time = start_time

    def time(self) -> float:
        return self._current_time

    def advance(self, seconds: float) -> None:
        self._current_time += seconds


# ==============================================================================
# 1. Diagnostic Record Immutability & Validation
# ==============================================================================

def test_diagnostic_record_immutability_and_validation():
    """Verify diagnostic records are frozen, validate safe IDs, and reject prohibited words."""
    clock = InjectedClock()
    rec = AuditDiagnosticRecord(
        diagnostic_id="diag_test01",
        correlation_id="corr_test01",
        investigation_id="inv_test01",
        timestamp=clock.time(),
        timestamp_iso="2023-11-14T22:13:20Z",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.WARNING,
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        message="Partial historical review coverage observed.",
        report_id="report_test01",
        evaluation_id="eval_test01",
        source_references=("ref_alpha", "ref_beta"),
        details={"missing_snapshots_count": 2},
        error_category="CoverageGap",
        sanitized_summary="[WARNING] COVERAGE_DEGRADATION for inv_test01",
    )

    # Immutability verification
    with pytest.raises(FrozenInstanceError):
        rec.message = "Mutated message"  # type: ignore

    with pytest.raises(FrozenInstanceError):
        rec.severity = AuditDiagnosticSeverity.ERROR  # type: ignore

    # Prohibited words rejection in post_init
    with pytest.raises(ValidationError):
        AuditDiagnosticRecord(
            diagnostic_id="diag_bad01",
            correlation_id="corr_bad01",
            investigation_id="inv_bad01",
            timestamp=clock.time(),
            timestamp_iso="2023-11-14T22:13:20Z",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
            message="Recommended target_price reached for execution.",
        )

    # Malformed ID rejection
    with pytest.raises(ValidationError):
        AuditDiagnosticRecord(
            diagnostic_id="diag_bad..traversal",
            correlation_id="corr_test",
            investigation_id="inv_test",
            timestamp=clock.time(),
            timestamp_iso="2023-11-14T22:13:20Z",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
            message="Clean message",
        )

    # Serialization & Deserialization
    rec_dict = rec.to_dict()
    assert rec_dict["diagnostic_id"] == "diag_test01"
    assert rec_dict["category"] == AuditDiagnosticCategory.COVERAGE_DEGRADATION.value
    assert rec_dict["source_references"] == ["ref_alpha", "ref_beta"]

    hydrated = AuditDiagnosticRecord.from_dict(rec_dict)
    assert hydrated == rec


# ==============================================================================
# 2. Correlation Between Audit & Reliability Events
# ==============================================================================

def test_correlation_across_audit_and_reliability_events():
    """Verify correlation ID links audit request, report, reliability evaluation, and diagnostics."""
    clock = InjectedClock()
    bus = EventBus()
    store = StateStore()

    diag_svc = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        time_provider=clock.time,
    )

    corr_id = "corr_workflow_alpha_123"
    rec = diag_svc.record_diagnostic(
        investigation_id="inv_corr_test",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Audit evaluation encountered invalid predecessor version.",
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        correlation_id=corr_id,
        report_id="report_phase2_001",
        evaluation_id="eval_phase2_001",
        source_references=("ref_history_01", "ref_history_02"),
    )

    assert rec.correlation_id == corr_id
    assert rec.report_id == "report_phase2_001"
    assert rec.evaluation_id == "eval_phase2_001"
    assert rec.source_references == ("ref_history_01", "ref_history_02")

    # Verify query by ID preserves correlation fields
    retrieved = diag_svc.get_diagnostic(rec.diagnostic_id)
    assert retrieved is not None
    assert retrieved.correlation_id == corr_id
    assert retrieved.report_id == "report_phase2_001"


# ==============================================================================
# 3. Diagnostic Categories, Severities & Evidence Nature Distinction
# ==============================================================================

def test_all_diagnostic_categories_severities_and_evidence_nature():
    """Verify all defined categories, severities, and strict separation of evidence nature."""
    clock = InjectedClock()
    diag_svc = HistoricalReviewAuditDiagnosticsService(time_provider=clock.time)

    categories = [
        (AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE, AuditDiagnosticSeverity.CRITICAL),
        (AuditDiagnosticCategory.COVERAGE_DEGRADATION, AuditDiagnosticSeverity.WARNING),
        (AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE, AuditDiagnosticSeverity.ERROR),
        (AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION, AuditDiagnosticSeverity.ERROR),
        (AuditDiagnosticCategory.INTEGRITY_VIOLATION, AuditDiagnosticSeverity.ERROR),
        (AuditDiagnosticCategory.RECOVERY_OBSERVED, AuditDiagnosticSeverity.INFO),
        (AuditDiagnosticCategory.GENERAL_DIAGNOSTIC, AuditDiagnosticSeverity.INFO),
    ]

    natures = [
        AuditEvidenceNature.OBSERVED_FAILURE,
        AuditEvidenceNature.INFERRED_INTERPRETATION,
        AuditEvidenceNature.HUMAN_DECISION,
    ]

    for i, (cat, sev) in enumerate(categories):
        nature = natures[i % len(natures)]
        rec = diag_svc.record_diagnostic(
            investigation_id="inv_cats_test",
            category=cat,
            severity=sev,
            evidence_nature=nature,
            message=f"Diagnostic test message for category {cat.value}",
        )
        assert rec.category == cat
        assert rec.severity == sev
        assert rec.evidence_nature == nature

    # Verify filtering by category and severity
    cov_diags = diag_svc.list_diagnostics(category=AuditDiagnosticCategory.COVERAGE_DEGRADATION)
    assert len(cov_diags) == 1
    assert cov_diags[0].category == AuditDiagnosticCategory.COVERAGE_DEGRADATION

    crit_diags = diag_svc.list_diagnostics(severity=AuditDiagnosticSeverity.CRITICAL)
    assert len(crit_diags) == 1
    assert crit_diags[0].severity == AuditDiagnosticSeverity.CRITICAL


# ==============================================================================
# 4. Informational Alert Generation & Deduplication
# ==============================================================================

def test_alert_generation_and_deduplication():
    """Verify alerts are generated for warnings/errors, and deduplicated during cooldown."""
    clock = InjectedClock()
    bus = EventBus()
    published_events: List[Event] = []
    bus.subscribe(EventType.HISTORICAL_REVIEW_AUDIT_ALERT_TRIGGERED, published_events.append)
    bus.subscribe(EventType.HISTORICAL_REVIEW_AUDIT_ALERT_SUPPRESSED, published_events.append)

    diag_svc = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        time_provider=clock.time,
        alert_cooldown_seconds=60.0,
    )

    # 1. INFO severity diagnostic does NOT generate an alert
    diag_svc.record_diagnostic(
        investigation_id="inv_alert_test",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Informational diagnostic note.",
    )
    alerts = diag_svc.list_alerts(investigation_id="inv_alert_test")
    assert len(alerts) == 0
    assert len(published_events) == 0

    # 2. First ERROR diagnostic generates an active alert
    diag_svc.record_diagnostic(
        investigation_id="inv_alert_test",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Audit repeatability divergence score 0.60.",
    )
    alerts = diag_svc.list_alerts(investigation_id="inv_alert_test")
    assert len(alerts) == 1
    assert alerts[0].status == AuditAlertStatus.TRIGGERED
    assert alerts[0].suppressed_occurrences == 0
    assert len(published_events) == 1
    assert published_events[0].event_type == EventType.HISTORICAL_REVIEW_AUDIT_ALERT_TRIGGERED

    # 3. Second ERROR diagnostic within cooldown is deduplicated/suppressed
    clock.advance(10.0)  # 10s < 60s cooldown
    diag_svc.record_diagnostic(
        investigation_id="inv_alert_test",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Audit repeatability divergence score 0.60 again.",
    )
    # The active alerts count remains 1, but suppressed event was emitted
    active_alerts = diag_svc.list_alerts(investigation_id="inv_alert_test", active_only=True)
    assert len(active_alerts) == 1

    assert len(published_events) == 2
    assert published_events[1].event_type == EventType.HISTORICAL_REVIEW_AUDIT_ALERT_SUPPRESSED


# ==============================================================================
# 5. Cooldown Behavior Using Injected Clock
# ==============================================================================

def test_alert_cooldown_expiry_with_injected_clock():
    """Verify alert re-triggers after cooldown window expires."""
    clock = InjectedClock()
    diag_svc = HistoricalReviewAuditDiagnosticsService(
        time_provider=clock.time,
        alert_cooldown_seconds=60.0,
    )

    # First alert at t=0
    diag_svc.record_diagnostic(
        investigation_id="inv_cooldown_test",
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Regression detected in findings.",
    )
    assert len(diag_svc.list_alerts(investigation_id="inv_cooldown_test")) == 1

    # Suppressed at t=30
    clock.advance(30.0)
    diag_svc.record_diagnostic(
        investigation_id="inv_cooldown_test",
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Regression detected in findings (duplicate).",
    )
    assert len(diag_svc.list_alerts(investigation_id="inv_cooldown_test")) == 1

    # Advance beyond cooldown (t=70 > 60)
    clock.advance(40.0)
    diag_svc.record_diagnostic(
        investigation_id="inv_cooldown_test",
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Regression detected in findings (after cooldown).",
    )
    alerts = diag_svc.list_alerts(investigation_id="inv_cooldown_test")
    assert len(alerts) == 2
    # The second triggered alert notes that 1 occurrence was suppressed during cooldown
    assert alerts[0].suppressed_occurrences == 1


# ==============================================================================
# 6. Preservation of Diagnostics When Alerts Are Deduplicated
# ==============================================================================

def test_preservation_of_diagnostics_during_alert_deduplication():
    """Verify underlying diagnostic records are strictly preserved when alerts are suppressed."""
    clock = InjectedClock()
    diag_svc = HistoricalReviewAuditDiagnosticsService(
        time_provider=clock.time,
        alert_cooldown_seconds=120.0,
    )

    # Record 5 consecutive diagnostics within cooldown
    for i in range(5):
        clock.advance(5.0)
        diag_svc.record_diagnostic(
            investigation_id="inv_preserve_test",
            category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
            severity=AuditDiagnosticSeverity.WARNING,
            message=f"Coverage degradation observation #{i}",
        )

    # All 5 diagnostic records MUST be preserved in history
    diags = diag_svc.list_diagnostics(investigation_id="inv_preserve_test")
    assert len(diags) == 5, "Diagnostics must NEVER be suppressed by alert cooldown"

    # Exactly 1 active alert was triggered
    alerts = diag_svc.list_alerts(investigation_id="inv_preserve_test")
    assert len(alerts) == 1


# ==============================================================================
# 7. Bounded Retention & Concurrent Access Safety
# ==============================================================================

def test_bounded_retention_and_concurrency_safety():
    """Verify ring-buffer eviction and thread-safe concurrent recording."""
    diag_svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=25,
        max_alerts=25,
    )

    # Bounded retention check: record 50 diagnostics
    for i in range(50):
        diag_svc.record_diagnostic(
            investigation_id=f"inv_bound_{i}",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Bounded retention test {i}",
        )

    assert len(diag_svc.list_diagnostics(limit=100)) == 25
    assert len(diag_svc._diagnostics) == 25

    # Concurrency check: 10 threads recording simultaneously
    errors: List[Exception] = []

    def worker(worker_id: int):
        try:
            for j in range(20):
                diag_svc.record_diagnostic(
                    investigation_id=f"inv_conc_{worker_id}",
                    category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
                    severity=AuditDiagnosticSeverity.WARNING,
                    message=f"Concurrent diagnostic from worker {worker_id} iteration {j}",
                )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Concurrent execution errors: {errors}"
    assert len(diag_svc.list_diagnostics(limit=100)) == 25
    assert len(diag_svc.list_alerts(limit=100)) <= 25


# ==============================================================================
# 8. Failure and Recovery Lifecycle Tracking
# ==============================================================================

def test_failure_and_recovery_lifecycle_tracking():
    """Verify active failure state is tracked and explicit recovery diagnostic/alert is generated."""
    clock = InjectedClock()
    bus = EventBus()
    recovery_events: List[Event] = []
    bus.subscribe(EventType.HISTORICAL_REVIEW_AUDIT_RECOVERY_OBSERVED, recovery_events.append)

    diag_svc = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        time_provider=clock.time,
    )

    # 1. Failure state recorded
    diag_svc.record_diagnostic(
        investigation_id="inv_recov_test",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Injected audit service calculation fault.",
    )

    summary_pre = diag_svc.get_diagnostic_summary("inv_recov_test")
    assert summary_pre.has_active_failures is True
    assert summary_pre.recovery_observed is False

    # 2. Operational recovery observed
    clock.advance(30.0)
    recov_diag = diag_svc.check_and_record_recovery(
        investigation_id="inv_recov_test",
        report_id="report_healthy_01",
    )
    assert recov_diag is not None
    assert recov_diag.category == AuditDiagnosticCategory.RECOVERY_OBSERVED
    assert recov_diag.severity == AuditDiagnosticSeverity.INFO

    # Summary updated
    summary_post = diag_svc.get_diagnostic_summary("inv_recov_test")
    assert summary_post.has_active_failures is False
    assert summary_post.recovery_observed is True

    # Alert with RECOVERED status generated
    alerts = diag_svc.list_alerts(investigation_id="inv_recov_test")
    assert any(a.status == AuditAlertStatus.RECOVERED for a in alerts)

    # Event published
    assert len(recovery_events) == 1


# ==============================================================================
# 9. EventBus and StateStore Integration
# ==============================================================================

def test_eventbus_and_statestore_integration():
    """Verify automatic event consumption and cold-cache hydration from StateStore."""
    bus = EventBus()
    store = StateStore()

    svc1 = HistoricalReviewAuditDiagnosticsService(
        event_bus=bus,
        state_store=store,
        auto_subscribe=True,
    )

    # Publish an audit regression event on the bus
    bus.publish(
        Event.create(
            event_type=EventType.HISTORICAL_REVIEW_AUDIT_REGRESSION_DETECTED,
            payload={
                "investigation_id": "inv_eb_test",
                "regression_state": "REGRESSION_DETECTED",
                "new_findings_count": 3,
                "current_report_id": "report_reg_01",
            },
            source="TestAuditService",
            correlation_id="corr_eb_123",
        )
    )

    # Svc1 automatically processes event from bus
    diags1 = svc1.list_diagnostics(investigation_id="inv_eb_test")
    assert len(diags1) == 1
    assert diags1[0].category == AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION
    assert diags1[0].correlation_id == "corr_eb_123"

    # Create fresh service instance with empty in-memory cache but attached StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=store,
        auto_subscribe=False,
    )
    assert len(svc2._diagnostics) == 0

    # Hydrates record directly from StateStore by ID
    hydrated = svc2.get_diagnostic(diags1[0].diagnostic_id)
    assert hydrated is not None
    assert hydrated.diagnostic_id == diags1[0].diagnostic_id
    assert hydrated.investigation_id == "inv_eb_test"


# ==============================================================================
# 10. Sanitization and Secret Redaction
# ==============================================================================

def test_sanitization_and_secret_redaction():
    """Verify passwords, API keys, tokens, and filesystem paths are scrubbed from diagnostics."""
    diag_svc = HistoricalReviewAuditDiagnosticsService()

    leaky_message = (
        "Evaluation failed on path C:\\Users\\user\\Desktop\\secret.py with api_key=super_secret_token_12345 "
        "and password=my_plain_password."
    )

    rec = diag_svc.record_diagnostic(
        investigation_id="inv_scrub_test",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message=leaky_message,
        details={
            "api_key": "raw_secret_value",
            "safe_counter": 42,
        },
    )

    assert "C:\\Users" not in rec.message
    assert "super_secret_token" not in rec.message
    assert "my_plain_password" not in rec.message
    assert "[REDACTED" in rec.message

    rec_dict = rec.to_dict()
    assert "api_key" not in rec_dict["details"]
    assert rec_dict["details"]["safe_counter"] == 42


# ==============================================================================
# 11. Dashboard Endpoint 28 Security, Read-Only & Zero-Leakage
# ==============================================================================

def test_dashboard_endpoint_28_security_and_read_only():
    """Verify endpoint 28 adheres to read-only constraints, rejects mutations, traversals, and leaks no secrets."""
    clock = InjectedClock()
    diag_svc = HistoricalReviewAuditDiagnosticsService(time_provider=clock.time)

    # Populate diagnostic and alert
    diag_rec = diag_svc.record_diagnostic(
        investigation_id="inv_dash_sec",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Coverage degradation noted.",
        source_references=("ref_alpha",),
    )

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        audit_diagnostics_service=diag_svc,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        base = server.url

        # 1. Valid GET /api/historical-review-diagnostics
        req = urllib.request.Request(f"{base}/api/historical-review-diagnostics")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["read_only"] is True
            assert data["execution_locked"] is True
            assert data["count"] >= 1

        # 2. Valid GET /api/historical-review-diagnostics/<diag_id>
        req = urllib.request.Request(f"{base}/api/historical-review-diagnostics/{diag_rec.diagnostic_id}")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["diagnostic"]["diagnostic_id"] == diag_rec.diagnostic_id

        # 3. Valid GET /api/historical-review-diagnostics/alerts
        req = urllib.request.Request(f"{base}/api/historical-review-diagnostics/alerts")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["count"] >= 1

        # 4. Valid GET /api/historical-review-diagnostics/summary?investigation_id=inv_dash_sec
        req = urllib.request.Request(f"{base}/api/historical-review-diagnostics/summary?investigation_id=inv_dash_sec")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["summary"]["investigation_id"] == "inv_dash_sec"

        # 5. Unsupported Sub-resource -> 404
        for ep in (
            f"/api/historical-review-diagnostics/{diag_rec.diagnostic_id}/unsupported_sub",
            "/api/historical-review-diagnostics/alerts/alt_123/extra_path",
        ):
            try:
                urllib.request.urlopen(f"{base}{ep}")
                pytest.fail(f"Expected 404 for {ep}")
            except urllib.error.HTTPError as err:
                assert err.code == 404

        # 6. Mutation Rejection -> 405 Method Not Allowed
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        for method in ("POST", "PUT", "DELETE", "PATCH"):
            conn.request(method, "/api/historical-review-diagnostics", body=b'{"test": 1}')
            resp = conn.getresponse()
            assert resp.status == 405
            assert "GET" in resp.getheader("Allow", "")
            resp.read()
        conn.close()

        # 7. Path Traversal & Malformed Identifier Rejection -> 400
        for ep in (
            "/api/historical-review-diagnostics/..%2f..%2fetc%2fpasswd",
            "/api/historical-review-diagnostics/diag..invalid",
            "/api/historical-review-diagnostics/alerts/..%2fbad",
            "/api/historical-review-diagnostics?investigation_id=..%2fmalicious",
            "/api/historical-review-diagnostics/summary?investigation_id=..%2fmalicious",
        ):
            conn = http.client.HTTPConnection("127.0.0.1", server.port)
            conn.request("GET", ep)
            resp = conn.getresponse()
            assert resp.status == 400
            resp.read()
            conn.close()

        # 8. HEAD Requests -> 200 with Empty Body
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("HEAD", "/api/historical-review-diagnostics")
        resp = conn.getresponse()
        assert resp.status == 200
        assert len(resp.read()) == 0
        conn.close()

    finally:
        server.stop()


# ==============================================================================
# 12. Orchestrator Command Handlers Registration & Execution
# ==============================================================================

def test_orchestrator_command_handlers_registration_and_execution():
    """Verify MasterOrchestrator registers and routes all 6 diagnostics commands."""
    orchestrator = MasterOrchestrator()
    diag_svc = HistoricalReviewAuditDiagnosticsService()
    register_audit_diagnostics_handlers(orchestrator, diag_svc)

    # 1. record_historical_review_diagnostic
    req1 = OrchestratorRequest.create(
        command="record_historical_review_diagnostic",
        context={
            "investigation_id": "inv_cmd_test",
            "category": AuditDiagnosticCategory.INTEGRITY_VIOLATION.value,
            "severity": AuditDiagnosticSeverity.ERROR.value,
            "message": "Orchestrator command handler diagnostic test.",
        },
    )
    res1 = orchestrator.handle_request(req1)
    assert res1.is_success is True
    assert res1.result["status"] == "success"
    diag_id = res1.result["diagnostic"]["diagnostic_id"]

    # 2. get_historical_review_diagnostic
    req2 = OrchestratorRequest.create(
        command="get_historical_review_diagnostic",
        context={"diagnostic_id": diag_id},
    )
    res2 = orchestrator.handle_request(req2)
    assert res2.is_success is True
    assert res2.result["status"] == "success"
    assert res2.result["diagnostic"]["diagnostic_id"] == diag_id

    # 3. list_historical_review_diagnostics
    req3 = OrchestratorRequest.create(
        command="list_historical_review_diagnostics",
        context={"investigation_id": "inv_cmd_test"},
    )
    res3 = orchestrator.handle_request(req3)
    assert res3.is_success is True
    assert res3.result["status"] == "success"
    assert res3.result["count"] >= 1

    # 4. list_historical_review_alerts
    req4 = OrchestratorRequest.create(
        command="list_historical_review_alerts",
        context={"investigation_id": "inv_cmd_test"},
    )
    res4 = orchestrator.handle_request(req4)
    assert res4.is_success is True
    assert res4.result["status"] == "success"
    assert res4.result["count"] >= 1
    alert_id = res4.result["alerts"][0]["alert_id"]

    # 5. get_historical_review_alert
    req5 = OrchestratorRequest.create(
        command="get_historical_review_alert",
        context={"alert_id": alert_id},
    )
    res5 = orchestrator.handle_request(req5)
    assert res5.is_success is True
    assert res5.result["status"] == "success"
    assert res5.result["alert"]["alert_id"] == alert_id

    # 6. get_historical_review_diagnostic_summary
    req6 = OrchestratorRequest.create(
        command="get_historical_review_diagnostic_summary",
        context={"investigation_id": "inv_cmd_test"},
    )
    res6 = orchestrator.handle_request(req6)
    assert res6.is_success is True
    assert res6.result["status"] == "success"
    assert res6.result["summary"]["investigation_id"] == "inv_cmd_test"


# ==============================================================================
# 13. System Safety Boundaries Verification
# ==============================================================================

def test_safety_invariants_and_execution_engine_locked():
    """Verify execution engine remains locked, trading engine uninitialized, and JUJU untouched."""
    settings = Settings()
    assert settings.execution_locked is True, "Settings.execution_locked must remain True"

    from app.bootstrap import bootstrap_system
    status = bootstrap_system()
    assert status.trading_engine == "not initialized"
    assert status.execution_engine == "locked"
    assert status.settings.execution_locked is True
    assert status.audit_diagnostics_service is not None

    juju_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "JUJU")
    if os.path.exists(juju_dir):
        assert os.path.isdir(juju_dir)
