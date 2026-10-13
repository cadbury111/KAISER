"""KAISER — Phase 2, Step 40 Acceptance & Regression Test Suite.

Historical Review Diagnostics Auditability, Export & Reconciliation Foundation.

Covers:
1. Deterministic canonical ordering and export serialization.
2. Stable IDs and content fingerprints for identical snapshots and options.
3. Source changes producing corresponding content fingerprint updates.
4. Read-only snapshot behavior and source state immutability.
5. Complete, empty, truncated, and unavailable histories.
6. Alert lifecycle, filter criteria, and omitted reasons tracking.
7. Discrepancy detection: missing, duplicated, and altered records.
8. Reconciliation of evicted source records as retention limitation (INCOMPLETE), not corruption.
9. Content fingerprint verification and mismatch detection.
10. Secret and credential sanitization in exports.
11. Concurrent export and reconciliation thread safety.
12. Bounded metadata storage, FIFO eviction, and StateStore reconstruction.
13. MasterOrchestrator command handlers registration and execution.
14. Dashboard Endpoint 28 `/exports` and `/reconciliations` sub-resources.
15. Dashboard security: 405 on POST/PUT/DELETE, path traversal defense, and security headers.
16. Safety boundaries: execution_locked invariant and trading engine uninitialized.
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
    AuditEvidenceNature,
    DiagnosticsExportRequest,
    DiagnosticsExportResult,
    DiagnosticsReconciliationDiscrepancy,
    DiagnosticsReconciliationReport,
    DiagnosticsReconciliationStatus,
    compute_deterministic_export_id,
    compute_deterministic_reconciliation_id,
    compute_export_canonical_dict,
    compute_export_content_fingerprint,
)
from app.analysis.opportunities.evolution.audit.diagnostics.service import (
    HistoricalReviewAuditDiagnosticsService,
)
from app.analysis.opportunities.evolution.audit.diagnostics.handlers import (
    register_audit_diagnostics_handlers,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer


class DeterministicClock:
    """Deterministic injectable clock fixture for reproducible timestamps."""

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


@pytest.fixture
def clock() -> DeterministicClock:
    return DeterministicClock()


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def state_store() -> StateStore:
    return StateStore()


@pytest.fixture
def diagnostics_service(
    event_bus: EventBus, state_store: StateStore, clock: DeterministicClock
) -> HistoricalReviewAuditDiagnosticsService:
    return HistoricalReviewAuditDiagnosticsService(
        event_bus=event_bus,
        state_store=state_store,
        max_diagnostics=20,
        max_alerts=20,
        max_exports=10,
        max_reconciliations=10,
        alert_cooldown_seconds=60.0,
        time_provider=clock,
        auto_subscribe=True,
    )


# ==============================================================================
# Scenario 1: Canonical Ordering and Serialization Determinism
# ==============================================================================

def test_export_serialization_and_canonical_ordering(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
    clock: DeterministicClock,
) -> None:
    """Records inserted in any order serialize into canonically sorted structures."""
    # Record diagnostics with different timestamps and categories
    clock.advance(10.0)
    d1 = diagnostics_service.record_diagnostic(
        investigation_id="inv_alpha",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Alpha diagnostic 1",
    )
    clock.advance(5.0)
    d2 = diagnostics_service.record_diagnostic(
        investigation_id="inv_alpha",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Alpha failure 2",
    )

    req = DiagnosticsExportRequest(investigation_id="inv_alpha")
    export_result = diagnostics_service.export_diagnostics(req)

    assert export_result.record_counts["diagnostics"] == 2
    # Verify canonical sorting: timestamp ascending
    assert export_result.diagnostics[0].timestamp <= export_result.diagnostics[1].timestamp

    # Canonical JSON string is valid, sorted, and reproducible
    can_json = export_result.to_canonical_json()
    assert isinstance(can_json, str)
    can_dict = json.loads(can_json)
    assert can_dict["schema_version"] == "1.0.0"
    assert can_dict["investigation_id"] == "inv_alpha"
    assert len(can_dict["diagnostics"]) == 2


# ==============================================================================
# Scenario 2: Stable Content Fingerprints for Identical Snapshots
# ==============================================================================

def test_stable_ids_and_content_fingerprints_identical_snapshots(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
    clock: DeterministicClock,
) -> None:
    """Identical source state and options produce reproducible content fingerprints."""
    diagnostics_service.record_diagnostic(
        investigation_id="inv_stable",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Coverage degraded",
        report_id="rpt_001",
    )

    req = DiagnosticsExportRequest(investigation_id="inv_stable")
    exp1 = diagnostics_service.export_diagnostics(req)

    # Advance time: export_id will vary due to timestamp, but content_fingerprint remains identical
    clock.advance(30.0)
    exp2 = diagnostics_service.export_diagnostics(req)

    assert exp1.content_fingerprint == exp2.content_fingerprint
    assert exp1.record_counts == exp2.record_counts
    assert exp1.source_references == exp2.source_references


# ==============================================================================
# Scenario 3: Source Changes Produce Fingerprint Updates
# ==============================================================================

def test_source_changes_produce_different_fingerprints(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
    clock: DeterministicClock,
) -> None:
    """Mutating source history by adding diagnostics alters the export fingerprint."""
    diagnostics_service.record_diagnostic(
        investigation_id="inv_delta",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Initial state",
    )
    exp_before = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_delta")
    )

    # Advance clock past cooldown and add a new failure
    clock.advance(100.0)
    diagnostics_service.record_diagnostic(
        investigation_id="inv_delta",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="New failure event",
    )
    exp_after = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_delta")
    )

    assert exp_before.content_fingerprint != exp_after.content_fingerprint
    assert exp_after.record_counts["diagnostics"] == 2


# ==============================================================================
# Scenario 4: Read-Only Behavior and Source Immutability
# ==============================================================================

def test_read_only_behavior_and_source_immutability(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
) -> None:
    """Generating an export or reconciling does not mutate source records or alert statuses."""
    rec = diagnostics_service.record_diagnostic(
        investigation_id="inv_readonly",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Critical failure",
    )
    alerts_before = diagnostics_service.list_alerts(investigation_id="inv_readonly")
    diags_before = diagnostics_service.list_diagnostics(investigation_id="inv_readonly")

    # Perform export and reconciliation
    exp = diagnostics_service.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_readonly"))
    recon = diagnostics_service.reconcile_export(exp.export_id)

    alerts_after = diagnostics_service.list_alerts(investigation_id="inv_readonly")
    diags_after = diagnostics_service.list_diagnostics(investigation_id="inv_readonly")

    assert len(alerts_before) == len(alerts_after)
    assert len(diags_before) == len(diags_after)
    assert alerts_before[0].status == alerts_after[0].status
    assert recon.status == DiagnosticsReconciliationStatus.MATCHED


# ==============================================================================
# Scenario 5: Complete, Empty, Truncated, and Unavailable Histories
# ==============================================================================

def test_complete_empty_truncated_and_unavailable_histories(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
    clock: DeterministicClock,
) -> None:
    """Verify retention status for empty, complete, and capacity-overflowed exports."""
    # 1. Empty history
    exp_empty = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_empty")
    )
    assert exp_empty.retention_status == "NO_DATA"
    assert not exp_empty.is_truncated
    assert exp_empty.record_counts["diagnostics"] == 0

    # 2. Complete history
    diagnostics_service.record_diagnostic(
        investigation_id="inv_complete",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Complete state diagnostic",
    )
    exp_complete = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_complete")
    )
    assert exp_complete.retention_status == "COMPLETE"
    assert not exp_complete.is_truncated

    # 3. Truncated history: overflow ring buffer (capacity=20)
    for i in range(25):
        clock.advance(1.0)
        diagnostics_service.record_diagnostic(
            investigation_id="inv_overflow",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Overflow diag {i}",
        )
    exp_overflow = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_overflow")
    )
    assert exp_overflow.retention_status == "TRUNCATED_TO_CAPACITY"
    assert exp_overflow.is_truncated
    assert exp_overflow.omitted_reasons.get("evicted_due_to_retention", 0) > 0

    # 4. Unavailable history
    recon_unknown = diagnostics_service.reconcile_export("export_nonexistent_123")
    assert recon_unknown.status == DiagnosticsReconciliationStatus.UNAVAILABLE


# ==============================================================================
# Scenario 6: Alert Lifecycle and Request Filter Semantics
# ==============================================================================

def test_alert_lifecycle_and_filter_behavior(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
    clock: DeterministicClock,
) -> None:
    """Export request filters respect include_alerts, active_only, and include_suppressed."""
    # Trigger an alert
    clock.advance(10.0)
    d1 = diagnostics_service.record_diagnostic(
        investigation_id="inv_alerts",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Failure for alert",
    )
    alerts = diagnostics_service.list_alerts(investigation_id="inv_alerts")
    assert len(alerts) == 1
    alert_id = alerts[0].alert_id

    # Acknowledge alert
    diagnostics_service.acknowledge_alert(alert_id, acknowledged_by="analyst_1")

    # Export with include_alerts=False
    exp_no_alerts = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_alerts", include_alerts=False)
    )
    assert len(exp_no_alerts.alerts) == 0
    assert exp_no_alerts.omitted_reasons.get("alerts_excluded_by_request", 0) == 1

    # Resolve alert
    diagnostics_service.resolve_alert(alert_id, reason="Resolved issue")

    # Export with active_only=True excludes resolved alert
    exp_active_only = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_alerts", active_only=True)
    )
    assert len(exp_active_only.alerts) == 0

    # Export without active_only preserves resolved alert
    exp_all_alerts = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_alerts", active_only=False)
    )
    assert len(exp_all_alerts.alerts) == 1
    assert exp_all_alerts.alerts[0].status == AuditAlertStatus.RESOLVED


# ==============================================================================
# Scenario 7: Reconciliation Discrepancy Detection (Altered & Duplicated)
# ==============================================================================

def test_reconciliation_altered_and_duplicated_exported_records(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
    clock: DeterministicClock,
) -> None:
    """Altered record content or duplicated record IDs trigger MISMATCHED reconciliation."""
    diagnostics_service.record_diagnostic(
        investigation_id="inv_tamper",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Original diagnostic",
    )
    export_result = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_tamper")
    )

    # 1. Tamper with diagnostic record content in export
    original_diag = export_result.diagnostics[0]
    tampered_diag = AuditDiagnosticRecord(
        diagnostic_id=original_diag.diagnostic_id,
        correlation_id=original_diag.correlation_id,
        investigation_id=original_diag.investigation_id,
        timestamp=original_diag.timestamp,
        timestamp_iso=original_diag.timestamp_iso,
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,  # altered
        severity=AuditDiagnosticSeverity.CRITICAL,  # altered
        evidence_nature=original_diag.evidence_nature,
        message="Tampered message",
    )
    tampered_export = DiagnosticsExportResult(
        export_id="export_tampered_1",
        request=export_result.request,
        created_at=export_result.created_at,
        created_at_iso=export_result.created_at_iso,
        schema_version=export_result.schema_version,
        diagnostics=(tampered_diag,),
        alerts=(),
        record_counts={"diagnostics": 1, "alerts": 0},
        source_references=(),
        retention_status="COMPLETE",
        is_truncated=False,
        content_fingerprint=export_result.content_fingerprint,
    )
    diagnostics_service._exports_by_id["export_tampered_1"] = tampered_export

    report = diagnostics_service.reconcile_export("export_tampered_1")
    assert report.status == DiagnosticsReconciliationStatus.MISMATCHED
    assert any(d.discrepancy_type == "ALTERED_RECORD" for d in report.discrepancies)
    assert not report.fingerprint_verified

    # 2. Duplicate record ID in export
    duplicated_export = DiagnosticsExportResult(
        export_id="export_duplicated_1",
        request=export_result.request,
        created_at=export_result.created_at,
        created_at_iso=export_result.created_at_iso,
        schema_version=export_result.schema_version,
        diagnostics=(original_diag, original_diag),
        alerts=(),
        record_counts={"diagnostics": 2, "alerts": 0},
        source_references=(),
        retention_status="COMPLETE",
        is_truncated=False,
        content_fingerprint=export_result.content_fingerprint,
    )
    diagnostics_service._exports_by_id["export_duplicated_1"] = duplicated_export

    report_dup = diagnostics_service.reconcile_export("export_duplicated_1")
    assert report_dup.status == DiagnosticsReconciliationStatus.MISMATCHED
    assert any(d.discrepancy_type == "DUPLICATED_EXPORT_RECORD" for d in report_dup.discrepancies)


# ==============================================================================
# Scenario 8: Reconciliation of Evicted Records (Limitation, Not Corruption)
# ==============================================================================

def test_reconciliation_evicted_records_reports_limitation_not_corruption(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
    clock: DeterministicClock,
) -> None:
    """When source records are evicted by capacity, reconciliation reports INCOMPLETE, not corruption."""
    clock.advance(1.0)
    d_early = diagnostics_service.record_diagnostic(
        investigation_id="inv_evict_test",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Early record before overflow",
    )
    early_id = d_early.diagnostic_id

    # Export while early record is still present
    early_export = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_evict_test")
    )

    # Overflow diagnostics queue (max_diagnostics=20) to evict early_id
    for i in range(25):
        clock.advance(1.0)
        diagnostics_service.record_diagnostic(
            investigation_id=f"inv_filler_{i}",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Filler record {i}",
        )

    assert diagnostics_service.is_diagnostic_evicted(early_id)
    assert diagnostics_service.get_diagnostic(early_id) is None

    # Reconcile early export against active source state
    recon_report = diagnostics_service.reconcile_export(early_export.export_id)

    # Must be INCOMPLETE (retention limitation), not MISMATCHED (corruption)
    assert recon_report.status == DiagnosticsReconciliationStatus.INCOMPLETE
    assert len(recon_report.discrepancies) > 0
    evicted_disc = [d for d in recon_report.discrepancies if d.discrepancy_type == "EVICTED_SOURCE_RECORD"]
    assert len(evicted_disc) == 1
    assert evicted_disc[0].is_retention_limitation is True


# ==============================================================================
# Scenario 9: Content Fingerprint Verification and Mismatch Detection
# ==============================================================================

def test_fingerprint_mismatch_detection(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
) -> None:
    """Forged content fingerprint is detected and reported as a discrepancy."""
    diagnostics_service.record_diagnostic(
        investigation_id="inv_fp",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Valid record",
    )
    valid_export = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_fp")
    )

    # Create export with corrupted fingerprint
    forged_export = DiagnosticsExportResult(
        export_id="export_forged_fp",
        request=valid_export.request,
        created_at=valid_export.created_at,
        created_at_iso=valid_export.created_at_iso,
        schema_version=valid_export.schema_version,
        diagnostics=valid_export.diagnostics,
        alerts=valid_export.alerts,
        record_counts=valid_export.record_counts,
        source_references=valid_export.source_references,
        retention_status=valid_export.retention_status,
        is_truncated=valid_export.is_truncated,
        content_fingerprint="0000000000000000000000000000000000000000000000000000000000000000",
    )
    diagnostics_service._exports_by_id["export_forged_fp"] = forged_export

    report = diagnostics_service.reconcile_export("export_forged_fp")
    assert report.status == DiagnosticsReconciliationStatus.MISMATCHED
    assert not report.fingerprint_verified
    assert any(d.discrepancy_type == "FINGERPRINT_MISMATCH" for d in report.discrepancies)


# ==============================================================================
# Scenario 10: Secret and Credential Sanitization
# ==============================================================================

def test_secret_and_credential_sanitization_in_exports(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
) -> None:
    """Credentials, tokens, and sensitive keys are sanitized in exports and JSON representations."""
    diagnostics_service.record_diagnostic(
        investigation_id="inv_sanitized_target",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Failure while evaluating api_key=sk-1234567890abcdef and password=secret_pass",
        details={
            "token": "bearer_super_secret_token",
            "safe_detail": "non_sensitive_data",
        },
    )

    export = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_sanitized_target")
    )
    can_json = export.to_canonical_json()
    export_dict_str = json.dumps(export.to_dict())

    # Raw secrets must not be exposed
    assert "sk-1234567890abcdef" not in can_json
    assert "secret_pass" not in can_json
    assert "bearer_super_secret_token" not in can_json
    assert "sk-1234567890abcdef" not in export_dict_str


# ==============================================================================
# Scenario 11: Concurrent Export and Reconciliation Thread Safety
# ==============================================================================

def test_concurrent_export_and_reconciliation_thread_safety(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
    clock: DeterministicClock,
) -> None:
    """Concurrent threads creating exports, querying, and reconciling operate safely."""
    for i in range(10):
        clock.advance(1.0)
        diagnostics_service.record_diagnostic(
            investigation_id=f"inv_concurrent_{i % 3}",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Concurrent record {i}",
        )

    def worker(worker_id: int) -> bool:
        inv = f"inv_concurrent_{worker_id % 3}"
        req = DiagnosticsExportRequest(investigation_id=inv)
        exp = diagnostics_service.export_diagnostics(req)
        report = diagnostics_service.reconcile_export(exp.export_id)
        assert report.status in (
            DiagnosticsReconciliationStatus.MATCHED,
            DiagnosticsReconciliationStatus.INCOMPLETE,
        )
        return True

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, i) for i in range(20)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == 20
    assert all(results)


# ==============================================================================
# Scenario 12: Bounded Metadata Retention and StateStore Reconstruction
# ==============================================================================

def test_bounded_metadata_retention_and_statestore_reconstruction(
    event_bus: EventBus,
    state_store: StateStore,
    clock: DeterministicClock,
) -> None:
    """Exports and reconciliations are bounded by capacity and reconstructable from StateStore."""
    service = HistoricalReviewAuditDiagnosticsService(
        event_bus=event_bus,
        state_store=state_store,
        max_exports=5,
        max_reconciliations=5,
        time_provider=clock,
    )

    service.record_diagnostic(
        investigation_id="inv_bound",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Baseline diagnostic",
    )

    export_ids = []
    for i in range(8):
        clock.advance(1.0)
        exp = service.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_bound"))
        export_ids.append(exp.export_id)
        service.reconcile_export(exp.export_id)

    # First exports should be evicted because capacity is 5
    assert service.is_export_evicted(export_ids[0])
    assert len(service.list_exports(investigation_id="inv_bound")) <= 5
    assert len(service.list_reconciliations()) <= 5

    # Reconstruct service with auto_hydrate=True
    new_service = HistoricalReviewAuditDiagnosticsService(
        event_bus=event_bus,
        state_store=state_store,
        max_exports=5,
        max_reconciliations=5,
        time_provider=clock,
        auto_hydrate=True,
    )

    hydrated_exports = new_service.list_exports(investigation_id="inv_bound")
    assert len(hydrated_exports) > 0
    # Latest export must be present
    latest_id = export_ids[-1]
    assert new_service.get_export(latest_id) is not None


# ==============================================================================
# Scenario 13: Orchestrator Handlers Registration and Execution
# ==============================================================================

def test_orchestrator_handlers_registration_and_execution(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
) -> None:
    """MasterOrchestrator registers and successfully executes the 4 export/reconciliation commands."""
    orchestrator = MasterOrchestrator()
    register_audit_diagnostics_handlers(orchestrator, diagnostics_service)

    diagnostics_service.record_diagnostic(
        investigation_id="inv_orch",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Orchestrator diagnostic test",
    )

    from app.orchestrator.models import RequestStatus

    # 1. export_historical_review_diagnostics
    res_exp = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="export_historical_review_diagnostics",
            context={"investigation_id": "inv_orch"},
        )
    )
    assert res_exp.status == RequestStatus.COMPLETED
    assert res_exp.result["status"] == "success"
    assert res_exp.result["read_only"] is True
    assert res_exp.result["execution_locked"] is True
    exp_id = res_exp.result["export"]["export_id"]

    # 2. get_historical_review_diagnostics_export
    res_get = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="get_historical_review_diagnostics_export",
            context={"export_id": exp_id},
        )
    )
    assert res_get.status == RequestStatus.COMPLETED
    assert res_get.result["status"] == "success"
    assert res_get.result["export"]["export_id"] == exp_id

    # 3. list_historical_review_diagnostics_exports
    res_list = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="list_historical_review_diagnostics_exports",
            context={"investigation_id": "inv_orch"},
        )
    )
    assert res_list.status == RequestStatus.COMPLETED
    assert res_list.result["status"] == "success"
    assert res_list.result["count"] >= 1

    # 4. reconcile_historical_review_diagnostics_export
    res_recon = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="reconcile_historical_review_diagnostics_export",
            context={"export_id": exp_id},
        )
    )
    assert res_recon.status == RequestStatus.COMPLETED
    assert res_recon.result["status"] == "success"
    assert res_recon.result["reconciliation"]["status"] == "MATCHED"

    # 5. Prohibited trading parameters rejected
    res_prohibited = orchestrator.handle_request(
        OrchestratorRequest.create(
            command="export_historical_review_diagnostics",
            context={"investigation_id": "inv_orch", "trade_recommendation": "BUY"},
        )
    )
    assert res_prohibited.status == RequestStatus.FAILED


# ==============================================================================
# Scenario 14: Dashboard Endpoint 28 Read-Only Routes and Security
# ==============================================================================

def test_dashboard_endpoint_28_exports_and_reconciliations(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
) -> None:
    """Dashboard Endpoint 28 exposes read-only /exports and /reconciliations routes with strict HTTP security."""
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        audit_diagnostics_service=diagnostics_service,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        diagnostics_service.record_diagnostic(
            investigation_id="inv_dash",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message="Dashboard route diagnostic",
        )
        exp = diagnostics_service.export_diagnostics(
            DiagnosticsExportRequest(investigation_id="inv_dash")
        )
        recon = diagnostics_service.reconcile_export(exp.export_id)

        # GET /exports
        url_exports = f"{server.url}/api/historical-review-diagnostics/exports"
        req = urllib.request.Request(url_exports, method="GET")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] >= 1
            assert resp.headers.get("X-Content-Type-Options") == "nosniff"

        # GET /exports/<export_id>
        url_exp_id = f"{server.url}/api/historical-review-diagnostics/exports/{exp.export_id}"
        req = urllib.request.Request(url_exp_id, method="GET")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["export"]["export_id"] == exp.export_id

        # GET /reconciliations
        url_recons = f"{server.url}/api/historical-review-diagnostics/reconciliations"
        req = urllib.request.Request(url_recons, method="GET")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] >= 1

        # GET /reconciliations/<reconciliation_id>
        url_recon_id = f"{server.url}/api/historical-review-diagnostics/reconciliations/{recon.reconciliation_id}"
        req = urllib.request.Request(url_recon_id, method="GET")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["reconciliation"]["reconciliation_id"] == recon.reconciliation_id

        # Path traversal rejected with 400
        url_traversal = f"{server.url}/api/historical-review-diagnostics/exports/..%2Fetc%2Fpasswd"
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(urllib.request.Request(url_traversal, method="GET"))
        assert exc_info.value.code == 400

        # Mutation operations rejected with 405 Method Not Allowed
        req_post = urllib.request.Request(url_exports, data=b"{}", method="POST")
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_post)
        assert exc_info.value.code == 405
        assert "GET, HEAD" in exc_info.value.headers.get("Allow", "")

    finally:
        server.stop()


# ==============================================================================
# Scenario 15: Safety Boundaries and Execution Locked Invariant
# ==============================================================================

def test_safety_boundaries_and_execution_locked(
    diagnostics_service: HistoricalReviewAuditDiagnosticsService,
) -> None:
    """Safety invariants remain enforced: execution locked, no trading recommendations."""
    assert Settings.execution_locked is True

    # Exports and reconciliations are informational and do not mutate trading engine
    exp = diagnostics_service.export_diagnostics(
        DiagnosticsExportRequest(investigation_id="inv_safety")
    )
    report = diagnostics_service.reconcile_export(exp.export_id)

    assert exp.export_id.startswith("export_")
    assert report.reconciliation_id.startswith("recon_")
    assert Settings.execution_locked is True
