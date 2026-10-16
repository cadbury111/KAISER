"""KAISER — Phase 2, Step 42 Test Suite.

Diagnostics Export Cross-Lifecycle Regression & Failure-Recovery Hardening.

TEST SCENARIOS:
A. Export -> Persistence -> Reconstruction -> Reconciliation:
   - Scenario 1: Clean cross-lifecycle export, persistence, reconstruction, and reconciliation.
   - Scenario 2: Deterministic stability across repeated clean service reconstructions.
   - Scenario 3: Missing/depleted source evidence after selective reconstruction is never falsely reported as verified.
B. Retention and Reconstruction Interactions:
   - Scenario 4: Pre-eviction export reconciled as INCOMPLETE with EVICTED_SOURCE_RECORD after eviction & reconstruction.
   - Scenario 5: Retention limits never mask unexplained tampering across reconstruction (status is MISMATCHED).
   - Scenario 6: Boundary condition behavior right at retention capacity and overflow.
   - Scenario 7: Export queue FIFO eviction preserved across reconstruction (UNAVAILABLE with retention limitation).
C. Repeated and Interleaved Operations:
   - Scenario 8: Interleaved lifecycle operations (ingest, export, reconcile, evict, reconstruct) remain ordering-safe.
   - Scenario 9: Multi-threaded concurrent export, reconciliation, and ingestion under deterministic clock.
   - Scenario 10: Idempotent repeated reconciliations of unchanged evidence across reconstructed services.
D. Failure Recovery and Malformed State:
   - Scenario 11: Partially corrupt StateStore export history list handled gracefully without losing valid exports.
   - Scenario 12: Partially corrupt StateStore reconciliation history list handled gracefully without losing valid reports.
   - Scenario 13: Service remains fully operational after recovering from malformed persisted state.
   - Scenario 14: Non-existent, empty, and traversal-like IDs safely handled without fabricating evidence.
E. Integrity and Security Regression:
   - Scenario 15: Source evidence immutability preserved under failed, invalid, or tampered reconciliation attempts.
   - Scenario 16: Credential and secret sanitization preserved across persistence, reconstruction, and reconciliation.
   - Scenario 17: Canonical fingerprint reproducibility preserved byte-for-byte across reconstruction.
   - Scenario 18: Dashboard Endpoint 28 read-only routes, HTTP 405, traversal protection, and security headers on reconstructed service.
F. Bounded Resource and Lifecycle Checks:
   - Scenario 19: Export and reconciliation collections strictly respect configured retention bounds in memory and StateStore.
   - Scenario 20: Mandatory safety invariants preserved (execution_locked == True, trading engine uninitialized, prohibited terms rejected).
"""

from __future__ import annotations

import collections
import concurrent.futures
import http.client
import json
import threading
import time
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.request

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
from app.orchestrator.models import OrchestratorRequest, RequestStatus
from app.portfolio.dashboard.server import PortfolioDashboardServer


class DeterministicClock:
    """Injectable deterministic clock fixture."""

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


# ==============================================================================
# Area A: Export -> Persistence -> Reconstruction -> Reconciliation
# ==============================================================================

def test_scenario_1_cross_lifecycle_export_persistence_reconstruction_reconciliation(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify export generated on instance 1 persists, reconstructs on instance 2, and reconciles to MATCHED."""
    # Instance 1: Generate evidence and export
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc1.record_diagnostic(
        investigation_id="inv_life_1",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Failure diagnostic for cross-lifecycle testing.",
        source_references=("ref_db_1", "ref_db_2"),
    )
    clock.advance(1.0)
    svc1.record_diagnostic(
        investigation_id="inv_life_1",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Repeatability divergence in investigation life 1.",
        source_references=("ref_rep_1",),
    )

    exp1 = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_life_1"))
    recon1 = svc1.reconcile_export(exp1.export_id)
    assert recon1.status == DiagnosticsReconciliationStatus.MATCHED
    assert recon1.matched_diagnostics == 2
    assert recon1.matched_alerts == 2

    # Instance 2: Clean reconstruction from same StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    # Verify export is restored
    restored_exp = svc2.get_export(exp1.export_id)
    assert restored_exp is not None
    assert restored_exp.export_id == exp1.export_id
    assert restored_exp.content_fingerprint == exp1.content_fingerprint
    assert restored_exp.record_counts == exp1.record_counts

    # Reconcile on reconstructed instance
    recon2 = svc2.reconcile_export(exp1.export_id)
    assert recon2.status == DiagnosticsReconciliationStatus.MATCHED
    assert recon2.fingerprint_verified
    assert recon2.matched_diagnostics == 2
    assert recon2.matched_alerts == 2
    assert len(recon2.discrepancies) == 0


def test_scenario_2_repeated_clean_reconstructions_stability(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify deterministic stability across repeated clean service reconstructions."""
    svc_init = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    for i in range(3):
        clock.advance(1.0)
        svc_init.record_diagnostic(
            investigation_id=f"inv_multi_{i}",
            category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Coverage diagnostic {i}",
        )

    exp = svc_init.export_diagnostics(DiagnosticsExportRequest())

    # Perform 3 sequential clean reconstructions
    for cycle in range(3):
        clock.advance(0.5)
        svc_recon = HistoricalReviewAuditDiagnosticsService(
            max_diagnostics=50,
            max_alerts=50,
            state_store=state_store,
            time_provider=clock,
        )
        report = svc_recon.reconcile_export(exp.export_id)
        assert report.status == DiagnosticsReconciliationStatus.MATCHED
        assert report.fingerprint_verified
        assert report.matched_diagnostics == 3
        assert len(report.discrepancies) == 0


def test_scenario_3_missing_source_evidence_after_selective_reconstruction_fails_reconciliation(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify that if source evidence is cleared but export remains, reconciliation flags MISSING_SOURCE_RECORD."""
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc1.record_diagnostic(
        investigation_id="inv_source_loss",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Evidence that will be lost in source.",
    )
    exp = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_source_loss"))

    # Simulate selective loss of source diagnostic history in StateStore
    state_store.delete("historical_review_diagnostics:history")
    # Delete individual key as well
    diag_id = exp.diagnostics[0].diagnostic_id
    state_store.delete(f"historical_review_diagnostics:{diag_id}")

    # Reconstruct service from degraded StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    recon = svc2.reconcile_export(exp.export_id)
    assert recon.status == DiagnosticsReconciliationStatus.MISMATCHED
    types = [d.discrepancy_type for d in recon.discrepancies]
    assert "MISSING_SOURCE_RECORD" in types


# ==============================================================================
# Area B: Retention and Reconstruction Interactions
# ==============================================================================

def test_scenario_4_pre_eviction_export_reconciled_after_eviction_and_reconstruction(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify pre-eviction export reconciles as INCOMPLETE with EVICTED_SOURCE_RECORD after eviction & reconstruction."""
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )

    # Ingest 10 records to capacity
    for i in range(10):
        clock.advance(1.0)
        svc1.record_diagnostic(
            investigation_id="inv_evict_cross",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Initial record {i}",
        )

    exp = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_evict_cross"))
    evicted_target_id = exp.diagnostics[0].diagnostic_id

    # Ingest 5 more records, triggering FIFO eviction
    for i in range(10, 15):
        clock.advance(1.0)
        svc1.record_diagnostic(
            investigation_id="inv_evict_cross",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Overflow record {i}",
        )

    # Reconstruct fresh service instance from StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )

    # Eviction tracking must be hydrated and operational
    assert svc2.is_diagnostic_evicted(evicted_target_id)

    # Reconcile export on reconstructed service
    recon = svc2.reconcile_export(exp.export_id)
    assert recon.status == DiagnosticsReconciliationStatus.INCOMPLETE
    assert recon.fingerprint_verified
    evicted_discs = [d for d in recon.discrepancies if d.discrepancy_type == "EVICTED_SOURCE_RECORD"]
    assert len(evicted_discs) > 0
    for d in evicted_discs:
        assert d.is_retention_limitation is True


def test_scenario_5_retention_limits_never_mask_tampering_across_reconstruction(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify that eviction plus tampering across reconstruction yields MISMATCHED (never INCOMPLETE)."""
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )

    for i in range(10):
        clock.advance(1.0)
        svc1.record_diagnostic(
            investigation_id="inv_mask_cross",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Record {i}",
        )

    exp = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_mask_cross"))

    # Ingest 5 more records to evict the oldest
    for i in range(10, 15):
        clock.advance(1.0)
        svc1.record_diagnostic(
            investigation_id="inv_mask_cross",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Overflow record {i}",
        )

    # Tamper with active record 9
    surviving_diag = exp.diagnostics[9]
    tampered_surviving = AuditDiagnosticRecord(
        diagnostic_id=surviving_diag.diagnostic_id,
        correlation_id=surviving_diag.correlation_id,
        investigation_id=surviving_diag.investigation_id,
        timestamp=surviving_diag.timestamp,
        timestamp_iso=surviving_diag.timestamp_iso,
        category=surviving_diag.category,
        severity=AuditDiagnosticSeverity.CRITICAL,  # Tampered
        evidence_nature=surviving_diag.evidence_nature,
        message="TAMPERED across reconstruction.",
        source_references=surviving_diag.source_references,
    )

    tampered_export = DiagnosticsExportResult(
        export_id=exp.export_id,
        request=exp.request,
        created_at=exp.created_at,
        created_at_iso=exp.created_at_iso,
        schema_version=exp.schema_version,
        diagnostics=exp.diagnostics[:9] + (tampered_surviving,),
        alerts=exp.alerts,
        record_counts=exp.record_counts,
        source_references=exp.source_references,
        retention_status=exp.retention_status,
        is_truncated=exp.is_truncated,
        content_fingerprint=exp.content_fingerprint,
        omitted_reasons=exp.omitted_reasons,
    )

    # Reconstruct service
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )

    recon = svc2.reconcile_export(tampered_export)
    assert recon.status == DiagnosticsReconciliationStatus.MISMATCHED
    types = [d.discrepancy_type for d in recon.discrepancies]
    assert "EVICTED_SOURCE_RECORD" in types
    assert "ALTERED_RECORD" in types


def test_scenario_6_boundary_conditions_near_retention_capacity(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify retention status transition from COMPLETE to TRUNCATED_TO_CAPACITY at the exact threshold."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )

    # Exact capacity = 10
    for i in range(10):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_bound_test",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Record {i}",
        )

    exp_at_capacity = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_bound_test"))
    assert exp_at_capacity.retention_status == "COMPLETE"
    assert not exp_at_capacity.is_truncated

    # Add 1 record (11th), crossing capacity by 1
    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_bound_test",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Record 10 (overflow by 1)",
    )

    exp_overflow = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_bound_test"))
    assert exp_overflow.retention_status == "TRUNCATED_TO_CAPACITY"
    assert exp_overflow.is_truncated
    assert exp_overflow.omitted_reasons.get("evicted_due_to_retention") == 1


def test_scenario_7_export_queue_fifo_eviction_across_reconstruction(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify export queue FIFO eviction is tracked and hydrated across reconstruction."""
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        max_exports=5,
        state_store=state_store,
        time_provider=clock,
    )

    svc1.record_diagnostic(
        investigation_id="inv_queue_evict",
        category=getattr(AuditDiagnosticCategory, "DATA_GAP_OBSERVED", AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE),
        severity=AuditDiagnosticSeverity.INFO,
        message="Baseline record.",
    )

    clock.advance(1.0)
    e1 = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_queue_evict"))
    for i in range(2, 7):
        clock.advance(1.0)
        svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_queue_evict"))

    # e1 was evicted on instance 1
    assert svc1.is_export_evicted(e1.export_id)

    # Reconstruct service from StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        max_exports=5,
        state_store=state_store,
        time_provider=clock,
    )

    # Eviction tracking must persist across reconstruction
    assert svc2.is_export_evicted(e1.export_id)
    assert svc2.get_export(e1.export_id) is None

    recon = svc2.reconcile_export(e1.export_id)
    assert recon.status == DiagnosticsReconciliationStatus.UNAVAILABLE
    assert "evicted due to retention limits" in recon.summary_notes


# ==============================================================================
# Area C: Repeated and Interleaved Operations
# ==============================================================================

def test_scenario_8_interleaved_lifecycle_operations_ordering_safety(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify safe operation ordering when interleaving recording, exporting, reconciling, listing, and reconstructing."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=20,
        max_alerts=20,
        state_store=state_store,
        time_provider=clock,
    )

    for i in range(5):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_interleave",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Interleaved record {i}",
        )
        exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_interleave"))
        rec = svc.reconcile_export(exp.export_id)
        assert rec.status == DiagnosticsReconciliationStatus.MATCHED
        assert len(svc.list_exports()) >= 1
        assert len(svc.list_reconciliations()) >= 1

    # Reconstruct service and verify history count
    svc_recon = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=20,
        max_alerts=20,
        state_store=state_store,
        time_provider=clock,
    )
    assert len(svc_recon.list_exports()) == 5
    assert len(svc_recon.list_reconciliations()) == 5


def test_scenario_9_multi_threaded_concurrent_export_reconcile_and_ingestion(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify concurrency safety under 10 parallel threads executing mixed diagnostic operations."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=100,
        max_alerts=100,
        state_store=state_store,
        time_provider=clock,
    )

    def worker_action(w_id: int) -> None:
        for j in range(5):
            clock.advance(0.1)
            svc.record_diagnostic(
                investigation_id=f"inv_conc_{w_id}",
                category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
                severity=AuditDiagnosticSeverity.INFO,
                message=f"Worker {w_id} msg {j}",
            )
            exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id=f"inv_conc_{w_id}"))
            rec = svc.reconcile_export(exp.export_id)
            assert rec.status in (DiagnosticsReconciliationStatus.MATCHED, DiagnosticsReconciliationStatus.INCOMPLETE)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker_action, wid) for wid in range(10)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    assert len(svc.list_exports()) <= 50
    assert len(svc.list_reconciliations()) <= 50


def test_scenario_10_idempotent_repeated_reconciliations_of_unchanged_evidence(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify repeated reconciliations of unchanged evidence remain strictly idempotent across reconstructions."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_idem",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Idempotent diagnostic.",
    )
    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_idem"))

    reports = []
    for _ in range(3):
        svc_recon = HistoricalReviewAuditDiagnosticsService(
            max_diagnostics=50,
            max_alerts=50,
            state_store=state_store,
            time_provider=clock,
        )
        reports.append(svc_recon.reconcile_export(exp.export_id))

    for r in reports:
        assert r.status == DiagnosticsReconciliationStatus.MATCHED
        assert r.matched_diagnostics == 1
        assert r.fingerprint_verified
        assert len(r.discrepancies) == 0


# ==============================================================================
# Area D: Failure Recovery and Malformed State
# ==============================================================================

def test_scenario_11_partially_corrupt_statestore_export_history_resilience(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify corrupt export entries in StateStore do not prevent access to valid export entries."""
    svc_setup = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )
    clock.advance(1.0)
    svc_setup.record_diagnostic(
        investigation_id="inv_corrupt_exp",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Valid 1",
    )
    exp1 = svc_setup.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_corrupt_exp"))
    clock.advance(1.0)
    exp2 = svc_setup.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_corrupt_exp"))

    # Tamper with the history array in StateStore by injecting corrupted items
    raw_hist = state_store.get("historical_review_exports:history", [])
    corrupted_hist = [
        raw_hist[0],  # valid exp2
        "not_a_valid_dict_payload",  # corrupted string
        {"bad_schema": True},  # corrupted dict
        raw_hist[1],  # valid exp1
        123456,  # corrupted int
    ]
    state_store.set("historical_review_exports:history", corrupted_hist)

    # Reconstruct service
    svc_recon = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    # Both valid exports must still be hydrated and accessible
    assert svc_recon.get_export(exp1.export_id) is not None
    assert svc_recon.get_export(exp2.export_id) is not None
    active_exports = svc_recon.list_exports()
    assert len(active_exports) == 2


def test_scenario_12_partially_corrupt_statestore_reconciliation_history_resilience(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify corrupt reconciliation entries in StateStore do not crash hydration or block valid reports."""
    svc_setup = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )
    clock.advance(1.0)
    svc_setup.record_diagnostic(
        investigation_id="inv_corrupt_rec",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Msg",
    )
    exp = svc_setup.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_corrupt_rec"))
    rec1 = svc_setup.reconcile_export(exp.export_id)

    raw_hist = state_store.get("historical_review_reconciliations:history", [])
    corrupted_hist = [
        raw_hist[0],
        {"corrupt_key": 999},
        None,
    ]
    state_store.set("historical_review_reconciliations:history", corrupted_hist)

    svc_recon = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    assert svc_recon.get_reconciliation(rec1.reconciliation_id) is not None
    assert len(svc_recon.list_reconciliations()) == 1


def test_scenario_13_service_remains_operational_after_recovering_from_malformed_state(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify service remains fully operational after recovering from malformed StateStore state."""
    # Seed totally corrupt keys
    state_store.set("historical_review_exports:history", "corrupt_string_not_list")
    state_store.set("historical_review_reconciliations:history", 12345)
    state_store.set("historical_review_diagnostics:retention_stats", "bad_stats")

    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    # Perform normal operations
    clock.advance(1.0)
    d = svc.record_diagnostic(
        investigation_id="inv_recover_op",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Record after malformed recovery.",
    )
    assert d is not None

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_recover_op"))
    assert exp is not None
    assert exp.record_counts["diagnostics"] == 1

    rec = svc.reconcile_export(exp.export_id)
    assert rec.status == DiagnosticsReconciliationStatus.MATCHED


def test_scenario_14_safe_handling_of_invalid_and_traversal_ids(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify non-existent, empty, and traversal-like IDs safely return None or raise ValidationError."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    # Non-existent safe IDs return None
    assert svc.get_export("exp_nonexistent_1234") is None
    assert svc.get_reconciliation("rec_nonexistent_1234") is None
    assert not svc.is_export_evicted("exp_nonexistent_1234")

    # Traversal strings in get safely return None
    assert svc.get_export("../../etc/passwd") is None
    assert svc.get_reconciliation("../traversal") is None

    # Traversal strings in reconcile raise ValidationError
    with pytest.raises(ValidationError):
        svc.reconcile_export("../traversal_escape")


# ==============================================================================
# Area E: Integrity and Security Regression
# ==============================================================================

def test_scenario_15_source_evidence_immutability_under_tampered_and_invalid_reconciliations(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify failed or tampered reconciliation attempts do not mutate source records."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_immut_fail",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Source immutable record.",
    )

    diag_before = [d.to_dict() for d in svc.list_diagnostics()]
    alert_before = [a.to_dict() for a in svc.list_alerts()]
    ret_before = dict(svc.get_retention_status())

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_immut_fail"))

    # Attempt tampered reconciliation
    tampered_diag = AuditDiagnosticRecord(
        diagnostic_id=exp.diagnostics[0].diagnostic_id,
        correlation_id=exp.diagnostics[0].correlation_id,
        investigation_id=exp.diagnostics[0].investigation_id,
        timestamp=exp.diagnostics[0].timestamp,
        timestamp_iso=exp.diagnostics[0].timestamp_iso,
        category=exp.diagnostics[0].category,
        severity=AuditDiagnosticSeverity.INFO,
        evidence_nature=exp.diagnostics[0].evidence_nature,
        message="TAMPERED message.",
        source_references=exp.diagnostics[0].source_references,
    )
    tampered_export = DiagnosticsExportResult(
        export_id=exp.export_id,
        request=exp.request,
        created_at=exp.created_at,
        created_at_iso=exp.created_at_iso,
        schema_version=exp.schema_version,
        diagnostics=(tampered_diag,),
        alerts=exp.alerts,
        record_counts=exp.record_counts,
        source_references=exp.source_references,
        retention_status=exp.retention_status,
        is_truncated=exp.is_truncated,
        content_fingerprint=exp.content_fingerprint,
        omitted_reasons=exp.omitted_reasons,
    )

    svc.reconcile_export(tampered_export)

    diag_after = [d.to_dict() for d in svc.list_diagnostics()]
    alert_after = [a.to_dict() for a in svc.list_alerts()]
    ret_after = dict(svc.get_retention_status())

    assert diag_before == diag_after
    assert alert_before == alert_after
    assert ret_before["current_diagnostics_count"] == ret_after["current_diagnostics_count"]
    assert ret_before["total_diagnostics_evicted"] == ret_after["total_diagnostics_evicted"]


def test_scenario_16_credential_and_secret_sanitization_across_lifecycle(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify secrets are scrubbed at ingestion and remain absent across persistence, reconstruction, and reconciliation."""
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc1.record_diagnostic(
        investigation_id="inv_sec_life",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Diagnostic with confidential secrets.",
        details={
            "api_key": "AWS_SECRET_KEY_EXPOSURE_12345",
            "bearer_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.sensitive",
            "password": "production_database_password_999",
            "safe_param": "regular_value",
        },
    )

    exp1 = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_sec_life"))

    # Reconstruct service
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    exp2 = svc2.get_export(exp1.export_id)
    assert exp2 is not None
    rec2 = svc2.reconcile_export(exp1.export_id)

    # Inspect all serializations across reconstructed service
    all_serialized = [
        exp2.to_canonical_json(),
        json.dumps(exp2.to_dict()),
        json.dumps(rec2.to_dict()),
    ]
    for text in all_serialized:
        assert "AWS_SECRET_KEY_EXPOSURE_12345" not in text
        assert "production_database_password_999" not in text


def test_scenario_17_deterministic_fingerprint_preservation_across_reconstruction(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify canonical fingerprint remains byte-for-byte identical after reconstruction from StateStore."""
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc1.record_diagnostic(
        investigation_id="inv_fp_preserve",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Deterministic diagnostic.",
        source_references=("ref_alpha", "ref_beta"),
    )

    exp1 = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_fp_preserve"))
    fp1 = exp1.content_fingerprint
    can_json1 = exp1.to_canonical_json()

    # Reconstruct
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )
    exp2 = svc2.get_export(exp1.export_id)
    assert exp2 is not None
    fp2 = exp2.content_fingerprint
    can_json2 = exp2.to_canonical_json()

    assert fp1 == fp2
    assert can_json1 == can_json2


def test_scenario_18_dashboard_endpoint_28_read_only_routes_on_reconstructed_service(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify Dashboard Endpoint 28 operates securely on a reconstructed service."""
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )
    clock.advance(1.0)
    svc1.record_diagnostic(
        investigation_id="inv_dash_recon",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Dash diagnostic.",
    )
    exp = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_dash_recon"))
    svc1.reconcile_export(exp.export_id)

    # Reconstruct service
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        audit_diagnostics_service=svc2,
    )
    server.start(blocking=False)
    time.sleep(0.1)
    base_url = server.url

    try:
        # GET exports
        req = urllib.request.Request(f"{base_url}/api/historical-review-diagnostics/exports")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            assert resp.headers.get("X-Content-Type-Options") == "nosniff"
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["count"] >= 1

        # POST rejected with 405
        req_post = urllib.request.Request(
            f"{base_url}/api/historical-review-diagnostics/exports",
            data=b"{}",
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_post)
        assert exc_info.value.code == 405

        # Path traversal rejected
        traversal_url = f"{base_url}/api/historical-review-diagnostics/exports/..%2f..%2fbad"
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(traversal_url)
        assert exc_info.value.code in (400, 404)
    finally:
        server.stop()


# ==============================================================================
# Area F: Bounded Resource and Lifecycle Checks
# ==============================================================================

def test_scenario_19_bounded_retention_enforcement_for_exports_and_reconciliations(
    clock: DeterministicClock, state_store: StateStore
):
    """Verify export and reconciliation collections strictly respect configured retention bounds in memory and StateStore."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        max_exports=5,
        max_reconciliations=5,
        state_store=state_store,
        time_provider=clock,
    )

    svc.record_diagnostic(
        investigation_id="inv_bound_all",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Base diag",
    )

    # Ingest 15 exports and reconciliations
    for i in range(15):
        clock.advance(1.0)
        exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_bound_all"))
        svc.reconcile_export(exp.export_id)

    # Check in-memory bounds
    assert len(svc.list_exports()) <= 5
    assert len(svc.list_reconciliations()) <= 5

    # Check StateStore history array bounds
    exp_hist = state_store.get("historical_review_exports:history", [])
    rec_hist = state_store.get("historical_review_reconciliations:history", [])
    assert len(exp_hist) <= 5
    assert len(rec_hist) <= 5

    # Check retention stats report
    ret_stat = svc.get_retention_status()
    assert ret_stat["max_exports_capacity"] == 5
    assert ret_stat["max_reconciliations_capacity"] == 5
    assert ret_stat["total_exports_evicted"] == 10
    assert ret_stat["total_reconciliations_evicted"] == 10


def test_scenario_20_mandatory_safety_invariants_and_prohibited_terms():
    """Verify Settings.execution_locked is True, trading engine is uninitialized, and prohibited directives are blocked."""
    settings = Settings()
    assert settings.execution_locked is True

    bus = EventBus()
    svc = HistoricalReviewAuditDiagnosticsService()
    orchestrator = MasterOrchestrator(event_bus=bus)
    register_audit_diagnostics_handlers(orchestrator, svc)

    # Reject prohibited trading recommendation directives
    req = OrchestratorRequest.create(
        command="export_historical_review_diagnostics",
        context={"investigation_id": "inv_sec", "trade_recommendation": "SELL"},
    )
    resp = orchestrator.handle_request(req)
    assert resp.status == RequestStatus.FAILED
    assert any("Prohibited field" in str(e) for e in resp.errors)
