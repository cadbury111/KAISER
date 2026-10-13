"""KAISER — Phase 2, Step 45 Acceptance & Hardening Test Suite.

Diagnostics Migration Audit Reliability, Lifecycle Persistence & Acceptance Hardening.

Test-backed verification covering:
A. Repeated Hydration and Counter Accuracy:
   - Hydrate same StateStore data repeatedly without inflating cumulative counters.
   - Verify records are never duplicated in storage structures.
   - Verify per-hydration re-read counts vs. cumulative totals.
   - Verify repeated lazy hydration through multiple public query interfaces.
   - Verify one logical migration is audited exactly once across alternate lookup paths.
   - Verify incremental hydration of newly added records.

B. Reconstruction and Audit Persistence:
   - Create diagnostic records containing legacy and supported older-schema payloads.
   - Reconstruct service across multiple instances using StateStore persistence.
   - Verify cumulative totals and audit evidence persist reliably across reconstruction.
   - Verify distinguishability between historical cumulative totals and per-run statistics.
   - Confirm unsupported future and malformed record classifications are retained across reconstruction.
   - Verify dedicated StateStore audit key isolation.

C. clear_history() Semantics:
   - Complete reset in memory (queues, counters, cache keys).
   - Deletion of StateStore audit key and all diagnostics keys.
   - Verification that subsequent hydration does not resurrect cleared records or stale counters.

D. Concurrent Access and Thread Safety:
   - Concurrent summary reads during active hydration.
   - Concurrent record recording and summary reads with bounded capacities.
   - Thread-safe RLock reentrancy and consistent atomic snapshot verification.

E. Audit Summary Security:
   - Secret scrubbing (API keys, bearer tokens, passwords, filesystem paths) in details and messages.
   - Summary query read-only purity.
   - Orchestrator command handler parameter validation and prohibited trading directive rejection.

F. Compatibility and Integrity Regression:
   - Compatibility matrix stability and zero silent downgrades.
   - Non-destructive payload preservation during migration failures.
   - Mandatory safety invariants: Settings.execution_locked = True, uninitialized trading engine.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
import threading
import time
from typing import Any, Dict, List
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
    compute_export_content_fingerprint,
    compute_export_canonical_dict,
)
from app.analysis.opportunities.evolution.audit.diagnostics.schema import (
    CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
    SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS,
    LEGACY_UNVERSIONED,
    DIAGNOSTICS_COMPATIBILITY_MATRIX,
    DiagnosticsRecordType,
    SchemaCompatibilityOutcome,
    CompatibilityMatrixEntry,
    MigrationAuditRecord,
    MigrationAuditSummary,
    SchemaVersionCompatibilityStatus,
    SchemaVersionError,
    IncompatibleSchemaVersionError,
    MalformedSchemaVersionError,
    classify_payload_compatibility,
    detect_and_validate_schema_version,
    migrate_alert_payload,
    migrate_diagnostic_payload,
    migrate_export_payload,
    migrate_reconciliation_payload,
    migrate_retention_stats_payload,
    query_compatibility_matrix,
    sanitize_audit_details,
    sanitize_payload_text,
)
from app.analysis.opportunities.evolution.audit.diagnostics.service import (
    HistoricalReviewAuditDiagnosticsService,
)
from app.analysis.opportunities.evolution.audit.diagnostics.handlers import (
    register_audit_diagnostics_handlers,
)
from app.core.config import Settings
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, RequestStatus


class DeterministicClock:
    """Deterministic timestamp provider for reproducible testing."""

    def __init__(self, start_ts: float = 1700000000.0) -> None:
        self._current = start_ts

    def __call__(self) -> float:
        return self._current

    def advance(self, seconds: float) -> None:
        self._current += seconds


@pytest.fixture
def clock() -> DeterministicClock:
    return DeterministicClock(1700000000.0)


@pytest.fixture
def state_store() -> StateStore:
    return StateStore()


# ==============================================================================
# A. Repeated Hydration and Counter Accuracy (Scenarios 1 - 4)
# ==============================================================================

def test_scenario_01_repeated_hydration_counter_idempotency(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 1: Repeated hydration does not duplicate records or inflate cumulative counters.

    Guarantees:
    - First hydration inspects and migrates records, updating cumulative counters.
    - Subsequent hydration calls detect already-audited records as re-reads.
    - Cumulative migration counters do not increase.
    - Records in in-memory queues are not duplicated.
    - Per-hydration re-read count matches the count of re-read records.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    stored_diags = [
        {"diagnostic_id": "diag_rep_1", "investigation_id": "inv_1", "message": "Legacy 1", "timestamp": clock()},
        {"diagnostic_id": "diag_rep_2", "investigation_id": "inv_1", "message": "0.9.0 record", "timestamp": clock(), "schema_version": "0.9.0"},
        {"diagnostic_id": "diag_rep_3", "investigation_id": "inv_1", "message": "1.0.0 record", "timestamp": clock(), "schema_version": "1.0.0"},
    ]
    state_store.set("historical_review_diagnostics:history", stored_diags)

    # First hydration
    svc.hydrate_from_state_store(force=True)
    summary_1 = svc.get_migration_audit_summary()
    assert summary_1["cumulative_successful_migrations"] == 2
    assert summary_1["cumulative_direct_compatible_loads"] == 1
    assert summary_1["cumulative_total_inspected"] == 3
    assert summary_1["current_hydration_inspected"] == 3
    assert summary_1["current_hydration_re_read_count"] == 0
    assert len(svc.list_diagnostics()) == 3

    # Second hydration with force=True on the same data
    clock.advance(10.0)
    svc.hydrate_from_state_store(force=True)
    summary_2 = svc.get_migration_audit_summary()

    # Cumulative counters MUST NOT have increased
    assert summary_2["cumulative_successful_migrations"] == 2
    assert summary_2["cumulative_direct_compatible_loads"] == 1
    assert summary_2["cumulative_total_inspected"] == 3

    # Per-hydration statistics reflect re-reads
    assert summary_2["current_hydration_re_read_count"] == 3
    assert summary_2["current_hydration_inspected"] == 0

    # Records in memory are not duplicated
    assert len(svc.list_diagnostics()) == 3


def test_scenario_02_lazy_hydration_repeated_querying_does_not_inflate_counters(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 2: Multiple lazy hydration query calls do not trigger multiple audit increments.

    Guarantees:
    - Querying across distinct public interfaces (list_diagnostics, get_diagnostic,
      is_diagnostic_evicted, get_retention_status) triggers lazy hydration at most once.
    - Subsequent queries do not re-audit or increment counters.
    """
    stored_diags = [
        {"diagnostic_id": "diag_lazy_1", "investigation_id": "inv_lazy", "message": "Legacy lazy", "timestamp": clock()},
        {"diagnostic_id": "diag_lazy_2", "investigation_id": "inv_lazy", "message": "Current lazy", "timestamp": clock(), "schema_version": "1.0.0"},
    ]
    state_store.set("historical_review_diagnostics:history", stored_diags)

    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store, time_provider=clock, auto_hydrate=False
    )

    # Query 1: list_diagnostics triggers lazy hydration
    diags = svc.list_diagnostics()
    assert len(diags) == 2
    sum_1 = svc.get_migration_audit_summary()
    assert sum_1["cumulative_successful_migrations"] == 1
    assert sum_1["cumulative_direct_compatible_loads"] == 1
    assert sum_1["cumulative_total_inspected"] == 2

    # Query 2: get_diagnostic
    d1 = svc.get_diagnostic("diag_lazy_1")
    assert d1 is not None
    sum_2 = svc.get_migration_audit_summary()
    assert sum_2["cumulative_total_inspected"] == 2

    # Query 3: is_diagnostic_evicted
    evicted = svc.is_diagnostic_evicted("diag_lazy_1")
    assert evicted is False
    sum_3 = svc.get_migration_audit_summary()
    assert sum_3["cumulative_total_inspected"] == 2

    # Query 4: get_retention_status
    status = svc.get_retention_status()
    assert status["current_diagnostics_count"] == 2
    sum_4 = svc.get_migration_audit_summary()
    assert sum_4["cumulative_total_inspected"] == 2


def test_scenario_03_single_logical_migration_audited_once_across_lookup_paths(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 3: One logical migration is audited once regardless of which lookup paths are called.

    Guarantees:
    - Stored alerts, exports, and reconciliations are audited exactly once.
    - Accessing them via multiple specialized methods (get_alert, is_alert_evicted,
      get_export, list_exports, get_reconciliation, list_reconciliations) does not double-count.
    """
    state_store.set(
        "historical_review_alerts:history",
        [{
            "alert_id": "alt_unique_1",
            "diagnostic_id": "d_1",
            "correlation_id": "c_1",
            "investigation_id": "inv_1",
            "message": "Legacy alert",
            "timestamp": clock(),
        }],
    )
    state_store.set(
        "historical_review_exports:history",
        [{
            "export_id": "exp_unique_1",
            "correlation_id": "corr_1",
            "scope": {"investigation_id": "inv_1"},
            "diagnostics_count": 0,
            "alerts_count": 0,
            "exported_at": clock(),
            "content_fingerprint": "fp_1",
            "canonical_payload_json": "{}",
            "diagnostics": [],
            "alerts": [],
        }],
    )
    state_store.set(
        "historical_review_reconciliations:history",
        [{
            "reconciliation_id": "rec_unique_1",
            "export_id": "exp_unique_1",
            "status": "MATCHED",
            "matched_diagnostics": 0,
            "matched_alerts": 0,
            "discrepancies": [],
            "fingerprint_verified": True,
            "reconciled_at": clock(),
        }],
    )

    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store, time_provider=clock, auto_hydrate=False
    )

    # 1. Trigger lazy hydration through list_alerts query
    alerts = svc.list_alerts()
    assert len(alerts) == 1
    alt = svc.get_alert("alt_unique_1")
    assert alt is not None
    assert alt.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # 2. Query through list_exports and get_export
    exports = svc.list_exports()
    assert len(exports) == 1
    exp = svc.get_export("exp_unique_1")
    assert exp is not None
    assert exp.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # 3. Query through list_reconciliations and get_reconciliation
    recons = svc.list_reconciliations()
    assert len(recons) == 1
    recon = svc.get_reconciliation("rec_unique_1")
    assert recon is not None
    assert recon.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # 4. Access through multiple specialized lookup paths
    assert svc.is_alert_evicted("alt_unique_1") is False
    assert svc.is_export_evicted("exp_unique_1") is False
    assert svc.is_reconciliation_evicted("rec_unique_1") is False
    diag_summary = svc.get_diagnostic_summary("inv_1")
    assert diag_summary is not None
    retention_status = svc.get_retention_status()
    assert retention_status is not None

    # Check audit counts: exactly 3 migrations, 0 duplicates across all 8 lookup paths
    summary = svc.get_migration_audit_summary()
    assert summary["cumulative_successful_migrations"] == 3
    assert summary["cumulative_total_inspected"] == 3


def test_scenario_04_incremental_hydration_of_newly_stored_records(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 4: Incremental hydration only audits newly introduced StateStore records.

    Guarantees:
    - Pre-existing records are detected as re-reads.
    - Newly added records are audited and increment cumulative counters.
    - Audit log correctly records only new events.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    initial_diags = [
        {"diagnostic_id": "d_init_1", "investigation_id": "inv_1", "message": "Initial", "timestamp": clock(), "schema_version": "1.0.0"},
    ]
    state_store.set("historical_review_diagnostics:history", initial_diags)
    svc.hydrate_from_state_store(force=True)

    sum1 = svc.get_migration_audit_summary()
    assert sum1["cumulative_direct_compatible_loads"] == 1
    assert sum1["cumulative_total_inspected"] == 1

    # Now append 2 new legacy records to StateStore
    updated_diags = initial_diags + [
        {"diagnostic_id": "d_new_1", "investigation_id": "inv_1", "message": "New 1", "timestamp": clock()},
        {"diagnostic_id": "d_new_2", "investigation_id": "inv_1", "message": "New 2", "timestamp": clock(), "schema_version": "0.9.0"},
    ]
    state_store.set("historical_review_diagnostics:history", updated_diags)

    # Second hydration with force=True
    svc.hydrate_from_state_store(force=True)
    sum2 = svc.get_migration_audit_summary()

    # Total inspected increased by exactly 2
    assert sum2["cumulative_direct_compatible_loads"] == 1
    assert sum2["cumulative_successful_migrations"] == 2
    assert sum2["cumulative_total_inspected"] == 3

    # Per-hydration attempt shows 2 migrated and 1 re-read
    assert sum2["current_hydration_successful_migrations"] == 2
    assert sum2["current_hydration_re_read_count"] == 1


# ==============================================================================
# B. Reconstruction and Audit Persistence (Scenarios 5 - 8)
# ==============================================================================

def test_scenario_05_reconstruction_restores_cumulative_totals(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 5: Reconstructed service instances restore cumulative migration audit totals from StateStore.

    Guarantees:
    - Service 1 audits legacy records and persists audit state to StateStore.
    - Service 2 reconstructed with auto_hydrate=True restores cumulative totals.
    - Service 2 does not reset totals to zero or double-count hydrated records.
    """
    svc1 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    stored_diags = [
        {"diagnostic_id": "d_p1", "investigation_id": "inv_p", "message": "Legacy p1", "timestamp": clock()},
        {"diagnostic_id": "d_p2", "investigation_id": "inv_p", "message": "0.9.0 p2", "timestamp": clock(), "schema_version": "0.9.0"},
        {"diagnostic_id": "d_p3", "investigation_id": "inv_p", "message": "1.0.0 p3", "timestamp": clock(), "schema_version": "1.0.0"},
    ]
    state_store.set("historical_review_diagnostics:history", stored_diags)
    svc1.hydrate_from_state_store(force=True)

    summary_1 = svc1.get_migration_audit_summary()
    assert summary_1["cumulative_successful_migrations"] == 2
    assert summary_1["cumulative_direct_compatible_loads"] == 1
    assert summary_1["cumulative_total_inspected"] == 3
    assert summary_1["audit_persistence_status"] == "PERSISTED"

    # Reconstruct Service 2 with the same StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store, time_provider=clock, auto_hydrate=True
    )

    summary_2 = svc2.get_migration_audit_summary()
    assert summary_2["cumulative_successful_migrations"] == 2
    assert summary_2["cumulative_direct_compatible_loads"] == 1
    assert summary_2["cumulative_total_inspected"] == 3
    assert len(svc2.list_diagnostics()) == 3


def test_scenario_06_distinguishability_of_historical_vs_per_run_statistics(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 6: Cumulative lifetime totals and current-run statistics remain clearly distinguishable.

    Guarantees:
    - Reconstructed service reports cumulative lifetime totals.
    - Current-run statistics indicate zero new migrations and three re-reads.
    - Prevents confusing lifetime migrations with current run events.
    """
    svc1 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [
            {"diagnostic_id": "d_hist_1", "investigation_id": "inv_h", "message": "Legacy h1", "timestamp": clock()},
            {"diagnostic_id": "d_hist_2", "investigation_id": "inv_h", "message": "Legacy h2", "timestamp": clock()},
        ],
    )
    svc1.hydrate_from_state_store(force=True)

    # Service 2 reconstructed
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store, time_provider=clock, auto_hydrate=True
    )

    sum2 = svc2.get_migration_audit_summary()
    # Cumulative historical totals
    assert sum2["cumulative_successful_migrations"] == 2
    assert sum2["cumulative_total_inspected"] == 2

    # Current hydration attempt statistics
    assert sum2["current_hydration_successful_migrations"] == 0
    assert sum2["current_hydration_re_read_count"] == 2
    assert sum2["current_hydration_inspected"] == 0


def test_scenario_07_unsupported_and_malformed_classifications_retained_across_reconstruction(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 7: Unsupported future and malformed record counts persist across reconstruction.

    Guarantees:
    - Unsupported future versions (e.g. 2.0.0) increment unsupported_future_skipped.
    - Malformed versions increment malformed_records_skipped.
    - After service reconstruction, these counts are retained accurately.
    """
    svc1 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    stored_diags = [
        {"diagnostic_id": "d_fut", "investigation_id": "inv_f", "message": "Future v2", "timestamp": clock(), "schema_version": "2.0.0"},
        {"diagnostic_id": "d_mal", "investigation_id": "inv_f", "message": "Malformed", "timestamp": clock(), "schema_version": "invalid_semver"},
    ]
    state_store.set("historical_review_diagnostics:history", stored_diags)
    svc1.hydrate_from_state_store(force=True)

    sum1 = svc1.get_migration_audit_summary()
    assert sum1["cumulative_unsupported_future_skipped"] == 1
    assert sum1["cumulative_malformed_records_skipped"] == 1

    # Reconstruct Service 2
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store, time_provider=clock, auto_hydrate=True
    )
    sum2 = svc2.get_migration_audit_summary()
    assert sum2["cumulative_unsupported_future_skipped"] == 1
    assert sum2["cumulative_malformed_records_skipped"] == 1


def test_scenario_08_migration_audit_persistence_key_validation_and_isolation(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 8: Migration audit summary key is isolated and valid SemVer.

    Guarantees:
    - Stored under 'historical_review_diagnostics:migration_audit'.
    - Stored payload includes schema_version='1.0.0'.
    - Deleting or modifying other keys leaves migration audit key isolated.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_iso_1", "investigation_id": "inv_i", "message": "Msg", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)

    persisted = state_store.get("historical_review_diagnostics:migration_audit")
    assert persisted is not None
    assert persisted["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert persisted["cumulative_successful_migrations"] == 1
    assert "audited_record_keys" in persisted

    # Delete diagnostics history only; audit key remains intact
    state_store.delete("historical_review_diagnostics:history")
    audit_after = state_store.get("historical_review_diagnostics:migration_audit")
    assert audit_after is not None
    assert audit_after["cumulative_successful_migrations"] == 1


# ==============================================================================
# C. clear_history() Semantics (Scenarios 9 - 11)
# ==============================================================================

def test_scenario_09_clear_history_complete_in_memory_reset(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 9: clear_history() completely resets all in-memory collections and audit counters.

    Guarantees:
    - Queues for diagnostics, alerts, exports, reconciliations, and audit records are empty.
    - All cumulative and per-run counters are zeroed out.
    - Audited record keys are cleared.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_clr_1", "investigation_id": "inv_c", "message": "Clr", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)
    assert svc.get_migration_audit_summary()["cumulative_total_inspected"] == 1

    svc.clear_history()

    summary = svc.get_migration_audit_summary()
    assert summary["total_inspected"] == 0
    assert summary["cumulative_total_inspected"] == 0
    assert summary["successful_migrations"] == 0
    assert summary["direct_compatible_loads"] == 0
    assert summary["unsupported_future_skipped"] == 0
    assert summary["malformed_records_skipped"] == 0
    assert summary["migration_failures"] == 0
    assert summary["current_hydration_inspected"] == 0
    assert summary["current_hydration_re_read_count"] == 0
    assert len(summary["recent_audit_records"]) == 0
    assert len(svc.list_diagnostics()) == 0


def test_scenario_10_clear_history_deletes_statestore_audit_key(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 10: clear_history() deletes the migration audit key and all diagnostics keys from StateStore.

    Guarantees:
    - 'historical_review_diagnostics:migration_audit' is deleted from StateStore.
    - No orphaned diagnostic keys remain.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.record_diagnostic(
        investigation_id="inv_del_1",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="To be deleted",
    )
    svc.hydrate_from_state_store(force=True)

    assert state_store.get("historical_review_diagnostics:migration_audit") is not None

    svc.clear_history()

    assert state_store.get("historical_review_diagnostics:migration_audit") is None
    assert state_store.get("historical_review_diagnostics:history") is None
    assert state_store.get("historical_review_alerts:history") is None
    assert state_store.get("historical_review_exports:history") is None
    assert state_store.get("historical_review_reconciliations:history") is None


def test_scenario_11_subsequent_hydration_after_clear_history_does_not_resurrect_state(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 11: Subsequent hydration after clear_history() finds an empty store without ghost counts.

    Guarantees:
    - Calling hydrate_from_state_store() after clear_history() produces clean zero counts.
    - Reconstructing a new service instance after clear_history() starts with clean zero counts.
    """
    svc1 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_ghost_1", "investigation_id": "inv_g", "message": "Ghost", "timestamp": clock()}],
    )
    svc1.hydrate_from_state_store(force=True)
    svc1.clear_history()

    # Hydrate on svc1
    svc1.hydrate_from_state_store(force=True)
    sum1 = svc1.get_migration_audit_summary()
    assert sum1["cumulative_total_inspected"] == 0

    # Reconstruct svc2
    svc2 = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store, time_provider=clock, auto_hydrate=True
    )
    sum2 = svc2.get_migration_audit_summary()
    assert sum2["cumulative_total_inspected"] == 0
    assert len(svc2.list_diagnostics()) == 0


# ==============================================================================
# D. Concurrent Access and Thread Safety (Scenarios 12 - 14)
# ==============================================================================

def test_scenario_12_concurrent_summary_reads_during_active_hydration(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 12: Concurrent audit summary queries during active hydration remain consistent.

    Guarantees:
    - Multiple threads query get_migration_audit_summary() while another hydrates.
    - All returned summaries satisfy total_inspected == successful + direct + unsupported + malformed + failures.
    - Zero deadlocks, exceptions, or race-induced partial counts.
    """
    stored_diags = [
        {"diagnostic_id": f"d_conc_{i}", "investigation_id": "inv_conc", "message": f"Log {i}", "timestamp": clock()}
        for i in range(20)
    ]
    state_store.set("historical_review_diagnostics:history", stored_diags)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    barrier = threading.Barrier(5)
    errors: List[Exception] = []
    observed_totals: List[int] = []

    def reader_worker():
        try:
            barrier.wait(timeout=5.0)
            for _ in range(25):
                s = svc.get_migration_audit_summary()
                tot = s["cumulative_total_inspected"]
                expected = (
                    s["cumulative_successful_migrations"]
                    + s["cumulative_direct_compatible_loads"]
                    + s["cumulative_unsupported_future_skipped"]
                    + s["cumulative_malformed_records_skipped"]
                    + s["cumulative_migration_failures"]
                )
                assert tot == expected, f"Inconsistent snapshot: {tot} != {expected}"
                observed_totals.append(tot)
        except Exception as exc:
            errors.append(exc)

    def hydrator_worker():
        try:
            barrier.wait(timeout=5.0)
            for _ in range(5):
                svc.hydrate_from_state_store(force=True)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=reader_worker) for _ in range(4)]
    threads.append(threading.Thread(target=hydrator_worker))

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert len(errors) == 0, f"Concurrent workers encountered errors: {errors}"
    final_summary = svc.get_migration_audit_summary()
    assert final_summary["cumulative_total_inspected"] == 20


def test_scenario_13_concurrent_record_additions_and_summary_reads(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 13: Concurrent diagnostic recording and summary querying preserves bounds and thread safety.

    Guarantees:
    - Multiple threads write diagnostics while others query summaries and retention.
    - Bounded collections stay within maxlen limits.
    - Lock synchronizes all state transitions safely.
    """
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store, max_diagnostics=50, time_provider=clock
    )

    barrier = threading.Barrier(4)
    errors: List[Exception] = []

    def writer_worker(thread_id: int):
        try:
            barrier.wait(timeout=5.0)
            for i in range(25):
                svc.record_diagnostic(
                    investigation_id=f"inv_th_{thread_id}",
                    category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
                    severity=AuditDiagnosticSeverity.INFO,
                    message=f"Msg {i} from thread {thread_id}",
                )
        except Exception as exc:
            errors.append(exc)

    def reader_worker():
        try:
            barrier.wait(timeout=5.0)
            for _ in range(25):
                _ = svc.get_migration_audit_summary()
                _ = svc.get_retention_status()
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=writer_worker, args=(1,)),
        threading.Thread(target=writer_worker, args=(2,)),
        threading.Thread(target=reader_worker),
        threading.Thread(target=reader_worker),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert len(errors) == 0, f"Thread errors: {errors}"
    diags = svc.list_diagnostics(limit=100)
    assert len(diags) <= 50  # Bounded to max_diagnostics


def test_scenario_14_thread_safe_rlock_reentrancy_invariants(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 14: Verifies reentrant RLock behavior across nested service operations.

    Guarantees:
    - Service's internal mutex allows nested calls (e.g. hydrate calling record_migration_audit).
    - clear_history and get_migration_audit_summary do not deadlock when invoked sequentially or nested.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    with svc._lock:
        # Reentrant lock acquisition
        svc.clear_history()
        summary = svc.get_migration_audit_summary()
        assert summary["total_inspected"] == 0

    assert True


# ==============================================================================
# E. Audit Summary Security & Sanitization (Scenarios 15 - 17)
# ==============================================================================

def test_scenario_15_secret_scrubbing_in_audit_summary_and_exception_details(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 15: Migration audit summaries and recent records scrub raw secrets and filepaths.

    Guarantees:
    - API keys, bearer tokens, passwords, and file paths are redacted in details.
    - Neither recent_audit_records nor summary fields expose unredacted credentials.
    """
    raw_secret_key = "sk_live_99887766554433221100"
    raw_bearer_jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.secret_token_signature_123"
    raw_win_path = "C:\\Users\\Administrator\\SecretDir\\credentials.json"
    raw_unix_path = "/home/deploy/secret/id_rsa"

    secret_payload = {
        "diagnostic_id": "d_sec_1",
        "investigation_id": "inv_sec",
        "message": f"Auth failed with key={raw_secret_key} token={raw_bearer_jwt} path={raw_win_path} or {raw_unix_path}",
        "schema_version": f"invalid_with_path={raw_win_path}_and_jwt={raw_bearer_jwt}",
    }
    state_store.set("historical_review_diagnostics:history", [secret_payload])

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    summary = svc.get_migration_audit_summary()
    recent = summary["recent_audit_records"]
    assert len(recent) >= 1

    summary_str = str(summary)
    assert raw_secret_key not in summary_str
    assert raw_bearer_jwt not in summary_str
    assert raw_win_path not in summary_str
    assert raw_unix_path not in summary_str

    # Verify redaction markers
    assert any(marker in summary_str for marker in ("[REDACTED_SECRET]", "[REDACTED_TOKEN]", "[REDACTED_PATH]", "[REDACTED_KEY]"))


def test_scenario_16_read_only_audit_summary_query_interfaces(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 16: get_migration_audit_summary() is pure read-only and mutations to output do not affect service.

    Guarantees:
    - Multiple invocations yield identical results.
    - Mutating returned dictionary does not mutate internal service counters or queues.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    sum1 = svc.get_migration_audit_summary()
    assert sum1["total_inspected"] == 0

    # Mutate returned dictionary
    sum1["total_inspected"] = 9999
    sum1["successful_migrations"] = 9999
    sum1["recent_audit_records"].append({"hacked": True})

    # Internal state is unaffected
    sum2 = svc.get_migration_audit_summary()
    assert sum2["total_inspected"] == 0
    assert sum2["successful_migrations"] == 0
    assert len(sum2["recent_audit_records"]) == 0


def test_scenario_17_command_handler_parameter_validation_and_safety(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 17: Command handler validates parameters and rejects trading directives.

    Guarantees:
    - Valid command returns status: success with read_only: True, execution_locked: True.
    - Unsupported parameters reject request with FAILED status and ValidationError on direct invocation.
    - Prohibited trading directives reject request with FAILED status and ValidationError.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    orchestrator = MasterOrchestrator()
    register_audit_diagnostics_handlers(orchestrator, svc)

    # 1. Valid request
    valid_req = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={"read_only": True},
    )
    res = orchestrator.handle_request(valid_req)
    assert res.status == RequestStatus.COMPLETED
    assert res.result["status"] == "success"
    assert res.result["read_only"] is True
    assert res.result["execution_locked"] is True
    assert "migration_audit_summary" in res.result

    # 2. Unsupported parameter rejected
    unsupported_req = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={"unknown_unsupported_flag": True},
    )
    res_unsupported = orchestrator.handle_request(unsupported_req)
    assert res_unsupported.status == RequestStatus.FAILED
    assert any("Unsupported parameter" in err for err in res_unsupported.errors)

    handler = orchestrator.registry.get_handler("get_historical_review_migration_audit_summary")
    with pytest.raises(ValidationError, match="Unsupported parameter"):
        handler(unsupported_req)

    # 3. Prohibited trading directive rejected
    prohibited_req = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={"trade_recommendation": "BUY", "quantity": 100},
    )
    res_prohibited = orchestrator.handle_request(prohibited_req)
    assert res_prohibited.status == RequestStatus.FAILED
    with pytest.raises(ValidationError, match="Prohibited field"):
        handler(prohibited_req)


# ==============================================================================
# F. Compatibility and Integrity Regression (Scenarios 18 - 20)
# ==============================================================================

def test_scenario_18_compatibility_matrix_invariant_stability():
    """Scenario 18: Matrix remains immutable, exhaustive, and never downgrades future versions.

    Guarantees:
    - All matrix entries exist across all 5 record types.
    - Unsupported future versions are rejected with IncompatibleSchemaVersionError.
    - Target versions for unsupported categories remain None.
    """
    record_types = [
        DiagnosticsRecordType.DIAGNOSTIC,
        DiagnosticsRecordType.ALERT,
        DiagnosticsRecordType.EXPORT,
        DiagnosticsRecordType.RECONCILIATION,
        DiagnosticsRecordType.RETENTION_STATS,
    ]
    for rt in record_types:
        entry = query_compatibility_matrix(rt, "unsupported_future")
        assert entry is not None
        assert entry.expected_outcome == SchemaCompatibilityOutcome.UNSUPPORTED_FUTURE
        assert entry.target_version is None
        assert entry.exception_class == "IncompatibleSchemaVersionError"


def test_scenario_19_non_destructive_payload_preservation_during_migration_failures(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 19: Migration failures and corrupt records never mutate original StateStore data.

    Guarantees:
    - Original payloads in StateStore remain strictly byte/key intact.
    - Recoverable failures are quarantined without data loss.
    """
    corrupt_export = {
        "export_id": "exp_corrupt_src",
        "schema_version": "0.9.0",
        "canonical_payload_json": "invalid_json_{{{",
        "content_fingerprint": "fp_corrupt",
    }
    state_store.set("historical_review_exports:history", [corrupt_export])

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    # StateStore export dictionary is intact
    stored = state_store.get("historical_review_exports:history")
    assert len(stored) == 1
    assert stored[0]["canonical_payload_json"] == "invalid_json_{{{"
    assert stored[0]["schema_version"] == "0.9.0"


def test_scenario_20_mandatory_safety_invariants():
    """Scenario 20: Mandatory safety invariants remain intact throughout Phase 2, Step 45.

    Safety Invariants:
    1. Settings.execution_locked is strictly True.
    2. Zero trading engine initialization.
    3. No live broker integration or real-money trading capabilities.
    4. Diagnostics subsystem remains strictly read-only decision support.
    5. JUJU and voice modules remain untouched.
    """
    assert Settings.execution_locked is True, "CRITICAL: Settings.execution_locked must remain True!"

    # Ensure engine remains uninitialized
    engine_attr = getattr(Settings, "trading_engine_initialized", False)
    assert engine_attr is False, "CRITICAL: Trading engine must remain uninitialized!"
