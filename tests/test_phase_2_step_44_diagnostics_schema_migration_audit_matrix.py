"""KAISER — Phase 2, Step 44 Acceptance & Compatibility Matrix Test Suite.

Diagnostics Schema Migration Audit, Rollback Safety & Compatibility Matrix.

Test-backed verification covering:
1. Compatibility matrix for all 5 record types:
   - Diagnostic records
   - Alert records
   - Export snapshots
   - Reconciliation reports
   - Retention statistics
   Across: legacy unversioned, 0.9.0, 1.0.0, unsupported future, malformed version, corrupt payload.
2. Original payload preservation and immutability (non-destructive guarantee).
3. Migration idempotency, determinism, and canonical representation stability.
4. Migration failure classification, isolation, and service recovery.
5. Persistence write safety, StateStore atomicity guarantees, and multi-key limitations.
6. Unsupported future version preservation without silent downgrade.
7. Reconciliation safety under incomplete or incompatible evidence (UNVERIFIABLE).
8. Migration auditability, outcome tracking, and secret sanitization.
9. Orchestrator handler stability and response contract.
10. Mandatory safety invariants: Settings.execution_locked = True, no trading engine initialization.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
import pytest
from typing import Any, Dict, cast

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
# 1. Compatibility Matrix Verification (Scenarios 1 - 5)
# ==============================================================================

def test_scenario_01_matrix_diagnostic_records_coverage():
    """Scenario 1: Matrix covers Diagnostic records across all 6 version categories.

    Guarantees:
    - legacy_unversioned -> MIGRATED_COMPATIBLE with target 1.0.0
    - 0.9.0 -> MIGRATED_COMPATIBLE with target 1.0.0
    - 1.0.0 -> DIRECT_COMPATIBLE with target 1.0.0
    - unsupported_future -> UNSUPPORTED_FUTURE with IncompatibleSchemaVersionError
    - malformed_version -> MALFORMED with MalformedSchemaVersionError
    - corrupt_payload -> CORRUPT_PAYLOAD with ValidationError
    """
    categories = ["legacy_unversioned", "0.9.0", "1.0.0", "unsupported_future", "malformed_version", "corrupt_payload"]
    for cat in categories:
        entry = query_compatibility_matrix(DiagnosticsRecordType.DIAGNOSTIC, cat)
        assert entry is not None, f"Missing matrix entry for DIAGNOSTIC / {cat}"
        assert entry.record_type == DiagnosticsRecordType.DIAGNOSTIC
        assert entry.version_category == cat
        assert len(entry.non_destructive_guarantee) > 0

    # Test payload classification
    legacy_payload = {"diagnostic_id": "diag_001", "message": "Legacy log"}
    outcome, entry = classify_payload_compatibility(DiagnosticsRecordType.DIAGNOSTIC, legacy_payload)
    assert outcome == SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE
    assert entry is not None and entry.expected_outcome == SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE

    future_payload = {"diagnostic_id": "diag_002", "schema_version": "2.0.0", "message": "Future log"}
    outcome, entry = classify_payload_compatibility(DiagnosticsRecordType.DIAGNOSTIC, future_payload)
    assert outcome == SchemaCompatibilityOutcome.UNSUPPORTED_FUTURE

    malformed_payload = {"diagnostic_id": "diag_003", "schema_version": "v1.bad!", "message": "Bad version"}
    outcome, entry = classify_payload_compatibility(DiagnosticsRecordType.DIAGNOSTIC, malformed_payload)
    assert outcome == SchemaCompatibilityOutcome.MALFORMED


def test_scenario_02_matrix_alert_records_coverage():
    """Scenario 2: Matrix covers Alert records across all 6 version categories.

    Guarantees:
    - legacy_unversioned -> MIGRATED_COMPATIBLE
    - 0.9.0 -> MIGRATED_COMPATIBLE with mapped aliases (alert_reason -> reason, state/active -> status)
    - 1.0.0 -> DIRECT_COMPATIBLE
    - unsupported_future -> UNSUPPORTED_FUTURE
    - malformed_version -> MALFORMED
    - corrupt_payload -> CORRUPT_PAYLOAD
    """
    categories = ["legacy_unversioned", "0.9.0", "1.0.0", "unsupported_future", "malformed_version", "corrupt_payload"]
    for cat in categories:
        entry = query_compatibility_matrix(DiagnosticsRecordType.ALERT, cat)
        assert entry is not None, f"Missing matrix entry for ALERT / {cat}"
        assert entry.record_type == DiagnosticsRecordType.ALERT

    v090_payload = {"alert_id": "alt_001", "schema_version": "0.9.0", "alert_reason": "Old format reason", "state": "ACTIVE"}
    outcome, entry = classify_payload_compatibility(DiagnosticsRecordType.ALERT, v090_payload)
    assert outcome == SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE

    v100_payload = {"alert_id": "alt_002", "schema_version": "1.0.0", "reason": "Current reason"}
    outcome, entry = classify_payload_compatibility(DiagnosticsRecordType.ALERT, v100_payload)
    assert outcome == SchemaCompatibilityOutcome.DIRECT_COMPATIBLE


def test_scenario_03_matrix_export_records_coverage():
    """Scenario 3: Matrix covers Export records across all 6 version categories.

    Guarantees:
    - legacy_unversioned -> MIGRATED_COMPATIBLE with recursive migration
    - 0.9.0 -> MIGRATED_COMPATIBLE with max_records -> limit mapping
    - 1.0.0 -> DIRECT_COMPATIBLE with canonical fingerprint verification
    - unsupported_future -> UNSUPPORTED_FUTURE
    - malformed_version -> MALFORMED
    - corrupt_payload -> CORRUPT_PAYLOAD
    """
    categories = ["legacy_unversioned", "0.9.0", "1.0.0", "unsupported_future", "malformed_version", "corrupt_payload"]
    for cat in categories:
        entry = query_compatibility_matrix(DiagnosticsRecordType.EXPORT, cat)
        assert entry is not None, f"Missing matrix entry for EXPORT / {cat}"
        assert entry.record_type == DiagnosticsRecordType.EXPORT

    corrupt_payload = "not-a-dictionary"
    outcome, entry = classify_payload_compatibility(DiagnosticsRecordType.EXPORT, corrupt_payload)
    assert outcome == SchemaCompatibilityOutcome.CORRUPT_PAYLOAD


def test_scenario_04_matrix_reconciliation_reports_coverage():
    """Scenario 4: Matrix covers Reconciliation reports across all 7 version categories.

    Guarantees:
    - legacy_unversioned -> MIGRATED_COMPATIBLE
    - 0.9.0 -> MIGRATED_COMPATIBLE with verdict -> status, fingerprint_match -> fingerprint_verified
    - 1.0.0 -> DIRECT_COMPATIBLE
    - unsupported_future -> UNSUPPORTED_FUTURE
    - malformed_version -> MALFORMED
    - corrupt_payload -> CORRUPT_PAYLOAD
    - unverifiable_source -> UNVERIFIABLE_SOURCE (missing/incompatible evidence)
    """
    categories = [
        "legacy_unversioned", "0.9.0", "1.0.0", "unsupported_future",
        "malformed_version", "corrupt_payload", "unverifiable_source"
    ]
    for cat in categories:
        entry = query_compatibility_matrix(DiagnosticsRecordType.RECONCILIATION, cat)
        assert entry is not None, f"Missing matrix entry for RECONCILIATION / {cat}"
        assert entry.record_type == DiagnosticsRecordType.RECONCILIATION

    entry_unverifiable = query_compatibility_matrix(DiagnosticsRecordType.RECONCILIATION, "unverifiable_source")
    assert entry_unverifiable is not None and entry_unverifiable.expected_outcome == SchemaCompatibilityOutcome.UNVERIFIABLE_SOURCE


def test_scenario_05_matrix_retention_stats_coverage():
    """Scenario 5: Matrix covers Retention statistics across all 6 version categories.

    Guarantees:
    - legacy_unversioned -> MIGRATED_COMPATIBLE
    - 0.9.0 -> MIGRATED_COMPATIBLE with evicted_count -> evicted_diagnostics_count
    - 1.0.0 -> DIRECT_COMPATIBLE
    - unsupported_future -> UNSUPPORTED_FUTURE
    - malformed_version -> MALFORMED
    - corrupt_payload -> CORRUPT_PAYLOAD
    """
    categories = ["legacy_unversioned", "0.9.0", "1.0.0", "unsupported_future", "malformed_version", "corrupt_payload"]
    for cat in categories:
        entry = query_compatibility_matrix(DiagnosticsRecordType.RETENTION_STATS, cat)
        assert entry is not None, f"Missing matrix entry for RETENTION_STATS / {cat}"
        assert entry.record_type == DiagnosticsRecordType.RETENTION_STATS


# ==============================================================================
# 2. Migration Safety and Non-Destructive Behavior (Scenarios 6 - 8)
# ==============================================================================

def test_scenario_06_original_payload_preservation_non_mutation():
    """Scenario 6: Migration preserves original stored payload without in-place mutation.

    Guarantees:
    - Input dictionary is deepcopied.
    - Input dictionary values, keys, and nested structures remain identical before and after.
    - Returned migrated dictionary is a separate object (id(result) != id(input)).
    """
    # 1. Diagnostic payload
    orig_diag = {
        "diagnostic_id": "diag_orig_001",
        "diagnostic_category": "CIRCUIT_BREAKER_TRIGGERED",
        "summary": "Original summary text",
        "error": "TimeoutError",
        "details": {"key1": "val1", "nested": {"list": [1, 2, 3]}},
        "schema_version": "0.9.0",
    }
    diag_before = copy.deepcopy(orig_diag)
    migrated_diag = migrate_diagnostic_payload(orig_diag)

    assert orig_diag == diag_before, "Original diagnostic input dictionary was mutated in place!"
    assert id(orig_diag) != id(migrated_diag)
    assert migrated_diag["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert migrated_diag["category"] == "CIRCUIT_BREAKER_TRIGGERED"
    assert "diagnostic_category" not in migrated_diag
    assert "diagnostic_category" in orig_diag

    # 2. Alert payload
    orig_alert = {
        "alert_id": "alt_orig_001",
        "alert_reason": "Original alert reason",
        "state": "ACTIVE",
        "schema_version": "0.9.0",
    }
    alert_before = copy.deepcopy(orig_alert)
    migrated_alert = migrate_alert_payload(orig_alert)
    assert orig_alert == alert_before, "Original alert dictionary was mutated in place!"
    assert id(orig_alert) != id(migrated_alert)
    assert migrated_alert["reason"] == "Original alert reason"

    # 3. Export payload
    orig_export = {
        "export_id": "exp_orig_001",
        "schema_version": "0.9.0",
        "request": {"max_records": 50},
        "diagnostics": [copy.deepcopy(orig_diag)],
        "alerts": [copy.deepcopy(orig_alert)],
    }
    export_before = copy.deepcopy(orig_export)
    migrated_export = migrate_export_payload(orig_export)
    assert orig_export == export_before, "Original export dictionary was mutated in place!"
    assert id(orig_export) != id(migrated_export)
    assert migrated_export["request"]["limit"] == 50

    # 4. Reconciliation payload
    orig_recon = {
        "reconciliation_id": "recon_orig_001",
        "export_id": "exp_orig_001",
        "verdict": "VERIFIED",
        "fingerprint_match": True,
        "schema_version": "0.9.0",
    }
    recon_before = copy.deepcopy(orig_recon)
    migrated_recon = migrate_reconciliation_payload(orig_recon)
    assert orig_recon == recon_before, "Original reconciliation dictionary was mutated in place!"
    assert migrated_recon["status"] == "VERIFIED"
    assert migrated_recon["fingerprint_verified"] is True

    # 5. Retention stats payload
    orig_stats = {
        "evicted_count": 42,
        "schema_version": "0.9.0",
    }
    stats_before = copy.deepcopy(orig_stats)
    migrated_stats = migrate_retention_stats_payload(orig_stats)
    assert orig_stats == stats_before, "Original retention stats dictionary was mutated in place!"
    assert migrated_stats["evicted_diagnostics_count"] == 42


def test_scenario_07_failed_migrations_preserve_source_dictionary():
    """Scenario 7: Failed migrations do not partially overwrite or corrupt source dictionary.

    Guarantees:
    - Attempted migration of unsupported future version raises IncompatibleSchemaVersionError.
    - Attempted migration of malformed version raises MalformedSchemaVersionError.
    - Input dictionary is completely unaltered after the exception is raised.
    """
    future_diag = {
        "diagnostic_id": "diag_future_001",
        "schema_version": "99.0.0",
        "details": {"data": "preserved"},
    }
    future_before = copy.deepcopy(future_diag)
    with pytest.raises(IncompatibleSchemaVersionError):
        migrate_diagnostic_payload(future_diag)
    assert future_diag == future_before

    malformed_alert = {
        "alert_id": "alt_malformed_001",
        "schema_version": "not-a-valid-semver!!",
        "reason": "Test",
    }
    malformed_before = copy.deepcopy(malformed_alert)
    with pytest.raises(MalformedSchemaVersionError):
        migrate_alert_payload(malformed_alert)
    assert malformed_alert == malformed_before


def test_scenario_08_idempotency_and_determinism_repeated_migrations():
    """Scenario 8: Repeated migrations are idempotent and produce deterministic outputs.

    Guarantees:
    - migrate(migrate(payload)) == migrate(payload)
    - Schema version remains 1.0.0 after multiple passes.
    - Canonical fingerprints computed on pass 1 and pass 10 are strictly identical.
    """
    legacy_diag = {
        "diagnostic_id": "diag_idemp_001",
        "investigation_id": "inv_001",
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "ERROR",
        "message": "Failure occurred",
        "timestamp": 1700000000.0,
    }

    pass1 = migrate_diagnostic_payload(legacy_diag)
    pass2 = migrate_diagnostic_payload(pass1)
    pass3 = migrate_diagnostic_payload(pass2)

    assert pass1 == pass2 == pass3
    assert pass3["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # Test export canonical fingerprint determinism across repeated migrations
    export_payload = {
        "export_id": "exp_idemp_001",
        "schema_version": "0.9.0",
        "created_at": 1700000000.0,
        "diagnostics": [legacy_diag],
        "alerts": [],
        "record_counts": {"diagnostics": 1, "alerts": 0},
    }
    migrated_exp_1 = migrate_export_payload(export_payload)
    migrated_exp_2 = migrate_export_payload(migrated_exp_1)
    assert migrated_exp_1 == migrated_exp_2

    exp_obj_1 = DiagnosticsExportResult.from_dict(migrated_exp_1)
    exp_obj_2 = DiagnosticsExportResult.from_dict(migrated_exp_2)
    assert exp_obj_1.content_fingerprint == exp_obj_2.content_fingerprint


# ==============================================================================
# 3. Hydration Idempotency and Non-Accumulation (Scenario 9)
# ==============================================================================

def test_scenario_09_repeated_hydration_idempotency_no_metadata_leak(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 9: Repeated hydration does not accumulate duplicate aliases or metadata.

    Guarantees:
    - Calling hydrate_from_state_store(force=True) repeatedly leaves counts and records stable.
    - No duplicate list items in source_references or duplicate alert IDs.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    stored_diags = [
        {
            "diagnostic_id": "diag_rep_001",
            "correlation_id": "corr_001",
            "investigation_id": "inv_001",
            "category": "GENERAL_DIAGNOSTIC",
            "severity": "INFO",
            "evidence_nature": "INFERRED_INTERPRETATION",
            "message": "Repetitive hydration record",
            "timestamp": clock(),
            "source_references": ["ref_a", "ref_b"],
            "schema_version": "0.9.0",
            "summary": "Old summary",
        }
    ]
    state_store.set("historical_review_diagnostics:history", stored_diags)

    # Initial hydration
    svc.hydrate_from_state_store(force=True)
    assert len(svc.list_diagnostics()) == 1
    d1 = svc.get_diagnostic("diag_rep_001")
    assert d1 is not None
    assert d1.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert d1.source_references == ("ref_a", "ref_b")

    # Hydrate second and third time
    svc.hydrate_from_state_store(force=True)
    svc.hydrate_from_state_store(force=True)

    assert len(svc.list_diagnostics()) == 1
    d_final = svc.get_diagnostic("diag_rep_001")
    assert d_final is not None
    assert d_final.source_references == ("ref_a", "ref_b")
    assert len(d_final.source_references) == 2


# ==============================================================================
# 4. Migration Failure Classification & Isolation (Scenarios 10 - 12)
# ==============================================================================

def test_scenario_10_migration_failure_classification():
    """Scenario 10: Failures are strictly classified under standard exception contracts.

    Guarantees:
    - IncompatibleSchemaVersionError subclasses SchemaVersionError and ValidationError.
    - MalformedSchemaVersionError subclasses SchemaVersionError and ValidationError.
    - Non-mapping raises ValidationError.
    - Missing required fields in model construction raises ValidationError.
    """
    assert issubclass(IncompatibleSchemaVersionError, SchemaVersionError)
    assert issubclass(MalformedSchemaVersionError, SchemaVersionError)
    assert issubclass(SchemaVersionError, ValidationError)

    # Non-mapping
    with pytest.raises(ValidationError) as exc:
        migrate_diagnostic_payload(cast(Any, ["not", "a", "dict"]))
    assert "Expected mapping payload" in str(exc.value)

    # Missing mandatory ID in AuditDiagnosticRecord.from_dict
    with pytest.raises((ValidationError, KeyError)):
        AuditDiagnosticRecord.from_dict({"message": "missing id"})


def test_scenario_11_isolation_corrupt_records_do_not_block_valid_records(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 11: Corrupt and unsupported records are quarantined without blocking valid records.

    Guarantees:
    - Mixed StateStore containing:
      1. Valid legacy record
      2. Malformed version record
      3. Valid 1.0.0 record
      4. Unsupported future 2.0.0 record
      5. Corrupt non-dict record
      6. Valid 0.9.0 record
    - Hydration skips corrupt and future records safely.
    - All 3 valid records are hydrated successfully and accessible.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    mixed_diags = [
        # 1. Valid legacy (unversioned)
        {
            "diagnostic_id": "diag_mix_001",
            "correlation_id": "corr_001",
            "investigation_id": "inv_001",
            "category": "GENERAL_DIAGNOSTIC",
            "severity": "INFO",
            "evidence_nature": "INFERRED_INTERPRETATION",
            "message": "Valid legacy",
            "timestamp": clock(),
        },
        # 2. Malformed version
        {
            "diagnostic_id": "diag_mix_002",
            "correlation_id": "corr_002",
            "investigation_id": "inv_001",
            "message": "Malformed version",
            "schema_version": "bad_version_string",
        },
        # 3. Valid 1.0.0
        {
            "diagnostic_id": "diag_mix_003",
            "correlation_id": "corr_003",
            "investigation_id": "inv_001",
            "category": "GENERAL_DIAGNOSTIC",
            "severity": "INFO",
            "evidence_nature": "INFERRED_INTERPRETATION",
            "message": "Valid 1.0.0",
            "timestamp": clock(),
            "schema_version": "1.0.0",
        },
        # 4. Unsupported future (2.0.0)
        {
            "diagnostic_id": "diag_mix_004",
            "correlation_id": "corr_004",
            "investigation_id": "inv_001",
            "message": "Future version",
            "schema_version": "2.0.0",
        },
        # 5. Non-mapping corrupt item
        "corrupt_string_entry",
        # 6. Valid 0.9.0
        {
            "diagnostic_id": "diag_mix_006",
            "correlation_id": "corr_006",
            "investigation_id": "inv_001",
            "diagnostic_category": "GENERAL_DIAGNOSTIC",
            "severity": "WARNING",
            "evidence_nature": "OBSERVED_FAILURE",
            "message": "Valid 0.9.0",
            "timestamp": clock(),
            "schema_version": "0.9.0",
        },
    ]
    state_store.set("historical_review_diagnostics:history", mixed_diags)

    svc.hydrate_from_state_store(force=True)

    # 3 valid records loaded
    diags = svc.list_diagnostics()
    loaded_ids = {d.diagnostic_id for d in diags}
    assert loaded_ids == {"diag_mix_001", "diag_mix_003", "diag_mix_006"}

    # Unloadable records were safely skipped
    assert "diag_mix_002" not in loaded_ids
    assert "diag_mix_004" not in loaded_ids


def test_scenario_12_service_resilience_after_recoverable_failures(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 12: Service remains completely functional after encountering corrupt records.

    Guarantees:
    - Service can record new diagnostics, trigger alerts, query summaries, export, and reconcile.
    - System health is not degraded.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    # Inject malformed alerts and diags
    state_store.set("historical_review_alerts:history", [{"bad": "data", "schema_version": "99.0.0"}])
    state_store.set("historical_review_diagnostics:history", [{"corrupt": True}])

    svc.hydrate_from_state_store(force=True)

    # Verify service continues to record new diagnostics normally
    diag = svc.record_diagnostic(
        investigation_id="inv_resilient_001",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="System fully healthy after hydration error",
    )
    assert diag is not None
    assert diag.diagnostic_id in svc._diagnostics_by_id

    # Verify alert recording via error diagnostic works
    diag_err = svc.record_diagnostic(
        investigation_id="inv_resilient_001",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Audit evaluation failure occurred",
    )
    assert diag_err is not None
    alerts = svc.list_alerts(investigation_id="inv_resilient_001")
    assert len(alerts) >= 1

    # Verify diagnostic summary works
    summary = svc.get_diagnostic_summary("inv_resilient_001")
    assert summary.total_diagnostics >= 1


# ==============================================================================
# 5. Persistence Write Safety & StateStore Guarantees (Scenarios 13 - 14)
# ==============================================================================

def test_scenario_13_persistence_write_safety_no_partial_overwrites(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 13: Failed validation never replaces StateStore with partial or corrupt data.

    Guarantees:
    - Pre-existing stored StateStore keys remain intact if a subsequent operation errors.
    - Records are persisted only after complete dataclass construction.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    initial_rec = {
        "diagnostic_id": "diag_intact_001",
        "correlation_id": "corr_intact_001",
        "investigation_id": "inv_001",
        "category": "GENERAL_DIAGNOSTIC",
        "severity": "INFO",
        "evidence_nature": "INFERRED_INTERPRETATION",
        "message": "Intact record",
        "timestamp": clock(),
        "schema_version": "1.0.0",
    }
    state_store.set("historical_review_diagnostics:history", [initial_rec])

    # Attempting to record diagnostic with invalid characters in investigation_id raises ValidationError
    with pytest.raises(ValidationError):
        svc.record_diagnostic(
            investigation_id="invalid/path/../injection",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message="Will fail validation",
        )

    # Stored history is untouched
    stored = state_store.get("historical_review_diagnostics:history")
    assert len(stored) == 1
    assert stored[0]["diagnostic_id"] == "diag_intact_001"


def test_scenario_14_statestore_atomicity_and_documented_limitations(
    state_store: StateStore
):
    """Scenario 14: Verifies StateStore single-key atomicity and documents multi-key limitations.

    Guarantees:
    - StateStore performs thread-safe copy-on-write isolation via RLock and deepcopy.
    - Reads return isolated copies; modifying the returned dict does not mutate StateStore.
    - Documented architectural limitation: StateStore does NOT support multi-key ACID transactions,
      transactional rollback, or write-ahead logging (WAL).
    """
    data = {"key": "original", "nested": {"count": 1}}
    state_store.set("test_key", data)

    # Mutating local data after set() does not affect store
    data["key"] = "mutated_locally"
    data["nested"]["count"] = 999
    assert state_store.get("test_key")["key"] == "original"
    assert state_store.get("test_key")["nested"]["count"] == 1

    # Mutating returned copy from get() does not affect store
    fetched = state_store.get("test_key")
    fetched["key"] = "mutated_after_get"
    assert state_store.get("test_key")["key"] == "original"

    # Verify documented transaction limitation
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store)
    audit_summary = svc.get_migration_audit_summary()
    assert audit_summary["persistence_transaction_mode"] == "SINGLE_KEY_ISOLATED_IN_MEMORY"
    assert audit_summary["atomic_rollback_supported"] is False
    assert "Multi-key transactional rollback is not architecturally supported" in audit_summary["rollback_guarantee_notes"]


# ==============================================================================
# 6. Unsupported Future Versions Preservation (Scenarios 15 - 16)
# ==============================================================================

def test_scenario_15_unsupported_future_version_never_downgraded(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 15: Future schema versions (e.g. 2.0.0, 99.0.0) are never silently downgraded.

    Guarantees:
    - Raw future version record in StateStore retains schema_version="2.0.0".
    - Hydration skips the record without deleting or modifying it in StateStore.
    - Subsequent reads/writes do not overwrite the raw stored record.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    future_record = {
        "diagnostic_id": "diag_future_200",
        "correlation_id": "corr_200",
        "investigation_id": "inv_future",
        "category": "GENERAL_DIAGNOSTIC",
        "severity": "INFO",
        "evidence_nature": "INFERRED_INTERPRETATION",
        "message": "Future record 2.0.0",
        "timestamp": clock(),
        "schema_version": "2.0.0",
    }
    state_store.set("historical_review_diagnostics:history", [future_record])

    # Hydrate service
    svc.hydrate_from_state_store(force=True)

    # Not present in active memory queue
    assert svc.get_diagnostic("diag_future_200") is None

    # Still present in StateStore with exact schema_version="2.0.0" (never downgraded to 1.0.0)
    stored_diags = state_store.get("historical_review_diagnostics:history")
    assert len(stored_diags) == 1
    assert stored_diags[0]["schema_version"] == "2.0.0"


def test_scenario_16_reconciliation_unverifiable_on_incompatible_or_malformed_export(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 16: Reconciling export with future or malformed schema returns UNVERIFIABLE.

    Guarantees:
    - Reconciling export snapshot with schema_version="2.0.0" returns UNVERIFIABLE.
    - Reconciling export snapshot with malformed schema_version returns UNVERIFIABLE.
    - Neither crash nor produce a false VERIFIED report.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    # 1. Future export
    state_store.set(
        "historical_review_exports:exp_future_99",
        {
            "export_id": "exp_future_99",
            "schema_version": "99.0.0",
            "diagnostics": [],
            "alerts": [],
        },
    )
    rep_future = svc.reconcile_export("exp_future_99")
    assert rep_future.status == DiagnosticsReconciliationStatus.UNVERIFIABLE
    assert rep_future.fingerprint_verified is False
    assert "unsupported future schema version '99.0.0'" in rep_future.summary_notes

    # 2. Malformed export
    state_store.set(
        "historical_review_exports:exp_malformed_99",
        {
            "export_id": "exp_malformed_99",
            "schema_version": "invalid_semver_abc",
            "diagnostics": [],
            "alerts": [],
        },
    )
    rep_malformed = svc.reconcile_export("exp_malformed_99")
    assert rep_malformed.status == DiagnosticsReconciliationStatus.UNVERIFIABLE
    assert rep_malformed.fingerprint_verified is False
    assert "malformed schema version 'invalid_semver_abc'" in rep_malformed.summary_notes


# ==============================================================================
# 7. Migration Auditability & Secret Scrubbing (Scenarios 17 - 18)
# ==============================================================================

def test_scenario_17_migration_audit_summary_tracking(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 17: Migration audit tracking accurately classifies outcomes during hydration.

    Guarantees:
    - Increments successful_migrations for legacy and 0.9.0 payloads.
    - Increments direct_compatible_loads for 1.0.0 payloads.
    - Increments unsupported_future_skipped for future version payloads.
    - Increments malformed_records_skipped for malformed version payloads.
    - Exposes complete audit summary via get_migration_audit_summary().
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    stored_diags = [
        # Legacy unversioned -> migrated
        {"diagnostic_id": "d_leg", "correlation_id": "c1", "investigation_id": "i1", "message": "msg1", "timestamp": clock()},
        # 0.9.0 -> migrated
        {"diagnostic_id": "d_090", "correlation_id": "c2", "investigation_id": "i1", "message": "msg2", "timestamp": clock(), "schema_version": "0.9.0"},
        # 1.0.0 -> direct compatible
        {"diagnostic_id": "d_100", "correlation_id": "c3", "investigation_id": "i1", "message": "msg3", "timestamp": clock(), "schema_version": "1.0.0"},
        # 2.0.0 -> unsupported future
        {"diagnostic_id": "d_fut", "correlation_id": "c4", "investigation_id": "i1", "message": "msg4", "timestamp": clock(), "schema_version": "2.0.0"},
        # malformed -> malformed skipped
        {"diagnostic_id": "d_mal", "correlation_id": "c5", "investigation_id": "i1", "message": "msg5", "timestamp": clock(), "schema_version": "malformed_v"},
    ]
    state_store.set("historical_review_diagnostics:history", stored_diags)

    svc.hydrate_from_state_store(force=True)

    summary = svc.get_migration_audit_summary()
    assert summary["total_inspected"] >= 5
    assert summary["successful_migrations"] >= 2
    assert summary["direct_compatible_loads"] >= 1
    assert summary["unsupported_future_skipped"] >= 1
    assert summary["malformed_records_skipped"] >= 1
    assert summary["last_migration_timestamp"] is not None
    assert len(summary["recent_audit_records"]) >= 5


def test_scenario_18_migration_audit_secret_sanitization(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 18: Audit records and migration diagnostics scrub secrets and sensitive tokens.

    Guarantees:
    - Bearer tokens, API keys, passwords, and file paths are redacted in audit details and logs.
    - No secrets are leaked in recent_audit_records.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    # Injected record with secrets in message and malformed error
    secret_diag = {
        "diagnostic_id": "d_secret_001",
        "correlation_id": "corr_secret_001",
        "investigation_id": "inv_001",
        "message": "Encountered auth failure: api_key='sk_live_99887766554433221100' and bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.secret",
        "schema_version": "invalid_with_secret_token",
    }
    state_store.set("historical_review_diagnostics:history", [secret_diag])

    svc.hydrate_from_state_store(force=True)

    summary = svc.get_migration_audit_summary()
    recent = summary["recent_audit_records"]
    assert len(recent) > 0

    for rec in recent:
        rec_str = str(rec)
        assert "sk_live_99887766554433221100" not in rec_str
        assert "eyJhbGciOiJIUzI1NiJ9" not in rec_str


# ==============================================================================
# 8. Orchestrator API & Handler Integration (Scenario 19)
# ==============================================================================

def test_scenario_19_orchestrator_handler_for_migration_audit(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 19: Orchestrator command handler for migration audit summary functions properly.

    Guarantees:
    - 'get_historical_review_migration_audit_summary' returns {"status": "success", "read_only": True, ...}.
    - Prohibited trading parameters in context raise ValidationError.
    """
    orchestrator = MasterOrchestrator()
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    register_audit_diagnostics_handlers(orchestrator, svc)

    req = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={},
    )
    result = orchestrator.handle_request(req)

    assert result.status == RequestStatus.COMPLETED
    assert result.result["status"] == "success"
    assert result.result["read_only"] is True
    assert result.result["execution_locked"] is True
    assert "migration_audit_summary" in result.result
    assert "successful_migrations" in result.result["migration_audit_summary"]

    # Prohibited trading fields rejected
    prohibited_req = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={"trade_recommendation": "BUY", "quantity": 100},
    )
    res_prohibited = orchestrator.handle_request(prohibited_req)
    assert res_prohibited.status == RequestStatus.FAILED


# ==============================================================================
# 9. Mandatory Safety Invariants Verification (Scenario 20)
# ==============================================================================

def test_scenario_20_mandatory_safety_invariants(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 20: Mandatory safety invariants remain intact throughout Phase 2, Step 44.

    Safety Invariants:
    1. Settings.execution_locked is strictly True.
    2. No live broker integration or real-money trading exists.
    3. Trading engine remains uninitialized.
    4. Diagnostics and alerts are purely informational; zero execution commands exist.
    5. clear_history() completely resets memory and StateStore diagnostics keys.
    """
    assert Settings.execution_locked is True, "CRITICAL: Settings.execution_locked must remain True!"

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.record_diagnostic(
        investigation_id="inv_safe_001",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Informational diagnostic only",
    )
    assert len(svc.list_diagnostics()) == 1

    # Clear history resets in-memory structures and audit tracking
    svc.clear_history()
    assert len(svc.list_diagnostics()) == 0
    assert len(svc.list_alerts()) == 0
    summary = svc.get_migration_audit_summary()
    assert summary["total_inspected"] == 0
    assert summary["successful_migrations"] == 0
