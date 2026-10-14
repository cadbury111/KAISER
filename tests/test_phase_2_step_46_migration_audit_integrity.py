"""KAISER — Phase 2, Step 46 Acceptance & Hardening Test Suite.

Migration Audit Integrity, Tamper Detection & Reconstruction Consistency.

Test-backed verification covering:
A. Baseline Reconstruction:
   - Persist migration audit data and audited-record fingerprints.
   - Reconstruct service across multiple instances and verify deterministic matching.
   - Confirm unchanged data does not inflate cumulative migration totals.
   - Deterministic canonical dictionary and SHA-256 fingerprint generation.

B. Tamper and Inconsistency Detection:
   - Modified cumulative counter detected via fingerprint mismatch.
   - Modified counter with recomputed hash detected via mathematical sum invariant.
   - Negative counter values detected and classified.
   - Modified audited-record key detected.
   - Removed audit entry detected via fingerprint mismatch.
   - Duplicate audit entries detected.
   - Audit record count exceeding cumulative total detected.
   - Altered schema version (unsupported future and malformed) classified as UNSUPPORTED_SCHEMA.
   - Non-mapping payload classified as CORRUPT_OR_INCONSISTENT.
   - Incomplete summary with missing required fields classified as INCOMPLETE.

C. Recovery and Data Preservation:
   - Corrupt audit metadata does not crash access to unrelated valid diagnostics.
   - Unsupported and corrupt audit payloads preserved in StateStore without silent overwrite.
   - Avoid silently replacing corrupted counters with fabricated values.
   - Subsequent valid hydration remains usable.
   - Incomplete evidence not reported as verified.

D. Lifecycle Consistency:
   - Audit state across repeated and lazy hydration.
   - Legitimate retention and eviction do not produce false tampering alerts.
   - clear_history() removes audit state; fresh hydration reports MISSING_EVIDENCE without resurrecting stale state.
   - Distinguishability between cumulative and per-hydration metrics.

E. Concurrency and Read-Only Behavior:
   - Concurrent audit-summary reads and updates maintain snapshot consistency.
   - Read-only audit verification does not mutate StateStore or source records.

F. Security and Sanitization:
   - Secret scrubbing in discrepancy descriptions and details.
   - Handler parameter allowlisting and trade directive rejection.
   - Mandatory safety invariants: Settings.execution_locked = True, uninitialized trading engine.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
import hashlib
import json
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
    MigrationAuditDiscrepancy,
    MigrationAuditIntegrityReport,
    MigrationAuditIntegrityStatus,
    compute_migration_audit_canonical_dict,
    compute_migration_audit_fingerprint,
    verify_migration_audit_payload,
)
from app.analysis.opportunities.evolution.audit.diagnostics.schema import (
    CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
    SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS,
    LEGACY_UNVERSIONED,
    DiagnosticsRecordType,
    SchemaCompatibilityOutcome,
    MigrationAuditRecord,
    MigrationAuditSummary,
    SchemaVersionCompatibilityStatus,
    SchemaVersionError,
    IncompatibleSchemaVersionError,
    MalformedSchemaVersionError,
    detect_and_validate_schema_version,
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
# A. Baseline Reconstruction (Scenarios 1 - 4)
# ==============================================================================


def test_scenario_01_baseline_reconstruction_with_verified_fingerprint(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 1: Baseline reconstruction with valid migration audit payload and verified fingerprint.

    Guarantees:
    - Service hydrates legacy records, computes canonical dict, and stores content_fingerprint.
    - Reconstructed service detects VERIFIED status with matching calculated fingerprint.
    - Cumulative counters and audited keys are restored accurately.
    """
    svc1 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [
            {"diagnostic_id": "diag_01", "investigation_id": "inv_1", "message": "Legacy diag", "timestamp": clock()},
            {"diagnostic_id": "diag_02", "investigation_id": "inv_1", "message": "Legacy diag 2", "timestamp": clock()},
        ],
    )
    svc1.hydrate_from_state_store(force=True)

    persisted = state_store.get("historical_review_diagnostics:migration_audit")
    assert persisted is not None
    assert "content_fingerprint" in persisted
    assert len(persisted["content_fingerprint"]) == 64
    assert persisted["cumulative_successful_migrations"] == 2
    assert persisted["cumulative_total_inspected"] == 2

    # Reconstruct service
    svc2 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc2.hydrate_from_state_store(force=True)

    summary = svc2.get_migration_audit_summary()
    assert summary["integrity_status"] == MigrationAuditIntegrityStatus.VERIFIED.value
    assert summary["integrity_verified"] is True
    assert summary["content_fingerprint"] == persisted["content_fingerprint"]
    assert summary["integrity_discrepancies_count"] == 0
    assert summary["cumulative_successful_migrations"] == 2
    assert summary["cumulative_total_inspected"] == 2


def test_scenario_02_deterministic_outcomes_across_repeated_reconstructions(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 2: Deterministic outcomes across multiple repeated service reconstructions.

    Guarantees:
    - Multiple successive reconstructions yield identical fingerprints, counts, and reports.
    """
    svc0 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_det_1", "investigation_id": "inv_det", "message": "Msg", "timestamp": clock()}],
    )
    svc0.hydrate_from_state_store(force=True)
    initial_summary = svc0.get_migration_audit_summary()

    for cycle in range(5):
        clock.advance(10.0)
        svc_cycle = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
        svc_cycle.hydrate_from_state_store(force=True)
        cycle_summary = svc_cycle.get_migration_audit_summary()

        assert cycle_summary["integrity_status"] == MigrationAuditIntegrityStatus.VERIFIED.value
        assert cycle_summary["integrity_verified"] is True
        assert cycle_summary["content_fingerprint"] == initial_summary["content_fingerprint"]
        assert cycle_summary["cumulative_total_inspected"] == initial_summary["cumulative_total_inspected"]
        assert cycle_summary["cumulative_successful_migrations"] == initial_summary["cumulative_successful_migrations"]


def test_scenario_03_unchanged_data_does_not_inflate_cumulative_migration_totals(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 3: Confirm unchanged data does not inflate cumulative migration totals upon reconstruction."""
    svc1 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_stable", "investigation_id": "inv_s", "message": "Stable", "timestamp": clock()}],
    )
    svc1.hydrate_from_state_store(force=True)
    assert svc1.get_migration_audit_summary()["cumulative_successful_migrations"] == 1

    svc2 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc2.hydrate_from_state_store(force=True)
    # Repeated hydration on svc2
    svc2.hydrate_from_state_store(force=True)
    svc2.hydrate_from_state_store(force=True)

    summary = svc2.get_migration_audit_summary()
    assert summary["cumulative_successful_migrations"] == 1
    assert summary["cumulative_total_inspected"] == 1
    assert summary["current_hydration_successful_migrations"] == 0
    assert summary["current_hydration_re_read_count"] >= 1
    assert summary["integrity_verified"] is True


def test_scenario_04_canonical_dict_and_sha256_fingerprint_determinism(
    clock: DeterministicClock
):
    """Scenario 4: Canonical dictionary and SHA-256 fingerprint generation are strictly deterministic."""
    data_a = {
        "schema_version": "1.0.0",
        "cumulative_total_inspected": 2,
        "cumulative_successful_migrations": 2,
        "cumulative_direct_compatible_loads": 0,
        "cumulative_unsupported_future_skipped": 0,
        "cumulative_malformed_records_skipped": 0,
        "cumulative_migration_failures": 0,
        "cumulative_unsupported_records_skipped": 0,
        "last_migration_timestamp": 1700000000.0,
        "last_migration_iso": "2023-11-14T22:13:20Z",
        "audited_record_keys": ["KEY_B", "KEY_A", "KEY_C"],
        "recent_audit_records": [
            {
                "record_type": "DIAGNOSTIC",
                "record_id": "d2",
                "timestamp": 1700000002.0,
                "outcome": "MIGRATED_COMPATIBLE",
                "details": {"b_key": 2, "a_key": 1},
            },
            {
                "record_type": "DIAGNOSTIC",
                "record_id": "d1",
                "timestamp": 1700000001.0,
                "outcome": "MIGRATED_COMPATIBLE",
                "details": {"z_key": 9, "m_key": 5},
            },
        ],
    }
    # Unordered dict with different internal key insertions
    data_b = {
        "recent_audit_records": list(reversed(data_a["recent_audit_records"])),
        "audited_record_keys": ["KEY_C", "KEY_A", "KEY_B"],
        "schema_version": "1.0.0",
        "last_migration_iso": "2023-11-14T22:13:20Z",
        "last_migration_timestamp": 1700000000.0,
        "cumulative_unsupported_records_skipped": 0,
        "cumulative_migration_failures": 0,
        "cumulative_malformed_records_skipped": 0,
        "cumulative_unsupported_future_skipped": 0,
        "cumulative_direct_compatible_loads": 0,
        "cumulative_successful_migrations": 2,
        "cumulative_total_inspected": 2,
    }

    can_a = compute_migration_audit_canonical_dict(data_a)
    can_b = compute_migration_audit_canonical_dict(data_b)
    assert can_a == can_b
    assert compute_migration_audit_fingerprint(can_a) == compute_migration_audit_fingerprint(can_b)


# ==============================================================================
# B. Tamper and Inconsistency Detection (Scenarios 5 - 14)
# ==============================================================================


def test_scenario_05_tamper_detection_modified_counter_causes_fingerprint_mismatch(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 5: Tamper detection: modifying a cumulative counter without recalculating fingerprint triggers FINGERPRINT_MISMATCH."""
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d1", "investigation_id": "inv1", "message": "M", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)

    persisted = state_store.get("historical_review_diagnostics:migration_audit")
    assert persisted is not None

    # Attacker alters cumulative counter directly in store without updating fingerprint
    tampered = dict(persisted)
    tampered["cumulative_successful_migrations"] = 99
    tampered["cumulative_total_inspected"] = 99
    state_store.set("historical_review_diagnostics:migration_audit", tampered)

    report = verify_migration_audit_payload(tampered, time_provider=clock)
    assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert report.is_verified is False
    disc_types = [d.discrepancy_type for d in report.discrepancies]
    assert "FINGERPRINT_MISMATCH" in disc_types


def test_scenario_06_tamper_detection_modified_counter_with_recomputed_hash_violates_sum(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 6: Modifying counter AND recomputing hash is caught by mathematical sum invariant."""
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d1", "investigation_id": "inv1", "message": "M", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)

    persisted = state_store.get("historical_review_diagnostics:migration_audit")
    tampered = dict(persisted)
    # Alter successful migrations without updating total inspected
    tampered["cumulative_successful_migrations"] = 10
    # Recompute hash for this tampered dict
    can = compute_migration_audit_canonical_dict(tampered)
    tampered["content_fingerprint"] = compute_migration_audit_fingerprint(can)

    report = verify_migration_audit_payload(tampered, time_provider=clock)
    assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert report.is_verified is False
    disc_types = [d.discrepancy_type for d in report.discrepancies]
    assert "COUNTER_SUM_MISMATCH" in disc_types


def test_scenario_07_tamper_detection_negative_counters_rejected(
    clock: DeterministicClock
):
    """Scenario 7: Negative counters are detected and classified as corruption."""
    payload = {
        "schema_version": "1.0.0",
        "cumulative_total_inspected": -1,
        "cumulative_successful_migrations": -1,
        "cumulative_direct_compatible_loads": 0,
        "cumulative_unsupported_future_skipped": 0,
        "cumulative_malformed_records_skipped": 0,
        "cumulative_migration_failures": 0,
        "cumulative_unsupported_records_skipped": 0,
        "last_migration_timestamp": clock(),
        "last_migration_iso": "2023-11-14T22:13:20Z",
        "audited_record_keys": [],
        "recent_audit_records": [],
    }
    can = compute_migration_audit_canonical_dict(payload)
    payload["content_fingerprint"] = compute_migration_audit_fingerprint(can)

    report = verify_migration_audit_payload(payload, time_provider=clock)
    assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert any(d.discrepancy_type == "NEGATIVE_COUNTER" for d in report.discrepancies)


def test_scenario_08_tamper_detection_modified_audited_record_key(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 8: Modified audited record key triggers fingerprint mismatch or INVALID_AUDIT_KEY."""
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d1", "investigation_id": "inv1", "message": "M", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)

    persisted = state_store.get("historical_review_diagnostics:migration_audit")
    tampered = dict(persisted)
    tampered["audited_record_keys"] = ["MODIFIED_FAKE_KEY"]

    report = verify_migration_audit_payload(tampered, time_provider=clock)
    assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert any(d.discrepancy_type == "FINGERPRINT_MISMATCH" for d in report.discrepancies)


def test_scenario_09_tamper_detection_removed_audit_entry(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 9: Removing an audit record entry triggers fingerprint mismatch."""
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [
            {"diagnostic_id": "d1", "investigation_id": "inv1", "message": "M1", "timestamp": clock()},
            {"diagnostic_id": "d2", "investigation_id": "inv2", "message": "M2", "timestamp": clock()},
        ],
    )
    svc.hydrate_from_state_store(force=True)

    persisted = state_store.get("historical_review_diagnostics:migration_audit")
    tampered = dict(persisted)
    # Remove one entry from recent_audit_records
    tampered["recent_audit_records"] = tampered["recent_audit_records"][:1]

    report = verify_migration_audit_payload(tampered, time_provider=clock)
    assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert any(d.discrepancy_type == "FINGERPRINT_MISMATCH" for d in report.discrepancies)


def test_scenario_10_tamper_detection_duplicate_audit_record_entries(
    clock: DeterministicClock
):
    """Scenario 10: Duplicate audit record entries are detected as DUPLICATE_AUDIT_RECORD."""
    rec = {
        "record_type": "DIAGNOSTIC",
        "record_id": "d_dup_1",
        "timestamp": clock(),
        "timestamp_iso": "2023-11-14T22:13:20Z",
        "source_version": None,
        "target_version": "1.0.0",
        "outcome": "MIGRATED_COMPATIBLE",
        "details": {},
    }
    payload = {
        "schema_version": "1.0.0",
        "cumulative_total_inspected": 2,
        "cumulative_successful_migrations": 2,
        "cumulative_direct_compatible_loads": 0,
        "cumulative_unsupported_future_skipped": 0,
        "cumulative_malformed_records_skipped": 0,
        "cumulative_migration_failures": 0,
        "cumulative_unsupported_records_skipped": 0,
        "last_migration_timestamp": clock(),
        "last_migration_iso": "2023-11-14T22:13:20Z",
        "audited_record_keys": ["DIAGNOSTIC:d_dup_1:None:1700000000.0"],
        "recent_audit_records": [rec, rec],  # Duplicated
    }
    can = compute_migration_audit_canonical_dict(payload)
    payload["content_fingerprint"] = compute_migration_audit_fingerprint(can)

    report = verify_migration_audit_payload(payload, time_provider=clock)
    assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert any(d.discrepancy_type == "DUPLICATE_AUDIT_RECORD" for d in report.discrepancies)


def test_scenario_11_tamper_detection_audit_records_count_exceeds_total(
    clock: DeterministicClock
):
    """Scenario 11: Audit record count exceeding cumulative total inspected is caught as AUDIT_RECORD_COUNT_EXCEEDS_TOTAL."""
    recs = [
        {
            "record_type": "DIAGNOSTIC",
            "record_id": f"d_{i}",
            "timestamp": clock() + i,
            "outcome": "MIGRATED_COMPATIBLE",
            "details": {},
        }
        for i in range(5)
    ]
    payload = {
        "schema_version": "1.0.0",
        "cumulative_total_inspected": 1,  # Only 1 total, but 5 records
        "cumulative_successful_migrations": 1,
        "cumulative_direct_compatible_loads": 0,
        "cumulative_unsupported_future_skipped": 0,
        "cumulative_malformed_records_skipped": 0,
        "cumulative_migration_failures": 0,
        "cumulative_unsupported_records_skipped": 0,
        "last_migration_timestamp": clock(),
        "last_migration_iso": "2023-11-14T22:13:20Z",
        "audited_record_keys": [],
        "recent_audit_records": recs,
    }
    can = compute_migration_audit_canonical_dict(payload)
    payload["content_fingerprint"] = compute_migration_audit_fingerprint(can)

    report = verify_migration_audit_payload(payload, time_provider=clock)
    assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert any(d.discrepancy_type == "AUDIT_RECORD_COUNT_EXCEEDS_TOTAL" for d in report.discrepancies)


def test_scenario_12_altered_schema_version_classified_as_unsupported_schema(
    clock: DeterministicClock
):
    """Scenario 12: Unsupported future and malformed schema versions are classified as UNSUPPORTED_SCHEMA."""
    # Future version
    p_future = {
        "schema_version": "2.0.0",
        "cumulative_total_inspected": 0,
        "cumulative_successful_migrations": 0,
        "cumulative_direct_compatible_loads": 0,
        "cumulative_unsupported_future_skipped": 0,
        "cumulative_malformed_records_skipped": 0,
        "cumulative_migration_failures": 0,
        "cumulative_unsupported_records_skipped": 0,
    }
    rep_future = verify_migration_audit_payload(p_future, time_provider=clock)
    assert rep_future.status == MigrationAuditIntegrityStatus.UNSUPPORTED_SCHEMA
    assert any(d.discrepancy_type == "UNSUPPORTED_FUTURE_SCHEMA" for d in rep_future.discrepancies)

    # Malformed version
    p_malformed = dict(p_future)
    p_malformed["schema_version"] = "not_semver_at_all"
    rep_malf = verify_migration_audit_payload(p_malformed, time_provider=clock)
    assert rep_malf.status == MigrationAuditIntegrityStatus.UNSUPPORTED_SCHEMA
    assert any(d.discrepancy_type == "MALFORMED_SCHEMA_VERSION" for d in rep_malf.discrepancies)


def test_scenario_13_non_mapping_payload_classified_as_corruption(
    clock: DeterministicClock
):
    """Scenario 13: Non-mapping payloads are classified as CORRUPT_OR_INCONSISTENT without crashing."""
    rep_str = verify_migration_audit_payload("corrupted_payload_string", time_provider=clock)
    assert rep_str.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert rep_str.is_verified is False
    assert any(d.discrepancy_type == "NON_MAPPING_PAYLOAD" for d in rep_str.discrepancies)

    rep_int = verify_migration_audit_payload(12345, time_provider=clock)
    assert rep_int.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT


def test_scenario_14_missing_required_counters_classified_as_incomplete(
    clock: DeterministicClock
):
    """Scenario 14: Incomplete summary missing counter fields is classified as INCOMPLETE."""
    payload = {
        "schema_version": "1.0.0",
        "note": "incomplete payload with no counters",
    }
    report = verify_migration_audit_payload(payload, time_provider=clock)
    assert report.status == MigrationAuditIntegrityStatus.INCOMPLETE
    assert report.is_verified is False
    assert any(d.discrepancy_type == "MISSING_REQUIRED_FIELD" for d in report.discrepancies)


# ==============================================================================
# C. Recovery and Data Preservation (Scenarios 15 - 18)
# ==============================================================================


def test_scenario_15_corrupt_audit_metadata_does_not_crash_unrelated_diagnostics(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 15: Corrupt audit metadata does not crash hydration or access to unrelated valid diagnostics.

    Guarantees:
    - StateStore contains corrupt migration audit metadata AND valid diagnostics records.
    - Hydration succeeds for valid diagnostics without raising unhandled exceptions.
    - Diagnostics are fully queryable.
    """
    state_store.set("historical_review_diagnostics:migration_audit", "ABSOLUTELY_CORRUPT_STRING")
    state_store.set(
        "historical_review_diagnostics:history",
        [
            {"diagnostic_id": "d_valid_1", "investigation_id": "inv_v", "message": "Valid 1", "timestamp": clock()},
            {"diagnostic_id": "d_valid_2", "investigation_id": "inv_v", "message": "Valid 2", "timestamp": clock()},
        ],
    )

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    # Valid diagnostics hydrated cleanly
    diags = svc.list_diagnostics("inv_v")
    assert len(diags) == 2
    assert {d.diagnostic_id for d in diags} == {"d_valid_1", "d_valid_2"}

    # Migration audit reports corruption without crashing
    summary = svc.get_migration_audit_summary()
    assert summary["integrity_status"] == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT.value
    assert summary["integrity_verified"] is False


def test_scenario_16_non_destructive_preservation_of_unsupported_audit_payload(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 16: Unsupported future audit payload is preserved in StateStore and not overwritten."""
    future_payload = {
        "schema_version": "2.0.0",
        "cumulative_successful_migrations": 999,
        "future_field": "do_not_overwrite_me",
    }
    state_store.set("historical_review_diagnostics:migration_audit", future_payload)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    # Inspect StateStore directly: future payload must NOT be overwritten with zeros
    stored_now = state_store.get("historical_review_diagnostics:migration_audit")
    assert stored_now == future_payload
    assert stored_now["future_field"] == "do_not_overwrite_me"


def test_scenario_17_no_fabrication_of_counters_on_corrupted_metadata(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 17: Service does not adopt corrupted or fabricated counters into memory."""
    tampered_payload = {
        "schema_version": "1.0.0",
        "cumulative_total_inspected": 1000000,
        "cumulative_successful_migrations": 5,  # Wild sum mismatch
        "content_fingerprint": "bad_hash",
    }
    state_store.set("historical_review_diagnostics:migration_audit", tampered_payload)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    summary = svc.get_migration_audit_summary()
    # In-memory counters did NOT adopt 1,000,000
    assert summary["cumulative_total_inspected"] == 0
    assert summary["cumulative_successful_migrations"] == 0
    assert summary["integrity_status"] == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT.value


def test_scenario_18_subsequent_valid_hydration_remains_usable(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 18: Service remains fully usable and can successfully hydrate valid state after corruption is cleared."""
    state_store.set("historical_review_diagnostics:migration_audit", "CORRUPT")
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)
    assert svc.get_migration_audit_summary()["integrity_verified"] is False

    # Clear corrupt state and write valid diagnostics
    svc.clear_history()
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_fresh", "investigation_id": "inv_f", "message": "Fresh", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)

    summary = svc.get_migration_audit_summary()
    assert summary["integrity_status"] == MigrationAuditIntegrityStatus.VERIFIED.value
    assert summary["integrity_verified"] is True
    assert summary["cumulative_successful_migrations"] == 1


# ==============================================================================
# D. Lifecycle Consistency (Scenarios 19 - 21)
# ==============================================================================


def test_scenario_19_repeated_and_lazy_hydration_preserves_verified_status(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 19: Repeated queries and lazy hydration maintain VERIFIED status without degradation."""
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_lazy", "investigation_id": "inv_l", "message": "Msg", "timestamp": clock()}],
    )
    # Lazy hydration via list_diagnostics
    diags = svc.list_diagnostics("inv_l")
    assert len(diags) == 1

    summary1 = svc.get_migration_audit_summary()
    assert summary1["integrity_status"] == MigrationAuditIntegrityStatus.VERIFIED.value
    assert summary1["integrity_verified"] is True

    # Repeated queries
    for _ in range(3):
        svc.list_diagnostics("inv_l")
        summary_loop = svc.get_migration_audit_summary()
        assert summary_loop["integrity_status"] == MigrationAuditIntegrityStatus.VERIFIED.value
        assert summary_loop["content_fingerprint"] == summary1["content_fingerprint"]


def test_scenario_20_legitimate_retention_eviction_does_not_trigger_tamper_alert(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 20: Eviction of diagnostic records beyond bounded capacity does not invalidate migration audit integrity."""
    # Set capacity to 10
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store,
        time_provider=clock,
        max_diagnostics=10,
    )
    # Store 15 records in StateStore
    diags = [
        {"diagnostic_id": f"d_ev_{i}", "investigation_id": "inv_ev", "message": f"Msg {i}", "timestamp": clock() + i}
        for i in range(15)
    ]
    state_store.set("historical_review_diagnostics:history", diags)
    svc.hydrate_from_state_store(force=True)

    summary = svc.get_migration_audit_summary()
    # 10 records kept in bounded deque, all 10 migrated
    assert summary["integrity_status"] == MigrationAuditIntegrityStatus.VERIFIED.value
    assert summary["integrity_verified"] is True
    assert summary["integrity_discrepancies_count"] == 0


def test_scenario_21_clear_history_purges_audit_and_fresh_hydration_reports_missing_evidence(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 21: clear_history() deletes audit key; subsequent check reports MISSING_EVIDENCE."""
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_clr", "investigation_id": "inv_c", "message": "M", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)
    assert state_store.get("historical_review_diagnostics:migration_audit") is not None

    svc.clear_history()
    assert state_store.get("historical_review_diagnostics:migration_audit") is None

    # Verification on empty state
    report = svc.verify_migration_audit_integrity()
    assert report.status == MigrationAuditIntegrityStatus.MISSING_EVIDENCE
    assert report.is_verified is False


# ==============================================================================
# E. Concurrency and Read-Only Behavior (Scenarios 22 - 23)
# ==============================================================================


def test_scenario_22_concurrent_reads_and_updates_maintain_snapshot_consistency(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 22: Concurrent summary queries during active record recording maintain snapshot consistency."""
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    errors: List[str] = []
    stop_event = threading.Event()

    def reader_task():
        while not stop_event.is_set():
            try:
                s = svc.get_migration_audit_summary()
                total = s["cumulative_total_inspected"]
                components = (
                    s["cumulative_successful_migrations"]
                    + s["cumulative_direct_compatible_loads"]
                    + s["cumulative_unsupported_future_skipped"]
                    + s["cumulative_malformed_records_skipped"]
                    + s["cumulative_migration_failures"]
                )
                if total != components:
                    errors.append(f"Inconsistent snapshot: total={total} components={components}")
            except Exception as exc:
                errors.append(f"Reader error: {exc}")

    reader_thread = threading.Thread(target=reader_task)
    reader_thread.daemon = True
    reader_thread.start()

    try:
        for i in range(50):
            clock.advance(1.0)
            svc.record_diagnostic(
                investigation_id="inv_conc",
                category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
                severity=AuditDiagnosticSeverity.INFO,
                message=f"Conc msg {i}",
            )
    finally:
        stop_event.set()
        reader_thread.join(timeout=2.0)

    assert not errors, f"Concurrency errors observed: {errors}"


def test_scenario_23_read_only_query_purity_no_statestore_mutation(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 23: get_migration_audit_summary() and verify_migration_audit_integrity() do not mutate StateStore."""
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_ro", "investigation_id": "inv_ro", "message": "Msg", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)

    audit_before = copy.deepcopy(state_store.get("historical_review_diagnostics:migration_audit"))

    # Execute multiple read-only queries
    _ = svc.get_migration_audit_summary()
    _ = svc.verify_migration_audit_integrity()

    audit_after = state_store.get("historical_review_diagnostics:migration_audit")
    assert audit_before == audit_after


# ==============================================================================
# F. Security, Sanitization & Safety Invariants (Scenarios 24 - 26)
# ==============================================================================


def test_scenario_24_secret_scrubbing_in_discrepancy_details(clock: DeterministicClock):
    """Scenario 24: Secret credentials, tokens, and filesystem paths are scrubbed from discrepancy descriptions."""
    dirty_key = "DIAGNOSTIC:secret_api_key_12345:C:\\Users\\admin\\kaiser\\secret:1700000000.0"
    payload = {
        "schema_version": "1.0.0",
        "cumulative_total_inspected": 1,
        "cumulative_successful_migrations": 1,
        "cumulative_direct_compatible_loads": 0,
        "cumulative_unsupported_future_skipped": 0,
        "cumulative_malformed_records_skipped": 0,
        "cumulative_migration_failures": 0,
        "cumulative_unsupported_records_skipped": 0,
        "last_migration_timestamp": clock(),
        "last_migration_iso": "2023-11-14T22:13:20Z",
        "audited_record_keys": [dirty_key],
        "recent_audit_records": [
            {
                "record_type": "DIAGNOSTIC",
                "record_id": "d_secret_token_abcdef123456",
                "timestamp": clock(),
                "outcome": "CORRUPT_PAYLOAD",
                "details": {"bearer_token": "Bearer eyJhbGciOi...", "file_path": "/var/secret/kaiser.key"},
            }
        ],
    }
    # Verify report does not crash and serializes cleanly
    report = verify_migration_audit_payload(payload, time_provider=clock)
    rep_dict = report.to_dict()
    rep_json = json.dumps(rep_dict)

    assert "secret_api_key" not in rep_json or "[REDACTED" in rep_json
    assert "C:\\Users\\admin" not in rep_json

    # Explicit direct discrepancy sanitization verification
    disc = MigrationAuditDiscrepancy(
        discrepancy_type="INVALID_AUDIT_KEY",
        field_name="audited_record_keys[0]",
        description="Found key with secret C:\\Users\\admin\\secret and password=supersecret123",
        expected="Valid key",
        actual="C:\\Users\\admin\\secret_token",
    )
    assert "C:\\Users\\admin" not in disc.description
    assert "[REDACTED_PATH]" in disc.description
    assert "supersecret123" not in disc.description
    assert "C:\\Users\\admin" not in str(disc.actual)
    assert "[REDACTED_PATH]" in str(disc.actual)


def test_scenario_25_command_handler_parameter_validation_and_directive_rejection(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 25: Orchestrator handler enforces parameter allowlisting and rejects trade execution directives."""
    orchestrator = MasterOrchestrator()
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    register_audit_diagnostics_handlers(orchestrator, svc)

    # 1. Valid request with verify_integrity
    req_valid = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={"read_only": True, "verify_integrity": True},
    )
    res_valid = orchestrator.handle_request(req_valid)
    assert res_valid.status == RequestStatus.COMPLETED
    assert "migration_audit_summary" in res_valid.result
    assert res_valid.result["migration_audit_summary"]["integrity_status"] in (
        MigrationAuditIntegrityStatus.MISSING_EVIDENCE.value,
        MigrationAuditIntegrityStatus.VERIFIED.value,
    )

    # 2. Unsupported parameter rejected
    req_unsupported = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={"unexpected_param": "foo"},
    )
    res_unsupported = orchestrator.handle_request(req_unsupported)
    assert res_unsupported.status == RequestStatus.FAILED

    # 3. Prohibited trading parameter rejected
    req_trade = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={"buy": True, "execute_order": True},
    )
    res_trade = orchestrator.handle_request(req_trade)
    assert res_trade.status == RequestStatus.FAILED


def test_scenario_26_mandatory_safety_invariants():
    """Scenario 26: Mandatory safety invariants remain intact across migration audit integrity subsystem.

    Guarantees:
    - Settings.execution_locked = True.
    - Zero autonomous order placement or trading execution methods.
    - Pure diagnostics and observability reliability.
    """
    assert Settings.execution_locked is True
    # Ensure service has no trading or broker execution methods
    svc = HistoricalReviewAuditDiagnosticsService()
    assert not hasattr(svc, "place_order")
    assert not hasattr(svc, "buy")
    assert not hasattr(svc, "sell")
    assert not hasattr(svc, "execute_order")
    assert not hasattr(svc, "connect_broker")
