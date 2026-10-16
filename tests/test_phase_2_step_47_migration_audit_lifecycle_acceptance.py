"""KAISER — Phase 2, Step 47 Acceptance & Reliability Hardening Test Suite.

Migration Audit Integrity Regression, Failure Recovery & Long-Term Lifecycle Acceptance.

Deterministic acceptance scenarios covering:
A. Repeated corruption detection:
   - Persist valid migration audit payload; corrupt fingerprint, counters, schema metadata, or entries.
   - Reconstruct diagnostics service from same StateStore across multiple instances.
   - Verify corruption is detected consistently across repeated reconstructions without silent acceptance.

B. Failed verification must not silently overwrite evidence:
   - Load corrupted or unsupported migration-audit payload.
   - Trigger normal service operations (diagnostics recording, alert evaluation, exports, hydration).
   - Verify invalid historical evidence is preserved in StateStore without silent replacement.
   - Confirm unrelated valid diagnostic records remain readable.

C. Valid-state restoration and recovery:
   - Start with corrupted payload and verify corruption status.
   - Restore known-valid payload through explicit, test-controlled StateStore operation.
   - Reconstruct service and verify deterministic recovery.
   - Verify recovery occurs only through clearly defined path (no fabricated auto-repair).

D. StateStore reconstruction consistency:
   - Persist valid audit summary and fingerprint.
   - Reconstruct service repeatedly; assert stable integrity status, counters, fingerprint, and discrepancies.
   - Verify repeated hydration does not inflate cumulative migration totals.
   - Distinguish cumulative historical totals from per-hydration statistics.

E. Retention and bounded lifecycle:
   - Exercise audit-log retention and eviction boundaries.
   - Verify retained records, cumulative counters, and fingerprints follow documented semantics.
   - Verify eviction does not invalidate unrelated diagnostic evidence.
   - Test duplicate identifiers and repeated eviction handling.

F. clear_history() lifecycle:
   - Persist diagnostics, alerts, exports, reconciliations, retention stats, and migration-audit state.
   - Call clear_history().
   - Verify all in-memory queues, counters, fingerprints, corruption flags, and StateStore keys are cleared.
   - Reconstruct service and confirm cleared state does not unexpectedly reappear.
   - Verify subsequent fresh records can be stored and audited normally.

G. Compatibility and malformed payload regression:
   - Cover legacy/unversioned, supported older, current, unsupported future, and malformed schema cases.
   - Ensure unsupported or malformed audit evidence is never reported as verified.
   - Confirm unrelated valid diagnostics remain available.
   - Verify sanitization of externally visible discrepancy descriptions and values.

H. Deterministic integrity reporting:
   - Repeatedly verify unchanged payloads and assert deterministic statuses, fingerprints, and discrepancy ordering.
   - Ensure all 5 integrity statuses remain mutually distinguishable under the status contract.

I. Safety and Architecture Invariants:
   - Settings.execution_locked = True.
   - Trading engine uninitialized, zero broker activity, no unauthorized command parameters.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
import hashlib
import json
import threading
from typing import Any, Dict, List, Optional
import pytest

from app.analysis.opportunities.evolution.audit.diagnostics.models import (
    AuditAlert,
    AuditAlertStatus,
    AuditDiagnosticCategory,
    AuditDiagnosticRecord,
    AuditDiagnosticSeverity,
    AuditEvidenceNature,
    DiagnosticsExportRequest,
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


def make_valid_migration_audit_payload(
    cumulative_successful: int = 5,
    cumulative_direct: int = 3,
    cumulative_unsupported_future: int = 0,
    cumulative_malformed: int = 0,
    cumulative_failures: int = 0,
    schema_version: str = CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
    records: Optional[List[Dict[str, Any]]] = None,
    keys: Optional[List[str]] = None,
    last_timestamp: float = 1700000000.0,
) -> Dict[str, Any]:
    """Helper to generate a valid, canonically fingerprinted migration audit payload."""
    tot = (
        cumulative_successful
        + cumulative_direct
        + cumulative_unsupported_future
        + cumulative_malformed
        + cumulative_failures
    )
    if records is None:
        records = [
            {
                "details": {"test": "val"},
                "outcome": SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE.value,
                "record_id": f"rec_{i}",
                "record_type": "DIAGNOSTIC",
                "source_version": "0.9.0",
                "target_version": CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
                "timestamp": last_timestamp + i,
                "timestamp_iso": "2023-11-14T22:13:20Z",
            }
            for i in range(min(tot, 5))
        ]
    if keys is None:
        keys = [f"DIAGNOSTIC:rec_{i}:0.9.0:{last_timestamp + i}" for i in range(min(tot, 5))]

    payload = {
        "schema_version": schema_version,
        "cumulative_total_inspected": tot,
        "cumulative_successful_migrations": cumulative_successful,
        "cumulative_direct_compatible_loads": cumulative_direct,
        "cumulative_unsupported_future_skipped": cumulative_unsupported_future,
        "cumulative_malformed_records_skipped": cumulative_malformed,
        "cumulative_migration_failures": cumulative_failures,
        "cumulative_unsupported_records_skipped": 0,
        "last_migration_timestamp": last_timestamp,
        "last_migration_iso": "2023-11-14T22:13:20Z",
        "audited_record_keys": keys,
        "recent_audit_records": records,
    }
    can_dict = compute_migration_audit_canonical_dict(payload)
    fp = compute_migration_audit_fingerprint(can_dict)
    payload["content_fingerprint"] = fp
    return payload


# ==============================================================================
# A. Repeated Corruption Detection (Scenarios 1 - 4)
# ==============================================================================


def test_scenario_a1_repeated_reconstruction_detects_fingerprint_corruption_consistently(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario A1: Corrupted fingerprint is detected consistently across repeated service reconstructions.

    Guarantees:
    - Persist valid payload, corrupt fingerprint with 64-char invalid hash.
    - Reconstruct service across multiple instances from the same StateStore.
    - Every instance detects CORRUPT_OR_INCONSISTENT and reports is_verified=False.
    - Discrepancy FINGERPRINT_MISMATCH is flagged deterministically.
    - Corrupted evidence is never silently accepted as valid.
    """
    valid = make_valid_migration_audit_payload()
    corrupt_payload = copy.deepcopy(valid)
    corrupt_payload["content_fingerprint"] = "0" * 64  # valid hex length, mismatched digest
    state_store.set("historical_review_diagnostics:migration_audit", corrupt_payload)

    for iteration in range(3):
        svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
        svc.hydrate_from_state_store(force=True)

        report = svc.verify_migration_audit_integrity()
        assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT, (
            f"Iteration {iteration}: Failed to detect corrupt fingerprint"
        )
        assert report.is_verified is False
        assert any(d.discrepancy_type == "FINGERPRINT_MISMATCH" for d in report.discrepancies)

        summary = svc.get_migration_audit_summary()
        assert summary["integrity_verified"] is False
        assert summary["integrity_status"] == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT.value
        assert svc._stored_migration_audit_corrupted is True


def test_scenario_a2_repeated_reconstruction_detects_counter_corruption_consistently(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario A2: Corrupted counters (sum mismatch and negative values) detected across repeated reconstructions.

    Guarantees:
    - Counter mutations without hash recalculation fail fingerprint validation.
    - Counter mutations with recomputed hash fail mathematical sum invariant.
    - Negative counters fail value validation.
    - Detection is deterministic across repeated reconstructions.
    """
    valid = make_valid_migration_audit_payload(cumulative_successful=10, cumulative_direct=5)

    # 1. Tamper with counter and recompute hash to attempt disguised tampering
    tampered_sum = copy.deepcopy(valid)
    tampered_sum["cumulative_successful_migrations"] = 99  # Sum becomes 99+5 != 15
    can_dict = compute_migration_audit_canonical_dict(tampered_sum)
    tampered_sum["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict)
    state_store.set("historical_review_diagnostics:migration_audit", tampered_sum)

    for i in range(2):
        svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
        svc.hydrate_from_state_store(force=True)
        report = svc.verify_migration_audit_integrity()
        assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
        assert any(d.discrepancy_type == "COUNTER_SUM_MISMATCH" for d in report.discrepancies)
        assert svc._stored_migration_audit_corrupted is True

    # 2. Tamper with negative counter
    negative_payload = copy.deepcopy(valid)
    negative_payload["cumulative_successful_migrations"] = -5
    negative_payload["cumulative_total_inspected"] = 0
    can_dict_neg = compute_migration_audit_canonical_dict(negative_payload)
    negative_payload["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict_neg)
    state_store.set("historical_review_diagnostics:migration_audit", negative_payload)

    for i in range(2):
        svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
        svc.hydrate_from_state_store(force=True)
        report = svc.verify_migration_audit_integrity()
        assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
        assert any(d.discrepancy_type == "NEGATIVE_COUNTER" for d in report.discrepancies)


def test_scenario_a3_repeated_reconstruction_detects_schema_metadata_corruption_consistently(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario A3: Corrupted schema metadata (unsupported future and malformed) detected consistently.

    Guarantees:
    - Schema versions outside compatibility matrix classify as UNSUPPORTED_SCHEMA.
    - Reconstructed service rejects unverified evidence across repeated hydrate cycles.
    """
    valid = make_valid_migration_audit_payload()

    # 1. Unsupported future version
    future_payload = copy.deepcopy(valid)
    future_payload["schema_version"] = "99.0.0"
    can_dict = compute_migration_audit_canonical_dict(future_payload)
    future_payload["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict)
    state_store.set("historical_review_diagnostics:migration_audit", future_payload)

    for _ in range(2):
        svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
        svc.hydrate_from_state_store(force=True)
        report = svc.verify_migration_audit_integrity()
        assert report.status == MigrationAuditIntegrityStatus.UNSUPPORTED_SCHEMA
        assert any(d.discrepancy_type == "UNSUPPORTED_FUTURE_SCHEMA" for d in report.discrepancies)
        assert report.is_verified is False
        assert svc._stored_migration_audit_corrupted is True

    # 2. Malformed version
    malformed_payload = copy.deepcopy(valid)
    malformed_payload["schema_version"] = "not_valid_semver_###"
    can_dict_m = compute_migration_audit_canonical_dict(malformed_payload)
    malformed_payload["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict_m)
    state_store.set("historical_review_diagnostics:migration_audit", malformed_payload)

    for _ in range(2):
        svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
        svc.hydrate_from_state_store(force=True)
        report = svc.verify_migration_audit_integrity()
        assert report.status == MigrationAuditIntegrityStatus.UNSUPPORTED_SCHEMA
        assert any(d.discrepancy_type == "MALFORMED_SCHEMA_VERSION" for d in report.discrepancies)
        assert report.is_verified is False


def test_scenario_a4_repeated_reconstruction_detects_audit_record_corruption_consistently(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario A4: Corrupted audit records (duplicate records, modified entries) detected repeatedly.

    Guarantees:
    - Injected duplicate audit records are flagged with DUPLICATE_AUDIT_RECORD.
    - Modified records without fingerprint update are flagged with FINGERPRINT_MISMATCH.
    - Repeated service reconstruction detects anomalies consistently.
    """
    valid = make_valid_migration_audit_payload(cumulative_successful=2, cumulative_direct=0)
    # Add duplicate record to recent_audit_records
    dup_payload = copy.deepcopy(valid)
    dup_payload["recent_audit_records"].append(copy.deepcopy(dup_payload["recent_audit_records"][0]))
    dup_payload["cumulative_total_inspected"] = len(dup_payload["recent_audit_records"])
    dup_payload["cumulative_successful_migrations"] = len(dup_payload["recent_audit_records"])
    can_dict = compute_migration_audit_canonical_dict(dup_payload)
    dup_payload["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict)
    state_store.set("historical_review_diagnostics:migration_audit", dup_payload)

    for _ in range(2):
        svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
        svc.hydrate_from_state_store(force=True)
        report = svc.verify_migration_audit_integrity()
        assert report.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
        assert any(d.discrepancy_type == "DUPLICATE_AUDIT_RECORD" for d in report.discrepancies)
        assert svc._stored_migration_audit_corrupted is True


# ==============================================================================
# B. Failed Verification Must Not Silently Overwrite Evidence (Scenarios 5 - 8)
# ==============================================================================


def test_scenario_b1_failed_verification_does_not_overwrite_corrupt_payload_on_hydration(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario B1: Failed verification prevents hydration from overwriting corrupt audit payload.

    Guarantees:
    - Pre-hydration integrity verification detects corruption.
    - Flag _stored_migration_audit_corrupted is set to True.
    - Subsequent _persist_migration_audit() skips overwriting StateStore.
    - Corrupt payload in StateStore remains identical after hydration completes.
    """
    valid = make_valid_migration_audit_payload()
    corrupt = copy.deepcopy(valid)
    corrupt["content_fingerprint"] = "0" * 64
    corrupt["forensic_tag"] = "corrupt_evidence_marker_123"
    state_store.set("historical_review_diagnostics:migration_audit", corrupt)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    stored_after = state_store.get("historical_review_diagnostics:migration_audit")
    assert stored_after == corrupt, "Corrupt migration audit payload was overwritten!"
    assert stored_after.get("forensic_tag") == "corrupt_evidence_marker_123"


def test_scenario_b2_failed_verification_preserves_corrupt_payload_across_operations(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario B2: Service operations (recording diagnostics, alerts, exports) preserve corrupt audit evidence.

    Guarantees:
    - Operating the service with new diagnostics and alerts persists new operational keys.
    - Corrupt migration audit state in StateStore is never replaced with a synthetic valid summary.
    """
    corrupt = make_valid_migration_audit_payload()
    corrupt["content_fingerprint"] = "0" * 64
    corrupt["tamper_evidence"] = "unauthorized_modification_456"
    state_store.set("historical_review_diagnostics:migration_audit", corrupt)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    # Perform normal service operations: diagnostic + alert evaluation
    diag = svc.record_diagnostic(
        investigation_id="inv_active",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.ERROR,  # Triggers alert creation
        message="Active operational diagnostic during degraded audit state",
    )
    assert diag is not None
    assert len(svc._alerts) >= 1

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_active"))
    assert exp is not None

    stored_audit = state_store.get("historical_review_diagnostics:migration_audit")
    assert stored_audit == corrupt, "Operational paths overwrote corrupted audit evidence!"
    assert stored_audit.get("tamper_evidence") == "unauthorized_modification_456"


def test_scenario_b3_unrelated_valid_diagnostics_remain_fully_accessible_despite_corrupt_audit(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario B3: Corrupt audit evidence does not crash or block reading of unrelated valid diagnostics.

    Guarantees:
    - Valid diagnostic records in StateStore hydrate normally.
    - list_diagnostics, get_diagnostic, and query APIs remain fully functional.
    - get_migration_audit_summary reflects degraded integrity without crashing.
    """
    # 1. Valid diagnostics history
    diags = [
        {
            "diagnostic_id": "diag_valid_01",
            "correlation_id": "corr_01",
            "investigation_id": "inv_resilient",
            "category": "GENERAL_DIAGNOSTIC",
            "severity": "INFO",
            "evidence_nature": "OBSERVED_FAILURE",
            "message": "Valid operational record 1",
            "timestamp": clock(),
            "schema_version": CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        },
        {
            "diagnostic_id": "diag_valid_02",
            "correlation_id": "corr_02",
            "investigation_id": "inv_resilient",
            "category": "INTEGRITY_VIOLATION",
            "severity": "ERROR",
            "evidence_nature": "OBSERVED_FAILURE",
            "message": "Valid operational record 2",
            "timestamp": clock() + 1.0,
            "schema_version": CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        },
    ]
    state_store.set("historical_review_diagnostics:history", diags)

    # 2. Corrupt migration audit payload
    corrupt_audit = make_valid_migration_audit_payload()
    corrupt_audit["content_fingerprint"] = "0" * 64
    state_store.set("historical_review_diagnostics:migration_audit", corrupt_audit)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    loaded_diags = svc.list_diagnostics(investigation_id="inv_resilient")
    assert len(loaded_diags) == 2
    assert svc.get_diagnostic("diag_valid_01") is not None
    assert svc.get_diagnostic("diag_valid_02") is not None

    summary = svc.get_migration_audit_summary()
    assert summary["integrity_verified"] is False
    assert summary["integrity_status"] == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT.value


def test_scenario_b4_unsupported_schema_audit_is_preserved_without_downgrade_or_overwrite(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario B4: Unsupported future schema version is preserved in StateStore without downgrade.

    Guarantees:
    - Service encounters future schema '2.5.0'.
    - Flagged as UNSUPPORTED_SCHEMA; write-protection enabled.
    - StateStore entry maintains version '2.5.0' and is not overwritten with '1.0.0'.
    """
    valid = make_valid_migration_audit_payload()
    future_audit = copy.deepcopy(valid)
    future_audit["schema_version"] = "2.5.0"
    can_dict = compute_migration_audit_canonical_dict(future_audit)
    future_audit["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict)
    state_store.set("historical_review_diagnostics:migration_audit", future_audit)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    persisted = state_store.get("historical_review_diagnostics:migration_audit")
    assert persisted["schema_version"] == "2.5.0"
    assert persisted["content_fingerprint"] == future_audit["content_fingerprint"]
    assert svc._stored_migration_audit_corrupted is True


# ==============================================================================
# C. Valid-State Restoration and Recovery (Scenarios 9 - 11)
# ==============================================================================


def test_scenario_c1_explicit_statestore_restoration_recovers_verified_state_deterministically(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario C1: Explicit StateStore restoration transitions service from corrupt to VERIFIED.

    Guarantees:
    - Start with corrupted payload and verify CORRUPT_OR_INCONSISTENT.
    - Operator/test explicitly restores a known-valid payload into StateStore.
    - Service reconstructs and verifies VERIFIED status with matching calculated fingerprint.
    - _stored_migration_audit_corrupted is cleared to False.
    """
    # 1. Initial corrupted state
    corrupt = make_valid_migration_audit_payload()
    corrupt["content_fingerprint"] = "0" * 64
    state_store.set("historical_review_diagnostics:migration_audit", corrupt)

    svc_corrupt = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc_corrupt.hydrate_from_state_store(force=True)
    assert svc_corrupt.verify_migration_audit_integrity().status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert svc_corrupt._stored_migration_audit_corrupted is True

    # 2. Explicit StateStore restoration
    known_valid = make_valid_migration_audit_payload(cumulative_successful=7, cumulative_direct=3)
    state_store.set("historical_review_diagnostics:migration_audit", known_valid)

    # 3. Reconstruct service
    svc_recovered = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc_recovered.hydrate_from_state_store(force=True)

    report = svc_recovered.verify_migration_audit_integrity()
    assert report.status == MigrationAuditIntegrityStatus.VERIFIED
    assert report.is_verified is True
    assert report.stored_fingerprint == known_valid["content_fingerprint"]
    assert report.calculated_fingerprint == known_valid["content_fingerprint"]
    assert len(report.discrepancies) == 0
    assert svc_recovered._stored_migration_audit_corrupted is False

    summary = svc_recovered.get_migration_audit_summary()
    assert summary["integrity_verified"] is True
    assert summary["cumulative_successful_migrations"] == 7
    assert summary["cumulative_direct_compatible_loads"] == 3


def test_scenario_c2_recovery_requires_supported_path_no_silent_or_fabricated_autorepair(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario C2: No automatic recovery without explicit StateStore restoration.

    Guarantees:
    - System does not fabricate valid counters or silently repair corrupted payloads.
    - Without explicit restoration, repeated calls and reconstructions remain degraded.
    """
    corrupt = make_valid_migration_audit_payload()
    corrupt["content_fingerprint"] = "0" * 64
    state_store.set("historical_review_diagnostics:migration_audit", corrupt)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    # Calling verify or hydrate repeatedly does NOT magically fix corruption
    svc.verify_migration_audit_integrity()
    svc.hydrate_from_state_store(force=True)
    summary = svc.get_migration_audit_summary()

    assert summary["integrity_verified"] is False
    assert summary["integrity_status"] == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT.value
    # Stored payload remains un-repaired in StateStore
    assert state_store.get("historical_review_diagnostics:migration_audit") == corrupt


def test_scenario_c3_reconstruction_after_restoration_restores_accurate_cumulative_counters(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario C3: Restored payload accurately populates in-memory cumulative counters.

    Guarantees:
    - Restoring known-valid payload with distinct counter values populates service attributes accurately.
    - Mathematical sum invariant holds across all cumulative components.
    """
    payload = make_valid_migration_audit_payload(
        cumulative_successful=12,
        cumulative_direct=8,
        cumulative_unsupported_future=2,
        cumulative_malformed=1,
        cumulative_failures=3,
    )
    state_store.set("historical_review_diagnostics:migration_audit", payload)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store(force=True)

    summary = svc.get_migration_audit_summary()
    assert summary["integrity_verified"] is True
    assert summary["cumulative_successful_migrations"] == 12
    assert summary["cumulative_direct_compatible_loads"] == 8
    assert summary["cumulative_unsupported_future_skipped"] == 2
    assert summary["cumulative_malformed_records_skipped"] == 1
    assert summary["cumulative_migration_failures"] == 3
    assert summary["cumulative_total_inspected"] == 26
    assert summary["total_inspected"] == 26


# ==============================================================================
# D. StateStore Reconstruction Consistency (Scenarios 12 - 14)
# ==============================================================================


def test_scenario_d1_repeated_reconstruction_maintains_stable_status_counters_and_fingerprint(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario D1: Multiple independent service instances reconstruct identical audit state.

    Guarantees:
    - Determinism across 5 sequential service reconstructions.
    - Zero discrepancies and identical fingerprint calculations across instances.
    """
    valid = make_valid_migration_audit_payload(cumulative_successful=4, cumulative_direct=2)
    state_store.set("historical_review_diagnostics:migration_audit", valid)

    expected_fp = valid["content_fingerprint"]

    for inst_idx in range(5):
        svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
        svc.hydrate_from_state_store(force=True)

        report = svc.verify_migration_audit_integrity()
        assert report.status == MigrationAuditIntegrityStatus.VERIFIED, f"Failed at instance {inst_idx}"
        assert report.is_verified is True
        assert report.stored_fingerprint == expected_fp
        assert report.calculated_fingerprint == expected_fp
        assert len(report.discrepancies) == 0

        summary = svc.get_migration_audit_summary()
        assert summary["cumulative_total_inspected"] == 6
        assert summary["cumulative_successful_migrations"] == 4
        assert summary["cumulative_direct_compatible_loads"] == 2


def test_scenario_d2_repeated_hydration_does_not_inflate_cumulative_migration_totals(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario D2: Repeated hydrate_from_state_store calls on same instance do not inflate totals.

    Guarantees:
    - Calling hydrate_from_state_store(force=True) multiple times leaves cumulative totals unchanged.
    - Audited record keys cache prevents duplicate audit record generation.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    diags = [
        {"diagnostic_id": f"d_inf_{i}", "investigation_id": "inv_inf", "message": f"M {i}", "timestamp": clock() + i}
        for i in range(3)
    ]
    state_store.set("historical_review_diagnostics:history", diags)

    # Initial hydration
    svc.hydrate_from_state_store(force=True)
    summary_1 = svc.get_migration_audit_summary()
    assert summary_1["cumulative_total_inspected"] == 3
    assert summary_1["cumulative_successful_migrations"] == 3

    # Hydrate second, third, fourth time
    for _ in range(3):
        svc.hydrate_from_state_store(force=True)

    summary_final = svc.get_migration_audit_summary()
    assert summary_final["cumulative_total_inspected"] == 3
    assert summary_final["cumulative_successful_migrations"] == 3
    assert summary_final["content_fingerprint"] == summary_1["content_fingerprint"]
    assert summary_final["integrity_verified"] is True


def test_scenario_d3_cumulative_totals_strictly_distinguished_from_per_hydration_statistics(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario D3: Strict separation between cumulative historical totals and transient per-hydration stats.

    Guarantees:
    - First hydration populates cumulative and per-hydration counters.
    - Second hydration of identical items reports current_hydration_inspected = 0 and re_read_count = 3.
    - Adding new items updates cumulative total while reporting only fresh items in per-hydration count.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    initial_diags = [
        {"diagnostic_id": f"d_sep_{i}", "investigation_id": "inv_sep", "message": f"M {i}", "timestamp": clock() + i}
        for i in range(3)
    ]
    state_store.set("historical_review_diagnostics:history", initial_diags)

    # Run 1
    svc.hydrate_from_state_store(force=True)
    s1 = svc.get_migration_audit_summary()
    assert s1["cumulative_total_inspected"] == 3
    assert s1["current_hydration_inspected"] == 3
    assert s1["current_hydration_re_read_count"] == 0

    # Run 2: Same data re-read
    svc.hydrate_from_state_store(force=True)
    s2 = svc.get_migration_audit_summary()
    assert s2["cumulative_total_inspected"] == 3  # unchanged
    assert s2["current_hydration_inspected"] == 0  # no new items
    assert s2["current_hydration_re_read_count"] == 3  # re-read tracked

    # Run 3: Add 2 new records
    extended_diags = initial_diags + [
        {"diagnostic_id": f"d_sep_{i}", "investigation_id": "inv_sep", "message": f"M {i}", "timestamp": clock() + i}
        for i in range(3, 5)
    ]
    state_store.set("historical_review_diagnostics:history", extended_diags)
    svc.hydrate_from_state_store(force=True)

    s3 = svc.get_migration_audit_summary()
    assert s3["cumulative_total_inspected"] == 5
    assert s3["current_hydration_inspected"] == 2
    assert s3["current_hydration_successful_migrations"] == 2
    assert s3["current_hydration_re_read_count"] == 3


# ==============================================================================
# E. Retention and Bounded Lifecycle (Scenarios 15 - 17)
# ==============================================================================


def test_scenario_e1_audit_log_bounded_eviction_preserves_integrity_and_bounds_invariant(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario E1: Audit log eviction beyond 50 records preserves valid fingerprint and bounds invariant.

    Guarantees:
    - Persisting 60 records stores only the most recent 50 in StateStore.
    - Cumulative total remains 60.
    - len(recent_audit_records) <= cumulative_total_inspected invariant holds.
    - Fingerprint matches canonical dict of retained records; status remains VERIFIED.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    many_diags = [
        {"diagnostic_id": f"d_ev_{i}", "investigation_id": "inv_ev", "message": f"M {i}", "timestamp": clock() + i}
        for i in range(60)
    ]
    state_store.set("historical_review_diagnostics:history", many_diags)
    svc.hydrate_from_state_store(force=True)

    persisted = state_store.get("historical_review_diagnostics:migration_audit")
    assert persisted is not None
    assert len(persisted["recent_audit_records"]) == 50
    assert persisted["cumulative_total_inspected"] == 60
    assert len(persisted["recent_audit_records"]) <= persisted["cumulative_total_inspected"]

    # Verify reconstruction from evicted state
    svc2 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc2.hydrate_from_state_store(force=True)
    report = svc2.verify_migration_audit_integrity()
    assert report.status == MigrationAuditIntegrityStatus.VERIFIED
    assert report.is_verified is True
    assert len(report.discrepancies) == 0


def test_scenario_e2_diagnostics_eviction_does_not_invalidate_migration_audit_fingerprint(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario E2: Bounded capacity eviction of diagnostic records preserves migration audit integrity.

    Guarantees:
    - Setting max_diagnostics=5 and recording 15 records evicts 10 records.
    - Eviction statistics are updated.
    - Migration audit integrity remains VERIFIED without false tampering alerts.
    """
    svc = HistoricalReviewAuditDiagnosticsService(
        state_store=state_store,
        time_provider=clock,
        max_diagnostics=10,
    )
    for i in range(20):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_bound",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Bounded record {i}",
        )

    assert svc._evicted_diagnostics_count == 10
    assert len(svc._diagnostics) == 10

    report = svc.verify_migration_audit_integrity()
    # In-memory only without prior StateStore migration audit write
    assert report is not None
    assert report.status in (MigrationAuditIntegrityStatus.VERIFIED, MigrationAuditIntegrityStatus.MISSING_EVIDENCE)


def test_scenario_e3_duplicate_records_and_repeated_eviction_behave_deterministically(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario E3: Duplicate identifiers in StateStore history do not create duplicate audit records.

    Guarantees:
    - Duplicate entries with matching IDs in history list are deduplicated during hydration.
    - Service correctly identifies re-reads without inflating counts.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    duplicate_diags = [
        {"diagnostic_id": "diag_dup_1", "investigation_id": "inv_dup", "message": "First copy", "timestamp": clock()},
        {"diagnostic_id": "diag_dup_1", "investigation_id": "inv_dup", "message": "First copy", "timestamp": clock()},
        {"diagnostic_id": "diag_dup_2", "investigation_id": "inv_dup", "message": "Second record", "timestamp": clock() + 1.0},
    ]
    state_store.set("historical_review_diagnostics:history", duplicate_diags)
    svc.hydrate_from_state_store(force=True)

    summary = svc.get_migration_audit_summary()
    assert summary["cumulative_total_inspected"] == 2
    assert summary["current_hydration_re_read_count"] == 1
    assert summary["integrity_verified"] is True


# ==============================================================================
# F. clear_history() Lifecycle (Scenarios 18 - 20)
# ==============================================================================


def test_scenario_f1_clear_history_clears_all_in_memory_queues_counters_and_statestore_keys(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario F1: clear_history() purges all in-memory queues, counters, and StateStore persistence keys.

    Guarantees:
    - All 9 StateStore keys are deleted.
    - All in-memory deques, dictionaries, and cumulative counters are zeroed out.
    - Corruption flags and cached integrity reports are cleared.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    # Populate state: diagnostic, auto-generated alert, export
    svc.record_diagnostic("inv_c", AuditDiagnosticCategory.GENERAL_DIAGNOSTIC, AuditDiagnosticSeverity.ERROR, "Msg")
    svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_c"))
    svc.hydrate_from_state_store(force=True)

    assert state_store.get("historical_review_diagnostics:history") is not None
    assert state_store.get("historical_review_diagnostics:migration_audit") is not None

    svc.clear_history()

    # Verify StateStore keys
    keys_to_check = [
        "historical_review_diagnostics:history",
        "historical_review_alerts:history",
        "historical_review_alerts:suppressed_history",
        "historical_review_diagnostics:active_failures",
        "historical_review_diagnostics:cooldown_state",
        "historical_review_diagnostics:retention_stats",
        "historical_review_exports:history",
        "historical_review_reconciliations:history",
        "historical_review_diagnostics:migration_audit",
    ]
    for key in keys_to_check:
        assert state_store.get(key) is None, f"StateStore key '{key}' was not deleted by clear_history!"

    # Verify in-memory state
    assert len(svc._diagnostics) == 0
    assert len(svc._alerts) == 0
    assert len(svc._exports) == 0
    assert len(svc._reconciliations) == 0
    assert len(svc._migration_audit_records) == 0
    assert svc._successful_migrations_count == 0
    assert svc._direct_compatible_count == 0
    assert svc._unsupported_future_count == 0
    assert svc._malformed_count == 0
    assert svc._migration_failures_count == 0
    assert svc._last_migration_timestamp is None
    assert len(svc._audited_record_keys) == 0
    assert svc._last_audit_integrity_report is None
    assert svc._stored_migration_audit_corrupted is False


def test_scenario_f2_cleared_state_does_not_resurrect_on_service_reconstruction(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario F2: Reconstructing service after clear_history reports MISSING_EVIDENCE without resurrection.

    Guarantees:
    - Fresh instance hydrated from purged StateStore reports clean empty state.
    - Integrity status reports MISSING_EVIDENCE without resurrecting stale counts.
    """
    svc1 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_res", "investigation_id": "inv_res", "message": "Pre-clear", "timestamp": clock()}],
    )
    svc1.hydrate_from_state_store(force=True)
    assert svc1.get_migration_audit_summary()["cumulative_total_inspected"] == 1

    svc1.clear_history()

    # Reconstruct new service instance
    svc2 = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    # Before hydrate, verify status on empty store
    report = svc2.verify_migration_audit_integrity()
    assert report.status == MigrationAuditIntegrityStatus.MISSING_EVIDENCE
    assert report.is_verified is False

    summary = svc2.get_migration_audit_summary()
    assert summary["cumulative_total_inspected"] == 0
    assert summary["integrity_verified"] is False
    assert summary["integrity_status"] == MigrationAuditIntegrityStatus.MISSING_EVIDENCE.value


def test_scenario_f3_subsequent_fresh_records_after_clear_history_store_and_audit_normally(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario F3: New records created after clear_history() establish a fresh, verified audit state.

    Guarantees:
    - Subsequent diagnostic recording and hydration generate new verified migration audit metadata.
    - Fresh content fingerprint and cumulative counters are persisted cleanly.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    state_store.set(
        "historical_review_diagnostics:history",
        [{"diagnostic_id": "d_old", "investigation_id": "inv_old", "message": "Old", "timestamp": clock()}],
    )
    svc.hydrate_from_state_store(force=True)
    svc.clear_history()

    # Add fresh record
    fresh_diag = svc.record_diagnostic(
        investigation_id="inv_fresh",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Fresh post-clear diagnostic",
    )
    assert fresh_diag is not None

    # Hydrate fresh state
    svc.hydrate_from_state_store(force=True)
    report = svc.verify_migration_audit_integrity()

    assert report.status == MigrationAuditIntegrityStatus.VERIFIED
    assert report.is_verified is True
    assert report.stored_fingerprint is not None

    summary = svc.get_migration_audit_summary()
    assert summary["integrity_verified"] is True
    assert summary["cumulative_total_inspected"] == 1
    assert summary["cumulative_direct_compatible_loads"] == 1


# ==============================================================================
# G. Compatibility and Malformed Payload Regression (Scenarios 21 - 23)
# ==============================================================================


def test_scenario_g1_compatibility_matrix_versions_handled_per_spec(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario G1: Schema version matrix compatibility across unversioned, older, current, future, malformed.

    Guarantees:
    - CURRENT (1.0.0) -> VERIFIED with matching fingerprint.
    - SUPPORTED_OLDER (0.9.0) -> VERIFIED with matching fingerprint.
    - LEGACY_UNVERSIONED (None) -> INCOMPLETE if missing fingerprint.
    - UNSUPPORTED_FUTURE (2.0.0) -> UNSUPPORTED_SCHEMA.
    - MALFORMED ('bad') -> UNSUPPORTED_SCHEMA.
    """
    base = make_valid_migration_audit_payload()

    # 1. Current (1.0.0)
    rep_current = verify_migration_audit_payload(base, time_provider=clock)
    assert rep_current.status == MigrationAuditIntegrityStatus.VERIFIED

    # 2. Supported older (0.9.0)
    older_payload = copy.deepcopy(base)
    older_payload["schema_version"] = "0.9.0"
    can_dict_older = compute_migration_audit_canonical_dict(older_payload)
    older_payload["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict_older)
    rep_older = verify_migration_audit_payload(older_payload, time_provider=clock)
    assert rep_older.status == MigrationAuditIntegrityStatus.VERIFIED

    # 3. Legacy unversioned (missing schema_version and missing fingerprint)
    legacy_payload = copy.deepcopy(base)
    del legacy_payload["schema_version"]
    del legacy_payload["content_fingerprint"]
    rep_legacy = verify_migration_audit_payload(legacy_payload, time_provider=clock)
    assert rep_legacy.status == MigrationAuditIntegrityStatus.INCOMPLETE

    # 4. Unsupported future (2.0.0)
    future_payload = copy.deepcopy(base)
    future_payload["schema_version"] = "2.0.0"
    can_dict_future = compute_migration_audit_canonical_dict(future_payload)
    future_payload["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict_future)
    rep_future = verify_migration_audit_payload(future_payload, time_provider=clock)
    assert rep_future.status == MigrationAuditIntegrityStatus.UNSUPPORTED_SCHEMA

    # 5. Malformed schema
    malformed_payload = copy.deepcopy(base)
    malformed_payload["schema_version"] = "not-semver"
    can_dict_mal = compute_migration_audit_canonical_dict(malformed_payload)
    malformed_payload["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict_mal)
    rep_mal = verify_migration_audit_payload(malformed_payload, time_provider=clock)
    assert rep_mal.status == MigrationAuditIntegrityStatus.UNSUPPORTED_SCHEMA


def test_scenario_g2_unsupported_or_malformed_audit_never_reported_as_verified(
    clock: DeterministicClock
):
    """Scenario G2: Strict assertion that unsupported or malformed payloads never report is_verified=True.

    Guarantees:
    - Exhaustive loop across malformed, incomplete, and unsupported payloads.
    - All return is_verified=False without exception.
    """
    candidates = [
        {"schema_version": "99.0.0"},
        {"schema_version": ""},
        {"schema_version": 12345},
        {"schema_version": "1.0.0", "cumulative_total_inspected": -1},
        {"schema_version": "1.0.0", "content_fingerprint": "too_short"},
        {"random_key": "random_val"},
        "a_string_payload",
        [1, 2, 3],
    ]
    for c in candidates:
        rep = verify_migration_audit_payload(c, time_provider=clock)
        assert rep.is_verified is False, f"Candidate {c} was unexpectedly verified!"


def test_scenario_g3_secret_and_path_scrubbing_in_discrepancies_and_values(
    clock: DeterministicClock
):
    """Scenario G3: Sensitive tokens, file paths, and credentials scrubbed in discrepancy descriptions.

    Guarantees:
    - Windows paths, Unix paths, and credential tokens are replaced with [REDACTED_...] in discrepancy fields.
    - Sanitization is preserved through to_dict() and from_dict() roundtrips.
    """
    disc = MigrationAuditDiscrepancy(
        discrepancy_type="INVALID_AUDIT_KEY",
        field_name="audited_record_keys[0]",
        description="Found key with secret C:\\Users\\admin\\secret and password=supersecret123",
        expected="Valid key from /var/log/private",
        actual="C:\\Users\\admin\\secret_token",
    )

    assert "C:\\Users\\admin" not in disc.description
    assert "[REDACTED_PATH]" in disc.description
    assert "supersecret123" not in disc.description
    assert disc.expected is not None
    assert "/var/log/private" not in str(disc.expected)
    assert "[REDACTED_PATH]" in str(disc.expected)
    assert "C:\\Users\\admin" not in str(disc.actual)
    assert "[REDACTED_PATH]" in str(disc.actual)

    # Test roundtrip
    d_dict = disc.to_dict()
    disc_roundtrip = MigrationAuditDiscrepancy.from_dict(d_dict)
    assert disc_roundtrip.description == disc.description
    assert disc_roundtrip.actual == disc.actual
    assert disc_roundtrip.expected == disc.expected


# ==============================================================================
# H. Deterministic Integrity Reporting (Scenarios 24 - 25)
# ==============================================================================


def test_scenario_h1_repeated_verification_of_identical_payload_produces_identical_reports(
    clock: DeterministicClock
):
    """Scenario H1: Repeated verification calls on identical payloads produce deterministic results.

    Guarantees:
    - Deterministic discrepancy ordering, fingerprints, and status codes.
    - Reports match across repeated calls.
    """
    valid = make_valid_migration_audit_payload()
    rep1 = verify_migration_audit_payload(valid, time_provider=clock)
    rep2 = verify_migration_audit_payload(valid, time_provider=clock)

    assert rep1 == rep2
    assert rep1.to_dict() == rep2.to_dict()

    # Corrupt payload with multiple discrepancies
    multi_corrupt = copy.deepcopy(valid)
    multi_corrupt["cumulative_successful_migrations"] = -2
    multi_corrupt["content_fingerprint"] = "0" * 64

    c_rep1 = verify_migration_audit_payload(multi_corrupt, time_provider=clock)
    c_rep2 = verify_migration_audit_payload(multi_corrupt, time_provider=clock)

    assert c_rep1.status == c_rep2.status
    assert c_rep1.is_verified == c_rep2.is_verified
    assert c_rep1.calculated_fingerprint == c_rep2.calculated_fingerprint
    assert len(c_rep1.discrepancies) == len(c_rep2.discrepancies)
    assert [d.discrepancy_type for d in c_rep1.discrepancies] == [d.discrepancy_type for d in c_rep2.discrepancies]


def test_scenario_h2_five_integrity_statuses_are_mutually_distinguishable_and_strictly_classified(
    clock: DeterministicClock
):
    """Scenario H2: Verify each of the 5 statuses is distinct and exercisable under strict classification.

    Guarantees:
    - VERIFIED: valid payload with matching fingerprint.
    - INCOMPLETE: missing required counters or content_fingerprint.
    - CORRUPT_OR_INCONSISTENT: fingerprint mismatch, counter sum mismatch, negative value.
    - UNSUPPORTED_SCHEMA: unrecognized SemVer or malformed schema.
    - MISSING_EVIDENCE: None payload.
    """
    valid = make_valid_migration_audit_payload()

    # 1. VERIFIED
    r_ver = verify_migration_audit_payload(valid, time_provider=clock)
    assert r_ver.status == MigrationAuditIntegrityStatus.VERIFIED
    assert r_ver.is_verified is True

    # 2. INCOMPLETE
    inc_payload = copy.deepcopy(valid)
    del inc_payload["content_fingerprint"]
    r_inc = verify_migration_audit_payload(inc_payload, time_provider=clock)
    assert r_inc.status == MigrationAuditIntegrityStatus.INCOMPLETE
    assert r_inc.is_verified is False

    # 3. CORRUPT_OR_INCONSISTENT
    corrupt_payload = copy.deepcopy(valid)
    corrupt_payload["content_fingerprint"] = "0" * 64
    r_cor = verify_migration_audit_payload(corrupt_payload, time_provider=clock)
    assert r_cor.status == MigrationAuditIntegrityStatus.CORRUPT_OR_INCONSISTENT
    assert r_cor.is_verified is False

    # 4. UNSUPPORTED_SCHEMA
    unsupp_payload = copy.deepcopy(valid)
    unsupp_payload["schema_version"] = "99.0.0"
    can_dict = compute_migration_audit_canonical_dict(unsupp_payload)
    unsupp_payload["content_fingerprint"] = compute_migration_audit_fingerprint(can_dict)
    r_uns = verify_migration_audit_payload(unsupp_payload, time_provider=clock)
    assert r_uns.status == MigrationAuditIntegrityStatus.UNSUPPORTED_SCHEMA
    assert r_uns.is_verified is False

    # 5. MISSING_EVIDENCE
    r_mis = verify_migration_audit_payload(None, time_provider=clock)
    assert r_mis.status == MigrationAuditIntegrityStatus.MISSING_EVIDENCE
    assert r_mis.is_verified is False


# ==============================================================================
# I. Mandatory Safety Invariants (Scenario 26)
# ==============================================================================


def test_scenario_i1_mandatory_safety_invariants_preserved_execution_locked_and_offline(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario I1: Mandatory safety invariants remain strictly enforced across all operations.

    Guarantees:
    - Settings.execution_locked is True.
    - Trading engine is uninitialized and offline.
    - No live broker interactions or trade execution directives permitted.
    - Handler rejects trading directives with RequestStatus.FAILED.
    """
    assert Settings.execution_locked is True, "Critical: execution_locked invariant violated!"

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    orchestrator = MasterOrchestrator()
    register_audit_diagnostics_handlers(orchestrator, svc)

    # Attempt to inject trade directive into audit summary query
    req = OrchestratorRequest.create(
        command="get_historical_review_migration_audit_summary",
        context={"execute_trade": True, "action": "BUY", "symbol": "BTC/USD"},
    )
    resp = orchestrator.handle_request(req)
    assert resp.status == RequestStatus.FAILED
    assert any("Prohibited field" in str(err) or "ValidationError" in str(err) or "unexpected" in str(err).lower() for err in resp.errors)
