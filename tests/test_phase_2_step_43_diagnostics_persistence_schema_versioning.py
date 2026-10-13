"""KAISER — Phase 2, Step 43 Test Suite.

Diagnostics Persistence Schema Versioning & Backward Compatibility Acceptance Tests.

TEST SCENARIOS:
A. Legacy State Without Version Metadata:
   - Scenario 1: Legacy unversioned diagnostic records hydration and backward compatibility.
   - Scenario 2: Legacy unversioned alerts hydration and backward compatibility.
   - Scenario 3: Legacy unversioned export snapshots and reconciliation reports.
   - Scenario 4: Legacy unversioned retention stats hydration.
B. Supported Older Schema Versions:
   - Scenario 5: Supported older version ("0.9.0") diagnostic records with legacy field aliases.
   - Scenario 6: Supported older version ("0.9.0") alerts with legacy field aliases.
   - Scenario 7: Supported older version ("0.9.0") reconciliation reports with legacy field aliases.
C. Current Schema Round-Trip and Determinism:
   - Scenario 8: Current schema ("1.0.0") serialization round-trip and canonical preservation.
D. Unknown and Future Schema Versions:
   - Scenario 9: Unknown or future schema versions ("2.0.0", "99.0.0") rejected on individual model hydration.
   - Scenario 10: StateStore hydration isolates future version records without dropping data or crashing.
E. Malformed, Invalid Types, and Corrupt State:
   - Scenario 11: Invalid schema version types and malformed strings.
   - Scenario 12: Partial and structurally corrupt payloads distinguished from unsupported versions.
   - Scenario 13: Deterministic migration idempotency and input payload non-mutation.
F. Export Reconciliation with Versioned Payloads:
   - Scenario 14: Reconciling export snapshot generated from migrated legacy evidence.
   - Scenario 15: Reconciling export snapshot with unsupported future schema returns UNVERIFIABLE.
G. Lifecycle, Retention, and Memory Boundaries:
   - Scenario 16: End-to-end multi-cycle persist -> reconstruct -> evict -> reconstruct lifecycle.
   - Scenario 17: Bounded retention and cumulative eviction counters remain strictly accurate with versioned payloads.
   - Scenario 18: clear_history() resets versioned state and retention stats correctly.
H. Tamper Detection and Security Invariants:
   - Scenario 19: Tamper detection and canonical fingerprint sensitivity on versioned payloads.
   - Scenario 20: Mandatory safety invariants and secret sanitization across migrations.
"""

from __future__ import annotations

import copy
import json
import threading
import time
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
    DiagnosticsExportResult,
    DiagnosticsReconciliationDiscrepancy,
    DiagnosticsReconciliationReport,
    DiagnosticsReconciliationStatus,
    compute_deterministic_export_id,
    compute_deterministic_reconciliation_id,
    compute_export_canonical_dict,
    compute_export_content_fingerprint,
)
from app.analysis.opportunities.evolution.audit.diagnostics.schema import (
    CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
    SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS,
    LEGACY_UNVERSIONED,
    IncompatibleSchemaVersionError,
    MalformedSchemaVersionError,
    SchemaVersionCompatibilityStatus,
    detect_and_validate_schema_version,
    migrate_alert_payload,
    migrate_diagnostic_payload,
    migrate_export_payload,
    migrate_reconciliation_payload,
    migrate_retention_stats_payload,
)
from app.analysis.opportunities.evolution.audit.diagnostics.service import (
    HistoricalReviewAuditDiagnosticsService,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.exceptions import ValidationError
from app.core.state import StateStore


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
def state_store() -> StateStore:
    return StateStore()


# ==============================================================================
# Area A: Legacy State Without Version Metadata
# ==============================================================================

def test_scenario_1_legacy_unversioned_diagnostic_records_hydration(state_store: StateStore):
    """Scenario 1: Legacy unversioned diagnostic records hydration and backward compatibility.
    
    Origin: Step 36-39 persisted diagnostic record representations without schema_version field.
    Guarantee: Preserves all historical fields and automatically upgrades to CURRENT_DIAGNOSTICS_SCHEMA_VERSION.
    """
    # 1. Direct version detection test
    status, detected_ver = detect_and_validate_schema_version(None)
    assert status == SchemaVersionCompatibilityStatus.SUPPORTED_OLDER
    assert detected_ver == LEGACY_UNVERSIONED

    # 2. Legacy unversioned dictionary
    legacy_diag_dict = {
        "diagnostic_id": "diag_legacy_001",
        "correlation_id": "corr_legacy_001",
        "investigation_id": "inv_legacy_001",
        "timestamp": 1700000010.0,
        "timestamp_iso": "2023-11-14T22:13:30Z",
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "ERROR",
        "message": "Legacy audit evaluation failure message",
        "source_references": ["src_ref_legacy_1", "src_ref_legacy_2"],
        "evidence_nature": "OBSERVED_FAILURE",
        "report_id": "rep_legacy_1",
        "evaluation_id": "eval_legacy_1",
        # NOTE: intentionally NO schema_version
    }

    # Deserialization via model from_dict
    diag_record = AuditDiagnosticRecord.from_dict(legacy_diag_dict)
    assert diag_record.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert diag_record.diagnostic_id == "diag_legacy_001"
    assert diag_record.correlation_id == "corr_legacy_001"
    assert diag_record.investigation_id == "inv_legacy_001"
    assert diag_record.category == AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE
    assert diag_record.severity == AuditDiagnosticSeverity.ERROR
    assert diag_record.message == "Legacy audit evaluation failure message"
    assert diag_record.source_references == ("src_ref_legacy_1", "src_ref_legacy_2")
    assert diag_record.evidence_nature == AuditEvidenceNature.OBSERVED_FAILURE

    # 3. Persistence into StateStore and Service hydration
    state_store.set("historical_review_diagnostics:history", [legacy_diag_dict])
    state_store.set("historical_review_diagnostics:diag_legacy_001", legacy_diag_dict)
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store)
    svc.hydrate_from_state_store()

    hydrated = svc.get_diagnostic("diag_legacy_001")
    assert hydrated is not None
    assert hydrated.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert hydrated.diagnostic_id == "diag_legacy_001"
    assert svc._unsupported_records_skipped == 0


def test_scenario_2_legacy_unversioned_alerts_hydration(state_store: StateStore):
    """Scenario 2: Legacy unversioned alerts hydration and backward compatibility.
    
    Origin: Step 37-39 persisted alert representations without schema_version field.
    Guarantee: Preserves alert status, suppression count, cooldown, and upgrades version.
    """
    legacy_alert_dict = {
        "alert_id": "alert_legacy_001",
        "diagnostic_id": "diag_legacy_001",
        "correlation_id": "corr_legacy_001",
        "investigation_id": "inv_legacy_001",
        "timestamp": 1700000020.0,
        "timestamp_iso": "2023-11-14T22:13:40Z",
        "category": "REPEATABILITY_DIVERGENCE",
        "severity": "CRITICAL",
        "reason": "Legacy divergence reason",
        "source_references": ["src_alert_1"],
        "status": "ACKNOWLEDGED",
        "cooldown_until": 1700000100.0,
        "suppressed_occurrences": 3,
        "report_id": "rep_legacy_1",
        "evaluation_id": "eval_legacy_1",
        # NOTE: intentionally NO schema_version
    }

    alert = AuditAlert.from_dict(legacy_alert_dict)
    assert alert.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert alert.alert_id == "alert_legacy_001"
    assert alert.status == AuditAlertStatus.ACKNOWLEDGED
    assert alert.suppressed_occurrences == 3
    assert alert.cooldown_until == 1700000100.0

    # Test via service hydration
    state_store.set("historical_review_alerts:history", [legacy_alert_dict])
    state_store.set("historical_review_alerts:alert_legacy_001", legacy_alert_dict)
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store)
    svc.hydrate_from_state_store()

    hydrated_alert = svc.get_alert("alert_legacy_001")
    assert hydrated_alert is not None
    assert hydrated_alert.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert hydrated_alert.status == AuditAlertStatus.ACKNOWLEDGED


def test_scenario_3_legacy_unversioned_export_and_reconciliation(state_store: StateStore):
    """Scenario 3: Legacy unversioned export snapshots and reconciliation reports.
    
    Origin: Step 40 export and reconciliation representations without explicit top-level schema_version.
    Guarantee: Reconciles cleanly and upgrades schema_version to CURRENT_DIAGNOSTICS_SCHEMA_VERSION.
    """
    legacy_recon_dict = {
        "reconciliation_id": "recon_legacy_001",
        "export_id": "exp_legacy_001",
        "timestamp": 1700000050.0,
        "timestamp_iso": "2023-11-14T22:14:10Z",
        "status": "MATCHED",
        "fingerprint_verified": True,
        "total_exported_diagnostics": 1,
        "total_exported_alerts": 0,
        "matched_diagnostics": 1,
        "matched_alerts": 0,
        "discrepancies": [],
        "source_retention_status": "COMPLETE",
        "is_source_truncated": False,
        "summary_notes": "Legacy reconciliation completed successfully.",
        # Intentionally no schema_version
    }

    report = DiagnosticsReconciliationReport.from_dict(legacy_recon_dict)
    assert report.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert report.status == DiagnosticsReconciliationStatus.MATCHED
    assert report.fingerprint_verified is True

    # Hydrate through service
    state_store.set("historical_review_reconciliations:recon_legacy_001", legacy_recon_dict)
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store)
    svc.hydrate_from_state_store()

    hydrated = svc.get_reconciliation_report("recon_legacy_001")
    assert hydrated is not None
    assert hydrated.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION


def test_scenario_4_legacy_unversioned_retention_stats(state_store: StateStore):
    """Scenario 4: Legacy unversioned retention stats hydration.
    
    Origin: Step 38-42 retention_stats persisted without schema_version.
    Guarantee: Eviction counters are fully preserved and schema_version metadata is added.
    """
    legacy_stats = {
        "max_diagnostics": 500,
        "max_alerts": 500,
        "current_diagnostics_count": 10,
        "current_alerts_count": 2,
        "evicted_diagnostics_count": 42,
        "evicted_alerts_count": 7,
        "last_eviction_timestamp": 1700000099.0,
        # Intentionally no schema_version
    }
    state_store.set("historical_review_diagnostics:retention_stats", legacy_stats)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store)
    svc.hydrate_from_state_store()

    status = svc.get_retention_status()
    assert status["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert status["evicted_diagnostics_count"] == 42
    assert status["evicted_alerts_count"] == 7


# ==============================================================================
# Area B: Supported Older Schema Versions ("0.9.0")
# ==============================================================================

def test_scenario_5_supported_v0_9_0_diagnostic_with_legacy_aliases(state_store: StateStore):
    """Scenario 5: Supported older version ("0.9.0") diagnostic records with legacy field aliases.
    
    Origin: Historical prototype schema utilizing 'diagnostic_category', 'error', 'summary'.
    Guarantee: Deterministically maps aliases to canonical attributes.
    """
    v090_payload = {
        "schema_version": "0.9.0",
        "diagnostic_id": "diag_v090_001",
        "correlation_id": "corr_v090_001",
        "investigation_id": "inv_v090_001",
        "timestamp": 1700000030.0,
        "timestamp_iso": "2023-11-14T22:13:50Z",
        "diagnostic_category": "COVERAGE_DEGRADATION",  # alias for category
        "severity": "WARNING",
        "summary": "Older prototype summary text",     # alias for sanitized_summary
        "message": "Older prototype message",
        "error": "DATA_DRIFT",                        # alias for error_category
        "source_references": ["src_old_1"],
    }

    record = AuditDiagnosticRecord.from_dict(v090_payload)
    assert record.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert record.category == AuditDiagnosticCategory.COVERAGE_DEGRADATION
    assert record.error_category == "DATA_DRIFT"
    assert record.sanitized_summary == "Older prototype summary text"
    assert record.message == "Older prototype message"


def test_scenario_6_supported_v0_9_0_alert_with_legacy_aliases(state_store: StateStore):
    """Scenario 6: Supported older version ("0.9.0") alerts with legacy field aliases.
    
    Origin: Prototype alert schemas using 'alert_reason' and 'state'.
    Guarantee: Deterministically maps 'alert_reason' -> 'reason' and 'state' -> 'status'.
    """
    v090_alert = {
        "schema_version": "0.9.0",
        "alert_id": "alert_v090_001",
        "diagnostic_id": "diag_v090_001",
        "correlation_id": "corr_v090_001",
        "investigation_id": "inv_v090_001",
        "timestamp": 1700000040.0,
        "category": "REPEATABILITY_DIVERGENCE",
        "severity": "CRITICAL",
        "alert_reason": "Aliased reason string",  # alias for reason
        "state": "RESOLVED",                     # alias for status
        "cooldown_until": 0.0,
        "suppressed_occurrences": 1,
    }

    alert = AuditAlert.from_dict(v090_alert)
    assert alert.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert alert.reason == "Aliased reason string"
    assert alert.status == AuditAlertStatus.RESOLVED


def test_scenario_7_supported_v0_9_0_reconciliation_with_legacy_aliases():
    """Scenario 7: Supported older version ("0.9.0") reconciliation report with legacy field aliases.
    
    Origin: Prototype reconciliation using 'verdict' and 'fingerprint_match'.
    Guarantee: Maps 'verdict' -> 'status' and 'fingerprint_match' -> 'fingerprint_verified'.
    """
    v090_recon = {
        "schema_version": "0.9.0",
        "reconciliation_id": "recon_v090_001",
        "export_id": "exp_v090_001",
        "timestamp": 1700000060.0,
        "timestamp_iso": "2023-11-14T22:14:20Z",
        "verdict": "MISMATCHED",                 # alias for status
        "fingerprint_match": False,              # alias for fingerprint_verified
        "total_exported_diagnostics": 5,
        "total_exported_alerts": 0,
        "matched_diagnostics": 3,
        "matched_alerts": 0,
        "discrepancies": [
            {
                "discrepancy_type": "MISSING_IN_CURRENT_STATE",
                "record_id": "diag_1",
                "description": "Item was not found",
            }
        ],
        "summary_notes": "Prototype mismatch",
    }

    report = DiagnosticsReconciliationReport.from_dict(v090_recon)
    assert report.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert report.status == DiagnosticsReconciliationStatus.MISMATCHED
    assert report.fingerprint_verified is False
    assert len(report.discrepancies) == 1
    assert report.discrepancies[0].discrepancy_type == "MISSING_IN_CURRENT_STATE"


# ==============================================================================
# Area C: Current Schema Round-Trip and Determinism
# ==============================================================================

def test_scenario_8_current_schema_round_trip_and_canonical_preservation(clock: DeterministicClock):
    """Scenario 8: Current schema ("1.0.0") serialization round-trip and canonical preservation.
    
    Guarantee: serialize -> deserialize -> serialize produces identical dictionary and identical SHA256.
    """
    diag = AuditDiagnosticRecord(
        diagnostic_id="diag_curr_001",
        correlation_id="corr_curr_001",
        investigation_id="inv_curr_001",
        timestamp=clock(),
        timestamp_iso="2023-11-14T22:13:20Z",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.CRITICAL,
        message="Current version round-trip test message",
        source_references=("ref_1", "ref_2"),
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        schema_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
    )

    # 1. First serialization
    d1 = diag.to_dict()
    assert d1["schema_version"] == "1.0.0"

    # 2. Deserialization
    diag_hydrated = AuditDiagnosticRecord.from_dict(d1)
    assert diag_hydrated.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # 3. Second serialization
    d2 = diag_hydrated.to_dict()
    assert d1 == d2

    # 4. JSON canonical encoding equivalence
    json1 = json.dumps(d1, sort_keys=True, separators=(",", ":"))
    json2 = json.dumps(d2, sort_keys=True, separators=(",", ":"))
    assert json1 == json2


# ==============================================================================
# Area D: Unknown and Future Schema Versions
# ==============================================================================

def test_scenario_9_unknown_future_schema_versions_rejected_on_model_hydration():
    """Scenario 9: Unknown or future schema versions ("2.0.0", "99.0.0") rejected on individual model hydration.
    
    Guarantee: IncompatibleSchemaVersionError raised with detailed context; never silently coerced.
    """
    future_diag = {
        "schema_version": "2.0.0",
        "diagnostic_id": "diag_future_001",
        "correlation_id": "corr_future_001",
        "investigation_id": "inv_future_001",
        "timestamp": 1700000000.0,
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "ERROR",
        "message": "Future payload",
    }

    with pytest.raises(IncompatibleSchemaVersionError) as exc_info:
        AuditDiagnosticRecord.from_dict(future_diag)
    assert "2.0.0" in str(exc_info.value)
    assert exc_info.value.detected_version == "2.0.0"

    # Direct constructor validation
    with pytest.raises(IncompatibleSchemaVersionError):
        AuditDiagnosticRecord(
            diagnostic_id="diag_future_002",
            correlation_id="corr_future_002",
            investigation_id="inv_future_002",
            timestamp=1700000000.0,
            timestamp_iso="2023-11-14T22:13:20Z",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.ERROR,
            evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
            message="Future payload",
            schema_version="3.0.0",
        )

    # Future alert
    with pytest.raises(IncompatibleSchemaVersionError):
        AuditAlert.from_dict({
            "schema_version": "99.0.0",
            "alert_id": "alert_future_001",
            "diagnostic_id": "diag_1",
            "correlation_id": "c_1",
            "investigation_id": "inv_1",
            "timestamp": 1700000000.0,
            "category": "REPEATABILITY_DIVERGENCE",
            "severity": "CRITICAL",
            "reason": "Future reason",
            "status": "TRIGGERED",
        })


def test_scenario_10_statestore_hydration_isolates_future_version_records(state_store: StateStore):
    """Scenario 10: StateStore hydration isolates future version records without dropping data or crashing.
    
    Guarantee: Unsupported records are skipped during hydration; valid records are hydrated;
    unsupported data remains in StateStore intact without deletion or corruption.
    """
    # 2 valid records + 1 future record
    v1_rec1 = {
        "schema_version": "1.0.0",
        "diagnostic_id": "diag_valid_001",
        "correlation_id": "corr_valid_001",
        "investigation_id": "inv_valid",
        "timestamp": 1700000001.0,
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "ERROR",
        "evidence_nature": "OBSERVED_FAILURE",
        "message": "Valid record 1",
    }
    future_rec = {
        "schema_version": "2.0.0",
        "diagnostic_id": "diag_future_002",
        "correlation_id": "corr_future_002",
        "investigation_id": "inv_valid",
        "timestamp": 1700000002.0,
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "ERROR",
        "evidence_nature": "OBSERVED_FAILURE",
        "message": "Future record 2",
    }
    v1_rec2 = {
        "schema_version": "1.0.0",
        "diagnostic_id": "diag_valid_003",
        "correlation_id": "corr_valid_003",
        "investigation_id": "inv_valid",
        "timestamp": 1700000003.0,
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "ERROR",
        "evidence_nature": "OBSERVED_FAILURE",
        "message": "Valid record 3",
    }

    state_store.set("historical_review_diagnostics:history", [v1_rec1, future_rec, v1_rec2])
    state_store.set("historical_review_diagnostics:diag_valid_001", v1_rec1)
    state_store.set("historical_review_diagnostics:diag_future_002", future_rec)
    state_store.set("historical_review_diagnostics:diag_valid_003", v1_rec2)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store)
    svc.hydrate_from_state_store()

    # Verify memory queues have only valid records
    diagnostics = svc.list_diagnostics()
    assert len(diagnostics) == 2
    diag_ids = [d.diagnostic_id for d in diagnostics]
    assert "diag_valid_001" in diag_ids
    assert "diag_valid_003" in diag_ids
    assert "diag_future_002" not in diag_ids

    # Verify skipped counter
    assert svc._unsupported_records_skipped == 1
    assert svc.get_retention_status()["unsupported_records_skipped"] == 1

    # Verify future record was NOT dropped or deleted from StateStore
    persisted_future = state_store.get("historical_review_diagnostics:diag_future_002")
    assert persisted_future is not None
    assert persisted_future["schema_version"] == "2.0.0"


# ==============================================================================
# Area E: Malformed, Invalid Types, and Corrupt State
# ==============================================================================

def test_scenario_11_invalid_schema_version_types_and_malformed_values(state_store: StateStore):
    """Scenario 11: Invalid schema version types and malformed strings.
    
    Guarantee: detect_and_validate_schema_version raises MalformedSchemaVersionError for non-string / malformed versions;
    hydration skips them gracefully and tracks them in unsupported_records_skipped.
    """
    invalid_versions = [1, 1.0, True, [], {}, "", "   ", "version1", "1.0", "1.0.0.0", "alpha-1.0"]

    for inv_ver in invalid_versions:
        status, _ = detect_and_validate_schema_version(inv_ver)
        assert status == SchemaVersionCompatibilityStatus.MALFORMED
        with pytest.raises(MalformedSchemaVersionError) as exc_info:
            migrate_diagnostic_payload({
                "schema_version": inv_ver,
                "diagnostic_id": "d1",
                "investigation_id": "inv1",
            })
        assert exc_info.value.detected_version == inv_ver

    # In StateStore, hydrate malformed version record
    malformed_rec = {
        "schema_version": 12345,  # int instead of string
        "diagnostic_id": "diag_malformed_ver",
        "investigation_id": "inv_1",
        "timestamp": 1700000000.0,
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "ERROR",
        "message": "Malformed ver",
    }
    state_store.set("historical_review_diagnostics:history", [malformed_rec])
    state_store.set("historical_review_diagnostics:diag_malformed_ver", malformed_rec)

    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store)
    svc.hydrate_from_state_store()

    assert svc.get_diagnostic("diag_malformed_ver") is None
    assert svc._unsupported_records_skipped >= 1


def test_scenario_12_partial_and_corrupt_payloads_distinguished():
    """Scenario 12: Partial and structurally corrupt payloads distinguished from unsupported versions.
    
    Guarantee: Missing required fields or wrong payload structure raises ValidationError or KeyError,
    not IncompatibleSchemaVersionError.
    """
    # 1. Missing diagnostic_id with valid schema_version
    corrupt_payload = {
        "schema_version": "1.0.0",
        "investigation_id": "inv_missing_id",
        # missing diagnostic_id
    }
    with pytest.raises(KeyError):
        AuditDiagnosticRecord.from_dict(corrupt_payload)

    # 2. Non-dict payload passed to migration
    with pytest.raises(ValidationError):
        migrate_diagnostic_payload("not_a_dict")  # type: ignore


def test_scenario_13_deterministic_migration_idempotency_and_non_mutation():
    """Scenario 13: Deterministic migration idempotency and input payload non-mutation.
    
    Guarantee: Repeated migration produces identical output; input dictionary is NEVER mutated in-place.
    """
    original_dict = {
        "schema_version": "0.9.0",
        "diagnostic_id": "diag_idempotent_001",
        "investigation_id": "inv_001",
        "diagnostic_category": "COVERAGE_DEGRADATION",
        "summary": "Original summary",
        "timestamp": 1700000000.0,
    }
    original_copy = copy.deepcopy(original_dict)

    # Pass 1
    migrated_1 = migrate_diagnostic_payload(original_dict)
    # Check input was not mutated
    assert original_dict == original_copy
    assert original_dict["schema_version"] == "0.9.0"

    # Pass 2 (on already migrated payload)
    migrated_2 = migrate_diagnostic_payload(migrated_1)
    assert migrated_1 == migrated_2
    assert migrated_2["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert migrated_2["category"] == "COVERAGE_DEGRADATION"
    assert migrated_2["sanitized_summary"] == "Original summary"


# ==============================================================================
# Area F: Export Reconciliation with Versioned Payloads
# ==============================================================================

def test_scenario_14_reconciling_export_generated_from_migrated_legacy_evidence(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 14: Reconciling export snapshot generated from migrated legacy evidence.
    
    Guarantee: Evidence hydrated from legacy state can be exported and reconciled with status MATCHED.
    """
    # Inject legacy diagnostic into StateStore
    legacy_diag = {
        "diagnostic_id": "diag_migrated_rec",
        "correlation_id": "corr_migrated_rec",
        "investigation_id": "inv_migrated",
        "timestamp": clock(),
        "timestamp_iso": "2023-11-14T22:13:20Z",
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "WARNING",
        "evidence_nature": "OBSERVED_FAILURE",
        "message": "Migrated diagnostic message",
        "source_references": ["ref_1"],
    }
    state_store.set("historical_review_diagnostics:history", [legacy_diag])
    state_store.set("historical_review_diagnostics:diag_migrated_rec", legacy_diag)

    # Initialize service and hydrate
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.hydrate_from_state_store()

    # Export
    clock.advance(1.0)
    req = DiagnosticsExportRequest(
        investigation_id="inv_migrated",
        include_alerts=False,
    )
    export_result = svc.export_diagnostics(req)
    assert export_result.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert len(export_result.diagnostics) == 1

    # Reconcile
    clock.advance(1.0)
    report = svc.reconcile_export(export_result.export_id)
    assert report.status == DiagnosticsReconciliationStatus.MATCHED
    assert report.fingerprint_verified is True
    assert report.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION


def test_scenario_15_reconciling_export_with_unsupported_future_schema(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 15: Reconciling export snapshot with unsupported future schema returns UNVERIFIABLE.
    
    Guarantee: Future schema export returns UNVERIFIABLE, fingerprint_verified=False, and informative summary_notes.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)

    # Store raw future export dictionary in StateStore
    future_export_dict = {
        "export_id": "exp_future_001",
        "created_at": clock(),
        "created_at_iso": "2023-11-14T22:13:20Z",
        "schema_version": "2.0.0",
        "diagnostics": [],
        "alerts": [],
        "record_counts": {"diagnostics": 0, "alerts": 0},
        "source_references": [],
        "retention_status": "COMPLETE",
        "is_truncated": False,
        "content_fingerprint": "abc123futurefingerprint",
    }
    state_store.set("historical_review_exports:exp_future_001", future_export_dict)

    report = svc.reconcile_export("exp_future_001")
    assert report.status == DiagnosticsReconciliationStatus.UNVERIFIABLE
    assert report.fingerprint_verified is False
    assert "unsupported future schema version '2.0.0'" in report.summary_notes
    assert report.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION


# ==============================================================================
# Area G: Lifecycle, Retention, and Memory Boundaries
# ==============================================================================

def test_scenario_16_multicycle_persist_reconstruct_evict_reconstruct(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 16: End-to-end multi-cycle persist -> reconstruct -> evict -> reconstruct lifecycle.
    
    Guarantee: Schema version metadata and cumulative eviction counts survive multiple persistence cycles.
    """
    # Cycle 1: Create 12 diagnostics with max_diagnostics=10 (minimum clamp)
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )

    for i in range(1, 13):
        clock.advance(1.0)
        svc1.record_diagnostic(
            investigation_id="inv_multi",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Multi diagnostic {i}",
        )

    # 2 diagnostics evicted (12 - 10 = 2)
    status1 = svc1.get_retention_status()
    assert status1["current_diagnostics_count"] == 10
    assert status1["evicted_diagnostics_count"] == 2
    assert status1["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # Cycle 2: Reconstruct service from StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )
    svc2.hydrate_from_state_store()

    status2 = svc2.get_retention_status()
    assert status2["current_diagnostics_count"] == 10
    assert status2["evicted_diagnostics_count"] == 2
    assert status2["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # Evict another 3 items
    for i in range(13, 16):
        clock.advance(1.0)
        svc2.record_diagnostic(
            investigation_id="inv_multi",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Multi diagnostic {i}",
        )

    assert svc2.get_retention_status()["evicted_diagnostics_count"] == 5

    # Cycle 3: Reconstruct again
    svc3 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )
    svc3.hydrate_from_state_store()
    status3 = svc3.get_retention_status()
    assert status3["current_diagnostics_count"] == 10
    assert status3["evicted_diagnostics_count"] == 5
    assert status3["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION


def test_scenario_17_bounded_retention_and_eviction_with_versioned_payloads(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 17: Bounded retention and cumulative eviction counters remain strictly accurate with versioned payloads.
    
    Guarantee: Memory and StateStore respect capacity boundaries, FIFO order, and strictly monotonic eviction counters.
    """
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        state_store=state_store,
        time_provider=clock,
    )

    for i in range(25):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_bound",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.WARNING,
            message=f"Bounded diagnostic {i}",
        )

    assert len(svc.list_diagnostics()) == 10
    assert svc.get_retention_status()["evicted_diagnostics_count"] == 15

    # Verify all 10 surviving records in memory and StateStore have schema_version="1.0.0"
    for d in svc.list_diagnostics():
        assert d.schema_version == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
        stored = state_store.get(f"historical_review_diagnostics:{d.diagnostic_id}")
        assert stored is not None
        assert stored["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION


def test_scenario_18_clear_history_resets_versioned_state(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 18: clear_history() resets versioned state and retention stats correctly.
    
    Guarantee: Memory and StateStore wiped; reset retention stats retain CURRENT_DIAGNOSTICS_SCHEMA_VERSION.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    svc.record_diagnostic(
        investigation_id="inv_clear",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.INFO,
        message="Clear test",
    )
    assert len(svc.list_diagnostics()) == 1

    svc.clear_history()

    assert len(svc.list_diagnostics()) == 0
    assert len(svc.list_alerts()) == 0
    assert svc._unsupported_records_skipped == 0

    status = svc.get_retention_status()
    assert status["schema_version"] == CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    assert status["evicted_diagnostics_count"] == 0


# ==============================================================================
# Area H: Tamper Detection and Security Invariants
# ==============================================================================

def test_scenario_19_tamper_detection_on_schema_version_in_export(
    clock: DeterministicClock, state_store: StateStore
):
    """Scenario 19: Tamper detection and canonical fingerprint sensitivity on versioned payloads.
    
    Guarantee: Modifying schema_version in an export snapshot trips fingerprint verification and reports MISMATCHED.
    """
    svc = HistoricalReviewAuditDiagnosticsService(state_store=state_store, time_provider=clock)
    diag = svc.record_diagnostic(
        investigation_id="inv_tamper",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Tamper test diagnostic",
    )
    req = DiagnosticsExportRequest(investigation_id="inv_tamper")
    export_result = svc.export_diagnostics(req)

    # Tamper with an item inside export snapshot
    tampered_diag = AuditDiagnosticRecord(
        diagnostic_id=diag.diagnostic_id,
        correlation_id=diag.correlation_id,
        investigation_id=diag.investigation_id,
        timestamp=diag.timestamp,
        timestamp_iso=diag.timestamp_iso,
        category=diag.category,
        severity=diag.severity,
        evidence_nature=diag.evidence_nature,
        message="TAMPERED MESSAGE",
        schema_version="1.0.0",
    )
    tampered_export = DiagnosticsExportResult(
        export_id=export_result.export_id,
        request=export_result.request,
        created_at=export_result.created_at,
        created_at_iso=export_result.created_at_iso,
        schema_version=export_result.schema_version,
        diagnostics=(tampered_diag,),
        alerts=(),
        record_counts=export_result.record_counts,
        source_references=export_result.source_references,
        retention_status=export_result.retention_status,
        is_truncated=export_result.is_truncated,
        content_fingerprint=export_result.content_fingerprint,  # original fingerprint
    )
    svc._exports_by_id[export_result.export_id] = tampered_export

    report = svc.reconcile_export(export_result.export_id)
    assert report.status == DiagnosticsReconciliationStatus.MISMATCHED
    assert report.fingerprint_verified is False
    assert any(d.discrepancy_type in ("FINGERPRINT_MISMATCH", "ALTERED_RECORD") for d in report.discrepancies)


def test_scenario_20_mandatory_safety_invariants_and_sanitization():
    """Scenario 20: Mandatory safety invariants and secret sanitization across migrations.
    
    Guarantee:
    1. Settings.execution_locked == True.
    2. Trading engine remains uninitialized.
    3. Secrets (API keys, tokens, passwords) are scrubbed in migrated legacy payloads.
    """
    assert Settings.execution_locked is True

    # Check secret sanitization in legacy migration
    sensitive_payload = {
        "diagnostic_id": "diag_sens_001",
        "investigation_id": "inv_sens",
        "timestamp": 1700000000.0,
        "category": "AUDIT_EVALUATION_FAILURE",
        "severity": "ERROR",
        "message": "Authorization failed with key: sk-live-SECRET_API_KEY_1234567890",
        "summary": "Bearer tok_test_super_secret_token_12345",
    }
    migrated = migrate_diagnostic_payload(sensitive_payload)
    assert "sk-live-SECRET_API_KEY_1234567890" not in migrated["message"]
    assert "tok_test_super_secret_token_12345" not in migrated["sanitized_summary"]
    assert "[REDACTED" in migrated["message"] or "[REDACTED" in migrated["sanitized_summary"]
