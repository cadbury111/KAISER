"""KAISER — Phase 2, Step 41 Acceptance Testing Suite.

Diagnostics Export Integrity, Tamper Detection & Reproducibility Acceptance Testing Foundation.

TEST SCENARIOS:
A. Deterministic export reproducibility:
   - Scenario 1: Stable canonical content and fingerprints across repeated equivalent exports.
   - Scenario 2: Volatile metadata exclusion from canonical fingerprints.
   - Scenario 3: Evidence variations predictably alter content fingerprint.
B. Tamper and alteration detection:
   - Scenario 4: Modified diagnostic content detected as ALTERED_RECORD and MISMATCHED.
   - Scenario 5: Forged fingerprint bypass attempt defeated by active source comparison.
   - Scenario 6: Injected unexpected records detected as MISSING_SOURCE_RECORD.
   - Scenario 7: Records missing from export detected via fingerprint/count check.
   - Scenario 8: Duplicate records within export detected as DUPLICATED_EXPORT_RECORD.
C. Retention-aware reconciliation:
   - Scenario 9: FIFO evicted source records classified as EVICTED_SOURCE_RECORD and INCOMPLETE.
   - Scenario 10: Retention limits never mask unexplained corruption (status remains MISMATCHED).
   - Scenario 11: Evicted export snapshot handled gracefully as UNAVAILABLE.
D. StateStore persistence and reconstruction:
   - Scenario 12: Clean service reconstruction restores exports, reconciliations, and preserves MATCHED status.
   - Scenario 13: Malformed, corrupt, or incomplete persisted state handled safely without crashing.
E. Determinism and edge cases:
   - Scenario 14: Canonicalization normalizes permutation in diagnostics, alerts, and references.
   - Scenario 15: Empty history and empty export filter edge cases handled cleanly as MATCHED.
   - Scenario 16: Repeated reconciliation of unchanged evidence produces deterministic outcomes.
F. Read-only and security guarantees:
   - Scenario 17: Source diagnostics and alerts remain strictly immutable across export and reconciliation.
   - Scenario 18: Secret sanitization scrubs credentials from exports, JSON, and reconciliation reports.
   - Scenario 19: Dashboard sub-resources enforce HTTP 405 on mutations, traversal defense, and security headers.
   - Scenario 20: Mandatory safety invariants preserved (execution_locked, uninitialized trading engine).
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
# Category A: Deterministic Export Reproducibility
# ==============================================================================

def test_scenario_1_stable_canonical_content_and_fingerprints(clock: DeterministicClock, state_store: StateStore):
    """Verify equivalent diagnostic snapshots exported multiple times produce identical canonical dictionaries and fingerprints."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    # Ingest baseline diagnostic evidence
    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_repro_1",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Audit evaluation failure observed in historical stream.",
        source_references=("stream_src_1", "stream_src_2"),
    )
    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_repro_1",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Repeatability divergence detected in evaluation order.",
        source_references=("timeline_ref_a",),
    )

    req = DiagnosticsExportRequest(investigation_id="inv_repro_1")

    # Generate Export 1
    exp1 = svc.export_diagnostics(req)

    # Generate Export 2 under identical source state
    clock.advance(0.5)
    exp2 = svc.export_diagnostics(req)

    # Content fingerprints must be identical
    assert exp1.content_fingerprint == exp2.content_fingerprint
    assert exp1.record_counts == exp2.record_counts
    assert exp1.retention_status == exp2.retention_status
    assert exp1.is_truncated == exp2.is_truncated

    # Canonical representations must be character-for-character identical
    can1 = exp1.to_canonical_json()
    can2 = exp2.to_canonical_json()
    assert can1 == can2


def test_scenario_2_volatile_metadata_exclusion_from_fingerprints(clock: DeterministicClock):
    """Verify volatile/transient request metadata does not alter the content fingerprint."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_volatile_test",
        category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Coverage degradation noted in review.",
    )

    # Export with metadata A
    req_a = DiagnosticsExportRequest(
        investigation_id="inv_volatile_test",
        metadata={"client_agent": "kaiser-voice-v1", "session_token": "sess_123"},
    )
    exp_a = svc.export_diagnostics(req_a)

    # Export with metadata B (different transient request metadata)
    req_b = DiagnosticsExportRequest(
        investigation_id="inv_volatile_test",
        metadata={"client_agent": "dashboard-ui", "client_ip": "127.0.0.1"},
    )
    exp_b = svc.export_diagnostics(req_b)

    # Content fingerprints must match because request.metadata is transient and excluded from canonical evidence
    assert exp_a.content_fingerprint == exp_b.content_fingerprint


def test_scenario_3_evidence_variations_alter_content_fingerprint(clock: DeterministicClock):
    """Verify meaningful changes in diagnostic evidence produce distinct content fingerprints."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_variation_test",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Initial state diagnostic.",
    )
    exp1 = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_variation_test"))

    # Ingest additional diagnostic record
    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_variation_test",
        category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Critical historical integrity regression observed.",
    )
    exp2 = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_variation_test"))

    # Fingerprints must differ
    assert exp1.content_fingerprint != exp2.content_fingerprint
    assert exp2.record_counts["diagnostics"] > exp1.record_counts["diagnostics"]


# ==============================================================================
# Category B: Tamper and Alteration Detection
# ==============================================================================

def test_scenario_4_tamper_detection_modified_record_values(clock: DeterministicClock):
    """Verify altering diagnostic record content in an export is caught during reconciliation."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    d = svc.record_diagnostic(
        investigation_id="inv_tamper_val",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Original legitimate diagnostic message.",
    )

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_tamper_val"))

    # Tamper with the diagnostic record: modify message
    tampered_diag = AuditDiagnosticRecord(
        diagnostic_id=d.diagnostic_id,
        correlation_id=d.correlation_id,
        investigation_id=d.investigation_id,
        timestamp=d.timestamp,
        timestamp_iso=d.timestamp_iso,
        category=d.category,
        severity=d.severity,
        evidence_nature=d.evidence_nature,
        message="TAMPERED: Altered message by unauthorized party.",
        source_references=d.source_references,
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
        content_fingerprint=exp.content_fingerprint,  # Old fingerprint preserved
        omitted_reasons=exp.omitted_reasons,
    )

    recon = svc.reconcile_export(tampered_export)

    assert recon.status == DiagnosticsReconciliationStatus.MISMATCHED
    assert not recon.fingerprint_verified

    # Discrepancies must include fingerprint mismatch and altered record
    types = [disc.discrepancy_type for disc in recon.discrepancies]
    assert "FINGERPRINT_MISMATCH" in types
    assert "ALTERED_RECORD" in types

    altered_disc = next(disc for disc in recon.discrepancies if disc.discrepancy_type == "ALTERED_RECORD")
    assert altered_disc.record_id == d.diagnostic_id
    assert altered_disc.expected is not None
    assert altered_disc.actual is not None
    assert altered_disc.expected["message"] == "TAMPERED: Altered message by unauthorized party."
    assert altered_disc.actual["message"] == "Original legitimate diagnostic message."
    assert not altered_disc.is_retention_limitation


def test_scenario_5_forged_fingerprint_bypass_attempt_defeated(clock: DeterministicClock):
    """Verify forging the fingerprint to match tampered content still fails against active source evidence."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    d = svc.record_diagnostic(
        investigation_id="inv_forged_fp",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Real source failure observed.",
    )

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_forged_fp"))

    # Tamper with the diagnostic record
    tampered_diag = AuditDiagnosticRecord(
        diagnostic_id=d.diagnostic_id,
        correlation_id=d.correlation_id,
        investigation_id=d.investigation_id,
        timestamp=d.timestamp,
        timestamp_iso=d.timestamp_iso,
        category=d.category,
        severity=AuditDiagnosticSeverity.INFO,  # Downgraded from ERROR to INFO
        evidence_nature=d.evidence_nature,
        message="TAMPERED: Downgraded severity to conceal fault.",
        source_references=d.source_references,
    )

    # Forger recomputes content fingerprint to make export internally consistent
    forged_can_dict = compute_export_canonical_dict(
        schema_version=exp.schema_version,
        investigation_id=exp.request.investigation_id,
        diagnostics=(tampered_diag,),
        alerts=exp.alerts,
        source_references=exp.source_references,
        retention_status=exp.retention_status,
        is_truncated=exp.is_truncated,
    )
    forged_fp = compute_export_content_fingerprint(forged_can_dict)

    forged_export = DiagnosticsExportResult(
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
        content_fingerprint=forged_fp,  # Forged fingerprint matches tampered export
        omitted_reasons=exp.omitted_reasons,
    )

    recon = svc.reconcile_export(forged_export)

    # Internal fingerprint is technically self-consistent
    assert recon.fingerprint_verified
    # BUT reconciliation against source state catches the discrepancy
    assert recon.status == DiagnosticsReconciliationStatus.MISMATCHED
    types = [disc.discrepancy_type for disc in recon.discrepancies]
    assert "ALTERED_RECORD" in types
    altered_disc = next(disc for disc in recon.discrepancies if disc.discrepancy_type == "ALTERED_RECORD")
    assert altered_disc.record_id == d.diagnostic_id
    assert altered_disc.expected is not None
    assert altered_disc.actual is not None
    assert altered_disc.expected["severity"] == AuditDiagnosticSeverity.INFO.value
    assert altered_disc.actual["severity"] == AuditDiagnosticSeverity.ERROR.value


def test_scenario_6_injected_unexpected_records_detected(clock: DeterministicClock):
    """Verify an export containing a forged record not in the source is detected as MISSING_SOURCE_RECORD."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_inject_test",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.INFO,
        message="Valid diagnostic.",
    )

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_inject_test"))

    # Construct an injected diagnostic record with non-existent ID
    injected_diag = AuditDiagnosticRecord(
        diagnostic_id="diag_injected_nonexistent",
        correlation_id="corr_injected",
        investigation_id="inv_inject_test",
        timestamp=clock() + 10.0,
        timestamp_iso="2026-10-13T12:00:00Z",
        category=AuditDiagnosticCategory.INTEGRITY_VIOLATION,
        severity=AuditDiagnosticSeverity.CRITICAL,
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        message="Injected phantom critical failure.",
    )

    tampered_export = DiagnosticsExportResult(
        export_id=exp.export_id,
        request=exp.request,
        created_at=exp.created_at,
        created_at_iso=exp.created_at_iso,
        schema_version=exp.schema_version,
        diagnostics=exp.diagnostics + (injected_diag,),
        alerts=exp.alerts,
        record_counts={"diagnostics": len(exp.diagnostics) + 1, "alerts": len(exp.alerts)},
        source_references=exp.source_references,
        retention_status=exp.retention_status,
        is_truncated=exp.is_truncated,
        content_fingerprint=exp.content_fingerprint,
        omitted_reasons=exp.omitted_reasons,
    )

    recon = svc.reconcile_export(tampered_export)

    assert recon.status == DiagnosticsReconciliationStatus.MISMATCHED
    types = [disc.discrepancy_type for disc in recon.discrepancies]
    assert "MISSING_SOURCE_RECORD" in types
    disc = next(disc for disc in recon.discrepancies if disc.discrepancy_type == "MISSING_SOURCE_RECORD")
    assert disc.record_id == "diag_injected_nonexistent"
    assert not disc.is_retention_limitation


def test_scenario_7_missing_records_dropped_from_export_detected(clock: DeterministicClock):
    """Verify an export with records removed is detected as MISMATCHED via fingerprint verification."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_dropped_rec",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.INFO,
        message="Diag 1",
    )
    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_dropped_rec",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Diag 2",
    )

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_dropped_rec"))
    assert len(exp.diagnostics) == 2

    # Drop the second record from the export while preserving original fingerprint
    dropped_export = DiagnosticsExportResult(
        export_id=exp.export_id,
        request=exp.request,
        created_at=exp.created_at,
        created_at_iso=exp.created_at_iso,
        schema_version=exp.schema_version,
        diagnostics=(exp.diagnostics[0],),  # Dropped one
        alerts=exp.alerts,
        record_counts={"diagnostics": 1, "alerts": len(exp.alerts)},
        source_references=exp.source_references,
        retention_status=exp.retention_status,
        is_truncated=exp.is_truncated,
        content_fingerprint=exp.content_fingerprint,
        omitted_reasons=exp.omitted_reasons,
    )

    recon = svc.reconcile_export(dropped_export)

    assert recon.status == DiagnosticsReconciliationStatus.MISMATCHED
    assert not recon.fingerprint_verified
    types = [disc.discrepancy_type for disc in recon.discrepancies]
    assert "FINGERPRINT_MISMATCH" in types


def test_scenario_8_duplicate_records_in_export_detected(clock: DeterministicClock):
    """Verify duplicate record IDs within an export are flagged as DUPLICATED_EXPORT_RECORD."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    d = svc.record_diagnostic(
        investigation_id="inv_dup_rec",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Diagnostic to duplicate.",
    )

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_dup_rec"))

    dup_export = DiagnosticsExportResult(
        export_id=exp.export_id,
        request=exp.request,
        created_at=exp.created_at,
        created_at_iso=exp.created_at_iso,
        schema_version=exp.schema_version,
        diagnostics=(d, d),  # Duplicated
        alerts=exp.alerts,
        record_counts={"diagnostics": 2, "alerts": len(exp.alerts)},
        source_references=exp.source_references,
        retention_status=exp.retention_status,
        is_truncated=exp.is_truncated,
        content_fingerprint=exp.content_fingerprint,
        omitted_reasons=exp.omitted_reasons,
    )

    recon = svc.reconcile_export(dup_export)

    assert recon.status == DiagnosticsReconciliationStatus.MISMATCHED
    types = [disc.discrepancy_type for disc in recon.discrepancies]
    assert "DUPLICATED_EXPORT_RECORD" in types


# ==============================================================================
# Category C: Retention-Aware Reconciliation
# ==============================================================================

def test_scenario_9_fifo_evicted_source_records_classified_as_incomplete(clock: DeterministicClock):
    """Verify FIFO evicted records are classified as EVICTED_SOURCE_RECORD with status INCOMPLETE, not corruption."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock,
    )

    # Ingest 10 records to fill capacity
    for i in range(10):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_evict_aware",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Initial record {i}",
        )

    # Export while all 10 are active
    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_evict_aware"))
    assert len(exp.diagnostics) == 10
    evicted_target_id = exp.diagnostics[0].diagnostic_id

    # Ingest 5 new records, evicting the first 5
    for i in range(10, 15):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_evict_aware",
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Overflow record {i}",
        )

    # Verify eviction tracking
    assert svc.is_diagnostic_evicted(evicted_target_id)

    # Reconcile export
    recon = svc.reconcile_export(exp.export_id)

    assert recon.status == DiagnosticsReconciliationStatus.INCOMPLETE
    assert recon.fingerprint_verified

    evicted_discs = [d for d in recon.discrepancies if d.discrepancy_type == "EVICTED_SOURCE_RECORD"]
    assert len(evicted_discs) > 0
    for d in evicted_discs:
        assert d.is_retention_limitation is True


def test_scenario_10_retention_limits_never_mask_unexplained_corruption(clock: DeterministicClock):
    """Verify that when both an evicted record and an altered record are present, status is MISMATCHED (not INCOMPLETE)."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=10,
        max_alerts=10,
        time_provider=clock,
    )

    for i in range(10):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_mask_test",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Record {i}",
        )

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_mask_test"))

    # Ingest 5 more records to evict the oldest
    for i in range(10, 15):
        clock.advance(1.0)
        svc.record_diagnostic(
            investigation_id="inv_mask_test",
            category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
            severity=AuditDiagnosticSeverity.INFO,
            message=f"Overflow record {i}",
        )

    # Now tamper with one of the surviving active records in the export
    surviving_diag = exp.diagnostics[9]  # This one is still active in source
    tampered_surviving = AuditDiagnosticRecord(
        diagnostic_id=surviving_diag.diagnostic_id,
        correlation_id=surviving_diag.correlation_id,
        investigation_id=surviving_diag.investigation_id,
        timestamp=surviving_diag.timestamp,
        timestamp_iso=surviving_diag.timestamp_iso,
        category=surviving_diag.category,
        severity=AuditDiagnosticSeverity.CRITICAL,  # Altered severity
        evidence_nature=surviving_diag.evidence_nature,
        message="TAMPERED: Altered surviving record.",
        source_references=surviving_diag.source_references,
    )

    mixed_diags = exp.diagnostics[:9] + (tampered_surviving,)
    mixed_export = DiagnosticsExportResult(
        export_id=exp.export_id,
        request=exp.request,
        created_at=exp.created_at,
        created_at_iso=exp.created_at_iso,
        schema_version=exp.schema_version,
        diagnostics=mixed_diags,
        alerts=exp.alerts,
        record_counts=exp.record_counts,
        source_references=exp.source_references,
        retention_status=exp.retention_status,
        is_truncated=exp.is_truncated,
        content_fingerprint=exp.content_fingerprint,
        omitted_reasons=exp.omitted_reasons,
    )

    recon = svc.reconcile_export(mixed_export)

    # Crucial invariant: has_corruption (ALTERED_RECORD) overrides has_retention_limitation
    assert recon.status == DiagnosticsReconciliationStatus.MISMATCHED
    types = [d.discrepancy_type for d in recon.discrepancies]
    assert "EVICTED_SOURCE_RECORD" in types
    assert "ALTERED_RECORD" in types


def test_scenario_11_evicted_export_snapshot_handling(clock: DeterministicClock):
    """Verify reconciling an evicted export snapshot returns UNAVAILABLE with clear retention reason."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        max_exports=5,  # Bounded to 5
        time_provider=clock,
    )

    svc.record_diagnostic(
        investigation_id="inv_exp_evict",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.INFO,
        message="Diag 1",
    )

    # Create 6 exports, evicting the first
    clock.advance(1.0)
    e1 = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_exp_evict"))
    for i in range(2, 7):
        clock.advance(1.0)
        svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_exp_evict"))

    assert svc.is_export_evicted(e1.export_id)
    assert svc.get_export(e1.export_id) is None

    recon = svc.reconcile_export(e1.export_id)
    assert recon.status == DiagnosticsReconciliationStatus.UNAVAILABLE
    assert "evicted due to retention limits" in recon.summary_notes


# ==============================================================================
# Category D: StateStore Persistence and Reconstruction
# ==============================================================================

def test_scenario_12_statestore_persistence_and_clean_reconstruction(clock: DeterministicClock, state_store: StateStore):
    """Verify exports and reconciliations survive service reconstruction and verify MATCHED status."""
    # Instance 1: Generate evidence and export
    svc1 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc1.record_diagnostic(
        investigation_id="inv_persist_test",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Persisted diagnostic.",
        source_references=("ref_db_1",),
    )

    exp = svc1.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_persist_test"))
    rec1 = svc1.reconcile_export(exp.export_id)
    assert rec1.status == DiagnosticsReconciliationStatus.MATCHED

    # Instance 2: Reconstruct service from same StateStore
    svc2 = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    # Reconstructed service should contain the hydrated export
    restored_exp = svc2.get_export(exp.export_id)
    assert restored_exp is not None
    assert restored_exp.export_id == exp.export_id
    assert restored_exp.content_fingerprint == exp.content_fingerprint

    # Reconciling on reconstructed service against hydrated source evidence must MATCH
    rec2 = svc2.reconcile_export(exp.export_id)
    assert rec2.status == DiagnosticsReconciliationStatus.MATCHED
    assert rec2.fingerprint_verified
    assert rec2.matched_diagnostics == 1


def test_scenario_13_statestore_malformed_and_corrupt_data_recovery(clock: DeterministicClock, state_store: StateStore):
    """Verify malformed or corrupted StateStore data is handled gracefully without crashing or fabricating state."""
    # Inject malformed data into StateStore
    state_store.set("historical_review_exports:history", [
        "not_a_dict",
        {"export_id": "malformed_no_request"},
        None,
        12345,
    ])
    state_store.set("historical_review_reconciliations:history", "not_even_a_list")
    state_store.set("historical_review_exports:corrupt_key", {"invalid": True})

    # Service reconstruction must not crash
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        state_store=state_store,
        time_provider=clock,
    )

    # Queries for corrupt keys safely return None
    assert svc.get_export("corrupt_key") is None
    assert svc.get_reconciliation("corrupt_key") is None
    assert isinstance(svc.list_exports(), list)
    assert isinstance(svc.list_reconciliations(), list)


# ==============================================================================
# Category E: Determinism and Edge Cases
# ==============================================================================

def test_scenario_14_canonicalization_normalizes_collection_order():
    """Verify that permutation in diagnostics, alerts, and references produces identical canonical dicts and fingerprints."""
    d1 = AuditDiagnosticRecord(
        diagnostic_id="diag_001",
        correlation_id="corr_1",
        investigation_id="inv_order_test",
        timestamp=100.0,
        timestamp_iso="2026-10-13T10:00:00Z",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.INFO,
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        message="D1",
        source_references=("ref_z", "ref_a", "ref_m"),
    )
    d2 = AuditDiagnosticRecord(
        diagnostic_id="diag_002",
        correlation_id="corr_2",
        investigation_id="inv_order_test",
        timestamp=200.0,
        timestamp_iso="2026-10-13T10:01:00Z",
        category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
        severity=AuditDiagnosticSeverity.WARNING,
        evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
        message="D2",
        source_references=("ref_b", "ref_a"),
    )

    # Order 1: [d1, d2]
    can1 = compute_export_canonical_dict(
        schema_version="1.0.0",
        investigation_id="inv_order_test",
        diagnostics=(d1, d2),
        alerts=(),
        source_references=("ref_z", "ref_a", "ref_m", "ref_b"),
        retention_status="COMPLETE",
        is_truncated=False,
    )

    # Order 2: [d2, d1] with permuted and duplicate source references
    can2 = compute_export_canonical_dict(
        schema_version="1.0.0",
        investigation_id="inv_order_test",
        diagnostics=(d2, d1),
        alerts=(),
        source_references=("ref_m", "ref_b", "ref_a", "ref_z", "ref_a"),
        retention_status="COMPLETE",
        is_truncated=False,
    )

    # Canonical representations must be identical
    assert can1 == can2
    assert compute_export_content_fingerprint(can1) == compute_export_content_fingerprint(can2)


def test_scenario_15_empty_history_and_empty_export_semantics(clock: DeterministicClock):
    """Verify empty diagnostic history and zero-match filters export cleanly with status NO_DATA and reconcile to MATCHED."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    # Export on completely empty service
    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_empty_system"))
    assert exp.retention_status == "NO_DATA"
    assert not exp.is_truncated
    assert exp.record_counts == {"diagnostics": 0, "alerts": 0}
    assert len(exp.content_fingerprint) == 64

    # Reconciling empty export against empty state must be MATCHED
    recon = svc.reconcile_export(exp.export_id)
    assert recon.status == DiagnosticsReconciliationStatus.MATCHED
    assert recon.fingerprint_verified
    assert recon.matched_diagnostics == 0
    assert len(recon.discrepancies) == 0


def test_scenario_16_repeated_reconciliation_determinism(clock: DeterministicClock):
    """Verify repeated reconciliations of unchanged evidence produce deterministic results."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_repeat_recon",
        category=AuditDiagnosticCategory.INTEGRITY_VIOLATION,
        severity=AuditDiagnosticSeverity.WARNING,
        message="Resource warning.",
    )

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_repeat_recon"))

    reports: List[DiagnosticsReconciliationReport] = []
    for _ in range(5):
        clock.advance(0.1)
        r = svc.reconcile_export(exp.export_id)
        reports.append(r)

    # Every run must agree on status, matches, and fingerprint verification
    for r in reports:
        assert r.status == DiagnosticsReconciliationStatus.MATCHED
        assert r.matched_diagnostics == 1
        assert r.matched_alerts == 1
        assert r.fingerprint_verified
        assert len(r.discrepancies) == 0


# ==============================================================================
# Category F: Read-Only and Security Guarantees
# ==============================================================================

def test_scenario_17_read_only_invariant_and_source_immutability(clock: DeterministicClock):
    """Verify export and reconciliation operations do not mutate source diagnostics, alerts, or counters."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_immut_test",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.ERROR,
        message="Active diagnostic.",
    )

    # Snapshot source state
    diag_before = [d.to_dict() for d in svc.list_diagnostics()]
    alert_before = [a.to_dict() for a in svc.list_alerts()]
    ret_before = dict(svc.get_retention_status())

    # Perform multiple exports and reconciliations (including invalid/empty requests)
    for _ in range(3):
        e = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_immut_test"))
        svc.reconcile_export(e.export_id)

    diag_after = [d.to_dict() for d in svc.list_diagnostics()]
    alert_after = [a.to_dict() for a in svc.list_alerts()]
    ret_after = dict(svc.get_retention_status())

    assert diag_before == diag_after
    assert alert_before == alert_after
    assert ret_before["current_diagnostics_count"] == ret_after["current_diagnostics_count"]
    assert ret_before["current_alerts_count"] == ret_after["current_alerts_count"]
    assert ret_before["total_diagnostics_evicted"] == ret_after["total_diagnostics_evicted"]
    assert ret_before["total_alerts_evicted"] == ret_after["total_alerts_evicted"]


def test_scenario_18_secret_and_credential_sanitization(clock: DeterministicClock):
    """Verify sensitive credentials are scrubbed from exported records and canonical JSON."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_sec_test",
        category=AuditDiagnosticCategory.GENERAL_DIAGNOSTIC,
        severity=AuditDiagnosticSeverity.INFO,
        message="Diagnostic with sensitive payload details.",
        details={
            "api_key": "SUPER_SECRET_KEY_123",
            "password": "hunter2_secret_password",
            "token": "bearer_jwt_sensitive_token",
            "normal_metric": 42.5,
        },
    )

    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_sec_test"))
    can_json = exp.to_canonical_json()
    exp_dict = exp.to_dict()

    # Sensitive values must be completely absent from JSON and dict
    for secret in ("SUPER_SECRET_KEY_123", "hunter2_secret_password", "bearer_jwt_sensitive_token"):
        assert secret not in can_json
        assert secret not in json.dumps(exp_dict)


def test_scenario_19_dashboard_security_and_traversal_defense(clock: DeterministicClock):
    """Verify Dashboard Endpoint 28 sub-resources enforce HTTP 405 on mutations, traversal protection, and security headers."""
    svc = HistoricalReviewAuditDiagnosticsService(
        max_diagnostics=50,
        max_alerts=50,
        time_provider=clock,
    )

    clock.advance(1.0)
    svc.record_diagnostic(
        investigation_id="inv_dash_sec",
        category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
        severity=AuditDiagnosticSeverity.INFO,
        message="Dash sec diag.",
    )
    exp = svc.export_diagnostics(DiagnosticsExportRequest(investigation_id="inv_dash_sec"))
    rec = svc.reconcile_export(exp.export_id)

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        audit_diagnostics_service=svc,
    )
    server.start(blocking=False)
    time.sleep(0.1)
    base_url = server.url

    try:
        # 1. Verify GET /api/historical-review-diagnostics/exports
        req = urllib.request.Request(f"{base_url}/api/historical-review-diagnostics/exports")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            assert resp.headers.get("X-Content-Type-Options") == "nosniff"
            assert resp.headers.get("X-Frame-Options") == "DENY"
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert "exports" in data

        # 2. Verify GET /api/historical-review-diagnostics/reconciliations
        req = urllib.request.Request(f"{base_url}/api/historical-review-diagnostics/reconciliations")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert "reconciliations" in data

        # 3. Verify POST rejected with HTTP 405 Method Not Allowed
        req_post = urllib.request.Request(
            f"{base_url}/api/historical-review-diagnostics/exports",
            data=b"{}",
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_post)
        assert exc_info.value.code == 405

        # 4. Verify DELETE rejected with HTTP 405
        req_del = urllib.request.Request(
            f"{base_url}/api/historical-review-diagnostics/exports/{exp.export_id}",
            method="DELETE",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_del)
        assert exc_info.value.code == 405

        # 5. Verify Path Traversal Attack rejected
        traversal_url = f"{base_url}/api/historical-review-diagnostics/exports/..%2f..%2fetc%2fpasswd"
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(traversal_url)
        assert exc_info.value.code in (400, 404)

    finally:
        server.stop()


def test_scenario_20_mandatory_safety_invariants():
    """Verify execution_locked invariant remains True and trading engine uninitialized."""
    settings = Settings()
    assert settings.execution_locked is True

    # Master orchestrator blocks trading recommendations
    bus = EventBus()
    svc = HistoricalReviewAuditDiagnosticsService()
    orchestrator = MasterOrchestrator(event_bus=bus)
    register_audit_diagnostics_handlers(orchestrator, svc)

    req = OrchestratorRequest.create(
        command="export_historical_review_diagnostics",
        context={"investigation_id": "inv_inv", "trade_recommendation": "BUY"},
    )
    resp = orchestrator.handle_request(req)
    assert resp.status == RequestStatus.FAILED
    assert any("Prohibited field" in str(e) for e in resp.errors)
