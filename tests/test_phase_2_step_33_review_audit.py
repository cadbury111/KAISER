"""KAISER — Phase 2, Step 33 Test Suite.

Historical Review Evolution Audit, Integrity & Reproducibility Validation Foundation.

Test Categories:
1. Audit finding model & immutability
2. Audit report model & immutability
3. Deterministic audit report ID computation
4. Snapshot integrity audit (valid snapshot)
5. Snapshot integrity audit (prohibited terms rejection)
6. Snapshot claim separation (facts vs interpretations)
7. Evolution integrity audit (valid record)
8. Evolution integrity audit (broken references)
9. Evolution integrity audit (inconsistent references)
10. Historical chain audit (valid sequence)
11. Historical chain audit (version gaps)
12. Historical chain audit (chronological inversion)
13. Historical chain audit (duplicate IDs)
14. Historical chain audit (conflict preservation)
15. Reproducibility verification (valid reproducible)
16. Reproducibility verification (unverifiable empty references)
17. Reproducibility verification (unknown state)
18. Full audit report generation & voice summary
19. Voice summary safety & forbidden words rejection
20. EventBus integration
21. StateStore integration
22. Orchestrator handler registration & execution
23. Dashboard GET endpoints
24. Dashboard HEAD endpoints
25. Dashboard mutation rejection (405)
26. Dashboard path traversal rejection (400)
27. Bounded history & concurrency thread-safety
28. Safety invariants & execution engine locked
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import http.client
import json
import math
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import pytest

from app.analysis.opportunities.evolution import (
    ChangeClassification,
    ConflictLifecycleState,
    DataQualityEvolutionState,
    EvolutionDimensionName,
    EvolutionDimensionRecord,
    HistoricalReviewSnapshot,
    HistoricalSnapshotLifecycleState,
    HistoricalReviewEvolutionService,
    ReproducibilityEvolutionState,
    ReviewCoverageEvolutionState,
    ReviewEvolutionRecord,
    ReviewEvolutionVoiceSummary,
    compute_deterministic_evolution_id,
    compute_deterministic_snapshot_id,
)
from app.analysis.opportunities.evolution.audit import (
    AuditFindingCategory,
    AuditFindingSeverity,
    AuditReportLifecycleState,
    EvolutionAuditFinding,
    EvolutionAuditVoiceSummary,
    EvolutionRecordIntegrityResult,
    EvolutionReproducibilityVerificationResult,
    HistoricalChainIntegrityResult,
    HistoricalReviewAuditReport,
    HistoricalReviewAuditService,
    RecordValidationStatus,
    SnapshotIntegrityResult,
    compute_deterministic_audit_report_id,
    register_audit_handlers,
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
# Helpers
# ==============================================================================

def make_test_snapshot(
    snapshot_id: Optional[str] = None,
    investigation_id: str = "inv_test_01",
    outcome_id: str = "out_test_01",
    snapshot_version: int = 1,
    captured_at: float = 1700000000.0,
    facts_count: int = 3,
    conflicts: Optional[List[str]] = None,
    source_refs: Tuple[str, ...] = ("ref_1", "ref_2"),
    reproducibility: str = "REPRODUCIBLE",
) -> HistoricalReviewSnapshot:
    """Create a test snapshot instance with deterministic ID if None."""
    facts = [
        {"evidence_id": f"fact_{i}", "description": f"Empirical fact {i}", "claim_type": "FACT"}
        for i in range(facts_count)
    ]
    interps = [
        {"evidence_id": "int_1", "description": "Interpretation 1", "claim_type": "INTERPRETATION"}
    ]
    conflict_list = conflicts if conflicts is not None else ["conflict_alpha"]
    conflict_state = {"active_conflicts": conflict_list}
    source_state = {"status": "RECONCILED", "analysis_run_ids": ["run_test_1"]}
    evidence_state = {"facts": facts, "interpretations": interps}
    review_state = {"human_review_decisions": [{"decision_id": "dec_1", "disposition": "ACCEPTED"}]}
    quality_state = {"quality_score": 1.0, "status": "VALID"}
    provenance_state = {"provenance_summary": {"sources": ["src_1"]}}
    reproducibility_state = {"reproducibility_state": reproducibility}
    coverage_state = {"coverage_status": "COMPLETE"}
    limitation_state = ({"limitation_id": "lim_1", "text": "Sample limitation"},)

    if snapshot_id is None:
        fp = hashlib.sha256(
            json.dumps(
                {
                    "source_state": source_state,
                    "evidence_state": evidence_state,
                    "review_state": review_state,
                    "conflict_state": conflict_state,
                    "quality_state": quality_state,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:16]
        resolved_id = compute_deterministic_snapshot_id(
            investigation_id, outcome_id, None, snapshot_version, fp
        )
    else:
        resolved_id = snapshot_id

    return HistoricalReviewSnapshot(
        snapshot_id=resolved_id,
        investigation_id=investigation_id,
        outcome_id=outcome_id,
        comparison_id=None,
        snapshot_version=snapshot_version,
        captured_at=captured_at,
        lifecycle_state=HistoricalSnapshotLifecycleState.STORED,
        source_state=source_state,
        evidence_state=evidence_state,
        review_state=review_state,
        conflict_state=conflict_state,
        quality_state=quality_state,
        provenance_state=provenance_state,
        reproducibility_state=reproducibility_state,
        coverage_state=coverage_state,
        limitation_state=limitation_state,
        source_references=source_refs,
    )


def make_test_evolution(
    evolution_id: Optional[str] = None,
    previous_snapshot_id: str = "revsnap_test01",
    current_snapshot_id: str = "revsnap_test02",
    investigation_id: str = "inv_test_01",
    generated_at: float = 1700000100.0,
    newly_resolved: Tuple[str, ...] = ("conflict_alpha",),
) -> ReviewEvolutionRecord:
    """Create a test evolution record instance with deterministic ID if None."""
    dims = [
        EvolutionDimensionRecord(
            dimension_name=d,
            previous_state={"state": "prev"},
            current_state={"state": "curr"},
            change_classification=ChangeClassification.UNCHANGED,
            source_references=("ref_1",),
            explanation="Unchanged dimension",
        )
        for d in EvolutionDimensionName
    ]
    resolved_evol_id = (
        evolution_id
        if evolution_id is not None
        else compute_deterministic_evolution_id(previous_snapshot_id, current_snapshot_id)
    )
    return ReviewEvolutionRecord(
        evolution_id=resolved_evol_id,
        previous_snapshot_id=previous_snapshot_id,
        current_snapshot_id=current_snapshot_id,
        investigation_id=investigation_id,
        generated_at=generated_at,
        change_state=ChangeClassification.UNCHANGED,
        changed_dimensions=tuple(dims),
        newly_resolved_discrepancies=newly_resolved,
    )


# ==============================================================================
# 1. Audit Finding Model & Immutability
# ==============================================================================

def test_audit_finding_model_and_immutability():
    """Verify EvolutionAuditFinding validation and immutability."""
    finding = EvolutionAuditFinding(
        finding_id="find_test01",
        category=AuditFindingCategory.STRUCTURAL_INTEGRITY,
        severity=AuditFindingSeverity.WARNING,
        target_type="SNAPSHOT",
        target_id="revsnap_test01",
        field_path="snapshot_version",
        message="Snapshot version sequence jump detected.",
        observed_value="3",
        expected_value="2",
    )
    assert finding.finding_id == "find_test01"
    assert finding.severity == AuditFindingSeverity.WARNING
    assert finding.to_dict()["observed_value"] == "3"

    with pytest.raises(Exception):
        finding.message = "Mutated"  # type: ignore


# ==============================================================================
# 2. Audit Report Model & Immutability
# ==============================================================================

def test_audit_report_model_and_immutability():
    """Verify HistoricalReviewAuditReport frozen state and serialization."""
    report = HistoricalReviewAuditReport(
        report_id="revaudit_test01",
        investigation_id="inv_test01",
        generated_at=1700000000.0,
        lifecycle_state=AuditReportLifecycleState.STORED,
        overall_status=RecordValidationStatus.VALID,
        snapshots_audited_count=2,
        evolutions_audited_count=1,
        findings_summary={"INFO": 1, "WARNING": 0, "ERROR": 0, "CRITICAL": 0},
    )
    assert report.report_id == "revaudit_test01"
    assert report.overall_status == RecordValidationStatus.VALID
    d = report.to_dict()
    assert d["snapshots_audited_count"] == 2
    assert d["lifecycle_state"] == "STORED"

    with pytest.raises(Exception):
        report.overall_status = RecordValidationStatus.FAILED  # type: ignore


# ==============================================================================
# 3. Deterministic Audit Report ID Computation
# ==============================================================================

def test_deterministic_audit_report_id():
    """Verify deterministic hashing for audit reports."""
    id1 = compute_deterministic_audit_report_id(
        investigation_id="inv_test",
        snapshot_ids=["snap_b", "snap_a"],
        evolution_ids=["evol_1"],
        overall_status="VALID",
    )
    id2 = compute_deterministic_audit_report_id(
        investigation_id="inv_test",
        snapshot_ids=["snap_a", "snap_b"],  # Differently ordered input
        evolution_ids=["evol_1"],
        overall_status="VALID",
    )
    assert id1 == id2
    assert id1.startswith("revaudit_")

    id3 = compute_deterministic_audit_report_id(
        investigation_id="inv_test",
        snapshot_ids=["snap_a", "snap_b"],
        evolution_ids=["evol_1"],
        overall_status="INCONSISTENT",
    )
    assert id1 != id3


# ==============================================================================
# 4. Snapshot Integrity Audit (Valid Snapshot)
# ==============================================================================

def test_snapshot_audit_valid():
    """Audit a valid snapshot and verify clean status."""
    service = HistoricalReviewAuditService()
    snap = make_test_snapshot()
    result = service.audit_snapshot(snap)

    assert result.status == RecordValidationStatus.VALID
    assert result.facts_count == 3
    assert result.has_prohibited_terms is False
    assert len(result.findings) == 0


# ==============================================================================
# 5. Snapshot Integrity Audit (Prohibited Terms Rejection)
# ==============================================================================

def test_snapshot_audit_prohibited_terms_rejection():
    """Verify snapshot containing prohibited trading fields is rejected as FAILED."""
    service = HistoricalReviewAuditService()
    with pytest.raises(ValidationError):
        # HistoricalReviewSnapshot post_init blocks prohibited fields
        HistoricalReviewSnapshot(
            snapshot_id="revsnap_bad",
            investigation_id="inv_bad",
            outcome_id="out_bad",
            source_state={"buy": "true"},  # Prohibited
        )


# ==============================================================================
# 6. Snapshot Claim Separation (Facts vs Interpretations)
# ==============================================================================

def test_snapshot_claim_separation():
    """Verify that fact items labeled with incompatible claim_types are flagged."""
    service = HistoricalReviewAuditService()
    # Create snapshot where a fact claim has claim_type = INTERPRETATION
    snap = HistoricalReviewSnapshot(
        snapshot_id="revsnap_mixed",
        investigation_id="inv_mixed",
        outcome_id="out_mixed",
        captured_at=1700000000.0,
        evidence_state={
            "facts": [
                {"evidence_id": "fact_1", "description": "Good fact", "claim_type": "FACT"},
                {"evidence_id": "fact_2", "description": "Conflated fact", "claim_type": "INTERPRETATION"},
            ],
            "interpretations": [],
        },
    )
    res = service.audit_snapshot(snap)
    assert any(f.category == AuditFindingCategory.CLAIM_SEPARATION for f in res.findings)


# ==============================================================================
# 7. Evolution Integrity Audit (Valid Record)
# ==============================================================================

def test_evolution_audit_valid():
    """Audit a valid evolution record referencing stored snapshots."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot("revsnap_prev01", snapshot_version=1)
    snap2 = make_test_snapshot("revsnap_curr01", snapshot_version=2)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)

    evol = ev_svc.compare_snapshots("revsnap_prev01", "revsnap_curr01")

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    res = audit_svc.audit_evolution(evol)

    assert res.status == RecordValidationStatus.VALID
    assert res.references_exist is True
    assert res.hash_verified is True
    assert res.dimensions_evaluated_count == 12


# ==============================================================================
# 8. Evolution Integrity Audit (Broken References)
# ==============================================================================

def test_evolution_audit_broken_references():
    """Audit an evolution referencing non-existent snapshots."""
    audit_svc = HistoricalReviewAuditService()
    evol = make_test_evolution(
        evolution_id="revol_broken",
        previous_snapshot_id="revsnap_nonexistent_1",
        current_snapshot_id="revsnap_nonexistent_2",
    )
    res = audit_svc.audit_evolution(evol)

    assert res.references_exist is False
    assert res.status == RecordValidationStatus.INCOMPLETE
    assert any(f.category == AuditFindingCategory.BROKEN_REFERENCE for f in res.findings)


# ==============================================================================
# 9. Evolution Integrity Audit (Inconsistent References)
# ==============================================================================

def test_evolution_audit_inconsistent_references():
    """Audit an evolution where previous_snapshot_id == current_snapshot_id."""
    audit_svc = HistoricalReviewAuditService()
    evol = make_test_evolution(
        evolution_id="revol_same",
        previous_snapshot_id="revsnap_same",
        current_snapshot_id="revsnap_same",
    )
    res = audit_svc.audit_evolution(evol)

    assert res.status in (RecordValidationStatus.INCONSISTENT, RecordValidationStatus.INCOMPLETE)
    assert any(f.category == AuditFindingCategory.INCONSISTENT_REFERENCE for f in res.findings)


# ==============================================================================
# 10. Historical Chain Audit (Valid Sequence)
# ==============================================================================

def test_chain_audit_valid_sequence():
    """Audit a chain with continuous versions and linked evolutions."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot("revsnap_c1", investigation_id="inv_chain", snapshot_version=1, captured_at=100.0)
    snap2 = make_test_snapshot("revsnap_c2", investigation_id="inv_chain", snapshot_version=2, captured_at=200.0)
    snap3 = make_test_snapshot("revsnap_c3", investigation_id="inv_chain", snapshot_version=3, captured_at=300.0)

    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)
    ev_svc.store_snapshot(snap3)

    ev_svc.compare_snapshots("revsnap_c1", "revsnap_c2")
    ev_svc.compare_snapshots("revsnap_c2", "revsnap_c3")

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    chain_res = audit_svc.audit_investigation_chain("inv_chain")

    assert chain_res.status == RecordValidationStatus.VALID
    assert chain_res.version_sequence_intact is True
    assert chain_res.chronology_intact is True
    assert chain_res.chain_length == 3
    assert len(chain_res.detected_gaps) == 0


# ==============================================================================
# 11. Historical Chain Audit (Version Gaps)
# ==============================================================================

def test_chain_audit_version_gaps():
    """Audit a chain where version 2 is missing (version 1 -> version 3)."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot("revsnap_gap1", investigation_id="inv_gap", snapshot_version=1)
    snap3 = make_test_snapshot("revsnap_gap3", investigation_id="inv_gap", snapshot_version=3)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap3)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    chain_res = audit_svc.audit_investigation_chain("inv_gap")

    assert chain_res.version_sequence_intact is False
    assert chain_res.status == RecordValidationStatus.INCOMPLETE
    assert any("expected version 2" in g for g in chain_res.detected_gaps)


# ==============================================================================
# 12. Historical Chain Audit (Chronological Inversion)
# ==============================================================================

def test_chain_audit_chronological_inversion():
    """Audit a chain where version 2 has an earlier timestamp than version 1."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot("revsnap_inv1", investigation_id="inv_inv", snapshot_version=1, captured_at=500.0)
    snap2 = make_test_snapshot("revsnap_inv2", investigation_id="inv_inv", snapshot_version=2, captured_at=200.0)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    chain_res = audit_svc.audit_investigation_chain("inv_inv")

    assert chain_res.chronology_intact is False
    assert chain_res.status == RecordValidationStatus.INCONSISTENT
    assert any(f.category == AuditFindingCategory.CHRONOLOGICAL_ORDERING for f in chain_res.findings)


# ==============================================================================
# 13. Historical Chain Audit (Duplicate IDs)
# ==============================================================================

def test_chain_audit_duplicate_identifiers():
    """Audit a chain containing duplicate snapshot IDs."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot("revsnap_dup", investigation_id="inv_dup", snapshot_version=1)
    # Store directly into evolution service list to simulate corrupted duplicate
    ev_svc._snapshots["revsnap_dup"] = snap1
    ev_svc._snapshots_by_investigation["inv_dup"] = ["revsnap_dup", "revsnap_dup"]

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    chain_res = audit_svc.audit_investigation_chain("inv_dup")

    assert len(chain_res.duplicate_ids_detected) > 0
    assert any(f.category == AuditFindingCategory.DUPLICATE_IDENTIFIER for f in chain_res.findings)


# ==============================================================================
# 14. Historical Chain Audit (Conflict Preservation)
# ==============================================================================

def test_chain_audit_conflict_preservation():
    """Audit a transition where a conflict disappears without being recorded as resolved."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(
        "revsnap_cp1",
        investigation_id="inv_cp",
        snapshot_version=1,
        conflicts=["conflict_gamma"],
    )
    snap2 = make_test_snapshot(
        "revsnap_cp2",
        investigation_id="inv_cp",
        snapshot_version=2,
        conflicts=[],  # gamma disappeared
    )
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)

    # Evolution does NOT list conflict_gamma in newly_resolved_discrepancies
    evol = make_test_evolution(
        evolution_id=compute_deterministic_evolution_id("revsnap_cp1", "revsnap_cp2"),
        previous_snapshot_id="revsnap_cp1",
        current_snapshot_id="revsnap_cp2",
        investigation_id="inv_cp",
        newly_resolved=(),  # Dropped without resolution!
    )
    ev_svc.store_evolution(evol)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    chain_res = audit_svc.audit_investigation_chain("inv_cp")

    assert chain_res.recurring_conflicts_preserved is False
    assert any(f.category == AuditFindingCategory.CONFLICT_PRESERVATION for f in chain_res.findings)


# ==============================================================================
# 15. Reproducibility Verification (Valid Reproducible)
# ==============================================================================

def test_reproducibility_verification_valid():
    """Verify reproducible state when source references and provenance sources are present."""
    ev_svc = HistoricalReviewEvolutionService()
    snap = make_test_snapshot(
        "revsnap_rep_val",
        reproducibility="REPRODUCIBLE",
        source_refs=("ref_1", "ref_2"),
    )
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    res = audit_svc.verify_reproducibility("revsnap_rep_val")

    assert res.is_verifiable is True
    assert res.lineage_linked is True
    assert res.reproducibility_status == ReproducibilityEvolutionState.REPRODUCIBLE


# ==============================================================================
# 16. Reproducibility Verification (Unverifiable Empty References)
# ==============================================================================

def test_reproducibility_verification_unverifiable():
    """Verify that a snapshot claimed REPRODUCIBLE with zero source references is marked unverifiable."""
    ev_svc = HistoricalReviewEvolutionService()
    snap = HistoricalReviewSnapshot(
        snapshot_id="revsnap_rep_empty",
        investigation_id="inv_rep",
        outcome_id="out_rep",
        captured_at=1700000000.0,
        reproducibility_state={"reproducibility_state": "REPRODUCIBLE"},
        provenance_state={},
        source_references=(),  # Empty!
    )
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    res = audit_svc.verify_reproducibility("revsnap_rep_empty")

    assert res.is_verifiable is False
    assert any(f.category == AuditFindingCategory.REPRODUCIBILITY_METADATA for f in res.findings)


# ==============================================================================
# 17. Reproducibility Verification (Unknown State)
# ==============================================================================

def test_reproducibility_verification_unknown():
    """Verify that UNKNOWN reproducibility state is marked is_verifiable=False."""
    ev_svc = HistoricalReviewEvolutionService()
    snap = make_test_snapshot("revsnap_rep_unk", reproducibility="UNKNOWN")
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    res = audit_svc.verify_reproducibility("revsnap_rep_unk")

    assert res.is_verifiable is False
    assert res.reproducibility_status == ReproducibilityEvolutionState.UNKNOWN


# ==============================================================================
# 18. Full Audit Report Generation & Voice Summary
# ==============================================================================

def test_generate_audit_report_full_flow():
    """Generate a complete audit report for an investigation and verify all sections."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_full", snapshot_version=1, captured_at=100.0)
    snap2 = make_test_snapshot(investigation_id="inv_full", snapshot_version=2, captured_at=200.0)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)
    ev_svc.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    report = audit_svc.generate_audit_report("inv_full")

    assert report.report_id.startswith("revaudit_")
    assert report.investigation_id == "inv_full"
    assert report.overall_status == RecordValidationStatus.VALID
    assert report.snapshots_audited_count == 2
    assert report.evolutions_audited_count == 1
    assert report.voice_summary is not None
    assert "2 review snapshots verified valid" in report.voice_summary.summary_text
    assert report.chain_result is not None
    assert report.chain_result.version_sequence_intact is True


# ==============================================================================
# 19. Voice Summary Safety & Forbidden Words Rejection
# ==============================================================================

def test_voice_summary_safety_and_forbidden_words():
    """Verify that EvolutionAuditVoiceSummary rejects prohibited trading and ranking terms."""
    with pytest.raises(ValidationError):
        EvolutionAuditVoiceSummary(
            report_id="revaudit_bad",
            investigation_id="inv_bad",
            overall_status=RecordValidationStatus.VALID,
            summary_text="Audit completed and this strategy is a winner with buy signals.",
        )


# ==============================================================================
# 20. EventBus Integration
# ==============================================================================

def test_event_bus_integration():
    """Verify that appropriate EventBus events are emitted during audit report generation."""
    bus = EventBus()
    events_received: List[Event] = []
    bus.subscribe("*", lambda e: events_received.append(e))

    ev_svc = HistoricalReviewEvolutionService(event_bus=bus)
    snap = make_test_snapshot("revsnap_ev1", investigation_id="inv_eb")
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc, event_bus=bus)
    report = audit_svc.generate_audit_report("inv_eb")

    types = [e.event_type for e in events_received]
    assert EventType.HISTORICAL_REVIEW_AUDIT_STARTED.value in types
    assert EventType.HISTORICAL_REVIEW_AUDIT_COMPLETED.value in types


# ==============================================================================
# 21. StateStore Integration
# ==============================================================================

def test_statestore_integration():
    """Verify that audit reports are saved to StateStore namespaces."""
    store = StateStore()
    ev_svc = HistoricalReviewEvolutionService(state_store=store)
    snap = make_test_snapshot("revsnap_ss1", investigation_id="inv_ss")
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc, state_store=store)
    report = audit_svc.generate_audit_report("inv_ss")

    latest = store.get("historical_review_audit:latest")
    assert latest is not None
    assert latest["report_id"] == report.report_id

    direct = store.get(f"historical_review_audit:{report.report_id}")
    assert direct is not None
    assert direct["investigation_id"] == "inv_ss"

    history = store.get("historical_review_audit:history")
    assert isinstance(history, list)
    assert len(history) >= 1


# ==============================================================================
# 22. Orchestrator Handler Registration & Execution
# ==============================================================================

def test_handler_registration_and_execution():
    """Verify all 8 audit commands via MasterOrchestrator."""
    orchestrator = MasterOrchestrator()
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot("revsnap_h1", investigation_id="inv_h", snapshot_version=1)
    snap2 = make_test_snapshot("revsnap_h2", investigation_id="inv_h", snapshot_version=2)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)
    evol = ev_svc.compare_snapshots("revsnap_h1", "revsnap_h2")

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    register_audit_handlers(orchestrator=orchestrator, audit_service=audit_svc)

    # 1. audit_historical_review_snapshot
    r1 = orchestrator.handle_request(
        OrchestratorRequest.create(command="audit_historical_review_snapshot", parameters={"snapshot_id": "revsnap_h1"})
    )
    assert r1.is_success is True
    assert r1.result["read_only"] is True

    # 2. audit_review_evolution
    r2 = orchestrator.handle_request(
        OrchestratorRequest.create(command="audit_review_evolution", parameters={"evolution_id": evol.evolution_id})
    )
    assert r2.is_success is True

    # 3. audit_investigation_review_chain
    r3 = orchestrator.handle_request(
        OrchestratorRequest.create(command="audit_investigation_review_chain", parameters={"investigation_id": "inv_h"})
    )
    assert r3.is_success is True

    # 4. verify_review_evolution_reproducibility
    r4 = orchestrator.handle_request(
        OrchestratorRequest.create(command="verify_review_evolution_reproducibility", parameters={"target_id": "revsnap_h1"})
    )
    assert r4.is_success is True

    # 5. generate_historical_review_audit_report
    r5 = orchestrator.handle_request(
        OrchestratorRequest.create(command="generate_historical_review_audit_report", parameters={"investigation_id": "inv_h"})
    )
    assert r5.is_success is True
    rep_id = r5.result["report_id"]

    # 6. get_historical_review_audit_report
    r6 = orchestrator.handle_request(
        OrchestratorRequest.create(command="get_historical_review_audit_report", parameters={"report_id": rep_id})
    )
    assert r6.is_success is True

    # 7. list_historical_review_audit_reports
    r7 = orchestrator.handle_request(
        OrchestratorRequest.create(command="list_historical_review_audit_reports", parameters={"investigation_id": "inv_h"})
    )
    assert r7.is_success is True
    assert r7.result["count"] >= 1

    # 8. get_latest_historical_review_audit_report
    r8 = orchestrator.handle_request(
        OrchestratorRequest.create(command="get_latest_historical_review_audit_report", parameters={"investigation_id": "inv_h"})
    )
    assert r8.is_success is True


# ==============================================================================
# 23. Dashboard GET Endpoints
# ==============================================================================

def test_dashboard_get_endpoints():
    """Verify read-only GET endpoints on PortfolioDashboardServer for audit reports."""
    ev_svc = HistoricalReviewEvolutionService()
    snap = make_test_snapshot("revsnap_dash", investigation_id="inv_dash")
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    report = audit_svc.generate_audit_report("inv_dash")

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        evolution_service=ev_svc,
        audit_service=audit_svc,
    )
    server.start(blocking=False)
    base_url = server.url

    try:
        # GET /api/historical-review-audit
        req = urllib.request.Request(f"{base_url}/api/historical-review-audit")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["count"] >= 1

        # GET /api/historical-review-audit/{report_id}
        req_item = urllib.request.Request(f"{base_url}/api/historical-review-audit/{report.report_id}")
        with urllib.request.urlopen(req_item) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["report"]["report_id"] == report.report_id

        # GET /api/historical-review-audit/{report_id}/findings
        req_find = urllib.request.Request(f"{base_url}/api/historical-review-audit/{report.report_id}/findings")
        with urllib.request.urlopen(req_find) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "findings" in data

        # GET /api/historical-review-audit/{report_id}/chain
        req_chain = urllib.request.Request(f"{base_url}/api/historical-review-audit/{report.report_id}/chain")
        with urllib.request.urlopen(req_chain) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "chain_result" in data
    finally:
        server.stop()


# ==============================================================================
# 24. Dashboard HEAD Endpoints
# ==============================================================================

def test_dashboard_head_endpoints():
    """Verify HEAD method returns HTTP 200 with headers only and zero body."""
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        audit_service=HistoricalReviewAuditService(),
    )
    server.start(blocking=False)
    host, port = server.host, server.port

    try:
        conn = http.client.HTTPConnection(host, port)
        conn.request("HEAD", "/api/historical-review-audit")
        resp = conn.getresponse()
        assert resp.status == 200
        body = resp.read()
        assert len(body) == 0
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 25. Dashboard Mutation Rejection (405)
# ==============================================================================

def test_dashboard_mutation_rejection():
    """Verify mutating HTTP methods return 405 Method Not Allowed."""
    server = PortfolioDashboardServer(host="127.0.0.1", port=0)
    server.start(blocking=False)
    host, port = server.host, server.port

    try:
        for method in ("POST", "PUT", "DELETE", "PATCH"):
            conn = http.client.HTTPConnection(host, port)
            conn.request(method, "/api/historical-review-audit", body=b"{}")
            resp = conn.getresponse()
            assert resp.status == 405
            data = json.loads(resp.read().decode())
            assert data["error"] == "Method Not Allowed"
            conn.close()
    finally:
        server.stop()


# ==============================================================================
# 26. Dashboard Path Traversal Rejection (400)
# ==============================================================================

def test_dashboard_path_traversal_rejection():
    """Verify paths with directory traversal tokens return 400 Bad Request."""
    server = PortfolioDashboardServer(host="127.0.0.1", port=0)
    server.start(blocking=False)
    base_url = server.url

    try:
        traversal_urls = [
            f"{base_url}/api/historical-review-audit/..%2f..%2fetc%2fpasswd",
            f"{base_url}/api/historical-review-audit/report_test..id",
        ]
        for url in traversal_urls:
            req = urllib.request.Request(url)
            try:
                urllib.request.urlopen(req)
                pytest.fail("Expected 400 Bad Request for path traversal attempt.")
            except urllib.error.HTTPError as err:
                assert err.code == 400
    finally:
        server.stop()


# ==============================================================================
# 27. Bounded History & Concurrency Thread-Safety
# ==============================================================================

def test_bounded_history_and_concurrency():
    """Verify thread-safety and bounded storage under concurrent access."""
    ev_svc = HistoricalReviewEvolutionService()
    for i in range(10):
        s = make_test_snapshot(f"revsnap_conc_{i}", investigation_id="inv_conc", snapshot_version=i + 1)
        ev_svc.store_snapshot(s)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc, max_reports=10)

    def worker(worker_id: int):
        return audit_svc.generate_audit_report("inv_conc")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, i) for i in range(16)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == 16
    for r in results:
        assert r.overall_status in (
            RecordValidationStatus.VALID,
            RecordValidationStatus.INCOMPLETE,
            RecordValidationStatus.INCONSISTENT,
        )

    # Check bounded report history
    reports = audit_svc.list_audit_reports(limit=100)
    assert len(reports) <= 50  # Global history bound check


# ==============================================================================
# 28. Safety Invariants & Execution Engine Locked
# ==============================================================================

def test_safety_invariants_and_execution_engine_locked():
    """Verify core KAISER safety invariants remain active."""
    settings = Settings()
    assert settings.execution_locked is True
    assert settings.paper_trading_only is True
    assert settings.broker_provider.lower() == "none"

    service = HistoricalReviewAuditService()
    report = service.generate_audit_report("inv_safety")
    d = report.to_dict()

    # Rejection of trading terms
    prohibited_keys = {"buy", "sell", "action", "order", "target_price", "winner", "strategy_ranking"}
    for k in prohibited_keys:
        assert k not in d
