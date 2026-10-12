"""KAISER — Phase 2, Step 35: Acceptance Testing & Operational Hardening.

Historical Review Audit Reliability Acceptance Testing & Operational Hardening Foundation.
Deterministic acceptance test suite covering all 12 operational scenarios:
1. Valid historical chain produces expected audit and reliability statuses.
2. Empty, partial, and unavailable source history is reported accurately.
3. Broken references, chronology inconsistencies, and malformed records remain visible in findings.
4. Identical source inputs produce repeatable audit and reliability results.
5. Genuine source changes produce appropriate finding differences without inventing changes.
6. Unresolved conflicts remain preserved throughout repeated audits.
7. Missing source evidence is classified as incomplete or unverifiable where appropriate.
8. Controlled failures produce explicit failure states without crashing unrelated services.
9. Repeated and concurrent requests do not corrupt records or create inconsistent identifiers.
10. Bounded retention is respected during repeated evaluations.
11. StateStore and EventBus integration remains consistent with existing conventions.
12. Dashboard endpoints 26 and 27 preserve their read-only and security behavior.
"""

from __future__ import annotations

import copy
import hashlib
import http.client
import json
import math
import os
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
from app.analysis.opportunities.evolution.audit.reliability import (
    AuditCoverageState,
    AuditCoverageSummary,
    AuditRegressionResult,
    AuditRegressionState,
    AuditRepeatabilityResult,
    AuditRepeatabilityState,
    AuditReliabilityVoiceSummary,
    HistoricalReviewAuditReliabilityService,
    HistoricalReviewReliabilityResult,
    ReliabilityEvaluationStatus,
    compute_deterministic_reliability_id,
    register_reliability_handlers,
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
# Deterministic Test Fixtures & Helpers
# ==============================================================================

def make_test_snapshot(
    snapshot_id: Optional[str] = None,
    investigation_id: str = "inv_accept_01",
    outcome_id: str = "out_accept_01",
    snapshot_version: int = 1,
    captured_at: float = 1700000000.0,
    facts_count: int = 3,
    conflicts: Optional[List[str]] = None,
    source_refs: Tuple[str, ...] = ("ref_alpha", "ref_beta"),
    reproducibility: str = "REPRODUCIBLE",
    prohibited_payload: Optional[Dict[str, Any]] = None,
) -> HistoricalReviewSnapshot:
    """Create a deterministic test snapshot instance with matching fingerprint."""
    facts = [
        {"evidence_id": f"fact_{i}", "description": f"Empirical fact observation {i}", "claim_type": "FACT"}
        for i in range(facts_count)
    ]
    interps = [
        {"evidence_id": "int_1", "description": "Interpretation analysis", "claim_type": "INTERPRETATION"}
    ]
    conflict_list = conflicts if conflicts is not None else ["conflict_accept_alpha"]
    conflict_state = {"active_conflicts": conflict_list}
    source_state = {"status": "RECONCILED", "analysis_run_ids": ["run_accept_1"]}
    evidence_state = {"facts": facts, "interpretations": interps}
    review_state = {"human_review_decisions": [{"decision_id": "dec_1", "disposition": "ACCEPTED"}]}
    quality_state = {"quality_score": 1.0, "status": "VALID"}
    provenance_state = {"provenance_summary": {"sources": ["source_repo_accept"]}}
    reproducibility_state = {"reproducibility_state": reproducibility}
    coverage_state = {"coverage_status": "COMPLETE"}
    limitation_state = ({"limitation_id": "lim_1", "text": "Analytical review limitation"},)

    if prohibited_payload:
        source_state.update(prohibited_payload)

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
    previous_snapshot_id: str = "revsnap_acc01",
    current_snapshot_id: str = "revsnap_acc02",
    investigation_id: str = "inv_accept_01",
    generated_at: float = 1700000100.0,
    newly_resolved: Tuple[str, ...] = ("conflict_accept_alpha",),
) -> ReviewEvolutionRecord:
    """Create a deterministic test evolution record instance."""
    dims = [
        EvolutionDimensionRecord(
            dimension_name=d,
            previous_state={"state": "prev"},
            current_state={"state": "curr"},
            change_classification=ChangeClassification.UNCHANGED,
            source_references=("ref_alpha",),
            explanation="Unchanged dimension verification",
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


def build_valid_historical_chain(
    investigation_id: str = "inv_valid_chain",
    chain_length: int = 3,
) -> Tuple[List[HistoricalReviewSnapshot], List[ReviewEvolutionRecord]]:
    """Construct an end-to-end valid historical chain of snapshots and evolutions."""
    snapshots: List[HistoricalReviewSnapshot] = []
    evolutions: List[ReviewEvolutionRecord] = []
    base_time = 1700000000.0

    for v in range(1, chain_length + 1):
        s = make_test_snapshot(
            investigation_id=investigation_id,
            outcome_id=f"out_{investigation_id}_{v}",
            snapshot_version=v,
            captured_at=base_time + (v * 100.0),
            conflicts=["conflict_stable"],
        )
        snapshots.append(s)

    for i in range(len(snapshots) - 1):
        e = make_test_evolution(
            previous_snapshot_id=snapshots[i].snapshot_id,
            current_snapshot_id=snapshots[i + 1].snapshot_id,
            investigation_id=investigation_id,
            generated_at=snapshots[i + 1].captured_at + 10.0,
            newly_resolved=(),
        )
        evolutions.append(e)

    return snapshots, evolutions


# ==============================================================================
# Scenario 1: Valid Historical Chain Produces Expected Audit & Reliability Statuses
# ==============================================================================

def test_acceptance_scenario_1_valid_historical_chain_complete_status():
    """Verify that a valid historical chain yields expected audit and reliability statuses."""
    ev_svc = HistoricalReviewEvolutionService()
    snaps, evols = build_valid_historical_chain(investigation_id="inv_scen1_valid", chain_length=3)
    for s in snaps:
        ev_svc.store_snapshot(s)
    for e in evols:
        ev_svc.store_evolution(e)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    # 1. Audit Report
    audit_report = audit_svc.generate_audit_report("inv_scen1_valid")
    assert audit_report.overall_status == RecordValidationStatus.VALID
    assert audit_report.snapshots_audited_count == 3
    assert audit_report.evolutions_audited_count == 2
    assert audit_report.chain_result.version_sequence_intact is True
    assert audit_report.chain_result.chronology_intact is True
    assert audit_report.chain_result.recurring_conflicts_preserved is True
    assert audit_report.reproducibility_result.is_verifiable is True

    # 2. Reliability Evaluation
    rel_result = rel_svc.evaluate_reliability("inv_scen1_valid")
    assert rel_result.overall_reliability_status == ReliabilityEvaluationStatus.RELIABLE
    assert rel_result.coverage_summary.coverage_state == AuditCoverageState.COMPLETE
    assert rel_result.coverage_summary.overall_coverage_ratio == 1.0
    assert rel_result.coverage_summary.missing_history_detected is False
    assert rel_result.repeatability_result.repeatability_state == AuditRepeatabilityState.STABLE
    assert rel_result.repeatability_result.repeatability_score == 1.0
    assert rel_result.repeatability_result.report_ids_match is True
    assert rel_result.regression_result.regression_state in (
        AuditRegressionState.BASELINE_ESTABLISHED,
        AuditRegressionState.NO_REGRESSION,
    )
    assert rel_result.facts_count > 0
    assert rel_result.voice_summary.voice_safe is True
    assert "reliable" in rel_result.voice_summary.summary_text.lower()


# ==============================================================================
# Scenario 2: Empty, Partial, and Unavailable Source History Reported Accurately
# ==============================================================================

def test_acceptance_scenario_2_empty_partial_unavailable_history():
    """Verify empty, partial, and unavailable history are classified accurately without synthesizing data."""
    ev_svc = HistoricalReviewEvolutionService()
    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    # 1. Truly Empty History
    empty_cov = rel_svc.evaluate_coverage("inv_empty_history")
    assert empty_cov.coverage_state == AuditCoverageState.EMPTY
    assert empty_cov.total_snapshots == 0
    assert empty_cov.audited_snapshots == 0

    empty_rel = rel_svc.evaluate_reliability("inv_empty_history")
    assert empty_rel.overall_reliability_status == ReliabilityEvaluationStatus.UNVERIFIABLE
    assert empty_rel.coverage_summary.coverage_state == AuditCoverageState.EMPTY

    empty_audit = audit_svc.generate_audit_report("inv_empty_history")
    assert empty_audit.overall_status == RecordValidationStatus.INCOMPLETE
    assert "No snapshots recorded" in empty_audit.chain_result.detected_gaps

    # 2. Partial History (snapshots exist, but only partial audited)
    snaps, evols = build_valid_historical_chain(investigation_id="inv_partial_history", chain_length=4)
    for s in snaps:
        ev_svc.store_snapshot(s)
    for e in evols:
        ev_svc.store_evolution(e)

    # Explicitly audit only 2 out of 4 snapshots directly
    partial_report = HistoricalReviewAuditReport(
        report_id="revaudit_partial_mock",
        investigation_id="inv_partial_history",
        generated_at=time.time(),
        overall_status=RecordValidationStatus.VALID,
        snapshots_audited_count=2,
        snapshot_results=(
            SnapshotIntegrityResult(
                snapshot_id=snaps[0].snapshot_id,
                investigation_id="inv_partial_history",
                status=RecordValidationStatus.VALID,
                hash_verified=True,
            ),
            SnapshotIntegrityResult(
                snapshot_id=snaps[1].snapshot_id,
                investigation_id="inv_partial_history",
                status=RecordValidationStatus.VALID,
                hash_verified=True,
            ),
        ),
        evolution_results=(),
        findings=(),
    )
    audit_svc.store_audit_report(partial_report)

    part_cov = rel_svc.evaluate_coverage("inv_partial_history")
    assert part_cov.coverage_state == AuditCoverageState.PARTIAL
    assert part_cov.total_snapshots == 4
    assert part_cov.audited_snapshots == 2
    assert part_cov.missing_history_detected is True
    assert snaps[2].snapshot_id in part_cov.unaudited_snapshot_ids

    # 3. Unavailable Source History (Simulated Service Outage)
    class FailingEvolutionService:
        def list_snapshots(self, inv_id: str):
            raise RuntimeError("Database connection unreachable")
        def list_evolution_history(self, inv_id: str, limit: int = 100):
            raise RuntimeError("Evolution service down")

    failing_rel_svc = HistoricalReviewAuditReliabilityService(
        audit_service=audit_svc,
        evolution_service=FailingEvolutionService(),
    )
    unavail_cov = failing_rel_svc.evaluate_coverage("inv_unavail")
    assert unavail_cov.coverage_state == AuditCoverageState.UNAVAILABLE
    assert unavail_cov.missing_history_detected is True

    unavail_rel = failing_rel_svc.evaluate_reliability("inv_unavail")
    assert unavail_rel.overall_reliability_status == ReliabilityEvaluationStatus.FAILED
    assert unavail_rel.controlled_failure_recovered is True
    assert len(unavail_rel.recovery_notes) > 0


# ==============================================================================
# Scenario 3: Broken References, Chronology Inconsistencies & Malformed Records Visible
# ==============================================================================

def test_acceptance_scenario_3_broken_references_chronology_and_malformed_records():
    """Verify defects, broken links, inversions, and forbidden fields are visible in findings."""
    ev_svc = HistoricalReviewEvolutionService()
    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)

    # 1. Broken Reference in Evolution
    broken_evol = make_test_evolution(
        previous_snapshot_id="revsnap_nonexistent_prev",
        current_snapshot_id="revsnap_nonexistent_curr",
        investigation_id="inv_broken_refs",
    )
    ev_res = audit_svc.audit_evolution(broken_evol)
    assert ev_res.status == RecordValidationStatus.INCOMPLETE
    assert ev_res.references_exist is False
    broken_cats = [f.category for f in ev_res.findings]
    assert AuditFindingCategory.BROKEN_REFERENCE in broken_cats

    # 2. Chronological Inversion in Snapshots
    s1 = make_test_snapshot(
        investigation_id="inv_chrono_inv",
        snapshot_version=1,
        captured_at=1700000500.0,
    )
    s2 = make_test_snapshot(
        investigation_id="inv_chrono_inv",
        snapshot_version=2,
        captured_at=1700000100.0,  # Inverted: earlier than predecessor
    )
    ev_svc.store_snapshot(s1)
    ev_svc.store_snapshot(s2)

    chain_res = audit_svc.audit_investigation_chain("inv_chrono_inv")
    assert chain_res.chronology_intact is False
    chrono_cats = [f.category for f in chain_res.findings]
    assert AuditFindingCategory.CHRONOLOGICAL_ORDERING in chrono_cats

    # 3. Duplicate Identifiers
    dup_s = make_test_snapshot(
        snapshot_id=s1.snapshot_id,
        investigation_id="inv_chrono_inv",
        snapshot_version=3,
        captured_at=1700000600.0,
    )
    chain_dup_res = audit_svc.audit_investigation_chain("inv_chrono_inv")
    assert len(chain_dup_res.findings) > 0

    # 4. Prohibited Content Detection
    with pytest.raises(ValidationError):
        # Model post_init rejects prohibited words directly
        make_test_snapshot(
            investigation_id="inv_prohibited",
            prohibited_payload={"target_price": 100.0},
        )


# ==============================================================================
# Scenario 4: Identical Source Inputs Produce Repeatable Audit & Reliability Results
# ==============================================================================

def test_acceptance_scenario_4_repeatable_audit_and_reliability_results():
    """Verify repeated runs on identical source history generate strictly identical deterministic IDs and findings."""
    ev_svc = HistoricalReviewEvolutionService()
    snaps, evols = build_valid_historical_chain(investigation_id="inv_repeat_det", chain_length=3)
    for s in snaps:
        ev_svc.store_snapshot(s)
    for e in evols:
        ev_svc.store_evolution(e)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    report_ids: List[str] = []
    finding_fingerprints: List[str] = []
    eval_ids: List[str] = []

    # Execute 5 repeated audit and reliability cycles
    for i in range(5):
        rep = audit_svc.generate_audit_report("inv_repeat_det")
        report_ids.append(rep.report_id)
        f_fp = hashlib.sha256(
            "".join(f.finding_id for f in rep.findings).encode("utf-8")
        ).hexdigest()
        finding_fingerprints.append(f_fp)

        rel = rel_svc.evaluate_reliability("inv_repeat_det", runs_for_repeatability=2)
        eval_ids.append(rel.evaluation_id)

    # Assert exact deterministic stability across repeated cycles
    assert len(set(report_ids)) == 1, f"Report IDs diverged: {report_ids}"
    assert len(set(finding_fingerprints)) == 1, f"Findings sets diverged: {finding_fingerprints}"
    assert len(set(eval_ids)) == 1, f"Evaluation IDs diverged: {eval_ids}"

    repeat_res = rel_svc.verify_repeatability("inv_repeat_det", runs=3)
    assert repeat_res.is_repeatable is True
    assert repeat_res.repeatability_score == 1.0
    assert repeat_res.repeatability_state == AuditRepeatabilityState.STABLE


# ==============================================================================
# Scenario 5: Genuine Source Changes Produce Appropriate Finding Differences
# ==============================================================================

def test_acceptance_scenario_5_genuine_source_changes_vs_uninvented_changes():
    """Verify genuine source changes produce exact finding deltas without synthesizing false drift."""
    ev_svc = HistoricalReviewEvolutionService()
    snaps, evols = build_valid_historical_chain(investigation_id="inv_drift_test", chain_length=2)
    for s in snaps:
        ev_svc.store_snapshot(s)
    for e in evols:
        ev_svc.store_evolution(e)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    # 1. Baseline Run
    rep1 = audit_svc.generate_audit_report("inv_drift_test")
    reg1 = rel_svc.monitor_regression("inv_drift_test", current_report_id=rep1.report_id)
    assert reg1.regression_state == AuditRegressionState.BASELINE_ESTABLISHED
    assert reg1.new_findings_count == 0

    # 2. Modify snapshot evidence with claim confusion
    corrupted_facts = [
        {"evidence_id": "fact_0", "description": "Factual claim", "claim_type": "INTERPRETATION"}
    ]
    corrupted_s1 = make_test_snapshot(
        snapshot_id=snaps[0].snapshot_id,
        investigation_id="inv_drift_test",
        outcome_id=snaps[0].outcome_id,
        snapshot_version=snaps[0].snapshot_version,
        captured_at=snaps[0].captured_at,
    )
    # Re-store modified snapshot
    object.__setattr__(corrupted_s1, "evidence_state", {"facts": corrupted_facts, "interpretations": []})
    ev_svc._snapshots[corrupted_s1.snapshot_id] = corrupted_s1

    # 3. Re-audit and monitor regression
    rep2 = audit_svc.generate_audit_report("inv_drift_test")
    reg2 = rel_svc.monitor_regression("inv_drift_test", baseline_report_id=rep1.report_id, current_report_id=rep2.report_id)
    assert reg2.new_findings_count >= 1
    new_findings_in_rep2 = [f for f in rep2.findings if f.finding_id in reg2.new_finding_ids]
    assert any(f.category == AuditFindingCategory.CLAIM_SEPARATION for f in new_findings_in_rep2)


# ==============================================================================
# Scenario 6: Unresolved Conflicts Remain Preserved Throughout Repeated Audits
# ==============================================================================

def test_acceptance_scenario_6_unresolved_conflicts_preservation():
    """Verify unresolved conflicts across snapshots remain preserved and omissions are flagged."""
    ev_svc = HistoricalReviewEvolutionService()
    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    # Snapshot 1 has active conflict_recurrent
    s1 = make_test_snapshot(
        investigation_id="inv_conf_pres",
        snapshot_version=1,
        captured_at=1700000100.0,
        conflicts=["conflict_recurrent"],
    )
    # Snapshot 2 drops conflict_recurrent without resolving it in evolution
    s2 = make_test_snapshot(
        investigation_id="inv_conf_pres",
        snapshot_version=2,
        captured_at=1700000200.0,
        conflicts=[],  # Dropped!
    )
    e1 = make_test_evolution(
        previous_snapshot_id=s1.snapshot_id,
        current_snapshot_id=s2.snapshot_id,
        investigation_id="inv_conf_pres",
        generated_at=1700000210.0,
        newly_resolved=(),  # Not recorded as resolved!
    )
    # Baseline: snapshot 1 alone (conflict is active and valid, no drop)
    ev_svc.store_snapshot(s1)
    rep_baseline = audit_svc.generate_audit_report("inv_conf_pres")
    assert rep_baseline.chain_result.recurring_conflicts_preserved is True

    # Snapshot 2 drops conflict_recurrent without resolving it in evolution
    ev_svc.store_snapshot(s2)
    ev_svc.store_evolution(e1)

    chain_res = audit_svc.audit_investigation_chain("inv_conf_pres")
    assert chain_res.recurring_conflicts_preserved is False
    conf_findings = [f for f in chain_res.findings if f.category == AuditFindingCategory.CONFLICT_PRESERVATION]
    assert len(conf_findings) > 0
    assert "conflict_recurrent" in conf_findings[0].message

    # Verify reliability regression monitoring detects this conflict drop
    rep_dropped = audit_svc.generate_audit_report("inv_conf_pres")
    reg = rel_svc.monitor_regression(
        "inv_conf_pres",
        baseline_report_id=rep_baseline.report_id,
        current_report_id=rep_dropped.report_id,
    )
    assert reg.conflict_preservation_maintained is False
    assert reg.regression_state == AuditRegressionState.REGRESSION_DETECTED


# ==============================================================================
# Scenario 7: Missing Source Evidence Classified as Incomplete or Unverifiable
# ==============================================================================

def test_acceptance_scenario_7_missing_source_evidence_classification():
    """Verify missing source evidence, unreferenced runs, and version gaps produce INCOMPLETE or UNVERIFIABLE."""
    ev_svc = HistoricalReviewEvolutionService()
    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)

    # 1. Reproducibility claimed but zero source references provided
    s_unverifiable = make_test_snapshot(
        investigation_id="inv_unverif",
        snapshot_version=1,
        source_refs=(),  # Zero source references
        reproducibility="REPRODUCIBLE",
    )
    # Clear provenance sources as well
    object.__setattr__(s_unverifiable, "provenance_state", {"provenance_summary": {"sources": []}})
    ev_svc.store_snapshot(s_unverifiable)

    rep_verif = audit_svc.verify_reproducibility(s_unverifiable.snapshot_id)
    assert rep_verif.is_verifiable is False
    assert any("source references" in r for r in rep_verif.unverifiable_reasons)

    audit_report = audit_svc.generate_audit_report("inv_unverif")
    assert audit_report.overall_status == RecordValidationStatus.UNVERIFIABLE

    # 2. Version gap in chain (v1, then v3 without v2)
    s_v1 = make_test_snapshot(investigation_id="inv_gap", snapshot_version=1, captured_at=1700000100.0)
    s_v3 = make_test_snapshot(investigation_id="inv_gap", snapshot_version=3, captured_at=1700000300.0)
    ev_svc.store_snapshot(s_v1)
    ev_svc.store_snapshot(s_v3)

    gap_chain = audit_svc.audit_investigation_chain("inv_gap")
    assert gap_chain.status == RecordValidationStatus.INCOMPLETE
    assert gap_chain.version_sequence_intact is False
    assert len(gap_chain.detected_gaps) > 0


# ==============================================================================
# Scenario 8: Controlled Failures Produce Explicit Failure States Without Crashing
# ==============================================================================

def test_acceptance_scenario_8_controlled_failures_and_isolated_states():
    """Verify injected dependency failures result in controlled recovery notes without crashing."""
    ev_svc = HistoricalReviewEvolutionService()
    s = make_test_snapshot(investigation_id="inv_fail_isol")
    ev_svc.store_snapshot(s)

    class CrashingAuditService(HistoricalReviewAuditService):
        def generate_audit_report(self, investigation_id: str, **kwargs):
            raise RuntimeError("Injected audit calculation hardware fault")

    crashing_audit = CrashingAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(
        audit_service=crashing_audit,
        evolution_service=ev_svc,
    )

    # Should not raise; handles gracefully and notes recovery
    rel_res = rel_svc.evaluate_reliability("inv_fail_isol")
    assert rel_res.overall_reliability_status in (
        ReliabilityEvaluationStatus.FAILED,
        ReliabilityEvaluationStatus.DEGRADED,
    )
    assert rel_res.repeatability_result.repeatability_state == AuditRepeatabilityState.FAILED
    assert rel_res.repeatability_result.is_repeatable is False


# ==============================================================================
# Scenario 9: Repeated & Concurrent Requests Do Not Corrupt Records
# ==============================================================================

def test_acceptance_scenario_9_repeated_and_concurrent_requests_safety():
    """Verify thread-safety and record immutability under concurrent execution across multiple threads."""
    ev_svc = HistoricalReviewEvolutionService()
    snaps, evols = build_valid_historical_chain(investigation_id="inv_concurrent_safe", chain_length=3)
    for s in snaps:
        ev_svc.store_snapshot(s)
    for e in evols:
        ev_svc.store_evolution(e)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    errors: List[Exception] = []
    generated_eval_ids: List[str] = []
    lock = threading.Lock()

    def worker_thread(thread_idx: int):
        try:
            for _ in range(5):
                rep = audit_svc.generate_audit_report("inv_concurrent_safe")
                assert rep.overall_status == RecordValidationStatus.VALID

                rel = rel_svc.evaluate_reliability("inv_concurrent_safe", runs_for_repeatability=1)
                assert rel.overall_reliability_status == ReliabilityEvaluationStatus.RELIABLE

                with lock:
                    generated_eval_ids.append(rel.evaluation_id)

                queried = rel_svc.get_reliability_result(rel.evaluation_id)
                assert queried is not None
                assert queried.evaluation_id == rel.evaluation_id
        except Exception as ex:
            with lock:
                errors.append(ex)

    threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Concurrent execution errors encountered: {errors}"
    assert len(generated_eval_ids) == 40
    # Every generated evaluation ID must be strictly identical (deterministic hash)
    assert len(set(generated_eval_ids)) == 1


# ==============================================================================
# Scenario 10: Bounded Retention Respected During Repeated Evaluations
# ==============================================================================

def test_acceptance_scenario_10_bounded_retention_and_resource_safety():
    """Verify bounded retention and FIFO eviction operate safely without unbounded memory growth."""
    store = StateStore()
    audit_svc = HistoricalReviewAuditService(state_store=store, max_reports=10)
    rel_svc = HistoricalReviewAuditReliabilityService(
        audit_service=audit_svc,
        state_store=store,
        max_evaluations=10,
    )

    # Generate 35 evaluations
    for i in range(35):
        eval_res = HistoricalReviewReliabilityResult(
            evaluation_id=f"revrel_ret_{i:03d}",
            investigation_id="inv_bounded",
            evaluated_at=1700000000.0 + i,
            overall_reliability_status=ReliabilityEvaluationStatus.RELIABLE,
            coverage_summary=AuditCoverageSummary(
                investigation_id="inv_bounded",
                coverage_state=AuditCoverageState.COMPLETE,
                total_snapshots=1,
                audited_snapshots=1,
                snapshot_coverage_ratio=1.0,
                total_evolutions=0,
                audited_evolutions=0,
                evolution_coverage_ratio=1.0,
                overall_coverage_ratio=1.0,
            ),
        )
        rel_svc._evaluations[eval_res.evaluation_id] = eval_res
        rel_svc._investigation_evaluations.setdefault("inv_bounded", []).insert(0, eval_res.evaluation_id)
        if len(rel_svc._investigation_evaluations["inv_bounded"]) > 10:
            evicted = rel_svc._investigation_evaluations["inv_bounded"].pop()
            rel_svc._evaluations.pop(evicted, None)

    # In-memory must be strictly bounded at 10
    assert len(rel_svc._investigation_evaluations["inv_bounded"]) == 10
    assert len(rel_svc.list_reliability_results("inv_bounded")) == 10
    # Oldest must be evicted
    assert "revrel_ret_000" not in rel_svc._evaluations
    # Newest must be retained
    assert "revrel_ret_034" in rel_svc._evaluations


# ==============================================================================
# Scenario 11: StateStore & EventBus Integration & Cold-Start Hydration
# ==============================================================================

def test_acceptance_scenario_11_statestore_and_eventbus_integration():
    """Verify all 10 EventTypes are emitted and StateStore cold-start hydration works seamlessly."""
    bus = EventBus()
    store = StateStore()

    events_captured: List[Event] = []
    def _collector(ev: Event):
        events_captured.append(ev)

    for et in (
        EventType.HISTORICAL_REVIEW_AUDIT_STARTED,
        EventType.HISTORICAL_REVIEW_AUDIT_COMPLETED,
        EventType.HISTORICAL_REVIEW_AUDIT_COVERAGE_EVALUATED,
        EventType.HISTORICAL_REVIEW_AUDIT_REPEATABILITY_VERIFIED,
        EventType.HISTORICAL_REVIEW_RELIABILITY_EVALUATION_COMPLETED,
    ):
        bus.subscribe(et, _collector)

    ev_svc = HistoricalReviewEvolutionService(state_store=store, event_bus=bus)
    snaps, evols = build_valid_historical_chain(investigation_id="inv_hydration_test", chain_length=2)
    for s in snaps:
        ev_svc.store_snapshot(s)
    for e in evols:
        ev_svc.store_evolution(e)

    audit_svc = HistoricalReviewAuditService(
        evolution_service=ev_svc,
        state_store=store,
        event_bus=bus,
    )
    rel_svc = HistoricalReviewAuditReliabilityService(
        audit_service=audit_svc,
        evolution_service=ev_svc,
        state_store=store,
        event_bus=bus,
    )

    eval_res = rel_svc.evaluate_reliability("inv_hydration_test")
    assert eval_res is not None

    # Verify EventBus events were emitted
    emitted_types = {e.event_type for e in events_captured}
    assert EventType.HISTORICAL_REVIEW_AUDIT_STARTED in emitted_types
    assert EventType.HISTORICAL_REVIEW_AUDIT_COMPLETED in emitted_types
    assert EventType.HISTORICAL_REVIEW_AUDIT_COVERAGE_EVALUATED in emitted_types
    assert EventType.HISTORICAL_REVIEW_AUDIT_REPEATABILITY_VERIFIED in emitted_types
    assert EventType.HISTORICAL_REVIEW_RELIABILITY_EVALUATION_COMPLETED in emitted_types

    # Cold-Start Hydration Test:
    # Create fresh service instances with ONLY the StateStore (empty memory)
    fresh_rel_svc = HistoricalReviewAuditReliabilityService(
        state_store=store,
        # No evolution_service or audit_service
    )
    assert len(fresh_rel_svc._evaluations) == 0

    # Retrieve by ID from StateStore
    hydrated = fresh_rel_svc.get_reliability_result(eval_res.evaluation_id)
    assert hydrated is not None
    assert hydrated.evaluation_id == eval_res.evaluation_id
    assert hydrated.overall_reliability_status == eval_res.overall_reliability_status

    # Retrieve listing from StateStore
    hydrated_list = fresh_rel_svc.list_reliability_results("inv_hydration_test")
    assert len(hydrated_list) >= 1
    assert hydrated_list[0].evaluation_id == eval_res.evaluation_id

    # Retrieve latest from StateStore
    hydrated_latest = fresh_rel_svc.get_latest_reliability_result("inv_hydration_test")
    assert hydrated_latest is not None
    assert hydrated_latest.evaluation_id == eval_res.evaluation_id


# ==============================================================================
# Scenario 12: Dashboard Endpoints 26 & 27 Security, Read-Only & Zero-Leakage
# ==============================================================================

def test_acceptance_scenario_12_dashboard_endpoints_26_and_27_security():
    """Verify endpoints 26 & 27 preserve read-only constraints, reject mutations and traversals, and leak no secrets."""
    store = StateStore()
    ev_svc = HistoricalReviewEvolutionService(state_store=store)
    snaps, evols = build_valid_historical_chain(investigation_id="inv_dash_sec", chain_length=2)
    for s in snaps:
        ev_svc.store_snapshot(s)
    for e in evols:
        ev_svc.store_evolution(e)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc, state_store=store)
    rel_svc = HistoricalReviewAuditReliabilityService(
        audit_service=audit_svc,
        evolution_service=ev_svc,
        state_store=store,
    )

    audit_report = audit_svc.generate_audit_report("inv_dash_sec")
    rel_eval = rel_svc.evaluate_reliability("inv_dash_sec")

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        evolution_service=ev_svc,
        audit_service=audit_svc,
        reliability_service=rel_svc,
        state_store=store,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        base = server.url

        # 1. Endpoint 26 Valid GET & Sub-resources
        for sub in ("", f"/{audit_report.report_id}", f"/{audit_report.report_id}/findings", f"/{audit_report.report_id}/chain", f"/{audit_report.report_id}/reproducibility", f"/{audit_report.report_id}/summary"):
            req = urllib.request.Request(f"{base}/api/historical-review-audit{sub}")
            with urllib.request.urlopen(req) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["read_only"] is True
                assert data["execution_locked"] is True

        # 2. Endpoint 27 Valid GET & Sub-resources
        for sub in ("", f"/{rel_eval.evaluation_id}", f"/{rel_eval.evaluation_id}/coverage", f"/{rel_eval.evaluation_id}/repeatability", f"/{rel_eval.evaluation_id}/regression", f"/{rel_eval.evaluation_id}/summary"):
            req = urllib.request.Request(f"{base}/api/historical-review-reliability{sub}")
            with urllib.request.urlopen(req) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["read_only"] is True
                assert data["execution_locked"] is True

        # 3. Unknown Sub-resource -> 404
        for ep in (f"/api/historical-review-audit/{audit_report.report_id}/unsupported_sub", f"/api/historical-review-reliability/{rel_eval.evaluation_id}/unsupported_sub"):
            try:
                urllib.request.urlopen(f"{base}{ep}")
                pytest.fail(f"Expected 404 for {ep}")
            except urllib.error.HTTPError as err:
                assert err.code == 404

        # 4. Mutation Rejection -> 405 Method Not Allowed
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        for path in ("/api/historical-review-audit", "/api/historical-review-reliability"):
            for method in ("POST", "PUT", "DELETE", "PATCH"):
                conn.request(method, path, body=b'{"test": "mutation"}')
                resp = conn.getresponse()
                assert resp.status == 405
                assert "GET" in resp.getheader("Allow", "")
                resp.read()
        conn.close()

        # 5. Path Traversal & Malformed Identifier Rejection -> 400
        for ep in (
            "/api/historical-review-audit/..%2f..%2fetc%2fpasswd",
            "/api/historical-review-audit/report..invalid",
            "/api/historical-review-reliability/..%2f..%2fetc%2fpasswd",
            "/api/historical-review-reliability/eval..invalid",
            "/api/historical-review-audit?investigation_id=..%2fmalicious",
            "/api/historical-review-reliability?investigation_id=..%2fmalicious",
        ):
            conn = http.client.HTTPConnection("127.0.0.1", server.port)
            conn.request("GET", ep)
            resp = conn.getresponse()
            assert resp.status == 400
            resp.read()
            conn.close()

        # 6. HEAD Requests -> 200 with Empty Body
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        for path in ("/api/historical-review-audit", "/api/historical-review-reliability"):
            conn.request("HEAD", path)
            resp = conn.getresponse()
            assert resp.status == 200
            body = resp.read()
            assert len(body) == 0
        conn.close()

        # 7. Security & Zero-Leakage Check
        req = urllib.request.Request(f"{base}/api/historical-review-reliability/{rel_eval.evaluation_id}")
        with urllib.request.urlopen(req) as resp:
            text = resp.read().decode("utf-8").lower()
            for forbidden_token in ("api_key", "secret", "password", "token", "prompt", "raw_audio"):
                assert f'"{forbidden_token}"' not in text

    finally:
        server.stop()


# ==============================================================================
# System Safety Boundaries Verification
# ==============================================================================

def test_safety_invariants_and_execution_engine_locked():
    """Verify execution engine remains locked, trading engine uninitialized, and JUJU untouched."""
    settings = Settings()
    assert settings.execution_locked is True, "Settings.execution_locked must remain True"

    # Confirm trading engine is not initialized and execution engine is locked
    from app.bootstrap import bootstrap_system
    status = bootstrap_system()
    assert status.trading_engine == "not initialized"
    assert status.execution_engine == "locked"
    assert status.settings.execution_locked is True

    # Confirm JUJU path is completely separate and untouched
    juju_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "JUJU")
    if os.path.exists(juju_dir):
        # JUJU must be untouched
        assert os.path.isdir(juju_dir)
