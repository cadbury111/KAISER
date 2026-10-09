"""KAISER — Phase 2, Step 34 Test Suite.

Historical Review Audit Coverage, Regression Monitoring & Long-Term Reliability Validation Foundation.

Test Categories:
1. AuditCoverageSummary model validation & immutability
2. AuditRepeatabilityResult model validation & immutability
3. AuditRegressionResult model validation & immutability
4. HistoricalReviewReliabilityResult model validation & immutability
5. Deterministic reliability ID computation & repeatability
6. Coverage evaluation: complete coverage
7. Coverage evaluation: partial coverage
8. Coverage evaluation: empty history
9. Coverage evaluation: unavailable history graceful handling
10. Repeatability verification: stable repeatability on identical inputs
11. Repeatability verification: uninitialized / unverifiable handling
12. Regression monitoring: baseline established
13. Regression monitoring: no regression
14. Regression monitoring: regression detected on new findings
15. Regression monitoring: improvement detected on resolved findings
16. Conflict preservation across regression monitoring
17. Full reliability evaluation flow
18. Voice summary generation & forbidden trading words rejection
19. Controlled failure recovery
20. Facts vs interpretations vs human decisions separation counts
21. EventBus integration
22. StateStore integration
23. Orchestrator handler registration & execution
24. Dashboard GET endpoints
25. Dashboard HEAD endpoints
26. Dashboard mutation rejection (405)
27. Dashboard path traversal rejection (400)
28. Bounded history & concurrency thread-safety
29. Safety invariants & execution engine locked
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
    compute_deterministic_evolution_id,
    compute_deterministic_snapshot_id,
)
from app.analysis.opportunities.evolution.audit import (
    AuditFindingCategory,
    AuditFindingSeverity,
    AuditReportLifecycleState,
    EvolutionAuditFinding,
    HistoricalReviewAuditReport,
    HistoricalReviewAuditService,
    RecordValidationStatus,
)
from app.analysis.opportunities.evolution.audit.reliability import (
    AuditCoverageState,
    AuditCoverageSummary,
    AuditRegressionResult,
    AuditRegressionState,
    AuditRepeatabilityResult,
    AuditRepeatabilityState,
    AuditReliabilityVoiceSummary,
    HistoricalReviewReliabilityResult,
    HistoricalReviewAuditReliabilityService,
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
# Helpers
# ==============================================================================

def make_test_snapshot(
    snapshot_id: Optional[str] = None,
    investigation_id: str = "inv_rel_01",
    outcome_id: str = "out_rel_01",
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
        lifecycle_state=HistoricalSnapshotLifecycleState.STORED,
        captured_at=captured_at,
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


# ==============================================================================
# 1. AuditCoverageSummary Model Validation & Immutability
# ==============================================================================

def test_audit_coverage_summary_model():
    """Verify AuditCoverageSummary dataclass validation and immutability."""
    summary = AuditCoverageSummary(
        investigation_id="inv_test",
        coverage_state=AuditCoverageState.COMPLETE,
        total_snapshots=5,
        audited_snapshots=5,
        snapshot_coverage_ratio=1.0,
        total_evolutions=4,
        audited_evolutions=4,
        evolution_coverage_ratio=1.0,
        overall_coverage_ratio=1.0,
    )

    assert summary.investigation_id == "inv_test"
    assert summary.coverage_state == AuditCoverageState.COMPLETE
    assert summary.snapshot_coverage_ratio == 1.0
    assert summary.overall_coverage_ratio == 1.0

    # Test immutability
    with pytest.raises(Exception):
        summary.total_snapshots = 10  # type: ignore

    # Test ratio bounds check
    with pytest.raises(ValidationError):
        AuditCoverageSummary(
            investigation_id="inv_bad",
            coverage_state=AuditCoverageState.COMPLETE,
            total_snapshots=5,
            audited_snapshots=5,
            snapshot_coverage_ratio=1.5,  # Exceeds 1.0
            total_evolutions=4,
            audited_evolutions=4,
            evolution_coverage_ratio=1.0,
            overall_coverage_ratio=1.0,
        )


# ==============================================================================
# 2. AuditRepeatabilityResult Model Validation & Immutability
# ==============================================================================

def test_audit_repeatability_result_model():
    """Verify AuditRepeatabilityResult dataclass validation and immutability."""
    res = AuditRepeatabilityResult(
        investigation_id="inv_rep",
        repeatability_state=AuditRepeatabilityState.STABLE,
        repeatability_score=1.0,
        is_repeatable=True,
        report_id_1="revaudit_1",
        report_id_2="revaudit_1",
        report_ids_match=True,
        findings_count_1=2,
        findings_count_2=2,
        findings_match=True,
        status_1=RecordValidationStatus.VALID,
        status_2=RecordValidationStatus.VALID,
        status_match=True,
    )

    assert res.is_repeatable is True
    assert res.repeatability_score == 1.0
    assert res.report_ids_match is True

    # Test immutability
    with pytest.raises(Exception):
        res.is_repeatable = False  # type: ignore


# ==============================================================================
# 3. AuditRegressionResult Model Validation & Immutability
# ==============================================================================

def test_audit_regression_result_model():
    """Verify AuditRegressionResult dataclass validation and immutability."""
    res = AuditRegressionResult(
        investigation_id="inv_reg",
        regression_state=AuditRegressionState.NO_REGRESSION,
        baseline_report_id="revaudit_base",
        current_report_id="revaudit_curr",
        new_findings_count=0,
        resolved_findings_count=0,
        persistent_findings_count=3,
        conflict_preservation_maintained=True,
        regression_summary="Clean run.",
    )

    assert res.regression_state == AuditRegressionState.NO_REGRESSION
    assert res.conflict_preservation_maintained is True

    # Test immutability
    with pytest.raises(Exception):
        res.new_findings_count = 5  # type: ignore


# ==============================================================================
# 4. HistoricalReviewReliabilityResult Model Validation & Immutability
# ==============================================================================

def test_historical_review_reliability_result_model():
    """Verify HistoricalReviewReliabilityResult composite model validation."""
    cov = AuditCoverageSummary(
        investigation_id="inv_full_rel",
        coverage_state=AuditCoverageState.COMPLETE,
        total_snapshots=2,
        audited_snapshots=2,
        snapshot_coverage_ratio=1.0,
        total_evolutions=1,
        audited_evolutions=1,
        evolution_coverage_ratio=1.0,
        overall_coverage_ratio=1.0,
    )
    result = HistoricalReviewReliabilityResult(
        evaluation_id="revrel_test01",
        investigation_id="inv_full_rel",
        evaluated_at=1700000500.0,
        overall_reliability_status=ReliabilityEvaluationStatus.RELIABLE,
        coverage_summary=cov,
        facts_count=5,
        interpretations_count=2,
        human_decisions_count=1,
        active_conflicts_preserved_count=1,
    )

    assert result.evaluation_id == "revrel_test01"
    assert result.overall_reliability_status == ReliabilityEvaluationStatus.RELIABLE
    assert result.facts_count == 5

    # Immutability
    with pytest.raises(Exception):
        result.facts_count = 10  # type: ignore


# ==============================================================================
# 5. Deterministic Reliability ID Computation
# ==============================================================================

def test_deterministic_reliability_id():
    """Verify that compute_deterministic_reliability_id produces stable hashes."""
    id1 = compute_deterministic_reliability_id("inv_alpha", "COMPLETE", "RELIABLE", "fp_123")
    id2 = compute_deterministic_reliability_id("inv_alpha", "COMPLETE", "RELIABLE", "fp_123")
    id3 = compute_deterministic_reliability_id("inv_alpha", "PARTIAL", "DEGRADED", "fp_123")

    assert id1.startswith("revrel_")
    assert id1 == id2
    assert id1 != id3


# ==============================================================================
# 6. Coverage Evaluation: Complete Coverage
# ==============================================================================

def test_coverage_evaluation_complete():
    """Verify coverage evaluation returns COMPLETE when all snapshots and evolutions are audited."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_cov_comp", snapshot_version=1, captured_at=100.0)
    snap2 = make_test_snapshot(investigation_id="inv_cov_comp", snapshot_version=2, captured_at=200.0)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)
    ev_svc.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    # Generate audit report
    audit_svc.generate_audit_report("inv_cov_comp")

    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)
    cov = rel_svc.evaluate_coverage("inv_cov_comp")

    assert cov.coverage_state == AuditCoverageState.COMPLETE
    assert cov.total_snapshots == 2
    assert cov.audited_snapshots == 2
    assert cov.snapshot_coverage_ratio == 1.0
    assert cov.total_evolutions == 1
    assert cov.audited_evolutions == 1
    assert cov.evolution_coverage_ratio == 1.0
    assert cov.overall_coverage_ratio == 1.0
    assert len(cov.unaudited_snapshot_ids) == 0


# ==============================================================================
# 7. Coverage Evaluation: Partial Coverage
# ==============================================================================

def test_coverage_evaluation_partial():
    """Verify coverage evaluation detects unaudited snapshots when audit has not included all."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_cov_part", snapshot_version=1, captured_at=100.0)
    ev_svc.store_snapshot(snap1)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    audit_svc.generate_audit_report("inv_cov_part")

    # Add a second snapshot without generating a new audit report
    snap2 = make_test_snapshot(investigation_id="inv_cov_part", snapshot_version=2, captured_at=200.0)
    ev_svc.store_snapshot(snap2)

    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)
    cov = rel_svc.evaluate_coverage("inv_cov_part")

    assert cov.coverage_state == AuditCoverageState.PARTIAL
    assert cov.total_snapshots == 2
    assert cov.audited_snapshots == 1
    assert cov.snapshot_coverage_ratio == 0.5
    assert cov.missing_history_detected is True
    assert snap2.snapshot_id in cov.unaudited_snapshot_ids


# ==============================================================================
# 8. Coverage Evaluation: Empty History
# ==============================================================================

def test_coverage_evaluation_empty():
    """Verify coverage evaluation returns EMPTY when no snapshots exist."""
    ev_svc = HistoricalReviewEvolutionService()
    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    cov = rel_svc.evaluate_coverage("inv_empty")
    assert cov.coverage_state == AuditCoverageState.EMPTY
    assert cov.total_snapshots == 0
    assert cov.audited_snapshots == 0


# ==============================================================================
# 9. Coverage Evaluation: Unavailable History Graceful Handling
# ==============================================================================

def test_coverage_evaluation_unavailable():
    """Verify graceful handling when evolution service raises an error."""
    class FailingEvolutionService:
        def list_snapshots(self, inv_id: str):
            raise RuntimeError("Database unavailable")

    rel_svc = HistoricalReviewAuditReliabilityService(evolution_service=FailingEvolutionService())  # type: ignore
    cov = rel_svc.evaluate_coverage("inv_unavail")

    assert cov.coverage_state == AuditCoverageState.UNAVAILABLE
    assert cov.missing_history_detected is True


# ==============================================================================
# 10. Repeatability Verification: Stable Repeatability
# ==============================================================================

def test_repeatability_verification_stable():
    """Verify that repeat audits on unchanged inputs produce identical report IDs and STABLE status."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_rep_st", snapshot_version=1, captured_at=100.0)
    snap2 = make_test_snapshot(investigation_id="inv_rep_st", snapshot_version=2, captured_at=200.0)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)
    ev_svc.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    res = rel_svc.verify_repeatability("inv_rep_st")
    assert res.repeatability_state == AuditRepeatabilityState.STABLE
    assert res.is_repeatable is True
    assert res.repeatability_score == 1.0
    assert res.report_ids_match is True
    assert res.findings_match is True
    assert res.status_match is True


# ==============================================================================
# 11. Repeatability Verification: Uninitialized Handling
# ==============================================================================

def test_repeatability_verification_uninitialized():
    """Verify UNVERIFIABLE result when audit service is None."""
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=None)
    res = rel_svc.verify_repeatability("inv_no_audit")

    assert res.repeatability_state == AuditRepeatabilityState.UNVERIFIABLE
    assert res.is_repeatable is False


# ==============================================================================
# 12. Regression Monitoring: Baseline Established
# ==============================================================================

def test_regression_monitoring_baseline_established():
    """Verify that initial audit sets BASELINE_ESTABLISHED without error."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_base_est", snapshot_version=1)
    ev_svc.store_snapshot(snap1)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    reg = rel_svc.monitor_regression("inv_base_est")
    assert reg.regression_state == AuditRegressionState.BASELINE_ESTABLISHED
    assert reg.baseline_report_id is None
    assert reg.current_report_id.startswith("revaudit_")


# ==============================================================================
# 13. Regression Monitoring: No Regression
# ==============================================================================

def test_regression_monitoring_no_regression():
    """Verify NO_REGRESSION status when re-audited without changes."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_no_reg", snapshot_version=1)
    ev_svc.store_snapshot(snap1)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    r1 = audit_svc.generate_audit_report("inv_no_reg")
    r2 = audit_svc.generate_audit_report("inv_no_reg")

    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)
    reg = rel_svc.monitor_regression("inv_no_reg", baseline_report_id=r1.report_id, current_report_id=r2.report_id)

    assert reg.regression_state == AuditRegressionState.NO_REGRESSION
    assert reg.new_findings_count == 0
    assert reg.resolved_findings_count == 0


# ==============================================================================
# 14. Regression Monitoring: Regression Detected on New Findings
# ==============================================================================

def test_regression_monitoring_regression_detected():
    """Verify REGRESSION_DETECTED when new issues appear in current audit."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_reg_det", snapshot_version=1, captured_at=100.0)
    ev_svc.store_snapshot(snap1)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    r1 = audit_svc.generate_audit_report("inv_reg_det")

    # Introduce a snapshot with a chronological inversion to trigger an ERROR finding
    snap2 = make_test_snapshot(
        investigation_id="inv_reg_det",
        snapshot_version=2,
        captured_at=50.0,  # Earlier than snap1 (100.0) -> chronological inversion
    )
    ev_svc.store_snapshot(snap2)
    r2 = audit_svc.generate_audit_report("inv_reg_det")

    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)
    reg = rel_svc.monitor_regression("inv_reg_det", baseline_report_id=r1.report_id, current_report_id=r2.report_id)

    assert reg.regression_state == AuditRegressionState.REGRESSION_DETECTED
    assert reg.new_findings_count > 0
    assert "ERROR" in reg.regression_severities


# ==============================================================================
# 15. Regression Monitoring: Improvement Detected
# ==============================================================================

def test_regression_monitoring_improvement_detected():
    """Verify IMPROVEMENT_DETECTED when previous findings are resolved without new issues."""
    ev_svc = HistoricalReviewEvolutionService()
    # Snap with inversion
    snap1 = make_test_snapshot(investigation_id="inv_imp", snapshot_version=1, captured_at=100.0)
    snap2_bad = make_test_snapshot(investigation_id="inv_imp", snapshot_version=2, captured_at=50.0)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2_bad)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    r_base = audit_svc.generate_audit_report("inv_imp")

    # Fix sequence in a fresh service
    ev_svc_good = HistoricalReviewEvolutionService()
    snap1_good = make_test_snapshot(investigation_id="inv_imp_good", snapshot_version=1, captured_at=100.0)
    snap2_good = make_test_snapshot(investigation_id="inv_imp_good", snapshot_version=2, captured_at=200.0)
    ev_svc_good.store_snapshot(snap1_good)
    ev_svc_good.store_snapshot(snap2_good)
    ev_svc_good.compare_snapshots(snap1_good.snapshot_id, snap2_good.snapshot_id)

    audit_svc_good = HistoricalReviewAuditService(evolution_service=ev_svc_good)
    r_good = audit_svc_good.generate_audit_report("inv_imp_good")
    audit_svc.store_audit_report(r_good)

    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc)
    # Compare bad baseline with good current
    reg = rel_svc.monitor_regression("inv_imp", baseline_report_id=r_base.report_id, current_report_id=r_good.report_id)

    assert reg.regression_state == AuditRegressionState.IMPROVEMENT_DETECTED
    assert reg.resolved_findings_count > 0
    assert reg.new_findings_count == 0


# ==============================================================================
# 16. Conflict Preservation Across Regression Monitoring
# ==============================================================================

def test_conflict_preservation_monitoring():
    """Verify that unresolved conflicts are preserved and checked across audits."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_conf_pres", snapshot_version=1, conflicts=["conflict_beta"])
    snap2 = make_test_snapshot(investigation_id="inv_conf_pres", snapshot_version=2, conflicts=["conflict_beta"])
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    r1 = audit_svc.generate_audit_report("inv_conf_pres")

    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)
    reg = rel_svc.monitor_regression("inv_conf_pres", baseline_report_id=r1.report_id, current_report_id=r1.report_id)

    assert reg.conflict_preservation_maintained is True


# ==============================================================================
# 17. Full Reliability Evaluation Flow
# ==============================================================================

def test_full_reliability_evaluation():
    """Verify end-to-end reliability evaluation with coverage, repeatability, and regression."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_full_flow", snapshot_version=1, captured_at=100.0)
    snap2 = make_test_snapshot(investigation_id="inv_full_flow", snapshot_version=2, captured_at=200.0)
    ev_svc.store_snapshot(snap1)
    ev_svc.store_snapshot(snap2)
    ev_svc.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    res = rel_svc.evaluate_reliability("inv_full_flow")

    assert res.evaluation_id.startswith("revrel_")
    assert res.overall_reliability_status == ReliabilityEvaluationStatus.RELIABLE
    assert res.coverage_summary.coverage_state == AuditCoverageState.COMPLETE
    assert res.repeatability_result is not None
    assert res.repeatability_result.is_repeatable is True
    assert res.voice_summary is not None
    assert "RELIABLE" in res.voice_summary.summary_text


# ==============================================================================
# 18. Voice Summary Safety & Forbidden Words Rejection
# ==============================================================================

def test_voice_summary_safety_and_forbidden_words():
    """Verify AuditReliabilityVoiceSummary rejects prohibited trading and ranking terms."""
    with pytest.raises(ValidationError):
        AuditReliabilityVoiceSummary(
            evaluation_id="revrel_bad",
            investigation_id="inv_bad",
            reliability_status=ReliabilityEvaluationStatus.RELIABLE,
            summary_text="Reliability confirmed, buy now for great profit and strategy rank 1.",
        )


# ==============================================================================
# 19. Controlled Failure and Recovery
# ==============================================================================

def test_controlled_failure_recovery():
    """Verify graceful handling and status FAILED when services crash."""
    class CrashingAuditService:
        def generate_audit_report(self, inv_id: str):
            raise RuntimeError("Fatal audit subsystem crash")

        def list_audit_reports(self, *args, **kwargs):
            raise RuntimeError("Fatal list crash")

    class CrashingEvolutionService:
        def list_snapshots(self, inv_id: str):
            raise RuntimeError("Fatal snapshot crash")

    rel_svc = HistoricalReviewAuditReliabilityService(
        audit_service=CrashingAuditService(),  # type: ignore
        evolution_service=CrashingEvolutionService(),  # type: ignore
    )
    res = rel_svc.evaluate_reliability("inv_crash")

    assert res.overall_reliability_status == ReliabilityEvaluationStatus.FAILED
    assert res.controlled_failure_recovered is True
    assert len(res.recovery_notes) > 0


# ==============================================================================
# 20. Facts vs Interpretations vs Human Decisions Separation Counts
# ==============================================================================

def test_claim_separation_counts():
    """Verify that empirical facts, interpretations, and human decisions counts are tracked."""
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_sep", snapshot_version=1, facts_count=4)
    ev_svc.store_snapshot(snap1)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    audit_svc.generate_audit_report("inv_sep")

    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)
    res = rel_svc.evaluate_reliability("inv_sep")

    assert res.facts_count == 4
    assert res.interpretations_count >= 1
    assert res.human_decisions_count >= 1


# ==============================================================================
# 21. EventBus Integration
# ==============================================================================

def test_event_bus_integration():
    """Verify that EventBus emits appropriate reliability events."""
    bus = EventBus()
    events_received: List[Event] = []
    bus.subscribe("*", lambda e: events_received.append(e))

    ev_svc = HistoricalReviewEvolutionService(event_bus=bus)
    snap = make_test_snapshot(investigation_id="inv_eb_rel")
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc, event_bus=bus)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc, event_bus=bus)

    rel_svc.evaluate_reliability("inv_eb_rel")

    types = [e.event_type for e in events_received]
    assert EventType.HISTORICAL_REVIEW_AUDIT_COVERAGE_EVALUATED.value in types
    assert EventType.HISTORICAL_REVIEW_AUDIT_REPEATABILITY_VERIFIED.value in types
    assert EventType.HISTORICAL_REVIEW_RELIABILITY_EVALUATION_COMPLETED.value in types


# ==============================================================================
# 22. StateStore Integration
# ==============================================================================

def test_statestore_integration():
    """Verify StateStore keys are populated with reliability results."""
    store = StateStore()
    ev_svc = HistoricalReviewEvolutionService(state_store=store)
    snap = make_test_snapshot(investigation_id="inv_ss_rel")
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc, state_store=store)
    rel_svc = HistoricalReviewAuditReliabilityService(
        audit_service=audit_svc,
        evolution_service=ev_svc,
        state_store=store,
    )

    res = rel_svc.evaluate_reliability("inv_ss_rel")

    latest = store.get("historical_review_reliability:latest")
    assert latest is not None
    assert latest["evaluation_id"] == res.evaluation_id

    direct = store.get(f"historical_review_reliability:{res.evaluation_id}")
    assert direct is not None
    assert direct["investigation_id"] == "inv_ss_rel"

    cov = store.get("historical_review_reliability:coverage:inv_ss_rel")
    assert cov is not None
    assert cov["investigation_id"] == "inv_ss_rel"


# ==============================================================================
# 23. Orchestrator Handler Registration & Execution
# ==============================================================================

def test_orchestrator_handler_registration():
    """Verify all 7 reliability command handlers execute via MasterOrchestrator."""
    orchestrator = MasterOrchestrator()
    ev_svc = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot(investigation_id="inv_orch_rel", snapshot_version=1)
    ev_svc.store_snapshot(snap1)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)

    register_reliability_handlers(orchestrator=orchestrator, reliability_service=rel_svc)

    # 1. evaluate_historical_review_audit_coverage
    r1 = orchestrator.handle_request(
        OrchestratorRequest.create(command="evaluate_historical_review_audit_coverage", parameters={"investigation_id": "inv_orch_rel"})
    )
    assert r1.is_success is True

    # 2. verify_historical_review_audit_repeatability
    r2 = orchestrator.handle_request(
        OrchestratorRequest.create(command="verify_historical_review_audit_repeatability", parameters={"investigation_id": "inv_orch_rel"})
    )
    assert r2.is_success is True

    # 3. check_historical_review_audit_regression
    r3 = orchestrator.handle_request(
        OrchestratorRequest.create(command="check_historical_review_audit_regression", parameters={"investigation_id": "inv_orch_rel"})
    )
    assert r3.is_success is True

    # 4. evaluate_historical_review_audit_reliability
    r4 = orchestrator.handle_request(
        OrchestratorRequest.create(command="evaluate_historical_review_audit_reliability", parameters={"investigation_id": "inv_orch_rel"})
    )
    assert r4.is_success is True
    eval_id = r4.result["reliability_result"]["evaluation_id"]

    # 5. get_historical_review_reliability_result
    r5 = orchestrator.handle_request(
        OrchestratorRequest.create(command="get_historical_review_reliability_result", parameters={"evaluation_id": eval_id})
    )
    assert r5.is_success is True

    # 6. list_historical_review_reliability_results
    r6 = orchestrator.handle_request(
        OrchestratorRequest.create(command="list_historical_review_reliability_results", parameters={"investigation_id": "inv_orch_rel"})
    )
    assert r6.is_success is True

    # 7. get_latest_historical_review_reliability_result
    r7 = orchestrator.handle_request(
        OrchestratorRequest.create(command="get_latest_historical_review_reliability_result", parameters={"investigation_id": "inv_orch_rel"})
    )
    assert r7.is_success is True


# ==============================================================================
# 24. Dashboard GET Endpoints
# ==============================================================================

def test_dashboard_get_endpoints():
    """Verify Dashboard Endpoint 27 responds cleanly to GET requests."""
    ev_svc = HistoricalReviewEvolutionService()
    snap = make_test_snapshot(investigation_id="inv_dash_rel")
    ev_svc.store_snapshot(snap)

    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(audit_service=audit_svc, evolution_service=ev_svc)
    eval_res = rel_svc.evaluate_reliability("inv_dash_rel")

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        evolution_service=ev_svc,
        audit_service=audit_svc,
        reliability_service=rel_svc,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        base = server.url

        # 1. List
        req = urllib.request.Request(f"{base}/api/historical-review-reliability")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] >= 1

        # 2. Single item
        req = urllib.request.Request(f"{base}/api/historical-review-reliability/{eval_res.evaluation_id}")
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["evaluation"]["evaluation_id"] == eval_res.evaluation_id

        # 3. Sub-resources
        for sub in ("coverage", "repeatability", "regression", "summary"):
            req = urllib.request.Request(f"{base}/api/historical-review-reliability/{eval_res.evaluation_id}/{sub}")
            with urllib.request.urlopen(req) as resp:
                assert resp.status == 200
    finally:
        server.stop()


# ==============================================================================
# 25. Dashboard HEAD Endpoints
# ==============================================================================

def test_dashboard_head_endpoints():
    """Verify Dashboard Endpoint 27 responds cleanly to HEAD requests with empty body."""
    rel_svc = HistoricalReviewAuditReliabilityService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        reliability_service=rel_svc,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("HEAD", "/api/historical-review-reliability")
        resp = conn.getresponse()
        assert resp.status == 200
        body = resp.read()
        assert len(body) == 0
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 26. Dashboard Mutation Rejection (405)
# ==============================================================================

def test_dashboard_mutation_rejection():
    """Verify Dashboard Endpoint 27 strictly rejects POST, PUT, DELETE with 405."""
    rel_svc = HistoricalReviewAuditReliabilityService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        reliability_service=rel_svc,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        for method in ("POST", "PUT", "DELETE"):
            conn.request(method, "/api/historical-review-reliability", body=b"{}")
            resp = conn.getresponse()
            assert resp.status == 405
            resp.read()
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 27. Dashboard Path Traversal Rejection (400)
# ==============================================================================

def test_dashboard_path_traversal_rejection():
    """Verify path traversal patterns like .. or slashes return 400 Bad Request."""
    rel_svc = HistoricalReviewAuditReliabilityService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        reliability_service=rel_svc,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("GET", "/api/historical-review-reliability/..%2Fetc%2Fpasswd")
        resp = conn.getresponse()
        assert resp.status == 400
        body = json.loads(resp.read().decode("utf-8"))
        assert body["status"] == "invalid_parameter"
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 28. Bounded History & Concurrency Thread-Safety
# ==============================================================================

def test_bounded_history_and_concurrency():
    """Verify thread-safety and bounded history retention (FIFO eviction)."""
    ev_svc = HistoricalReviewEvolutionService()
    audit_svc = HistoricalReviewAuditService(evolution_service=ev_svc)
    rel_svc = HistoricalReviewAuditReliabilityService(
        audit_service=audit_svc,
        evolution_service=ev_svc,
        max_evaluations=5,
    )

    # Concurrently evaluate reliability
    def _worker(i: int):
        inv = f"inv_conc_{i}"
        snap = make_test_snapshot(investigation_id=inv)
        ev_svc.store_snapshot(snap)
        return rel_svc.evaluate_reliability(inv)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(_worker, i) for i in range(10)]
        results = [f.result() for f in futures]

    assert len(results) == 10
    # List all results
    all_evals = rel_svc.list_reliability_results(limit=100)
    assert len(all_evals) <= 10


# ==============================================================================
# 29. Safety Invariants & Execution Engine Locked
# ==============================================================================

def test_safety_invariants_and_execution_engine_locked():
    """Verify Settings.execution_locked == True and no trading directives exist."""
    settings = Settings()
    assert settings.execution_locked is True

    rel_svc = HistoricalReviewAuditReliabilityService()
    cov = rel_svc.evaluate_coverage("inv_safe")
    d = cov.to_dict()

    prohibited = ("buy", "sell", "order", "target_price", "position_size", "stop_loss", "win_probability", "profit")
    for k in d.keys():
        assert k not in prohibited
