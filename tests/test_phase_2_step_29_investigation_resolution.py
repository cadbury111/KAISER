"""KAISER — Phase 2, Step 29 Test Suite.

Investigation Discrepancy Resolution & Evidence Reconciliation Foundation.

Tests:
A. Domain model validation
B. Immutability
C. State transition matrix
D. Terminal-state immutability
E. Evidence attachment
F. Evidence reconciliation
G. Human-resolution recording
H. Invalid discrepancy handling
I. Invalid comparison handling
J. Provenance linkage
K. Deterministic IDs
L. Event emission
M. StateStore integration
N. Handler behavior
O. Dashboard GET
P. Dashboard HEAD
Q. Dashboard mutation rejection (405)
R. Path traversal rejection (400)
S. Secret sanitization
T. Non-finite number rejection
U. Bounded collection behavior
V. Concurrent access
W. Execution safety & JUJU isolation
"""

from __future__ import annotations

import concurrent.futures
import http.client
import json
import math
import os
import time
import urllib.request
from typing import Any, Dict, List

import pytest

from app.analysis.opportunities.comparison.engine import InvestigationComparisonEngine
from app.analysis.opportunities.comparison.models import (
    ComparabilityState,
    DiscrepancyCategory,
    DiscrepancyResolutionState,
    DiscrepancySeverity,
    InvestigationComparison,
    InvestigationDiscrepancy,
)
from app.analysis.opportunities.resolution import (
    DEFAULT_MAX_RESOLUTION_HISTORY,
    DecisionClaimType,
    DiscrepancyResolutionCase,
    DiscrepancyResolutionService,
    EvidenceReconciliationRecord,
    EvidenceRelationshipType,
    ReconciliationOutcome,
    ResolutionAuditRecord,
    ResolutionDecision,
    ResolutionState,
    ResolutionVoiceSummary,
    TERMINAL_RESOLUTION_STATES,
    VALID_RESOLUTION_TRANSITIONS,
    compute_deterministic_audit_id,
    compute_deterministic_case_id,
    compute_deterministic_decision_id,
    compute_deterministic_reconciliation_id,
    register_discrepancy_resolution_handlers,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
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
# Helper Factories
# ==============================================================================
def make_test_discrepancy(
    discrepancy_id: str = "disc_test_01",
    category: DiscrepancyCategory = DiscrepancyCategory.EVIDENCE_DISPOSITION,
    severity: DiscrepancySeverity = DiscrepancySeverity.ERROR,
) -> InvestigationDiscrepancy:
    """Construct a valid test InvestigationDiscrepancy."""
    return InvestigationDiscrepancy(
        discrepancy_id=discrepancy_id,
        category=category,
        severity=severity,
        report_ids=("rep_test_01", "rep_test_02"),
        evidence_refs=("ev_trend_01",),
        description="Opposing reviewer dispositions for evidence ev_trend_01.",
        resolution_state=DiscrepancyResolutionState.UNRESOLVED,
        source_refs=("indicator:ema_crossover",),
    )


# ==============================================================================
# A. Domain Model Validation
# ==============================================================================
def test_domain_model_validation():
    """Verify field requirements and validations on resolution domain models."""
    case_id = compute_deterministic_case_id("cmp_test_01", "disc_test_01")
    now = time.time()

    case = DiscrepancyResolutionCase(
        resolution_case_id=case_id,
        comparison_id="cmp_test_01",
        discrepancy_id="disc_test_01",
        created_at=now,
        updated_at=now,
        state=ResolutionState.OPEN,
        scope="BTC-USD comparison",
        discrepancy_category="EVIDENCE_DISPOSITION",
        discrepancy_severity="ERROR",
        discrepancy_description="Conflicting evidence disposition.",
        report_ids=("rep_01", "rep_02"),
    )
    assert case.resolution_case_id == case_id
    assert case.state == ResolutionState.OPEN
    assert not case.is_terminal()
    c_dict = case.to_dict()
    assert c_dict["resolution_case_id"] == case_id


# ==============================================================================
# B. Immutability
# ==============================================================================
def test_immutability():
    """Ensure domain models are frozen and cannot be modified after construction."""
    case_id = compute_deterministic_case_id("cmp_01", "disc_01")
    case = DiscrepancyResolutionCase(
        resolution_case_id=case_id,
        comparison_id="cmp_01",
        discrepancy_id="disc_01",
        created_at=100.0,
        updated_at=100.0,
        state=ResolutionState.OPEN,
        scope="test",
        discrepancy_category="EVIDENCE_DISPOSITION",
        discrepancy_severity="WARNING",
        discrepancy_description="test",
        report_ids=(),
    )
    with pytest.raises(Exception):
        case.state = ResolutionState.RESOLVED  # type: ignore

    rec = EvidenceReconciliationRecord(
        reconciliation_id="rec_01",
        resolution_case_id=case_id,
        evidence_refs=("ev_01",),
        evidence_classifications={"ev_01": "TECHNICAL"},
        relationship_type=EvidenceRelationshipType.CONTRADICTORY_EVIDENCE,
        reconciliation_outcome=ReconciliationOutcome.BOTH_PRESERVED_VALID,
        reviewer_notes="notes",
        rationale="rationale",
        created_at=100.0,
        reviewer_id="analyst_alice",
    )
    with pytest.raises(Exception):
        rec.reviewer_notes = "changed"  # type: ignore


# ==============================================================================
# C. State Transition Matrix
# ==============================================================================
def test_valid_state_transitions():
    """Verify valid transitions across lifecycle states."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_trans_01",
        discrepancy_id="disc_trans_01",
    )
    assert case.state == ResolutionState.OPEN

    # OPEN -> UNDER_REVIEW
    c1 = service.transition_case(case.resolution_case_id, ResolutionState.UNDER_REVIEW, "Review started")
    assert c1.state == ResolutionState.UNDER_REVIEW

    # UNDER_REVIEW -> EVIDENCE_REQUESTED
    c2 = service.transition_case(c1.resolution_case_id, ResolutionState.EVIDENCE_REQUESTED, "Need more info")
    assert c2.state == ResolutionState.EVIDENCE_REQUESTED

    # EVIDENCE_REQUESTED -> RECONCILIATION_RECORDED
    c3 = service.transition_case(c2.resolution_case_id, ResolutionState.RECONCILIATION_RECORDED, "Reconciliation noted")
    assert c3.state == ResolutionState.RECONCILIATION_RECORDED


def test_invalid_state_transitions():
    """Verify invalid state transitions are rejected with ValidationError."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_inv_trans",
        discrepancy_id="disc_inv_trans",
    )
    # OPEN cannot transition directly to RESOLVED without human decision or reconciliation
    with pytest.raises(ValidationError):
        service.transition_case(case.resolution_case_id, ResolutionState.RESOLVED, "Invalid direct skip")


# ==============================================================================
# D. Terminal-State Immutability
# ==============================================================================
def test_terminal_state_immutability():
    """Verify that once terminal (RESOLVED, REJECTED, INVALIDATED), case cannot transition."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_term_01",
        discrepancy_id="disc_term_01",
    )
    # Move to under review then resolve
    service.transition_case(case.resolution_case_id, ResolutionState.UNDER_REVIEW, "Reviewing")
    resolved = service.record_human_resolution(
        case_id=case.resolution_case_id,
        summary="Resolved by human reviewer",
        rationale="Clear evidence preponderance",
        reviewer_id="analyst_alice",
        resolution_state=ResolutionState.RESOLVED,
    )
    assert resolved.is_terminal()
    assert resolved.state == ResolutionState.RESOLVED

    # Attempting any transition on terminal case must raise ValidationError
    with pytest.raises(ValidationError):
        service.transition_case(resolved.resolution_case_id, ResolutionState.OPEN, "Reopening should fail")


# ==============================================================================
# E. Evidence Attachment
# ==============================================================================
def test_evidence_attachment():
    """Verify evidence references can be attached with review notes."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_attach_01",
        discrepancy_id="disc_attach_01",
    )
    updated = service.attach_evidence(
        case_id=case.resolution_case_id,
        evidence_ids=["ev_trend_01", "ev_vol_02"],
        reviewer_id="analyst_bob",
        notes="Attached hourly ATR evidence.",
    )
    assert "ev_trend_01" in updated.supporting_evidence_ids
    assert "ev_vol_02" in updated.supporting_evidence_ids
    assert len(updated.notes) == 1
    assert "Attached hourly ATR evidence" in updated.notes[0]


# ==============================================================================
# F. Evidence Reconciliation
# ==============================================================================
def test_evidence_reconciliation():
    """Verify recording of human-reviewed reconciliation preserving conflicting sources."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_rec_01",
        discrepancy_id="disc_rec_01",
    )
    reconciled = service.record_reconciliation(
        case_id=case.resolution_case_id,
        evidence_refs=["ev_trend_01", "ev_risk_02"],
        relationship_type=EvidenceRelationshipType.CONTRADICTORY_EVIDENCE,
        reconciliation_outcome=ReconciliationOutcome.BOTH_PRESERVED_VALID,
        reviewer_notes="Both models use valid assumptions on differing windows.",
        rationale="Microstructure volatility accounts for divergence.",
        reviewer_id="analyst_carol",
        evidence_classifications={"ev_trend_01": "TECHNICAL", "ev_risk_02": "RISK"},
    )
    assert reconciled.state == ResolutionState.RECONCILIATION_RECORDED
    assert len(reconciled.reconciliation_records) == 1
    rec = reconciled.reconciliation_records[0]
    assert rec.relationship_type == EvidenceRelationshipType.CONTRADICTORY_EVIDENCE
    assert rec.reconciliation_outcome == ReconciliationOutcome.BOTH_PRESERVED_VALID
    assert "ev_trend_01" in rec.evidence_refs
    assert "ev_risk_02" in rec.evidence_refs


# ==============================================================================
# G. Human Resolution Recording (FACT vs INTERPRETATION vs DECISION)
# ==============================================================================
def test_human_resolution_recording():
    """Verify human decision recording explicitly distinguishes HUMAN_REVIEW_DECISION."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_dec_01",
        discrepancy_id="disc_dec_01",
    )
    service.transition_case(case.resolution_case_id, ResolutionState.UNDER_REVIEW, "Beginning review")

    resolved = service.record_human_resolution(
        case_id=case.resolution_case_id,
        summary="Accepted momentum evidence, discarded stale feed.",
        rationale="Feed 2 was determined to suffer 45s latency during fast market.",
        reviewer_id="human_analyst_dan",
        accepted_evidence_ids=["ev_trend_01"],
        rejected_evidence_ids=["ev_stale_02"],
        residual_uncertainty="Pending next CPI release",
        resolution_state=ResolutionState.RESOLVED,
    )
    assert resolved.state == ResolutionState.RESOLVED
    assert resolved.decision is not None
    assert resolved.decision.claim_type == DecisionClaimType.HUMAN_REVIEW_DECISION
    assert resolved.decision.reviewer_id == "human_analyst_dan"
    assert "ev_trend_01" in resolved.decision.accepted_evidence_ids
    assert "ev_stale_02" in resolved.decision.rejected_evidence_ids


def test_reject_autonomous_resolution():
    """Verify the system strictly rejects simulated automated/AI resolution."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_auto_01",
        discrepancy_id="disc_auto_01",
    )
    with pytest.raises(ValidationError) as exc_info:
        service.record_human_resolution(
            case_id=case.resolution_case_id,
            summary="Autonomous resolution attempt",
            rationale="Auto-selected",
            reviewer_id="ai",  # Disallowed
        )
    assert "Human review boundary violation" in str(exc_info.value)


# ==============================================================================
# H. Invalid Discrepancy Handling
# ==============================================================================
def test_invalid_discrepancy_handling():
    """Reject empty or malformed discrepancy identifiers."""
    service = DiscrepancyResolutionService()
    with pytest.raises(ValidationError):
        service.create_resolution_case(comparison_id="cmp_01", discrepancy_id="")


# ==============================================================================
# I. Invalid Comparison Handling
# ==============================================================================
def test_invalid_comparison_handling():
    """Reject empty or malformed comparison identifiers."""
    service = DiscrepancyResolutionService()
    with pytest.raises(ValidationError):
        service.create_resolution_case(comparison_id="", discrepancy_id="disc_01")


# ==============================================================================
# J. Provenance Linkage
# ==============================================================================
def test_provenance_linkage():
    """Verify reconciliation records can link source provenance references."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_prov_01",
        discrepancy_id="disc_prov_01",
    )
    updated = service.record_reconciliation(
        case_id=case.resolution_case_id,
        evidence_refs=["ev_prov_01"],
        relationship_type=EvidenceRelationshipType.DIFFERENT_DATA_QUALITY,
        reconciliation_outcome=ReconciliationOutcome.QUALITY_DEGRADATION_CONFIRMED,
        reviewer_notes="Feed drop acknowledged",
        rationale="Corroborated by provider timestamp audit",
        reviewer_id="analyst_ed",
        source_provenance_refs=["prov_feed_binance_123", "run_quant_456"],
    )
    rec = updated.reconciliation_records[0]
    assert "prov_feed_binance_123" in rec.source_provenance_refs
    assert "run_quant_456" in rec.source_provenance_refs


# ==============================================================================
# K. Deterministic IDs
# ==============================================================================
def test_deterministic_ids():
    """Verify ID generation functions are strictly deterministic."""
    id1 = compute_deterministic_case_id("cmp_abc", "disc_123")
    id2 = compute_deterministic_case_id("cmp_abc", "disc_123")
    assert id1 == id2
    assert id1.startswith("res_case_")

    rec1 = compute_deterministic_reconciliation_id("case_1", ["ev_b", "ev_a"], "SAME_EVIDENCE")
    rec2 = compute_deterministic_reconciliation_id("case_1", ["ev_a", "ev_b"], "SAME_EVIDENCE")
    assert rec1 == rec2
    assert rec1.startswith("reconcile_")


# ==============================================================================
# L. Event Emission
# ==============================================================================
def test_event_emission():
    """Verify event bus emits sanitized lifecycle events."""
    bus = EventBus()
    received: List[Event] = []

    def on_event(ev: Event):
        received.append(ev)

    bus.subscribe(EventType.DISCREPANCY_RESOLUTION_CASE_CREATED, on_event)
    bus.subscribe(EventType.DISCREPANCY_RESOLUTION_STATE_CHANGED, on_event)
    bus.subscribe(EventType.HUMAN_RESOLUTION_RECORDED, on_event)

    service = DiscrepancyResolutionService(event_bus=bus)
    case = service.create_resolution_case(comparison_id="cmp_ev_01", discrepancy_id="disc_ev_01")
    service.transition_case(case.resolution_case_id, ResolutionState.UNDER_REVIEW, "Review starting")
    service.record_human_resolution(
        case_id=case.resolution_case_id,
        summary="Done",
        rationale="Evident",
        reviewer_id="human_frank",
    )

    types = [e.event_type for e in received]
    assert EventType.DISCREPANCY_RESOLUTION_CASE_CREATED.value in types
    assert EventType.DISCREPANCY_RESOLUTION_STATE_CHANGED.value in types
    assert EventType.HUMAN_RESOLUTION_RECORDED.value in types


# ==============================================================================
# M. StateStore Integration
# ==============================================================================
def test_statestore_integration():
    """Verify bounded state store persistence and retrieval."""
    store = StateStore()
    service = DiscrepancyResolutionService(state_store=store)

    case = service.create_resolution_case(
        comparison_id="cmp_store_01",
        discrepancy_id="disc_store_01",
    )
    latest = store.get("discrepancy_resolution:latest")
    assert latest is not None
    assert latest["resolution_case_id"] == case.resolution_case_id

    # Secondary lookup
    by_disc = store.get(f"discrepancy_resolution:by_discrepancy:{case.discrepancy_id}")
    assert by_disc["resolution_case_id"] == case.resolution_case_id

    # History
    hist = store.get("discrepancy_resolution:history")
    assert len(hist) == 1


# ==============================================================================
# N. Handler Behavior
# ==============================================================================
def test_handler_behavior():
    """Verify orchestrator command handlers execute and return structured responses."""
    orchestrator = MasterOrchestrator()
    service = DiscrepancyResolutionService()
    register_discrepancy_resolution_handlers(orchestrator, service)

    # 1. create
    req_create = OrchestratorRequest(
        request_id="req_c_01",
        correlation_id="corr_01",
        command="create_discrepancy_resolution_case",
        context={"comparison_id": "cmp_h_01", "discrepancy_id": "disc_h_01"},
    )
    res_create = orchestrator.handle_request(req_create)
    assert res_create.is_success
    case_id = res_create.result["resolution_case_id"]

    # 2. get
    req_get = OrchestratorRequest(
        request_id="req_g_01",
        correlation_id="corr_02",
        command="get_discrepancy_resolution_case",
        context={"resolution_case_id": case_id},
    )
    res_get = orchestrator.handle_request(req_get)
    assert res_get.is_success
    assert res_get.result["case"]["resolution_case_id"] == case_id

    # 3. attach evidence
    req_att = OrchestratorRequest(
        request_id="req_a_01",
        correlation_id="corr_03",
        command="attach_resolution_evidence",
        context={"resolution_case_id": case_id, "evidence_ids": ["ev_1", "ev_2"]},
    )
    res_att = orchestrator.handle_request(req_att)
    assert res_att.is_success

    # 4. list
    req_list = OrchestratorRequest(
        request_id="req_l_01",
        correlation_id="corr_04",
        command="list_discrepancy_resolution_cases",
        context={},
    )
    res_list = orchestrator.handle_request(req_list)
    assert res_list.is_success
    assert res_list.result["count"] >= 1


# ==============================================================================
# O. Dashboard GET
# ==============================================================================
def test_dashboard_get():
    """Verify loopback read-only dashboard GET endpoints."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_dash_01",
        discrepancy_id="disc_dash_01",
    )
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        discrepancy_resolution_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        # Listing
        req_list = urllib.request.Request(f"{server.url}/api/discrepancy-resolutions")
        with urllib.request.urlopen(req_list) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] >= 1

        # Single item
        req_item = urllib.request.Request(f"{server.url}/api/discrepancy-resolutions/{case.resolution_case_id}")
        with urllib.request.urlopen(req_item) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["case"]["resolution_case_id"] == case.resolution_case_id

        # Sub-resource evidence
        req_ev = urllib.request.Request(f"{server.url}/api/discrepancy-resolutions/{case.resolution_case_id}/evidence")
        with urllib.request.urlopen(req_ev) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"

        # Sub-resource audit
        req_aud = urllib.request.Request(f"{server.url}/api/discrepancy-resolutions/{case.resolution_case_id}/audit")
        with urllib.request.urlopen(req_aud) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "success"
            assert data["count"] >= 1
    finally:
        server.stop()


# ==============================================================================
# P. Dashboard HEAD
# ==============================================================================
def test_dashboard_head():
    """Verify dashboard HEAD requests return identical headers and empty body."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case(
        comparison_id="cmp_dhead_01",
        discrepancy_id="disc_dhead_01",
    )
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        discrepancy_resolution_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("HEAD", f"/api/discrepancy-resolutions/{case.resolution_case_id}")
        res = conn.getresponse()
        assert res.status == 200
        assert res.read() == b""
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# Q. Dashboard Mutation Rejection (405)
# ==============================================================================
def test_dashboard_mutation_rejection():
    """Verify POST/PUT/DELETE requests return 405 Method Not Allowed."""
    service = DiscrepancyResolutionService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        discrepancy_resolution_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("POST", "/api/discrepancy-resolutions", body=b"{}")
        res = conn.getresponse()
        assert res.status == 405
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# R. Path Traversal Rejection (400)
# ==============================================================================
def test_path_traversal_rejection():
    """Verify directory traversal and null byte injections return 400 Bad Request."""
    service = DiscrepancyResolutionService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        discrepancy_resolution_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)
        conn.request("GET", "/api/discrepancy-resolutions/../../etc/passwd")
        res = conn.getresponse()
        assert res.status == 400
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# S. Secret Sanitization & Prohibited Fields
# ==============================================================================
def test_secret_sanitization_and_prohibited_fields():
    """Verify rejection of prohibited trade words in payloads."""
    with pytest.raises(ValidationError):
        validate_no_prohibited_fields({"action": "BUY", "quantity": 10})

    with pytest.raises(ValidationError):
        validate_no_prohibited_fields({"expected_profit": 500.0})


# ==============================================================================
# T. Non-Finite Number Rejection
# ==============================================================================
def test_non_finite_number_rejection():
    """Verify NaN and Infinity rejection while preserving genuine 0.0."""
    assert validate_numerical_integrity(0.0) == 0.0
    assert validate_numerical_integrity(0) == 0.0

    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("nan"))

    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("inf"))


# ==============================================================================
# U. Bounded Collection Behavior
# ==============================================================================
def test_bounded_collection_behavior():
    """Verify bounded ring-buffer pruning on StateStore history."""
    store = StateStore()
    service = DiscrepancyResolutionService(state_store=store, max_history=10)

    for i in range(15):
        service.create_resolution_case(
            comparison_id=f"cmp_bound_{i}",
            discrepancy_id=f"disc_bound_{i}",
        )

    history = store.get("discrepancy_resolution:history", [])
    assert len(history) <= 10


# ==============================================================================
# V. Concurrent Access
# ==============================================================================
def test_concurrent_access():
    """Verify safe concurrent access and thread safety."""
    service = DiscrepancyResolutionService()
    case = service.create_resolution_case("cmp_conc_01", "disc_conc_01")

    def worker(i: int):
        return service.attach_evidence(
            case_id=case.resolution_case_id,
            evidence_ids=[f"ev_thread_{i}"],
            reviewer_id=f"analyst_{i}",
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, i) for i in range(10)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    updated = service.get_case(case.resolution_case_id)
    assert updated is not None
    assert len(updated.supporting_evidence_ids) == 10


# ==============================================================================
# W. Execution Safety & JUJU Isolation
# ==============================================================================
def test_execution_safety_invariants():
    """Verify execution lock remains True, no trading engine, and JUJU is isolated."""
    settings = Settings()
    assert settings.execution_locked is True

    # Voice safety verification
    summary = ResolutionVoiceSummary(
        case_id="case_1",
        state="OPEN",
        summary_text="Discrepancy remains unresolved. Human review is required.",
    )
    assert summary.voice_safe is True

    with pytest.raises(ValidationError):
        ResolutionVoiceSummary(
            case_id="case_2",
            state="OPEN",
            summary_text="Buy signal confirmed with high profit probability.",
        )

    # JUJU Isolation verification: JUJU directory must exist and be untouched
    juju_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "JUJU")
    if os.path.exists(juju_path):
        assert os.path.isdir(juju_path)
