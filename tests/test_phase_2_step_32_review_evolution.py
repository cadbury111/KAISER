"""KAISER — Phase 2, Step 32 Test Suite.

Consolidated Investigation Review Evolution & Historical Consistency Tracking Foundation.

Test Categories:
A. Snapshot model validation
B. Snapshot immutability
C. Deterministic snapshot identity
D. Snapshot lifecycle
E. Evidence evolution
F. Fact evolution
G. Interpretation evolution
H. Human-review evolution
I. Discrepancy lifecycle evolution
J. Conflict recurrence
K. Review coverage evolution
L. Quality evolution
M. Provenance evolution
N. Reproducibility evolution
O. Deterministic evolution IDs
P. EventBus integration
Q. StateStore integration
R. Handler registration
S. Dashboard GET endpoints
T. Dashboard HEAD endpoints
U. Dashboard mutation rejection (405)
V. Dashboard path traversal rejection (400)
W. Secret sanitization & prohibited fields
X. Non-finite value rejection (NaN, Infinity)
Y. Bounded history
Z. Concurrency & thread-safety
AA. Regression tests for Steps 28–31 & Safety Invariants
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import http.client
import json
import math
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest

from app.analysis.opportunities.consolidation import (
    ConsolidatedConflictItem,
    ConsolidatedEvidenceItem,
    ConsolidatedInvestigationOutcome,
    ConsolidatedReviewDecision,
    ConsolidationLimitation,
    ConsolidationLimitationItem,
    ConsolidationState,
    ConsolidationVoiceSummary,
    DecisionClaimType,
    InvestigationOutcomeConsolidationService,
    QualityAggregationSummary,
    ReproducibilityConsolidationState,
    ReproducibilitySummary,
)
from app.analysis.opportunities.consistency.models import (
    ComparisonDimensionName,
    ComparisonLifecycleState,
    ConsolidatedOutcomeComparability,
    ConsolidatedOutcomeComparison,
    CoverageStatus,
    DimensionMatchStatus,
    OutcomeComparisonDimensionRecord,
    OutcomeComparisonLimitationItem,
    OutcomeComparisonVoiceSummary,
    ReviewCoverageSummary,
)
from app.analysis.opportunities.evolution import (
    ChangeClassification,
    ConflictLifecycleState,
    DataQualityEvolutionState,
    EvolutionDimensionName,
    EvolutionDimensionRecord,
    HistoricalReviewSnapshot,
    HistoricalSnapshotLifecycleState,
    HistoricalReviewEvolutionService,
    RecurringReviewConflict,
    ReproducibilityEvolutionState,
    ReviewCoverageEvolutionState,
    ReviewEvolutionRecord,
    ReviewEvolutionVoiceSummary,
    TERMINAL_SNAPSHOT_LIFECYCLE_STATES,
    compute_deterministic_evolution_id,
    compute_deterministic_snapshot_id,
    register_evolution_handlers,
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

def make_test_snapshot(
    snapshot_id: str = "revsnap_test01",
    investigation_id: str = "inv_test_01",
    outcome_id: str = "out_test_01",
    comparison_id: Optional[str] = None,
    snapshot_version: int = 1,
    captured_at: float = 1700000000.0,
    lifecycle_state: HistoricalSnapshotLifecycleState = HistoricalSnapshotLifecycleState.STORED,
    source_state: Optional[Dict[str, Any]] = None,
    evidence_state: Optional[Dict[str, Any]] = None,
    review_state: Optional[Dict[str, Any]] = None,
    conflict_state: Optional[Dict[str, Any]] = None,
    quality_state: Optional[Dict[str, Any]] = None,
    provenance_state: Optional[Dict[str, Any]] = None,
    reproducibility_state: Optional[Dict[str, Any]] = None,
    coverage_state: Optional[Dict[str, Any]] = None,
    limitation_state: Tuple[Dict[str, Any], ...] = (),
    source_references: Tuple[str, ...] = ("report_01",),
) -> HistoricalReviewSnapshot:
    """Create a valid HistoricalReviewSnapshot for testing."""
    return HistoricalReviewSnapshot(
        snapshot_id=snapshot_id,
        investigation_id=investigation_id,
        outcome_id=outcome_id,
        comparison_id=comparison_id,
        snapshot_version=snapshot_version,
        captured_at=captured_at,
        lifecycle_state=lifecycle_state,
        source_state=source_state or {"outcome_state": "RECONCILED"},
        evidence_state=evidence_state or {"evidence_summary": {"k1": "v1"}, "facts": [], "interpretations": []},
        review_state=review_state or {"human_review_decisions": []},
        conflict_state=conflict_state or {"conflicts": [], "resolved_items": [], "unresolved_items": []},
        quality_state=quality_state or {"overall_quality_state": "VALID"},
        provenance_state=provenance_state or {"provenance_summary": {"feed": "binance"}},
        reproducibility_state=reproducibility_state or {"reproducibility_state": "REPRODUCIBLE"},
        coverage_state=coverage_state or {"coverage_status": "COMPLETE", "reviewed_evidence_count": 2},
        limitation_state=limitation_state,
        source_references=source_references,
    )


def make_test_consolidated_outcome(
    outcome_id: str = "out_01",
    investigation_id: str = "inv_01",
    outcome_state: ConsolidationState = ConsolidationState.RECONCILED,
    facts: Optional[Sequence[ConsolidatedEvidenceItem]] = None,
    interpretations: Optional[Sequence[ConsolidatedEvidenceItem]] = None,
    decisions: Optional[Sequence[ConsolidatedReviewDecision]] = None,
    conflicts: Optional[Sequence[ConsolidatedConflictItem]] = None,
    resolved_items: Sequence[str] = (),
    unresolved_items: Sequence[str] = (),
) -> ConsolidatedInvestigationOutcome:
    """Create a valid ConsolidatedInvestigationOutcome for testing."""
    fact_items = tuple(facts) if facts else ()
    interp_items = tuple(interpretations) if interpretations else ()
    decision_items = tuple(decisions) if decisions else ()
    conflict_items = tuple(conflicts) if conflicts else ()

    return ConsolidatedInvestigationOutcome(
        outcome_id=outcome_id,
        investigation_id=investigation_id,
        source_report_ids=("rep_01", "rep_02"),
        source_comparison_ids=("comp_01",),
        source_resolution_case_ids=("res_01",),
        generated_at=1700000000.0,
        outcome_state=outcome_state,
        evidence_summary={"ev_1": "empirical_tick", "ev_2": "orderbook_depth"},
        resolved_items=tuple(resolved_items),
        unresolved_items=tuple(unresolved_items),
        conflicts=conflict_items,
        facts=fact_items,
        interpretations=interp_items,
        human_review_decisions=decision_items,
        quality_summary=QualityAggregationSummary(
            overall_quality_state="VALID",
            source_quality_statuses={"rep_01": "VALID", "rep_02": "VALID"},
        ),
        provenance_summary={"source": "exchange_feed_v1"},
        reproducibility_summary=ReproducibilitySummary(
            reproducibility_state=ReproducibilityConsolidationState.REPRODUCIBLE,
            reproducible_sources=("rep_01", "rep_02"),
        ),
        limitations=(),
        voice_summary=ConsolidationVoiceSummary(
            outcome_id=outcome_id,
            outcome_state="reconciled",
            summary_text="Investigation evidence has been consolidated and reconciled.",
        ),
    )


# ==============================================================================
# A. Snapshot Model Validation
# ==============================================================================

def test_snapshot_model_validation():
    """Verify HistoricalReviewSnapshot enforces non-empty IDs, safe patterns, and version >= 1."""
    snap = make_test_snapshot()
    assert snap.snapshot_id == "revsnap_test01"
    assert snap.investigation_id == "inv_test_01"
    assert snap.outcome_id == "out_test_01"
    assert snap.snapshot_version == 1
    assert snap.lifecycle_state == HistoricalSnapshotLifecycleState.STORED
    assert "HISTORICAL REVIEW SNAPSHOT ONLY" in snap.disclaimer

    # Empty snapshot ID
    with pytest.raises(ValidationError):
        make_test_snapshot(snapshot_id="")

    # Unsafe characters in IDs
    with pytest.raises(ValidationError):
        make_test_snapshot(snapshot_id="snap/with/slash")

    with pytest.raises(ValidationError):
        make_test_snapshot(investigation_id="inv@bad")

    # Version < 1
    with pytest.raises(ValidationError):
        make_test_snapshot(snapshot_version=0)


# ==============================================================================
# B. Snapshot Immutability
# ==============================================================================

def test_snapshot_immutability():
    """Verify HistoricalReviewSnapshot cannot be mutated after creation."""
    snap = make_test_snapshot()

    # Dataclass is frozen
    with pytest.raises(dataclasses.FrozenInstanceError):
        snap.snapshot_id = "revsnap_mutated"  # type: ignore

    with pytest.raises(dataclasses.FrozenInstanceError):
        snap.snapshot_version = 2  # type: ignore

    # Collection fields are MappingProxyType / tuple
    with pytest.raises(TypeError):
        snap.source_state["outcome_state"] = "MODIFIED"  # type: ignore

    with pytest.raises(TypeError):
        snap.evidence_state["facts"] = []  # type: ignore

    # to_dict produces independent dictionary
    d = snap.to_dict()
    d["snapshot_id"] = "modified_id"
    assert snap.snapshot_id == "revsnap_test01"


# ==============================================================================
# C. Deterministic Snapshot Identity
# ==============================================================================

def test_deterministic_snapshot_identity():
    """Verify snapshot ID is strictly reproducible via SHA-256 canonical hashing."""
    id1 = compute_deterministic_snapshot_id(
        investigation_id="inv_btc_01",
        outcome_id="out_btc_01",
        comparison_id=None,
        snapshot_version=1,
        source_state_fingerprint="fp_abc123",
    )
    id2 = compute_deterministic_snapshot_id(
        investigation_id="inv_btc_01",
        outcome_id="out_btc_01",
        comparison_id=None,
        snapshot_version=1,
        source_state_fingerprint="fp_abc123",
    )
    assert id1 == id2
    assert id1.startswith("revsnap_")
    assert len(id1) == 24  # revsnap_ (8) + 16 hex chars

    # Changing any component changes the ID
    id_diff_ver = compute_deterministic_snapshot_id(
        investigation_id="inv_btc_01",
        outcome_id="out_btc_01",
        comparison_id=None,
        snapshot_version=2,
        source_state_fingerprint="fp_abc123",
    )
    assert id1 != id_diff_ver

    id_diff_fp = compute_deterministic_snapshot_id(
        investigation_id="inv_btc_01",
        outcome_id="out_btc_01",
        comparison_id=None,
        snapshot_version=1,
        source_state_fingerprint="fp_different",
    )
    assert id1 != id_diff_fp


# ==============================================================================
# D. Snapshot Lifecycle
# ==============================================================================

def test_snapshot_lifecycle():
    """Verify HistoricalSnapshotLifecycleState states and terminal classification."""
    assert HistoricalSnapshotLifecycleState.STORED.is_terminal is True
    assert HistoricalSnapshotLifecycleState.FAILED.is_terminal is True
    assert HistoricalSnapshotLifecycleState.INVALID.is_terminal is True
    assert HistoricalSnapshotLifecycleState.CAPTURED.is_terminal is False
    assert HistoricalSnapshotLifecycleState.VALIDATED.is_terminal is False

    assert len(TERMINAL_SNAPSHOT_LIFECYCLE_STATES) == 3


# ==============================================================================
# E. Evidence Evolution
# ==============================================================================

def test_evidence_evolution():
    """Verify tracking of added, removed, and modified evidence items across snapshots."""
    service = HistoricalReviewEvolutionService()

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_e1",
        snapshot_version=1,
        evidence_state={
            "evidence_summary": {"ev_a": "value_a", "ev_b": "value_b"},
            "facts": [],
            "interpretations": [],
        },
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_e2",
        snapshot_version=2,
        evidence_state={
            "evidence_summary": {"ev_b": "value_b_modified", "ev_c": "value_c"},
            "facts": [],
            "interpretations": [],
        },
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    assert "ev_c" in record.added_evidence
    assert "ev_a" in record.removed_evidence
    assert "ev_b" in record.modified_evidence
    assert record.change_state in (ChangeClassification.MODIFIED, ChangeClassification.ADDED)

    # Dimension record check
    dim = next(d for d in record.changed_dimensions if d.dimension_name == EvolutionDimensionName.EVIDENCE)
    assert dim.change_classification == ChangeClassification.MODIFIED


# ==============================================================================
# F. Fact Evolution
# ==============================================================================

def test_fact_evolution():
    """Verify distinct tracking of empirical facts without conflation with interpretations."""
    service = HistoricalReviewEvolutionService()

    f1 = {"evidence_id": "fact_01", "claim_type": "FACT", "statement": "Volume was 5000 units"}
    f2 = {"evidence_id": "fact_02", "claim_type": "FACT", "statement": "Spread was 0.05%"}

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_f1",
        evidence_state={"evidence_summary": {}, "facts": [f1], "interpretations": []},
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_f2",
        evidence_state={"evidence_summary": {}, "facts": [f1, f2], "interpretations": []},
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    dim = next(d for d in record.changed_dimensions if d.dimension_name == EvolutionDimensionName.FACTS)
    assert dim.change_classification == ChangeClassification.ADDED
    assert "fact_02" in dim.current_state["added"]
    assert len(dim.previous_state["removed"]) == 0


# ==============================================================================
# G. Interpretation Evolution
# ==============================================================================

def test_interpretation_evolution():
    """Verify tracking of non-empirical analytical interpretations."""
    service = HistoricalReviewEvolutionService()

    i1 = {"evidence_id": "interp_01", "claim_type": "INTERPRETATION", "text": "Volume accumulation hypothesis"}

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_i1",
        evidence_state={"evidence_summary": {}, "facts": [], "interpretations": [i1]},
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_i2",
        evidence_state={"evidence_summary": {}, "facts": [], "interpretations": []},
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    dim = next(d for d in record.changed_dimensions if d.dimension_name == EvolutionDimensionName.INTERPRETATIONS)
    assert dim.change_classification == ChangeClassification.REMOVED
    assert "interp_01" in dim.previous_state["removed"]


# ==============================================================================
# H. Human-Review Evolution
# ==============================================================================

def test_human_review_evolution():
    """Verify tracking of documented reviewer decisions and rationales."""
    service = HistoricalReviewEvolutionService()

    d1 = {"decision_id": "dec_01", "decision_text": "Approved reconciliation", "rationale": "Sufficient provenance"}
    d2 = {"decision_id": "dec_02", "decision_text": "Accepted spread difference", "rationale": "Different tick feeds"}

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_h1",
        review_state={"human_review_decisions": [d1]},
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_h2",
        review_state={"human_review_decisions": [d1, d2]},
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    dim = next(d for d in record.changed_dimensions if d.dimension_name == EvolutionDimensionName.HUMAN_REVIEW_DECISIONS)
    assert dim.change_classification == ChangeClassification.ADDED
    assert "dec_02" in record.changed_review_decisions


# ==============================================================================
# I. Discrepancy Lifecycle Evolution
# ==============================================================================

def test_discrepancy_lifecycle_evolution():
    """Verify transitions between unresolved, resolved, and reopened discrepancy states."""
    service = HistoricalReviewEvolutionService()

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_d1",
        conflict_state={"conflicts": [], "resolved_items": [], "unresolved_items": ["disc_01", "disc_02"]},
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_d2",
        conflict_state={"conflicts": [], "resolved_items": ["disc_01"], "unresolved_items": ["disc_02"]},
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    assert "disc_01" in record.newly_resolved_discrepancies
    assert len(record.newly_open_discrepancies) == 0
    assert record.change_state == ChangeClassification.RESOLVED


# ==============================================================================
# J. Conflict Recurrence
# ==============================================================================

def test_conflict_recurrence():
    """Verify descriptive detection of recurring conflicts across snapshots without subjective ranking."""
    bus = EventBus()
    recurrent_events = []
    bus.subscribe(EventType.REVIEW_CONFLICT_RECURRED, lambda e: recurrent_events.append(e))

    service = HistoricalReviewEvolutionService(event_bus=bus)

    conf_a = {
        "conflict_id": "conf_persistent_01",
        "discrepancy_id": "disc_spread",
        "conflicting_evidence_ids": ["ev_1", "ev_2"],
        "conflict_state": "ACTIVE",
    }

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_c1",
        investigation_id="inv_recurring",
        snapshot_version=1,
        conflict_state={"conflicts": [conf_a], "resolved_items": [], "unresolved_items": []},
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_c2",
        investigation_id="inv_recurring",
        snapshot_version=2,
        conflict_state={"conflicts": [conf_a], "resolved_items": [], "unresolved_items": []},
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    recurring = service.get_recurring_review_conflicts("inv_recurring")
    assert len(recurring) == 1
    rec = recurring[0]
    assert rec.conflict_id == "conf_persistent_01"
    assert rec.occurrence_count == 2
    assert rec.lifecycle_status == ConflictLifecycleState.ACTIVE
    assert "ev_1" in rec.evidence_lineage
    assert len(recurrent_events) >= 1


# ==============================================================================
# K. Review Coverage Evolution
# ==============================================================================

def test_review_coverage_evolution():
    """Verify categorical review coverage tracking without invented percentages."""
    bus = EventBus()
    cov_events = []
    bus.subscribe(EventType.REVIEW_COVERAGE_CHANGED, lambda e: cov_events.append(e))

    service = HistoricalReviewEvolutionService(event_bus=bus)

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_cov1",
        coverage_state={"coverage_status": "PARTIAL", "reviewed_evidence_count": 1, "unresolved_discrepancies_count": 1},
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_cov2",
        coverage_state={"coverage_status": "COMPLETE", "reviewed_evidence_count": 2, "unresolved_discrepancies_count": 0},
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    dim = next(d for d in record.changed_dimensions if d.dimension_name == EvolutionDimensionName.REVIEW_COVERAGE)
    assert dim.change_classification == ChangeClassification.REVIEW_COVERAGE_CHANGED
    assert "partial to complete" in dim.explanation
    assert len(cov_events) >= 1


# ==============================================================================
# L. Quality Evolution
# ==============================================================================

def test_quality_evolution():
    """Verify data quality transitions (improved / degraded data availability)."""
    service = HistoricalReviewEvolutionService()

    snap_stale = make_test_snapshot(
        snapshot_id="revsnap_q1",
        quality_state={"overall_quality_state": "STALE", "source_quality_statuses": {"feed_a": "STALE"}},
    )
    snap_valid = make_test_snapshot(
        snapshot_id="revsnap_q2",
        quality_state={"overall_quality_state": "VALID", "source_quality_statuses": {"feed_a": "VALID"}},
    )

    service.store_snapshot(snap_stale)
    service.store_snapshot(snap_valid)

    record = service.compare_snapshots(snap_stale.snapshot_id, snap_valid.snapshot_id)
    dim = next(d for d in record.changed_dimensions if d.dimension_name == EvolutionDimensionName.DATA_QUALITY)
    assert dim.change_classification == ChangeClassification.IMPROVED_DATA_AVAILABILITY
    assert len(dim.current_state.get("source_quality_changes", [])) >= 1


# ==============================================================================
# M. Provenance Evolution
# ==============================================================================

def test_provenance_evolution():
    """Verify tracking of newly linked or missing provenance sources."""
    bus = EventBus()
    prov_events = []
    bus.subscribe(EventType.REVIEW_PROVENANCE_CHANGED, lambda e: prov_events.append(e))

    service = HistoricalReviewEvolutionService(event_bus=bus)

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_p1",
        source_references=("rep_01",),
        provenance_state={"provenance_summary": {"source_ids": ["rep_01"]}},
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_p2",
        source_references=("rep_01", "rep_02"),
        provenance_state={"provenance_summary": {"source_ids": ["rep_01", "rep_02"]}},
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    dim = next(d for d in record.changed_dimensions if d.dimension_name == EvolutionDimensionName.PROVENANCE)
    assert dim.change_classification == ChangeClassification.PROVENANCE_CHANGED
    assert "rep_02" in dim.current_state["newly_linked_provenance"]
    assert len(prov_events) >= 1


# ==============================================================================
# N. Reproducibility Evolution
# ==============================================================================

def test_reproducibility_evolution():
    """Verify categorical reproducibility state changes backed by analysis records."""
    service = HistoricalReviewEvolutionService()

    snap1 = make_test_snapshot(
        snapshot_id="revsnap_r1",
        reproducibility_state={"reproducibility_state": "UNKNOWN", "reproducible_sources": []},
    )
    snap2 = make_test_snapshot(
        snapshot_id="revsnap_r2",
        reproducibility_state={"reproducibility_state": "REPRODUCIBLE", "reproducible_sources": ["rep_01"]},
    )

    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    dim = next(d for d in record.changed_dimensions if d.dimension_name == EvolutionDimensionName.REPRODUCIBILITY)
    assert dim.change_classification == ChangeClassification.REPRODUCIBILITY_CHANGED
    assert "rep_01" in dim.current_state["reproducible_sources"]


# ==============================================================================
# O. Deterministic Evolution IDs
# ==============================================================================

def test_deterministic_evolution_ids():
    """Verify compute_deterministic_evolution_id produces reproducible SHA-256 hashes."""
    eid1 = compute_deterministic_evolution_id("revsnap_01", "revsnap_02")
    eid2 = compute_deterministic_evolution_id("revsnap_01", "revsnap_02")
    assert eid1 == eid2
    assert eid1.startswith("revevol_")
    assert len(eid1) == 24

    eid_diff = compute_deterministic_evolution_id("revsnap_01", "revsnap_03")
    assert eid1 != eid_diff


# ==============================================================================
# P. EventBus Integration
# ==============================================================================

def test_event_bus_integration():
    """Verify published lifecycle events are properly sanitized and correlated."""
    bus = EventBus()
    events_received: List[Event] = []

    def _handler(ev: Event) -> None:
        events_received.append(ev)

    bus.subscribe(EventType.HISTORICAL_REVIEW_SNAPSHOT_CAPTURED, _handler)
    bus.subscribe(EventType.HISTORICAL_REVIEW_SNAPSHOT_STORED, _handler)
    bus.subscribe(EventType.HISTORICAL_REVIEW_SNAPSHOT_FAILED, _handler)
    bus.subscribe(EventType.REVIEW_EVOLUTION_DETECTED, _handler)

    service = HistoricalReviewEvolutionService(event_bus=bus)

    snap1 = make_test_snapshot("revsnap_bus1", snapshot_version=1)
    snap2 = make_test_snapshot("revsnap_bus2", snapshot_version=2)
    service.store_snapshot(snap1, correlation_id="corr_test_101")
    service.store_snapshot(snap2, correlation_id="corr_test_102")

    # Verify captured and stored events
    captured_events = [e for e in events_received if e.event_type == EventType.HISTORICAL_REVIEW_SNAPSHOT_CAPTURED]
    stored_events = [e for e in events_received if e.event_type == EventType.HISTORICAL_REVIEW_SNAPSHOT_STORED]
    assert len(captured_events) == 2
    assert len(stored_events) == 2
    assert captured_events[0].correlation_id == "corr_test_101"

    # Compare snapshots emits REVIEW_EVOLUTION_DETECTED
    service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id, correlation_id="corr_test_103")
    evol_events = [e for e in events_received if e.event_type == EventType.REVIEW_EVOLUTION_DETECTED]
    assert len(evol_events) == 1
    assert evol_events[0].correlation_id == "corr_test_103"

    # FAILED snapshot emission
    snap_failed = make_test_snapshot(
        "revsnap_failed",
        lifecycle_state=HistoricalSnapshotLifecycleState.FAILED,
    )
    service.store_snapshot(snap_failed)
    failed_events = [e for e in events_received if e.event_type == EventType.HISTORICAL_REVIEW_SNAPSHOT_FAILED]
    assert len(failed_events) == 1


# ==============================================================================
# Q. StateStore Integration
# ==============================================================================

def test_statestore_integration():
    """Verify snapshots and evolution records are saved in bounded StateStore namespaces."""
    state_store = StateStore(name="test_state_store")
    service = HistoricalReviewEvolutionService(state_store=state_store)

    snap1 = make_test_snapshot("revsnap_st1", snapshot_version=1)
    snap2 = make_test_snapshot("revsnap_st2", snapshot_version=2)
    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    # Check latest and individual keys
    latest_snap = state_store.get("historical_review:latest")
    assert latest_snap is not None
    assert latest_snap["snapshot_id"] == "revsnap_st2"

    snap1_stored = state_store.get("historical_review:revsnap_st1")
    assert snap1_stored is not None
    assert snap1_stored["snapshot_id"] == "revsnap_st1"

    history = state_store.get("historical_review:history")
    assert isinstance(history, list)
    assert len(history) == 2

    # Evolution record persistence
    record = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    latest_evol = state_store.get("historical_review_evolution:latest")
    assert latest_evol is not None
    assert latest_evol["evolution_id"] == record.evolution_id

    evol_history = state_store.get("historical_review_evolution:history")
    assert isinstance(evol_history, list)
    assert len(evol_history) == 1


# ==============================================================================
# R. Handler Registration
# ==============================================================================

def test_handler_registration():
    """Verify MasterOrchestrator registration and execution of all 9 command handlers."""
    bus = EventBus()
    store = StateStore(name="handler_store")
    settings = Settings()
    orchestrator = MasterOrchestrator(event_bus=bus, state_store=store, settings=settings)

    service = HistoricalReviewEvolutionService(state_store=store, event_bus=bus)
    register_evolution_handlers(orchestrator=orchestrator, evolution_service=service)

    snap1 = make_test_snapshot("revsnap_cmd1", snapshot_version=1)
    snap2 = make_test_snapshot("revsnap_cmd2", snapshot_version=2)
    service.store_snapshot(snap1)
    service.store_snapshot(snap2)

    # 1. get_historical_review_snapshot
    req_get = OrchestratorRequest.create(
        command="get_historical_review_snapshot",
        parameters={"snapshot_id": "revsnap_cmd1"},
    )
    res_get = orchestrator.handle_request(req_get)
    assert res_get.is_success is True
    assert res_get.result["snapshot"]["snapshot_id"] == "revsnap_cmd1"
    assert res_get.result["read_only"] is True

    # 2. list_historical_review_snapshots
    req_list = OrchestratorRequest.create(
        command="list_historical_review_snapshots",
        parameters={"investigation_id": "inv_test_01"},
    )
    res_list = orchestrator.handle_request(req_list)
    assert res_list.is_success is True
    assert res_list.result["count"] == 2

    # 3. compare_historical_review_snapshots
    req_comp = OrchestratorRequest.create(
        command="compare_historical_review_snapshots",
        parameters={"previous_snapshot_id": "revsnap_cmd1", "current_snapshot_id": "revsnap_cmd2"},
    )
    res_comp = orchestrator.handle_request(req_comp)
    assert res_comp.is_success is True
    evol_id = res_comp.result["evolution_id"]

    # 4. get_review_evolution
    req_gevol = OrchestratorRequest.create(
        command="get_review_evolution",
        parameters={"evolution_id": evol_id},
    )
    res_gevol = orchestrator.handle_request(req_gevol)
    assert res_gevol.is_success is True
    assert res_gevol.result["evolution"]["evolution_id"] == evol_id

    # 5. list_review_evolution_history
    req_levol = OrchestratorRequest.create(
        command="list_review_evolution_history",
        parameters={},
    )
    res_levol = orchestrator.handle_request(req_levol)
    assert res_levol.is_success is True
    assert res_levol.result["count"] >= 1

    # 6. get_recurring_review_conflicts
    req_rec = OrchestratorRequest.create(
        command="get_recurring_review_conflicts",
        parameters={"investigation_id": "inv_test_01"},
    )
    res_rec = orchestrator.handle_request(req_rec)
    assert res_rec.is_success is True

    # 7. get_review_coverage_evolution
    req_cov = OrchestratorRequest.create(
        command="get_review_coverage_evolution",
        parameters={"investigation_id": "inv_test_01"},
    )
    res_cov = orchestrator.handle_request(req_cov)
    assert res_cov.is_success is True
    assert len(res_cov.result["coverage_history"]) == 2

    # 8. get_provenance_evolution
    req_prov = OrchestratorRequest.create(
        command="get_provenance_evolution",
        parameters={"investigation_id": "inv_test_01"},
    )
    res_prov = orchestrator.handle_request(req_prov)
    assert res_prov.is_success is True
    assert len(res_prov.result["provenance_history"]) == 2

    # 9. capture_historical_review_snapshot via generic payload
    req_cap = OrchestratorRequest.create(
        command="capture_historical_review_snapshot",
        parameters={
            "investigation_id": "inv_captured_cmd",
            "outcome_id": "out_captured_cmd",
            "source_state": {"outcome_state": "RECONCILED"},
        },
    )
    res_cap = orchestrator.handle_request(req_cap)
    assert res_cap.is_success is True
    assert res_cap.result["investigation_id"] == "inv_captured_cmd"


# ==============================================================================
# S. Dashboard GET Endpoints
# ==============================================================================

def test_dashboard_get_endpoints():
    """Verify read-only GET endpoints on PortfolioDashboardServer for historical review & evolution."""
    service = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot("revsnap_dash1", investigation_id="inv_dash", snapshot_version=1)
    snap2 = make_test_snapshot("revsnap_dash2", investigation_id="inv_dash", snapshot_version=2)
    service.store_snapshot(snap1)
    service.store_snapshot(snap2)
    evol = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)

    server = PortfolioDashboardServer(
        port=0,
        evolution_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.1)

    try:
        base_url = server.url

        # 1. Listing snapshots: /api/historical-review
        with urllib.request.urlopen(f"{base_url}/api/historical-review") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["count"] >= 2

        # 2. Single snapshot: /api/historical-review/<id>
        with urllib.request.urlopen(f"{base_url}/api/historical-review/{snap1.snapshot_id}") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["snapshot"]["snapshot_id"] == snap1.snapshot_id

        # 3. Snapshot evolution: /api/historical-review/<id>/evolution
        with urllib.request.urlopen(f"{base_url}/api/historical-review/{snap1.snapshot_id}/evolution") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "evolutions" in data

        # 4. Snapshot coverage: /api/historical-review/<id>/coverage
        with urllib.request.urlopen(f"{base_url}/api/historical-review/{snap1.snapshot_id}/coverage") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "coverage_state" in data

        # 5. Snapshot conflicts: /api/historical-review/<id>/conflicts
        with urllib.request.urlopen(f"{base_url}/api/historical-review/{snap1.snapshot_id}/conflicts") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "conflict_state" in data

        # 6. Snapshot provenance: /api/historical-review/<id>/provenance
        with urllib.request.urlopen(f"{base_url}/api/historical-review/{snap1.snapshot_id}/provenance") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert "provenance_state" in data

        # 7. Listing evolutions: /api/review-evolution
        with urllib.request.urlopen(f"{base_url}/api/review-evolution") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["count"] >= 1

        # 8. Single evolution: /api/review-evolution/<id>
        with urllib.request.urlopen(f"{base_url}/api/review-evolution/{evol.evolution_id}") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["evolution"]["evolution_id"] == evol.evolution_id

        # 9. Evolution dimensions: /api/review-evolution/<id>/dimensions
        with urllib.request.urlopen(f"{base_url}/api/review-evolution/{evol.evolution_id}/dimensions") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert len(data["changed_dimensions"]) == 12

        # 10. Not found returns 404
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(f"{base_url}/api/historical-review/revsnap_nonexistent_99")
        assert exc_info.value.code == 404

    finally:
        server.stop()


# ==============================================================================
# T. Dashboard HEAD Endpoints
# ==============================================================================

def test_dashboard_head_endpoints():
    """Verify loopback read-only dashboard HEAD endpoints return headers only with zero-length body."""
    service = HistoricalReviewEvolutionService()
    snap1 = make_test_snapshot("revsnap_hd1", snapshot_version=1)
    snap2 = make_test_snapshot("revsnap_hd2", snapshot_version=2)
    service.store_snapshot(snap1)
    service.store_snapshot(snap2)
    evol = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        evolution_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)

        # Snapshot HEAD
        conn.request("HEAD", f"/api/historical-review/{snap1.snapshot_id}")
        res = conn.getresponse()
        assert res.status == 200
        body = res.read()
        assert len(body) == 0

        # Evolution HEAD
        conn.request("HEAD", f"/api/review-evolution/{evol.evolution_id}")
        res2 = conn.getresponse()
        assert res2.status == 200
        body2 = res2.read()
        assert len(body2) == 0

        conn.close()
    finally:
        server.stop()


# ==============================================================================
# U. Dashboard Mutation Rejection (405)
# ==============================================================================

def test_dashboard_mutation_rejection():
    """Verify POST, PUT, DELETE, and PATCH methods return 405 Method Not Allowed."""
    server = PortfolioDashboardServer(port=0)
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection(server.host, server.port)
        for method in ("POST", "PUT", "DELETE", "PATCH"):
            conn.request(method, "/api/historical-review", body=b"{}")
            resp = conn.getresponse()
            assert resp.status == 405
            allow_header = resp.getheader("Allow")
            assert "GET" in allow_header
            assert "HEAD" in allow_header
            resp.read()

            conn.request(method, "/api/review-evolution", body=b"{}")
            resp2 = conn.getresponse()
            assert resp2.status == 405
            resp2.read()
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# V. Dashboard Path Traversal Rejection (400)
# ==============================================================================

def test_dashboard_path_traversal_rejection():
    """Verify path traversal sequences and invalid characters return 400 Bad Request."""
    service = HistoricalReviewEvolutionService()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        evolution_service=service,
    )
    server.start(blocking=False)
    time.sleep(0.05)

    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.port)

        # Path traversal on historical-review
        conn.request("GET", "/api/historical-review/../../etc/passwd")
        res1 = conn.getresponse()
        assert res1.status == 400
        res1.read()

        # Path traversal on review-evolution
        conn.request("GET", "/api/review-evolution/../../etc/shadow")
        res2 = conn.getresponse()
        assert res2.status == 400
        res2.read()

        # Illegal identifier characters
        conn.request("GET", "/api/historical-review/invalid@snap*id")
        res3 = conn.getresponse()
        assert res3.status == 400
        res3.read()

        conn.close()
    finally:
        server.stop()


# ==============================================================================
# W. Secret Sanitization & Prohibited Fields
# ==============================================================================

def test_prohibited_fields_rejection():
    """Verify recursive rejection of trade execution directives, ranking terms, and profitability claims."""
    bad_payloads = [
        {"action": "BUY"},
        {"winner": "inv_01"},
        {"winning_investigation": "inv_01"},
        {"winning_strategy": "trend_follower"},
        {"best_investigation": "inv_01"},
        {"trade_recommendation": "buy_dip"},
        {"expected_return": 0.25},
        {"win_probability": 0.85},
        {"execution_command": "place_order"},
        {"order_type": "LIMIT"},
    ]

    for p in bad_payloads:
        with pytest.raises(ValidationError):
            validate_no_prohibited_fields(p)

    # Voice summary must not contain forbidden words
    with pytest.raises(ValidationError):
        ReviewEvolutionVoiceSummary(
            evolution_id="revevol_bad",
            previous_snapshot_id="revsnap_1",
            current_snapshot_id="revsnap_2",
            summary_text="Buy signal confirmed based on evidence.",
        )

    with pytest.raises(ValidationError):
        ReviewEvolutionVoiceSummary(
            evolution_id="revevol_bad2",
            previous_snapshot_id="revsnap_1",
            current_snapshot_id="revsnap_2",
            summary_text="Investigation A is the winner and best strategy.",
        )


# ==============================================================================
# X. Non-Finite Numerical Rejection
# ==============================================================================

def test_non_finite_rejection():
    """Verify rejection of NaN and +/- Infinity across numerical inputs."""
    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("nan"), "test_nan")

    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("inf"), "test_inf")

    with pytest.raises(ValidationError):
        validate_numerical_integrity(float("-inf"), "test_neg_inf")

    with pytest.raises(ValidationError):
        make_test_snapshot(captured_at=float("nan"))


# ==============================================================================
# Y. Bounded History Retention
# ==============================================================================

def test_bounded_history():
    """Verify snapshots and evolution records respect configured capacity limits."""
    service = HistoricalReviewEvolutionService(
        max_snapshots_per_investigation=5,
        max_evolution_per_investigation=5,
        max_global_history=10,
    )

    for i in range(10):
        snap = make_test_snapshot(
            snapshot_id=f"revsnap_bound_{i}",
            investigation_id="inv_bounded",
            snapshot_version=i + 1,
        )
        service.store_snapshot(snap)

    # Max per investigation is clamped to 5
    inv_snapshots = service.list_snapshots(investigation_id="inv_bounded", limit=100)
    assert len(inv_snapshots) == 5

    # Max global is clamped to 10
    global_snapshots = service.list_snapshots(limit=100)
    assert len(global_snapshots) <= 10


# ==============================================================================
# Z. Concurrency & Thread-Safety
# ==============================================================================

def test_concurrency_and_thread_safety():
    """Verify multi-threaded snapshot storage and queries execute cleanly without race conditions."""
    service = HistoricalReviewEvolutionService(
        max_snapshots_per_investigation=50,
        max_global_history=200,
    )

    def worker(worker_id: int) -> int:
        count = 0
        for i in range(5):
            snap = make_test_snapshot(
                snapshot_id=f"revsnap_w{worker_id}_{i}",
                investigation_id=f"inv_worker_{worker_id}",
                snapshot_version=i + 1,
            )
            service.store_snapshot(snap)
            retrieved = service.get_snapshot(snap.snapshot_id)
            if retrieved is not None:
                count += 1
        return count

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(worker, wid) for wid in range(6)]
        results = [f.result() for f in futures]

    assert sum(results) == 30  # 6 workers * 5 snapshots


# ==============================================================================
# AA. Regression Tests (Steps 28–31) & Safety Boundaries
# ==============================================================================

def test_steps_28_to_32_pipeline_regression():
    """Verify seamless analytical workflow from consolidation (Step 30/31) into evolution (Step 32)."""
    # Create consolidated outcome
    fact = ConsolidatedEvidenceItem(
        evidence_id="ev_btc_tick",
        source_id="rep_btc_1",
        claim_type=DecisionClaimType.FACT,
        review_status="REVIEWED",
    )
    decision = ConsolidatedReviewDecision(
        decision_id="dec_btc_01",
        source_resolution_case_id="res_01",
        decision_text="Consolidated tick data verified",
    )
    outcome1 = make_test_consolidated_outcome(
        outcome_id="out_flow_1",
        investigation_id="inv_flow",
        facts=[fact],
        decisions=[decision],
    )

    service = HistoricalReviewEvolutionService()
    snap1 = service.capture_snapshot_from_outcome(outcome1)
    assert snap1.snapshot_id.startswith("revsnap_")
    assert snap1.investigation_id == "inv_flow"
    assert snap1.outcome_id == "out_flow_1"

    # Capture snapshot 2 with resolved discrepancy
    outcome2 = make_test_consolidated_outcome(
        outcome_id="out_flow_2",
        investigation_id="inv_flow",
        facts=[fact],
        decisions=[decision],
        resolved_items=["disc_spread_resolved"],
    )
    snap2 = service.capture_snapshot_from_outcome(outcome2)

    evolution = service.compare_snapshots(snap1.snapshot_id, snap2.snapshot_id)
    assert evolution.evolution_id.startswith("revevol_")
    assert evolution.investigation_id == "inv_flow"
    assert "disc_spread_resolved" in evolution.newly_resolved_discrepancies


def test_safety_invariants_and_execution_engine_locked():
    """Explicitly verify Step 32 cannot execute trades, rank strategies, or unlock execution."""
    settings = Settings()
    # Execution locked invariant maintained at all times
    assert settings.execution_locked is True
    assert settings.broker_provider == "none"
    assert settings.paper_trading_only is True

    # Prohibited words check in evolution domain
    service = HistoricalReviewEvolutionService()
    snap = make_test_snapshot()
    service.store_snapshot(snap)

    # Evolution record disclaimer check
    evol = service.compare_snapshots(snap.snapshot_id, snap.snapshot_id)
    assert "REVIEW EVOLUTION RECORD ONLY" in evol.disclaimer
    assert "Does not determine superiority" in evol.disclaimer
    assert evol.voice_summary.voice_safe is True
