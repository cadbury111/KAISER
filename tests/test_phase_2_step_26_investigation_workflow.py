"""KAISER — Phase 2 Step 26: Human Investigation Session & Evidence Review Workflow Tests.

Covers all 39 required verification areas:
1. session creation
2. valid lifecycle transitions
3. invalid lifecycle transitions
4. pause/resume
5. evidence review
6. invalid evidence reference
7. observation creation
8. fact vs interpretation separation
9. uncertainty recording
10. limitation recording
11. conclusion validation
12. insufficient evidence conclusion
13. conflicting evidence conclusion
14. session completion
15. invalidation
16. cancellation
17. completed session immutability
18. follow-up session lineage
19. reviewer ID validation
20. note length validation
21. prohibited field rejection
22. NaN rejection
23. Infinity rejection
24. zero preservation
25. EventBus events
26. StateStore persistence
27. audit trail
28. queue integration
29. concurrent session mutation
30. deterministic behavior
31. handler registration
32. dashboard GET
33. dashboard HEAD
34. dashboard 405
35. traversal protection
36. execution lock
37. no broker initialization
38. no autonomous execution
39. JUJU isolation
"""

import copy
import http.client
import math
import os
import threading
import time
from typing import Any, Dict, List, Sequence
import urllib.request

import pytest

from app.analysis.opportunities.investigation import (
    DEFAULT_MAX_SESSION_HISTORY,
    MAX_NOTE_LENGTH,
    MAX_STATEMENT_LENGTH,
    AnalyticalConclusion,
    ConclusionState,
    EvidenceReviewDisposition,
    EvidenceReviewRecord,
    InvestigationAuditRecord,
    InvestigationLimitation,
    InvestigationObservation,
    InvestigationReviewNote,
    InvestigationSession,
    InvestigationSessionManager,
    InvestigationUncertainty,
    InvestigationVoiceSummary,
    ObservationType,
    SessionState,
    SourceType,
    VALID_SESSION_TRANSITIONS,
    register_investigation_handlers,
    sanitize_reviewer_id,
)
from app.analysis.opportunities.models import (
    ConfidenceState,
    OpportunityCandidate,
    OpportunityState,
)
from app.analysis.opportunities.prioritization.models import (
    OpportunityPriorityRecord,
    PriorityState,
)
from app.analysis.opportunities.prioritization.queue import (
    OpportunityInvestigationQueue,
)
from app.analysis.opportunities.prioritization.engine import (
    OpportunityPrioritizationEngine,
)
from app.core.config import Settings
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer


from app.analysis.evidence.models import EvidenceStatus


def make_test_candidate(
    candidate_id: str = "cand_test_01",
    supporting_eids: Sequence[str] = ("ev_trend_01", "ev_mom_02"),
    conflicting_eids: Sequence[str] = ("ev_vol_03",),
) -> OpportunityCandidate:
    """Helper to construct a valid OpportunityCandidate."""
    return OpportunityCandidate(
        candidate_id=candidate_id,
        symbol="BTC-USD",
        timeframe="1h",
        assessment_state=OpportunityState.ANALYTICALLY_SUPPORTED,
        evidence_status=EvidenceStatus.AVAILABLE,
        evidence_completeness_score=0.85,
        confidence_state=ConfidenceState.MODERATE,
        source_evidence_report_id="rep_synth_01",
        generated_at=1000.0,
        supporting_evidence_refs=tuple(supporting_eids),
        conflicting_evidence_refs=tuple(conflicting_eids),
        uncertainty_refs=("uncertainty_01",),
        limitation_refs=("limitation_01",),
        invalidation_conditions=("stale_feed",),
        source_analysis_run_ids=("run_01", "run_02"),
        reproducibility_id="repro_abc_123",
    )


def make_test_priority(candidate: OpportunityCandidate) -> OpportunityPriorityRecord:
    """Helper to construct a valid OpportunityPriorityRecord."""
    engine = OpportunityPrioritizationEngine()
    return engine.prioritize_candidate(candidate)


# ==============================================================================
# 1. Session Creation
# ==============================================================================
def test_session_creation():
    bus = EventBus()
    store = StateStore()
    mgr = InvestigationSessionManager(event_bus=bus, state_store=store)

    cand = make_test_candidate("cand_sess_create")
    prio = make_test_priority(cand)

    sess = mgr.create_session(
        candidate=cand,
        priority_record=prio,
        reviewer_id="analyst_alice",
        metadata={"project": "alpha"},
    )

    assert sess.session_id.startswith("inv_sess_cand_sess_create_")
    assert sess.candidate_id == "cand_sess_create"
    assert sess.priority_id == prio.priority_id
    assert sess.status == SessionState.CREATED
    assert sess.reviewer_id == "analyst_alice"
    assert "ev_trend_01" in sess.known_evidence_ids
    assert "ev_vol_03" in sess.known_evidence_ids
    assert sess.revision == 1
    assert not sess.is_terminal()


# ==============================================================================
# 2. Valid Lifecycle Transitions
# ==============================================================================
def test_valid_lifecycle_transitions():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_lifecycle")
    sess = mgr.create_session(cand)
    assert sess.status == SessionState.CREATED

    # CREATED -> ACTIVE
    sess = mgr.start_session(sess.session_id, reviewer_id="analyst_bob")
    assert sess.status == SessionState.ACTIVE
    assert sess.started_at is not None

    # ACTIVE -> PAUSED
    sess = mgr.pause_session(sess.session_id, reason="Lunch break")
    assert sess.status == SessionState.PAUSED

    # PAUSED -> ACTIVE
    sess = mgr.resume_session(sess.session_id)
    assert sess.status == SessionState.ACTIVE

    # ACTIVE -> REQUIRES_MORE_EVIDENCE
    sess = mgr.request_more_evidence(sess.session_id, reason="Missing volume depth")
    assert sess.status == SessionState.REQUIRES_MORE_EVIDENCE

    # REQUIRES_MORE_EVIDENCE -> ACTIVE
    sess = mgr.resume_session(sess.session_id)
    assert sess.status == SessionState.ACTIVE


# ==============================================================================
# 3. Invalid Lifecycle Transitions
# ==============================================================================
def test_invalid_lifecycle_transitions():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_invalid_trans")
    sess = mgr.create_session(cand)

    # Cannot pause a CREATED session directly (must be ACTIVE)
    with pytest.raises(ValidationError, match="Cannot pause session"):
        mgr.pause_session(sess.session_id)

    # Cannot resume a CREATED session directly
    with pytest.raises(ValidationError, match="Cannot resume session"):
        mgr.resume_session(sess.session_id)

    # Start to ACTIVE
    sess = mgr.start_session(sess.session_id)

    # Complete it
    sess = mgr.complete_session(
        sess.session_id,
        conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
        rationale="Evidence validated across 2 independent runs.",
    )
    assert sess.status == SessionState.REVIEW_COMPLETE

    # Cannot transition from REVIEW_COMPLETE (terminal)
    with pytest.raises(ValidationError, match="cannot be started"):
        mgr.start_session(sess.session_id)

    with pytest.raises(ValidationError, match="Cannot pause session"):
        mgr.pause_session(sess.session_id)


# ==============================================================================
# 4. Pause / Resume
# ==============================================================================
def test_pause_and_resume():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_pause_resume")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    paused = mgr.pause_session(sess.session_id, reason="Need sleep")
    assert paused.status == SessionState.PAUSED

    resumed = mgr.resume_session(sess.session_id)
    assert resumed.status == SessionState.ACTIVE


# ==============================================================================
# 5. Evidence Review
# ==============================================================================
def test_evidence_review():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_ev_rev")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    rec1 = mgr.review_evidence(
        session_id=sess.session_id,
        evidence_id="ev_trend_01",
        disposition=EvidenceReviewDisposition.SUPPORTING,
        notes="SMA cross verified against clean historical bars",
        reviewer_id="analyst_carol",
    )
    assert rec1.evidence_id == "ev_trend_01"
    assert rec1.disposition == EvidenceReviewDisposition.SUPPORTING
    assert rec1.reviewer_id == "analyst_carol"

    rec2 = mgr.review_evidence(
        session_id=sess.session_id,
        evidence_id="ev_vol_03",
        disposition=EvidenceReviewDisposition.CONFLICTING,
        notes="ATR spike exceeds normal volatility bounds",
    )
    assert rec2.evidence_id == "ev_vol_03"
    assert rec2.disposition == EvidenceReviewDisposition.CONFLICTING

    updated = mgr.get_session(sess.session_id)
    assert updated is not None
    assert len(updated.evidence_reviews) == 2


# ==============================================================================
# 6. Invalid Evidence Reference
# ==============================================================================
def test_invalid_evidence_reference():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_invalid_ev")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    # Fabricated / unknown evidence ID
    with pytest.raises(ValidationError, match="not an established evidence reference"):
        mgr.review_evidence(
            session_id=sess.session_id,
            evidence_id="fabricated_fake_evidence_999",
            disposition=EvidenceReviewDisposition.SUPPORTING,
        )


# ==============================================================================
# 7. Observation Creation
# ==============================================================================
def test_observation_creation():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_obs_create")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    obs = mgr.add_observation(
        session_id=sess.session_id,
        observation_type=ObservationType.INTERPRETATION,
        statement="Market regime appears to be consolidating above the 200 EMA.",
        reviewer_id="analyst_dan",
    )
    assert obs.observation_type == ObservationType.INTERPRETATION
    assert obs.reviewer_id == "analyst_dan"

    updated = mgr.get_session(sess.session_id)
    assert updated is not None
    assert len(updated.observations) == 1


# ==============================================================================
# 8. Fact vs Interpretation Separation
# ==============================================================================
def test_fact_vs_interpretation_separation():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_fact_vs_interp")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    # 1. FACT without evidence references MUST fail
    with pytest.raises(ValidationError, match="Epistemological rule"):
        mgr.add_observation(
            session_id=sess.session_id,
            observation_type=ObservationType.FACT,
            statement="Volume dropped 40% on the last 3 bars.",
            evidence_refs=[],
        )

    # 2. FACT with verified evidence reference MUST succeed
    fact_obs = mgr.add_observation(
        session_id=sess.session_id,
        observation_type=ObservationType.FACT,
        statement="Confirmed that ev_trend_01 exhibits positive divergence.",
        evidence_refs=["ev_trend_01"],
    )
    assert fact_obs.observation_type == ObservationType.FACT
    assert fact_obs.evidence_refs == ("ev_trend_01",)

    # 3. INTERPRETATION without evidence references IS allowed
    interp_obs = mgr.add_observation(
        session_id=sess.session_id,
        observation_type=ObservationType.INTERPRETATION,
        statement="Analyst feels this pattern could be a bull flag.",
    )
    assert interp_obs.observation_type == ObservationType.INTERPRETATION


# ==============================================================================
# 9. Uncertainty Recording
# ==============================================================================
def test_uncertainty_recording():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_uncertainty")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    unc = mgr.add_uncertainty(
        session_id=sess.session_id,
        description="Upcoming macroeconomic data release may induce volatility regime change.",
        severity="HIGH",
        evidence_refs=["ev_vol_03"],
        reviewer_id="analyst_elena",
    )
    assert unc.severity == "HIGH"
    assert unc.reviewer_id == "analyst_elena"

    updated = mgr.get_session(sess.session_id)
    assert updated is not None
    assert len(updated.uncertainties) == 1


# ==============================================================================
# 10. Limitation Recording
# ==============================================================================
def test_limitation_recording():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_limitation")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    lim = mgr.add_limitation(
        session_id=sess.session_id,
        description="Dataset only covers 30 days of 1-minute bars; out-of-sample stress test unverified.",
        category="SAMPLE_COVERAGE",
    )
    assert lim.category == "SAMPLE_COVERAGE"

    updated = mgr.get_session(sess.session_id)
    assert updated is not None
    assert len(updated.limitations) == 1


# ==============================================================================
# 11. Conclusion Validation
# ==============================================================================
def test_conclusion_validation():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_conclusion_val")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    # Rationale cannot be empty
    with pytest.raises(ValidationError, match="rationale must be a non-empty string"):
        mgr.set_conclusion(
            session_id=sess.session_id,
            conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
            rationale="",
        )

    # Prohibited words in rationale (e.g. 'buy', 'profitable')
    with pytest.raises(ValidationError, match="Forbidden trading or execution directive"):
        mgr.set_conclusion(
            session_id=sess.session_id,
            conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
            rationale="This looks like a profitable opportunity to buy immediately.",
        )


# ==============================================================================
# 12. Insufficient Evidence Conclusion
# ==============================================================================
def test_insufficient_evidence_conclusion():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_insufficient_ev")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    conc = mgr.set_conclusion(
        session_id=sess.session_id,
        conclusion_state=ConclusionState.INSUFFICIENT_EVIDENCE,
        rationale="Volume feed gaps render technical evidence inconclusive.",
    )
    assert conc.conclusion_state == ConclusionState.INSUFFICIENT_EVIDENCE


# ==============================================================================
# 13. Conflicting Evidence Conclusion
# ==============================================================================
def test_conflicting_evidence_conclusion():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_conflicting_ev")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    conc = mgr.set_conclusion(
        session_id=sess.session_id,
        conclusion_state=ConclusionState.EVIDENCE_REMAINS_CONFLICTING,
        rationale="Momentum indicator indicates trend continuation while volatility indicator shows regime exhaustion.",
    )
    assert conc.conclusion_state == ConclusionState.EVIDENCE_REMAINS_CONFLICTING


# ==============================================================================
# 14. Session Completion
# ==============================================================================
def test_session_completion():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_complete")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    completed = mgr.complete_session(
        session_id=sess.session_id,
        conclusion_state=ConclusionState.REVIEW_COMPLETED_WITH_LIMITATIONS,
        rationale="Evidence validated within observation window; historical sample limited.",
        reviewer_id="analyst_frank",
    )
    assert completed.status == SessionState.REVIEW_COMPLETE
    assert completed.is_terminal()
    assert completed.closed_at is not None
    assert completed.voice_summary is not None
    assert "review complete" in completed.voice_summary.status_speech


# ==============================================================================
# 15. Invalidation
# ==============================================================================
def test_session_invalidation():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_invalidate")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    inv = mgr.invalidate_session(
        session_id=sess.session_id,
        reason="Upstream market data quality monitor marked data source degraded.",
    )
    assert inv.status == SessionState.INVALIDATED
    assert inv.is_terminal()


# ==============================================================================
# 16. Cancellation
# ==============================================================================
def test_session_cancellation():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_cancel")
    sess = mgr.create_session(cand)

    # Cancel directly from CREATED
    canc = mgr.cancel_session(sess.session_id, reason="Candidate superseded by new report")
    assert canc.status == SessionState.CANCELLED
    assert canc.is_terminal()


# ==============================================================================
# 17. Completed Session Immutability
# ==============================================================================
def test_completed_session_immutability():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_immutable")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)
    mgr.complete_session(
        sess.session_id,
        conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
        rationale="Sound analytical basis.",
    )

    # Attempt to add observation to completed session
    with pytest.raises(ValidationError, match="Cannot add observation to session in state 'REVIEW_COMPLETE'"):
        mgr.add_observation(
            session_id=sess.session_id,
            observation_type=ObservationType.INTERPRETATION,
            statement="Trying to modify completed session.",
        )

    # Attempt to review evidence in completed session
    with pytest.raises(ValidationError, match="Cannot review evidence in session state 'REVIEW_COMPLETE'"):
        mgr.review_evidence(
            session_id=sess.session_id,
            evidence_id="ev_trend_01",
            disposition=EvidenceReviewDisposition.SUPPORTING,
        )


# ==============================================================================
# 18. Follow-up Session Lineage
# ==============================================================================
def test_followup_session_lineage():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_followup")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)
    completed = mgr.complete_session(
        sess.session_id,
        conclusion_state=ConclusionState.REVIEW_COMPLETED_WITH_LIMITATIONS,
        rationale="First pass completed.",
    )

    followup = mgr.reopen_session(
        session_id=completed.session_id,
        reviewer_id="analyst_george",
        reason="New 4-hour bar data arrived.",
    )

    assert followup.parent_session_id == completed.session_id
    assert followup.revision == 2
    assert followup.status == SessionState.ACTIVE
    assert followup.session_id != completed.session_id

    # Original completed session remains untouched in REVIEW_COMPLETE
    orig = mgr.get_session(completed.session_id)
    assert orig is not None
    assert orig.status == SessionState.REVIEW_COMPLETE


# ==============================================================================
# 19. Reviewer ID Validation
# ==============================================================================
def test_reviewer_id_validation():
    # Valid
    assert sanitize_reviewer_id("analyst_42") == "analyst_42"
    assert sanitize_reviewer_id(None) == "analyst_unassigned"
    assert sanitize_reviewer_id("") == "analyst_unassigned"

    # Traversal characters rejected
    with pytest.raises(ValidationError, match="path traversal"):
        sanitize_reviewer_id("../etc/passwd")

    with pytest.raises(ValidationError, match="path traversal"):
        sanitize_reviewer_id("analyst/admin")

    # Control chars rejected
    with pytest.raises(ValidationError, match="control characters"):
        sanitize_reviewer_id("analyst\ninjection")

    # Credential keywords rejected
    with pytest.raises(ValidationError, match="credential terminology"):
        sanitize_reviewer_id("bearer_token_123")


# ==============================================================================
# 20. Note Length Validation
# ==============================================================================
def test_note_length_validation():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_note_len")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    # Valid note
    note = mgr.add_review_note(sess.session_id, content="Concise analytical note.")
    assert note.content == "Concise analytical note."

    # Oversized note (> MAX_NOTE_LENGTH = 5000)
    oversized = "A" * (MAX_NOTE_LENGTH + 50)
    with pytest.raises(ValidationError, match="exceeds maximum length"):
        mgr.add_review_note(sess.session_id, content=oversized)


# ==============================================================================
# 21. Prohibited Field Rejection
# ==============================================================================
def test_prohibited_field_rejection():
    mgr = InvestigationSessionManager()
    cand_dict = {
        "candidate_id": "cand_prohibited",
        "action": "BUY",  # Prohibited!
        "expected_profit": 5000.0,  # Prohibited!
    }

    with pytest.raises(ValidationError, match="Prohibited trade recommendation field"):
        mgr.create_session(cand_dict)


# ==============================================================================
# 22. NaN Rejection
# ==============================================================================
def test_nan_rejection():
    with pytest.raises(ValidationError, match="cannot be NaN or Infinity"):
        InvestigationSession(
            session_id="sess_nan",
            candidate_id="cand_nan",
            priority_id=None,
            status=SessionState.CREATED,
            created_at=float("nan"),
        )


# ==============================================================================
# 23. Infinity Rejection
# ==============================================================================
def test_infinity_rejection():
    with pytest.raises(ValidationError, match="cannot be NaN or Infinity"):
        InvestigationSession(
            session_id="sess_inf",
            candidate_id="cand_inf",
            priority_id=None,
            status=SessionState.CREATED,
            created_at=float("inf"),
        )


# ==============================================================================
# 24. Zero Preservation
# ==============================================================================
def test_zero_preservation():
    sess = InvestigationSession(
        session_id="sess_zero",
        candidate_id="cand_zero",
        priority_id=None,
        status=SessionState.CREATED,
        created_at=0.0,
        updated_at=0.0,
    )
    assert sess.created_at == 0.0
    assert sess.updated_at == 0.0


# ==============================================================================
# 25. EventBus Events
# ==============================================================================
def test_eventbus_events():
    bus = EventBus()
    received_events: List[Event] = []

    def _collector(e: Event) -> None:
        received_events.append(e)

    for et in (
        EventType.INVESTIGATION_SESSION_CREATED,
        EventType.INVESTIGATION_SESSION_STARTED,
        EventType.INVESTIGATION_OBSERVATION_ADDED,
        EventType.INVESTIGATION_SESSION_COMPLETED,
    ):
        bus.subscribe(et, _collector)

    mgr = InvestigationSessionManager(event_bus=bus)
    cand = make_test_candidate("cand_bus_events")

    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)
    mgr.add_observation(
        sess.session_id,
        ObservationType.INTERPRETATION,
        statement="Observing normal distribution of residual errors.",
    )
    mgr.complete_session(
        sess.session_id,
        conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
        rationale="Satisfactory empirical backing.",
    )

    types = [e.event_type for e in received_events]
    assert EventType.INVESTIGATION_SESSION_CREATED.value in types
    assert EventType.INVESTIGATION_SESSION_STARTED.value in types
    assert EventType.INVESTIGATION_OBSERVATION_ADDED.value in types
    assert EventType.INVESTIGATION_SESSION_COMPLETED.value in types


# ==============================================================================
# 26. StateStore Persistence
# ==============================================================================
def test_statestore_persistence():
    store = StateStore()
    mgr = InvestigationSessionManager(state_store=store)
    cand = make_test_candidate("cand_state_store")

    sess = mgr.create_session(cand)
    latest = store.get("investigation_session:latest")
    assert latest is not None
    assert latest["session_id"] == sess.session_id

    by_id = store.get(f"investigation_session:{sess.session_id}")
    assert by_id["candidate_id"] == "cand_state_store"


# ==============================================================================
# 27. Audit Trail
# ==============================================================================
def test_audit_trail():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_audit_trail")

    sess = mgr.create_session(cand, reviewer_id="analyst_auditor")
    mgr.start_session(sess.session_id, reviewer_id="analyst_auditor")
    mgr.pause_session(sess.session_id, reason="Auditing")
    mgr.resume_session(sess.session_id)

    trail = mgr.get_audit_trail(sess.session_id)
    assert len(trail) >= 4
    # Check states transition sequence in audit records
    states = [(a.previous_state, a.new_state) for a in trail]
    assert (None, "CREATED") in states
    assert ("CREATED", "ACTIVE") in states
    assert ("ACTIVE", "PAUSED") in states
    assert ("PAUSED", "ACTIVE") in states


# ==============================================================================
# 28. Queue Integration
# ==============================================================================
def test_queue_integration():
    engine = OpportunityPrioritizationEngine()
    queue = OpportunityInvestigationQueue(engine=engine, max_capacity=10)
    cand = make_test_candidate("cand_queue_integ")

    prio = queue.add_candidate(cand)
    assert queue.size() == 1

    # Select candidate from queue and open investigation session
    top_prio = queue.get_next()
    assert top_prio is not None
    assert top_prio.candidate_id == cand.candidate_id

    mgr = InvestigationSessionManager()
    sess = mgr.create_session(candidate=cand, priority_record=top_prio)
    assert sess.priority_id == top_prio.priority_id

    # Removing candidate from queue does not affect investigation session
    queue.remove_candidate(cand.candidate_id)
    assert queue.size() == 0

    assert mgr.get_session(sess.session_id) is not None


# ==============================================================================
# 29. Concurrent Session Mutation
# ==============================================================================
def test_concurrent_session_mutation():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_concurrency")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    errors = []

    def worker(idx: int):
        try:
            mgr.add_observation(
                session_id=sess.session_id,
                observation_type=ObservationType.INTERPRETATION,
                statement=f"Concurrent observation {idx} executed cleanly.",
                reviewer_id=f"analyst_{idx}",
            )
            mgr.add_review_note(
                session_id=sess.session_id,
                content=f"Concurrent note {idx}.",
                reviewer_id=f"analyst_{idx}",
            )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    updated = mgr.get_session(sess.session_id)
    assert updated is not None
    assert len(updated.observations) == 10
    assert len(updated.review_notes) == 10


# ==============================================================================
# 30. Deterministic Behavior
# ==============================================================================
def test_deterministic_behavior():
    cand = make_test_candidate("cand_deterministic")

    # Run two identical workflows with fixed clock
    def run_flow():
        mgr = InvestigationSessionManager(clock=lambda: 1700000000.0)
        s = mgr.create_session(cand, reviewer_id="analyst_det")
        mgr.start_session(s.session_id)
        mgr.review_evidence(s.session_id, "ev_trend_01", EvidenceReviewDisposition.SUPPORTING, notes="Solid")
        c = mgr.complete_session(
            s.session_id,
            conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
            rationale="Deterministic analysis pass.",
        )
        return c

    c1 = run_flow()
    c2 = run_flow()

    assert c1.status == c2.status
    assert c1.analytical_conclusion is not None and c2.analytical_conclusion is not None
    assert c1.analytical_conclusion.conclusion_state == c2.analytical_conclusion.conclusion_state
    assert c1.voice_summary is not None and c2.voice_summary is not None
    assert c1.voice_summary.conclusion_speech == c2.voice_summary.conclusion_speech
    assert c1.voice_summary.caveats_speech == c2.voice_summary.caveats_speech


# ==============================================================================
# 31. Handler Registration
# ==============================================================================
def test_handler_registration():
    orchestrator = MasterOrchestrator(event_bus=EventBus(), state_store=StateStore())
    mgr = InvestigationSessionManager()
    register_investigation_handlers(orchestrator=orchestrator, investigation_manager=mgr)

    cand = make_test_candidate("cand_handler_reg")

    # Test create command via orchestrator
    req_create = OrchestratorRequest.create(
        command="create_investigation_session",
        parameters={"candidate": cand.to_dict(), "reviewer_id": "analyst_handler"},
    )
    res_create = orchestrator.handle_request(req_create)
    assert res_create.is_success
    session_id = res_create.result["session_id"]

    # Test start command via orchestrator
    req_start = OrchestratorRequest.create(
        command="start_investigation_session",
        parameters={"session_id": session_id},
    )
    res_start = orchestrator.handle_request(req_start)
    assert res_start.is_success
    assert res_start.result["session_status"] == SessionState.ACTIVE.value

    # Test get command
    req_get = OrchestratorRequest.create(
        command="get_investigation_session",
        parameters={"session_id": session_id},
    )
    res_get = orchestrator.handle_request(req_get)
    assert res_get.is_success
    assert res_get.result["session"]["status"] == SessionState.ACTIVE.value


# ==============================================================================
# 32. Dashboard GET
# ==============================================================================
def test_dashboard_get():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_dash_get")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        investigation_manager=mgr,
    )
    server.start(blocking=False)
    assert server._server is not None
    actual_port = server._server.server_port
    try:
        # Listing endpoint
        url_list = f"http://127.0.0.1:{actual_port}/api/investigation-sessions"
        with urllib.request.urlopen(url_list) as resp:
            assert resp.status == 200
            import json
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["count"] >= 1

        # Detail endpoint
        url_detail = f"http://127.0.0.1:{actual_port}/api/investigation-sessions/{sess.session_id}"
        with urllib.request.urlopen(url_detail) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["session"]["session_id"] == sess.session_id

        # Events endpoint
        url_events = f"http://127.0.0.1:{actual_port}/api/investigation-sessions/{sess.session_id}/events"
        with urllib.request.urlopen(url_events) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode())
            assert data["status"] == "success"
            assert data["count"] >= 2
    finally:
        server.stop()


# ==============================================================================
# 33. Dashboard HEAD
# ==============================================================================
def test_dashboard_head():
    mgr = InvestigationSessionManager()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        investigation_manager=mgr,
    )
    server.start(blocking=False)
    assert server._server is not None
    actual_port = server._server.server_port
    try:
        conn = http.client.HTTPConnection("127.0.0.1", actual_port, timeout=5)
        conn.request("HEAD", "/api/investigation-sessions")
        resp = conn.getresponse()
        assert resp.status == 200
        body = resp.read()
        assert len(body) == 0
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 34. Dashboard 405 on Mutating Methods
# ==============================================================================
def test_dashboard_405_mutating():
    server = PortfolioDashboardServer(host="127.0.0.1", port=0)
    server.start(blocking=False)
    assert server._server is not None
    actual_port = server._server.server_port
    try:
        conn = http.client.HTTPConnection("127.0.0.1", actual_port, timeout=5)
        conn.request("POST", "/api/investigation-sessions")
        resp = conn.getresponse()
        assert resp.status == 405
        assert "GET, HEAD" in resp.getheader("Allow", "")
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 35. Traversal Protection
# ==============================================================================
def test_traversal_protection():
    mgr = InvestigationSessionManager()
    server = PortfolioDashboardServer(
        host="127.0.0.1",
        port=0,
        investigation_manager=mgr,
    )
    server.start(blocking=False)
    assert server._server is not None
    actual_port = server._server.server_port
    try:
        # Request with path traversal in ID
        conn = http.client.HTTPConnection("127.0.0.1", actual_port, timeout=5)
        conn.request("GET", "/api/investigation-sessions/..%2F..%2Fetc%2Fpasswd")
        resp = conn.getresponse()
        assert resp.status == 400
        conn.close()
    finally:
        server.stop()


# ==============================================================================
# 36. Execution Lock Invariant
# ==============================================================================
def test_execution_lock_invariant():
    settings = Settings()
    assert settings.execution_locked is True, "Settings.execution_locked MUST remain True."


# ==============================================================================
# 37. No Broker Initialization
# ==============================================================================
def test_no_broker_initialization():
    settings = Settings()
    assert getattr(settings, "broker_api_key", None) is None
    assert getattr(settings, "broker_secret", None) is None
    assert getattr(settings, "broker_url", None) is None


# ==============================================================================
# 38. No Autonomous Execution Path
# ==============================================================================
def test_no_autonomous_execution_path():
    mgr = InvestigationSessionManager()
    cand = make_test_candidate("cand_no_exec")
    sess = mgr.create_session(cand)
    mgr.start_session(sess.session_id)

    # Manager methods cannot trigger trades
    for forbidden_method in ("execute_trade", "place_order", "send_order", "execute"):
        assert not hasattr(mgr, forbidden_method)

    # Completed session conclusion does not contain execution orders
    completed = mgr.complete_session(
        sess.session_id,
        conclusion_state=ConclusionState.EVIDENCE_SUPPORTS_FURTHER_ANALYSIS,
        rationale="Evidence validated; further analytical exploration supported.",
    )
    assert not hasattr(completed.analytical_conclusion, "order")
    assert not hasattr(completed.analytical_conclusion, "side")


# ==============================================================================
# 39. JUJU Directory Complete Isolation
# ==============================================================================
def test_juju_isolation():
    # JUJU must not be modified or present in KAISER codebase
    juju_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "JUJU"))
    if os.path.exists(juju_path):
        git_dir = os.path.join(juju_path, ".git")
        # Ensure no KAISER files were placed in JUJU
        kaiser_traces = [f for f in os.listdir(juju_path) if "kaiser" in f.lower()]
        assert len(kaiser_traces) == 0, f"JUJU directory corrupted with KAISER files: {kaiser_traces}"
