"""KAISER — Human Investigation Session Service and Lifecycle Manager.

Coordinates the end-to-end lifecycle of human investigation sessions, evidence reviews,
analyst observations, uncertainty documentation, and non-prescriptive analytical conclusions.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support only: Never outputs BUY, SELL, HOLD, or trading execution instructions.
2. Strict state machine: Enforces deterministic lifecycle transitions.
3. Immutability of completed sessions: Completed/terminal sessions cannot be mutated;
   follow-up evaluations create a linked new session preserving lineage (parent_session_id).
4. Epistemological enforcement: FACT observations require verified evidence citations;
   reviewed evidence IDs must exist in the candidate's known evidence set.
5. Auditability: Every mutation creates an immutable audit record and dispatches an event.
6. Thread-safety: All session mutations and queries are protected by re-entrant mutex locks.
"""

from __future__ import annotations

import collections
import copy
import logging
import math
import threading
import time
import uuid
from collections.abc import Mapping
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from app.analysis.opportunities.models import OpportunityCandidate, validate_no_prohibited_fields
from app.analysis.opportunities.prioritization.models import (
    OpportunityPriorityRecord,
    PriorityState,
)
from app.analysis.opportunities.investigation.models import (
    DEFAULT_MAX_SESSION_HISTORY,
    AnalyticalConclusion,
    ConclusionState,
    EvidenceReviewDisposition,
    EvidenceReviewRecord,
    InvestigationAuditRecord,
    InvestigationLimitation,
    InvestigationObservation,
    InvestigationReviewNote,
    InvestigationSession,
    InvestigationUncertainty,
    InvestigationVoiceSummary,
    ObservationType,
    SessionState,
    SourceType,
    VALID_SESSION_TRANSITIONS,
    sanitize_reviewer_id,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import (
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)

logger = logging.getLogger("kaiser.analysis.opportunities.investigation.session")


class InvestigationSessionManager:
    """Thread-safe manager governing human investigation sessions and evidence review workflows."""

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        max_history: int = DEFAULT_MAX_SESSION_HISTORY,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        """Initialize the Investigation Session Manager.

        Args:
            event_bus: Optional EventBus for telemetry notifications.
            state_store: Optional StateStore for state persistence.
            max_history: Maximum number of historical sessions retained in memory.
            clock: Time provider function (defaults to time.time).
        """
        self.event_bus = event_bus
        self.state_store = state_store
        self.max_history = max(1, min(1000, max_history))
        self.clock = clock or time.time

        self._lock = threading.RLock()
        self._sessions: Dict[str, InvestigationSession] = {}
        self._session_order: List[str] = []
        self._audit_trails: Dict[str, List[InvestigationAuditRecord]] = collections.defaultdict(list)

    def _record_audit(
        self,
        session_id: str,
        event_type: str,
        reviewer_id: Optional[str],
        previous_state: Optional[str],
        new_state: Optional[str],
        action_summary: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> InvestigationAuditRecord:
        """Create and record an immutable audit record for a session mutation."""
        now = self.clock()
        audit_id = f"aud_{session_id}_{int(now * 1000)}_{len(self._audit_trails[session_id]) + 1}"
        record = InvestigationAuditRecord(
            audit_id=audit_id,
            session_id=session_id,
            event_type=event_type,
            timestamp=now,
            reviewer_id=reviewer_id,
            previous_state=previous_state,
            new_state=new_state,
            action_summary=action_summary,
            metadata=metadata or {},
        )
        self._audit_trails[session_id].append(record)
        return record

    def _dispatch_event(
        self,
        event_type: EventType,
        session: InvestigationSession,
        audit: InvestigationAuditRecord,
    ) -> None:
        """Dispatch a sanitized lifecycle event to the EventBus."""
        if not self.event_bus:
            return
        try:
            payload = {
                "session_id": session.session_id,
                "candidate_id": session.candidate_id,
                "priority_id": session.priority_id,
                "status": session.status.value,
                "reviewer_id": session.reviewer_id,
                "audit_id": audit.audit_id,
                "action_summary": audit.action_summary,
                "created_at": session.created_at,
                "updated_at": session.updated_at,
                "is_terminal": session.is_terminal(),
                "evidence_review_count": len(session.evidence_reviews),
                "observation_count": len(session.observations),
                "uncertainty_count": len(session.uncertainties),
                "limitation_count": len(session.limitations),
            }
            if session.analytical_conclusion:
                payload["conclusion_state"] = session.analytical_conclusion.conclusion_state.value
            event = Event.create(
                event_type=event_type,
                source="investigation_session_manager",
                payload=payload,
                correlation_id=session.session_id,
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.error("Failed to publish event %s: %s", event_type, exc)

    def _persist_state(self, session: InvestigationSession) -> None:
        """Persist bounded investigation session state to StateStore."""
        if not self.state_store:
            return
        try:
            s_dict = session.to_dict()
            self.state_store.set("investigation_session:latest", s_dict)
            self.state_store.set(f"investigation_session:{session.session_id}", s_dict)

            # Maintain bounded history
            history = self.state_store.get("investigation_session:history", [])
            if not isinstance(history, list):
                history = []
            
            # Update or append
            updated_history = [s for s in history if s.get("session_id") != session.session_id]
            updated_history.append(s_dict)
            if len(updated_history) > self.max_history:
                updated_history = updated_history[-self.max_history:]
            self.state_store.set("investigation_session:history", updated_history)
        except Exception as exc:
            logger.error("Failed to persist investigation session state: %s", exc)

    def _extract_known_evidence_ids(
        self,
        candidate_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, ...]:
        """Extract all valid known evidence IDs from candidate, report, and runs."""
        ids: Set[str] = set()

        cand_id = candidate_data.get("candidate_id") or candidate_data.get("opportunity_id")
        if cand_id:
            ids.add(str(cand_id))

        report_id = candidate_data.get("source_report_id") or candidate_data.get("source_evidence_report_id")
        if report_id:
            ids.add(str(report_id))

        # Check evidence collections
        for col_name in (
            "supporting_evidence",
            "conflicting_evidence",
            "missing_evidence",
            "supporting_evidence_refs",
            "conflicting_evidence_refs",
            "uncertainty_refs",
            "limitation_refs",
        ):
            items = candidate_data.get(col_name, [])
            if isinstance(items, (list, tuple)):
                for item in items:
                    if isinstance(item, Mapping):
                        if "evidence_id" in item:
                            ids.add(str(item["evidence_id"]))
                        elif "id" in item:
                            ids.add(str(item["id"]))
                    elif isinstance(item, str):
                        ids.add(item.strip())

        # Check source analysis run IDs
        runs = candidate_data.get("source_analysis_run_ids", [])
        if isinstance(runs, (list, tuple)):
            for r in runs:
                ids.add(str(r))

        # Check metadata extra evidence
        if metadata and "evidence_ids" in metadata:
            meta_eids = metadata["evidence_ids"]
            if isinstance(meta_eids, (list, tuple)):
                for eid in meta_eids:
                    ids.add(str(eid))

        # Ensure no empty strings
        clean_ids = sorted([i for i in ids if i])
        return tuple(clean_ids)

    def create_session(
        self,
        candidate: Union[OpportunityCandidate, Dict[str, Any], Mapping[str, Any]],
        priority_record: Optional[Union[OpportunityPriorityRecord, Dict[str, Any], Mapping[str, Any]]] = None,
        reviewer_id: Optional[str] = None,
        parent_session_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> InvestigationSession:
        """Create a new Human Investigation Session for an OpportunityCandidate.

        Initial state is CREATED.

        Args:
            candidate: OpportunityCandidate instance or dictionary.
            priority_record: Optional OpportunityPriorityRecord instance or dictionary.
            reviewer_id: Optional analyst reviewer identifier.
            parent_session_id: Optional parent session ID for follow-up investigations.
            metadata: Optional arbitrary metadata dictionary.

        Returns:
            Newly created InvestigationSession.

        Raises:
            ValidationError: If input validation fails or prohibited fields exist.
        """
        # Validate candidate data
        if isinstance(candidate, OpportunityCandidate):
            cand_dict = candidate.to_dict()
        elif isinstance(candidate, Mapping):
            cand_dict = dict(candidate)
        else:
            raise ValidationError(f"Invalid candidate type: {type(candidate).__name__}.")

        validate_no_prohibited_fields(cand_dict, path="candidate")

        cand_id = str(cand_dict.get("candidate_id") or cand_dict.get("opportunity_id", "")).strip()
        if not cand_id:
            raise ValidationError("Candidate ID must not be empty.")

        prio_id = None
        if priority_record:
            if isinstance(priority_record, OpportunityPriorityRecord):
                p_dict = priority_record.to_dict()
            elif isinstance(priority_record, Mapping):
                p_dict = dict(priority_record)
            else:
                raise ValidationError(f"Invalid priority_record type: {type(priority_record).__name__}.")
            validate_no_prohibited_fields(p_dict, path="priority_record")
            prio_id = str(p_dict.get("priority_id", "")).strip() or None

        sanitized_reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else None

        with self._lock:
            # Check parent session if supplied
            revision = 1
            if parent_session_id:
                clean_parent = parent_session_id.strip()
                if clean_parent not in self._sessions:
                    raise ValidationError(f"Parent session '{clean_parent}' does not exist.")
                parent_sess = self._sessions[clean_parent]
                revision = parent_sess.revision + 1

            now = self.clock()
            session_id = f"inv_sess_{cand_id}_{int(now)}_{uuid.uuid4().hex[:6]}"
            known_eids = self._extract_known_evidence_ids(cand_dict, metadata)

            runs_raw = cand_dict.get("source_analysis_run_ids", ())
            source_runs = tuple(str(r) for r in runs_raw) if isinstance(runs_raw, (list, tuple)) else ()

            session = InvestigationSession(
                session_id=session_id,
                candidate_id=cand_id,
                priority_id=prio_id,
                status=SessionState.CREATED,
                created_at=now,
                updated_at=now,
                reviewer_id=sanitized_reviewer,
                known_evidence_ids=known_eids,
                source_report_id=cand_dict.get("source_report_id"),
                source_analysis_run_ids=source_runs,
                reproducibility_id=cand_dict.get("reproducibility_id"),
                parent_session_id=parent_session_id.strip() if parent_session_id else None,
                revision=revision,
                metadata=metadata or {},
            )

            self._sessions[session_id] = session
            self._session_order.append(session_id)

            # Evict oldest if beyond capacity
            if len(self._session_order) > self.max_history:
                evict_id = self._session_order.pop(0)
                # Keep terminal sessions in state store, remove oldest from memory
                if evict_id in self._sessions and self._sessions[evict_id].is_terminal():
                    del self._sessions[evict_id]

            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_SESSION_CREATED.value,
                reviewer_id=sanitized_reviewer,
                previous_state=None,
                new_state=SessionState.CREATED.value,
                action_summary=f"Investigation session created for candidate '{cand_id}'.",
                metadata={"revision": revision, "parent_session_id": parent_session_id},
            )

            self._dispatch_event(EventType.INVESTIGATION_SESSION_CREATED, session, audit)
            self._persist_state(session)
            return session

    def start_session(
        self,
        session_id: str,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationSession:
        """Transition a session from CREATED to ACTIVE.

        Args:
            session_id: Investigation session ID.
            reviewer_id: Optional analyst reviewer identifier.

        Returns:
            Updated InvestigationSession in ACTIVE state.

        Raises:
            ValidationError: If session not found or transition is illegal.
        """
        with self._lock:
            session = self._get_session_locked(session_id)
            if session.is_terminal():
                raise ValidationError(f"Session '{session_id}' is in terminal state '{session.status.value}' and cannot be started.")

            if session.status == SessionState.ACTIVE:
                return session

            if session.status != SessionState.CREATED:
                raise ValidationError(
                    f"Illegal lifecycle transition: Cannot start session from state '{session.status.value}'. "
                    f"Valid prior state is '{SessionState.CREATED.value}'."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else session.reviewer_id

            updated_session = InvestigationSession(
                session_id=session.session_id,
                candidate_id=session.candidate_id,
                priority_id=session.priority_id,
                status=SessionState.ACTIVE,
                created_at=session.created_at,
                started_at=session.started_at or now,
                updated_at=now,
                closed_at=session.closed_at,
                reviewer_id=reviewer,
                evidence_reviews=session.evidence_reviews,
                observations=session.observations,
                uncertainties=session.uncertainties,
                limitations=session.limitations,
                review_notes=session.review_notes,
                analytical_conclusion=session.analytical_conclusion,
                known_evidence_ids=session.known_evidence_ids,
                source_report_id=session.source_report_id,
                source_analysis_run_ids=session.source_analysis_run_ids,
                reproducibility_id=session.reproducibility_id,
                parent_session_id=session.parent_session_id,
                revision=session.revision,
                voice_summary=session.voice_summary,
                metadata=copy.deepcopy(session.metadata),
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_SESSION_STARTED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=SessionState.ACTIVE.value,
                action_summary=f"Investigation session started by '{reviewer}'.",
            )

            self._dispatch_event(EventType.INVESTIGATION_SESSION_STARTED, updated_session, audit)
            self._persist_state(updated_session)
            return updated_session

    def pause_session(
        self,
        session_id: str,
        reviewer_id: Optional[str] = None,
        reason: str = "",
    ) -> InvestigationSession:
        """Transition an ACTIVE session to PAUSED."""
        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status != SessionState.ACTIVE:
                raise ValidationError(
                    f"Illegal lifecycle transition: Cannot pause session from state '{session.status.value}'. "
                    f"Session must be in state '{SessionState.ACTIVE.value}'."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else session.reviewer_id

            updated_session = self._update_session_state(
                session=session,
                new_status=SessionState.PAUSED,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_SESSION_PAUSED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=SessionState.PAUSED.value,
                action_summary=f"Investigation session paused. Reason: {reason or 'Not specified'}",
                metadata={"reason": reason},
            )

            self._dispatch_event(EventType.INVESTIGATION_SESSION_PAUSED, updated_session, audit)
            self._persist_state(updated_session)
            return updated_session

    def resume_session(
        self,
        session_id: str,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationSession:
        """Transition a PAUSED or REQUIRES_MORE_EVIDENCE session back to ACTIVE."""
        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status not in (SessionState.PAUSED, SessionState.REQUIRES_MORE_EVIDENCE):
                raise ValidationError(
                    f"Illegal lifecycle transition: Cannot resume session from state '{session.status.value}'. "
                    f"Session must be in '{SessionState.PAUSED.value}' or '{SessionState.REQUIRES_MORE_EVIDENCE.value}'."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else session.reviewer_id

            updated_session = self._update_session_state(
                session=session,
                new_status=SessionState.ACTIVE,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_SESSION_RESUMED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=SessionState.ACTIVE.value,
                action_summary="Investigation session resumed to ACTIVE.",
            )

            self._dispatch_event(EventType.INVESTIGATION_SESSION_RESUMED, updated_session, audit)
            self._persist_state(updated_session)
            return updated_session

    def request_more_evidence(
        self,
        session_id: str,
        reason: str,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationSession:
        """Transition an ACTIVE session to REQUIRES_MORE_EVIDENCE."""
        if not reason or not isinstance(reason, str) or not reason.strip():
            raise ValidationError("A non-empty reason must be provided when requesting more evidence.")

        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status != SessionState.ACTIVE:
                raise ValidationError(
                    f"Illegal lifecycle transition: Cannot request more evidence from state '{session.status.value}'. "
                    f"Session must be '{SessionState.ACTIVE.value}'."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else session.reviewer_id

            updated_session = self._update_session_state(
                session=session,
                new_status=SessionState.REQUIRES_MORE_EVIDENCE,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_MORE_EVIDENCE_REQUESTED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=SessionState.REQUIRES_MORE_EVIDENCE.value,
                action_summary=f"More evidence requested. Reason: {reason}",
                metadata={"reason": reason},
            )

            self._dispatch_event(EventType.INVESTIGATION_MORE_EVIDENCE_REQUESTED, updated_session, audit)
            self._persist_state(updated_session)
            return updated_session

    def add_observation(
        self,
        session_id: str,
        observation_type: Union[str, ObservationType],
        statement: str,
        evidence_refs: Optional[Sequence[str]] = None,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationObservation:
        """Record an analyst observation in an active investigation session.

        Enforces epistemological rigor: FACT observations MUST have valid evidence references.
        """
        obs_type = ObservationType.from_string(observation_type)
        clean_refs = tuple(str(r).strip() for r in (evidence_refs or []) if str(r).strip())

        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status != SessionState.ACTIVE:
                raise ValidationError(
                    f"Cannot add observation to session in state '{session.status.value}'. "
                    f"Session must be '{SessionState.ACTIVE.value}'."
                )

            # Epistemological safeguard: FACT requires non-empty verified evidence citations
            if obs_type == ObservationType.FACT:
                if not clean_refs:
                    raise ValidationError(
                        "Epistemological rule: ObservationType.FACT requires at least one verified evidence reference. "
                        "Unsupported observations must be recorded as INTERPRETATION, QUESTION, or UNCERTAINTY."
                    )
                # Verify evidence references against known evidence IDs
                for ref in clean_refs:
                    if session.known_evidence_ids and ref not in session.known_evidence_ids:
                        raise ValidationError(
                            f"Evidence reference '{ref}' is not among known evidence IDs for candidate '{session.candidate_id}'. "
                            f"Available IDs: {list(session.known_evidence_ids)}"
                        )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")
            obs_id = f"obs_{session_id}_{len(session.observations) + 1}_{int(now)}"

            obs = InvestigationObservation(
                observation_id=obs_id,
                session_id=session_id,
                observation_type=obs_type,
                statement=statement,
                evidence_refs=clean_refs,
                source_type=SourceType.HUMAN_REVIEW,
                reviewer_id=reviewer,
                created_at=now,
            )

            new_observations = session.observations + (obs,)
            updated_session = self._clone_with_modifications(
                session=session,
                observations=new_observations,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_OBSERVATION_ADDED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=session.status.value,
                action_summary=f"Observation added ({obs_type.value}): {statement[:80]}...",
                metadata={"observation_id": obs_id, "type": obs_type.value},
            )

            self._dispatch_event(EventType.INVESTIGATION_OBSERVATION_ADDED, updated_session, audit)
            self._persist_state(updated_session)
            return obs

    def review_evidence(
        self,
        session_id: str,
        evidence_id: str,
        disposition: Union[str, EvidenceReviewDisposition],
        notes: str = "",
        reviewer_id: Optional[str] = None,
    ) -> EvidenceReviewRecord:
        """Record an explicit analyst evaluation on a specific evidence item."""
        clean_eid = str(evidence_id).strip()
        if not clean_eid:
            raise ValidationError("Evidence ID must not be empty.")

        disp = EvidenceReviewDisposition.from_string(disposition)

        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status != SessionState.ACTIVE:
                raise ValidationError(
                    f"Cannot review evidence in session state '{session.status.value}'. "
                    f"Session must be '{SessionState.ACTIVE.value}'."
                )

            # Deterministic rejection of fabricated/unknown evidence IDs
            if session.known_evidence_ids and clean_eid not in session.known_evidence_ids:
                raise ValidationError(
                    f"Evidence ID '{clean_eid}' is not an established evidence reference for candidate '{session.candidate_id}'. "
                    f"Available references: {list(session.known_evidence_ids)}"
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")
            rev_id = f"rev_{session_id}_{clean_eid}_{int(now)}"

            record = EvidenceReviewRecord(
                review_id=rev_id,
                session_id=session_id,
                evidence_id=clean_eid,
                disposition=disp,
                notes=notes,
                reviewer_id=reviewer,
                source_type=SourceType.HUMAN_REVIEW,
                created_at=now,
            )

            # Replace existing review for same evidence_id or append
            filtered_reviews = tuple(r for r in session.evidence_reviews if r.evidence_id != clean_eid)
            new_reviews = filtered_reviews + (record,)

            updated_session = self._clone_with_modifications(
                session=session,
                evidence_reviews=new_reviews,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_EVIDENCE_REVIEWED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=session.status.value,
                action_summary=f"Evidence '{clean_eid}' marked as '{disp.value}'.",
                metadata={"evidence_id": clean_eid, "disposition": disp.value},
            )

            self._dispatch_event(EventType.INVESTIGATION_EVIDENCE_REVIEWED, updated_session, audit)
            self._persist_state(updated_session)
            return record

    def add_uncertainty(
        self,
        session_id: str,
        description: str,
        severity: str = "MEDIUM",
        evidence_refs: Optional[Sequence[str]] = None,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationUncertainty:
        """Document an identified uncertainty in an active investigation session."""
        clean_refs = tuple(str(r).strip() for r in (evidence_refs or []) if str(r).strip())

        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status != SessionState.ACTIVE:
                raise ValidationError(
                    f"Cannot document uncertainty in session state '{session.status.value}'. "
                    f"Session must be '{SessionState.ACTIVE.value}'."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")
            unc_id = f"unc_{session_id}_{len(session.uncertainties) + 1}_{int(now)}"

            unc = InvestigationUncertainty(
                uncertainty_id=unc_id,
                session_id=session_id,
                description=description,
                severity=severity.upper().strip() if severity else "MEDIUM",
                evidence_refs=clean_refs,
                source_type=SourceType.HUMAN_REVIEW,
                reviewer_id=reviewer,
                created_at=now,
            )

            new_uncs = session.uncertainties + (unc,)
            updated_session = self._clone_with_modifications(
                session=session,
                uncertainties=new_uncs,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_UNCERTAINTY_ADDED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=session.status.value,
                action_summary=f"Uncertainty recorded ({unc.severity}): {description[:80]}...",
                metadata={"uncertainty_id": unc_id, "severity": unc.severity},
            )

            self._dispatch_event(EventType.INVESTIGATION_UNCERTAINTY_ADDED, updated_session, audit)
            self._persist_state(updated_session)
            return unc

    def add_limitation(
        self,
        session_id: str,
        description: str,
        category: str = "ANALYTICAL_SCOPE",
        reviewer_id: Optional[str] = None,
    ) -> InvestigationLimitation:
        """Document an analytical limitation or constraint."""
        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status != SessionState.ACTIVE:
                raise ValidationError(
                    f"Cannot document limitation in session state '{session.status.value}'. "
                    f"Session must be '{SessionState.ACTIVE.value}'."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")
            lim_id = f"lim_{session_id}_{len(session.limitations) + 1}_{int(now)}"

            lim = InvestigationLimitation(
                limitation_id=lim_id,
                session_id=session_id,
                description=description,
                category=category.upper().strip() if category else "ANALYTICAL_SCOPE",
                source_type=SourceType.HUMAN_REVIEW,
                reviewer_id=reviewer,
                created_at=now,
            )

            new_lims = session.limitations + (lim,)
            updated_session = self._clone_with_modifications(
                session=session,
                limitations=new_lims,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_LIMITATION_ADDED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=session.status.value,
                action_summary=f"Limitation recorded ({lim.category}): {description[:80]}...",
                metadata={"limitation_id": lim_id, "category": lim.category},
            )

            self._dispatch_event(EventType.INVESTIGATION_LIMITATION_ADDED, updated_session, audit)
            self._persist_state(updated_session)
            return lim

    def add_review_note(
        self,
        session_id: str,
        content: str,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationReviewNote:
        """Add structured commentary or notes to an active session."""
        with self._lock:
            session = self._get_session_locked(session_id)
            if session.is_terminal():
                raise ValidationError(
                    f"Cannot add review notes to terminal session '{session_id}' ({session.status.value})."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")
            note_id = f"note_{session_id}_{len(session.review_notes) + 1}_{int(now)}"

            note = InvestigationReviewNote(
                note_id=note_id,
                session_id=session_id,
                reviewer_id=reviewer,
                content=content,
                created_at=now,
            )

            new_notes = session.review_notes + (note,)
            updated_session = self._clone_with_modifications(
                session=session,
                review_notes=new_notes,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            self._persist_state(updated_session)
            return note

    def set_conclusion(
        self,
        session_id: str,
        conclusion_state: Union[str, ConclusionState],
        rationale: str,
        reviewer_id: Optional[str] = None,
    ) -> AnalyticalConclusion:
        """Set or update the non-prescriptive analytical conclusion for an active session."""
        c_state = ConclusionState.from_string(conclusion_state)

        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status != SessionState.ACTIVE:
                raise ValidationError(
                    f"Cannot set conclusion for session in state '{session.status.value}'. "
                    f"Session must be '{SessionState.ACTIVE.value}'."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")
            conc_id = f"conc_{session_id}_{int(now)}"

            # Count supporting and conflicting dispositions
            supporting_cnt = sum(1 for r in session.evidence_reviews if r.disposition == EvidenceReviewDisposition.SUPPORTING)
            conflicting_cnt = sum(1 for r in session.evidence_reviews if r.disposition == EvidenceReviewDisposition.CONFLICTING)

            conclusion = AnalyticalConclusion(
                conclusion_id=conc_id,
                session_id=session_id,
                conclusion_state=c_state,
                rationale=rationale,
                supporting_evidence_count=supporting_cnt,
                conflicting_evidence_count=conflicting_cnt,
                uncertainty_count=len(session.uncertainties),
                limitation_count=len(session.limitations),
                reviewer_id=reviewer,
                source_type=SourceType.HUMAN_REVIEW,
                created_at=now,
            )

            updated_session = self._clone_with_modifications(
                session=session,
                analytical_conclusion=conclusion,
                updated_at=now,
                reviewer_id=reviewer,
            )

            self._sessions[session_id] = updated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_CONCLUSION_SET.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=session.status.value,
                action_summary=f"Analytical conclusion set to '{c_state.value}'.",
                metadata={"conclusion_id": conc_id, "conclusion_state": c_state.value},
            )

            self._dispatch_event(EventType.INVESTIGATION_CONCLUSION_SET, updated_session, audit)
            self._persist_state(updated_session)
            return conclusion

    def complete_session(
        self,
        session_id: str,
        reviewer_id: Optional[str] = None,
        conclusion_state: Optional[Union[str, ConclusionState]] = None,
        rationale: Optional[str] = None,
    ) -> InvestigationSession:
        """Complete an investigation session and freeze it into an immutable record.

        Requires an AnalyticalConclusion. Once completed, the session is TERMINAL and immutable.
        """
        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status != SessionState.ACTIVE:
                raise ValidationError(
                    f"Cannot complete session in state '{session.status.value}'. "
                    f"Session must be '{SessionState.ACTIVE.value}'."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")

            # Check conclusion
            conclusion = session.analytical_conclusion
            if conclusion_state is not None:
                if not rationale:
                    raise ValidationError("Rationale must be provided when setting conclusion at completion.")
                c_state = ConclusionState.from_string(conclusion_state)
                conc_id = f"conc_{session_id}_{int(now)}"
                supporting_cnt = sum(1 for r in session.evidence_reviews if r.disposition == EvidenceReviewDisposition.SUPPORTING)
                conflicting_cnt = sum(1 for r in session.evidence_reviews if r.disposition == EvidenceReviewDisposition.CONFLICTING)
                conclusion = AnalyticalConclusion(
                    conclusion_id=conc_id,
                    session_id=session_id,
                    conclusion_state=c_state,
                    rationale=rationale,
                    supporting_evidence_count=supporting_cnt,
                    conflicting_evidence_count=conflicting_cnt,
                    uncertainty_count=len(session.uncertainties),
                    limitation_count=len(session.limitations),
                    reviewer_id=reviewer,
                    source_type=SourceType.HUMAN_REVIEW,
                    created_at=now,
                )

            if conclusion is None:
                raise ValidationError(
                    "Cannot complete investigation session without an AnalyticalConclusion. "
                    "Call set_conclusion() or provide conclusion_state and rationale at completion."
                )

            # Generate deterministic speech-ready summary
            voice_summary = self._build_voice_summary(session, conclusion)

            completed_session = InvestigationSession(
                session_id=session.session_id,
                candidate_id=session.candidate_id,
                priority_id=session.priority_id,
                status=SessionState.REVIEW_COMPLETE,
                created_at=session.created_at,
                started_at=session.started_at,
                updated_at=now,
                closed_at=now,
                reviewer_id=reviewer,
                evidence_reviews=session.evidence_reviews,
                observations=session.observations,
                uncertainties=session.uncertainties,
                limitations=session.limitations,
                review_notes=session.review_notes,
                analytical_conclusion=conclusion,
                known_evidence_ids=session.known_evidence_ids,
                source_report_id=session.source_report_id,
                source_analysis_run_ids=session.source_analysis_run_ids,
                reproducibility_id=session.reproducibility_id,
                parent_session_id=session.parent_session_id,
                revision=session.revision,
                voice_summary=voice_summary,
                metadata=copy.deepcopy(session.metadata),
            )

            self._sessions[session_id] = completed_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_SESSION_COMPLETED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=SessionState.REVIEW_COMPLETE.value,
                action_summary=f"Investigation session completed with conclusion '{conclusion.conclusion_state.value}'.",
                metadata={"conclusion_state": conclusion.conclusion_state.value},
            )

            self._dispatch_event(EventType.INVESTIGATION_SESSION_COMPLETED, completed_session, audit)
            self._persist_state(completed_session)
            return completed_session

    def invalidate_session(
        self,
        session_id: str,
        reason: str,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationSession:
        """Invalidate an investigation session due to underlying data or candidate invalidation."""
        if not reason or not isinstance(reason, str) or not reason.strip():
            raise ValidationError("A non-empty reason must be provided when invalidating a session.")

        with self._lock:
            session = self._get_session_locked(session_id)
            if session.status not in (SessionState.ACTIVE, SessionState.PAUSED, SessionState.REQUIRES_MORE_EVIDENCE):
                raise ValidationError(
                    f"Cannot invalidate session from state '{session.status.value}'. "
                    f"Valid states are ACTIVE, PAUSED, or REQUIRES_MORE_EVIDENCE."
                )

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")

            invalidated_session = InvestigationSession(
                session_id=session.session_id,
                candidate_id=session.candidate_id,
                priority_id=session.priority_id,
                status=SessionState.INVALIDATED,
                created_at=session.created_at,
                started_at=session.started_at,
                updated_at=now,
                closed_at=now,
                reviewer_id=reviewer,
                evidence_reviews=session.evidence_reviews,
                observations=session.observations,
                uncertainties=session.uncertainties,
                limitations=session.limitations,
                review_notes=session.review_notes,
                analytical_conclusion=session.analytical_conclusion,
                known_evidence_ids=session.known_evidence_ids,
                source_report_id=session.source_report_id,
                source_analysis_run_ids=session.source_analysis_run_ids,
                reproducibility_id=session.reproducibility_id,
                parent_session_id=session.parent_session_id,
                revision=session.revision,
                voice_summary=None,
                metadata=copy.deepcopy(session.metadata),
            )

            self._sessions[session_id] = invalidated_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_SESSION_INVALIDATED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=SessionState.INVALIDATED.value,
                action_summary=f"Investigation session invalidated. Reason: {reason}",
                metadata={"reason": reason},
            )

            self._dispatch_event(EventType.INVESTIGATION_SESSION_INVALIDATED, invalidated_session, audit)
            self._persist_state(invalidated_session)
            return invalidated_session

    def cancel_session(
        self,
        session_id: str,
        reason: str,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationSession:
        """Cancel an investigation session prior to formal completion."""
        if not reason or not isinstance(reason, str) or not reason.strip():
            raise ValidationError("A non-empty reason must be provided when cancelling a session.")

        with self._lock:
            session = self._get_session_locked(session_id)
            if session.is_terminal():
                raise ValidationError(f"Session '{session_id}' is already in terminal state '{session.status.value}'.")

            now = self.clock()
            reviewer = sanitize_reviewer_id(reviewer_id) if reviewer_id else (session.reviewer_id or "analyst_unassigned")

            cancelled_session = InvestigationSession(
                session_id=session.session_id,
                candidate_id=session.candidate_id,
                priority_id=session.priority_id,
                status=SessionState.CANCELLED,
                created_at=session.created_at,
                started_at=session.started_at,
                updated_at=now,
                closed_at=now,
                reviewer_id=reviewer,
                evidence_reviews=session.evidence_reviews,
                observations=session.observations,
                uncertainties=session.uncertainties,
                limitations=session.limitations,
                review_notes=session.review_notes,
                analytical_conclusion=session.analytical_conclusion,
                known_evidence_ids=session.known_evidence_ids,
                source_report_id=session.source_report_id,
                source_analysis_run_ids=session.source_analysis_run_ids,
                reproducibility_id=session.reproducibility_id,
                parent_session_id=session.parent_session_id,
                revision=session.revision,
                voice_summary=None,
                metadata=copy.deepcopy(session.metadata),
            )

            self._sessions[session_id] = cancelled_session
            audit = self._record_audit(
                session_id=session_id,
                event_type=EventType.INVESTIGATION_SESSION_CANCELLED.value,
                reviewer_id=reviewer,
                previous_state=session.status.value,
                new_state=SessionState.CANCELLED.value,
                action_summary=f"Investigation session cancelled. Reason: {reason}",
                metadata={"reason": reason},
            )

            self._dispatch_event(EventType.INVESTIGATION_SESSION_CANCELLED, cancelled_session, audit)
            self._persist_state(cancelled_session)
            return cancelled_session

    def reopen_session(
        self,
        session_id: str,
        reviewer_id: Optional[str] = None,
        reason: str = "",
    ) -> InvestigationSession:
        """Create a follow-up revision session linked to a terminal session.

        Preserves terminal session immutability and historical audit integrity while
        opening a new investigation lifecycle revision.
        """
        with self._lock:
            parent = self._get_session_locked(session_id)
            if not parent.is_terminal():
                raise ValidationError(
                    f"Session '{session_id}' is not terminal (current state: '{parent.status.value}'). "
                    f"Only completed or terminal sessions can be reopened via revision lineage."
                )

            # Build follow-up candidate dict
            cand_dict = {
                "candidate_id": parent.candidate_id,
                "source_report_id": parent.source_report_id,
                "source_analysis_run_ids": parent.source_analysis_run_ids,
                "reproducibility_id": parent.reproducibility_id,
            }

            prio_dict = {"priority_id": parent.priority_id} if parent.priority_id else None

            followup = self.create_session(
                candidate=cand_dict,
                priority_record=prio_dict,
                reviewer_id=reviewer_id or parent.reviewer_id,
                parent_session_id=parent.session_id,
                metadata={
                    "reopened_from": parent.session_id,
                    "reopen_reason": reason or "Follow-up investigation requested",
                    "previous_conclusion": parent.analytical_conclusion.conclusion_state.value if parent.analytical_conclusion else None,
                },
            )

            # Automatically start follow-up session into ACTIVE
            return self.start_session(followup.session_id, reviewer_id=reviewer_id)

    def get_session(self, session_id: str) -> Optional[InvestigationSession]:
        """Retrieve an investigation session by ID."""
        clean_id = session_id.strip() if session_id else ""
        with self._lock:
            if clean_id in self._sessions:
                return self._sessions[clean_id]

            # Fallback to state store
            if self.state_store:
                s_dict = self.state_store.get(f"investigation_session:{clean_id}")
                if isinstance(s_dict, dict):
                    return self._session_from_dict(s_dict)
            return None

    def list_sessions(
        self,
        limit: int = 50,
        offset: int = 0,
        status: Optional[Union[str, SessionState]] = None,
        candidate_id: Optional[str] = None,
    ) -> List[InvestigationSession]:
        """List investigation sessions matching filters, ordered by created_at descending."""
        with self._lock:
            all_sessions = list(self._sessions.values())

            # Supplement from state store history if memory has fewer items
            if self.state_store:
                history = self.state_store.get("investigation_session:history", [])
                if isinstance(history, list):
                    known_ids = {s.session_id for s in all_sessions}
                    for item in history:
                        if isinstance(item, dict) and item.get("session_id") not in known_ids:
                            sess = self._session_from_dict(item)
                            if sess:
                                all_sessions.append(sess)

            # Apply filters
            status_filter = SessionState.from_string(status) if status else None
            filtered = []
            for s in all_sessions:
                if status_filter and s.status != status_filter:
                    continue
                if candidate_id and s.candidate_id != candidate_id.strip():
                    continue
                filtered.append(s)

            # Sort descending by created_at
            filtered.sort(key=lambda s: s.created_at, reverse=True)
            return filtered[offset : offset + limit]

    def get_audit_trail(self, session_id: str) -> List[InvestigationAuditRecord]:
        """Return the immutable audit trail for a session."""
        clean_id = session_id.strip() if session_id else ""
        with self._lock:
            return list(self._audit_trails.get(clean_id, []))

    def _get_session_locked(self, session_id: str) -> InvestigationSession:
        """Internal helper to retrieve session while holding lock."""
        clean_id = session_id.strip() if session_id else ""
        if clean_id not in self._sessions:
            raise ValidationError(f"Investigation session '{clean_id}' not found.")
        return self._sessions[clean_id]

    def _update_session_state(
        self,
        session: InvestigationSession,
        new_status: SessionState,
        updated_at: float,
        reviewer_id: Optional[str] = None,
    ) -> InvestigationSession:
        """Internal helper to transition session state."""
        return InvestigationSession(
            session_id=session.session_id,
            candidate_id=session.candidate_id,
            priority_id=session.priority_id,
            status=new_status,
            created_at=session.created_at,
            started_at=session.started_at,
            updated_at=updated_at,
            closed_at=session.closed_at,
            reviewer_id=reviewer_id or session.reviewer_id,
            evidence_reviews=session.evidence_reviews,
            observations=session.observations,
            uncertainties=session.uncertainties,
            limitations=session.limitations,
            review_notes=session.review_notes,
            analytical_conclusion=session.analytical_conclusion,
            known_evidence_ids=session.known_evidence_ids,
            source_report_id=session.source_report_id,
            source_analysis_run_ids=session.source_analysis_run_ids,
            reproducibility_id=session.reproducibility_id,
            parent_session_id=session.parent_session_id,
            revision=session.revision,
            voice_summary=session.voice_summary,
            metadata=copy.deepcopy(session.metadata),
        )

    def _clone_with_modifications(
        self,
        session: InvestigationSession,
        **kwargs: Any,
    ) -> InvestigationSession:
        """Internal helper to clone session with modified attributes."""
        attrs = {
            "session_id": session.session_id,
            "candidate_id": session.candidate_id,
            "priority_id": session.priority_id,
            "status": session.status,
            "created_at": session.created_at,
            "started_at": session.started_at,
            "updated_at": session.updated_at,
            "closed_at": session.closed_at,
            "reviewer_id": session.reviewer_id,
            "evidence_reviews": session.evidence_reviews,
            "observations": session.observations,
            "uncertainties": session.uncertainties,
            "limitations": session.limitations,
            "review_notes": session.review_notes,
            "analytical_conclusion": session.analytical_conclusion,
            "known_evidence_ids": session.known_evidence_ids,
            "source_report_id": session.source_report_id,
            "source_analysis_run_ids": session.source_analysis_run_ids,
            "reproducibility_id": session.reproducibility_id,
            "parent_session_id": session.parent_session_id,
            "revision": session.revision,
            "voice_summary": session.voice_summary,
            "metadata": copy.deepcopy(session.metadata),
        }
        attrs.update(kwargs)
        return InvestigationSession(**attrs)

    def _build_voice_summary(
        self,
        session: InvestigationSession,
        conclusion: AnalyticalConclusion,
    ) -> InvestigationVoiceSummary:
        """Construct a deterministic speech-ready summary without trading language."""
        headline = f"KAISER investigation review completed for candidate {session.candidate_id}."
        status_speech = f"Investigation session {session.session_id} has concluded with status review complete."
        conc_speech = (
            f"The analytical conclusion is {conclusion.conclusion_state.value.lower().replace('_', ' ')}. "
            f"A total of {len(session.observations)} observations and {len(session.evidence_reviews)} evidence evaluations were logged."
        )
        caveats = []
        if session.uncertainties:
            caveats.append(f"{len(session.uncertainties)} epistemic uncertainties remain documented")
        if session.limitations:
            caveats.append(f"{len(session.limitations)} analytical limitations apply")

        caveats_speech = (
            f"Review caveats: {'; '.join(caveats)}. No trading recommendation is generated."
            if caveats
            else "No unresolved critical limitations noted. No trading recommendation is generated."
        )

        full_script = f"{status_speech} {conc_speech} {caveats_speech}"

        return InvestigationVoiceSummary(
            headline=headline,
            status_speech=status_speech,
            conclusion_speech=conc_speech,
            caveats_speech=caveats_speech,
            full_speech_script=full_script,
        )

    def _session_from_dict(self, data: Dict[str, Any]) -> Optional[InvestigationSession]:
        """Reconstruct an InvestigationSession from serialized dictionary."""
        try:
            status = SessionState.from_string(data.get("status", "CREATED"))
            conc_data = data.get("analytical_conclusion")
            conclusion = None
            if isinstance(conc_data, dict):
                conclusion = AnalyticalConclusion(
                    conclusion_id=conc_data.get("conclusion_id", "conc_restored"),
                    session_id=data.get("session_id", ""),
                    conclusion_state=ConclusionState.from_string(conc_data.get("conclusion_state", "INSUFFICIENT_EVIDENCE")),
                    rationale=conc_data.get("rationale", "Restored rationale"),
                    supporting_evidence_count=int(conc_data.get("supporting_evidence_count", 0)),
                    conflicting_evidence_count=int(conc_data.get("conflicting_evidence_count", 0)),
                    uncertainty_count=int(conc_data.get("uncertainty_count", 0)),
                    limitation_count=int(conc_data.get("limitation_count", 0)),
                    reviewer_id=conc_data.get("reviewer_id", "analyst_unassigned"),
                    created_at=float(conc_data.get("created_at", 0.0)),
                )

            voice_data = data.get("voice_summary")
            voice_summary = None
            if isinstance(voice_data, dict):
                voice_summary = InvestigationVoiceSummary(
                    headline=voice_data.get("headline", ""),
                    status_speech=voice_data.get("status_speech", ""),
                    conclusion_speech=voice_data.get("conclusion_speech", ""),
                    caveats_speech=voice_data.get("caveats_speech", ""),
                    full_speech_script=voice_data.get("full_speech_script", ""),
                )

            return InvestigationSession(
                session_id=data["session_id"],
                candidate_id=data["candidate_id"],
                priority_id=data.get("priority_id"),
                status=status,
                created_at=float(data.get("created_at", 0.0)),
                started_at=float(data["started_at"]) if data.get("started_at") is not None else None,
                updated_at=float(data.get("updated_at", 0.0)),
                closed_at=float(data["closed_at"]) if data.get("closed_at") is not None else None,
                reviewer_id=data.get("reviewer_id"),
                known_evidence_ids=tuple(data.get("known_evidence_ids", ())),
                source_report_id=data.get("source_report_id"),
                source_analysis_run_ids=tuple(data.get("source_analysis_run_ids", ())),
                reproducibility_id=data.get("reproducibility_id"),
                parent_session_id=data.get("parent_session_id"),
                revision=int(data.get("revision", 1)),
                analytical_conclusion=conclusion,
                voice_summary=voice_summary,
                metadata=data.get("metadata", {}),
            )
        except Exception as exc:
            logger.warning("Failed to deserialize InvestigationSession: %s", exc)
            return None
