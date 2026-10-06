"""KAISER — Thread-Safe Bounded Opportunity Investigation Queue.

Maintains a deterministic, strictly ordered queue of OpportunityCandidates prioritized
for human analytical investigation.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support only: Strictly prioritizes candidates for human analytical investigation.
2. No trade execution: Prohibits order placement, trade routing, or execution operations.
3. Thread-safety: All mutations and queries are protected by re-entrant mutex locks.
4. Bounded capacity: Prevents unbounded memory growth by enforcing strict capacity limits.
5. Deterministic tie-breaking: Stable ordering guaranteed across re-rankings.
"""

from __future__ import annotations

import collections
import copy
import logging
import threading
from collections.abc import Mapping
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from app.analysis.evidence.models import EvidenceStatus, EvidenceSynthesisReport
from app.analysis.opportunities.models import (
    ConfidenceState,
    OpportunityCandidate,
    OpportunityState,
    parse_data_quality_status,
    validate_no_prohibited_fields,
)
from app.analysis.opportunities.prioritization.engine import (
    OpportunityPrioritizationEngine,
)
from app.analysis.opportunities.prioritization.models import (
    DEFAULT_MAX_QUEUE_CAPACITY,
    OpportunityPriorityRecord,
    PriorityState,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import sanitize_provenance_string

logger = logging.getLogger("kaiser.analysis.opportunities.prioritization.queue")


class OpportunityInvestigationQueue:
    """Thread-safe, bounded, deterministic investigation queue for opportunity candidates."""

    def __init__(
        self,
        engine: OpportunityPrioritizationEngine,
        max_capacity: int = DEFAULT_MAX_QUEUE_CAPACITY,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        """Initialize the Opportunity Investigation Queue.

        Args:
            engine: OpportunityPrioritizationEngine for evaluating candidates.
            max_capacity: Maximum number of candidates retained in queue.
            event_bus: Optional EventBus for telemetry notifications.
            state_store: Optional StateStore for persistence.
            clock: Time provider function (defaults to time.time).
        """
        self.engine = engine
        self.max_capacity = max(1, min(1000, max_capacity))
        self.event_bus = event_bus
        self.state_store = state_store
        self.clock = clock or time.time

        self._lock = threading.RLock()
        self._candidates: Dict[str, OpportunityCandidate] = {}
        self._records_by_id: Dict[str, OpportunityPriorityRecord] = {}
        self._ordered_records: List[OpportunityPriorityRecord] = []

    def size(self) -> int:
        """Return the current number of prioritized candidates in the queue."""
        with self._lock:
            return len(self._ordered_records)

    def is_empty(self) -> bool:
        """Return True if the queue contains zero candidates."""
        with self._lock:
            return len(self._ordered_records) == 0

    def add_candidate(
        self,
        candidate: Union[OpportunityCandidate, Dict[str, Any], Mapping[str, Any]],
        report: Optional[Union[EvidenceSynthesisReport, Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> OpportunityPriorityRecord:
        """Add or update an OpportunityCandidate in the investigation queue.

        Re-ranks all queued candidates deterministically and emits a queue change event.

        Args:
            candidate: OpportunityCandidate instance, dictionary, or Mapping.
            report: Optional EvidenceSynthesisReport.
            metadata: Optional metadata.

        Returns:
            OpportunityPriorityRecord: Prioritized record of the added candidate.

        Raises:
            ValidationError: If inputs are invalid or contain prohibited execution fields.
        """
        # Validate prohibited fields upfront
        if isinstance(candidate, (dict, Mapping)):
            validate_no_prohibited_fields(candidate, path="candidate")
        if metadata:
            validate_no_prohibited_fields(metadata, path="metadata")

        with self._lock:
            # 1. Resolve candidate object
            cand_obj: OpportunityCandidate
            if isinstance(candidate, OpportunityCandidate):
                cand_obj = candidate
            elif isinstance(candidate, (dict, Mapping)):
                c_dict = dict(candidate)
                cand_obj = OpportunityCandidate(
                    candidate_id=str(c_dict.get("candidate_id", "")),
                    symbol=str(c_dict.get("symbol", "")),
                    timeframe=str(c_dict.get("timeframe", "")),
                    assessment_state=OpportunityState.from_string(c_dict.get("assessment_state", "")),
                    evidence_status=EvidenceStatus.from_string(c_dict.get("evidence_status", "")),
                    evidence_completeness_score=float(c_dict.get("evidence_completeness_score", 0.0)),
                    confidence_state=ConfidenceState.from_string(c_dict.get("confidence_state", "")),
                    source_evidence_report_id=str(c_dict.get("source_evidence_report_id", "")),
                    generated_at=float(c_dict.get("generated_at", self.clock())),
                    data_quality_status=parse_data_quality_status(c_dict.get("data_quality_status")),
                    provenance_status=str(c_dict.get("provenance_status", "UNVERIFIED")),
                    reproducibility_status=str(c_dict.get("reproducibility_status", "UNVERIFIED")),
                    supporting_evidence_refs=tuple(c_dict.get("supporting_evidence_refs", ())),
                    conflicting_evidence_refs=tuple(c_dict.get("conflicting_evidence_refs", ())),
                    uncertainty_refs=tuple(c_dict.get("uncertainty_refs", ())),
                    limitation_refs=tuple(c_dict.get("limitation_refs", ())),
                    key_observations=tuple(c_dict.get("key_observations", ())),
                    invalidation_conditions=tuple(c_dict.get("invalidation_conditions", ())),
                    source_analysis_run_ids=tuple(c_dict.get("source_analysis_run_ids", ())),
                    reproducibility_id=c_dict.get("reproducibility_id"),
                    metadata=c_dict.get("metadata", {}),
                )
            else:
                raise ValidationError("Candidate must be an OpportunityCandidate or dict.")

            # Check capacity limits for new candidates
            if (
                cand_obj.candidate_id not in self._candidates
                and len(self._candidates) >= self.max_capacity
            ):
                # Capacity reached: evaluate if new candidate outranks lowest
                test_rec = self.engine.prioritize_candidate(cand_obj, report=report, metadata=metadata)
                lowest = self._ordered_records[-1] if self._ordered_records else None
                if lowest and test_rec.evidence_priority_score <= lowest.evidence_priority_score:
                    # Lowest ranks higher or equal; do not enqueue to preserve bounds
                    return test_rec
                elif lowest:
                    # Evict lowest
                    self._candidates.pop(lowest.candidate_id, None)

            self._candidates[cand_obj.candidate_id] = cand_obj
            self._re_rank_locked()

            record = self._records_by_id[cand_obj.candidate_id]
            self._notify_queue_changed_locked()
            return record

    def update_candidate(
        self,
        candidate: OpportunityCandidate,
    ) -> Optional[OpportunityPriorityRecord]:
        """Update an existing candidate in the queue if present.

        Returns:
            Updated OpportunityPriorityRecord if candidate was in queue, None otherwise.
        """
        with self._lock:
            if candidate.candidate_id not in self._candidates:
                return None
            self._candidates[candidate.candidate_id] = candidate
            self._re_rank_locked()
            self._notify_queue_changed_locked()
            return self._records_by_id.get(candidate.candidate_id)

    def remove_candidate(self, candidate_id: str) -> bool:
        """Remove a candidate from the queue by candidate_id.

        Returns:
            True if candidate was removed, False if not present.
        """
        if not candidate_id:
            return False
        clean_id = str(candidate_id).strip()
        with self._lock:
            if clean_id not in self._candidates:
                return False
            del self._candidates[clean_id]
            self._re_rank_locked()
            self._notify_queue_changed_locked()
            return True

    def get_next(self) -> Optional[OpportunityPriorityRecord]:
        """Peek at the highest-priority candidate (rank 1) without removing it from queue."""
        with self._lock:
            if not self._ordered_records:
                return None
            return self._ordered_records[0]

    def list_queue(
        self,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[OpportunityPriorityRecord]:
        """Return a snapshot of queued records in ranked investigation priority order."""
        with self._lock:
            records = list(self._ordered_records)

        clamped_offset = max(0, offset)
        if limit is not None:
            clamped_limit = max(1, min(1000, limit))
            return records[clamped_offset : clamped_offset + clamped_limit]
        return records[clamped_offset:]

    def get(self, candidate_id: str) -> Optional[OpportunityPriorityRecord]:
        """Retrieve priority record for candidate_id from current queue."""
        if not candidate_id:
            return None
        clean_id = str(candidate_id).strip()
        with self._lock:
            return self._records_by_id.get(clean_id)

    def invalidate_candidate(
        self,
        candidate_id: str,
        reason: str = "Candidate invalidated",
    ) -> Optional[OpportunityPriorityRecord]:
        """Mark a queued candidate as invalidated and re-rank the queue.

        Moves the invalidated candidate to NOT_ELIGIBLE.
        """
        if not candidate_id:
            return None
        clean_id = str(candidate_id).strip()
        with self._lock:
            cand = self._candidates.get(clean_id)
            if not cand:
                return None

            # Create invalidated copy
            invalidated_cand = OpportunityCandidate(
                candidate_id=cand.candidate_id,
                symbol=cand.symbol,
                timeframe=cand.timeframe,
                assessment_state=OpportunityState.INVALIDATED,
                evidence_status=cand.evidence_status,
                evidence_completeness_score=0.0,
                confidence_state=cand.confidence_state,
                source_evidence_report_id=cand.source_evidence_report_id,
                generated_at=cand.generated_at,
                data_quality_status=cand.data_quality_status,
                provenance_status=cand.provenance_status,
                reproducibility_status=cand.reproducibility_status,
                observation_window=cand.observation_window,
                sample_coverage=cand.sample_coverage,
                supporting_evidence_refs=cand.supporting_evidence_refs,
                conflicting_evidence_refs=cand.conflicting_evidence_refs,
                uncertainty_refs=cand.uncertainty_refs,
                limitation_refs=cand.limitation_refs,
                key_observations=cand.key_observations,
                invalidation_conditions=(
                    cand.invalidation_conditions + (f"Invalidated: {reason}",)
                ),
                source_analysis_run_ids=cand.source_analysis_run_ids,
                reproducibility_id=cand.reproducibility_id,
                metadata={**cand.metadata, "invalidation_reason": reason},
            )

            self._candidates[clean_id] = invalidated_cand
            self._re_rank_locked()
            self._notify_queue_changed_locked()
            return self._records_by_id.get(clean_id)

    def clear(self) -> None:
        """Clear all candidates from the queue."""
        with self._lock:
            self._candidates.clear()
            self._records_by_id.clear()
            self._ordered_records.clear()
            self._notify_queue_changed_locked()

    def _re_rank_locked(self) -> None:
        """Re-rank all queued candidates deterministically. Caller must hold _lock."""
        cand_list = list(self._candidates.values())
        ranked = self.engine.prioritize_candidates(cand_list)
        self._ordered_records = ranked
        self._records_by_id = {r.candidate_id: r for r in ranked}

    def _notify_queue_changed_locked(self) -> None:
        """Publish OPPORTUNITY_QUEUE_CHANGED event and persist StateStore. Caller must hold _lock."""
        now = self.clock()
        queue_size = len(self._ordered_records)
        top = self._ordered_records[0] if self._ordered_records else None

        # 1. Publish EventBus event
        if self.event_bus:
            payload = {
                "queue_size": queue_size,
                "top_candidate_id": top.candidate_id if top else None,
                "top_priority_state": top.priority_state.value if top else None,
                "top_priority_score": top.evidence_priority_score if top else None,
                "timestamp": now,
            }
            try:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.OPPORTUNITY_QUEUE_CHANGED,
                        source="OpportunityInvestigationQueue",
                        payload=payload,
                    )
                )
            except Exception as exc:
                logger.warning("Failed to publish queue changed event: %s", exc)

        # 2. Persist StateStore
        if self.state_store:
            try:
                serialized = [r.to_dict() for r in self._ordered_records]
                self.state_store.set("opportunity_queue:latest", serialized)
            except Exception as exc:
                logger.warning("Failed to persist opportunity queue state: %s", exc)
