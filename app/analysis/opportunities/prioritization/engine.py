"""KAISER — Deterministic Opportunity Prioritization Engine.

Evaluates OpportunityCandidate records and prioritizes them for human analytical
investigation based strictly on evidence completeness, data quality, provenance,
reproducibility, and conflict status.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support only: NEVER outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE.
2. Absolutely no order execution fields: Prohibits action, side, order, entry/exit price,
   position size, leverage, stop-loss, take-profit, broker orders, expected profit, or trade probability.
3. No profitability claims: No numeric probability of profit or fake confidence percentage.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
5. Determinism: Identical candidate inputs produce identical priority records and rankings.
6. Traceability: Every priority record references originating candidate, evidence report, and runs.
7. Bounded state: In-memory history and StateStore retention have strict capacity limits.
"""

from __future__ import annotations

import collections
import copy
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
import re
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union
import uuid

from app.analysis.comparison.metrics import safe_float
from app.analysis.evidence.models import EvidenceStatus, EvidenceSynthesisReport
from app.analysis.opportunities.models import (
    ConfidenceState,
    OpportunityCandidate,
    OpportunityState,
    parse_data_quality_status,
)
from app.analysis.opportunities.prioritization.models import (
    DEFAULT_MAX_PRIORITY_HISTORY,
    FORBIDDEN_VOICE_WORDS,
    OpportunityPriorityRecord,
    PriorityState,
    PriorityVoiceSummary,
    validate_no_prohibited_fields,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import (
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)
from app.market.quality.status import DataQualityStatus

logger = logging.getLogger("kaiser.analysis.opportunities.prioritization.engine")

STATE_RANK: Mapping[PriorityState, int] = {
    PriorityState.HIGHER_REVIEW_PRIORITY: 6,
    PriorityState.NORMAL_PRIORITY: 5,
    PriorityState.REVIEW_REQUIRED: 4,
    PriorityState.CONFLICTING_EVIDENCE: 3,
    PriorityState.DATA_QUALITY_LIMITED: 2,
    PriorityState.INSUFFICIENT_EVIDENCE: 1,
    PriorityState.NOT_ELIGIBLE: 0,
}


class OpportunityPrioritizationEngine:
    """Deterministic, thread-safe engine for prioritizing opportunity candidates for human investigation."""

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        clock: Optional[Callable[[], float]] = None,
        max_history: int = DEFAULT_MAX_PRIORITY_HISTORY,
    ) -> None:
        """Initialize the Opportunity Prioritization Engine.

        Args:
            event_bus: Optional EventBus for telemetry notifications.
            state_store: Optional StateStore for persisting bounded state.
            clock: Time provider function (defaults to time.time).
            max_history: Maximum capacity of in-memory priority history.
        """
        self.event_bus = event_bus
        self.state_store = state_store
        self.clock = clock or time.time
        self.max_history = max(1, min(1000, max_history))

        self._lock = threading.RLock()
        self._history: collections.deque[OpportunityPriorityRecord] = collections.deque(
            maxlen=self.max_history
        )
        self._cache: Dict[str, OpportunityPriorityRecord] = {}

    def prioritize_candidate(
        self,
        candidate: Union[OpportunityCandidate, Dict[str, Any]],
        report: Optional[Union[EvidenceSynthesisReport, Dict[str, Any]]] = None,
        priority_id: Optional[str] = None,
        investigation_priority: int = 1,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> OpportunityPriorityRecord:
        """Prioritize a single OpportunityCandidate for human analytical review.

        Args:
            candidate: OpportunityCandidate instance or serialized dictionary.
            report: Optional EvidenceSynthesisReport for auxiliary cross-reference.
            priority_id: Optional explicit priority ID.
            investigation_priority: Rank in investigation queue (default 1).
            metadata: Optional auxiliary metadata.

        Returns:
            OpportunityPriorityRecord: Immutable prioritization record.

        Raises:
            ValidationError: If input is invalid or contains prohibited execution fields.
        """
        now = self.clock()

        try:
            # 1. Validate prohibited fields in inputs
            if isinstance(candidate, (dict, Mapping)):
                validate_no_prohibited_fields(candidate, path="candidate")
            if isinstance(report, (dict, Mapping)):
                validate_no_prohibited_fields(report, path="report")
            if metadata:
                validate_no_prohibited_fields(metadata, path="metadata")

            # 2. Parse candidate if dictionary or Mapping
            cand_obj: OpportunityCandidate
            if isinstance(candidate, OpportunityCandidate):
                cand_obj = candidate
            elif isinstance(candidate, (dict, Mapping)):
                c_dict = dict(candidate)
                # Ensure nested enums are parsed safely
                cand_obj = OpportunityCandidate(
                    candidate_id=str(c_dict.get("candidate_id", "")),
                    symbol=str(c_dict.get("symbol", "")),
                    timeframe=str(c_dict.get("timeframe", "")),
                    assessment_state=OpportunityState.from_string(c_dict.get("assessment_state", "")),
                    evidence_status=EvidenceStatus.from_string(c_dict.get("evidence_status", "")),
                    evidence_completeness_score=float(c_dict.get("evidence_completeness_score", 0.0)),
                    confidence_state=ConfidenceState.from_string(c_dict.get("confidence_state", "")),
                    source_evidence_report_id=str(c_dict.get("source_evidence_report_id", "")),
                    generated_at=float(c_dict.get("generated_at", now)),
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

            # Validate numerical properties of candidate
            if math.isnan(cand_obj.evidence_completeness_score) or math.isinf(cand_obj.evidence_completeness_score):
                raise ValidationError("Candidate evidence_completeness_score cannot be NaN or Infinity.")

            # 3. Extract factual evidence attributes
            dq_state = (
                cand_obj.data_quality_status.value
                if cand_obj.data_quality_status
                else "UNVERIFIED"
            )
            prov_state = cand_obj.provenance_status or "UNVERIFIED"
            repro_state = cand_obj.reproducibility_status or "UNVERIFIED"

            supporting_count = len(cand_obj.supporting_evidence_refs)
            conflicting_count = len(cand_obj.conflicting_evidence_refs)
            uncertainty_count = len(cand_obj.uncertainty_refs)
            limitation_count = len(cand_obj.limitation_refs)

            missing_count = 0
            if report is not None:
                if isinstance(report, dict):
                    missing_count = len(report.get("missing_evidence", []))
                elif hasattr(report, "missing_evidence"):
                    missing_count = len(report.missing_evidence)

            conflict_state = (
                "CONFLICTING_OBSERVATIONS_DETECTED"
                if (conflicting_count > 0 or cand_obj.has_conflicts)
                else "NO_CONFLICTS"
            )
            uncertainty_state = (
                "UNCERTAINTIES_PRESENT"
                if uncertainty_count > 0
                else "NO_UNCERTAINTIES"
            )

            # Historical sample coverage classification
            count: Optional[int] = None
            if cand_obj.sample_coverage:
                cov = cand_obj.sample_coverage
                count = getattr(cov, "record_count", None)
                if count is None:
                    count = getattr(cov, "bar_count", None)
            if count is None and cand_obj.observation_window:
                count = cand_obj.observation_window.record_count

            if count is not None:
                if count >= 100:
                    historical_coverage_state = "ADEQUATE_COVERAGE"
                elif count >= 30:
                    historical_coverage_state = "MODERATE_COVERAGE"
                else:
                    historical_coverage_state = "SPARSE_COVERAGE"
            else:
                historical_coverage_state = "UNKNOWN_COVERAGE"

            # 4. Deterministic Priority State & Reasons Evaluation
            priority_state: PriorityState
            p_reasons: List[str] = []
            dp_reasons: List[str] = []

            # Rule A: Invalidated or non-eligible
            if cand_obj.is_invalidated or cand_obj.assessment_state == OpportunityState.INVALIDATED:
                priority_state = PriorityState.NOT_ELIGIBLE
                dp_reasons.append("Opportunity candidate has been explicitly invalidated.")
            # Rule B: Data quality degraded or invalid
            elif (
                cand_obj.assessment_state == OpportunityState.DATA_QUALITY_LIMITED
                or (cand_obj.data_quality_status and cand_obj.data_quality_status.value in ("INVALID", "DEGRADED"))
                or dq_state in ("INVALID", "DEGRADED")
            ):
                priority_state = PriorityState.DATA_QUALITY_LIMITED
                dp_reasons.append(f"Market data quality is limited ({dq_state}); empirical basis is compromised.")
            # Rule C: Conflicting evidence
            elif conflicting_count > 0 or cand_obj.assessment_state == OpportunityState.EVIDENCE_CONFLICTING:
                priority_state = PriorityState.CONFLICTING_EVIDENCE
                dp_reasons.append(f"Detected {conflicting_count} conflicting evidence observations across timeframes or indicators.")
            # Rule D: Insufficient evidence
            elif (
                cand_obj.assessment_state == OpportunityState.INSUFFICIENT_EVIDENCE
                or cand_obj.evidence_status in (EvidenceStatus.UNAVAILABLE, EvidenceStatus.STALE)
                or cand_obj.evidence_completeness_score < 0.20
            ):
                priority_state = PriorityState.INSUFFICIENT_EVIDENCE
                dp_reasons.append("Available evidence is insufficient, stale, or unavailable for analytical investigation.")
            # Rule E: Review required (uncertainties, limitations, or partial coverage)
            elif (
                uncertainty_count > 0
                or limitation_count > 0
                or historical_coverage_state == "SPARSE_COVERAGE"
                or cand_obj.assessment_state in (OpportunityState.REQUIRES_REVIEW, OpportunityState.EVIDENCE_PARTIAL)
            ):
                priority_state = PriorityState.REVIEW_REQUIRED
                p_reasons.append("Sufficient empirical observations present to justify investigation.")
                if uncertainty_count > 0:
                    dp_reasons.append(f"{uncertainty_count} epistemic uncertainties require verification.")
                if limitation_count > 0:
                    dp_reasons.append(f"{limitation_count} model or data limitations documented.")
                if historical_coverage_state == "SPARSE_COVERAGE":
                    dp_reasons.append("Historical sample coverage is sparse (< 30 bars).")
            # Rule F: Higher review priority
            elif (
                cand_obj.assessment_state == OpportunityState.ANALYTICALLY_SUPPORTED
                and cand_obj.confidence_state == ConfidenceState.HIGH
                and prov_state == "AVAILABLE"
                and repro_state == "AVAILABLE"
                and dq_state == "VALID"
                and cand_obj.evidence_completeness_score >= 0.70
            ):
                priority_state = PriorityState.HIGHER_REVIEW_PRIORITY
                p_reasons.append("High evidence completeness across technical and historical observations.")
                p_reasons.append("Market data provenance verified and auditable.")
                p_reasons.append("Analysis run reproducibility verified.")
                p_reasons.append("Clean market data quality verified.")
                p_reasons.append("Zero unresolved evidence conflicts.")
            # Rule G: Normal priority
            else:
                priority_state = PriorityState.NORMAL_PRIORITY
                p_reasons.append("Evidence completeness sufficient for standard human investigation queue.")

            # 5. Deterministic Evidence Priority Score Calculation
            if priority_state == PriorityState.NOT_ELIGIBLE:
                score = 0.0
            else:
                base_score = cand_obj.evidence_completeness_score
                adj = 0.0
                if prov_state == "AVAILABLE":
                    adj += 0.10
                if repro_state == "AVAILABLE":
                    adj += 0.10
                if dq_state == "VALID":
                    adj += 0.10
                if conflicting_count == 0:
                    adj += 0.05
                else:
                    adj -= 0.30

                if dq_state in ("INVALID", "DEGRADED"):
                    adj -= 0.25
                if uncertainty_count > 0:
                    adj -= 0.10
                if limitation_count > 0:
                    adj -= 0.05
                if historical_coverage_state == "SPARSE_COVERAGE":
                    adj -= 0.10

                raw_score = base_score + adj
                score = max(0.0, min(1.0, raw_score))
                score = round(score, 4)

            # Numerical safety checks
            if math.isnan(score) or math.isinf(score):
                raise ValidationError(f"Computed evidence_priority_score is invalid: {score}")

            # 6. Deterministic Voice Summary
            voice_summary = self._build_voice_summary(
                symbol=cand_obj.symbol,
                timeframe=cand_obj.timeframe,
                priority_state=priority_state,
                score=score,
                p_reasons=p_reasons,
                dp_reasons=dp_reasons,
            )

            # 7. Construct Immutable Priority Record
            p_id = priority_id or f"prio_{uuid.uuid4().hex[:12]}"
            clean_metadata = dict(metadata or {})
            clean_metadata["source_candidate_state"] = cand_obj.assessment_state.value
            clean_metadata["source_confidence_state"] = cand_obj.confidence_state.value

            record = OpportunityPriorityRecord(
                priority_id=p_id,
                candidate_id=cand_obj.candidate_id,
                symbol=cand_obj.symbol,
                timeframe=cand_obj.timeframe,
                investigation_priority=max(1, investigation_priority),
                priority_state=priority_state,
                evidence_completeness=cand_obj.evidence_completeness_score,
                evidence_priority_score=score,
                data_quality_state=dq_state,
                provenance_state=prov_state,
                reproducibility_state=repro_state,
                conflict_state=conflict_state,
                uncertainty_state=uncertainty_state,
                historical_coverage_state=historical_coverage_state,
                supporting_evidence_count=supporting_count,
                conflicting_evidence_count=conflicting_count,
                missing_evidence_count=missing_count,
                limitation_count=limitation_count,
                priority_reasons=tuple(p_reasons),
                deprioritization_reasons=tuple(dp_reasons),
                source_report_id=cand_obj.source_evidence_report_id,
                source_analysis_run_ids=cand_obj.source_analysis_run_ids,
                reproducibility_id=cand_obj.reproducibility_id,
                generated_at=now,
                voice_summary=voice_summary,
                metadata=clean_metadata,
            )

            # 8. Record caching, history retention, EventBus publication, and StateStore persistence
            with self._lock:
                self._cache[record.priority_id] = record
                self._cache[f"cand_{record.candidate_id}"] = record
                self._history.append(record)

            self._publish_priority_assigned_event(record)
            self._persist_state(record)

            return record

        except ValidationError:
            self._publish_priority_failed_event(str(candidate), "ValidationError")
            raise
        except Exception as exc:
            logger.error("Error prioritizing opportunity candidate: %s", exc, exc_info=True)
            self._publish_priority_failed_event(str(candidate), str(exc))
            raise ValidationError(f"Prioritization failed: {str(exc)}")

    def prioritize_candidates(
        self,
        candidates: Sequence[Union[OpportunityCandidate, Dict[str, Any]]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[OpportunityPriorityRecord]:
        """Deterministically prioritize and rank multiple OpportunityCandidate records.

        Ranking order rules (descending investigation urgency):
        1. PriorityState rank (HIGHER_REVIEW_PRIORITY down to NOT_ELIGIBLE)
        2. evidence_priority_score descending
        3. evidence_completeness descending
        4. generated_at descending
        5. candidate_id ascending (strict deterministic tie-breaker)

        Args:
            candidates: Sequence of OpportunityCandidate instances or dicts.
            metadata: Optional shared metadata.

        Returns:
            List[OpportunityPriorityRecord]: Ranked list with sequential investigation_priority (1..N).
        """
        if not candidates:
            return []

        # First pass: evaluate each candidate standalone
        records: List[OpportunityPriorityRecord] = []
        for cand in candidates:
            rec = self.prioritize_candidate(
                candidate=cand,
                investigation_priority=1,
                metadata=metadata,
            )
            records.append(rec)

        # Sort deterministically
        def sort_key(r: OpportunityPriorityRecord) -> Tuple[int, float, float, float, str]:
            state_val = STATE_RANK.get(r.priority_state, 0)
            return (
                -state_val,
                -r.evidence_priority_score,
                -r.evidence_completeness,
                -r.generated_at,
                r.candidate_id,  # ascending tie-breaker
            )

        sorted_records = sorted(records, key=sort_key)

        # Second pass: assign ranked investigation_priority (1, 2, 3...)
        ranked_records: List[OpportunityPriorityRecord] = []
        for rank_idx, r in enumerate(sorted_records, start=1):
            if r.investigation_priority == rank_idx:
                ranked_records.append(r)
            else:
                # Re-freeze with updated rank
                updated = OpportunityPriorityRecord(
                    priority_id=r.priority_id,
                    candidate_id=r.candidate_id,
                    symbol=r.symbol,
                    timeframe=r.timeframe,
                    investigation_priority=rank_idx,
                    priority_state=r.priority_state,
                    evidence_completeness=r.evidence_completeness,
                    evidence_priority_score=r.evidence_priority_score,
                    data_quality_state=r.data_quality_state,
                    provenance_state=r.provenance_state,
                    reproducibility_state=r.reproducibility_state,
                    conflict_state=r.conflict_state,
                    uncertainty_state=r.uncertainty_state,
                    historical_coverage_state=r.historical_coverage_state,
                    supporting_evidence_count=r.supporting_evidence_count,
                    conflicting_evidence_count=r.conflicting_evidence_count,
                    missing_evidence_count=r.missing_evidence_count,
                    limitation_count=r.limitation_count,
                    priority_reasons=r.priority_reasons,
                    deprioritization_reasons=r.deprioritization_reasons,
                    source_report_id=r.source_report_id,
                    source_analysis_run_ids=r.source_analysis_run_ids,
                    reproducibility_id=r.reproducibility_id,
                    generated_at=r.generated_at,
                    voice_summary=r.voice_summary,
                    metadata=dict(r.metadata),
                )
                with self._lock:
                    self._cache[updated.priority_id] = updated
                    self._cache[f"cand_{updated.candidate_id}"] = updated
                ranked_records.append(updated)

        return ranked_records

    def get_priority(self, identifier: str) -> Optional[OpportunityPriorityRecord]:
        """Retrieve priority record by priority_id or candidate_id."""
        if not identifier:
            return None
        clean_id = str(identifier).strip()
        with self._lock:
            # Check direct priority_id
            if clean_id in self._cache:
                return self._cache[clean_id]
            # Check cand_<candidate_id>
            cand_key = f"cand_{clean_id}"
            if cand_key in self._cache:
                return self._cache[cand_key]
        return None

    def list_priorities(
        self,
        limit: int = 50,
        offset: int = 0,
        state: Optional[Union[str, PriorityState]] = None,
    ) -> List[OpportunityPriorityRecord]:
        """List historical priority records with optional filtering and pagination."""
        filter_state: Optional[PriorityState] = None
        if state:
            filter_state = PriorityState.from_string(state)

        with self._lock:
            items = list(self._history)

        if filter_state:
            items = [r for r in items if r.priority_state == filter_state]

        items.reverse()  # Latest first
        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)
        return items[clamped_offset : clamped_offset + clamped_limit]

    def _build_voice_summary(
        self,
        symbol: str,
        timeframe: str,
        priority_state: PriorityState,
        score: float,
        p_reasons: Sequence[str],
        dp_reasons: Sequence[str],
    ) -> PriorityVoiceSummary:
        """Deterministically build speech text strictly avoiding forbidden recommendation words."""
        sym_clean = sanitize_provenance_string(symbol)
        tf_clean = sanitize_provenance_string(timeframe)
        headline = f"KAISER Investigation Queue: {sym_clean} {tf_clean}"

        if priority_state == PriorityState.HIGHER_REVIEW_PRIORITY:
            p_speech = "Higher review priority assigned based on complete and verified empirical evidence."
        elif priority_state == PriorityState.NORMAL_PRIORITY:
            p_speech = "Normal investigation priority assigned for standard analytical review."
        elif priority_state == PriorityState.REVIEW_REQUIRED:
            p_speech = "Review required priority assigned due to documented uncertainties or limitations."
        elif priority_state == PriorityState.CONFLICTING_EVIDENCE:
            p_speech = "Conflicting evidence priority assigned. Conflicting observations require manual resolution."
        elif priority_state == PriorityState.DATA_QUALITY_LIMITED:
            p_speech = "Data quality limited. Market data limitations prevent higher investigation priority."
        elif priority_state == PriorityState.INSUFFICIENT_EVIDENCE:
            p_speech = "Insufficient evidence. Minimal analytical observations are available."
        else:
            p_speech = "Not eligible. Analytical candidate has been invalidated or corrupted."

        r_speech = (
            f"Key observations: {p_reasons[0]}"
            if p_reasons
            else "Standard empirical criteria evaluated."
        )

        c_speech = (
            f"Caveats: {dp_reasons[0]}"
            if dp_reasons
            else "Investigation decision support only. No trading directives generated."
        )

        full_script = (
            f"KAISER investigation assessment for {sym_clean} on {tf_clean}. "
            f"{p_speech} {r_speech} {c_speech}"
        )

        # Enforce safety check against forbidden words
        script_lower = full_script.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            if re.search(r"\b" + re.escape(forbidden) + r"\b", script_lower):
                # Fallback to ultra-safe deterministic speech script
                full_script = (
                    f"KAISER investigation assessment for {sym_clean} on {tf_clean}. "
                    f"Evidence completeness score is {score:.2f}. "
                    f"Human analytical review is required. No trading directives generated."
                )
                break

        return PriorityVoiceSummary(
            headline=headline,
            priority_speech=p_speech,
            reasons_speech=r_speech,
            caveats_speech=c_speech,
            full_speech_script=full_script,
        )

    def _publish_priority_assigned_event(self, record: OpportunityPriorityRecord) -> None:
        """Publish sanitized priority assigned event to EventBus."""
        if not self.event_bus:
            return
        payload = {
            "priority_id": record.priority_id,
            "candidate_id": record.candidate_id,
            "symbol": record.symbol,
            "timeframe": record.timeframe,
            "priority_state": record.priority_state.value,
            "investigation_priority": record.investigation_priority,
            "evidence_priority_score": record.evidence_priority_score,
            "source_report_id": record.source_report_id,
            "generated_at": record.generated_at,
        }
        try:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.OPPORTUNITY_PRIORITY_ASSIGNED,
                    source="OpportunityPrioritizationEngine",
                    payload=payload,
                )
            )
        except Exception as exc:
            logger.warning("Failed to publish priority assigned event: %s", exc)

    def _publish_priority_failed_event(self, candidate_repr: str, reason: str) -> None:
        """Publish sanitized priority failed event to EventBus."""
        if not self.event_bus:
            return
        payload = {
            "candidate_identifier": sanitize_provenance_string(candidate_repr[:64]),
            "error_reason": sanitize_provenance_string(reason[:128]),
            "timestamp": self.clock(),
        }
        try:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.OPPORTUNITY_PRIORITY_FAILED,
                    source="OpportunityPrioritizationEngine",
                    payload=payload,
                )
            )
        except Exception as exc:
            logger.warning("Failed to publish priority failed event: %s", exc)

    def _persist_state(self, record: OpportunityPriorityRecord) -> None:
        """Persist bounded priority record state to StateStore."""
        if not self.state_store:
            return
        try:
            r_dict = record.to_dict()
            self.state_store.set("opportunity_priority:latest", r_dict)

            history: List[Dict[str, Any]] = self.state_store.get("opportunity_priority:history", [])
            if not isinstance(history, list):
                history = []
            history.append(r_dict)
            if len(history) > self.max_history:
                history = history[-self.max_history :]
            self.state_store.set("opportunity_priority:history", history)
        except Exception as exc:
            logger.warning("Failed to persist priority state: %s", exc)
