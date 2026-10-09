"""KAISER — Consolidated Investigation Review Evolution & Historical Consistency Service.

Phase 2, Step 32: Deterministic historical tracking service for recording how investigation
evidence, discrepancy resolution, review coverage, and analytical consistency change over time.

CORE ARCHITECTURAL INVARIANT:
"This historical review layer records how investigation evidence,
reconciliation, review coverage, and analytical consistency change over
time. It describes historical evolution only. It does not determine
correctness, rank investigations, predict profitability, or recommend
trading actions."
Settings.execution_locked remains True.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.analysis.opportunities.consolidation.models import (
    ConsolidatedInvestigationOutcome,
    ConsolidationState,
)
from app.analysis.opportunities.consistency.models import (
    ConsolidatedOutcomeComparison,
)
from app.analysis.opportunities.evolution.models import (
    FORBIDDEN_VOICE_WORDS,
    MAX_EVOLUTION_PER_INVESTIGATION,
    MAX_GLOBAL_HISTORY,
    MAX_SNAPSHOTS_PER_INVESTIGATION,
    SAFE_ID_PATTERN,
    ChangeClassification,
    ConflictLifecycleState,
    DataQualityEvolutionState,
    EvolutionDimensionName,
    EvolutionDimensionRecord,
    HistoricalReviewSnapshot,
    HistoricalSnapshotLifecycleState,
    RecurringReviewConflict,
    ReproducibilityEvolutionState,
    ReviewCoverageEvolutionState,
    ReviewEvolutionRecord,
    ReviewEvolutionVoiceSummary,
    _unfreeze,
    compute_deterministic_evolution_id,
    compute_deterministic_snapshot_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore

logger = logging.getLogger("kaiser.analysis.opportunities.evolution.service")


class HistoricalReviewEvolutionService:
    """Deterministic analytical service for tracking review evolution over time.

    Manages immutable snapshots and produces auditable delta records without
    ranking, profitability scoring, or trade recommendation logic.
    """

    def __init__(
        self,
        state_store: Optional[StateStore] = None,
        event_bus: Optional[EventBus] = None,
        consolidation_service: Optional[Any] = None,
        consistency_service: Optional[Any] = None,
        max_snapshots_per_investigation: int = MAX_SNAPSHOTS_PER_INVESTIGATION,
        max_evolution_per_investigation: int = MAX_EVOLUTION_PER_INVESTIGATION,
        max_global_history: int = MAX_GLOBAL_HISTORY,
        name: str = "historical_review_evolution_service",
    ) -> None:
        self.state_store = state_store
        self.event_bus = event_bus
        self.consolidation_service = consolidation_service
        self.consistency_service = consistency_service

        self.max_snapshots_per_investigation = max(5, min(200, max_snapshots_per_investigation))
        self.max_evolution_per_investigation = max(5, min(200, max_evolution_per_investigation))
        self.max_global_history = max(10, min(1000, max_global_history))
        self.name = name

        self._lock = threading.RLock()
        self._snapshots: Dict[str, HistoricalReviewSnapshot] = {}
        self._snapshots_by_investigation: Dict[str, List[str]] = {}
        self._evolutions: Dict[str, ReviewEvolutionRecord] = {}
        self._evolutions_by_investigation: Dict[str, List[str]] = {}
        self._snapshot_global_history: List[str] = []
        self._evolution_global_history: List[str] = []

    # ==============================================================================
    # Snapshot Capture Methods
    # ==============================================================================

    def capture_snapshot_from_outcome(
        self,
        outcome: ConsolidatedInvestigationOutcome,
        snapshot_version: Optional[int] = None,
        correlation_id: Optional[str] = None,
    ) -> HistoricalReviewSnapshot:
        """Capture an immutable historical review snapshot from a ConsolidatedInvestigationOutcome."""
        if not outcome or not isinstance(outcome, ConsolidatedInvestigationOutcome):
            raise ValidationError("Valid ConsolidatedInvestigationOutcome is required.")

        investigation_id = outcome.investigation_id
        outcome_id = outcome.outcome_id

        with self._lock:
            if snapshot_version is None:
                existing = self._snapshots_by_investigation.get(investigation_id, [])
                snapshot_version = len(existing) + 1

        # Extract structured states
        source_state = {
            "outcome_state": (
                outcome.outcome_state.value
                if hasattr(outcome.outcome_state, "value")
                else str(outcome.outcome_state)
            ),
            "source_report_ids": list(outcome.source_report_ids),
            "source_comparison_ids": list(outcome.source_comparison_ids),
            "source_resolution_case_ids": list(outcome.source_resolution_case_ids),
        }

        facts_list = [f.to_dict() if hasattr(f, "to_dict") else dict(f) for f in outcome.facts]
        interpretations_list = [
            i.to_dict() if hasattr(i, "to_dict") else dict(i) for i in outcome.interpretations
        ]
        evidence_state = {
            "evidence_summary": dict(outcome.evidence_summary) if outcome.evidence_summary else {},
            "facts": facts_list,
            "interpretations": interpretations_list,
            "fact_count": len(facts_list),
            "interpretation_count": len(interpretations_list),
        }

        review_decisions_list = [
            d.to_dict() if hasattr(d, "to_dict") else dict(d) for d in outcome.human_review_decisions
        ]
        review_state = {
            "human_review_decisions": review_decisions_list,
            "decision_count": len(review_decisions_list),
        }

        conflicts_list = [c.to_dict() if hasattr(c, "to_dict") else dict(c) for c in outcome.conflicts]
        conflict_state = {
            "conflicts": conflicts_list,
            "resolved_items": list(outcome.resolved_items),
            "unresolved_items": list(outcome.unresolved_items),
            "conflict_count": len(conflicts_list),
        }

        qs = outcome.quality_summary
        quality_state = {
            "overall_quality_state": getattr(qs, "overall_quality_state", "VALID"),
            "source_quality_statuses": (
                dict(getattr(qs, "source_quality_statuses", {}))
                if getattr(qs, "source_quality_statuses", None)
                else {}
            ),
            "stale_sources": list(getattr(qs, "stale_sources", ())),
            "missing_sources": list(getattr(qs, "missing_sources", ())),
            "degraded_sources": list(getattr(qs, "degraded_sources", ())),
            "invalid_sources": list(getattr(qs, "invalid_sources", ())),
        }

        provenance_state = {
            "provenance_summary": dict(outcome.provenance_summary) if outcome.provenance_summary else {}
        }

        rs = outcome.reproducibility_summary
        reproducibility_state = (
            rs.to_dict() if hasattr(rs, "to_dict") else (dict(rs) if rs else {})
        )

        # Coverage state categorical deduction
        unresolved_count = len(outcome.unresolved_items)
        decisions_count = len(outcome.human_review_decisions)
        evidence_total = len(outcome.facts) + len(outcome.interpretations)
        if unresolved_count == 0 and decisions_count > 0:
            cov_status = ReviewCoverageEvolutionState.COMPLETE
        elif decisions_count > 0 or evidence_total > 0:
            cov_status = ReviewCoverageEvolutionState.PARTIAL
        elif evidence_total == 0 and decisions_count == 0:
            cov_status = ReviewCoverageEvolutionState.NONE
        else:
            cov_status = ReviewCoverageEvolutionState.UNKNOWN

        coverage_state = {
            "coverage_status": cov_status.value,
            "reviewed_evidence_count": decisions_count,
            "unresolved_discrepancies_count": unresolved_count,
            "evidence_total_count": evidence_total,
            "provenance_verified": bool(outcome.provenance_summary),
            "reproducibility_checked": bool(
                getattr(rs, "reproducible_sources", ())
            ),
            "quality_checked": bool(getattr(qs, "source_quality_statuses", {})),
        }

        limitation_state = tuple(
            l.to_dict() if hasattr(l, "to_dict") else dict(l) for l in outcome.limitations
        )
        source_refs = tuple(outcome.source_report_ids)

        # Generate deterministic fingerprint of source components
        fingerprint_raw = json.dumps(
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
        source_fingerprint = hashlib.sha256(fingerprint_raw).hexdigest()[:16]

        snapshot_id = compute_deterministic_snapshot_id(
            investigation_id=investigation_id,
            outcome_id=outcome_id,
            comparison_id=None,
            snapshot_version=snapshot_version,
            source_state_fingerprint=source_fingerprint,
        )

        snapshot = HistoricalReviewSnapshot(
            snapshot_id=snapshot_id,
            investigation_id=investigation_id,
            outcome_id=outcome_id,
            comparison_id=None,
            snapshot_version=snapshot_version,
            captured_at=time.time(),
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
            generated_from="CONSOLIDATED_INVESTIGATION_OUTCOME",
        )

        return self.store_snapshot(snapshot, correlation_id=correlation_id)

    def capture_snapshot_from_comparison(
        self,
        comparison: ConsolidatedOutcomeComparison,
        snapshot_version: Optional[int] = None,
        correlation_id: Optional[str] = None,
    ) -> HistoricalReviewSnapshot:
        """Capture an immutable historical review snapshot from a ConsolidatedOutcomeComparison."""
        if not comparison or not isinstance(comparison, ConsolidatedOutcomeComparison):
            raise ValidationError("Valid ConsolidatedOutcomeComparison is required.")

        investigation_id = (
            comparison.investigation_ids[0]
            if comparison.investigation_ids
            else "investigation_cross"
        )
        outcome_id = (
            comparison.outcome_ids[0]
            if comparison.outcome_ids
            else "outcome_cross"
        )
        comparison_id = comparison.comparison_id

        with self._lock:
            if snapshot_version is None:
                existing = self._snapshots_by_investigation.get(investigation_id, [])
                snapshot_version = len(existing) + 1

        source_state = {
            "comparison_scope": comparison.comparison_scope,
            "comparability_state": (
                comparison.comparability_state.value
                if hasattr(comparison.comparability_state, "value")
                else str(comparison.comparability_state)
            ),
            "outcome_ids": list(comparison.outcome_ids),
            "investigation_ids": list(comparison.investigation_ids),
        }

        evidence_state = {
            "evidence_consistency": [
                ec.to_dict() if hasattr(ec, "to_dict") else dict(ec)
                for ec in comparison.evidence_consistency
            ],
            "fact_consistency": [
                fc.to_dict() if hasattr(fc, "to_dict") else dict(fc)
                for fc in comparison.fact_consistency
            ],
            "interpretation_consistency": [
                ic.to_dict() if hasattr(ic, "to_dict") else dict(ic)
                for ic in comparison.interpretation_consistency
            ],
            "divergence_items": [
                dv.to_dict() if hasattr(dv, "to_dict") else dict(dv)
                for dv in comparison.divergence_items
            ],
        }

        review_state = {
            "human_review_consistency": [
                hc.to_dict() if hasattr(hc, "to_dict") else dict(hc)
                for hc in comparison.human_review_consistency
            ]
        }

        conflict_state = {
            "conflict_summary": dict(comparison.conflict_summary) if comparison.conflict_summary else {}
        }

        quality_state = {
            "quality_comparison": dict(comparison.quality_comparison) if comparison.quality_comparison else {}
        }

        provenance_state = {
            "provenance_comparison": dict(comparison.provenance_comparison) if comparison.provenance_comparison else {}
        }

        reproducibility_state = {
            "reproducibility_comparison": (
                dict(comparison.reproducibility_comparison)
                if comparison.reproducibility_comparison
                else {}
            )
        }

        rc = comparison.review_coverage
        coverage_state = (
            rc.to_dict() if hasattr(rc, "to_dict") else (dict(rc) if rc else {})
        )

        limitation_state = tuple(
            l.to_dict() if hasattr(l, "to_dict") else dict(l)
            for l in comparison.limitations
        )
        source_refs = tuple(comparison.outcome_ids)

        fingerprint_raw = json.dumps(
            {
                "source_state": source_state,
                "evidence_state": evidence_state,
                "review_state": review_state,
                "conflict_state": conflict_state,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        source_fingerprint = hashlib.sha256(fingerprint_raw).hexdigest()[:16]

        snapshot_id = compute_deterministic_snapshot_id(
            investigation_id=investigation_id,
            outcome_id=outcome_id,
            comparison_id=comparison_id,
            snapshot_version=snapshot_version,
            source_state_fingerprint=source_fingerprint,
        )

        snapshot = HistoricalReviewSnapshot(
            snapshot_id=snapshot_id,
            investigation_id=investigation_id,
            outcome_id=outcome_id,
            comparison_id=comparison_id,
            snapshot_version=snapshot_version,
            captured_at=time.time(),
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
            generated_from="CONSOLIDATED_OUTCOME_COMPARISON",
        )

        return self.store_snapshot(snapshot, correlation_id=correlation_id)

    def capture_snapshot(
        self,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> HistoricalReviewSnapshot:
        """Generic capture of an immutable snapshot from a dictionary specification."""
        if not isinstance(payload, dict):
            raise ValidationError("Payload must be a dictionary.")

        validate_no_prohibited_fields(payload)

        investigation_id = payload.get("investigation_id")
        outcome_id = payload.get("outcome_id")
        if not investigation_id:
            raise ValidationError("Parameter 'investigation_id' is required.")
        if not outcome_id:
            raise ValidationError("Parameter 'outcome_id' is required.")

        comparison_id = payload.get("comparison_id")

        with self._lock:
            snapshot_version = payload.get("snapshot_version")
            if snapshot_version is None:
                existing = self._snapshots_by_investigation.get(investigation_id, [])
                snapshot_version = len(existing) + 1

        source_state = dict(payload.get("source_state", {}))
        evidence_state = dict(payload.get("evidence_state", {}))
        review_state = dict(payload.get("review_state", {}))
        conflict_state = dict(payload.get("conflict_state", {}))
        quality_state = dict(payload.get("quality_state", {}))
        provenance_state = dict(payload.get("provenance_state", {}))
        reproducibility_state = dict(payload.get("reproducibility_state", {}))
        coverage_state = dict(payload.get("coverage_state", {}))
        limitation_state = tuple(payload.get("limitation_state", ()))
        source_references = tuple(payload.get("source_references", ()))
        generated_from = payload.get("generated_from", "CONSOLIDATED_INVESTIGATION_OUTCOME")

        fingerprint_raw = json.dumps(
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
        source_fingerprint = hashlib.sha256(fingerprint_raw).hexdigest()[:16]

        snapshot_id = payload.get("snapshot_id") or compute_deterministic_snapshot_id(
            investigation_id=investigation_id,
            outcome_id=outcome_id,
            comparison_id=comparison_id,
            snapshot_version=snapshot_version,
            source_state_fingerprint=source_fingerprint,
        )

        snapshot = HistoricalReviewSnapshot(
            snapshot_id=snapshot_id,
            investigation_id=investigation_id,
            outcome_id=outcome_id,
            comparison_id=comparison_id,
            snapshot_version=snapshot_version,
            captured_at=float(payload.get("captured_at", time.time())),
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
            source_references=source_references,
            generated_from=generated_from,
        )

        return self.store_snapshot(snapshot, correlation_id=correlation_id)

    # ==============================================================================
    # Storage and Retrieval
    # ==============================================================================

    def store_snapshot(
        self,
        snapshot: HistoricalReviewSnapshot,
        correlation_id: Optional[str] = None,
    ) -> HistoricalReviewSnapshot:
        """Store snapshot with append-only immutability and bounded capacity."""
        if not isinstance(snapshot, HistoricalReviewSnapshot):
            raise ValidationError("Expected HistoricalReviewSnapshot instance.")

        corr_id = correlation_id or f"corr_snap_{int(time.time() * 1000)}"

        with self._lock:
            # If already exists, enforce idempotent immutability
            if snapshot.snapshot_id in self._snapshots:
                existing = self._snapshots[snapshot.snapshot_id]
                if existing.snapshot_id != snapshot.snapshot_id:
                    raise ValidationError(
                        f"Collision or mismatch for snapshot '{snapshot.snapshot_id}'."
                    )
                return existing

            # Check bounded capacity for investigation
            inv_list = self._snapshots_by_investigation.setdefault(snapshot.investigation_id, [])
            if len(inv_list) >= self.max_snapshots_per_investigation:
                # Ring-buffer behavior: discard oldest from investigation tracking
                oldest_id = inv_list.pop(0)
                # Keep in snapshots dictionary unless global bound is exceeded

            inv_list.append(snapshot.snapshot_id)
            self._snapshots[snapshot.snapshot_id] = snapshot
            self._snapshot_global_history.append(snapshot.snapshot_id)

            # Global bound check
            if len(self._snapshot_global_history) > self.max_global_history:
                discarded_id = self._snapshot_global_history.pop(0)
                self._snapshots.pop(discarded_id, None)

            # Persist to StateStore
            self._persist_snapshot_to_statestore(snapshot)

        # Emit events
        if self.event_bus:
            try:
                if snapshot.lifecycle_state == HistoricalSnapshotLifecycleState.FAILED:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.HISTORICAL_REVIEW_SNAPSHOT_FAILED,
                            source=self.name,
                            payload={
                                "snapshot_id": snapshot.snapshot_id,
                                "investigation_id": snapshot.investigation_id,
                                "lifecycle_state": snapshot.lifecycle_state.value,
                            },
                            correlation_id=corr_id,
                        )
                    )
                else:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.HISTORICAL_REVIEW_SNAPSHOT_CAPTURED,
                            source=self.name,
                            payload={
                                "snapshot_id": snapshot.snapshot_id,
                                "investigation_id": snapshot.investigation_id,
                                "outcome_id": snapshot.outcome_id,
                                "snapshot_version": snapshot.snapshot_version,
                            },
                            correlation_id=corr_id,
                        )
                    )
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.HISTORICAL_REVIEW_SNAPSHOT_STORED,
                            source=self.name,
                            payload={
                                "snapshot_id": snapshot.snapshot_id,
                                "investigation_id": snapshot.investigation_id,
                                "lifecycle_state": snapshot.lifecycle_state.value,
                            },
                            correlation_id=corr_id,
                        )
                    )
            except Exception as ev_err:
                logger.warning("Failed to publish snapshot event: %s", ev_err)

        return snapshot

    def get_snapshot(self, snapshot_id: str) -> Optional[HistoricalReviewSnapshot]:
        """Retrieve historical review snapshot by ID."""
        if not snapshot_id or not SAFE_ID_PATTERN.match(snapshot_id):
            return None

        with self._lock:
            snap = self._snapshots.get(snapshot_id)
            if snap is not None:
                return snap

        if self.state_store:
            stored = self.state_store.get(f"historical_review:{snapshot_id}", None)
            if isinstance(stored, dict):
                try:
                    return HistoricalReviewSnapshot(
                        snapshot_id=stored["snapshot_id"],
                        investigation_id=stored["investigation_id"],
                        outcome_id=stored["outcome_id"],
                        comparison_id=stored.get("comparison_id"),
                        snapshot_version=stored.get("snapshot_version", 1),
                        captured_at=stored.get("captured_at", 0.0),
                        lifecycle_state=HistoricalSnapshotLifecycleState(
                            stored.get("lifecycle_state", "STORED")
                        ),
                        source_state=stored.get("source_state", {}),
                        evidence_state=stored.get("evidence_state", {}),
                        review_state=stored.get("review_state", {}),
                        conflict_state=stored.get("conflict_state", {}),
                        quality_state=stored.get("quality_state", {}),
                        provenance_state=stored.get("provenance_state", {}),
                        reproducibility_state=stored.get("reproducibility_state", {}),
                        coverage_state=stored.get("coverage_state", {}),
                        limitation_state=tuple(stored.get("limitation_state", ())),
                        source_references=tuple(stored.get("source_references", ())),
                        generated_from=stored.get("generated_from", "CONSOLIDATED_INVESTIGATION_OUTCOME"),
                    )
                except Exception as exc:
                    logger.warning("Error deserializing snapshot from StateStore: %s", exc)

        return None

    def list_snapshots(
        self,
        investigation_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[HistoricalReviewSnapshot]:
        """List historical snapshots with pagination and optional investigation filtering."""
        limit_clamped = max(1, min(100, limit))
        offset_clamped = max(0, offset)

        with self._lock:
            if investigation_id:
                ids = self._snapshots_by_investigation.get(investigation_id, [])
            else:
                ids = self._snapshot_global_history

            selected_ids = ids[offset_clamped : offset_clamped + limit_clamped]
            results: List[HistoricalReviewSnapshot] = []
            for sid in selected_ids:
                snap = self._snapshots.get(sid)
                if snap:
                    results.append(snap)
            return results

    # ==============================================================================
    # Evolution Comparison Engine
    # ==============================================================================

    def compare_snapshots(
        self,
        previous_snapshot_id: str,
        current_snapshot_id: str,
        correlation_id: Optional[str] = None,
    ) -> ReviewEvolutionRecord:
        """Compare two historical review snapshots across all 12 dimensions deterministically."""
        corr_id = correlation_id or f"corr_evol_{int(time.time() * 1000)}"

        prev = self.get_snapshot(previous_snapshot_id)
        if not prev:
            raise ValidationError(
                f"Previous snapshot '{previous_snapshot_id}' not found."
            )
        curr = self.get_snapshot(current_snapshot_id)
        if not curr:
            raise ValidationError(
                f"Current snapshot '{current_snapshot_id}' not found."
            )

        evolution_id = compute_deterministic_evolution_id(
            previous_snapshot_id=prev.snapshot_id,
            current_snapshot_id=curr.snapshot_id,
        )

        with self._lock:
            if evolution_id in self._evolutions:
                return self._evolutions[evolution_id]

        # Independent tracking across 12 dimensions
        dimension_records: List[EvolutionDimensionRecord] = []
        added_evidence: List[str] = []
        removed_evidence: List[str] = []
        modified_evidence: List[str] = []
        newly_resolved_discrepancies: List[str] = []
        newly_open_discrepancies: List[str] = []
        changed_review_decisions: List[str] = []
        changed_quality: List[str] = []
        changed_provenance: List[str] = []
        changed_reproducibility: List[str] = []
        changed_coverage: List[str] = []
        changed_limitations: List[str] = []

        # 1. Dimension: EVIDENCE
        dim_ev = self._diff_evidence_dimension(prev, curr)
        dimension_records.append(dim_ev)
        added_evidence.extend(dim_ev.current_state.get("added_items", []))
        removed_evidence.extend(dim_ev.previous_state.get("removed_items", []))
        modified_evidence.extend(dim_ev.current_state.get("modified_items", []))

        # 2. Dimension: FACTS
        dim_facts = self._diff_facts_dimension(prev, curr)
        dimension_records.append(dim_facts)

        # 3. Dimension: INTERPRETATIONS
        dim_interp = self._diff_interpretations_dimension(prev, curr)
        dimension_records.append(dim_interp)

        # 4. Dimension: HUMAN_REVIEW_DECISIONS
        dim_hrd = self._diff_human_decisions_dimension(prev, curr)
        dimension_records.append(dim_hrd)
        changed_review_decisions.extend(dim_hrd.current_state.get("changed_decision_ids", []))

        # 5. Dimension: DISCREPANCIES
        dim_disc = self._diff_discrepancies_dimension(prev, curr)
        dimension_records.append(dim_disc)
        newly_resolved_discrepancies.extend(dim_disc.current_state.get("newly_resolved", []))
        newly_open_discrepancies.extend(dim_disc.current_state.get("newly_open", []))

        # 6. Dimension: RECONCILIATIONS
        dim_rec = self._diff_reconciliations_dimension(prev, curr)
        dimension_records.append(dim_rec)

        # 7. Dimension: CONFLICTS
        dim_conf = self._diff_conflicts_dimension(prev, curr)
        dimension_records.append(dim_conf)

        # 8. Dimension: DATA_QUALITY
        dim_qual = self._diff_quality_dimension(prev, curr)
        dimension_records.append(dim_qual)
        if dim_qual.change_classification != ChangeClassification.UNCHANGED:
            changed_quality.append(dim_qual.explanation)

        # 9. Dimension: PROVENANCE
        dim_prov = self._diff_provenance_dimension(prev, curr)
        dimension_records.append(dim_prov)
        if dim_prov.change_classification != ChangeClassification.UNCHANGED:
            changed_provenance.append(dim_prov.explanation)

        # 10. Dimension: REPRODUCIBILITY
        dim_repro = self._diff_reproducibility_dimension(prev, curr)
        dimension_records.append(dim_repro)
        if dim_repro.change_classification != ChangeClassification.UNCHANGED:
            changed_reproducibility.append(dim_repro.explanation)

        # 11. Dimension: REVIEW_COVERAGE
        dim_cov = self._diff_coverage_dimension(prev, curr)
        dimension_records.append(dim_cov)
        if dim_cov.change_classification != ChangeClassification.UNCHANGED:
            changed_coverage.append(dim_cov.explanation)

        # 12. Dimension: LIMITATIONS
        dim_lim = self._diff_limitations_dimension(prev, curr)
        dimension_records.append(dim_lim)
        if dim_lim.change_classification != ChangeClassification.UNCHANGED:
            changed_limitations.append(dim_lim.explanation)

        # Overall Change State Determination
        changed_dims_count = sum(
            1 for d in dimension_records if d.change_classification != ChangeClassification.UNCHANGED
        )
        if changed_dims_count == 0:
            overall_change_state = ChangeClassification.UNCHANGED
        elif newly_resolved_discrepancies and not newly_open_discrepancies:
            overall_change_state = ChangeClassification.RESOLVED
        elif newly_open_discrepancies:
            overall_change_state = ChangeClassification.REOPENED
        elif added_evidence and not removed_evidence:
            overall_change_state = ChangeClassification.ADDED
        elif removed_evidence and not added_evidence:
            overall_change_state = ChangeClassification.REMOVED
        else:
            overall_change_state = ChangeClassification.MODIFIED

        # Generate Voice-Safe Summary
        voice_summary_text = self._generate_voice_summary_text(
            prev=prev,
            curr=curr,
            overall_state=overall_change_state,
            newly_resolved=newly_resolved_discrepancies,
            dim_cov=dim_cov,
            dim_conf=dim_conf,
            dim_ev=dim_ev,
            dim_prov=dim_prov,
        )

        voice_summary = ReviewEvolutionVoiceSummary(
            evolution_id=evolution_id,
            previous_snapshot_id=prev.snapshot_id,
            current_snapshot_id=curr.snapshot_id,
            summary_text=voice_summary_text,
            voice_safe=True,
        )

        traceability = {
            "previous_version": prev.snapshot_version,
            "current_version": curr.snapshot_version,
            "previous_captured_at": prev.captured_at,
            "current_captured_at": curr.captured_at,
            "investigation_id": curr.investigation_id,
            "changed_dimension_count": changed_dims_count,
        }

        record = ReviewEvolutionRecord(
            evolution_id=evolution_id,
            previous_snapshot_id=prev.snapshot_id,
            current_snapshot_id=curr.snapshot_id,
            investigation_id=curr.investigation_id,
            generated_at=time.time(),
            change_state=overall_change_state,
            changed_dimensions=tuple(dimension_records),
            added_evidence=tuple(added_evidence),
            removed_evidence=tuple(removed_evidence),
            modified_evidence=tuple(modified_evidence),
            newly_resolved_discrepancies=tuple(newly_resolved_discrepancies),
            newly_open_discrepancies=tuple(newly_open_discrepancies),
            changed_review_decisions=tuple(changed_review_decisions),
            changed_quality=tuple(changed_quality),
            changed_provenance=tuple(changed_provenance),
            changed_reproducibility=tuple(changed_reproducibility),
            changed_coverage=tuple(changed_coverage),
            changed_limitations=tuple(changed_limitations),
            traceability=traceability,
            voice_summary=voice_summary,
        )

        return self.store_evolution(record, correlation_id=corr_id)

    def store_evolution(
        self,
        record: ReviewEvolutionRecord,
        correlation_id: Optional[str] = None,
    ) -> ReviewEvolutionRecord:
        """Store review evolution record in memory and StateStore."""
        if not isinstance(record, ReviewEvolutionRecord):
            raise ValidationError("Expected ReviewEvolutionRecord instance.")

        corr_id = correlation_id or f"corr_evol_{int(time.time() * 1000)}"

        with self._lock:
            if record.evolution_id in self._evolutions:
                return self._evolutions[record.evolution_id]

            inv_list = self._evolutions_by_investigation.setdefault(record.investigation_id, [])
            if len(inv_list) >= self.max_evolution_per_investigation:
                inv_list.pop(0)

            inv_list.append(record.evolution_id)
            self._evolutions[record.evolution_id] = record
            self._evolution_global_history.append(record.evolution_id)

            if len(self._evolution_global_history) > self.max_global_history:
                discarded_id = self._evolution_global_history.pop(0)
                self._evolutions.pop(discarded_id, None)

            self._persist_evolution_to_statestore(record)

        # Emit events
        if self.event_bus:
            try:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.REVIEW_EVOLUTION_DETECTED,
                        source=self.name,
                        payload={
                            "evolution_id": record.evolution_id,
                            "previous_snapshot_id": record.previous_snapshot_id,
                            "current_snapshot_id": record.current_snapshot_id,
                            "investigation_id": record.investigation_id,
                            "change_state": record.change_state.value,
                        },
                        correlation_id=corr_id,
                    )
                )

                if record.changed_coverage:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.REVIEW_COVERAGE_CHANGED,
                            source=self.name,
                            payload={
                                "evolution_id": record.evolution_id,
                                "investigation_id": record.investigation_id,
                                "details": list(record.changed_coverage),
                            },
                            correlation_id=corr_id,
                        )
                    )

                if record.changed_provenance:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.REVIEW_PROVENANCE_CHANGED,
                            source=self.name,
                            payload={
                                "evolution_id": record.evolution_id,
                                "investigation_id": record.investigation_id,
                                "details": list(record.changed_provenance),
                            },
                            correlation_id=corr_id,
                        )
                    )
            except Exception as ev_err:
                logger.warning("Failed to publish evolution events: %s", ev_err)

        return record

    def get_evolution(self, evolution_id: str) -> Optional[ReviewEvolutionRecord]:
        """Retrieve review evolution record by ID."""
        if not evolution_id or not SAFE_ID_PATTERN.match(evolution_id):
            return None

        with self._lock:
            rec = self._evolutions.get(evolution_id)
            if rec is not None:
                return rec

        if self.state_store:
            stored = self.state_store.get(f"historical_review_evolution:{evolution_id}", None)
            if isinstance(stored, dict):
                try:
                    dims = []
                    for d in stored.get("changed_dimensions", []):
                        dims.append(
                            EvolutionDimensionRecord(
                                dimension_name=EvolutionDimensionName(d["dimension_name"]),
                                previous_state=d.get("previous_state"),
                                current_state=d.get("current_state"),
                                change_classification=ChangeClassification(d["change_classification"]),
                                source_references=tuple(d.get("source_references", ())),
                                explanation=d.get("explanation", ""),
                            )
                        )
                    vs_dict = stored.get("voice_summary")
                    vs_obj = None
                    if isinstance(vs_dict, dict):
                        vs_obj = ReviewEvolutionVoiceSummary(
                            evolution_id=vs_dict["evolution_id"],
                            previous_snapshot_id=vs_dict["previous_snapshot_id"],
                            current_snapshot_id=vs_dict["current_snapshot_id"],
                            summary_text=vs_dict["summary_text"],
                            voice_safe=vs_dict.get("voice_safe", True),
                        )
                    return ReviewEvolutionRecord(
                        evolution_id=stored["evolution_id"],
                        previous_snapshot_id=stored["previous_snapshot_id"],
                        current_snapshot_id=stored["current_snapshot_id"],
                        investigation_id=stored["investigation_id"],
                        generated_at=float(stored.get("generated_at", 0.0)),
                        change_state=ChangeClassification(stored.get("change_state", "UNCHANGED")),
                        changed_dimensions=tuple(dims),
                        added_evidence=tuple(stored.get("added_evidence", ())),
                        removed_evidence=tuple(stored.get("removed_evidence", ())),
                        modified_evidence=tuple(stored.get("modified_evidence", ())),
                        newly_resolved_discrepancies=tuple(stored.get("newly_resolved_discrepancies", ())),
                        newly_open_discrepancies=tuple(stored.get("newly_open_discrepancies", ())),
                        changed_review_decisions=tuple(stored.get("changed_review_decisions", ())),
                        changed_quality=tuple(stored.get("changed_quality", ())),
                        changed_provenance=tuple(stored.get("changed_provenance", ())),
                        changed_reproducibility=tuple(stored.get("changed_reproducibility", ())),
                        changed_coverage=tuple(stored.get("changed_coverage", ())),
                        changed_limitations=tuple(stored.get("changed_limitations", ())),
                        traceability=stored.get("traceability", {}),
                        voice_summary=vs_obj,
                    )
                except Exception as exc:
                    logger.warning("Error deserializing evolution from StateStore: %s", exc)

        return None

    def list_evolution_history(
        self,
        investigation_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ReviewEvolutionRecord]:
        """List evolution history records with pagination."""
        limit_clamped = max(1, min(100, limit))
        offset_clamped = max(0, offset)

        with self._lock:
            if investigation_id:
                ids = self._evolutions_by_investigation.get(investigation_id, [])
            else:
                ids = self._evolution_global_history

            selected_ids = ids[offset_clamped : offset_clamped + limit_clamped]
            results: List[ReviewEvolutionRecord] = []
            for eid in selected_ids:
                rec = self._evolutions.get(eid)
                if rec:
                    results.append(rec)
            return results

    # ==============================================================================
    # Specialized Analytical Query Methods
    # ==============================================================================

    def get_recurring_review_conflicts(
        self,
        investigation_id: str,
    ) -> List[RecurringReviewConflict]:
        """Identify conflicts recurring across multiple snapshots descriptively."""
        if not investigation_id or not SAFE_ID_PATTERN.match(investigation_id):
            raise ValidationError(f"Invalid investigation_id: '{investigation_id}'")

        snapshots = self.list_snapshots(investigation_id=investigation_id, limit=100)
        conflict_map: Dict[str, Dict[str, Any]] = {}

        for snap in snapshots:
            c_state = _unfreeze(snap.conflict_state)
            conflicts = c_state.get("conflicts", [])
            for c in conflicts:
                cid = c.get("conflict_id") or c.get("id") or str(c)
                lineage_items = (
                    c.get("conflicting_evidence_ids")
                    or c.get("evidence_lineage")
                    or c.get("involved_sources")
                    or []
                )
                if cid not in conflict_map:
                    conflict_map[cid] = {
                        "conflict_id": cid,
                        "original_discrepancy_id": c.get("discrepancy_id") or c.get("original_discrepancy_id"),
                        "evidence_lineage": set(lineage_items),
                        "observed_snapshot_ids": [],
                        "status": c.get("conflict_state", "ACTIVE"),
                        "notes": c.get("description", "Identified conflict"),
                    }
                else:
                    conflict_map[cid]["evidence_lineage"].update(lineage_items)
                conflict_map[cid]["observed_snapshot_ids"].append(snap.snapshot_id)

        recurring: List[RecurringReviewConflict] = []
        for cid, data in conflict_map.items():
            count = len(data["observed_snapshot_ids"])
            status = (
                ConflictLifecycleState.ACTIVE
                if count > 1
                else ConflictLifecycleState.NEWLY_DETECTED
            )
            rec = RecurringReviewConflict(
                conflict_id=cid,
                original_discrepancy_id=data["original_discrepancy_id"],
                evidence_lineage=tuple(sorted(data["evidence_lineage"])),
                occurrence_count=count,
                observed_snapshot_ids=tuple(data["observed_snapshot_ids"]),
                lifecycle_status=status,
                descriptive_note=f"Observed across {count} review snapshot(s).",
            )
            recurring.append(rec)

            if count > 1 and self.event_bus:
                try:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.REVIEW_CONFLICT_RECURRED,
                            source=self.name,
                            payload={
                                "investigation_id": investigation_id,
                                "conflict_id": cid,
                                "occurrence_count": count,
                            },
                        )
                    )
                except Exception:
                    pass

        return recurring

    def get_review_coverage_evolution(
        self,
        investigation_id: str,
    ) -> List[Dict[str, Any]]:
        """Retrieve chronological evolution of review coverage for an investigation."""
        if not investigation_id or not SAFE_ID_PATTERN.match(investigation_id):
            raise ValidationError(f"Invalid investigation_id: '{investigation_id}'")

        snapshots = self.list_snapshots(investigation_id=investigation_id, limit=100)
        history: List[Dict[str, Any]] = []
        for snap in snapshots:
            cov = _unfreeze(snap.coverage_state)
            history.append({
                "snapshot_id": snap.snapshot_id,
                "snapshot_version": snap.snapshot_version,
                "captured_at": snap.captured_at,
                "coverage_status": cov.get("coverage_status", "UNKNOWN"),
                "reviewed_evidence_count": cov.get("reviewed_evidence_count", 0),
                "unresolved_discrepancies_count": cov.get("unresolved_discrepancies_count", 0),
                "provenance_verified": cov.get("provenance_verified", False),
                "reproducibility_checked": cov.get("reproducibility_checked", False),
                "quality_checked": cov.get("quality_checked", False),
            })
        return history

    def get_provenance_evolution(
        self,
        investigation_id: str,
    ) -> List[Dict[str, Any]]:
        """Retrieve chronological provenance evolution for an investigation."""
        if not investigation_id or not SAFE_ID_PATTERN.match(investigation_id):
            raise ValidationError(f"Invalid investigation_id: '{investigation_id}'")

        snapshots = self.list_snapshots(investigation_id=investigation_id, limit=100)
        history: List[Dict[str, Any]] = []
        for snap in snapshots:
            prov = _unfreeze(snap.provenance_state)
            history.append({
                "snapshot_id": snap.snapshot_id,
                "snapshot_version": snap.snapshot_version,
                "captured_at": snap.captured_at,
                "source_references": list(snap.source_references),
                "provenance_summary": prov.get("provenance_summary", {}),
            })
        return history

    # ==============================================================================
    # Dimension Diff Helpers
    # ==============================================================================

    def _diff_evidence_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_ev = _unfreeze(prev.evidence_state)
        curr_ev = _unfreeze(curr.evidence_state)

        prev_keys = set(prev_ev.get("evidence_summary", {}).keys())
        curr_keys = set(curr_ev.get("evidence_summary", {}).keys())

        added = sorted(list(curr_keys - prev_keys))
        removed = sorted(list(prev_keys - curr_keys))

        common = curr_keys.intersection(prev_keys)
        modified = [
            k for k in common if prev_ev.get("evidence_summary", {}).get(k) != curr_ev.get("evidence_summary", {}).get(k)
        ]

        if not added and not removed and not modified:
            classification = ChangeClassification.UNCHANGED
            expl = "Evidence summary items are identical."
        elif added and not removed and not modified:
            classification = ChangeClassification.ADDED
            expl = f"{len(added)} evidence item(s) added."
        elif removed and not added and not modified:
            classification = ChangeClassification.REMOVED
            expl = f"{len(removed)} evidence item(s) removed."
        else:
            classification = ChangeClassification.MODIFIED
            expl = f"Evidence items modified (added: {len(added)}, removed: {len(removed)}, modified: {len(modified)})."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.EVIDENCE,
            previous_state={"total_items": len(prev_keys), "removed_items": removed},
            current_state={
                "total_items": len(curr_keys),
                "added_items": added,
                "modified_items": modified,
            },
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_facts_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_facts = _unfreeze(prev.evidence_state).get("facts", [])
        curr_facts = _unfreeze(curr.evidence_state).get("facts", [])

        prev_ids = {
            f.get("evidence_id") or f.get("item_id") or f.get("id") or str(f)
            if isinstance(f, dict) else str(f)
            for f in prev_facts
        }
        curr_ids = {
            f.get("evidence_id") or f.get("item_id") or f.get("id") or str(f)
            if isinstance(f, dict) else str(f)
            for f in curr_facts
        }

        added = sorted(list(curr_ids - prev_ids))
        removed = sorted(list(prev_ids - curr_ids))

        if not added and not removed:
            classification = ChangeClassification.UNCHANGED
            expl = "Fact statements remain unchanged."
        elif added and not removed:
            classification = ChangeClassification.ADDED
            expl = f"{len(added)} new fact(s) recorded."
        elif removed and not added:
            classification = ChangeClassification.REMOVED
            expl = f"{len(removed)} fact(s) removed from scope."
        else:
            classification = ChangeClassification.MODIFIED
            expl = f"Facts updated (added: {len(added)}, removed: {len(removed)})."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.FACTS,
            previous_state={"fact_count": len(prev_facts), "removed": removed},
            current_state={"fact_count": len(curr_facts), "added": added},
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_interpretations_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_interp = _unfreeze(prev.evidence_state).get("interpretations", [])
        curr_interp = _unfreeze(curr.evidence_state).get("interpretations", [])

        prev_ids = {
            i.get("evidence_id") or i.get("item_id") or i.get("id") or str(i)
            if isinstance(i, dict) else str(i)
            for i in prev_interp
        }
        curr_ids = {
            i.get("evidence_id") or i.get("item_id") or i.get("id") or str(i)
            if isinstance(i, dict) else str(i)
            for i in curr_interp
        }

        added = sorted(list(curr_ids - prev_ids))
        removed = sorted(list(prev_ids - curr_ids))

        if not added and not removed:
            classification = ChangeClassification.UNCHANGED
            expl = "Analytical interpretations remain unchanged."
        elif added and not removed:
            classification = ChangeClassification.ADDED
            expl = f"{len(added)} new interpretation(s) recorded."
        elif removed and not added:
            classification = ChangeClassification.REMOVED
            expl = f"{len(removed)} interpretation(s) removed."
        else:
            classification = ChangeClassification.MODIFIED
            expl = f"Interpretations changed (added: {len(added)}, removed: {len(removed)})."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.INTERPRETATIONS,
            previous_state={"interpretation_count": len(prev_interp), "removed": removed},
            current_state={"interpretation_count": len(curr_interp), "added": added},
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_human_decisions_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_decs = _unfreeze(prev.review_state).get("human_review_decisions", [])
        curr_decs = _unfreeze(curr.review_state).get("human_review_decisions", [])

        prev_map = {d.get("decision_id") or str(idx): d for idx, d in enumerate(prev_decs)}
        curr_map = {d.get("decision_id") or str(idx): d for idx, d in enumerate(curr_decs)}

        added = sorted(list(set(curr_map.keys()) - set(prev_map.keys())))
        removed = sorted(list(set(prev_map.keys()) - set(curr_map.keys())))
        modified = [k for k in set(curr_map.keys()) & set(prev_map.keys()) if curr_map[k] != prev_map[k]]

        if not added and not removed and not modified:
            classification = ChangeClassification.UNCHANGED
            expl = "Human review decisions remain unchanged."
        elif added and not removed and not modified:
            classification = ChangeClassification.ADDED
            expl = f"{len(added)} new review decision(s) logged."
        elif modified:
            classification = ChangeClassification.MODIFIED
            expl = f"Documented review decisions modified ({len(modified)} changed)."
        else:
            classification = ChangeClassification.MODIFIED
            expl = "Review decisions changed."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.HUMAN_REVIEW_DECISIONS,
            previous_state={"decision_count": len(prev_decs)},
            current_state={"decision_count": len(curr_decs), "changed_decision_ids": added + modified},
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_discrepancies_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_c = _unfreeze(prev.conflict_state)
        curr_c = _unfreeze(curr.conflict_state)

        prev_unresolved = set(prev_c.get("unresolved_items", []))
        curr_unresolved = set(curr_c.get("unresolved_items", []))
        prev_resolved = set(prev_c.get("resolved_items", []))
        curr_resolved = set(curr_c.get("resolved_items", []))

        newly_resolved = sorted(list((prev_unresolved - curr_unresolved) | (curr_resolved - prev_resolved)))
        newly_open = sorted(list(curr_unresolved - prev_unresolved))

        if not newly_resolved and not newly_open:
            classification = ChangeClassification.UNCHANGED
            expl = "Discrepancy resolution state is unchanged."
        elif newly_resolved and not newly_open:
            classification = ChangeClassification.RESOLVED
            expl = f"{len(newly_resolved)} discrepancy/discrepancies newly resolved."
        elif newly_open and not newly_resolved:
            classification = ChangeClassification.REOPENED
            expl = f"{len(newly_open)} discrepancy/discrepancies newly identified or reopened."
        else:
            classification = ChangeClassification.MODIFIED
            expl = f"Discrepancies altered (resolved: {len(newly_resolved)}, open: {len(newly_open)})."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.DISCREPANCIES,
            previous_state={"unresolved_count": len(prev_unresolved)},
            current_state={
                "unresolved_count": len(curr_unresolved),
                "newly_resolved": newly_resolved,
                "newly_open": newly_open,
            },
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_reconciliations_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_resolved = set(_unfreeze(prev.conflict_state).get("resolved_items", []))
        curr_resolved = set(_unfreeze(curr.conflict_state).get("resolved_items", []))

        added = sorted(list(curr_resolved - prev_resolved))
        removed = sorted(list(prev_resolved - curr_resolved))

        if not added and not removed:
            classification = ChangeClassification.UNCHANGED
            expl = "Reconciliation records remain unchanged."
        elif added and not removed:
            classification = ChangeClassification.RESOLVED
            expl = f"{len(added)} new evidence reconciliation(s) recorded."
        else:
            classification = ChangeClassification.MODIFIED
            expl = f"Reconciliations updated."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.RECONCILIATIONS,
            previous_state={"reconciled_count": len(prev_resolved)},
            current_state={"reconciled_count": len(curr_resolved), "added": added},
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_conflicts_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_confs = _unfreeze(prev.conflict_state).get("conflicts", [])
        curr_confs = _unfreeze(curr.conflict_state).get("conflicts", [])

        prev_ids = {c.get("conflict_id") or c.get("id") or str(c) for c in prev_confs}
        curr_ids = {c.get("conflict_id") or c.get("id") or str(c) for c in curr_confs}

        newly_detected = sorted(list(curr_ids - prev_ids))
        resolved = sorted(list(prev_ids - curr_ids))

        if prev_confs == curr_confs:
            classification = ChangeClassification.UNCHANGED
            expl = "Active conflict state is unchanged."
        else:
            classification = ChangeClassification.CONFLICT_STATE_CHANGED
            expl = f"Active conflicts changed from {len(prev_confs)} to {len(curr_confs)}."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.CONFLICTS,
            previous_state={
                "conflict_count": len(prev_confs),
                "conflict_ids": sorted(list(prev_ids)),
            },
            current_state={
                "conflict_count": len(curr_confs),
                "conflict_ids": sorted(list(curr_ids)),
                "newly_detected": newly_detected,
                "resolved": resolved,
            },
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_quality_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_q = _unfreeze(prev.quality_state)
        curr_q = _unfreeze(curr.quality_state)

        prev_status = prev_q.get("overall_quality_state", "VALID")
        curr_status = curr_q.get("overall_quality_state", "VALID")

        prev_sources = prev_q.get("source_quality_statuses", {})
        curr_sources = curr_q.get("source_quality_statuses", {})

        source_changes = []
        all_sources = sorted(list(set(prev_sources.keys()) | set(curr_sources.keys())))
        for s in all_sources:
            pst = prev_sources.get(s)
            cst = curr_sources.get(s)
            if pst != cst:
                source_changes.append({
                    "source": s,
                    "previous": pst,
                    "current": cst,
                    "timestamp": curr.captured_at,
                })

        if prev_status == curr_status and not source_changes and prev_q == curr_q:
            classification = ChangeClassification.UNCHANGED
            expl = "Data quality state is unchanged."
        elif prev_status in ("DEGRADED", "STALE", "INVALID") and curr_status == "VALID":
            classification = ChangeClassification.IMPROVED_DATA_AVAILABILITY
            expl = f"Data quality improved from {prev_status} to {curr_status}."
        elif curr_status in ("DEGRADED", "STALE", "INVALID") and prev_status == "VALID":
            classification = ChangeClassification.DEGRADED_DATA_AVAILABILITY
            expl = f"Data quality degraded from {prev_status} to {curr_status}."
        else:
            classification = ChangeClassification.MODIFIED
            expl = f"Data quality state transitioned from {prev_status} to {curr_status}."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.DATA_QUALITY,
            previous_state={"overall_quality_state": prev_status, "source_quality_statuses": prev_sources},
            current_state={
                "overall_quality_state": curr_status,
                "source_quality_statuses": curr_sources,
                "source_quality_changes": source_changes,
            },
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_provenance_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_p = _unfreeze(prev.provenance_state)
        curr_p = _unfreeze(curr.provenance_state)

        prev_sources = set(prev.source_references)
        curr_sources = set(curr.source_references)

        newly_linked = sorted(list(curr_sources - prev_sources))
        missing_provenance = sorted(list(prev_sources - curr_sources))

        if prev_p == curr_p and prev.source_references == curr.source_references:
            classification = ChangeClassification.UNCHANGED
            expl = "Provenance lineage is unchanged."
        else:
            classification = ChangeClassification.PROVENANCE_CHANGED
            expl = "Provenance information was updated."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.PROVENANCE,
            previous_state={
                "sources": list(prev.source_references),
                "provenance_summary": prev_p.get("provenance_summary", {}),
            },
            current_state={
                "sources": list(curr.source_references),
                "newly_linked_provenance": newly_linked,
                "missing_provenance": missing_provenance,
                "provenance_summary": curr_p.get("provenance_summary", {}),
            },
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_reproducibility_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_r = _unfreeze(prev.reproducibility_state)
        curr_r = _unfreeze(curr.reproducibility_state)

        prev_state = prev_r.get("reproducibility_state", "UNKNOWN")
        curr_state = curr_r.get("reproducibility_state", "UNKNOWN")

        prev_rep_sources = set(prev_r.get("reproducible_sources", []))
        curr_rep_sources = set(curr_r.get("reproducible_sources", []))
        prev_nonrep_sources = set(prev_r.get("non_reproducible_sources", []))
        curr_nonrep_sources = set(curr_r.get("non_reproducible_sources", []))

        if prev_state == curr_state and prev_r == curr_r:
            classification = ChangeClassification.UNCHANGED
            expl = "Reproducibility assessment is unchanged."
        else:
            classification = ChangeClassification.REPRODUCIBILITY_CHANGED
            expl = f"Reproducibility state changed from {prev_state} to {curr_state}."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.REPRODUCIBILITY,
            previous_state={
                "reproducibility_state": prev_state,
                "reproducible_sources": sorted(list(prev_rep_sources)),
                "non_reproducible_sources": sorted(list(prev_nonrep_sources)),
            },
            current_state={
                "reproducibility_state": curr_state,
                "reproducible_sources": sorted(list(curr_rep_sources)),
                "non_reproducible_sources": sorted(list(curr_nonrep_sources)),
            },
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_coverage_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_c = _unfreeze(prev.coverage_state)
        curr_c = _unfreeze(curr.coverage_state)

        prev_cov = prev_c.get("coverage_status", "UNKNOWN")
        curr_cov = curr_c.get("coverage_status", "UNKNOWN")

        prev_reviewed = prev_c.get("reviewed_evidence_count", 0)
        curr_reviewed = curr_c.get("reviewed_evidence_count", 0)

        prev_unresolved = prev_c.get("unresolved_discrepancies_count", 0)
        curr_unresolved = curr_c.get("unresolved_discrepancies_count", 0)

        if prev_cov == curr_cov and prev_c == curr_c:
            classification = ChangeClassification.UNCHANGED
            expl = "Review coverage state is unchanged."
        else:
            classification = ChangeClassification.REVIEW_COVERAGE_CHANGED
            expl = f"Review coverage changed from {prev_cov.lower()} to {curr_cov.lower()}."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.REVIEW_COVERAGE,
            previous_state={
                "coverage_status": prev_cov,
                "previously_reviewed_evidence": prev_reviewed,
                "unresolved_discrepancies": prev_unresolved,
                "provenance_verified": prev_c.get("provenance_verified", False),
                "reproducibility_checked": prev_c.get("reproducibility_checked", False),
                "quality_checked": prev_c.get("quality_checked", False),
            },
            current_state={
                "coverage_status": curr_cov,
                "newly_reviewed_evidence": max(0, curr_reviewed - prev_reviewed),
                "newly_unreviewed_evidence": max(0, prev_reviewed - curr_reviewed),
                "unresolved_discrepancies": curr_unresolved,
                "provenance_verified": curr_c.get("provenance_verified", False),
                "reproducibility_checked": curr_c.get("reproducibility_checked", False),
                "quality_checked": curr_c.get("quality_checked", False),
            },
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _diff_limitations_dimension(
        self, prev: HistoricalReviewSnapshot, curr: HistoricalReviewSnapshot
    ) -> EvolutionDimensionRecord:
        prev_lim = _unfreeze(prev.limitation_state)
        curr_lim = _unfreeze(curr.limitation_state)

        if prev_lim == curr_lim:
            classification = ChangeClassification.UNCHANGED
            expl = "Explicit review limitations are unchanged."
        else:
            classification = ChangeClassification.MODIFIED
            expl = f"Limitations count changed from {len(prev_lim)} to {len(curr_lim)}."

        return EvolutionDimensionRecord(
            dimension_name=EvolutionDimensionName.LIMITATIONS,
            previous_state={"limitation_count": len(prev_lim)},
            current_state={"limitation_count": len(curr_lim)},
            change_classification=classification,
            source_references=curr.source_references,
            explanation=expl,
        )

    def _generate_voice_summary_text(
        self,
        prev: HistoricalReviewSnapshot,
        curr: HistoricalReviewSnapshot,
        overall_state: ChangeClassification,
        newly_resolved: List[str],
        dim_cov: EvolutionDimensionRecord,
        dim_conf: EvolutionDimensionRecord,
        dim_ev: EvolutionDimensionRecord,
        dim_prov: EvolutionDimensionRecord,
    ) -> str:
        """Construct deterministic voice-safe summary devoid of ranking or buy/sell wording."""
        if overall_state == ChangeClassification.UNCHANGED:
            return "Review state is unchanged since the previous review."

        if newly_resolved:
            if len(newly_resolved) == 1:
                return "One discrepancy was resolved since the previous review."
            return f"{len(newly_resolved)} discrepancies were resolved since the previous review."

        if dim_cov.change_classification == ChangeClassification.REVIEW_COVERAGE_CHANGED:
            prev_status = dim_cov.previous_state.get("coverage_status", "unknown").lower()
            curr_status = dim_cov.current_state.get("coverage_status", "unknown").lower()
            return f"Review coverage changed from {prev_status} to {curr_status}."

        if dim_conf.change_classification == ChangeClassification.CONFLICT_STATE_CHANGED:
            return "An existing conflict remains unresolved."

        if dim_ev.change_classification in (ChangeClassification.ADDED, ChangeClassification.MODIFIED):
            return "Evidence changed since the previous review."

        if dim_prov.change_classification == ChangeClassification.PROVENANCE_CHANGED:
            return "Provenance information was updated."

        return "Review state changed since the previous review."

    # ==============================================================================
    # StateStore Persistence
    # ==============================================================================

    def _persist_snapshot_to_statestore(self, snapshot: HistoricalReviewSnapshot) -> None:
        """Persist snapshot to StateStore under bounded historical_review namespaces."""
        if not self.state_store:
            return
        try:
            snap_dict = snapshot.to_dict()
            self.state_store.set("historical_review:latest", snap_dict)
            self.state_store.set(f"historical_review:{snapshot.snapshot_id}", snap_dict)

            history = self.state_store.get("historical_review:history", [])
            if not isinstance(history, list):
                history = []
            history.append(snap_dict)
            if len(history) > self.max_global_history:
                history = history[-self.max_global_history:]
            self.state_store.set("historical_review:history", history)
        except Exception as exc:
            logger.warning("Failed to persist snapshot to StateStore: %s", exc)

    def _persist_evolution_to_statestore(self, record: ReviewEvolutionRecord) -> None:
        """Persist evolution record to StateStore under bounded historical_review_evolution namespaces."""
        if not self.state_store:
            return
        try:
            rec_dict = record.to_dict()
            self.state_store.set("historical_review_evolution:latest", rec_dict)
            self.state_store.set(f"historical_review_evolution:{record.evolution_id}", rec_dict)

            history = self.state_store.get("historical_review_evolution:history", [])
            if not isinstance(history, list):
                history = []
            history.append(rec_dict)
            if len(history) > self.max_global_history:
                history = history[-self.max_global_history:]
            self.state_store.set("historical_review_evolution:history", history)
        except Exception as exc:
            logger.warning("Failed to persist evolution to StateStore: %s", exc)
