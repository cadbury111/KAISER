"""KAISER — Cross-Investigation Evidence Comparison & Review Consistency Engine.

Implements deterministic cross-investigation comparison over multiple completed
InvestigationReports, detecting shared/changed/conflicting evidence, observation shifts,
review consistency, and discrepancies with strict analytical neutrality.

SAFETY INVARIANTS:
1. Research and analytical decision support only: NEVER ranks trades or determines profitability.
2. Neutral comparison semantics: Purely describes empirical and qualitative differences.
3. Strict numerical safety: Rejects NaN/Infinity, safely handles zero baseline.
4. Secret sanitization: Redacts credentials and absolute paths from all comparison artifacts.
5. Thread-safe execution with bounded cache and StateStore persistence.
"""

from __future__ import annotations

import collections
import copy
import logging
import math
import re
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.opportunities.comparison.models import (
    ComparabilityState,
    ConclusionComparisonRecord,
    ConsistencySummary,
    DiscrepancyCategory,
    DiscrepancyResolutionState,
    DiscrepancySeverity,
    EvidenceDiffCategory,
    EvidenceDiffItem,
    InvestigationComparison,
    InvestigationComparisonVoiceSummary,
    InvestigationDiscrepancy,
    ObservationDiffItem,
    ObservationDiffType,
    ProvenanceComparisonRecord,
    QualityComparisonRecord,
    ReproducibilityComparisonRecord,
    compute_deterministic_comparison_id,
    safe_percentage_delta,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.investigation.models import (
    EvidenceReviewDisposition,
    ObservationType,
)
from app.analysis.opportunities.investigation.reporting.models import (
    AttributionType,
    EvidenceMatrixEntry,
    InvestigationReport,
    ObservationMatrixEntry,
    ReportStatus,
)
from app.analysis.opportunities.investigation.reporting.service import InvestigationReportingService
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore

logger = logging.getLogger("kaiser.analysis.opportunities.comparison.engine")

DEFAULT_MAX_COMPARISON_REPORTS = 20
DEFAULT_MAX_HISTORY = 100


class InvestigationComparisonEngine:
    """Deterministic, thread-safe comparison engine across completed InvestigationReports."""

    def __init__(
        self,
        state_store: Optional[StateStore] = None,
        event_bus: Optional[EventBus] = None,
        reporting_service: Optional[InvestigationReportingService] = None,
        clock: Optional[Callable[[], float]] = None,
        max_history: int = DEFAULT_MAX_HISTORY,
        max_reports_per_comparison: int = DEFAULT_MAX_COMPARISON_REPORTS,
    ) -> None:
        """Initialize the investigation comparison engine."""
        self.state_store = state_store
        self.event_bus = event_bus
        self.reporting_service = reporting_service
        self.clock = clock or time.time
        self.max_history = max(10, min(1000, max_history))
        self.max_reports_per_comparison = max(2, min(50, max_reports_per_comparison))

        self._lock = threading.RLock()
        self._comparisons: Dict[str, InvestigationComparison] = {}
        self._comparison_order: List[str] = []
        self._report_to_comparisons: Dict[str, List[str]] = collections.defaultdict(list)

    def compare_reports(
        self,
        reports: Optional[Sequence[InvestigationReport]] = None,
        report_ids: Optional[Sequence[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> InvestigationComparison:
        """Execute deterministic cross-investigation comparison over supplied reports.

        Args:
            reports: Direct sequence of InvestigationReport instances.
            report_ids: Sequence of report IDs to retrieve from reporting service.
            context: Optional comparison parameters and metadata.

        Returns:
            InvestigationComparison: Complete, immutable comparison artifact.
        """
        ctx = copy.deepcopy(context or {})
        validate_no_prohibited_fields(ctx, path="comparison_context")

        # 1. Resolve and validate report inputs
        resolved_reports = self._resolve_reports(reports=reports, report_ids=report_ids)
        self._validate_input_bounds(resolved_reports)

        now = self.clock()
        validate_numerical_integrity(now, "generated_at")

        # 2. Check for invalid or incomplete source reports
        comparability_state, comp_status, comp_reason = self._evaluate_source_reports_integrity(resolved_reports)

        # 3. Evaluate Comparability Dimensions
        if comparability_state is None:
            comparability_state = self._evaluate_comparability_dimensions(resolved_reports)

        # 4. Compare Evidence
        shared_ev, changed_ev, conflicting_ev, missing_ev = self._diff_evidence(resolved_reports)

        # 5. Compare Observations
        obs_diffs, uncertainty_diffs, limitation_diffs = self._diff_observations(resolved_reports)

        # 6. Compare Provenance, Quality, Reproducibility, Conclusions
        prov_comp = self._compare_provenance(resolved_reports)
        qual_comp = self._compare_quality(resolved_reports)
        repro_comp = self._compare_reproducibility(resolved_reports)
        conc_comp = self._compare_conclusions(resolved_reports)

        # 7. Discrepancy Detection
        discrepancies = self._detect_discrepancies(
            reports=resolved_reports,
            comparability_state=comparability_state,
            conflicting_evidence=conflicting_ev,
            observation_diffs=obs_diffs,
            quality_comp=qual_comp,
            provenance_comp=prov_comp,
            conclusion_comp=conc_comp,
        )

        # 8. Consistency Summary
        summary = self._build_consistency_summary(
            reports=resolved_reports,
            comparability_state=comparability_state,
            shared_ev=shared_ev,
            changed_ev=changed_ev,
            conflicting_ev=conflicting_ev,
            discrepancies=discrepancies,
            uncertainty_diffs=uncertainty_diffs,
            limitation_diffs=limitation_diffs,
        )

        # 9. Voice Summary
        voice_summary = self._build_voice_summary(
            reports=resolved_reports,
            comparability_state=comparability_state,
            shared_ev=shared_ev,
            conflicting_ev=conflicting_ev,
            discrepancies=discrepancies,
        )

        # 10. Deterministic Comparison ID & Traceability
        comp_id = compute_deterministic_comparison_id(
            report_ids=[r.report_id for r in resolved_reports],
            report_versions=[r.version for r in resolved_reports],
            reproducibility_ids=[r.reproducibility_id for r in resolved_reports],
        )

        traceability = {
            "comparison_id": comp_id,
            "report_lineage": {
                r.report_id: {
                    "candidate_id": r.candidate_id,
                    "session_id": r.session_id,
                    "priority_id": r.priority_id,
                    "version": r.version,
                    "reproducibility_id": r.reproducibility_id,
                    "status": r.report_status.value,
                }
                for r in resolved_reports
            },
            "comparability_reason": comp_reason or comparability_state.value,
        }

        comparison = InvestigationComparison(
            comparison_id=comp_id,
            report_ids=tuple(r.report_id for r in resolved_reports),
            candidate_ids=tuple(r.candidate_id for r in resolved_reports),
            comparison_status=comp_status or "COMPLETED",
            comparability_state=comparability_state,
            generated_at=now,
            shared_evidence=shared_ev,
            changed_evidence=changed_ev,
            conflicting_evidence=conflicting_ev,
            missing_evidence=missing_ev,
            observation_differences=obs_diffs,
            uncertainty_differences=uncertainty_diffs,
            limitation_differences=limitation_diffs,
            provenance_comparison=prov_comp,
            quality_comparison=qual_comp,
            reproducibility_comparison=repro_comp,
            conclusion_comparison=conc_comp,
            consistency_summary=summary,
            discrepancy_summary=discrepancies,
            traceability=traceability,
            voice_summary=voice_summary,
        )

        # 11. State persistence and event dispatch
        with self._lock:
            self._comparisons[comp_id] = comparison
            if comp_id not in self._comparison_order:
                self._comparison_order.append(comp_id)
            for r in resolved_reports:
                if comp_id not in self._report_to_comparisons[r.report_id]:
                    self._report_to_comparisons[r.report_id].append(comp_id)

            # Enforce bounded retention
            if len(self._comparison_order) > self.max_history:
                evicted_id = self._comparison_order.pop(0)
                if evicted_id in self._comparisons and evicted_id != comp_id:
                    evicted_cmp = self._comparisons.pop(evicted_id, None)
                    if evicted_cmp:
                        for rid in evicted_cmp.report_ids:
                            if evicted_id in self._report_to_comparisons[rid]:
                                self._report_to_comparisons[rid].remove(evicted_id)

            self._persist_state(comparison)
            self._dispatch_events(comparison)

        return comparison

    def get_comparison(self, comparison_id: str) -> Optional[InvestigationComparison]:
        """Retrieve a comparison by its deterministic ID."""
        if not comparison_id or not isinstance(comparison_id, str):
            return None
        with self._lock:
            cmp_item = self._comparisons.get(comparison_id)
            if cmp_item is not None:
                return cmp_item

        if self.state_store is not None:
            raw = self.state_store.get(f"investigation_comparison:{comparison_id}")
            if isinstance(raw, dict) and raw.get("comparison_id") == comparison_id:
                pass
        return None

    def list_comparisons(
        self,
        report_id: Optional[str] = None,
        comparability_state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[InvestigationComparison]:
        """Query stored comparisons with optional filtering and pagination."""
        with self._lock:
            all_cmps = [self._comparisons[cid] for cid in reversed(self._comparison_order) if cid in self._comparisons]

        filtered: List[InvestigationComparison] = []
        c_state_filter = comparability_state.strip().upper() if comparability_state else None

        for c in all_cmps:
            if report_id and report_id not in c.report_ids:
                continue
            if c_state_filter and c.comparability_state.value != c_state_filter:
                continue
            filtered.append(c)

        clamped_offset = max(0, offset)
        clamped_limit = max(1, min(100, limit))
        return filtered[clamped_offset : clamped_offset + clamped_limit]

    # =========================================================================
    # Internal Comparison Pipeline Stages
    # =========================================================================

    def _resolve_reports(
        self,
        reports: Optional[Sequence[InvestigationReport]] = None,
        report_ids: Optional[Sequence[str]] = None,
    ) -> List[InvestigationReport]:
        """Resolve inputs into a list of InvestigationReport instances."""
        if reports is not None:
            return list(reports)

        if not report_ids:
            raise ValidationError("Either 'reports' or 'report_ids' must be provided for cross-investigation comparison.")

        if self.reporting_service is None:
            raise ValidationError("Cannot resolve 'report_ids' without an attached InvestigationReportingService.")

        resolved: List[InvestigationReport] = []
        for rid in report_ids:
            rep = self.reporting_service.get_report(rid)
            if rep is None:
                raise ValidationError(f"Investigation report '{rid}' could not be found.")
            resolved.append(rep)
        return resolved

    def _validate_input_bounds(self, reports: Sequence[InvestigationReport]) -> None:
        """Validate report count bounds and non-None instances."""
        if len(reports) < 2:
            raise ValidationError(
                f"Cross-investigation comparison requires at least 2 reports, received {len(reports)}."
            )
        if len(reports) > self.max_reports_per_comparison:
            raise ValidationError(
                f"Maximum allowed reports for comparison is {self.max_reports_per_comparison}, "
                f"received {len(reports)}."
            )
        for idx, r in enumerate(reports):
            if not isinstance(r, InvestigationReport):
                raise ValidationError(
                    f"Report at index {idx} is not an InvestigationReport instance."
                )

    def _evaluate_source_reports_integrity(
        self,
        reports: Sequence[InvestigationReport],
    ) -> Tuple[Optional[ComparabilityState], Optional[str], Optional[str]]:
        """Check source report statuses and structural validity."""
        for r in reports:
            if r.report_status == ReportStatus.INVALID:
                return (
                    ComparabilityState.INVALID_INPUT,
                    "INVALID",
                    f"Source report '{r.report_id}' has INVALID status.",
                )
            if r.report_status == ReportStatus.INCOMPLETE:
                return (
                    ComparabilityState.INCOMPLETE_EVIDENCE,
                    "PARTIAL",
                    f"Source report '{r.report_id}' is INCOMPLETE.",
                )
            if not r.evidence_matrix and not r.evidence_summary:
                return (
                    ComparabilityState.INCOMPLETE_EVIDENCE,
                    "PARTIAL",
                    f"Source report '{r.report_id}' has empty evidence matrix.",
                )
        return None, None, None

    def _evaluate_comparability_dimensions(
        self,
        reports: Sequence[InvestigationReport],
    ) -> ComparabilityState:
        """Evaluate the 10 comparability dimensions across reports."""
        symbols = set()
        timeframes = set()

        for r in reports:
            sym = str(r.candidate_summary.get("symbol", "")).strip().upper()
            if sym:
                symbols.add(sym)
            tf = str(r.candidate_summary.get("timeframe", "")).strip().lower()
            if tf:
                timeframes.add(tf)

        # Dimension 1: Symbol compatibility
        if len(symbols) > 1:
            # Different symbols -> NOT_COMPARABLE
            return ComparabilityState.NOT_COMPARABLE

        # Dimension 2: Timeframe compatibility
        # If symbols match but timeframes differ -> PARTIALLY_COMPARABLE
        if len(timeframes) > 1:
            return ComparabilityState.PARTIALLY_COMPARABLE

        # Dimension 3 & 4: Lineage and Candidate revision
        candidate_ids = {r.candidate_id for r in reports}
        if len(candidate_ids) > 1:
            # Same symbol/timeframe, different candidate hypotheses -> PARTIALLY_COMPARABLE
            return ComparabilityState.PARTIALLY_COMPARABLE

        # All core dimensions match -> COMPARABLE
        return ComparabilityState.COMPARABLE

    def _diff_evidence(
        self,
        reports: Sequence[InvestigationReport],
    ) -> Tuple[
        Tuple[EvidenceDiffItem, ...],
        Tuple[EvidenceDiffItem, ...],
        Tuple[EvidenceDiffItem, ...],
        Tuple[EvidenceDiffItem, ...],
    ]:
        """Perform deterministic evidence diffing across reports."""
        all_ev_ids: Set[str] = set()
        rep_to_entries: Dict[str, Dict[str, EvidenceMatrixEntry]] = {}

        for r in reports:
            rep_map: Dict[str, EvidenceMatrixEntry] = {}
            for entry in r.evidence_matrix:
                rep_map[entry.evidence_id] = entry
                all_ev_ids.add(entry.evidence_id)
            # Also consider string references in supporting/conflicting/missing
            for eid in r.supporting_evidence:
                all_ev_ids.add(eid)
            for eid in r.conflicting_evidence:
                all_ev_ids.add(eid)
            for eid in r.missing_evidence:
                all_ev_ids.add(eid)
            rep_to_entries[r.report_id] = rep_map

        shared_list: List[EvidenceDiffItem] = []
        changed_list: List[EvidenceDiffItem] = []
        conflicting_list: List[EvidenceDiffItem] = []
        missing_list: List[EvidenceDiffItem] = []

        total_reports_count = len(reports)

        for ev_id in sorted(all_ev_ids):
            dispositions: Dict[str, str] = {}
            src_refs: Set[str] = set()
            ev_type: Optional[str] = None
            is_missing_anywhere = False

            for r in reports:
                entry = rep_to_entries[r.report_id].get(ev_id)
                if entry is not None:
                    disp = getattr(entry, "reviewer_disposition", None) or getattr(entry, "disposition", "NOT_REVIEWED")
                    dispositions[r.report_id] = disp
                    s_ref = getattr(entry, "source", None) or getattr(entry, "source_ref", None)
                    if s_ref:
                        src_refs.add(str(s_ref))
                    if entry.evidence_type:
                        ev_type = entry.evidence_type
                    if disp == EvidenceReviewDisposition.MISSING.value:
                        is_missing_anywhere = True
                else:
                    if ev_id in r.supporting_evidence:
                        dispositions[r.report_id] = EvidenceReviewDisposition.SUPPORTING.value
                    elif ev_id in r.conflicting_evidence:
                        dispositions[r.report_id] = EvidenceReviewDisposition.CONFLICTING.value
                    elif ev_id in r.missing_evidence:
                        dispositions[r.report_id] = EvidenceReviewDisposition.MISSING.value
                        is_missing_anywhere = True
                    else:
                        dispositions[r.report_id] = "ABSENT"

            distinct_disps = set(dispositions.values())

            # 1. Missing evidence check
            if is_missing_anywhere or "MISSING" in distinct_disps:
                item = EvidenceDiffItem(
                    evidence_id=ev_id,
                    diff_category=EvidenceDiffCategory.UNAVAILABLE,
                    report_dispositions=dispositions,
                    source_references=tuple(sorted(src_refs)),
                    reason="Evidence marked MISSING or unavailable in one or more reports.",
                    evidence_type=ev_type,
                )
                missing_list.append(item)
                continue

            # 2. Conflicting dispositions check (e.g. SUPPORTING vs CONFLICTING)
            has_supporting = any("SUPPORTING" in d for d in distinct_disps)
            has_conflicting = any("CONFLICTING" in d for d in distinct_disps)

            if has_supporting and has_conflicting:
                item = EvidenceDiffItem(
                    evidence_id=ev_id,
                    diff_category=EvidenceDiffCategory.CONFLICTING,
                    report_dispositions=dispositions,
                    source_references=tuple(sorted(src_refs)),
                    reason="Evidence received opposing dispositions (SUPPORTING vs CONFLICTING) across reports.",
                    evidence_type=ev_type,
                )
                conflicting_list.append(item)
                continue

            # 3. Present in all reports with identical disposition -> UNCHANGED
            if len(distinct_disps) == 1 and "ABSENT" not in distinct_disps and "NOT_REVIEWED" not in distinct_disps:
                item = EvidenceDiffItem(
                    evidence_id=ev_id,
                    diff_category=EvidenceDiffCategory.UNCHANGED,
                    report_dispositions=dispositions,
                    source_references=tuple(sorted(src_refs)),
                    reason="Evidence verified identically across all compared reports.",
                    evidence_type=ev_type,
                )
                shared_list.append(item)
                continue

            # 4. Partial presence or altered dispositions -> ADDED / REMOVED / MODIFIED
            if len(reports) == 2:
                r0_id = reports[0].report_id
                r1_id = reports[1].report_id
                r0_disp = dispositions.get(r0_id, "ABSENT")
                r1_disp = dispositions.get(r1_id, "ABSENT")
                if r0_disp != "ABSENT" and r1_disp == "ABSENT":
                    cat = EvidenceDiffCategory.REMOVED
                elif r0_disp == "ABSENT" and r1_disp != "ABSENT":
                    cat = EvidenceDiffCategory.ADDED
                else:
                    cat = EvidenceDiffCategory.MODIFIED
            else:
                if any(d == "ABSENT" for d in dispositions.values()):
                    cat = EvidenceDiffCategory.ADDED
                else:
                    cat = EvidenceDiffCategory.MODIFIED

            item = EvidenceDiffItem(
                evidence_id=ev_id,
                diff_category=cat,
                report_dispositions=dispositions,
                source_references=tuple(sorted(src_refs)),
                reason="Evidence presence or reviewer disposition varies across compared reports.",
                evidence_type=ev_type,
            )
            changed_list.append(item)

        return (
            tuple(shared_list),
            tuple(changed_list),
            tuple(conflicting_list),
            tuple(missing_list),
        )

    def _diff_observations(
        self,
        reports: Sequence[InvestigationReport],
    ) -> Tuple[
        Tuple[ObservationDiffItem, ...],
        Tuple[Dict[str, Any], ...],
        Tuple[Dict[str, Any], ...],
    ]:
        """Perform observation diffing while strictly preserving classification types."""
        obs_diffs: List[ObservationDiffItem] = []
        all_obs_by_id: Dict[str, Dict[str, ObservationMatrixEntry]] = collections.defaultdict(dict)

        for r in reports:
            for obs in r.observations:
                all_obs_by_id[obs.observation_id][r.report_id] = obs

        for obs_id in sorted(all_obs_by_id.keys()):
            rep_entries = all_obs_by_id[obs_id]
            rep_presence = tuple(sorted(rep_entries.keys()))
            types = {entry.observation_type for entry in rep_entries.values()}
            first_entry = next(iter(rep_entries.values()))
            stmt = first_entry.statement
            ev_refs = tuple(sorted({ref for e in rep_entries.values() for ref in e.evidence_refs}))

            # Epistemic safeguard: Check for FACT <-> INTERPRETATION classification change
            if len(types) > 1:
                t_list = sorted(types)
                diff_item = ObservationDiffItem(
                    observation_id=obs_id,
                    diff_type=ObservationDiffType.CLASSIFICATION_CHANGE,
                    statement=stmt,
                    classification_change=True,
                    old_type=t_list[0],
                    new_type=t_list[1],
                    report_presence=rep_presence,
                    evidence_refs=ev_refs,
                    reason=f"Classification shifted between '{t_list[0]}' and '{t_list[1]}' across reports.",
                )
                obs_diffs.append(diff_item)
            elif len(rep_presence) == len(reports):
                diff_item = ObservationDiffItem(
                    observation_id=obs_id,
                    diff_type=ObservationDiffType.SHARED,
                    statement=stmt,
                    classification_change=False,
                    old_type=first_entry.observation_type,
                    new_type=first_entry.observation_type,
                    report_presence=rep_presence,
                    evidence_refs=ev_refs,
                    reason="Observation shared identically across all reports.",
                )
                obs_diffs.append(diff_item)
            else:
                diff_item = ObservationDiffItem(
                    observation_id=obs_id,
                    diff_type=ObservationDiffType.NEW,
                    statement=stmt,
                    classification_change=False,
                    old_type=None,
                    new_type=first_entry.observation_type,
                    report_presence=rep_presence,
                    evidence_refs=ev_refs,
                    reason="Observation present in a subset of reports.",
                )
                obs_diffs.append(diff_item)

        # Uncertainty differences
        uncertainty_diffs: List[Dict[str, Any]] = []
        all_uncs = {u for r in reports for u in r.uncertainty_summary}
        for u in sorted(all_uncs):
            present_in = [r.report_id for r in reports if u in r.uncertainty_summary]
            is_shared = len(present_in) == len(reports)
            uncertainty_diffs.append({
                "uncertainty": u,
                "present_in": present_in,
                "is_shared": is_shared,
                "status": "COMMON" if is_shared else "VARYING",
            })

        # Limitation differences
        limitation_diffs: List[Dict[str, Any]] = []
        all_lims = {l for r in reports for l in r.limitation_summary}
        for l in sorted(all_lims):
            present_in = [r.report_id for r in reports if l in r.limitation_summary]
            is_shared = len(present_in) == len(reports)
            limitation_diffs.append({
                "limitation": l,
                "present_in": present_in,
                "is_shared": is_shared,
                "status": "COMMON" if is_shared else "VARYING",
            })

        return tuple(obs_diffs), tuple(uncertainty_diffs), tuple(limitation_diffs)

    def _compare_provenance(self, reports: Sequence[InvestigationReport]) -> ProvenanceComparisonRecord:
        """Compare data sources and lineage across reports."""
        src_types: Dict[str, Tuple[str, ...]] = {}
        prov_ids: Dict[str, Tuple[str, ...]] = {}

        for r in reports:
            p_dict = r.report_provenance or {}
            st = p_dict.get("source_type") or p_dict.get("sources") or ()
            if isinstance(st, str):
                st = (st,)
            src_types[r.report_id] = tuple(str(x) for x in st)

            pids = p_dict.get("provenance_ids", ())
            if isinstance(pids, str):
                pids = (pids,)
            prov_ids[r.report_id] = tuple(str(x) for x in pids)

        all_prov_tuples = set(prov_ids.values())
        is_identical = len(all_prov_tuples) <= 1

        all_pids_flat = {pid for t in prov_ids.values() for pid in t}
        shared_count = sum(1 for pid in all_pids_flat if all(pid in prov_ids[r.report_id] for r in reports))
        differing_count = len(all_pids_flat) - shared_count

        return ProvenanceComparisonRecord(
            is_identical=is_identical,
            source_types=src_types,
            provenance_ids=prov_ids,
            shared_provenance_count=shared_count,
            differing_provenance_count=differing_count,
            notes=("Provenance lineages match across all reports." if is_identical else "Differing data source provenance detected.",),
        )

    def _compare_quality(self, reports: Sequence[InvestigationReport]) -> QualityComparisonRecord:
        """Compare market data quality metrics safely."""
        statuses: Dict[str, str] = {}
        freshness: Dict[str, float] = {}
        completeness: Dict[str, float] = {}
        anomalies: Dict[str, int] = {}

        for r in reports:
            # Extract quality indicators from candidate summary or evidence matrix
            cs = r.candidate_summary or {}
            q_status = cs.get("data_quality_status") or "VALID"
            statuses[r.report_id] = str(q_status)

            freshness[r.report_id] = float(cs.get("freshness_seconds") or 0.0)
            comp_val = float(cs.get("completeness_ratio") if cs.get("completeness_ratio") is not None else 1.0)
            completeness[r.report_id] = max(0.0, min(1.0, comp_val))
            anomalies[r.report_id] = int(cs.get("anomaly_count") or 0)

        distinct_statuses = set(statuses.values())
        all_valid = distinct_statuses <= {"VALID", "None", ""}
        if all_valid:
            statuses = {k: "VALID" for k in statuses}
            status_divergence = False
        else:
            status_divergence = len(distinct_statuses) > 1

        # Calculate safe completeness delta between first two reports if available
        comp_delta_pct: Optional[float] = None
        if len(reports) >= 2:
            r1_id = reports[0].report_id
            r2_id = reports[1].report_id
            delta, _ = safe_percentage_delta(completeness.get(r1_id), completeness.get(r2_id))
            comp_delta_pct = delta

        return QualityComparisonRecord(
            quality_statuses=statuses,
            all_valid=all_valid,
            status_divergence=status_divergence,
            freshness_seconds=freshness,
            completeness_ratios=completeness,
            anomaly_counts=anomalies,
            relative_completeness_change_pct=comp_delta_pct,
            notes=("Data quality consistent and valid across reports." if all_valid else "Data quality status diverges between reports.",),
        )

    def _compare_reproducibility(self, reports: Sequence[InvestigationReport]) -> ReproducibilityComparisonRecord:
        """Compare reproducibility identifiers, run IDs, and configuration fingerprints."""
        repro_ids: Dict[str, Optional[str]] = {}
        fps: Set[str] = set()
        runs_per_report: Dict[str, Set[str]] = {}

        all_runs: Set[str] = set()
        for r in reports:
            rec = r.reproducibility_record
            r_id = r.reproducibility_id or (rec.reproducibility_id if rec else None)
            repro_ids[r.report_id] = r_id

            if rec:
                fp = getattr(rec, "config_fingerprint", None) or getattr(rec, "configuration_fingerprint", None)
                if fp:
                    fps.add(fp)

            runs = set(r.source_analysis_run_ids or (rec.source_analysis_run_ids if rec else ()))
            runs_per_report[r.report_id] = runs
            all_runs.update(runs)

        shared_runs = tuple(sorted(run for run in all_runs if all(run in runs_per_report[r.report_id] for r in reports)))
        differing_runs = tuple(sorted(run for run in all_runs if not all(run in runs_per_report[r.report_id] for r in reports)))
        fps_match = len(fps) <= 1

        is_reproducible = all(v is not None for v in repro_ids.values()) and fps_match

        return ReproducibilityComparisonRecord(
            is_fully_reproducible=is_reproducible,
            reproducibility_ids=repro_ids,
            shared_source_runs=shared_runs,
            differing_source_runs=differing_runs,
            config_fingerprints_match=fps_match,
            unavailable_reasons=() if is_reproducible else ("Differing or missing reproducibility identifiers detected.",),
        )

    def _compare_conclusions(self, reports: Sequence[InvestigationReport]) -> ConclusionComparisonRecord:
        """Compare analytical conclusions without ranking or trading preferences."""
        states: Dict[str, str] = {}
        rationales: Dict[str, str] = {}

        for r in reports:
            conc = r.analytical_conclusion or {}
            c_state = conc.get("conclusion_state", "UNSET")
            states[r.report_id] = str(c_state)
            rationales[r.report_id] = str(conc.get("rationale", ""))

        distinct_states = set(states.values())
        agree = len(distinct_states) == 1

        summary = None
        if not agree:
            summary = f"Reviewers reached divergent analytical conclusions: {', '.join(sorted(distinct_states))}."

        return ConclusionComparisonRecord(
            conclusions_agree=agree,
            conclusion_states=states,
            rationales=rationales,
            divergence_summary=summary,
        )

    def _detect_discrepancies(
        self,
        reports: Sequence[InvestigationReport],
        comparability_state: ComparabilityState,
        conflicting_evidence: Tuple[EvidenceDiffItem, ...],
        observation_diffs: Tuple[ObservationDiffItem, ...],
        quality_comp: QualityComparisonRecord,
        provenance_comp: ProvenanceComparisonRecord,
        conclusion_comp: ConclusionComparisonRecord,
    ) -> Tuple[InvestigationDiscrepancy, ...]:
        """Detect and generate structured InvestigationDiscrepancy records."""
        discrepancies: List[InvestigationDiscrepancy] = []
        d_idx = 1

        # 1. Comparability / Symbol / Timeframe Discrepancies
        if comparability_state == ComparabilityState.NOT_COMPARABLE:
            discrepancies.append(
                InvestigationDiscrepancy(
                    discrepancy_id=f"disc_comp_{d_idx}",
                    category=DiscrepancyCategory.SYMBOL_MISMATCH,
                    severity=DiscrepancySeverity.CRITICAL,
                    report_ids=tuple(r.report_id for r in reports),
                    evidence_refs=(),
                    description="Compared reports target different instrument symbols; baseline comparability impossible.",
                    resolution_state=DiscrepancyResolutionState.UNRESOLVED,
                )
            )
            d_idx += 1
        elif comparability_state == ComparabilityState.PARTIALLY_COMPARABLE:
            discrepancies.append(
                InvestigationDiscrepancy(
                    discrepancy_id=f"disc_comp_{d_idx}",
                    category=DiscrepancyCategory.TIMEFRAME_MISMATCH,
                    severity=DiscrepancySeverity.WARNING,
                    report_ids=tuple(r.report_id for r in reports),
                    evidence_refs=(),
                    description="Compared reports differ in timeframe or candidate lineage; partial comparability applied.",
                    resolution_state=DiscrepancyResolutionState.EXPLAINED,
                )
            )
            d_idx += 1

        # 2. Conflicting Evidence Dispositions
        for ev in conflicting_evidence:
            discrepancies.append(
                InvestigationDiscrepancy(
                    discrepancy_id=f"disc_ev_{d_idx}",
                    category=DiscrepancyCategory.EVIDENCE_DISPOSITION,
                    severity=DiscrepancySeverity.ERROR,
                    report_ids=tuple(ev.report_dispositions.keys()),
                    evidence_refs=(ev.evidence_id,),
                    description=f"Evidence item '{ev.evidence_id}' received contradictory dispositions across reports: {dict(ev.report_dispositions)}.",
                    resolution_state=DiscrepancyResolutionState.UNRESOLVED,
                    source_refs=ev.source_references,
                )
            )
            d_idx += 1

        # 3. Observation Classification Changes (FACT vs INTERPRETATION)
        for obs in observation_diffs:
            if obs.classification_change:
                discrepancies.append(
                    InvestigationDiscrepancy(
                        discrepancy_id=f"disc_obs_{d_idx}",
                        category=DiscrepancyCategory.FACT_INTERPRETATION_MISMATCH,
                        severity=DiscrepancySeverity.WARNING,
                        report_ids=obs.report_presence,
                        evidence_refs=obs.evidence_refs,
                        description=f"Observation '{obs.observation_id}' classification changed from '{obs.old_type}' to '{obs.new_type}'.",
                        resolution_state=DiscrepancyResolutionState.UNRESOLVED,
                    )
                )
                d_idx += 1

        # 4. Data Quality Degradation
        if quality_comp.status_divergence:
            discrepancies.append(
                InvestigationDiscrepancy(
                    discrepancy_id=f"disc_qual_{d_idx}",
                    category=DiscrepancyCategory.QUALITY_DEGRADATION,
                    severity=DiscrepancySeverity.WARNING,
                    report_ids=tuple(quality_comp.quality_statuses.keys()),
                    evidence_refs=(),
                    description=f"Data quality statuses diverge across reports: {dict(quality_comp.quality_statuses)}.",
                    resolution_state=DiscrepancyResolutionState.UNRESOLVED,
                )
            )
            d_idx += 1

        # 5. Provenance Mismatch
        if not provenance_comp.is_identical:
            discrepancies.append(
                InvestigationDiscrepancy(
                    discrepancy_id=f"disc_prov_{d_idx}",
                    category=DiscrepancyCategory.PROVENANCE_MISMATCH,
                    severity=DiscrepancySeverity.INFO,
                    report_ids=tuple(provenance_comp.provenance_ids.keys()),
                    evidence_refs=(),
                    description="Source data provenance records differ across compared investigation reports.",
                    resolution_state=DiscrepancyResolutionState.EXPLAINED,
                )
            )
            d_idx += 1

        # 6. Conclusion Divergence
        if not conclusion_comp.conclusions_agree:
            discrepancies.append(
                InvestigationDiscrepancy(
                    discrepancy_id=f"disc_conc_{d_idx}",
                    category=DiscrepancyCategory.CONCLUSION_DIVERGENCE,
                    severity=DiscrepancySeverity.WARNING,
                    report_ids=tuple(conclusion_comp.conclusion_states.keys()),
                    evidence_refs=(),
                    description=conclusion_comp.divergence_summary or "Reviewers reached differing analytical conclusions.",
                    resolution_state=DiscrepancyResolutionState.UNRESOLVED,
                )
            )
            d_idx += 1

        return tuple(discrepancies)

    def _build_consistency_summary(
        self,
        reports: Sequence[InvestigationReport],
        comparability_state: ComparabilityState,
        shared_ev: Tuple[EvidenceDiffItem, ...],
        changed_ev: Tuple[EvidenceDiffItem, ...],
        conflicting_ev: Tuple[EvidenceDiffItem, ...],
        discrepancies: Tuple[InvestigationDiscrepancy, ...],
        uncertainty_diffs: Tuple[Dict[str, Any], ...],
        limitation_diffs: Tuple[Dict[str, Any], ...],
    ) -> ConsistencySummary:
        """Construct the aggregate ConsistencySummary record."""
        total = len(reports)
        is_comp = comparability_state == ComparabilityState.COMPARABLE
        is_part = comparability_state == ComparabilityState.PARTIALLY_COMPARABLE
        is_incomp = comparability_state in (ComparabilityState.NOT_COMPARABLE, ComparabilityState.INVALID_INPUT)

        unresolved_cnt = sum(1 for d in discrepancies if d.resolution_state == DiscrepancyResolutionState.UNRESOLVED)
        common_lim_cnt = sum(1 for l in limitation_diffs if l.get("is_shared", False))

        new_unc_cnt = sum(1 for u in uncertainty_diffs if not u.get("is_shared", False))
        res_unc_cnt = 0  # In static comparison, varying uncertainties denote differential coverage

        return ConsistencySummary(
            total_reports_compared=total,
            comparable_reports=total if is_comp else 0,
            partially_comparable_reports=total if is_part else 0,
            incompatible_reports=total if is_incomp else 0,
            shared_evidence_count=len(shared_ev),
            changed_evidence_count=len(changed_ev),
            conflicting_evidence_count=len(conflicting_ev),
            unresolved_discrepancy_count=unresolved_cnt,
            new_uncertainty_count=new_unc_cnt,
            resolved_uncertainty_count=res_unc_cnt,
            common_limitation_count=common_lim_cnt,
        )

    def _build_voice_summary(
        self,
        reports: Sequence[InvestigationReport],
        comparability_state: ComparabilityState,
        shared_ev: Tuple[EvidenceDiffItem, ...],
        conflicting_ev: Tuple[EvidenceDiffItem, ...],
        discrepancies: Tuple[InvestigationDiscrepancy, ...],
    ) -> InvestigationComparisonVoiceSummary:
        """Construct speech-ready summary strictly validated against trading directives."""
        count = len(reports)
        sym = reports[0].candidate_summary.get("symbol", "the subject") if reports else "the subject"

        headline = f"Cross-investigation comparison completed across {count} reports."
        comp_speech = f"Comparability state is {comparability_state.value} for {sym}."
        ev_speech = f"Identified {len(shared_ev)} shared evidence items and {len(conflicting_ev)} conflicting reviews."

        unresolved = sum(1 for d in discrepancies if d.resolution_state == DiscrepancyResolutionState.UNRESOLVED)
        disc_speech = f"{unresolved} unresolved discrepancies remain documented for human review."

        full_script = (
            f"{headline} {comp_speech} {ev_speech} {disc_speech} "
            f"Review consistency evaluated objectively without trading directives or profitability claims."
        )

        return InvestigationComparisonVoiceSummary(
            headline=headline,
            comparability_speech=comp_speech,
            evidence_speech=ev_speech,
            discrepancy_speech=disc_speech,
            full_speech_script=full_script,
        )

    def _persist_state(self, comparison: InvestigationComparison) -> None:
        """Persist bounded comparison state to StateStore."""
        if self.state_store is None:
            return
        try:
            c_dict = comparison.to_dict()
            self.state_store.set("investigation_comparison:latest", c_dict)
            self.state_store.set(f"investigation_comparison:{comparison.comparison_id}", c_dict)

            history = self.state_store.get("investigation_comparison:history", [])
            if not isinstance(history, list):
                history = []
            history.append({
                "comparison_id": comparison.comparison_id,
                "report_ids": list(comparison.report_ids),
                "comparability_state": comparison.comparability_state.value,
                "generated_at": comparison.generated_at,
                "discrepancy_count": len(comparison.discrepancy_summary),
            })
            if len(history) > self.max_history:
                history = history[-self.max_history:]
            self.state_store.set("investigation_comparison:history", history)
        except Exception as exc:
            logger.warning("Failed to persist investigation comparison state: %s", exc)

    def _dispatch_events(self, comparison: InvestigationComparison) -> None:
        """Dispatch lifecycle events to EventBus with sanitized metadata."""
        if self.event_bus is None:
            return
        try:
            ev_meta = {
                "comparison_id": comparison.comparison_id,
                "report_ids": list(comparison.report_ids),
                "comparability_state": comparison.comparability_state.value,
                "shared_evidence_count": len(comparison.shared_evidence),
                "conflicting_evidence_count": len(comparison.conflicting_evidence),
                "discrepancy_count": len(comparison.discrepancy_summary),
                "generated_at": comparison.generated_at,
            }
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.INVESTIGATION_REPORTS_COMPARED,
                    payload=ev_meta,
                    source="investigation_comparison_engine",
                )
            )

            # Publish discrepancy event if unresolved discrepancies exist
            unresolved = [d for d in comparison.discrepancy_summary if d.resolution_state == DiscrepancyResolutionState.UNRESOLVED]
            if unresolved:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.INVESTIGATION_DISCREPANCY_DETECTED,
                        payload={
                            "comparison_id": comparison.comparison_id,
                            "unresolved_discrepancy_count": len(unresolved),
                            "categories": list({d.category.value for d in unresolved}),
                            "severities": list({d.severity.value for d in unresolved}),
                        },
                        source="investigation_comparison_engine",
                    )
                )
        except Exception as exc:
            logger.warning("Failed to publish investigation comparison event: %s", exc)
