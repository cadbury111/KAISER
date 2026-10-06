"""KAISER — Deterministic Evidence Synthesis Engine.

Orchestrates deterministic synthesis of data quality, provenance lineage,
analytical execution runs, experiment comparisons, and portfolio risk signals
into structured, auditable decision-support evidence reports.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Determinism: Given identical structured inputs, outputs are strictly equivalent.
2. Zero generative LLM/web dependencies: Purely rule-based, deterministic synthesis.
3. No trading recommendations: Never outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE.
4. Strict facts vs. interpretations separation: Every fact is traceable to source references.
5. Objective conflict detection: Exposes contradictions without picking an automatic winner.
6. Thread-safe & Bounded: All collections, histories, and caches have strict capacity limits.
"""

from __future__ import annotations

import collections
import copy
import hashlib
import json
import logging
import math
import re
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.comparison.engine import RunComparisonEngine
from app.analysis.comparison.metrics import extract_metrics_from_result, safe_float
from app.analysis.comparison.models import (
    ComparabilityStatus,
    ComparisonDimension,
    MetricDifference,
    MetricDirection,
    RunComparison,
)
from app.analysis.evidence.models import (
    DEFAULT_MAX_CONFLICTS,
    DEFAULT_MAX_EVIDENCE_ITEMS,
    DEFAULT_MAX_LIMITATIONS,
    DEFAULT_MAX_OBSERVATIONS,
    DEFAULT_MAX_SOURCE_RUNS,
    ConflictResolutionStatus,
    ConflictSeverity,
    ConflictType,
    EvidenceConflict,
    EvidenceItem,
    EvidenceStatus,
    EvidenceSynthesisReport,
    EvidenceType,
    EvidenceUncertainty,
    EvidenceVoiceSummary,
    KeyObservation,
    MissingEvidence,
    ObservationType,
    SampleCoverage,
    UncertaintySource,
    evidence_priority_key,
)
from app.analysis.runs.models import AnalysisRun, OperationType, RunStatus
from app.analysis.runs.registry import AnalysisRunRegistry
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import MarketDataProvenance, sanitize_provenance_metadata, sanitize_provenance_string
from app.market.provenance.tracker import MarketDataProvenanceTracker
from app.market.quality.models import MarketAnomaly, QualityFinding, QualitySeverity
from app.market.quality.monitor import MarketDataQualityMonitor
from app.market.quality.status import DataQualityAssessment, DataQualityStatus

logger = logging.getLogger("kaiser.analysis.evidence.engine")

DEFAULT_MAX_SYNTHESIS_HISTORY = 100


class EvidenceSynthesisEngine:
    """Deterministic, thread-safe engine for synthesizing analytical evidence into decision-support contracts."""

    def __init__(
        self,
        run_registry: Optional[AnalysisRunRegistry] = None,
        comparison_engine: Optional[RunComparisonEngine] = None,
        provenance_tracker: Optional[MarketDataProvenanceTracker] = None,
        quality_monitor: Optional[MarketDataQualityMonitor] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        clock: Optional[Callable[[], float]] = None,
        max_history: int = DEFAULT_MAX_SYNTHESIS_HISTORY,
        max_evidence_items: int = DEFAULT_MAX_EVIDENCE_ITEMS,
        max_source_runs: int = DEFAULT_MAX_SOURCE_RUNS,
    ) -> None:
        """Initialize the Evidence Synthesis Engine.

        Args:
            run_registry: Optional AnalysisRunRegistry for resolving execution runs.
            comparison_engine: Optional RunComparisonEngine for resolving run comparisons.
            provenance_tracker: Optional MarketDataProvenanceTracker for verifying lineage.
            quality_monitor: Optional MarketDataQualityMonitor for querying quality findings.
            event_bus: Optional EventBus for broadcasting synthesis lifecycle events.
            state_store: Optional StateStore for persisting bounded report snapshots.
            clock: Deterministic POSIX timestamp supplier.
            max_history: Maximum synthesized reports kept in memory.
            max_evidence_items: Maximum atomic evidence items allowed per report.
            max_source_runs: Maximum runs evaluated simultaneously.
        """
        self.run_registry = run_registry
        self.comparison_engine = comparison_engine
        self.provenance_tracker = provenance_tracker
        self.quality_monitor = quality_monitor
        self.event_bus = event_bus
        self.state_store = state_store
        self.clock = clock or time.time
        self.max_history = max(1, min(1000, max_history))
        self.max_evidence_items = max(10, min(5000, max_evidence_items))
        self.max_source_runs = max(1, min(50, max_source_runs))

        self._lock = threading.RLock()
        self._reports_by_id: Dict[str, EvidenceSynthesisReport] = {}
        self._history: collections.deque[str] = collections.deque(maxlen=self.max_history)

    def synthesize(
        self,
        subject: str,
        runs: Sequence[Union[AnalysisRun, str]] = (),
        comparison: Optional[Union[RunComparison, str]] = None,
        quality_assessments: Optional[Sequence[DataQualityAssessment]] = None,
        provenance_records: Optional[Sequence[MarketDataProvenance]] = None,
        extra_evidence: Optional[Sequence[EvidenceItem]] = None,
        scope: Optional[str] = None,
        additional_limitations: Optional[Sequence[str]] = None,
    ) -> EvidenceSynthesisReport:
        """Deterministically synthesize multi-source signals into an EvidenceSynthesisReport.

        Args:
            subject: High-level subject of investigation (e.g. strategy name, run comparison, or symbol).
            runs: Sequence of AnalysisRun objects or run_id strings.
            comparison: Optional RunComparison instance or comparison_id string.
            quality_assessments: Explicit quality assessments (or queried automatically).
            provenance_records: Explicit provenance records (or queried automatically).
            extra_evidence: Additional pre-validated EvidenceItem records.
            scope: Operational scope label ('SINGLE_RUN', 'RUN_COMPARISON', 'DATASET_EVALUATION', etc.).
            additional_limitations: Additional domain limitations to append.

        Returns:
            EvidenceSynthesisReport: Immutable, auditable synthesis report.
        """
        if not subject or not isinstance(subject, str) or not subject.strip():
            raise ValidationError("Synthesis requires a non-empty 'subject'.")

        now = self.clock()

        try:
            # 1. Resolve and validate runs
            resolved_runs = self._resolve_runs(runs)
            if len(resolved_runs) > self.max_source_runs:
                raise ValidationError(
                    f"Requested {len(resolved_runs)} runs exceeds maximum allowable bound of {self.max_source_runs} runs."
                )

            # 2. Resolve comparison
            resolved_comparison = self._resolve_comparison(comparison)

            # Determine scope if omitted
            if not scope or not str(scope).strip():
                if resolved_comparison is not None:
                    scope = "RUN_COMPARISON"
                elif len(resolved_runs) == 1:
                    scope = "SINGLE_RUN_EVALUATION"
                elif len(resolved_runs) > 1:
                    scope = "MULTI_RUN_EVALUATION"
                else:
                    scope = "GENERAL_EVIDENCE_SYNTHESIS"

            # 3. Accumulate atomic evidence items
            items: List[EvidenceItem] = []
            missing_items: List[MissingEvidence] = []
            uncertainties: List[EvidenceUncertainty] = []

            # 3a. Process runs & linked evidence
            source_run_ids: List[str] = []
            repro_refs: List[str] = []
            combined_metrics: Dict[str, Any] = {}
            dq_summaries: Dict[str, Any] = {}
            prov_summaries: Dict[str, Any] = {}

            for run in resolved_runs:
                source_run_ids.append(run.run_id)
                repro_refs.append(run.reproducibility_id)
                self._extract_run_evidence(
                    run=run,
                    items=items,
                    missing_items=missing_items,
                    uncertainties=uncertainties,
                    combined_metrics=combined_metrics,
                    dq_summaries=dq_summaries,
                    prov_summaries=prov_summaries,
                )

            # 3b. Process explicit quality assessments
            if quality_assessments:
                for qa in quality_assessments:
                    self._extract_quality_assessment_evidence(qa, items, uncertainties, dq_summaries)

            # 3c. Process explicit provenance records
            if provenance_records:
                for prov in provenance_records:
                    self._extract_provenance_evidence(prov, items, prov_summaries)

            # 3d. Process comparison
            comp_summary: Dict[str, Any] = {}
            if resolved_comparison is not None:
                self._extract_comparison_evidence(
                    comp=resolved_comparison,
                    items=items,
                    uncertainties=uncertainties,
                    comp_summary=comp_summary,
                )

            # 3e. Process extra evidence
            if extra_evidence:
                for ee in extra_evidence:
                    if isinstance(ee, EvidenceItem):
                        items.append(ee)

            # Deduplicate items by evidence_id
            unique_items: Dict[str, EvidenceItem] = {}
            for it in items:
                if it.evidence_id not in unique_items:
                    unique_items[it.evidence_id] = it

            # Sort deterministically using prioritization rationale
            sorted_items = sorted(unique_items.values(), key=evidence_priority_key)
            if len(sorted_items) > self.max_evidence_items:
                sorted_items = sorted_items[: self.max_evidence_items]

            # 4. Extract structured KeyObservations (Facts vs. Interpretations)
            key_observations: List[KeyObservation] = []
            self._generate_observations(
                items=sorted_items,
                runs=resolved_runs,
                comparison=resolved_comparison,
                observations=key_observations,
            )

            # 5. Detect conflicts deterministically
            conflicts: List[EvidenceConflict] = []
            self._detect_conflicts(
                items=sorted_items,
                comparison=resolved_comparison,
                conflicts=conflicts,
            )

            # 6. Sample coverage aggregation
            sample_coverage = self._compute_sample_coverage(resolved_runs)

            # 7. Formulate Risk Summary
            risk_summary = self._formulate_risk_summary(resolved_runs, combined_metrics)

            # 8. Consolidate limitations
            limitations_list = [
                "Historical backtest and simulation results do not establish or guarantee future market performance.",
                "Decision-support evidence synthesis only. Execution engine is locked (Settings.execution_locked = True).",
                "Zero automated trade recommendations, entries, exits, or position size directives are provided.",
            ]
            if resolved_comparison and resolved_comparison.comparability_status != ComparabilityStatus.COMPARABLE:
                limitations_list.append(
                    f"Compared runs are {resolved_comparison.comparability_status.value}: conclusions are subject to configuration or data divergences."
                )
            if additional_limitations:
                for lim in additional_limitations:
                    lim_str = str(lim).strip()
                    if lim_str and lim_str not in limitations_list:
                        limitations_list.append(lim_str)

            # 9. Formulate Voice Summary
            voice_summary = self._synthesize_voice_summary(
                subject=subject,
                items=sorted_items,
                observations=key_observations,
                conflicts=conflicts,
                risk_summary=risk_summary,
                comparison=resolved_comparison,
            )

            # 10. Generate deterministic report ID
            sorted_run_ids = sorted(source_run_ids)
            id_payload = {
                "subject": subject.strip().upper(),
                "scope": scope,
                "source_runs": sorted_run_ids,
                "item_count": len(sorted_items),
                "generated_at": round(now, 2),
            }
            id_digest = hashlib.sha256(json.dumps(id_payload, sort_keys=True).encode("utf-8")).hexdigest()[:20]
            report_id = f"syn_{id_digest}"

            report = EvidenceSynthesisReport(
                report_id=report_id,
                generated_at=now,
                subject=subject.strip(),
                scope=scope,
                source_run_ids=tuple(sorted_run_ids),
                evidence_items=tuple(sorted_items),
                key_observations=tuple(key_observations),
                supporting_metrics=combined_metrics,
                data_quality_summary=dq_summaries,
                provenance_summary=prov_summaries,
                comparison_summary=comp_summary,
                risk_summary=risk_summary,
                conflicts=tuple(conflicts),
                missing_evidence=tuple(missing_items),
                uncertainties=tuple(uncertainties),
                sample_coverage=sample_coverage,
                limitations=tuple(limitations_list),
                reproducibility_references=tuple(sorted(set(repro_refs))),
                voice_summary=voice_summary,
            )

            # Record in memory and StateStore
            with self._lock:
                self._reports_by_id[report.report_id] = report
                self._history.append(report.report_id)

            self._publish_success(report)
            self._update_state_store(report)

            return report

        except ValidationError:
            self._publish_failure(subject, "Validation error during evidence synthesis.")
            raise
        except Exception as exc:
            err_msg = f"Evidence synthesis failed: {str(exc)}"
            logger.error(err_msg, exc_info=True)
            self._publish_failure(subject, sanitize_provenance_string(str(exc)))
            raise ValidationError(err_msg)

    def get_synthesis(self, report_id: str) -> Optional[EvidenceSynthesisReport]:
        """Look up an evidence synthesis report by report_id."""
        if not report_id or not isinstance(report_id, str):
            return None
        with self._lock:
            return self._reports_by_id.get(report_id.strip())

    def list_syntheses(
        self,
        limit: int = 50,
        offset: int = 0,
        subject: Optional[str] = None,
    ) -> List[EvidenceSynthesisReport]:
        """List historical synthesis reports with deterministic pagination and filtering."""
        with self._lock:
            ordered_ids = list(self._history)

        # Most recent first
        ordered_ids.reverse()

        matched: List[EvidenceSynthesisReport] = []
        target_subj = subject.strip().upper() if subject else None

        with self._lock:
            for r_id in ordered_ids:
                rep = self._reports_by_id.get(r_id)
                if rep is None:
                    continue
                if target_subj and target_subj not in rep.subject.upper():
                    continue
                matched.append(rep)

        clamped_offset = max(0, offset)
        clamped_limit = max(1, min(100, limit))
        return matched[clamped_offset : clamped_offset + clamped_limit]

    # =========================================================================
    # INTERNAL RESOLUTION HELPERS
    # =========================================================================

    def _resolve_runs(self, runs: Sequence[Union[AnalysisRun, str]]) -> List[AnalysisRun]:
        """Resolve run objects from registry if IDs were provided."""
        resolved: List[AnalysisRun] = []
        for r in runs:
            if isinstance(r, AnalysisRun):
                resolved.append(r)
            elif isinstance(r, str) and r.strip():
                rid = r.strip()
                if self.run_registry:
                    rec = self.run_registry.get_run(rid)
                    if not rec:
                        raise ValidationError(f"AnalysisRun '{rid}' not found in run registry.")
                    resolved.append(rec)
                else:
                    raise ValidationError(f"Run registry not attached; cannot resolve run ID '{rid}'.")
            else:
                raise ValidationError(f"Invalid run input: expected AnalysisRun or run_id string, got {type(r)}")
        return resolved

    def _resolve_comparison(self, comparison: Optional[Union[RunComparison, str]]) -> Optional[RunComparison]:
        """Resolve comparison object from comparison engine if ID was provided."""
        if comparison is None:
            return None
        if isinstance(comparison, RunComparison):
            return comparison
        if isinstance(comparison, str) and comparison.strip():
            cid = comparison.strip()
            if self.comparison_engine:
                c = self.comparison_engine.get_comparison(cid)
                if not c:
                    raise ValidationError(f"RunComparison '{cid}' not found in comparison engine.")
                return c
            raise ValidationError(f"Comparison engine not attached; cannot resolve comparison ID '{cid}'.")
        raise ValidationError(f"Invalid comparison input: expected RunComparison or ID string, got {type(comparison)}")

    # =========================================================================
    # EVIDENCE EXTRACTION HELPERS
    # =========================================================================

    def _extract_run_evidence(
        self,
        run: AnalysisRun,
        items: List[EvidenceItem],
        missing_items: List[MissingEvidence],
        uncertainties: List[EvidenceUncertainty],
        combined_metrics: Dict[str, Any],
        dq_summaries: Dict[str, Any],
        prov_summaries: Dict[str, Any],
    ) -> None:
        """Extract atomic evidence from a single AnalysisRun instance."""
        run_ts = run.completed_at or run.started_at or run.created_at

        # 1. Run Lifecycle & Status Evidence
        status_ev_state = EvidenceStatus.AVAILABLE if run.status == RunStatus.COMPLETED else (
            EvidenceStatus.INVALID if run.status in (RunStatus.FAILED, RunStatus.INVALID) else EvidenceStatus.PARTIAL
        )
        items.append(
            EvidenceItem(
                evidence_id=f"ev_run_status_{run.run_id}",
                evidence_type=EvidenceType.SYSTEM_DIAGNOSTIC,
                source_reference=f"run_registry:{run.run_id}",
                source_run_id=run.run_id,
                observation=f"Analytical execution run completed with status {run.status.value}.",
                observed_value=run.status.value,
                timestamp=run_ts,
                status=status_ev_state,
                supporting_references=(run.run_id, run.reproducibility_id),
            )
        )

        # 2. Input Record Count & Data Coverage Evidence
        items.append(
            EvidenceItem(
                evidence_id=f"ev_run_coverage_{run.run_id}",
                evidence_type=EvidenceType.DATA_QUALITY,
                source_reference=f"run_registry:{run.run_id}",
                source_run_id=run.run_id,
                observation=f"Run evaluated {run.input_record_count} input records across {len(run.symbol_universe)} symbols.",
                observed_value=run.input_record_count,
                units="records",
                timestamp=run_ts,
                status=EvidenceStatus.AVAILABLE if run.input_record_count > 0 else EvidenceStatus.UNAVAILABLE,
                supporting_references=(run.run_id,),
            )
        )

        # 3. Data Quality Linkage & Assessments
        if run.quality_status:
            dq_state = EvidenceStatus.AVAILABLE if run.quality_status == DataQualityStatus.VALID else (
                EvidenceStatus.INVALID if run.quality_status == DataQualityStatus.INVALID else (
                    EvidenceStatus.STALE if run.quality_status == DataQualityStatus.STALE else EvidenceStatus.PARTIAL
                )
            )
            items.append(
                EvidenceItem(
                    evidence_id=f"ev_dq_status_{run.run_id}",
                    evidence_type=EvidenceType.DATA_QUALITY,
                    source_reference=f"run:{run.run_id}:quality_status",
                    source_run_id=run.run_id,
                    observation=f"Market data quality status evaluated as {run.quality_status.value}.",
                    observed_value=run.quality_status.value,
                    timestamp=run_ts,
                    status=dq_state,
                    supporting_references=(run.run_id,),
                )
            )
            dq_summaries[f"{run.run_id}_status"] = run.quality_status.value

            if run.quality_status in (DataQualityStatus.DEGRADED, DataQualityStatus.STALE, DataQualityStatus.INVALID):
                uncertainties.append(
                    EvidenceUncertainty(
                        uncertainty_id=f"unc_dq_{run.run_id}",
                        source=UncertaintySource.DEGRADED_QUALITY if run.quality_status != DataQualityStatus.STALE else UncertaintySource.STALE_DATA,
                        description=f"Run '{run.run_id}' used market data classified as {run.quality_status.value}.",
                        impact="Downstream calculations may reflect data artifacts, gaps, or staleness.",
                        affected_evidence_ids=(f"ev_dq_status_{run.run_id}",),
                    )
                )

        # Query assessments from QualityMonitor if attached
        if self.quality_monitor and run.quality_assessment_ids:
            for q_id in run.quality_assessment_ids:
                qa = self.quality_monitor.get_assessment_by_id(q_id)
                if qa:
                    self._extract_quality_assessment_evidence(qa, items, uncertainties, dq_summaries, run_id=run.run_id)

        # 4. Provenance Lineage Linkage
        if run.input_provenance_ids:
            prov_summaries[run.run_id] = list(run.input_provenance_ids)
            for p_id in run.input_provenance_ids:
                if self.provenance_tracker:
                    p_rec = self.provenance_tracker.get_provenance(p_id)
                    if p_rec:
                        self._extract_provenance_evidence(p_rec, items, prov_summaries, run_id=run.run_id)
                    else:
                        items.append(
                            EvidenceItem(
                                evidence_id=f"ev_prov_missing_{p_id}",
                                evidence_type=EvidenceType.PROVENANCE,
                                source_reference=f"provenance_tracker:{p_id}",
                                source_run_id=run.run_id,
                                provenance_id=p_id,
                                observation=f"Linked input provenance record '{p_id}' was not found in tracker.",
                                timestamp=run_ts,
                                status=EvidenceStatus.UNAVAILABLE,
                                supporting_references=(run.run_id, p_id),
                            )
                        )
                        uncertainties.append(
                            EvidenceUncertainty(
                                uncertainty_id=f"unc_prov_missing_{p_id}",
                                source=UncertaintySource.INCOMPLETE_PROVENANCE,
                                description=f"Input provenance '{p_id}' referenced by run '{run.run_id}' is missing from tracker.",
                                impact="Full data lineage cannot be verified.",
                                affected_evidence_ids=(f"ev_prov_missing_{p_id}",),
                            )
                        )
        else:
            missing_items.append(
                MissingEvidence(
                    missing_id=f"miss_prov_{run.run_id}",
                    evidence_type=EvidenceType.PROVENANCE,
                    description=f"Run '{run.run_id}' has no linked input data provenance records.",
                    impact="Origin and transformation path of input data cannot be audited.",
                    expected_source=f"run_registry:{run.run_id}",
                )
            )

        # 5. Extract Metrics from Run Execution Metadata / Results
        metrics = extract_metrics_from_result(
            result=run.execution_metadata,
            execution_metadata=run.execution_metadata,
        )
        for k, v in metrics.items():
            combined_metrics[f"{run.run_id}:{k}"] = v
            ev_type = EvidenceType.BACKTEST
            if "risk" in k or "drawdown" in k or "volatility" in k:
                ev_type = EvidenceType.RISK
            elif "attribution" in k:
                ev_type = EvidenceType.ATTRIBUTION
            elif run.operation_type == OperationType.WALK_FORWARD:
                ev_type = EvidenceType.WALK_FORWARD
            elif run.operation_type == OperationType.ROBUSTNESS:
                ev_type = EvidenceType.ROBUSTNESS

            items.append(
                EvidenceItem(
                    evidence_id=f"ev_metric_{run.run_id}_{k}",
                    evidence_type=ev_type,
                    source_reference=f"run:{run.run_id}:metric:{k}",
                    source_run_id=run.run_id,
                    observation=f"Observed metric '{k}' = {v} on run {run.run_id}.",
                    observed_value=v,
                    units="%" if "pct" in k or "rate" in k else ("$" if "pnl" in k or "profit" in k or "cost" in k else None),
                    timestamp=run_ts,
                    status=EvidenceStatus.AVAILABLE if v is not None else EvidenceStatus.UNAVAILABLE,
                    supporting_references=(run.run_id,),
                )
            )

    def _extract_quality_assessment_evidence(
        self,
        qa: DataQualityAssessment,
        items: List[EvidenceItem],
        uncertainties: List[EvidenceUncertainty],
        dq_summaries: Dict[str, Any],
        run_id: Optional[str] = None,
    ) -> None:
        """Extract evidence from a DataQualityAssessment instance."""
        dq_state = EvidenceStatus.AVAILABLE if qa.quality_status == DataQualityStatus.VALID else (
            EvidenceStatus.INVALID if qa.quality_status == DataQualityStatus.INVALID else (
                EvidenceStatus.STALE if qa.quality_status == DataQualityStatus.STALE else EvidenceStatus.PARTIAL
            )
        )
        items.append(
            EvidenceItem(
                evidence_id=f"ev_qa_{qa.assessment_id}",
                evidence_type=EvidenceType.DATA_QUALITY,
                source_reference=f"quality_assessment:{qa.assessment_id}",
                source_run_id=run_id,
                quality_assessment_id=qa.assessment_id,
                observation=f"Dataset quality for {qa.symbol} {qa.timeframe} evaluated as {qa.quality_status.value} (score={qa.quality_score:.1f}).",
                observed_value=qa.quality_score,
                units="score [0-100]",
                timestamp=qa.evaluated_at or qa.assessment_timestamp,
                status=dq_state,
                supporting_references=(qa.assessment_id,),
            )
        )
        dq_summaries[qa.assessment_id] = {
            "symbol": qa.symbol,
            "status": qa.quality_status.value,
            "score": qa.quality_score,
            "anomalies": len(qa.anomalies),
        }

        # Anomalies
        for idx, anom in enumerate(qa.anomalies):
            anom_desc = getattr(anom, "reason", "") or getattr(anom, "description", "")
            anom_ts = getattr(anom, "start_timestamp", None) or getattr(anom, "detected_at", None) or qa.evaluated_at
            items.append(
                EvidenceItem(
                    evidence_id=f"ev_anom_{qa.assessment_id}_{idx}",
                    evidence_type=EvidenceType.DATA_ANOMALY,
                    source_reference=f"quality_assessment:{qa.assessment_id}:anomaly:{anom.anomaly_type.value}",
                    source_run_id=run_id,
                    quality_assessment_id=qa.assessment_id,
                    observation=f"Data anomaly detected: {anom.anomaly_type.value} on {qa.symbol} ({anom_desc}).",
                    observed_value=anom.anomaly_type.value,
                    timestamp=anom_ts,
                    status=EvidenceStatus.INVALID if anom.severity == QualitySeverity.CRITICAL else EvidenceStatus.AVAILABLE,
                    supporting_references=(qa.assessment_id,),
                )
            )

    def _extract_provenance_evidence(
        self,
        prov: MarketDataProvenance,
        items: List[EvidenceItem],
        prov_summaries: Dict[str, Any],
        run_id: Optional[str] = None,
    ) -> None:
        """Extract evidence from a MarketDataProvenance instance."""
        items.append(
            EvidenceItem(
                evidence_id=f"ev_prov_{prov.provenance_id}",
                evidence_type=EvidenceType.PROVENANCE,
                source_reference=f"provenance:{prov.provenance_id}",
                source_run_id=run_id,
                provenance_id=prov.provenance_id,
                observation=f"Data originated from source '{prov.source_type.value}:{prov.source_identifier}' ({prov.record_count} records).",
                observed_value=prov.record_count,
                units="records",
                timestamp=prov.processing_timestamp,
                status=EvidenceStatus.AVAILABLE,
                supporting_references=(prov.provenance_id,),
            )
        )
        prov_summaries[prov.provenance_id] = {
            "source_type": prov.source_type.value,
            "source_identifier": prov.source_identifier,
            "record_count": prov.record_count,
            "parents_count": len(prov.parent_provenance_ids),
        }

    def _extract_comparison_evidence(
        self,
        comp: RunComparison,
        items: List[EvidenceItem],
        uncertainties: List[EvidenceUncertainty],
        comp_summary: Dict[str, Any],
    ) -> None:
        """Extract evidence from a RunComparison instance."""
        # 1. Comparability Status
        status_state = EvidenceStatus.AVAILABLE if comp.comparability_status == ComparabilityStatus.COMPARABLE else (
            EvidenceStatus.PARTIAL if comp.comparability_status == ComparabilityStatus.PARTIALLY_COMPARABLE else EvidenceStatus.INVALID
        )
        items.append(
            EvidenceItem(
                evidence_id=f"ev_comp_status_{comp.comparison_id}",
                evidence_type=EvidenceType.COMPARISON,
                source_reference=f"comparison:{comp.comparison_id}",
                comparison_id=comp.comparison_id,
                observation=f"Runs {comp.left_run_id} and {comp.right_run_id} evaluated as {comp.comparability_status.value}.",
                observed_value=comp.comparability_status.value,
                timestamp=comp.generated_at,
                status=status_state,
                supporting_references=(comp.comparison_id, comp.left_run_id, comp.right_run_id),
            )
        )
        comp_summary["comparability_status"] = comp.comparability_status.value
        comp_summary["changed_dimensions"] = [d.value for d in comp.changed_dimensions]

        if comp.comparability_status != ComparabilityStatus.COMPARABLE:
            uncertainties.append(
                EvidenceUncertainty(
                    uncertainty_id=f"unc_comp_status_{comp.comparison_id}",
                    source=UncertaintySource.CONFIGURATION_DIFFERENCE if ComparisonDimension.CONFIGURATION in comp.changed_dimensions else UncertaintySource.DATASET_DIFFERENCE,
                    description=f"Compared runs are {comp.comparability_status.value}: {'; '.join(comp.comparability_reasons)}",
                    impact="Cross-run metric deltas may be attributable to differing environments or data rather than strategy behavior.",
                    affected_evidence_ids=(f"ev_comp_status_{comp.comparison_id}",),
                )
            )

        # 2. Metric Differences
        for m in comp.metric_differences:
            diff_state = EvidenceStatus.AVAILABLE if m.is_available else EvidenceStatus.UNAVAILABLE
            direction_text = m.direction.value.lower()
            obs_text = (
                f"Metric '{m.metric_name}' {direction_text} from {m.left_value} to {m.right_value} "
                f"(delta={m.absolute_difference})."
                if m.is_available
                else f"Metric '{m.metric_name}' comparison unavailable: {m.reason or 'missing data'}."
            )
            items.append(
                EvidenceItem(
                    evidence_id=f"ev_comp_metric_{comp.comparison_id}_{m.metric_name}",
                    evidence_type=EvidenceType.COMPARISON,
                    source_reference=f"comparison:{comp.comparison_id}:metric:{m.metric_name}",
                    comparison_id=comp.comparison_id,
                    observation=obs_text,
                    observed_value=m.right_value,
                    expected_value=m.left_value,
                    timestamp=comp.generated_at,
                    status=diff_state,
                    supporting_references=(comp.comparison_id, comp.left_run_id, comp.right_run_id),
                )
            )

        # 3. Configuration Differences
        for c in comp.configuration_differences:
            items.append(
                EvidenceItem(
                    evidence_id=f"ev_comp_cfg_{comp.comparison_id}_{c.parameter_path}",
                    evidence_type=EvidenceType.COMPARISON,
                    source_reference=f"comparison:{comp.comparison_id}:config:{c.parameter_path}",
                    comparison_id=comp.comparison_id,
                    observation=f"Configuration parameter '{c.parameter_path}' changed ({c.diff_type.value}).",
                    timestamp=comp.generated_at,
                    status=EvidenceStatus.AVAILABLE,
                    supporting_references=(comp.comparison_id,),
                )
            )

    # =========================================================================
    # OBSERVATIONS & FACTS GENERATION
    # =========================================================================

    def _generate_observations(
        self,
        items: List[EvidenceItem],
        runs: List[AnalysisRun],
        comparison: Optional[RunComparison],
        observations: List[KeyObservation],
    ) -> None:
        """Strictly formulate FACTS and INTERPRETATIONS with mandatory traceability."""
        # 1. Facts from Runs and Items
        for run in runs:
            # Fact: Terminal lifecycle status
            observations.append(
                KeyObservation(
                    observation_id=f"fact_run_status_{run.run_id}",
                    observation_type=ObservationType.FACT,
                    statement=f"Analysis run '{run.run_id}' completed with status '{run.status.value}'.",
                    source_references=(run.run_id,),
                    observed_value=run.status.value,
                    confidence_or_status=EvidenceStatus.AVAILABLE,
                )
            )

            # Fact: Data coverage
            observations.append(
                KeyObservation(
                    observation_id=f"fact_coverage_{run.run_id}",
                    observation_type=ObservationType.FACT,
                    statement=f"Analysis run '{run.run_id}' evaluated {run.input_record_count} records across symbols: {list(run.symbol_universe)}.",
                    source_references=(run.run_id,),
                    observed_value=run.input_record_count,
                    confidence_or_status=EvidenceStatus.AVAILABLE if run.input_record_count > 0 else EvidenceStatus.UNAVAILABLE,
                )
            )

            # Facts: Key performance & risk metrics
            metrics = extract_metrics_from_result(run.execution_metadata)
            if "max_drawdown_pct" in metrics and metrics["max_drawdown_pct"] is not None:
                dd_val = metrics["max_drawdown_pct"]
                observations.append(
                    KeyObservation(
                        observation_id=f"fact_drawdown_{run.run_id}",
                        observation_type=ObservationType.FACT,
                        statement=f"Maximum observed drawdown on run '{run.run_id}' was {dd_val:.2f}%.",
                        source_references=(run.run_id,),
                        metric_name="max_drawdown_pct",
                        observed_value=dd_val,
                        confidence_or_status=EvidenceStatus.AVAILABLE,
                    )
                )

            if "total_return_pct" in metrics and metrics["total_return_pct"] is not None:
                ret_val = metrics["total_return_pct"]
                observations.append(
                    KeyObservation(
                        observation_id=f"fact_return_{run.run_id}",
                        observation_type=ObservationType.FACT,
                        statement=f"Total return on run '{run.run_id}' was {ret_val:.2f}%.",
                        source_references=(run.run_id,),
                        metric_name="total_return_pct",
                        observed_value=ret_val,
                        confidence_or_status=EvidenceStatus.AVAILABLE,
                    )
                )

            if "trade_count" in metrics and metrics["trade_count"] is not None:
                tc_val = metrics["trade_count"]
                observations.append(
                    KeyObservation(
                        observation_id=f"fact_trades_{run.run_id}",
                        observation_type=ObservationType.FACT,
                        statement=f"Total executed trade count for run '{run.run_id}' was {int(tc_val)}.",
                        source_references=(run.run_id,),
                        metric_name="trade_count",
                        observed_value=int(tc_val),
                        confidence_or_status=EvidenceStatus.AVAILABLE,
                    )
                )

        # 2. Interpretations from Comparison (Clearly labelled!)
        if comparison is not None:
            # Interpretation: Comparability assessment
            comp_stmt = (
                f"The candidate run '{comparison.right_run_id}' is directly comparable to baseline '{comparison.left_run_id}'."
                if comparison.comparability_status == ComparabilityStatus.COMPARABLE
                else f"The candidate run '{comparison.right_run_id}' is partially comparable or divergent from baseline '{comparison.left_run_id}' due to: {'; '.join(comparison.comparability_reasons)}."
            )
            observations.append(
                KeyObservation(
                    observation_id=f"interp_comparability_{comparison.comparison_id}",
                    observation_type=ObservationType.INTERPRETATION,
                    statement=comp_stmt,
                    source_references=(comparison.comparison_id, comparison.left_run_id, comparison.right_run_id),
                    confidence_or_status=EvidenceStatus.AVAILABLE if comparison.comparability_status == ComparabilityStatus.COMPARABLE else EvidenceStatus.PARTIAL,
                    rationale="Evaluated across 11 architectural comparability dimensions.",
                )
            )

            # Interpretations from Metric Deltas (Neutral wording, no winner!)
            for m in comparison.metric_differences:
                if not m.is_available:
                    continue
                if m.direction == MetricDirection.UNCHANGED:
                    continue

                dir_word = "greater" if m.direction == MetricDirection.INCREASED else "less"
                obs_stmt = (
                    f"Candidate run experienced {dir_word} '{m.metric_name}' than baseline "
                    f"({m.left_value} -> {m.right_value}, delta={m.absolute_difference})."
                )
                observations.append(
                    KeyObservation(
                        observation_id=f"interp_delta_{comparison.comparison_id}_{m.metric_name}",
                        observation_type=ObservationType.INTERPRETATION,
                        statement=obs_stmt,
                        source_references=(comparison.comparison_id, comparison.left_run_id, comparison.right_run_id),
                        metric_name=m.metric_name,
                        observed_value=m.absolute_difference,
                        confidence_or_status=EvidenceStatus.AVAILABLE,
                        rationale="Directional numeric delta between compared executions.",
                    )
                )

    # =========================================================================
    # CONFLICT DETECTION
    # =========================================================================

    def _detect_conflicts(
        self,
        items: List[EvidenceItem],
        comparison: Optional[RunComparison],
        conflicts: List[EvidenceConflict],
    ) -> None:
        """Detect contradictory signals between evidence items without choosing a winner."""
        # 1. Conflict: Risk vs. Performance Tension
        if comparison is not None:
            ret_diff = next((m for m in comparison.metric_differences if m.metric_name in ("total_return_pct", "net_profit") and m.is_available), None)
            dd_diff = next((m for m in comparison.metric_differences if m.metric_name in ("max_drawdown_pct", "maximum_drawdown") and m.is_available), None)

            if ret_diff and dd_diff:
                # If return increased, but drawdown also increased (i.e. larger drawdown = more risk)
                if ret_diff.direction == MetricDirection.INCREASED and dd_diff.direction == MetricDirection.INCREASED:
                    conflicts.append(
                        EvidenceConflict(
                            conflict_id=f"conf_risk_perf_{comparison.comparison_id}",
                            evidence_references=(
                                f"ev_comp_metric_{comparison.comparison_id}_{ret_diff.metric_name}",
                                f"ev_comp_metric_{comparison.comparison_id}_{dd_diff.metric_name}",
                            ),
                            conflict_type=ConflictType.RISK_PERFORMANCE_TENSION,
                            description=(
                                f"Performance metric '{ret_diff.metric_name}' increased by {ret_diff.absolute_difference}, "
                                f"but downside risk metric '{dd_diff.metric_name}' also increased by {dd_diff.absolute_difference}."
                            ),
                            severity=ConflictSeverity.HIGH,
                            resolution_status=ConflictResolutionStatus.UNRESOLVED,
                        )
                    )

            # 2. Conflict: Quality degradation between runs
            if comparison.quality_differences:
                conflicts.append(
                    EvidenceConflict(
                        conflict_id=f"conf_quality_{comparison.comparison_id}",
                        evidence_references=(f"ev_comp_status_{comparison.comparison_id}",),
                        conflict_type=ConflictType.QUALITY_DEGRADATION,
                        description=(
                            f"Compared runs exhibit data quality divergence: {len(comparison.quality_differences)} quality condition differences detected."
                        ),
                        severity=ConflictSeverity.MEDIUM,
                        resolution_status=ConflictResolutionStatus.UNRESOLVED,
                    )
                )

            # 3. Conflict: Provenance divergence
            if comparison.provenance_differences:
                conflicts.append(
                    EvidenceConflict(
                        conflict_id=f"conf_prov_{comparison.comparison_id}",
                        evidence_references=(f"ev_comp_status_{comparison.comparison_id}",),
                        conflict_type=ConflictType.PROVENANCE_MISMATCH,
                        description=(
                            f"Compared runs evaluated different underlying data provenance lineages: "
                            f"{len(comparison.provenance_differences)} provenance differences detected."
                        ),
                        severity=ConflictSeverity.MEDIUM,
                        resolution_status=ConflictResolutionStatus.UNRESOLVED,
                    )
                )

        # 4. Conflict: Positive return recorded despite degraded/invalid market data
        invalid_dq_items = [it for it in items if it.evidence_type == EvidenceType.DATA_QUALITY and it.status in (EvidenceStatus.INVALID, EvidenceStatus.STALE, EvidenceStatus.PARTIAL)]
        positive_perf_items = [it for it in items if it.evidence_type in (EvidenceType.BACKTEST, EvidenceType.PORTFOLIO_PERFORMANCE) and it.observed_value is not None and isinstance(it.observed_value, (int, float)) and it.observed_value > 0]

        if invalid_dq_items and positive_perf_items:
            conflicts.append(
                EvidenceConflict(
                    conflict_id="conf_dq_vs_perf",
                    evidence_references=(invalid_dq_items[0].evidence_id, positive_perf_items[0].evidence_id),
                    conflict_type=ConflictType.QUALITY_DEGRADATION,
                    description="Performance metrics show positive outcomes, but underlying market data quality is degraded or stale.",
                    severity=ConflictSeverity.CRITICAL if invalid_dq_items[0].status == EvidenceStatus.INVALID else ConflictSeverity.HIGH,
                    resolution_status=ConflictResolutionStatus.UNRESOLVED,
                )
            )

    # =========================================================================
    # SAMPLE COVERAGE & RISK SUMMARIES
    # =========================================================================

    def _compute_sample_coverage(self, runs: List[AnalysisRun]) -> SampleCoverage:
        """Preserve and summarize sample information without arbitrary sufficiency thresholds."""
        total_records = 0
        total_trades = 0
        symbols: Set[str] = set()
        timeframes: Set[str] = set()
        min_start: Optional[float] = None
        max_end: Optional[float] = None
        has_trades = False

        for r in runs:
            total_records += r.input_record_count
            symbols.update(r.symbol_universe)
            timeframes.update(r.timeframe_universe)
            if r.data_start is not None:
                min_start = r.data_start if min_start is None else min(min_start, r.data_start)
            if r.data_end is not None:
                max_end = r.data_end if max_end is None else max(max_end, r.data_end)

            metrics = extract_metrics_from_result(r.execution_metadata)
            if "trade_count" in metrics and metrics["trade_count"] is not None:
                total_trades += int(metrics["trade_count"])
                has_trades = True

        duration_sec: Optional[float] = None
        duration_days: Optional[float] = None
        if min_start is not None and max_end is not None and max_end >= min_start:
            duration_sec = max_end - min_start
            duration_days = round(duration_sec / 86400.0, 2)

        notes = []
        if total_records < 100:
            notes.append(f"Observed record count ({total_records}) is limited.")
        if has_trades and total_trades < 30:
            notes.append(f"Observed trade count ({total_trades}) is limited.")

        return SampleCoverage(
            record_count=total_records if total_records > 0 else None,
            trade_count=total_trades if has_trades else None,
            duration_seconds=duration_sec,
            duration_days=duration_days,
            symbol_count=len(symbols) if symbols else None,
            timeframe_count=len(timeframes) if timeframes else None,
            notes=tuple(notes),
        )

    def _formulate_risk_summary(
        self,
        runs: List[AnalysisRun],
        metrics: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Consolidate risk metrics across runs preserving missing/zero semantics."""
        summary: Dict[str, Any] = {
            "max_drawdown_observed": None,
            "worst_case_drawdown_run": None,
            "volatility_observed": None,
            "risk_status": "CONSTRAINED",
        }

        worst_dd: Optional[float] = None
        worst_run: Optional[str] = None

        for r in runs:
            m = extract_metrics_from_result(r.execution_metadata)
            dd = m.get("max_drawdown_pct")
            if dd is not None:
                if worst_dd is None or dd > worst_dd:
                    worst_dd = dd
                    worst_run = r.run_id

        if worst_dd is not None:
            summary["max_drawdown_observed"] = worst_dd
            summary["worst_case_drawdown_run"] = worst_run

        return summary

    # =========================================================================
    # VOICE SUMMARY GENERATION
    # =========================================================================

    def _synthesize_voice_summary(
        self,
        subject: str,
        items: List[EvidenceItem],
        observations: List[KeyObservation],
        conflicts: List[EvidenceConflict],
        risk_summary: Dict[str, Any],
        comparison: Optional[RunComparison],
    ) -> EvidenceVoiceSummary:
        """Formulate a concise, deterministic voice summary without trading instructions."""
        facts_count = sum(1 for obs in observations if obs.observation_type == ObservationType.FACT)
        interp_count = sum(1 for obs in observations if obs.observation_type == ObservationType.INTERPRETATION)

        headline = f"Evidence synthesis for {subject} compiled {facts_count} facts and {interp_count} interpretations."

        # Data quality
        invalid_items = [it for it in items if it.status == EvidenceStatus.INVALID]
        stale_items = [it for it in items if it.status == EvidenceStatus.STALE]
        if invalid_items:
            dq_speech = f"Attention: {len(invalid_items)} evidence items indicate critical data quality issues."
        elif stale_items:
            dq_speech = f"Notice: {len(stale_items)} evidence items reflect stale market data."
        else:
            dq_speech = "Underlying market data quality and provenance checks passed with no critical issues."

        # Comparison
        if comparison is not None:
            comp_speech = f"Comparison status is {comparison.comparability_status.value} with {len(comparison.metric_differences)} tracked metric deltas."
        else:
            comp_speech = "Single run evaluation; no cross-run comparison evaluated."

        # Risk
        worst_dd = risk_summary.get("max_drawdown_observed")
        if worst_dd is not None:
            risk_speech = f"Maximum observed peak-to-trough drawdown was {worst_dd:.1f} percent."
        else:
            risk_speech = "No explicit drawdown or downside risk metrics were available in evaluated runs."

        # Conflicts & Limitations
        if conflicts:
            lim_speech = (
                f"Detected {len(conflicts)} unresolved evidence conflicts. "
                "Decision support only. Historical performance does not guarantee future outcomes, and trading execution remains locked."
            )
        else:
            lim_speech = (
                "Decision support only. Historical performance does not guarantee future outcomes, and trading execution remains locked."
            )

        full_script = f"{headline} {dq_speech} {comp_speech} {risk_speech} {lim_speech}"

        return EvidenceVoiceSummary(
            headline=headline,
            data_quality_speech=dq_speech,
            comparison_speech=comp_speech,
            risk_speech=risk_speech,
            limitations_speech=lim_speech,
            full_speech_script=full_script,
        )

    # =========================================================================
    # TELEMETRY & STATESTORE
    # =========================================================================

    def _publish_success(self, report: EvidenceSynthesisReport) -> None:
        """Publish EVIDENCE_SYNTHESIS_GENERATED event to the event bus."""
        if not self.event_bus:
            return
        try:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.EVIDENCE_SYNTHESIS_GENERATED,
                    source="evidence_synthesis_engine",
                    payload={
                        "report_id": report.report_id,
                        "subject": report.subject,
                        "source_run_count": len(report.source_run_ids),
                        "evidence_count": len(report.evidence_items),
                        "status": report.overall_evidence_status.value,
                        "bounded_summary": {
                            "facts_count": len(report.facts),
                            "interpretations_count": len(report.interpretations),
                            "conflicts_count": len(report.conflicts),
                            "has_conflicts": report.has_conflicts,
                        },
                    },
                )
            )
        except Exception as exc:
            logger.warning("Failed to publish EVIDENCE_SYNTHESIS_GENERATED event: %s", exc)

    def _publish_failure(self, subject: str, error_reason: str) -> None:
        """Publish EVIDENCE_SYNTHESIS_FAILED event to the event bus."""
        if not self.event_bus:
            return
        try:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.EVIDENCE_SYNTHESIS_FAILED,
                    source="evidence_synthesis_engine",
                    payload={
                        "subject": sanitize_provenance_string(subject),
                        "status": "FAILED",
                        "error_category": "SYNTHESIS_FAILED",
                        "reason": sanitize_provenance_string(error_reason),
                    },
                )
            )
        except Exception as exc:
            logger.warning("Failed to publish EVIDENCE_SYNTHESIS_FAILED event: %s", exc)

    def _update_state_store(self, report: EvidenceSynthesisReport) -> None:
        """Update StateStore with latest report and bounded history."""
        if not self.state_store:
            return
        try:
            # Latest report dict
            rep_dict = report.to_dict()
            self.state_store.set("evidence_synthesis:latest", rep_dict)

            # Bounded summary history
            existing_history = self.state_store.get("evidence_synthesis:history", [])
            if not isinstance(existing_history, list):
                existing_history = []

            summary_item = {
                "report_id": report.report_id,
                "generated_at": report.generated_at,
                "subject": report.subject,
                "scope": report.scope,
                "overall_status": report.overall_evidence_status.value,
                "evidence_items_count": len(report.evidence_items),
                "conflicts_count": len(report.conflicts),
            }

            updated_history = [summary_item] + existing_history
            if len(updated_history) > self.max_history:
                updated_history = updated_history[: self.max_history]

            self.state_store.set("evidence_synthesis:history", updated_history)
        except Exception as exc:
            logger.warning("Failed to update StateStore for evidence synthesis: %s", exc)
