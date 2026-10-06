"""KAISER — Deterministic Opportunity Assessment Engine.

Orchestrates evidence-grounded assessment of analyzed market situations into
structured OpportunityCandidate records for decision support.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Determinism: Given identical structured evidence inputs, outputs are strictly equivalent.
2. Decision support only: NEVER outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE.
3. Absolutely no order execution fields: Prohibits action, side, order, entry/exit price,
   position size, leverage, stop-loss, take-profit, or broker orders.
4. No profitability claims: No numeric probability of profit or fake confidence percentage.
5. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
6. Traceability: Every candidate references originating evidence report and runs.
7. Bounded state: All histories, collections, and caches have strict capacity limits.
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
from app.analysis.evidence.engine import EvidenceSynthesisEngine
from app.analysis.evidence.models import (
    EvidenceConflict,
    EvidenceItem,
    EvidenceStatus,
    EvidenceSynthesisReport,
    EvidenceType,
    EvidenceUncertainty,
    KeyObservation,
    MissingEvidence,
    ObservationType,
    SampleCoverage,
)
from app.analysis.opportunities.models import (
    DEFAULT_MAX_CONFLICTING_REFS,
    DEFAULT_MAX_LIMITATION_REFS,
    DEFAULT_MAX_OPPORTUNITY_HISTORY,
    DEFAULT_MAX_SUPPORTING_REFS,
    DEFAULT_MAX_UNCERTAINTY_REFS,
    ConfidenceState,
    ObservationWindow,
    OpportunityCandidate,
    OpportunityState,
    OpportunityVoiceSummary,
    parse_data_quality_status,
    validate_no_prohibited_fields,
)


from app.analysis.runs.models import AnalysisRun, OperationType, RunStatus
from app.analysis.runs.registry import AnalysisRunRegistry
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import (
    CREDENTIAL_KEY_PATTERNS,
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)
from app.market.quality.status import DataQualityStatus

logger = logging.getLogger("kaiser.analysis.opportunities.engine")


class OpportunityAssessmentEngine:
    """Deterministic, thread-safe engine for evaluating analytical opportunity candidates."""

    def __init__(
        self,
        evidence_engine: Optional[EvidenceSynthesisEngine] = None,
        run_registry: Optional[AnalysisRunRegistry] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        clock: Optional[Callable[[], float]] = None,
        max_history: int = DEFAULT_MAX_OPPORTUNITY_HISTORY,
    ) -> None:
        """Initialize the Opportunity Assessment Engine.

        Args:
            evidence_engine: Optional EvidenceSynthesisEngine for resolving reports.
            run_registry: Optional AnalysisRunRegistry for resolving execution runs.
            event_bus: Optional EventBus for telemetry notifications.
            state_store: Optional StateStore for persisting bounded state.
            clock: Time provider function (defaults to time.time).
            max_history: Maximum capacity of in-memory candidate history.
        """
        self.evidence_engine = evidence_engine
        self.run_registry = run_registry
        self.event_bus = event_bus
        self.state_store = state_store
        self.clock = clock or time.time
        self.max_history = max(1, min(1000, max_history))

        self._lock = threading.RLock()
        self._history: collections.deque[OpportunityCandidate] = collections.deque(maxlen=self.max_history)
        self._cache: Dict[str, OpportunityCandidate] = {}

    def assess(
        self,
        report: Union[EvidenceSynthesisReport, Dict[str, Any]],
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        observation_window: Optional[ObservationWindow] = None,
        candidate_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> OpportunityCandidate:
        """Assess an evidence synthesis report into an immutable OpportunityCandidate.

        Args:
            report: EvidenceSynthesisReport instance or serialized dictionary.
            symbol: Target market symbol (optional; inferred from report subject if omitted).
            timeframe: Target timeframe (optional; inferred from scope/runs if omitted).
            observation_window: Optional explicit observation window bounds.
            candidate_id: Optional specific candidate ID.
            metadata: Optional auxiliary metadata.

        Returns:
            OpportunityCandidate: Immutable decision-support assessment.

        Raises:
            ValidationError: If inputs are invalid or contain prohibited execution fields.
        """
        now = self.clock()

        try:
            # 1. Validate and resolve report
            rep_obj: Optional[EvidenceSynthesisReport] = None
            if isinstance(report, EvidenceSynthesisReport):
                rep_obj = report
            elif isinstance(report, dict):
                validate_no_prohibited_fields(report, path="report")
                # If report has a report_id and evidence_engine is available, try to resolve complete report
                rep_id = report.get("report_id")
                if rep_id and self.evidence_engine:
                    rep_obj = self.evidence_engine.get_report(rep_id)

            if rep_obj is None and not isinstance(report, dict):
                raise ValidationError("Assessment requires a valid EvidenceSynthesisReport or serialized dictionary.")

            # Extract fields from report
            rep_id_val = rep_obj.report_id if rep_obj else str(report.get("report_id", ""))
            if not rep_id_val:
                raise ValidationError("Evidence report must have a valid non-empty 'report_id'.")

            subject_val = rep_obj.subject if rep_obj else str(report.get("subject", ""))
            scope_val = rep_obj.scope if rep_obj else str(report.get("scope", ""))
            ev_status = rep_obj.overall_evidence_status if rep_obj else EvidenceStatus.from_string(report.get("overall_status", "AVAILABLE"))
            source_run_ids = tuple(rep_obj.source_run_ids if rep_obj else report.get("source_run_ids", ()))

            # Validate input metadata
            clean_metadata = copy.deepcopy(metadata or {})
            validate_no_prohibited_fields(clean_metadata, path="metadata")

            # 2. Infer Symbol & Timeframe
            resolved_symbol = str(symbol).strip() if symbol else subject_val.strip()
            if not resolved_symbol:
                resolved_symbol = "UNSPECIFIED"

            resolved_timeframe = str(timeframe).strip() if timeframe else "1h"
            if not timeframe and "multi" in scope_val.lower():
                resolved_timeframe = "multi"

            # 3. Resolve runs if registry is available
            source_runs: List[AnalysisRun] = []
            if self.run_registry and source_run_ids:
                for rid in source_run_ids:
                    r = self.run_registry.get_run(rid)
                    if r:
                        source_runs.append(r)

            # 4. Multi-Timeframe Analysis Evaluation
            multi_tf_state = self._evaluate_multi_timeframe(rep_obj, report, source_runs)

            # 5. Data Quality & Provenance Verification
            dq_status, dq_findings = self._evaluate_data_quality(rep_obj, report, source_runs)
            prov_status, prov_reasons = self._evaluate_provenance(rep_obj, report, source_runs)
            repro_status, repro_id = self._evaluate_reproducibility(rep_obj, report, source_runs)

            # 6. Sample Coverage Evaluation
            coverage = rep_obj.sample_coverage if rep_obj else None
            if coverage is None and isinstance(report, dict) and report.get("sample_coverage"):
                sc_dict = report["sample_coverage"]
                coverage = SampleCoverage(
                    record_count=sc_dict.get("record_count", 0),
                    trade_count=sc_dict.get("trade_count", 0),
                    window_count=sc_dict.get("window_count"),
                    oos_window_count=sc_dict.get("oos_window_count"),
                    duration_seconds=sc_dict.get("duration_seconds"),
                    duration_days=sc_dict.get("duration_days"),
                    symbol_count=sc_dict.get("symbol_count"),
                    timeframe_count=sc_dict.get("timeframe_count"),
                    notes=tuple(sc_dict.get("notes", ())),
                )

            # 7. Collect Supporting, Conflicting, Uncertainty, and Limitation References
            supporting_refs, conflicting_refs, uncertainty_refs, limitation_refs, key_obs = self._extract_evidence_references(
                rep_obj=rep_obj,
                report_dict=report if isinstance(report, dict) else {},
                source_runs=source_runs,
                multi_tf_state=multi_tf_state,
                dq_findings=dq_findings,
                prov_reasons=prov_reasons,
                coverage=coverage,
            )

            # 8. Deterministic Evidence Completeness Score Calculation
            score, score_breakdown = self._calculate_evidence_completeness_score(
                rep_obj=rep_obj,
                report_dict=report if isinstance(report, dict) else {},
                dq_status=dq_status,
                prov_status=prov_status,
                repro_status=repro_status,
                coverage=coverage,
                conflicting_refs_count=len(conflicting_refs),
            )

            # 9. Assign Deterministic OpportunityState & ConfidenceState
            state = self._determine_assessment_state(
                rep_obj=rep_obj,
                report_dict=report if isinstance(report, dict) else {},
                ev_status=ev_status,
                dq_status=dq_status,
                conflicting_refs=conflicting_refs,
                supporting_refs=supporting_refs,
                score=score,
            )

            confidence = self._determine_confidence_state(
                state=state,
                score=score,
                dq_status=dq_status,
                prov_status=prov_status,
                repro_status=repro_status,
            )

            # 10. Generate Analytical Invalidation Conditions
            invalidation_conditions = self._generate_invalidation_conditions(
                symbol=resolved_symbol,
                timeframe=resolved_timeframe,
                dq_status=dq_status,
                prov_status=prov_status,
            )

            # 11. Generate Deterministic Voice Summary
            voice_summary = self._generate_voice_summary(
                symbol=resolved_symbol,
                timeframe=resolved_timeframe,
                state=state,
                score=score,
                dq_status=dq_status,
                conflicts_count=len(conflicting_refs),
                uncertainty_count=len(uncertainty_refs),
            )

            # Construct Candidate
            cid = candidate_id or f"opp_{uuid.uuid4().hex[:12]}"
            candidate = OpportunityCandidate(
                candidate_id=cid,
                symbol=resolved_symbol,
                timeframe=resolved_timeframe,
                assessment_state=state,
                evidence_status=ev_status,
                evidence_completeness_score=score,
                confidence_state=confidence,
                source_evidence_report_id=rep_id_val,
                generated_at=now,
                data_quality_status=dq_status,
                provenance_status=prov_status,
                reproducibility_status=repro_status,
                observation_window=observation_window,
                sample_coverage=coverage,
                supporting_evidence_refs=supporting_refs,
                conflicting_evidence_refs=conflicting_refs,
                uncertainty_refs=uncertainty_refs,
                limitation_refs=limitation_refs,
                key_observations=key_obs,
                invalidation_conditions=invalidation_conditions,
                source_analysis_run_ids=source_run_ids,
                reproducibility_id=repro_id,
                evidence_completeness_breakdown=score_breakdown,
                multi_timeframe_alignment=multi_tf_state,
                voice_summary=voice_summary,
                metadata=clean_metadata,
            )

            # Cache and persist state
            self._store_candidate(candidate)

            # Publish event
            self._publish_event(
                event_type=EventType.OPPORTUNITY_ASSESSMENT_GENERATED,
                payload={
                    "candidate_id": candidate.candidate_id,
                    "symbol": candidate.symbol,
                    "timeframe": candidate.timeframe,
                    "assessment_state": candidate.assessment_state.value,
                    "evidence_completeness_score": candidate.evidence_completeness_score,
                    "confidence_state": candidate.confidence_state.value,
                    "source_evidence_report_id": candidate.source_evidence_report_id,
                    "supporting_refs_count": len(candidate.supporting_evidence_refs),
                    "conflicting_refs_count": len(candidate.conflicting_evidence_refs),
                },
            )

            return candidate

        except ValidationError as val_err:
            self._publish_event(
                event_type=EventType.OPPORTUNITY_ASSESSMENT_FAILED,
                payload={
                    "error_category": "VALIDATION_ERROR",
                    "error_message": str(val_err),
                },
            )
            raise
        except Exception as exc:
            logger.error("Unhandled error generating opportunity assessment: %s", exc, exc_info=True)
            self._publish_event(
                event_type=EventType.OPPORTUNITY_ASSESSMENT_FAILED,
                payload={
                    "error_category": "INTERNAL_ERROR",
                    "error_message": sanitize_provenance_string(str(exc)),
                },
            )
            raise ValidationError(f"Opportunity assessment failed: {exc}") from exc

    def get_assessment(self, candidate_id: str) -> Optional[OpportunityCandidate]:
        """Retrieve an assessment by candidate ID from bounded cache or StateStore."""
        if not candidate_id or not isinstance(candidate_id, str):
            return None
        clean_id = candidate_id.strip()

        with self._lock:
            cached = self._cache.get(clean_id)
            if cached:
                return cached

        # Fallback to state store
        if self.state_store:
            latest = self.state_store.get("opportunity_assessment:latest", None)
            if latest and isinstance(latest, dict) and latest.get("candidate_id") == clean_id:
                try:
                    return self._dict_to_candidate(latest)
                except Exception:
                    pass

            history = self.state_store.get("opportunity_assessment:history", [])
            for item in history:
                if isinstance(item, dict) and item.get("candidate_id") == clean_id:
                    try:
                        return self._dict_to_candidate(item)
                    except Exception:
                        pass

        return None

    def list_assessments(self, limit: int = 50, offset: int = 0) -> List[OpportunityCandidate]:
        """List assessment history with bounded pagination."""
        limit_clamped = max(1, min(100, int(limit)))
        offset_clamped = max(0, int(offset))

        with self._lock:
            all_items = list(self._history)

        return all_items[offset_clamped : offset_clamped + limit_clamped]

    def invalidate_assessment(self, candidate_id: str, reason: str) -> Optional[OpportunityCandidate]:
        """Deterministically transition an assessment to INVALIDATED state.

        Args:
            candidate_id: ID of candidate to invalidate.
            reason: Analytical reason for invalidation.

        Returns:
            OpportunityCandidate: Updated invalidated candidate.
        """
        if not candidate_id or not isinstance(candidate_id, str):
            raise ValidationError("candidate_id must be a non-empty string.")
        if not reason or not isinstance(reason, str):
            raise ValidationError("reason must be a non-empty string.")

        clean_id = candidate_id.strip()
        existing = self.get_assessment(clean_id)
        if not existing:
            return None

        # Create invalidated version
        new_obs = list(existing.key_observations) + [f"INVALIDATION: {reason.strip()}"]
        new_meta = dict(existing.metadata)
        new_meta["invalidated_at"] = self.clock()
        new_meta["invalidation_reason"] = reason.strip()

        invalidated = OpportunityCandidate(
            candidate_id=existing.candidate_id,
            symbol=existing.symbol,
            timeframe=existing.timeframe,
            assessment_state=OpportunityState.INVALIDATED,
            evidence_status=EvidenceStatus.INVALID,
            evidence_completeness_score=0.0,
            confidence_state=ConfidenceState.UNVERIFIED,
            source_evidence_report_id=existing.source_evidence_report_id,
            generated_at=existing.generated_at,
            data_quality_status=existing.data_quality_status,
            provenance_status=existing.provenance_status,
            reproducibility_status=existing.reproducibility_status,
            observation_window=existing.observation_window,
            sample_coverage=existing.sample_coverage,
            supporting_evidence_refs=existing.supporting_evidence_refs,
            conflicting_evidence_refs=tuple(list(existing.conflicting_evidence_refs) + [f"invalidation:{reason.strip()}"]),
            uncertainty_refs=existing.uncertainty_refs,
            limitation_refs=existing.limitation_refs,
            key_observations=tuple(new_obs),
            invalidation_conditions=existing.invalidation_conditions,
            source_analysis_run_ids=existing.source_analysis_run_ids,
            reproducibility_id=existing.reproducibility_id,
            evidence_completeness_breakdown=dict(existing.evidence_completeness_breakdown),
            multi_timeframe_alignment=existing.multi_timeframe_alignment,
            voice_summary=None,
            metadata=new_meta,
        )

        self._store_candidate(invalidated)

        self._publish_event(
            event_type=EventType.OPPORTUNITY_ASSESSMENT_INVALIDATED,
            payload={
                "candidate_id": invalidated.candidate_id,
                "symbol": invalidated.symbol,
                "invalidation_reason": reason.strip(),
                "timestamp": self.clock(),
            },
        )

        return invalidated

    # =========================================================================
    # INTERNAL EVALUATION METHODS
    # =========================================================================

    def _evaluate_data_quality(
        self,
        rep_obj: Optional[EvidenceSynthesisReport],
        report_dict: Dict[str, Any],
        source_runs: Sequence[AnalysisRun],
    ) -> Tuple[Optional[DataQualityStatus], List[str]]:
        """Evaluate data quality integrity across report summaries and source runs."""
        findings: List[str] = []
        dq_status: Optional[DataQualityStatus] = None

        dq_summary = rep_obj.data_quality_summary if rep_obj else report_dict.get("data_quality_summary", {})
        if dq_summary and isinstance(dq_summary, dict):
            status_str = dq_summary.get("status") or dq_summary.get("overall_status")
            if status_str:
                dq_status = parse_data_quality_status(status_str)

        # Inspect source runs
        for run in source_runs:
            if run.quality_status:
                if dq_status is None:
                    dq_status = run.quality_status
                elif run.quality_status == DataQualityStatus.INVALID:
                    dq_status = DataQualityStatus.INVALID

        # Inspect evidence items for anomalies
        items = rep_obj.evidence_items if rep_obj else [EvidenceItem.from_dict(i) for i in report_dict.get("evidence_items", []) if isinstance(i, dict)]
        for it in items:
            if it.evidence_type == EvidenceType.DATA_ANOMALY:
                findings.append(f"anomaly:{it.observation}")
                if it.status == EvidenceStatus.INVALID:
                    dq_status = DataQualityStatus.INVALID

        if dq_status == DataQualityStatus.INVALID:
            findings.append("Data quality validation failed: INVALID status detected.")
        elif dq_status == DataQualityStatus.DEGRADED:
            findings.append("Data quality status is DEGRADED: historical or market bars contain warnings.")

        return dq_status, findings

    def _evaluate_provenance(
        self,
        rep_obj: Optional[EvidenceSynthesisReport],
        report_dict: Dict[str, Any],
        source_runs: Sequence[AnalysisRun],
    ) -> Tuple[str, List[str]]:
        """Evaluate provenance completeness and audit lineage."""
        reasons: List[str] = []
        prov_summary = rep_obj.provenance_summary if rep_obj else report_dict.get("provenance_summary", {})

        has_runs = len(source_runs) > 0 or len(rep_obj.source_run_ids if rep_obj else report_dict.get("source_run_ids", ())) > 0
        if not has_runs:
            return "MISSING", ["No source execution runs linked to assessment."]

        has_prov = False
        if prov_summary and isinstance(prov_summary, dict) and prov_summary.get("provenance_records_count", 0) > 0:
            has_prov = True
        for r in source_runs:
            if r.input_provenance_ids:
                has_prov = True
                break

        if has_prov:
            return "VERIFIED", []
        reasons.append("Source runs lack recorded input provenance IDs.")
        return "PARTIAL", reasons

    def _evaluate_reproducibility(
        self,
        rep_obj: Optional[EvidenceSynthesisReport],
        report_dict: Dict[str, Any],
        source_runs: Sequence[AnalysisRun],
    ) -> Tuple[str, Optional[str]]:
        """Evaluate reproducibility identity and configuration fingerprints."""
        repro_refs = rep_obj.reproducibility_references if rep_obj else report_dict.get("reproducibility_references", ())
        primary_repro_id: Optional[str] = None

        if repro_refs:
            primary_repro_id = str(repro_refs[0])

        for r in source_runs:
            if r.reproducibility_id:
                primary_repro_id = r.reproducibility_id
                break

        if primary_repro_id:
            return "REPRODUCIBLE", primary_repro_id
        return "UNVERIFIED", None

    def _evaluate_multi_timeframe(
        self,
        rep_obj: Optional[EvidenceSynthesisReport],
        report_dict: Dict[str, Any],
        source_runs: Sequence[AnalysisRun],
    ) -> Optional[str]:
        """Evaluate multi-timeframe analytical alignment without directional trade bias."""
        timeframes: Set[str] = set()
        for r in source_runs:
            if r.timeframe_universe:
                timeframes.update(r.timeframe_universe)
            elif r.configuration_summary and "timeframe" in r.configuration_summary:
                timeframes.add(str(r.configuration_summary["timeframe"]))

        if len(timeframes) <= 1:
            return None

        # Check if conflicts in report mention timeframe
        conflicts = rep_obj.conflicts if rep_obj else report_dict.get("conflicts", ())
        for c in conflicts:
            desc = c.description if hasattr(c, "description") else str(c.get("description", ""))
            if "timeframe" in desc.lower():
                return "CONFLICTING"

        return "ALIGNED"

    def _extract_evidence_references(
        self,
        rep_obj: Optional[EvidenceSynthesisReport],
        report_dict: Dict[str, Any],
        source_runs: Sequence[AnalysisRun],
        multi_tf_state: Optional[str],
        dq_findings: List[str],
        prov_reasons: List[str],
        coverage: Optional[SampleCoverage],
    ) -> Tuple[Tuple[str, ...], Tuple[str, ...], Tuple[str, ...], Tuple[str, ...], Tuple[str, ...]]:
        """Strictly categorize supporting, conflicting, uncertainty, and limitation references."""
        supporting: List[str] = []
        conflicting: List[str] = []
        uncertainties: List[str] = []
        limitations: List[str] = []
        key_obs: List[str] = []

        # 1. Facts from report
        facts = rep_obj.facts if rep_obj else [KeyObservation.from_dict(f) for f in report_dict.get("facts", []) if isinstance(f, dict)]
        for f in facts:
            key_obs.append(f"FACT: {f.statement}")
            for ref in f.source_references:
                supporting.append(f"fact:{ref}")

        # 2. Conflicts from report
        confs = rep_obj.conflicts if rep_obj else [EvidenceConflict.from_dict(c) for c in report_dict.get("conflicts", []) if isinstance(c, dict)]
        for c in confs:
            conflicting.append(f"conflict:{c.conflict_type.value if hasattr(c.conflict_type, 'value') else c.conflict_type}:{c.description}")
            key_obs.append(f"CONFLICT: {c.description}")

        # Multi-timeframe conflict
        if multi_tf_state == "CONFLICTING":
            conflicting.append("multi_timeframe:inconsistent_observations_across_timeframes")
            key_obs.append("CONFLICT: Inconsistent analytical observations across multiple timeframes.")
        elif multi_tf_state == "ALIGNED":
            supporting.append("multi_timeframe:aligned_observations")
            key_obs.append("FACT: Analytical observations aligned across multiple timeframes.")

        # Data quality findings
        for dq in dq_findings:
            if "INVALID" in dq or "anomaly" in dq:
                conflicting.append(f"data_quality:{dq}")
            else:
                uncertainties.append(f"data_quality:{dq}")

        # Provenance findings
        for pr in prov_reasons:
            uncertainties.append(f"provenance:{pr}")

        # Uncertainties from report
        uncs = rep_obj.uncertainties if rep_obj else [EvidenceUncertainty.from_dict(u) for u in report_dict.get("uncertainties", []) if isinstance(u, dict)]
        for u in uncs:
            uncertainties.append(f"uncertainty:{u.source.value if hasattr(u.source, 'value') else u.source}:{u.description}")

        # Sample coverage checks
        if coverage:
            if coverage.trade_count is not None and coverage.trade_count < 10:
                uncertainties.append(f"coverage:limited_trade_count_{coverage.trade_count}")
                key_obs.append(f"UNCERTAINTY: Limited historical trade sample ({coverage.trade_count} trades).")
            if coverage.record_count is not None and coverage.record_count < 100:
                uncertainties.append(f"coverage:limited_record_count_{coverage.record_count}")

        # Limitations from report
        lims = rep_obj.limitations if rep_obj else report_dict.get("limitations", ())
        for lim in lims:
            limitations.append(f"limitation:{lim}")

        # Standard quantitative modeling limitations
        standard_lims = [
            "Historical analytical evidence does not guarantee future operational behavior.",
            "Decision support assessment only: execution instructions prohibited.",
            "Market regime non-stationarity may alter observed relationships.",
        ]
        for sl in standard_lims:
            if sl not in limitations:
                limitations.append(sl)

        return (
            tuple(sorted(set(supporting))[:DEFAULT_MAX_SUPPORTING_REFS]),
            tuple(sorted(set(conflicting))[:DEFAULT_MAX_CONFLICTING_REFS]),
            tuple(sorted(set(uncertainties))[:DEFAULT_MAX_UNCERTAINTY_REFS]),
            tuple(sorted(set(limitations))[:DEFAULT_MAX_LIMITATION_REFS]),
            tuple(key_obs[:100]),
        )

    def _calculate_evidence_completeness_score(
        self,
        rep_obj: Optional[EvidenceSynthesisReport],
        report_dict: Dict[str, Any],
        dq_status: Optional[DataQualityStatus],
        prov_status: str,
        repro_status: str,
        coverage: Optional[SampleCoverage],
        conflicting_refs_count: int,
    ) -> Tuple[float, Dict[str, float]]:
        """Calculate deterministic evidence completeness score and component breakdown.

        Score represents completeness, integrity, and auditability only (0.0 to 1.0).
        STRICTLY NOT a probability of profit.
        """
        # Component 1: Data Quality (0.25)
        dq_score = 0.0
        if dq_status == DataQualityStatus.VALID:
            dq_score = 0.25
        elif dq_status == DataQualityStatus.DEGRADED:
            dq_score = 0.12
        elif dq_status == DataQualityStatus.INVALID:
            dq_score = 0.0
        else:
            dq_score = 0.10  # unverified

        # Component 2: Provenance & Lineage (0.20)
        prov_score = 0.0
        if prov_status == "VERIFIED":
            prov_score = 0.20
        elif prov_status == "PARTIAL":
            prov_score = 0.10
        else:
            prov_score = 0.0

        # Component 3: Reproducibility (0.20)
        repro_score = 0.0
        if repro_status == "REPRODUCIBLE":
            repro_score = 0.20
        else:
            repro_score = 0.05

        # Component 4: Sample Coverage (0.20)
        cov_score = 0.0
        if coverage and (coverage.record_count > 0 or (coverage.trade_count and coverage.trade_count > 0)):
            cov_score = 0.20
            if coverage.trade_count is not None and coverage.trade_count < 10:
                cov_score = 0.10
        else:
            cov_score = 0.05

        # Component 5: Observations & Metrics (0.15)
        metrics_score = 0.15
        missing_count = len(rep_obj.missing_evidence if rep_obj else report_dict.get("missing_evidence", ()))
        if missing_count > 0:
            metrics_score = max(0.0, 0.15 - (0.03 * missing_count))

        # Conflict penalty (up to 0.15 deduction)
        conflict_penalty = min(0.15, 0.05 * conflicting_refs_count)

        raw_total = (dq_score + prov_score + repro_score + cov_score + metrics_score) - conflict_penalty
        final_score = round(max(0.0, min(1.0, raw_total)), 4)

        breakdown = {
            "data_quality_component": round(dq_score, 4),
            "provenance_component": round(prov_score, 4),
            "reproducibility_component": round(repro_score, 4),
            "sample_coverage_component": round(cov_score, 4),
            "metrics_availability_component": round(metrics_score, 4),
            "conflict_penalty": round(conflict_penalty, 4),
        }

        return final_score, breakdown

    def _determine_assessment_state(
        self,
        rep_obj: Optional[EvidenceSynthesisReport],
        report_dict: Dict[str, Any],
        ev_status: EvidenceStatus,
        dq_status: Optional[DataQualityStatus],
        conflicting_refs: Sequence[str],
        supporting_refs: Sequence[str],
        score: float,
    ) -> OpportunityState:
        """Assign deterministic OpportunityState following strict architectural hierarchy."""
        # 1. Invalid data quality
        if dq_status == DataQualityStatus.INVALID or ev_status == EvidenceStatus.INVALID:
            return OpportunityState.DATA_QUALITY_LIMITED

        # 2. Conflicting evidence
        if len(conflicting_refs) > 0 or ev_status == EvidenceStatus.CONFLICTING:
            return OpportunityState.EVIDENCE_CONFLICTING

        # 3. Insufficient evidence
        items_count = len(rep_obj.evidence_items if rep_obj else report_dict.get("evidence_items", ()))
        if items_count == 0 or len(supporting_refs) == 0 or ev_status == EvidenceStatus.UNAVAILABLE:
            return OpportunityState.INSUFFICIENT_EVIDENCE

        # 4. Partial or degraded data
        if dq_status == DataQualityStatus.DEGRADED or ev_status in (EvidenceStatus.PARTIAL, EvidenceStatus.STALE):
            return OpportunityState.EVIDENCE_PARTIAL

        # 5. Analytically supported vs requires review
        if score >= 0.75 and dq_status == DataQualityStatus.VALID and len(supporting_refs) >= 2:
            return OpportunityState.ANALYTICALLY_SUPPORTED

        return OpportunityState.REQUIRES_REVIEW

    def _determine_confidence_state(
        self,
        state: OpportunityState,
        score: float,
        dq_status: Optional[DataQualityStatus],
        prov_status: str,
        repro_status: str,
    ) -> ConfidenceState:
        """Assign categorical ConfidenceState based on audit integrity."""
        if state in (OpportunityState.DATA_QUALITY_LIMITED, OpportunityState.INVALIDATED):
            return ConfidenceState.UNVERIFIED
        if dq_status == DataQualityStatus.VALID and prov_status == "VERIFIED" and repro_status == "REPRODUCIBLE" and score >= 0.80:
            return ConfidenceState.HIGH
        if score >= 0.50 and dq_status != DataQualityStatus.INVALID:
            return ConfidenceState.MODERATE
        return ConfidenceState.LOW

    def _generate_invalidation_conditions(
        self,
        symbol: str,
        timeframe: str,
        dq_status: Optional[DataQualityStatus],
        prov_status: str,
    ) -> Tuple[str, ...]:
        """Generate deterministic analytical invalidation conditions."""
        return (
            f"Market data quality for '{symbol}' degrades to INVALID or critical anomaly detected.",
            f"Market data bar timestamps for '{symbol}' exceed 24-hour freshness threshold (STALE).",
            f"Source input provenance records become unavailable or lineage checksums mismatch.",
            f"Reproducibility fingerprint or analytical configuration diverges from registered baseline.",
            f"Sample coverage observation window falls below minimum structural criteria.",
            f"Contradictory analytical signal emerges on primary timeframe '{timeframe}'.",
            f"Source analysis run lifecycle status transitions to FAILED or CANCELLED.",
        )

    def _generate_voice_summary(
        self,
        symbol: str,
        timeframe: str,
        state: OpportunityState,
        score: float,
        dq_status: Optional[DataQualityStatus],
        conflicts_count: int,
        uncertainty_count: int,
    ) -> OpportunityVoiceSummary:
        """Construct deterministic speech script conforming to strict safety rules."""
        headline = f"KAISER opportunity assessment for {symbol} on {timeframe}."
        state_speech = f"Analytical state is {state.value.replace('_', ' ').lower()} with evidence completeness score {score:.2f}."

        quality_speech = "Market data quality is valid."
        if dq_status == DataQualityStatus.DEGRADED:
            quality_speech = "Market data quality has recorded warnings and is degraded."
        elif dq_status == DataQualityStatus.INVALID:
            quality_speech = "Market data quality has failed validation."

        coverage_speech = "Sample coverage is established."
        if conflicts_count > 0:
            coverage_speech = f"Attention: {conflicts_count} contradictory evidence signals detected."
        elif uncertainty_count > 0:
            coverage_speech = f"{uncertainty_count} analytical uncertainty caveats recorded."

        review_speech = "Decision support only. Human review is advised."

        full_script = f"{headline} {state_speech} {quality_speech} {coverage_speech} {review_speech}"

        return OpportunityVoiceSummary(
            headline=headline,
            state_speech=state_speech,
            quality_speech=quality_speech,
            coverage_speech=coverage_speech,
            review_speech=review_speech,
            full_speech_script=full_script,
        )

    def _store_candidate(self, candidate: OpportunityCandidate) -> None:
        """Thread-safe storage in memory, cache, and StateStore."""
        with self._lock:
            self._cache[candidate.candidate_id] = candidate
            self._history.appendleft(candidate)

        if self.state_store:
            try:
                c_dict = candidate.to_dict()
                self.state_store.set("opportunity_assessment:latest", copy.deepcopy(c_dict))

                with self._lock:
                    hist_dicts = [c.to_dict() for c in list(self._history)[:self.max_history]]
                self.state_store.set("opportunity_assessment:history", hist_dicts)
            except Exception as err:
                logger.warning("Failed updating StateStore for opportunity assessment: %s", err)

    def _publish_event(self, event_type: EventType | str, payload: Mapping[str, Any]) -> None:
        """Publish sanitized event to EventBus if connected."""
        if not self.event_bus:
            return

        try:
            clean_payload = sanitize_provenance_metadata(dict(payload))
            validate_no_prohibited_fields(clean_payload, path="event_payload")
            ev = Event.create(
                event_type=event_type,
                source="kaiser_opportunity_assessment_engine",
                payload=clean_payload,
            )
            self.event_bus.publish(ev)
        except Exception as err:
            logger.warning("Failed publishing event '%s': %s", event_type, err)

    def _dict_to_candidate(self, data: Dict[str, Any]) -> OpportunityCandidate:
        """Reconstruct OpportunityCandidate from dictionary."""
        obs_win = None
        if data.get("observation_window"):
            ow = data["observation_window"]
            obs_win = ObservationWindow(
                start_time=ow.get("start_time"),
                end_time=ow.get("end_time"),
                record_count=ow.get("record_count", 0),
                duration_seconds=ow.get("duration_seconds"),
            )

        cov = None
        if data.get("sample_coverage"):
            sc = data["sample_coverage"]
            cov = SampleCoverage(
                record_count=sc.get("record_count", 0),
                trade_count=sc.get("trade_count", 0),
                window_count=sc.get("window_count"),
                oos_window_count=sc.get("oos_window_count"),
                duration_seconds=sc.get("duration_seconds"),
                duration_days=sc.get("duration_days"),
                symbol_count=sc.get("symbol_count"),
                timeframe_count=sc.get("timeframe_count"),
                notes=tuple(sc.get("notes", ())),
            )

        voice = None
        if data.get("voice_summary"):
            vs = data["voice_summary"]
            voice = OpportunityVoiceSummary(
                headline=vs.get("headline", ""),
                state_speech=vs.get("state_speech", ""),
                quality_speech=vs.get("quality_speech", ""),
                coverage_speech=vs.get("coverage_speech", ""),
                review_speech=vs.get("review_speech", ""),
                full_speech_script=vs.get("full_speech_script", ""),
            )

        return OpportunityCandidate(
            candidate_id=data["candidate_id"],
            symbol=data["symbol"],
            timeframe=data["timeframe"],
            assessment_state=OpportunityState.from_string(data["assessment_state"]),
            evidence_status=EvidenceStatus.from_string(data["evidence_status"]),
            evidence_completeness_score=safe_float(data.get("evidence_completeness_score", 0.0)) or 0.0,
            confidence_state=ConfidenceState.from_string(data["confidence_state"]),
            source_evidence_report_id=data["source_evidence_report_id"],
            generated_at=data["generated_at"],
            data_quality_status=parse_data_quality_status(data["data_quality_status"]) if data.get("data_quality_status") else None,
            provenance_status=data.get("provenance_status", "UNVERIFIED"),
            reproducibility_status=data.get("reproducibility_status", "UNVERIFIED"),
            observation_window=obs_win,
            sample_coverage=cov,
            supporting_evidence_refs=tuple(data.get("supporting_evidence_refs", ())),
            conflicting_evidence_refs=tuple(data.get("conflicting_evidence_refs", ())),
            uncertainty_refs=tuple(data.get("uncertainty_refs", ())),
            limitation_refs=tuple(data.get("limitation_refs", ())),
            key_observations=tuple(data.get("key_observations", ())),
            invalidation_conditions=tuple(data.get("invalidation_conditions", ())),
            source_analysis_run_ids=tuple(data.get("source_analysis_run_ids", ())),
            reproducibility_id=data.get("reproducibility_id"),
            evidence_completeness_breakdown=dict(data.get("evidence_completeness_breakdown", {})),
            multi_timeframe_alignment=data.get("multi_timeframe_alignment"),
            voice_summary=voice,
            metadata=dict(data.get("metadata", {})),
            disclaimer=data.get("disclaimer", ""),
        )
