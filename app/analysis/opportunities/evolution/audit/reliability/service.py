"""KAISER — Historical Review Audit Reliability, Coverage & Regression Service.

Phase 2, Step 34: Thread-safe, bounded reliability validation service for assessing
audit coverage, deterministic repeatability, finding drift, and historical stability.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from app.analysis.opportunities.evolution.audit.models import (
    AuditFindingCategory,
    AuditFindingSeverity,
    HistoricalReviewAuditReport,
    RecordValidationStatus,
)
from app.analysis.opportunities.evolution.audit.reliability.models import (
    MAX_RELIABILITY_EVALUATIONS_PER_INVESTIGATION,
    AuditCoverageState,
    AuditCoverageSummary,
    AuditRegressionResult,
    AuditRegressionState,
    AuditRepeatabilityResult,
    AuditRepeatabilityState,
    AuditReliabilityVoiceSummary,
    HistoricalReviewReliabilityResult,
    ReliabilityEvaluationStatus,
    compute_deterministic_reliability_id,
)
from app.analysis.opportunities.evolution.audit.service import (
    HistoricalReviewAuditService,
)
from app.analysis.opportunities.evolution.models import (
    HistoricalReviewSnapshot,
    ReviewEvolutionRecord,
    SAFE_ID_PATTERN,
    _freeze,
    _unfreeze,
)
from app.analysis.opportunities.evolution.service import (
    HistoricalReviewEvolutionService,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore

logger = logging.getLogger(__name__)


class HistoricalReviewAuditReliabilityService:
    """Evaluates and validates the long-term reliability of historical review audits."""

    def __init__(
        self,
        audit_service: Optional[HistoricalReviewAuditService] = None,
        evolution_service: Optional[HistoricalReviewEvolutionService] = None,
        state_store: Optional[StateStore] = None,
        event_bus: Optional[EventBus] = None,
        max_evaluations: int = MAX_RELIABILITY_EVALUATIONS_PER_INVESTIGATION,
    ) -> None:
        self._audit_service = audit_service
        self._evolution_service = evolution_service
        self._state_store = state_store
        self._event_bus = event_bus
        self._max_evaluations = max(1, max_evaluations)
        self._lock = threading.RLock()

        # In-memory storage
        self._evaluations: Dict[str, HistoricalReviewReliabilityResult] = {}
        self._investigation_evaluations: Dict[str, List[str]] = {}
        self._coverage_cache: Dict[str, AuditCoverageSummary] = {}

    @property
    def max_evaluations(self) -> int:
        """Return the maximum number of evaluations retained per investigation."""
        return self._max_evaluations

    # ==============================================================================
    # 1. Audit Coverage Assessment
    # ==============================================================================

    def evaluate_coverage(self, investigation_id: str) -> AuditCoverageSummary:
        """Assess audit coverage across available historical snapshots and evolutions."""
        clean_inv = (investigation_id or "").strip()
        if not clean_inv or not SAFE_ID_PATTERN.match(clean_inv):
            raise ValidationError(f"Invalid investigation_id for coverage evaluation: '{investigation_id}'")

        with self._lock:
            # 1. Resolve snapshots from evolution service
            total_snapshots = 0
            snapshot_ids_available: List[str] = []
            if self._evolution_service is not None:
                try:
                    snaps = self._evolution_service.list_snapshots(clean_inv)
                    total_snapshots = len(snaps)
                    snapshot_ids_available = [s.snapshot_id for s in snaps]
                except Exception as ex:
                    logger.warning("Error fetching snapshots for investigation '%s': %s", clean_inv, ex)
                    return AuditCoverageSummary(
                        investigation_id=clean_inv,
                        coverage_state=AuditCoverageState.UNAVAILABLE,
                        total_snapshots=0,
                        audited_snapshots=0,
                        snapshot_coverage_ratio=0.0,
                        total_evolutions=0,
                        audited_evolutions=0,
                        evolution_coverage_ratio=0.0,
                        overall_coverage_ratio=0.0,
                        missing_history_detected=True,
                        details={"error": str(ex)},
                    )
            elif self._state_store is not None:
                # Fallback to state store
                stored_snaps = self._state_store.get(f"historical_review_snapshot:{clean_inv}:history", None)
                if not isinstance(stored_snaps, list):
                    stored_snaps = self._state_store.get("historical_review:history", [])
                if isinstance(stored_snaps, list):
                    inv_snaps = [
                        s for s in stored_snaps
                        if isinstance(s, dict) and (s.get("investigation_id") == clean_inv or not s.get("investigation_id"))
                    ]
                    total_snapshots = len(inv_snaps)
                    snapshot_ids_available = [
                        s.get("snapshot_id", "") for s in inv_snaps if s.get("snapshot_id")
                    ]

            # 2. Resolve evolutions from evolution service
            total_evolutions = 0
            evolution_ids_available: List[str] = []
            if self._evolution_service is not None:
                try:
                    if hasattr(self._evolution_service, "list_evolution_history"):
                        evols = self._evolution_service.list_evolution_history(clean_inv, limit=100)
                    elif hasattr(self._evolution_service, "list_evolutions"):
                        evols = self._evolution_service.list_evolutions(clean_inv)
                    else:
                        evols = []
                    total_evolutions = len(evols)
                    evolution_ids_available = [e.evolution_id for e in evols]
                except Exception as ex:
                    logger.warning("Error fetching evolutions for investigation '%s': %s", clean_inv, ex)
            elif self._state_store is not None:
                # Fallback to state store for evolutions
                stored_evols = self._state_store.get("historical_review_evolution:history", [])
                if isinstance(stored_evols, list):
                    inv_evols = [
                        e for e in stored_evols
                        if isinstance(e, dict) and (e.get("investigation_id") == clean_inv or not e.get("investigation_id"))
                    ]
                    total_evolutions = len(inv_evols)
                    evolution_ids_available = [
                        e.get("evolution_id", "") for e in inv_evols if e.get("evolution_id")
                    ]

            # 3. Check what has been audited
            audited_snapshot_ids: Set[str] = set()
            audited_evolution_ids: Set[str] = set()

            if self._audit_service is not None:
                try:
                    reports = self._audit_service.list_audit_reports(clean_inv, limit=100)
                    for r in reports:
                        for s_res in r.snapshot_results:
                            audited_snapshot_ids.add(s_res.snapshot_id)
                        for e_res in r.evolution_results:
                            audited_evolution_ids.add(e_res.evolution_id)
                except Exception as ex:
                    logger.warning("Error inspecting audit reports for investigation '%s': %s", clean_inv, ex)
            elif self._state_store is not None:
                # Fallback to state store audit reports
                stored_reports = self._state_store.get("historical_review_audit:history", [])
                if isinstance(stored_reports, list):
                    for r in stored_reports:
                        if isinstance(r, dict) and r.get("investigation_id") == clean_inv:
                            for s_res in r.get("snapshot_results", []):
                                if isinstance(s_res, dict) and s_res.get("snapshot_id"):
                                    audited_snapshot_ids.add(s_res["snapshot_id"])
                            for e_res in r.get("evolution_results", []):
                                if isinstance(e_res, dict) and e_res.get("evolution_id"):
                                    audited_evolution_ids.add(e_res["evolution_id"])

            audited_snapshots = len(audited_snapshot_ids.intersection(set(snapshot_ids_available)))
            audited_evolutions = len(audited_evolution_ids.intersection(set(evolution_ids_available)))

            unaudited_snaps = tuple(s for s in snapshot_ids_available if s not in audited_snapshot_ids)
            unaudited_evols = tuple(e for e in evolution_ids_available if e not in audited_evolution_ids)

            # Ratios
            snap_ratio = (audited_snapshots / total_snapshots) if total_snapshots > 0 else 1.0
            evol_ratio = (audited_evolutions / total_evolutions) if total_evolutions > 0 else 1.0

            total_entities = total_snapshots + total_evolutions
            audited_entities = audited_snapshots + audited_evolutions
            overall_ratio = (audited_entities / total_entities) if total_entities > 0 else 1.0

            # State classification
            if total_snapshots == 0:
                coverage_state = AuditCoverageState.EMPTY
            elif snap_ratio >= 1.0 and (total_evolutions == 0 or evol_ratio >= 1.0):
                coverage_state = AuditCoverageState.COMPLETE
            else:
                coverage_state = AuditCoverageState.PARTIAL

            summary = AuditCoverageSummary(
                investigation_id=clean_inv,
                coverage_state=coverage_state,
                total_snapshots=total_snapshots,
                audited_snapshots=audited_snapshots,
                snapshot_coverage_ratio=snap_ratio,
                total_evolutions=total_evolutions,
                audited_evolutions=audited_evolutions,
                evolution_coverage_ratio=evol_ratio,
                overall_coverage_ratio=overall_ratio,
                unaudited_snapshot_ids=unaudited_snaps,
                unaudited_evolution_ids=unaudited_evols,
                missing_history_detected=(len(unaudited_snaps) > 0 or len(unaudited_evols) > 0),
                details={
                    "total_entities": total_entities,
                    "audited_entities": audited_entities,
                },
            )

            self._coverage_cache[clean_inv] = summary

            # EventBus publication
            if self._event_bus is not None:
                self._event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REVIEW_AUDIT_COVERAGE_EVALUATED,
                        payload=summary.to_dict(),
                        source="HistoricalReviewAuditReliabilityService",
                    )
                )

            # StateStore persistence
            if self._state_store is not None:
                self._state_store.set(f"historical_review_reliability:coverage:{clean_inv}", summary.to_dict())

            return summary

    # ==============================================================================
    # 2. Audit Repeatability Verification
    # ==============================================================================

    def verify_repeatability(
        self,
        investigation_id: str,
        runs: int = 2,
    ) -> AuditRepeatabilityResult:
        """Verify repeatability of audit reports across repeat executions on unchanged inputs."""
        clean_inv = (investigation_id or "").strip()
        if not clean_inv or not SAFE_ID_PATTERN.match(clean_inv):
            raise ValidationError(f"Invalid investigation_id for repeatability: '{investigation_id}'")

        try:
            runs_clamped = max(1, min(10, int(runs)))
        except (ValueError, TypeError):
            runs_clamped = 2

        with self._lock:
            if self._audit_service is None:
                return AuditRepeatabilityResult(
                    investigation_id=clean_inv,
                    repeatability_state=AuditRepeatabilityState.UNVERIFIABLE,
                    repeatability_score=0.0,
                    is_repeatable=False,
                    report_id_1="revaudit_unverifiable_1",
                    report_id_2="revaudit_unverifiable_2",
                    report_ids_match=False,
                    findings_count_1=0,
                    findings_count_2=0,
                    findings_match=False,
                    status_1=RecordValidationStatus.UNVERIFIABLE,
                    status_2=RecordValidationStatus.UNVERIFIABLE,
                    status_match=True,
                    divergent_fields=("audit_service",),
                    divergence_reasons=("Audit service is not initialized",),
                )

            try:
                # Run two successive audits on the same investigation
                report_1 = self._audit_service.generate_audit_report(clean_inv)
                report_2 = self._audit_service.generate_audit_report(clean_inv)
            except Exception as ex:
                logger.error("Audit repeatability execution failed for '%s': %s", clean_inv, ex)
                return AuditRepeatabilityResult(
                    investigation_id=clean_inv,
                    repeatability_state=AuditRepeatabilityState.FAILED,
                    repeatability_score=0.0,
                    is_repeatable=False,
                    report_id_1="revaudit_failed_1",
                    report_id_2="revaudit_failed_2",
                    report_ids_match=False,
                    findings_count_1=0,
                    findings_count_2=0,
                    findings_match=False,
                    status_1=RecordValidationStatus.FAILED,
                    status_2=RecordValidationStatus.FAILED,
                    status_match=True,
                    divergent_fields=("exception",),
                    divergence_reasons=(f"Audit execution exception: {ex}",),
                )

            # Compare report IDs (deterministic ID stability)
            report_ids_match = (report_1.report_id == report_2.report_id)

            # Compare findings
            findings_1 = {f.finding_id: f for f in report_1.findings}
            findings_2 = {f.finding_id: f for f in report_2.findings}
            findings_match = (findings_1.keys() == findings_2.keys() and len(findings_1) == len(findings_2))

            # Compare status
            status_match = (report_1.overall_status == report_2.overall_status)

            divergent_fields: List[str] = []
            divergence_reasons: List[str] = []

            if not report_ids_match:
                divergent_fields.append("report_id")
                divergence_reasons.append(
                    f"Report IDs diverged: '{report_1.report_id}' vs '{report_2.report_id}'"
                )
            if not findings_match:
                divergent_fields.append("findings")
                diff_1 = set(findings_1.keys()) - set(findings_2.keys())
                diff_2 = set(findings_2.keys()) - set(findings_1.keys())
                divergence_reasons.append(
                    f"Findings set diverged: run1 has {len(diff_1)} unique, run2 has {len(diff_2)} unique"
                )
            if not status_match:
                divergent_fields.append("overall_status")
                divergence_reasons.append(
                    f"Status diverged: '{report_1.overall_status}' vs '{report_2.overall_status}'"
                )

            # Weighted score: report_id (0.4), findings (0.4), status (0.2)
            score = 0.0
            if report_ids_match:
                score += 0.4
            if findings_match:
                score += 0.4
            if status_match:
                score += 0.2

            is_repeatable = (score >= 0.99)
            rep_state = AuditRepeatabilityState.STABLE if is_repeatable else AuditRepeatabilityState.DIVERGENT

            result = AuditRepeatabilityResult(
                investigation_id=clean_inv,
                repeatability_state=rep_state,
                repeatability_score=score,
                is_repeatable=is_repeatable,
                report_id_1=report_1.report_id,
                report_id_2=report_2.report_id,
                report_ids_match=report_ids_match,
                findings_count_1=len(report_1.findings),
                findings_count_2=len(report_2.findings),
                findings_match=findings_match,
                status_1=report_1.overall_status,
                status_2=report_2.overall_status,
                status_match=status_match,
                divergent_fields=tuple(divergent_fields),
                divergence_reasons=tuple(divergence_reasons),
            )

            # EventBus publication
            if self._event_bus is not None:
                self._event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REVIEW_AUDIT_REPEATABILITY_VERIFIED,
                        payload=result.to_dict(),
                        source="HistoricalReviewAuditReliabilityService",
                    )
                )

            return result

    # ==============================================================================
    # 3. Regression Monitoring & Finding Drift
    # ==============================================================================

    def monitor_regression(
        self,
        investigation_id: str,
        baseline_report_id: Optional[str] = None,
        current_report_id: Optional[str] = None,
    ) -> AuditRegressionResult:
        """Monitor finding drift and regressions between baseline and current audit reports."""
        clean_inv = (investigation_id or "").strip()
        if not clean_inv or not SAFE_ID_PATTERN.match(clean_inv):
            raise ValidationError(f"Invalid investigation_id for regression monitoring: '{investigation_id}'")

        with self._lock:
            if self._audit_service is None:
                return AuditRegressionResult(
                    investigation_id=clean_inv,
                    regression_state=AuditRegressionState.UNVERIFIABLE,
                    baseline_report_id=None,
                    current_report_id="revaudit_unverifiable",
                    new_findings_count=0,
                    resolved_findings_count=0,
                    persistent_findings_count=0,
                    conflict_preservation_maintained=True,
                    regression_summary="Audit service not initialized; regression unverifiable.",
                )

            # Resolve current report
            current_report: Optional[HistoricalReviewAuditReport] = None
            if current_report_id:
                current_report = self._audit_service.get_audit_report(current_report_id)
            if current_report is None:
                reports = self._audit_service.list_audit_reports(clean_inv, limit=2)
                if reports:
                    current_report = reports[0]
                else:
                    current_report = self._audit_service.generate_audit_report(clean_inv)

            # Resolve baseline report
            baseline_report: Optional[HistoricalReviewAuditReport] = None
            if baseline_report_id:
                baseline_report = self._audit_service.get_audit_report(baseline_report_id)
            else:
                reports = self._audit_service.list_audit_reports(clean_inv, limit=10)
                # If current_report is the latest, pick the second latest as baseline if available
                candidates = [r for r in reports if r.report_id != current_report.report_id]
                if candidates:
                    baseline_report = candidates[0]

            # If no baseline exists, this establishes a new baseline
            if baseline_report is None:
                return AuditRegressionResult(
                    investigation_id=clean_inv,
                    regression_state=AuditRegressionState.BASELINE_ESTABLISHED,
                    baseline_report_id=None,
                    current_report_id=current_report.report_id,
                    new_findings_count=0,
                    resolved_findings_count=0,
                    persistent_findings_count=len(current_report.findings),
                    persistent_finding_ids=tuple(f.finding_id for f in current_report.findings),
                    conflict_preservation_maintained=True,
                    regression_summary="Initial audit baseline established. No predecessor report to compare against.",
                )

            # Compare baseline vs current findings
            base_findings = {f.finding_id: f for f in baseline_report.findings}
            curr_findings = {f.finding_id: f for f in current_report.findings}

            new_findings = [f for fid, f in curr_findings.items() if fid not in base_findings]
            resolved_findings = [f for fid, f in base_findings.items() if fid not in curr_findings]
            persistent_findings = [f for fid, f in curr_findings.items() if fid in base_findings]

            new_severities = tuple(sorted(list({f.severity.value for f in new_findings})))

            # Check conflict preservation
            # If baseline preserved conflicts, does current also preserve them?
            base_conf_pres = (
                baseline_report.chain_result.recurring_conflicts_preserved
                if baseline_report.chain_result is not None
                else True
            )
            curr_conf_pres = (
                current_report.chain_result.recurring_conflicts_preserved
                if current_report.chain_result is not None
                else True
            )
            conflict_maintained = (not base_conf_pres) or curr_conf_pres

            # Classify regression status
            has_new_critical_or_error = any(
                f.severity in (AuditFindingSeverity.CRITICAL, AuditFindingSeverity.ERROR)
                for f in new_findings
            )
            has_resolved_critical_or_error = any(
                f.severity in (AuditFindingSeverity.CRITICAL, AuditFindingSeverity.ERROR)
                for f in resolved_findings
            )

            if has_new_critical_or_error or (not conflict_maintained):
                reg_state = AuditRegressionState.REGRESSION_DETECTED
                summary_text = (
                    f"Regression detected: {len(new_findings)} new findings identified "
                    f"including CRITICAL or ERROR severity issues."
                )
            elif has_resolved_critical_or_error and len(new_findings) == 0:
                reg_state = AuditRegressionState.IMPROVEMENT_DETECTED
                summary_text = (
                    f"Improvement detected: {len(resolved_findings)} findings resolved "
                    f"with zero newly introduced validation issues."
                )
            elif len(new_findings) == 0 and len(resolved_findings) == 0:
                reg_state = AuditRegressionState.NO_REGRESSION
                summary_text = "No regression detected. Findings remain completely consistent with baseline."
            else:
                reg_state = AuditRegressionState.NO_REGRESSION
                summary_text = (
                    f"Findings changed without critical regression: {len(new_findings)} new, "
                    f"{len(resolved_findings)} resolved, {len(persistent_findings)} persistent."
                )

            result = AuditRegressionResult(
                investigation_id=clean_inv,
                regression_state=reg_state,
                baseline_report_id=baseline_report.report_id,
                current_report_id=current_report.report_id,
                new_findings_count=len(new_findings),
                resolved_findings_count=len(resolved_findings),
                persistent_findings_count=len(persistent_findings),
                new_finding_ids=tuple(f.finding_id for f in new_findings),
                resolved_finding_ids=tuple(f.finding_id for f in resolved_findings),
                persistent_finding_ids=tuple(f.finding_id for f in persistent_findings),
                regression_severities=new_severities,
                conflict_preservation_maintained=conflict_maintained,
                regression_summary=summary_text,
            )

            # EventBus publication if regression detected
            if self._event_bus is not None and reg_state == AuditRegressionState.REGRESSION_DETECTED:
                self._event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REVIEW_AUDIT_REGRESSION_DETECTED,
                        payload=result.to_dict(),
                        source="HistoricalReviewAuditReliabilityService",
                    )
                )

            return result

    # ==============================================================================
    # 4. Long-Term Reliability Evaluation
    # ==============================================================================

    def evaluate_reliability(
        self,
        investigation_id: str,
        runs_for_repeatability: int = 2,
    ) -> HistoricalReviewReliabilityResult:
        """Run complete reliability evaluation including coverage, repeatability, and regression monitoring."""
        clean_inv = (investigation_id or "").strip()
        if not clean_inv or not SAFE_ID_PATTERN.match(clean_inv):
            raise ValidationError(f"Invalid investigation_id for reliability evaluation: '{investigation_id}'")

        with self._lock:
            evaluated_at = time.time()
            recovery_notes: List[str] = []
            controlled_recovered = False

            try:
                runs_clamped = max(1, min(10, int(runs_for_repeatability)))
            except (ValueError, TypeError):
                runs_clamped = 2

            # 1. Repeatability (executes audit verification runs)
            try:
                repeatability = self.verify_repeatability(clean_inv, runs=runs_clamped)
            except Exception as rep_err:
                logger.error("Controlled error during repeatability verification: %s", rep_err)
                controlled_recovered = True
                recovery_notes.append(f"Repeatability evaluation recovered from error: {rep_err}")
                repeatability = None

            # 2. Coverage (assesses audited vs total entities)
            try:
                coverage = self.evaluate_coverage(clean_inv)
                if coverage.coverage_state == AuditCoverageState.UNAVAILABLE:
                    controlled_recovered = True
                    recovery_notes.append(
                        f"Coverage evaluation reported service error: {coverage.details.get('error', 'unavailable')}"
                    )
            except Exception as cov_err:
                logger.error("Controlled error during coverage evaluation: %s", cov_err)
                controlled_recovered = True
                recovery_notes.append(f"Coverage evaluation recovered from error: {cov_err}")
                coverage = AuditCoverageSummary(
                    investigation_id=clean_inv,
                    coverage_state=AuditCoverageState.UNAVAILABLE,
                    total_snapshots=0,
                    audited_snapshots=0,
                    snapshot_coverage_ratio=0.0,
                    total_evolutions=0,
                    audited_evolutions=0,
                    evolution_coverage_ratio=0.0,
                    overall_coverage_ratio=0.0,
                    missing_history_detected=True,
                    details={"error": str(cov_err)},
                )

            # 3. Regression
            try:
                regression = self.monitor_regression(clean_inv)
            except Exception as reg_err:
                logger.error("Controlled error during regression monitoring: %s", reg_err)
                controlled_recovered = True
                recovery_notes.append(f"Regression monitoring recovered from error: {reg_err}")
                regression = None

            # 4. Fact vs Interpretation vs Human Decision Separation Counts
            facts_count = 0
            interps_count = 0
            human_decisions_count = 0
            conflicts_count = 0

            latest_report: Optional[HistoricalReviewAuditReport] = None
            if self._audit_service is not None:
                try:
                    reports = self._audit_service.list_audit_reports(clean_inv, limit=1)
                    if reports:
                        latest_report = reports[0]
                except Exception as list_err:
                    logger.error("Controlled error listing audit reports: %s", list_err)
                    controlled_recovered = True
                    recovery_notes.append(f"Audit report listing recovered from error: {list_err}")

            if latest_report is not None:
                for s_res in latest_report.snapshot_results:
                    facts_count += s_res.facts_count
                    interps_count += s_res.interpretations_count
                    human_decisions_count += s_res.human_decisions_count
                    conflicts_count += s_res.active_conflicts_count

            # 5. Overall Status Classification
            if coverage.coverage_state == AuditCoverageState.UNAVAILABLE:
                overall_status = ReliabilityEvaluationStatus.FAILED
            elif coverage.coverage_state == AuditCoverageState.EMPTY:
                overall_status = ReliabilityEvaluationStatus.UNVERIFIABLE
            elif repeatability and repeatability.repeatability_state in (
                AuditRepeatabilityState.FAILED,
                AuditRepeatabilityState.DIVERGENT,
            ):
                overall_status = (
                    ReliabilityEvaluationStatus.UNRELIABLE
                    if repeatability.repeatability_state == AuditRepeatabilityState.DIVERGENT
                    else ReliabilityEvaluationStatus.FAILED
                )
            elif regression and regression.regression_state == AuditRegressionState.REGRESSION_DETECTED:
                overall_status = ReliabilityEvaluationStatus.DEGRADED
            elif coverage.coverage_state == AuditCoverageState.PARTIAL:
                overall_status = ReliabilityEvaluationStatus.DEGRADED
            elif controlled_recovered:
                overall_status = ReliabilityEvaluationStatus.DEGRADED
            else:
                overall_status = ReliabilityEvaluationStatus.RELIABLE

            # Deterministic Evaluation ID
            fp = hashlib.sha256(
                json.dumps(
                    {
                        "investigation_id": clean_inv,
                        "coverage": coverage.to_dict(),
                        "repeatability": repeatability.to_dict() if repeatability else None,
                        "regression": regression.to_dict() if regression else None,
                        "overall_status": overall_status.value,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()[:16]

            eval_id = compute_deterministic_reliability_id(
                investigation_id=clean_inv,
                coverage_state=coverage.coverage_state.value,
                reliability_status=overall_status.value,
                fingerprint=fp,
            )

            # Voice-safe summary
            voice_text = self._build_voice_summary_text(
                investigation_id=clean_inv,
                status=overall_status,
                coverage=coverage,
                repeatability=repeatability,
                regression=regression,
            )
            voice_summary = AuditReliabilityVoiceSummary(
                evaluation_id=eval_id,
                investigation_id=clean_inv,
                reliability_status=overall_status,
                summary_text=voice_text,
                voice_safe=True,
            )

            result = HistoricalReviewReliabilityResult(
                evaluation_id=eval_id,
                investigation_id=clean_inv,
                evaluated_at=evaluated_at,
                overall_reliability_status=overall_status,
                coverage_summary=coverage,
                repeatability_result=repeatability,
                regression_result=regression,
                facts_count=facts_count,
                interpretations_count=interps_count,
                human_decisions_count=human_decisions_count,
                active_conflicts_preserved_count=conflicts_count,
                controlled_failure_recovered=controlled_recovered,
                recovery_notes=tuple(recovery_notes),
                voice_summary=voice_summary,
            )

            # Store in memory with FIFO eviction
            self._evaluations[eval_id] = result
            if clean_inv not in self._investigation_evaluations:
                self._investigation_evaluations[clean_inv] = []
            inv_list = self._investigation_evaluations[clean_inv]
            inv_list.insert(0, eval_id)

            if len(inv_list) > self._max_evaluations:
                evicted_id = inv_list.pop()
                self._evaluations.pop(evicted_id, None)

            # EventBus publication
            if self._event_bus is not None:
                evt_type = (
                    EventType.HISTORICAL_REVIEW_RELIABILITY_EVALUATION_COMPLETED
                    if overall_status != ReliabilityEvaluationStatus.FAILED
                    else EventType.HISTORICAL_REVIEW_RELIABILITY_EVALUATION_FAILED
                )
                self._event_bus.publish(
                    Event.create(
                        event_type=evt_type,
                        payload=result.to_dict(),
                        source="HistoricalReviewAuditReliabilityService",
                    )
                )

            # StateStore persistence
            if self._state_store is not None:
                self._state_store.set("historical_review_reliability:latest", result.to_dict())
                self._state_store.set(f"historical_review_reliability:{eval_id}", result.to_dict())

                hist = self._state_store.get("historical_review_reliability:history", [])
                if not isinstance(hist, list):
                    hist = []
                hist.insert(0, result.to_dict())
                self._state_store.set(
                    "historical_review_reliability:history",
                    hist[: self._max_evaluations],
                )

            return result

    # ==============================================================================
    # 5. Queries & Listing
    # ==============================================================================

    def get_reliability_result(self, evaluation_id: str) -> Optional[HistoricalReviewReliabilityResult]:
        """Retrieve a cached reliability result by evaluation ID with StateStore hydration."""
        clean_id = (evaluation_id or "").strip()
        with self._lock:
            if clean_id in self._evaluations:
                return self._evaluations[clean_id]

            if self._state_store is not None:
                stored = self._state_store.get(f"historical_review_reliability:{clean_id}", None)
                if isinstance(stored, dict):
                    deserialized = self._deserialize_reliability_result(stored)
                    if deserialized:
                        self._evaluations[clean_id] = deserialized
                        return deserialized

                latest = self._state_store.get("historical_review_reliability:latest", None)
                if isinstance(latest, dict) and latest.get("evaluation_id") == clean_id:
                    deserialized = self._deserialize_reliability_result(latest)
                    if deserialized:
                        self._evaluations[clean_id] = deserialized
                        return deserialized
            return None

    def list_reliability_results(
        self,
        investigation_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[HistoricalReviewReliabilityResult]:
        """List reliability results with pagination, optional filter, and StateStore hydration."""
        limit_clamped = max(1, min(100, limit))
        offset_clamped = max(0, offset)

        with self._lock:
            if investigation_id:
                clean_inv = investigation_id.strip()
                ids = self._investigation_evaluations.get(clean_inv, [])
                results = [self._evaluations[eid] for eid in ids if eid in self._evaluations]
            else:
                # Return all sorted by evaluated_at descending
                results = sorted(
                    self._evaluations.values(),
                    key=lambda r: r.evaluated_at,
                    reverse=True,
                )

            if not results and self._state_store is not None:
                # Hydrate from StateStore history if in-memory cache is cold
                history = self._state_store.get("historical_review_reliability:history", [])
                if isinstance(history, list):
                    hydrated: List[HistoricalReviewReliabilityResult] = []
                    for h in history:
                        if isinstance(h, dict):
                            if investigation_id and h.get("investigation_id") != clean_inv:
                                continue
                            deserialized = self._deserialize_reliability_result(h)
                            if deserialized:
                                hydrated.append(deserialized)
                                self._evaluations[deserialized.evaluation_id] = deserialized
                    results = hydrated

            return results[offset_clamped : offset_clamped + limit_clamped]

    def get_latest_reliability_result(
        self,
        investigation_id: Optional[str] = None,
    ) -> Optional[HistoricalReviewReliabilityResult]:
        """Get the latest reliability result for an investigation or overall."""
        results = self.list_reliability_results(investigation_id=investigation_id, limit=1, offset=0)
        if results:
            return results[0]

        if self._state_store is not None:
            latest = self._state_store.get("historical_review_reliability:latest", None)
            if isinstance(latest, dict):
                if investigation_id is None or latest.get("investigation_id") == investigation_id:
                    deserialized = self._deserialize_reliability_result(latest)
                    if deserialized:
                        with self._lock:
                            self._evaluations[deserialized.evaluation_id] = deserialized
                        return deserialized
        return None

    def _deserialize_reliability_result(
        self,
        data: Dict[str, Any],
    ) -> Optional[HistoricalReviewReliabilityResult]:
        """Safely deserialize a HistoricalReviewReliabilityResult dictionary."""
        try:
            return HistoricalReviewReliabilityResult.from_dict(data)
        except Exception as exc:
            logger.warning("Failed to deserialize HistoricalReviewReliabilityResult: %s", exc)
            return None

    # ==============================================================================
    # 6. Helpers
    # ==============================================================================

    def _build_voice_summary_text(
        self,
        investigation_id: str,
        status: ReliabilityEvaluationStatus,
        coverage: AuditCoverageSummary,
        repeatability: Optional[AuditRepeatabilityResult],
        regression: Optional[AuditRegressionResult],
    ) -> str:
        """Construct concise, safe voice summary strictly excluding trading terms."""
        cov_pct = int(round(coverage.overall_coverage_ratio * 100))
        parts = [
            f"Review audit reliability for investigation {investigation_id} evaluated as {status.value}.",
            f"Audit coverage is {coverage.coverage_state.value} at {cov_pct} percent.",
        ]

        if repeatability is not None:
            rep_pct = int(round(repeatability.repeatability_score * 100))
            parts.append(
                f"Repeatability status is {repeatability.repeatability_state.value} with stability score {rep_pct} percent."
            )

        if regression is not None:
            parts.append(f"Regression monitoring status is {regression.regression_state.value}.")

        return " ".join(parts)
