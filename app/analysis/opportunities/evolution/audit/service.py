"""KAISER — Historical Review Evolution Audit, Integrity & Reproducibility Validation Service.

Phase 2, Step 33: Auditing foundation for historical review snapshots, evolution deltas,
chain continuity, deterministic identity verification, and reproducibility metadata.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support & analytical review audit only:
   Never outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE directives.
2. Absolutely no order execution fields: Prohibits action, order, target_price,
   position_size, stop_loss, win_probability, expected_return, or strategy_ranking.
3. No profitability claims: No numeric probability of profit or ranking claims.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
5. Strict append-only immutability: Snapshots and audit records are never mutated or silently overwritten.
6. Execution engine remains locked: Settings.execution_locked == True.
7. Trading engine remains uninitialized.
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
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.opportunities.evolution.audit.models import (
    MAX_AUDIT_FINDINGS_PER_REPORT,
    MAX_AUDIT_REPORTS_PER_INVESTIGATION,
    AuditFindingCategory,
    AuditFindingSeverity,
    AuditReportLifecycleState,
    EvolutionAuditFinding,
    EvolutionAuditVoiceSummary,
    EvolutionRecordIntegrityResult,
    EvolutionReproducibilityVerificationResult,
    HistoricalChainIntegrityResult,
    HistoricalReviewAuditReport,
    RecordValidationStatus,
    SnapshotIntegrityResult,
    compute_deterministic_audit_report_id,
)
from app.analysis.opportunities.evolution.models import (
    FORBIDDEN_VOICE_WORDS,
    MAX_STRING_LENGTH,
    EvolutionDimensionName,
    HistoricalReviewSnapshot,
    HistoricalSnapshotLifecycleState,
    ReproducibilityEvolutionState,
    ReviewEvolutionRecord,
    SAFE_ID_PATTERN,
    _freeze,
    _unfreeze,
    compute_deterministic_evolution_id,
    compute_deterministic_snapshot_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.evolution.service import HistoricalReviewEvolutionService
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore

logger = logging.getLogger("kaiser.analysis.opportunities.evolution.audit.service")


class HistoricalReviewAuditService:
    """Thread-safe analytical audit service for historical review snapshots and evolution records.

    Provides deterministic verification of snapshot structural integrity, hash identities,
    chronological progression, gap identification, conflict preservation, and reproducibility.
    """

    def __init__(
        self,
        evolution_service: Optional[HistoricalReviewEvolutionService] = None,
        state_store: Optional[StateStore] = None,
        event_bus: Optional[EventBus] = None,
        run_registry: Optional[Any] = None,
        max_reports: int = MAX_AUDIT_REPORTS_PER_INVESTIGATION,
        name: str = "historical_review_audit_service",
    ) -> None:
        self.evolution_service = evolution_service
        self.state_store = state_store
        self.event_bus = event_bus
        self.run_registry = run_registry
        self.max_reports = max(5, min(200, max_reports))
        self.name = name

        self._lock = threading.RLock()
        self._reports: Dict[str, HistoricalReviewAuditReport] = {}
        self._reports_by_investigation: Dict[str, List[str]] = {}
        self._report_history: List[str] = []

    # ==============================================================================
    # Snapshot Auditing
    # ==============================================================================

    def audit_snapshot(
        self,
        snapshot: Union[HistoricalReviewSnapshot, str],
    ) -> SnapshotIntegrityResult:
        """Audit structural integrity, deterministic identity, and claim separation of a snapshot."""
        if isinstance(snapshot, str):
            snap_obj = self._resolve_snapshot(snapshot)
            if snap_obj is None:
                finding = EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(snapshot, 'missing')[:16]}",
                    category=AuditFindingCategory.BROKEN_REFERENCE,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="SNAPSHOT",
                    target_id=snapshot,
                    field_path="snapshot_id",
                    message=f"Snapshot '{snapshot}' could not be resolved from evolution service or state store.",
                )
                return SnapshotIntegrityResult(
                    snapshot_id=snapshot,
                    investigation_id="UNKNOWN",
                    status=RecordValidationStatus.INCOMPLETE,
                    hash_verified=False,
                    findings=(finding,),
                )
            snap = snap_obj
        elif isinstance(snapshot, HistoricalReviewSnapshot):
            snap = snapshot
        else:
            raise ValidationError("Expected HistoricalReviewSnapshot instance or snapshot_id string.")

        findings: List[EvolutionAuditFinding] = []
        has_prohibited = False

        # 1. Structural checks
        try:
            validate_no_prohibited_fields(snap.to_dict(), path=f"snapshot[{snap.snapshot_id}]")
        except ValidationError as pe:
            has_prohibited = True
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(snap.snapshot_id, 'prohibited')[:16]}",
                    category=AuditFindingCategory.PROHIBITED_CONTENT,
                    severity=AuditFindingSeverity.CRITICAL,
                    target_type="SNAPSHOT",
                    target_id=snap.snapshot_id,
                    field_path="prohibited_fields",
                    message=str(pe),
                )
            )

        if snap.captured_at <= 0 or math.isnan(snap.captured_at) or math.isinf(snap.captured_at):
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(snap.snapshot_id, 'captured_at')[:16]}",
                    category=AuditFindingCategory.STRUCTURAL_INTEGRITY,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="SNAPSHOT",
                    target_id=snap.snapshot_id,
                    field_path="captured_at",
                    message=f"Snapshot captured_at must be positive finite float, got {snap.captured_at}.",
                )
            )

        if snap.snapshot_version < 1:
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(snap.snapshot_id, 'version')[:16]}",
                    category=AuditFindingCategory.STRUCTURAL_INTEGRITY,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="SNAPSHOT",
                    target_id=snap.snapshot_id,
                    field_path="snapshot_version",
                    message=f"Snapshot snapshot_version must be >= 1, got {snap.snapshot_version}.",
                )
            )

        # 2. Deterministic Hash Verification
        hash_verified, recalc_id = self._verify_snapshot_hash(snap)
        if not hash_verified and snap.snapshot_id.startswith("revsnap_"):
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(snap.snapshot_id, 'hash')[:16]}",
                    category=AuditFindingCategory.IDENTITY_DETERMINISM,
                    severity=AuditFindingSeverity.WARNING,
                    target_type="SNAPSHOT",
                    target_id=snap.snapshot_id,
                    field_path="snapshot_id",
                    message=(
                        f"Snapshot ID '{snap.snapshot_id}' does not match canonical recomputed hash '{recalc_id}'."
                    ),
                    observed_value=snap.snapshot_id,
                    expected_value=recalc_id,
                )
            )

        # 3. Facts vs Interpretations vs Human Decisions Separation
        ev_state = _unfreeze(snap.evidence_state)
        facts = ev_state.get("facts", [])
        if not isinstance(facts, list):
            facts = []
        interpretations = ev_state.get("interpretations", [])
        if not isinstance(interpretations, list):
            interpretations = []

        rev_state = _unfreeze(snap.review_state)
        human_decisions = (
            rev_state.get("human_review_decisions", [])
            or rev_state.get("human_review_consistency", [])
        )
        if not isinstance(human_decisions, list):
            human_decisions = []

        conf_state = _unfreeze(snap.conflict_state)
        conflicts = (
            conf_state.get("active_conflicts", [])
            or conf_state.get("conflicts", [])
            or (conf_state.get("conflict_summary", {}).get("active_conflicts", []))
        )
        if not isinstance(conflicts, list):
            conflicts = []

        # Validate that facts are not labeled as interpretations or human reviews
        for idx, fact_item in enumerate(facts):
            if isinstance(fact_item, dict):
                claim_type = str(fact_item.get("claim_type", "")).upper()
                if claim_type in ("INTERPRETATION", "HUMAN_REVIEW_DECISION"):
                    findings.append(
                        EvolutionAuditFinding(
                            finding_id=f"find_{uuid_hex(snap.snapshot_id, f'claim_{idx}')[:16]}",
                            category=AuditFindingCategory.CLAIM_SEPARATION,
                            severity=AuditFindingSeverity.WARNING,
                            target_type="SNAPSHOT",
                            target_id=snap.snapshot_id,
                            field_path=f"evidence_state.facts[{idx}].claim_type",
                            message=(
                                f"Fact item at index {idx} has incompatible claim_type '{claim_type}'. "
                                "Facts must be strictly separated from interpretations and human review decisions."
                            ),
                            observed_value=claim_type,
                            expected_value="FACT",
                        )
                    )

        # Status determination
        if has_prohibited or any(f.severity == AuditFindingSeverity.CRITICAL for f in findings):
            status = RecordValidationStatus.FAILED
        elif any(f.severity == AuditFindingSeverity.ERROR for f in findings):
            status = RecordValidationStatus.INCONSISTENT
        elif any(f.severity == AuditFindingSeverity.WARNING for f in findings):
            status = RecordValidationStatus.INCONSISTENT
        else:
            status = RecordValidationStatus.VALID

        return SnapshotIntegrityResult(
            snapshot_id=snap.snapshot_id,
            investigation_id=snap.investigation_id,
            status=status,
            hash_verified=hash_verified,
            recalculated_id=recalc_id,
            chronology_valid=True,
            has_prohibited_terms=has_prohibited,
            facts_count=len(facts),
            interpretations_count=len(interpretations),
            human_decisions_count=len(human_decisions),
            active_conflicts_count=len(conflicts),
            findings=tuple(findings),
        )

    # ==============================================================================
    # Evolution Auditing
    # ==============================================================================

    def audit_evolution(
        self,
        evolution: Union[ReviewEvolutionRecord, str],
    ) -> EvolutionRecordIntegrityResult:
        """Audit structural integrity, references, and deterministic identity of an evolution record."""
        if isinstance(evolution, str):
            evol_obj = self._resolve_evolution(evolution)
            if evol_obj is None:
                finding = EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evolution, 'missing_evol')[:16]}",
                    category=AuditFindingCategory.BROKEN_REFERENCE,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="EVOLUTION",
                    target_id=evolution,
                    field_path="evolution_id",
                    message=f"Review evolution record '{evolution}' could not be resolved.",
                )
                return EvolutionRecordIntegrityResult(
                    evolution_id=evolution,
                    previous_snapshot_id="UNKNOWN",
                    current_snapshot_id="UNKNOWN",
                    investigation_id="UNKNOWN",
                    status=RecordValidationStatus.INCOMPLETE,
                    hash_verified=False,
                    findings=(finding,),
                )
            evol = evol_obj
        elif isinstance(evolution, ReviewEvolutionRecord):
            evol = evolution
        else:
            raise ValidationError("Expected ReviewEvolutionRecord instance or evolution_id string.")

        findings: List[EvolutionAuditFinding] = []
        has_prohibited = False

        # 1. Structural checks
        try:
            validate_no_prohibited_fields(evol.to_dict(), path=f"evolution[{evol.evolution_id}]")
        except ValidationError as pe:
            has_prohibited = True
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'prohibited')[:16]}",
                    category=AuditFindingCategory.PROHIBITED_CONTENT,
                    severity=AuditFindingSeverity.CRITICAL,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="prohibited_fields",
                    message=str(pe),
                )
            )

        if evol.generated_at <= 0 or math.isnan(evol.generated_at) or math.isinf(evol.generated_at):
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'generated_at')[:16]}",
                    category=AuditFindingCategory.STRUCTURAL_INTEGRITY,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="generated_at",
                    message=f"Evolution generated_at must be positive finite float, got {evol.generated_at}.",
                )
            )

        # 2. Reference checks
        references_exist = True
        prev_snap = self._resolve_snapshot(evol.previous_snapshot_id)
        if prev_snap is None:
            references_exist = False
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'broken_prev')[:16]}",
                    category=AuditFindingCategory.BROKEN_REFERENCE,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="previous_snapshot_id",
                    message=f"Referenced previous snapshot '{evol.previous_snapshot_id}' does not exist.",
                    observed_value=evol.previous_snapshot_id,
                )
            )

        curr_snap = self._resolve_snapshot(evol.current_snapshot_id)
        if curr_snap is None:
            references_exist = False
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'broken_curr')[:16]}",
                    category=AuditFindingCategory.BROKEN_REFERENCE,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="current_snapshot_id",
                    message=f"Referenced current snapshot '{evol.current_snapshot_id}' does not exist.",
                    observed_value=evol.current_snapshot_id,
                )
            )

        if evol.previous_snapshot_id == evol.current_snapshot_id:
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'same_snap')[:16]}",
                    category=AuditFindingCategory.INCONSISTENT_REFERENCE,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="current_snapshot_id",
                    message="Evolution previous_snapshot_id and current_snapshot_id must be distinct.",
                )
            )

        # Inconsistent investigation references
        if prev_snap and prev_snap.investigation_id != evol.investigation_id:
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'mismatch_prev_inv')[:16]}",
                    category=AuditFindingCategory.INCONSISTENT_REFERENCE,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="investigation_id",
                    message=(
                        f"Previous snapshot investigation_id '{prev_snap.investigation_id}' "
                        f"does not match evolution investigation_id '{evol.investigation_id}'."
                    ),
                )
            )

        if curr_snap and curr_snap.investigation_id != evol.investigation_id:
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'mismatch_curr_inv')[:16]}",
                    category=AuditFindingCategory.INCONSISTENT_REFERENCE,
                    severity=AuditFindingSeverity.ERROR,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="investigation_id",
                    message=(
                        f"Current snapshot investigation_id '{curr_snap.investigation_id}' "
                        f"does not match evolution investigation_id '{evol.investigation_id}'."
                    ),
                )
            )

        # 3. Deterministic Hash Verification
        recalc_id = compute_deterministic_evolution_id(
            previous_snapshot_id=evol.previous_snapshot_id,
            current_snapshot_id=evol.current_snapshot_id,
        )
        hash_verified = (evol.evolution_id == recalc_id)
        if not hash_verified and (evol.evolution_id.startswith("revol_") or evol.evolution_id.startswith("revevol_")):
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'hash_evol')[:16]}",
                    category=AuditFindingCategory.IDENTITY_DETERMINISM,
                    severity=AuditFindingSeverity.WARNING,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="evolution_id",
                    message=(
                        f"Evolution ID '{evol.evolution_id}' does not match canonical recomputed ID '{recalc_id}'."
                    ),
                    observed_value=evol.evolution_id,
                    expected_value=recalc_id,
                )
            )

        # 4. Check dimension count
        dims_count = len(evol.changed_dimensions)
        if dims_count < len(EvolutionDimensionName):
            findings.append(
                EvolutionAuditFinding(
                    finding_id=f"find_{uuid_hex(evol.evolution_id, 'dims_count')[:16]}",
                    category=AuditFindingCategory.STRUCTURAL_INTEGRITY,
                    severity=AuditFindingSeverity.WARNING,
                    target_type="EVOLUTION",
                    target_id=evol.evolution_id,
                    field_path="changed_dimensions",
                    message=(
                        f"Evolution record evaluates {dims_count} dimensions; "
                        f"expected {len(EvolutionDimensionName)} standard dimensions."
                    ),
                )
            )

        # Status determination
        if has_prohibited or any(f.severity == AuditFindingSeverity.CRITICAL for f in findings):
            status = RecordValidationStatus.FAILED
        elif not references_exist or any(f.category == AuditFindingCategory.BROKEN_REFERENCE for f in findings):
            status = RecordValidationStatus.INCOMPLETE
        elif any(f.severity in (AuditFindingSeverity.ERROR, AuditFindingSeverity.WARNING) for f in findings):
            status = RecordValidationStatus.INCONSISTENT
        else:
            status = RecordValidationStatus.VALID

        return EvolutionRecordIntegrityResult(
            evolution_id=evol.evolution_id,
            previous_snapshot_id=evol.previous_snapshot_id,
            current_snapshot_id=evol.current_snapshot_id,
            investigation_id=evol.investigation_id,
            status=status,
            hash_verified=hash_verified,
            recalculated_id=recalc_id,
            references_exist=references_exist,
            dimensions_evaluated_count=dims_count,
            findings=tuple(findings),
        )

    # ==============================================================================
    # Investigation Chain Auditing
    # ==============================================================================

    def audit_investigation_chain(
        self,
        investigation_id: str,
    ) -> HistoricalChainIntegrityResult:
        """Validate chronological ordering, continuity, gaps, and conflict preservation in chain."""
        clean_inv = (investigation_id or "").strip()
        if not clean_inv or not SAFE_ID_PATTERN.match(clean_inv):
            raise ValidationError(f"Invalid investigation_id format: '{investigation_id}'")

        findings: List[EvolutionAuditFinding] = []
        detected_gaps: List[str] = []
        duplicate_ids: List[str] = []
        broken_links: List[str] = []
        conflicts_preserved = True

        snapshots = self._get_investigation_snapshots(clean_inv)
        if not snapshots:
            finding = EvolutionAuditFinding(
                finding_id=f"find_{uuid_hex(clean_inv, 'empty_chain')[:16]}",
                category=AuditFindingCategory.HISTORICAL_GAP,
                severity=AuditFindingSeverity.INFO,
                target_type="CHAIN",
                target_id=clean_inv,
                field_path="snapshots",
                message=f"No historical review snapshots found for investigation '{clean_inv}'.",
            )
            return HistoricalChainIntegrityResult(
                investigation_id=clean_inv,
                chain_length=0,
                status=RecordValidationStatus.INCOMPLETE,
                version_sequence_intact=False,
                chronology_intact=True,
                detected_gaps=("No snapshots recorded",),
                findings=(finding,),
            )

        # 1. Check duplicate IDs
        seen_ids: Set[str] = set()
        for s in snapshots:
            if s.snapshot_id in seen_ids:
                duplicate_ids.append(s.snapshot_id)
                findings.append(
                    EvolutionAuditFinding(
                        finding_id=f"find_{uuid_hex(s.snapshot_id, 'dup')[:16]}",
                        category=AuditFindingCategory.DUPLICATE_IDENTIFIER,
                        severity=AuditFindingSeverity.ERROR,
                        target_type="CHAIN",
                        target_id=clean_inv,
                        field_path="snapshot_id",
                        message=f"Duplicate snapshot ID detected in chain: '{s.snapshot_id}'.",
                        observed_value=s.snapshot_id,
                    )
                )
            seen_ids.add(s.snapshot_id)

        # 2. Sort by version and check sequence
        sorted_snaps = sorted(snapshots, key=lambda x: (x.snapshot_version, x.captured_at))
        version_intact = True
        expected_version = 1

        for idx, snap in enumerate(sorted_snaps):
            if snap.snapshot_version != expected_version:
                version_intact = False
                gap_msg = (
                    f"Version sequence gap: expected version {expected_version}, "
                    f"found version {snap.snapshot_version} (snapshot '{snap.snapshot_id}')."
                )
                detected_gaps.append(gap_msg)
                findings.append(
                    EvolutionAuditFinding(
                        finding_id=f"find_{uuid_hex(clean_inv, f'gap_{expected_version}')[:16]}",
                        category=AuditFindingCategory.HISTORICAL_GAP,
                        severity=AuditFindingSeverity.WARNING,
                        target_type="CHAIN",
                        target_id=clean_inv,
                        field_path=f"snapshots[{idx}].snapshot_version",
                        message=gap_msg,
                        observed_value=str(snap.snapshot_version),
                        expected_value=str(expected_version),
                    )
                )
                expected_version = snap.snapshot_version + 1
            else:
                expected_version += 1

        # 3. Chronological Monotonic Ordering
        chronology_intact = True
        for i in range(1, len(sorted_snaps)):
            prev_s = sorted_snaps[i - 1]
            curr_s = sorted_snaps[i]
            if curr_s.captured_at < prev_s.captured_at:
                chronology_intact = False
                chrono_msg = (
                    f"Chronological inversion: snapshot version {curr_s.snapshot_version} "
                    f"captured at {curr_s.captured_at} is earlier than predecessor version "
                    f"{prev_s.snapshot_version} captured at {prev_s.captured_at}."
                )
                findings.append(
                    EvolutionAuditFinding(
                        finding_id=f"find_{uuid_hex(curr_s.snapshot_id, 'chrono_inv')[:16]}",
                        category=AuditFindingCategory.CHRONOLOGICAL_ORDERING,
                        severity=AuditFindingSeverity.ERROR,
                        target_type="CHAIN",
                        target_id=clean_inv,
                        field_path=f"snapshots[{i}].captured_at",
                        message=chrono_msg,
                    )
                )

        # 4. Evolution Link Continuity
        for i in range(1, len(sorted_snaps)):
            prev_s = sorted_snaps[i - 1]
            curr_s = sorted_snaps[i]
            evol = self._find_evolution_between(prev_s.snapshot_id, curr_s.snapshot_id)
            if evol is None:
                link_msg = (
                    f"Missing evolution link between version {prev_s.snapshot_version} "
                    f"('{prev_s.snapshot_id}') and version {curr_s.snapshot_version} ('{curr_s.snapshot_id}')."
                )
                broken_links.append(link_msg)
                findings.append(
                    EvolutionAuditFinding(
                        finding_id=f"find_{uuid_hex(prev_s.snapshot_id, curr_s.snapshot_id)[:16]}",
                        category=AuditFindingCategory.HISTORICAL_GAP,
                        severity=AuditFindingSeverity.WARNING,
                        target_type="CHAIN",
                        target_id=clean_inv,
                        field_path="evolution_records",
                        message=link_msg,
                    )
                )
            else:
                # 5. Conflict Preservation Check
                prev_conf = self._extract_conflict_identifiers(prev_s)
                curr_conf = self._extract_conflict_identifiers(curr_s)
                resolved_in_evol = set(evol.newly_resolved_discrepancies)

                # Conflicts that were active in prev but absent in curr must be recorded as resolved
                dropped_conflicts = prev_conf - curr_conf
                unaccounted = dropped_conflicts - resolved_in_evol
                if unaccounted:
                    conflicts_preserved = False
                    for unc in sorted(list(unaccounted)):
                        findings.append(
                            EvolutionAuditFinding(
                                finding_id=f"find_{uuid_hex(curr_s.snapshot_id, unc)[:16]}",
                                category=AuditFindingCategory.CONFLICT_PRESERVATION,
                                severity=AuditFindingSeverity.WARNING,
                                target_type="CHAIN",
                                target_id=clean_inv,
                                field_path="conflict_state",
                                message=(
                                    f"Conflict '{unc}' was present in version {prev_s.snapshot_version} "
                                    f"but disappeared in version {curr_s.snapshot_version} without being "
                                    "recorded in newly_resolved_discrepancies or documented resolution."
                                ),
                                source_reference=f"transition:{prev_s.snapshot_id}->{curr_s.snapshot_id}",
                            )
                        )

        # Status determination
        if any(f.severity == AuditFindingSeverity.CRITICAL for f in findings):
            status = RecordValidationStatus.FAILED
        elif not chronology_intact or duplicate_ids:
            status = RecordValidationStatus.INCONSISTENT
        elif not version_intact or broken_links or not conflicts_preserved:
            status = RecordValidationStatus.INCOMPLETE
        elif any(f.severity in (AuditFindingSeverity.ERROR, AuditFindingSeverity.WARNING) for f in findings):
            status = RecordValidationStatus.INCONSISTENT
        else:
            status = RecordValidationStatus.VALID

        return HistoricalChainIntegrityResult(
            investigation_id=clean_inv,
            chain_length=len(sorted_snaps),
            status=status,
            version_sequence_intact=version_intact,
            chronology_intact=chronology_intact,
            detected_gaps=tuple(detected_gaps),
            duplicate_ids_detected=tuple(duplicate_ids),
            broken_links_detected=tuple(broken_links),
            recurring_conflicts_preserved=conflicts_preserved,
            findings=tuple(findings),
        )

    # ==============================================================================
    # Reproducibility Verification
    # ==============================================================================

    def verify_reproducibility(
        self,
        target_id: str,
        target_type: str = "SNAPSHOT",
    ) -> EvolutionReproducibilityVerificationResult:
        """Verify reproducibility metadata, analysis run linkages, and provenance traceability."""
        clean_target = (target_id or "").strip()
        if not clean_target or not SAFE_ID_PATTERN.match(clean_target):
            raise ValidationError(f"Invalid target_id format: '{target_id}'")

        findings: List[EvolutionAuditFinding] = []
        unverifiable_reasons: List[str] = []
        analysis_runs_linked: List[str] = []
        is_verifiable = True
        lineage_linked = False
        prov_sources_count = 0
        rep_state = ReproducibilityEvolutionState.UNKNOWN

        snap = self._resolve_snapshot(clean_target)
        if snap is None:
            finding = EvolutionAuditFinding(
                finding_id=f"find_{uuid_hex(clean_target, 'rep_missing')[:16]}",
                category=AuditFindingCategory.BROKEN_REFERENCE,
                severity=AuditFindingSeverity.ERROR,
                target_type=target_type,
                target_id=clean_target,
                field_path="target_id",
                message=f"Target '{clean_target}' could not be resolved for reproducibility verification.",
            )
            return EvolutionReproducibilityVerificationResult(
                target_id=clean_target,
                target_type=target_type,
                reproducibility_status=ReproducibilityEvolutionState.UNKNOWN,
                is_verifiable=False,
                lineage_linked=False,
                provenance_sources_count=0,
                unverifiable_reasons=(f"Target '{clean_target}' not found",),
                findings=(finding,),
            )

        rep_dict = _unfreeze(snap.reproducibility_state)
        rep_val = str(rep_dict.get("reproducibility_state", "UNKNOWN")).upper()
        try:
            rep_state = ReproducibilityEvolutionState(rep_val)
        except ValueError:
            rep_state = ReproducibilityEvolutionState.UNKNOWN

        prov_dict = _unfreeze(snap.provenance_state)
        prov_summary = prov_dict.get("provenance_summary", {})
        prov_sources = (
            prov_dict.get("sources", [])
            or prov_summary.get("sources", [])
            or list(snap.source_references)
        )
        prov_sources_count = len(prov_sources)

        # Extract linked analysis runs if available
        src_state = _unfreeze(snap.source_state)
        runs_in_src = src_state.get("analysis_run_ids", []) or src_state.get("run_ids", [])
        if isinstance(runs_in_src, list):
            analysis_runs_linked.extend(str(r) for r in runs_in_src)

        if snap.source_references:
            lineage_linked = True

        # Validation rules:
        # Rule A: If claimed REPRODUCIBLE, must have source references or provenance sources
        if rep_state == ReproducibilityEvolutionState.REPRODUCIBLE:
            if not snap.source_references and prov_sources_count == 0:
                is_verifiable = False
                reason = "Snapshot is marked REPRODUCIBLE but has no source references or provenance sources."
                unverifiable_reasons.append(reason)
                findings.append(
                    EvolutionAuditFinding(
                        finding_id=f"find_{uuid_hex(snap.snapshot_id, 'rep_no_src')[:16]}",
                        category=AuditFindingCategory.REPRODUCIBILITY_METADATA,
                        severity=AuditFindingSeverity.WARNING,
                        target_type="SNAPSHOT",
                        target_id=snap.snapshot_id,
                        field_path="reproducibility_state",
                        message=reason,
                    )
                )

            # Rule B: Verify linked analysis runs in registry if available
            if self.run_registry and analysis_runs_linked:
                missing_runs: List[str] = []
                for r_id in analysis_runs_linked:
                    run_obj = None
                    if hasattr(self.run_registry, "get_run"):
                        run_obj = self.run_registry.get_run(r_id)
                    elif hasattr(self.run_registry, "get"):
                        run_obj = self.run_registry.get(r_id)
                    if run_obj is None:
                        missing_runs.append(r_id)

                if missing_runs:
                    is_verifiable = False
                    reason = f"Analysis runs referenced in snapshot are not found in run registry: {missing_runs}."
                    unverifiable_reasons.append(reason)
                    findings.append(
                        EvolutionAuditFinding(
                            finding_id=f"find_{uuid_hex(snap.snapshot_id, 'missing_runs')[:16]}",
                            category=AuditFindingCategory.REPRODUCIBILITY_METADATA,
                            severity=AuditFindingSeverity.INFO,
                            target_type="SNAPSHOT",
                            target_id=snap.snapshot_id,
                            field_path="source_state.analysis_run_ids",
                            message=reason,
                        )
                    )

        elif rep_state == ReproducibilityEvolutionState.UNKNOWN:
            is_verifiable = False
            unverifiable_reasons.append("Reproducibility state is explicitly marked UNKNOWN.")

        return EvolutionReproducibilityVerificationResult(
            target_id=clean_target,
            target_type=target_type,
            reproducibility_status=rep_state,
            is_verifiable=is_verifiable,
            lineage_linked=lineage_linked,
            provenance_sources_count=prov_sources_count,
            analysis_runs_linked=tuple(analysis_runs_linked),
            unverifiable_reasons=tuple(unverifiable_reasons),
            findings=tuple(findings),
        )

    # ==============================================================================
    # Full Audit Report Generation
    # ==============================================================================

    def generate_audit_report(
        self,
        investigation_id: str,
        include_reproducibility: bool = True,
        correlation_id: Optional[str] = None,
    ) -> HistoricalReviewAuditReport:
        """Execute full review evolution audit across snapshots, evolutions, and continuity chain."""
        clean_inv = (investigation_id or "").strip()
        if not clean_inv or not SAFE_ID_PATTERN.match(clean_inv):
            raise ValidationError(f"Invalid investigation_id format: '{investigation_id}'")

        corr_id = correlation_id or f"corr_audit_{int(time.time() * 1000)}"

        # Emit audit started event
        if self.event_bus:
            try:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REVIEW_AUDIT_STARTED,
                        source=self.name,
                        payload={"investigation_id": clean_inv},
                        correlation_id=corr_id,
                    )
                )
            except Exception as ev_err:
                logger.warning("Failed to publish audit started event: %s", ev_err)

        snapshots = self._get_investigation_snapshots(clean_inv)
        evolutions = self._get_investigation_evolutions(clean_inv)

        # 1. Audit individual snapshots
        snapshot_results: List[SnapshotIntegrityResult] = []
        all_findings: List[EvolutionAuditFinding] = []
        for s in snapshots:
            s_res = self.audit_snapshot(s)
            snapshot_results.append(s_res)
            all_findings.extend(s_res.findings)

        # 2. Audit individual evolution records
        evolution_results: List[EvolutionRecordIntegrityResult] = []
        for e in evolutions:
            e_res = self.audit_evolution(e)
            evolution_results.append(e_res)
            all_findings.extend(e_res.findings)

        # 3. Audit chain continuity
        chain_res = self.audit_investigation_chain(clean_inv)
        all_findings.extend(chain_res.findings)

        # 4. Audit reproducibility (latest snapshot if present)
        rep_res = None
        if include_reproducibility and snapshots:
            latest_snap = max(snapshots, key=lambda x: (x.snapshot_version, x.captured_at))
            rep_res = self.verify_reproducibility(latest_snap.snapshot_id, target_type="SNAPSHOT")
            all_findings.extend(rep_res.findings)

        # Deduplicate findings by finding_id
        unique_findings_map: Dict[str, EvolutionAuditFinding] = {}
        for f in all_findings:
            if f.finding_id not in unique_findings_map:
                unique_findings_map[f.finding_id] = f
        unique_findings = tuple(unique_findings_map.values())[:MAX_AUDIT_FINDINGS_PER_REPORT]

        # Summarize findings by severity
        findings_summary: Dict[str, int] = {
            AuditFindingSeverity.INFO.value: 0,
            AuditFindingSeverity.WARNING.value: 0,
            AuditFindingSeverity.ERROR.value: 0,
            AuditFindingSeverity.CRITICAL.value: 0,
        }
        for f in unique_findings:
            findings_summary[f.severity.value] = findings_summary.get(f.severity.value, 0) + 1

        # Determine overall status
        if findings_summary[AuditFindingSeverity.CRITICAL.value] > 0:
            overall_status = RecordValidationStatus.FAILED
        elif findings_summary[AuditFindingSeverity.ERROR.value] > 0:
            overall_status = RecordValidationStatus.INCONSISTENT
        elif not chain_res.version_sequence_intact or chain_res.detected_gaps or len(snapshots) == 0:
            overall_status = RecordValidationStatus.INCOMPLETE
        elif rep_res and not rep_res.is_verifiable and rep_res.reproducibility_status == ReproducibilityEvolutionState.REPRODUCIBLE:
            overall_status = RecordValidationStatus.UNVERIFIABLE
        elif findings_summary[AuditFindingSeverity.WARNING.value] > 0:
            overall_status = RecordValidationStatus.INCONSISTENT
        else:
            overall_status = RecordValidationStatus.VALID

        # Deterministic Report ID
        report_id = compute_deterministic_audit_report_id(
            investigation_id=clean_inv,
            snapshot_ids=[s.snapshot_id for s in snapshots],
            evolution_ids=[e.evolution_id for e in evolutions],
            overall_status=overall_status.value,
        )

        # Construct voice-safe summary
        voice_summary_text = self._build_voice_summary_text(
            investigation_id=clean_inv,
            overall_status=overall_status,
            snapshots_count=len(snapshots),
            findings_count=len(unique_findings),
            chain_res=chain_res,
        )
        voice_summary = EvolutionAuditVoiceSummary(
            report_id=report_id,
            investigation_id=clean_inv,
            overall_status=overall_status,
            summary_text=voice_summary_text,
            voice_safe=True,
        )

        traceability = {
            "audited_investigation_id": clean_inv,
            "correlation_id": corr_id,
            "snapshots_audited": [s.snapshot_id for s in snapshots],
            "evolutions_audited": [e.evolution_id for e in evolutions],
            "findings_count": len(unique_findings),
            "generated_by": self.name,
        }

        report = HistoricalReviewAuditReport(
            report_id=report_id,
            investigation_id=clean_inv,
            generated_at=time.time(),
            lifecycle_state=AuditReportLifecycleState.STORED,
            overall_status=overall_status,
            snapshots_audited_count=len(snapshots),
            evolutions_audited_count=len(evolutions),
            findings_summary=findings_summary,
            snapshot_results=tuple(snapshot_results),
            evolution_results=tuple(evolution_results),
            chain_result=chain_res,
            reproducibility_result=rep_res,
            findings=unique_findings,
            voice_summary=voice_summary,
            traceability=traceability,
        )

        # Store report
        stored_report = self.store_audit_report(report, correlation_id=corr_id)

        # Emit completion / integrity violation events
        if self.event_bus:
            try:
                if overall_status == RecordValidationStatus.FAILED:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.HISTORICAL_REVIEW_AUDIT_FAILED,
                            source=self.name,
                            payload={
                                "report_id": stored_report.report_id,
                                "investigation_id": clean_inv,
                                "overall_status": overall_status.value,
                            },
                            correlation_id=corr_id,
                        )
                    )
                else:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.HISTORICAL_REVIEW_AUDIT_COMPLETED,
                            source=self.name,
                            payload={
                                "report_id": stored_report.report_id,
                                "investigation_id": clean_inv,
                                "overall_status": overall_status.value,
                                "snapshots_audited": len(snapshots),
                                "findings_count": len(unique_findings),
                            },
                            correlation_id=corr_id,
                        )
                    )

                if findings_summary[AuditFindingSeverity.ERROR.value] > 0 or findings_summary[AuditFindingSeverity.CRITICAL.value] > 0:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.HISTORICAL_REVIEW_INTEGRITY_VIOLATION_DETECTED,
                            source=self.name,
                            payload={
                                "report_id": stored_report.report_id,
                                "investigation_id": clean_inv,
                                "violations_count": (
                                    findings_summary[AuditFindingSeverity.ERROR.value]
                                    + findings_summary[AuditFindingSeverity.CRITICAL.value]
                                ),
                            },
                            correlation_id=corr_id,
                        )
                    )

                if rep_res and rep_res.is_verifiable:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.HISTORICAL_REVIEW_REPRODUCIBILITY_VERIFIED,
                            source=self.name,
                            payload={
                                "report_id": stored_report.report_id,
                                "investigation_id": clean_inv,
                                "reproducibility_status": rep_res.reproducibility_status.value,
                            },
                            correlation_id=corr_id,
                        )
                    )
            except Exception as ev_err:
                logger.warning("Failed to publish audit completion events: %s", ev_err)

        return stored_report

    # ==============================================================================
    # Storage and Querying
    # ==============================================================================

    def store_audit_report(
        self,
        report: HistoricalReviewAuditReport,
        correlation_id: Optional[str] = None,
    ) -> HistoricalReviewAuditReport:
        """Store audit report in memory ring buffer and StateStore."""
        if not isinstance(report, HistoricalReviewAuditReport):
            raise ValidationError("Expected HistoricalReviewAuditReport instance.")

        with self._lock:
            if report.report_id in self._reports:
                return self._reports[report.report_id]

            inv_list = self._reports_by_investigation.setdefault(report.investigation_id, [])
            if len(inv_list) >= self.max_reports:
                inv_list.pop(0)

            inv_list.append(report.report_id)
            self._reports[report.report_id] = report
            self._report_history.append(report.report_id)

            if len(self._report_history) > self.max_reports * 5:
                oldest = self._report_history.pop(0)
                self._reports.pop(oldest, None)

            self._persist_report_to_statestore(report)

        return report

    def get_audit_report(self, report_id: str) -> Optional[HistoricalReviewAuditReport]:
        """Retrieve audit report by ID from memory or StateStore."""
        if not report_id or not SAFE_ID_PATTERN.match(report_id):
            return None

        with self._lock:
            rep = self._reports.get(report_id)
            if rep is not None:
                return rep

        if self.state_store:
            stored = self.state_store.get(f"historical_review_audit:{report_id}", None)
            if isinstance(stored, dict):
                return self._deserialize_audit_report(stored)

        return None

    def list_audit_reports(
        self,
        investigation_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[HistoricalReviewAuditReport]:
        """List audit reports with pagination and optional investigation filtering."""
        limit_clamped = max(1, min(100, limit))
        offset_clamped = max(0, offset)

        with self._lock:
            if investigation_id:
                ids = self._reports_by_investigation.get(investigation_id, [])
            else:
                ids = self._report_history

            selected_ids = ids[offset_clamped : offset_clamped + limit_clamped]
            results: List[HistoricalReviewAuditReport] = []
            for rid in selected_ids:
                r = self._reports.get(rid)
                if r:
                    results.append(r)
            return results

    def get_latest_audit_report(
        self,
        investigation_id: Optional[str] = None,
    ) -> Optional[HistoricalReviewAuditReport]:
        """Retrieve the latest audit report, optionally for a specific investigation."""
        with self._lock:
            if investigation_id:
                ids = self._reports_by_investigation.get(investigation_id, [])
                if ids:
                    return self._reports.get(ids[-1])
            elif self._report_history:
                return self._reports.get(self._report_history[-1])

        if self.state_store:
            stored = self.state_store.get("historical_review_audit:latest", None)
            if isinstance(stored, dict):
                rep = self._deserialize_audit_report(stored)
                if rep and (investigation_id is None or rep.investigation_id == investigation_id):
                    return rep

        return None

    # ==============================================================================
    # Private Helpers
    # ==============================================================================

    def _resolve_snapshot(self, snapshot_id: str) -> Optional[HistoricalReviewSnapshot]:
        """Resolve snapshot from evolution service or state store."""
        if self.evolution_service:
            s = self.evolution_service.get_snapshot(snapshot_id)
            if s:
                return s

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
                    logger.debug("Failed deserializing snapshot from StateStore: %s", exc)

        return None

    def _resolve_evolution(self, evolution_id: str) -> Optional[ReviewEvolutionRecord]:
        """Resolve evolution record from evolution service or state store."""
        if self.evolution_service:
            e = self.evolution_service.get_evolution(evolution_id)
            if e:
                return e

        if self.state_store:
            stored = self.state_store.get(f"historical_review_evolution:{evolution_id}", None)
            if isinstance(stored, dict):
                # We could deserialize if needed, or return None
                pass

        return None

    def _get_investigation_snapshots(self, investigation_id: str) -> List[HistoricalReviewSnapshot]:
        """Retrieve all snapshots for an investigation from evolution service or state store."""
        if self.evolution_service:
            return self.evolution_service.list_snapshots(investigation_id=investigation_id, limit=100)

        if self.state_store:
            history = self.state_store.get("historical_review:history", [])
            snaps: List[HistoricalReviewSnapshot] = []
            if isinstance(history, list):
                for item in history:
                    if isinstance(item, dict) and item.get("investigation_id") == investigation_id:
                        s = self._resolve_snapshot(item.get("snapshot_id", ""))
                        if s:
                            snaps.append(s)
            return snaps

        return []

    def _get_investigation_evolutions(self, investigation_id: str) -> List[ReviewEvolutionRecord]:
        """Retrieve all evolution records for an investigation."""
        if self.evolution_service:
            return self.evolution_service.list_evolution_history(investigation_id=investigation_id, limit=100)
        return []

    def _find_evolution_between(
        self,
        prev_snapshot_id: str,
        curr_snapshot_id: str,
    ) -> Optional[ReviewEvolutionRecord]:
        """Find evolution record that directly links prev_snapshot_id to curr_snapshot_id."""
        if not self.evolution_service:
            return None

        # Check by computed deterministic ID
        calc_id = compute_deterministic_evolution_id(prev_snapshot_id, curr_snapshot_id)
        evol = self.evolution_service.get_evolution(calc_id)
        if evol:
            return evol

        # Search list
        history = self.evolution_service.list_evolution_history(limit=100)
        for e in history:
            if e.previous_snapshot_id == prev_snapshot_id and e.current_snapshot_id == curr_snapshot_id:
                return e

        return None

    def _verify_snapshot_hash(self, snap: HistoricalReviewSnapshot) -> Tuple[bool, str]:
        """Verify whether snapshot_id matches deterministic hash calculation."""
        # Calculate fingerprint from states
        source_state = _unfreeze(snap.source_state)
        evidence_state = _unfreeze(snap.evidence_state)
        review_state = _unfreeze(snap.review_state)
        conflict_state = _unfreeze(snap.conflict_state)
        quality_state = _unfreeze(snap.quality_state)

        # 5-state variant (generic capture)
        fp_5 = hashlib.sha256(
            json.dumps(
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
        ).hexdigest()[:16]

        recalc_id_5 = compute_deterministic_snapshot_id(
            investigation_id=snap.investigation_id,
            outcome_id=snap.outcome_id,
            comparison_id=snap.comparison_id,
            snapshot_version=snap.snapshot_version,
            source_state_fingerprint=fp_5,
        )

        if snap.snapshot_id == recalc_id_5:
            return True, recalc_id_5

        # 4-state variant (outcome/comparison capture)
        fp_4 = hashlib.sha256(
            json.dumps(
                {
                    "source_state": source_state,
                    "evidence_state": evidence_state,
                    "review_state": review_state,
                    "conflict_state": conflict_state,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:16]

        recalc_id_4 = compute_deterministic_snapshot_id(
            investigation_id=snap.investigation_id,
            outcome_id=snap.outcome_id,
            comparison_id=snap.comparison_id,
            snapshot_version=snap.snapshot_version,
            source_state_fingerprint=fp_4,
        )

        if snap.snapshot_id == recalc_id_4:
            return True, recalc_id_4

        return False, recalc_id_5

    def _extract_conflict_identifiers(self, snap: HistoricalReviewSnapshot) -> Set[str]:
        """Extract set of active conflict IDs from snapshot conflict_state."""
        c_state = _unfreeze(snap.conflict_state)
        conflicts: Set[str] = set()

        raw_conflicts = (
            c_state.get("active_conflicts", [])
            or c_state.get("conflicts", [])
            or c_state.get("conflict_summary", {}).get("active_conflicts", [])
        )
        if isinstance(raw_conflicts, list):
            for c in raw_conflicts:
                if isinstance(c, dict):
                    cid = c.get("conflict_id") or c.get("discrepancy_id") or c.get("id")
                    if cid:
                        conflicts.add(str(cid))
                elif isinstance(c, str):
                    conflicts.add(c)

        return conflicts

    def _build_voice_summary_text(
        self,
        investigation_id: str,
        overall_status: RecordValidationStatus,
        snapshots_count: int,
        findings_count: int,
        chain_res: HistoricalChainIntegrityResult,
    ) -> str:
        """Construct voice-safe analytical audit summary avoiding prohibited financial vocabulary."""
        if overall_status == RecordValidationStatus.VALID:
            return (
                f"Audit completed for investigation {investigation_id}. "
                f"All {snapshots_count} review snapshots verified valid."
            )

        if chain_res.detected_gaps:
            return (
                f"Audit completed for investigation {investigation_id}. "
                f"Historical gaps detected in review sequence with status {overall_status.value.lower()}."
            )

        if not chain_res.chronology_intact:
            return (
                f"Audit completed for investigation {investigation_id}. "
                f"Chronological ordering inconsistencies detected across snapshots."
            )

        if findings_count > 0:
            return (
                f"Audit completed for investigation {investigation_id} with status "
                f"{overall_status.value.lower()}. {findings_count} findings recorded."
            )

        return (
            f"Audit completed for investigation {investigation_id} with status "
            f"{overall_status.value.lower()}."
        )

    def _persist_report_to_statestore(self, report: HistoricalReviewAuditReport) -> None:
        """Persist report to StateStore under bounded historical_review_audit namespaces."""
        if not self.state_store:
            return
        try:
            rep_dict = report.to_dict()
            self.state_store.set("historical_review_audit:latest", rep_dict)
            self.state_store.set(f"historical_review_audit:{report.report_id}", rep_dict)

            history = self.state_store.get("historical_review_audit:history", [])
            if not isinstance(history, list):
                history = []
            history.append(rep_dict)
            if len(history) > self.max_reports * 5:
                history = history[-(self.max_reports * 5):]
            self.state_store.set("historical_review_audit:history", history)
        except Exception as exc:
            logger.warning("Failed to persist audit report to StateStore: %s", exc)

    def _deserialize_audit_report(self, data: Dict[str, Any]) -> Optional[HistoricalReviewAuditReport]:
        """Safely deserialize audit report dictionary from StateStore."""
        try:
            # Reconstruct findings
            raw_findings = data.get("findings", [])
            findings: List[EvolutionAuditFinding] = []
            for f_dict in raw_findings:
                if isinstance(f_dict, dict):
                    findings.append(
                        EvolutionAuditFinding(
                            finding_id=f_dict["finding_id"],
                            category=AuditFindingCategory(f_dict["category"]),
                            severity=AuditFindingSeverity(f_dict["severity"]),
                            target_type=f_dict["target_type"],
                            target_id=f_dict["target_id"],
                            field_path=f_dict["field_path"],
                            message=f_dict["message"],
                            observed_value=f_dict.get("observed_value"),
                            expected_value=f_dict.get("expected_value"),
                            source_reference=f_dict.get("source_reference"),
                        )
                    )

            # Reconstruct voice summary
            raw_voice = data.get("voice_summary")
            voice_summary = None
            if isinstance(raw_voice, dict):
                voice_summary = EvolutionAuditVoiceSummary(
                    report_id=raw_voice["report_id"],
                    investigation_id=raw_voice["investigation_id"],
                    overall_status=RecordValidationStatus(raw_voice["overall_status"]),
                    summary_text=raw_voice["summary_text"],
                    voice_safe=raw_voice.get("voice_safe", True),
                )

            return HistoricalReviewAuditReport(
                report_id=data["report_id"],
                investigation_id=data["investigation_id"],
                generated_at=data.get("generated_at", 0.0),
                lifecycle_state=AuditReportLifecycleState(data.get("lifecycle_state", "STORED")),
                overall_status=RecordValidationStatus(data.get("overall_status", "VALID")),
                snapshots_audited_count=data.get("snapshots_audited_count", 0),
                evolutions_audited_count=data.get("evolutions_audited_count", 0),
                findings_summary=data.get("findings_summary", {}),
                snapshot_results=(),
                evolution_results=(),
                chain_result=None,
                reproducibility_result=None,
                findings=tuple(findings),
                voice_summary=voice_summary,
                traceability=data.get("traceability", {}),
            )
        except Exception as exc:
            logger.warning("Error deserializing HistoricalReviewAuditReport: %s", exc)
            return None


def uuid_hex(val1: str, val2: str = "") -> str:
    """Helper to produce deterministic short hex from strings."""
    return hashlib.sha256(f"{val1}:{val2}".encode("utf-8")).hexdigest()
