"""KAISER — Configuration, Provenance, and Quality Difference Engines.

Provides deterministic, secret-scrubbed diff algorithms for:
- Nested configuration dictionaries with dot-notation paths.
- Step 20 market data provenance metadata across 10 dimensions.
- Step 19 market data quality assessments across 7 dimensions.
"""

from __future__ import annotations

import collections
import copy
from enum import Enum
import hashlib
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.comparison.models import (
    ConfigurationDifference,
    DifferenceType,
    ProvenanceDifference,
    QualityDifference,
)
from app.analysis.runs.models import AnalysisRun
from app.market.provenance.models import CREDENTIAL_KEY_PATTERNS
from app.market.quality.status import DataQualityStatus

DEFAULT_MAX_DIFFS = 500


def _is_secret_key(key: str) -> bool:
    """Check if a parameter key is likely to contain sensitive credentials."""
    lowered = key.lower()
    if CREDENTIAL_KEY_PATTERNS.search(lowered):
        return True
    return any(s in lowered for s in ("secret", "token", "password", "key", "credential", "auth", "private"))


def _flatten_mapping(
    obj: Any,
    prefix: str = "",
    parent_is_secret: bool = False,
) -> Dict[str, Tuple[Any, bool]]:
    """Recursively flatten nested mappings and iterables into dot-notated paths.

    Returns:
        Dict[str, Tuple[value, is_secret]]
    """
    flat: Dict[str, Tuple[Any, bool]] = {}

    if obj is None:
        return {prefix: (None, parent_is_secret)} if prefix else {}

    if isinstance(obj, Mapping):
        for k, v in sorted(obj.items(), key=lambda item: str(item[0])):
            str_k = str(k).strip()
            path = f"{prefix}.{str_k}" if prefix else str_k
            is_secret = parent_is_secret or _is_secret_key(str_k)
            if isinstance(v, (Mapping, list, tuple, set, frozenset)) and not isinstance(v, (str, bytes)):
                sub = _flatten_mapping(v, prefix=path, parent_is_secret=is_secret)
                flat.update(sub)
            else:
                flat[path] = (v, is_secret)
        return flat

    # Extract dict if object has to_dict
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        try:
            extracted = obj.to_dict()
            return _flatten_mapping(extracted, prefix=prefix, parent_is_secret=parent_is_secret)
        except Exception:
            pass
    elif hasattr(obj, "__dict__") and not isinstance(obj, (type, Enum)):
        try:
            extracted = vars(obj)
            return _flatten_mapping(extracted, prefix=prefix, parent_is_secret=parent_is_secret)
        except Exception:
            pass

    if isinstance(obj, (list, tuple, set, frozenset)) and not isinstance(obj, (str, bytes)):
        items = sorted(list(obj), key=lambda x: str(x)) if isinstance(obj, (set, frozenset)) else list(obj)
        if len(items) == 0:
            flat[prefix] = ((), parent_is_secret)
        else:
            for idx, item in enumerate(items):
                path = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
                if isinstance(item, (Mapping, list, tuple, set, frozenset)) and not isinstance(item, (str, bytes)):
                    sub = _flatten_mapping(item, prefix=path, parent_is_secret=parent_is_secret)
                    flat.update(sub)
                else:
                    flat[path] = (item, parent_is_secret)
    else:
        flat[prefix] = (obj, parent_is_secret)

    return flat


def diff_configurations(
    left_config: Any,
    right_config: Any,
    max_diffs: int = DEFAULT_MAX_DIFFS,
) -> List[ConfigurationDifference]:
    """Deterministically compute parameter differences between two run configurations.

    Args:
        left_config: Baseline configuration dictionary or object.
        right_config: Candidate configuration dictionary or object.
        max_diffs: Maximum number of differences to return before truncation.

    Returns:
        List[ConfigurationDifference] sorted lexicographically by parameter path.
    """
    left_flat = _flatten_mapping(left_config)
    right_flat = _flatten_mapping(right_config)

    all_keys = sorted(set(left_flat.keys()) | set(right_flat.keys()))
    diffs: List[ConfigurationDifference] = []

    for k in all_keys:
        in_left = k in left_flat
        in_right = k in right_flat

        if in_left and in_right:
            l_val, l_sec = left_flat[k]
            r_val, r_sec = right_flat[k]
            is_sec = l_sec or r_sec

            # Equality comparison
            is_equal = False
            if is_sec:
                is_equal = (l_val == r_val)
            elif l_val == r_val:
                is_equal = True
            elif isinstance(l_val, (int, float)) and isinstance(r_val, (int, float)):
                is_equal = abs(float(l_val) - float(r_val)) < 1e-9

            diff_type = DifferenceType.UNCHANGED if is_equal else DifferenceType.CHANGED
            diffs.append(
                ConfigurationDifference(
                    parameter_path=k,
                    left_value="[REDACTED]" if is_sec and l_val is not None else l_val,
                    right_value="[REDACTED]" if is_sec and r_val is not None else r_val,
                    diff_type=diff_type,
                    is_secret=is_sec,
                )
            )
        elif in_left:
            l_val, l_sec = left_flat[k]
            diffs.append(
                ConfigurationDifference(
                    parameter_path=k,
                    left_value="[REDACTED]" if l_sec and l_val is not None else l_val,
                    right_value=None,
                    diff_type=DifferenceType.REMOVED,
                    is_secret=l_sec,
                )
            )
        else:
            r_val, r_sec = right_flat[k]
            diffs.append(
                ConfigurationDifference(
                    parameter_path=k,
                    left_value=None,
                    right_value="[REDACTED]" if r_sec and r_val is not None else r_val,
                    diff_type=DifferenceType.ADDED,
                    is_secret=r_sec,
                )
            )

        if len(diffs) >= max_diffs:
            break

    return diffs


def _extract_provenance_details(run: AnalysisRun, provenance_tracker: Optional[Any] = None) -> Dict[str, Any]:
    """Extract secondary provenance metadata from run execution_metadata or tracker."""
    pm = dict(run.execution_metadata.get("provenance_summary", {}) or {})
    for k in ("source", "source_identifier", "transformation_version", "parent_lineage"):
        if k in run.execution_metadata and k not in pm:
            pm[k] = run.execution_metadata[k]

    if provenance_tracker is not None and run.input_provenance_ids:
        for pid in run.input_provenance_ids:
            p_rec = None
            if hasattr(provenance_tracker, "get_provenance"):
                p_rec = provenance_tracker.get_provenance(pid)
            elif hasattr(provenance_tracker, "get"):
                p_rec = provenance_tracker.get(pid)
            if p_rec is not None:
                if "source" not in pm and hasattr(p_rec, "source"):
                    pm["source"] = getattr(p_rec.source, "value", str(p_rec.source))
                if "source_identifier" not in pm and hasattr(p_rec, "source_identifier"):
                    pm["source_identifier"] = p_rec.source_identifier
                if "transformation_version" not in pm and hasattr(p_rec, "transformation_history"):
                    pm["transformation_version"] = str(p_rec.transformation_history)
                if "parent_lineage" not in pm and hasattr(p_rec, "parent_provenance_ids"):
                    pm["parent_lineage"] = tuple(p_rec.parent_provenance_ids)
                break

    return pm


def diff_provenance(
    left_run: AnalysisRun,
    right_run: AnalysisRun,
    provenance_tracker: Optional[Any] = None,
) -> List[ProvenanceDifference]:
    """Compute structured differences across Step 20 market data provenance references.

    Evaluates:
    - input_provenance_ids
    - provenance availability (unrecorded/missing check)
    - source
    - source identifier
    - symbol universe
    - timeframe universe
    - data range
    - record count
    - quality assessment IDs
    - transformation / version
    - parent lineage
    """
    diffs: List[ProvenanceDifference] = []

    # 1. Input Provenance IDs
    l_prov = set(left_run.input_provenance_ids)
    r_prov = set(right_run.input_provenance_ids)
    if not l_prov and not r_prov:
        diffs.append(
            ProvenanceDifference(
                dimension="input_provenance_ids",
                left_value=(),
                right_value=(),
                diff_type=DifferenceType.UNCHANGED,
                description="Provenance IDs unrecorded on both runs",
            )
        )
        diffs.append(
            ProvenanceDifference(
                dimension="provenance_availability",
                left_value=False,
                right_value=False,
                diff_type=DifferenceType.CHANGED,
                description="Market data provenance is unrecorded or missing on both runs; equivalence cannot be inferred.",
            )
        )
    elif not l_prov or not r_prov:
        diffs.append(
            ProvenanceDifference(
                dimension="provenance_availability",
                left_value=bool(l_prov),
                right_value=bool(r_prov),
                diff_type=DifferenceType.CHANGED,
                description="Market data provenance is unrecorded or missing on one run; equivalence cannot be inferred.",
            )
        )
        diffs.append(
            ProvenanceDifference(
                dimension="input_provenance_ids",
                left_value=tuple(sorted(l_prov)),
                right_value=tuple(sorted(r_prov)),
                diff_type=DifferenceType.CHANGED,
                description=f"Provenance IDs missing on {'baseline' if not l_prov else 'candidate'} run",
            )
        )
    elif l_prov == r_prov:
        diffs.append(
            ProvenanceDifference(
                dimension="input_provenance_ids",
                left_value=tuple(sorted(l_prov)),
                right_value=tuple(sorted(r_prov)),
                diff_type=DifferenceType.UNCHANGED,
                description="Identical input provenance IDs",
            )
        )
    else:
        desc = (
            f"Provenance ID mismatch: {len(l_prov - r_prov)} left-only, "
            f"{len(r_prov - l_prov)} right-only, {len(l_prov & r_prov)} shared"
        )
        diffs.append(
            ProvenanceDifference(
                dimension="input_provenance_ids",
                left_value=tuple(sorted(l_prov)),
                right_value=tuple(sorted(r_prov)),
                diff_type=DifferenceType.CHANGED,
                description=desc,
            )
        )

    # 2. Source & Source Identifier
    l_pdet = _extract_provenance_details(left_run, provenance_tracker)
    r_pdet = _extract_provenance_details(right_run, provenance_tracker)

    l_src = l_pdet.get("source")
    r_src = r_pdet.get("source")
    if l_src is not None or r_src is not None:
        src_diff = DifferenceType.UNCHANGED if l_src == r_src else DifferenceType.CHANGED
        diffs.append(
            ProvenanceDifference(
                dimension="source",
                left_value=l_src,
                right_value=r_src,
                diff_type=src_diff,
                description=f"Source unchanged ({l_src})" if src_diff == DifferenceType.UNCHANGED else f"Source shifted from {l_src} to {r_src}",
            )
        )

    l_src_id = l_pdet.get("source_identifier")
    r_src_id = r_pdet.get("source_identifier")
    if l_src_id is not None or r_src_id is not None:
        src_id_diff = DifferenceType.UNCHANGED if l_src_id == r_src_id else DifferenceType.CHANGED
        diffs.append(
            ProvenanceDifference(
                dimension="source_identifier",
                left_value=l_src_id,
                right_value=r_src_id,
                diff_type=src_id_diff,
                description=f"Source identifier unchanged ({l_src_id})" if src_id_diff == DifferenceType.UNCHANGED else f"Source identifier shifted from {l_src_id} to {r_src_id}",
            )
        )

    # 3. Symbol Universe
    l_syms = tuple(sorted(left_run.symbol_universe))
    r_syms = tuple(sorted(right_run.symbol_universe))
    diffs.append(
        ProvenanceDifference(
            dimension="symbol_universe",
            left_value=l_syms,
            right_value=r_syms,
            diff_type=DifferenceType.UNCHANGED if l_syms == r_syms else DifferenceType.CHANGED,
            description="Identical symbol universe" if l_syms == r_syms else f"Symbols changed from {l_syms} to {r_syms}",
        )
    )

    # 4. Timeframe Universe
    l_tfs = tuple(sorted(left_run.timeframe_universe))
    r_tfs = tuple(sorted(right_run.timeframe_universe))
    diffs.append(
        ProvenanceDifference(
            dimension="timeframe_universe",
            left_value=l_tfs,
            right_value=r_tfs,
            diff_type=DifferenceType.UNCHANGED if l_tfs == r_tfs else DifferenceType.CHANGED,
            description="Identical timeframe universe" if l_tfs == r_tfs else f"Timeframes changed from {l_tfs} to {r_tfs}",
        )
    )

    # 5. Data Range (start, end)
    l_range = (left_run.data_start, left_run.data_end)
    r_range = (right_run.data_start, right_run.data_end)
    diffs.append(
        ProvenanceDifference(
            dimension="data_range",
            left_value=l_range,
            right_value=r_range,
            diff_type=DifferenceType.UNCHANGED if l_range == r_range else DifferenceType.CHANGED,
            description="Identical date range" if l_range == r_range else f"Date range shifted from {l_range} to {r_range}",
        )
    )

    # 6. Input Record Count
    l_cnt = left_run.input_record_count
    r_cnt = right_run.input_record_count
    diffs.append(
        ProvenanceDifference(
            dimension="input_record_count",
            left_value=l_cnt,
            right_value=r_cnt,
            diff_type=DifferenceType.UNCHANGED if l_cnt == r_cnt else DifferenceType.CHANGED,
            description="Identical record count" if l_cnt == r_cnt else f"Record count delta: {r_cnt - l_cnt:+d}",
        )
    )

    # 7. Quality Assessment IDs
    l_qids = tuple(sorted(left_run.quality_assessment_ids))
    r_qids = tuple(sorted(right_run.quality_assessment_ids))
    diffs.append(
        ProvenanceDifference(
            dimension="quality_assessment_ids",
            left_value=l_qids,
            right_value=r_qids,
            diff_type=DifferenceType.UNCHANGED if l_qids == r_qids else DifferenceType.CHANGED,
            description="Identical quality assessments" if l_qids == r_qids else f"Quality assessment IDs changed ({len(l_qids)} left vs {len(r_qids)} right)",
        )
    )

    # 8. Transformation / Version
    l_tr = l_pdet.get("transformation_version", left_run.analysis_version)
    r_tr = r_pdet.get("transformation_version", right_run.analysis_version)
    diffs.append(
        ProvenanceDifference(
            dimension="transformation_version",
            left_value=l_tr,
            right_value=r_tr,
            diff_type=DifferenceType.UNCHANGED if l_tr == r_tr else DifferenceType.CHANGED,
            description=f"Transformation version unchanged ({l_tr})" if l_tr == r_tr else f"Transformation version changed from {l_tr} to {r_tr}",
        )
    )

    # 9. Parent Lineage
    l_pl = tuple(sorted(left_run.parent_run_ids)) or l_pdet.get("parent_lineage", ())
    r_pl = tuple(sorted(right_run.parent_run_ids)) or r_pdet.get("parent_lineage", ())
    diffs.append(
        ProvenanceDifference(
            dimension="parent_lineage",
            left_value=tuple(l_pl),
            right_value=tuple(r_pl),
            diff_type=DifferenceType.UNCHANGED if l_pl == r_pl else DifferenceType.CHANGED,
            description="Identical parent lineage" if l_pl == r_pl else f"Parent lineage changed from {tuple(l_pl)} to {tuple(r_pl)}",
        )
    )

    return diffs


def _extract_quality_metrics(run: AnalysisRun, quality_monitor: Optional[Any] = None) -> Dict[str, Any]:
    """Extract Step 19 quality metrics from run metadata or quality monitor."""
    qm = dict(run.execution_metadata.get("quality_summary", {}) or {})
    for k in (
        "quality_score",
        "invalid_record_count",
        "invalid_records",
        "duplicate_count",
        "gap_count",
        "anomaly_count",
        "highest_severity",
    ):
        if k in run.execution_metadata and k not in qm:
            qm[k] = run.execution_metadata[k]

    if quality_monitor is not None and run.quality_assessment_ids:
        for qid in run.quality_assessment_ids:
            qa = None
            if hasattr(quality_monitor, "get_assessment"):
                qa = quality_monitor.get_assessment(qid)
            if qa is not None:
                if "quality_score" not in qm and hasattr(qa, "quality_score"):
                    qm["quality_score"] = qa.quality_score
                if "invalid_records" not in qm and hasattr(qa, "invalid_records"):
                    qm["invalid_records"] = qa.invalid_records
                if "duplicate_count" not in qm and hasattr(qa, "duplicate_count"):
                    qm["duplicate_count"] = qa.duplicate_count
                if "gap_count" not in qm and hasattr(qa, "gap_count"):
                    qm["gap_count"] = qa.gap_count
                if "anomaly_count" not in qm and hasattr(qa, "anomaly_count"):
                    qm["anomaly_count"] = qa.anomaly_count
                if "highest_severity" not in qm and hasattr(qa, "highest_severity"):
                    qm["highest_severity"] = getattr(qa.highest_severity, "value", str(qa.highest_severity))
                break

    if "invalid_records" in qm and "invalid_record_count" not in qm:
        qm["invalid_record_count"] = qm["invalid_records"]

    return qm


def diff_quality(
    left_run: AnalysisRun,
    right_run: AnalysisRun,
    quality_monitor: Optional[Any] = None,
) -> List[QualityDifference]:
    """Compute structured differences across Step 19 data quality conditions.

    Evaluates:
    - quality_status (e.g. VALID -> DEGRADED)
    - quality_score
    - invalid_record_count
    - duplicate_count
    - gap_count
    - anomaly_count
    - highest_severity
    - quality_assessment_ids
    """
    diffs: List[QualityDifference] = []

    # 1. Quality Status
    l_status = left_run.quality_status
    r_status = right_run.quality_status
    l_st_val = l_status.value if isinstance(l_status, DataQualityStatus) else (str(l_status) if l_status is not None else None)
    r_st_val = r_status.value if isinstance(r_status, DataQualityStatus) else (str(r_status) if r_status is not None else None)

    if l_st_val is None or r_st_val is None:
        diffs.append(
            QualityDifference(
                dimension="quality_status",
                left_value=l_st_val,
                right_value=r_st_val,
                diff_type=DifferenceType.CHANGED,
                description=f"Quality status missing on {'both runs' if l_st_val is None and r_st_val is None else ('baseline run' if l_st_val is None else 'candidate run')}",
            )
        )
    elif l_st_val == r_st_val:
        diffs.append(
            QualityDifference(
                dimension="quality_status",
                left_value=l_st_val,
                right_value=r_st_val,
                diff_type=DifferenceType.UNCHANGED,
                description=f"Quality status unchanged ({l_st_val})",
            )
        )
    else:
        diffs.append(
            QualityDifference(
                dimension="quality_status",
                left_value=l_st_val,
                right_value=r_st_val,
                diff_type=DifferenceType.CHANGED,
                description=f"Quality status shifted from {l_st_val} to {r_st_val}",
            )
        )

    # Extract extended quality state
    l_qmetrics = _extract_quality_metrics(left_run, quality_monitor)
    r_qmetrics = _extract_quality_metrics(right_run, quality_monitor)

    # 2. Quality Score
    l_sc = l_qmetrics.get("quality_score")
    r_sc = r_qmetrics.get("quality_score")
    if l_sc is not None or r_sc is not None:
        sc_diff = DifferenceType.UNCHANGED if l_sc == r_sc else DifferenceType.CHANGED
        diffs.append(
            QualityDifference(
                dimension="quality_score",
                left_value=l_sc,
                right_value=r_sc,
                diff_type=sc_diff,
                description=f"Quality score unchanged ({l_sc})" if sc_diff == DifferenceType.UNCHANGED else f"Quality score changed from {l_sc} to {r_sc}",
            )
        )

    # 3. Invalid Record Count
    l_inv = l_qmetrics.get("invalid_record_count")
    r_inv = r_qmetrics.get("invalid_record_count")
    if l_inv is not None or r_inv is not None:
        inv_diff = DifferenceType.UNCHANGED if l_inv == r_inv else DifferenceType.CHANGED
        diffs.append(
            QualityDifference(
                dimension="invalid_record_count",
                left_value=l_inv,
                right_value=r_inv,
                diff_type=inv_diff,
                description=f"Invalid record count unchanged ({l_inv})" if inv_diff == DifferenceType.UNCHANGED else f"Invalid record count changed from {l_inv} to {r_inv}",
            )
        )

    # 4. Duplicate Count
    l_dup = l_qmetrics.get("duplicate_count")
    r_dup = r_qmetrics.get("duplicate_count")
    if l_dup is not None or r_dup is not None:
        dup_diff = DifferenceType.UNCHANGED if l_dup == r_dup else DifferenceType.CHANGED
        diffs.append(
            QualityDifference(
                dimension="duplicate_count",
                left_value=l_dup,
                right_value=r_dup,
                diff_type=dup_diff,
                description=f"Duplicate count unchanged ({l_dup})" if dup_diff == DifferenceType.UNCHANGED else f"Duplicate count changed from {l_dup} to {r_dup}",
            )
        )

    # 5. Gap Count
    l_gap = l_qmetrics.get("gap_count")
    r_gap = r_qmetrics.get("gap_count")
    if l_gap is not None or r_gap is not None:
        gap_diff = DifferenceType.UNCHANGED if l_gap == r_gap else DifferenceType.CHANGED
        diffs.append(
            QualityDifference(
                dimension="gap_count",
                left_value=l_gap,
                right_value=r_gap,
                diff_type=gap_diff,
                description=f"Gap count unchanged ({l_gap})" if gap_diff == DifferenceType.UNCHANGED else f"Gap count changed from {l_gap} to {r_gap}",
            )
        )

    # 6. Anomaly Count
    l_anom = l_qmetrics.get("anomaly_count")
    r_anom = r_qmetrics.get("anomaly_count")
    if l_anom is not None or r_anom is not None:
        anom_diff = DifferenceType.UNCHANGED if l_anom == r_anom else DifferenceType.CHANGED
        diffs.append(
            QualityDifference(
                dimension="anomaly_count",
                left_value=l_anom,
                right_value=r_anom,
                diff_type=anom_diff,
                description=f"Anomaly count unchanged ({l_anom})" if anom_diff == DifferenceType.UNCHANGED else f"Anomaly count changed from {l_anom} to {r_anom}",
            )
        )

    # 7. Highest Severity
    l_sev = l_qmetrics.get("highest_severity")
    r_sev = r_qmetrics.get("highest_severity")
    if l_sev is not None or r_sev is not None:
        sev_diff = DifferenceType.UNCHANGED if l_sev == r_sev else DifferenceType.CHANGED
        diffs.append(
            QualityDifference(
                dimension="highest_severity",
                left_value=l_sev,
                right_value=r_sev,
                diff_type=sev_diff,
                description=f"Highest severity unchanged ({l_sev})" if sev_diff == DifferenceType.UNCHANGED else f"Highest severity changed from {l_sev} to {r_sev}",
            )
        )

    # 8. Quality Assessment IDs
    l_qids = tuple(sorted(left_run.quality_assessment_ids))
    r_qids = tuple(sorted(right_run.quality_assessment_ids))
    diffs.append(
        QualityDifference(
            dimension="quality_assessment_ids",
            left_value=l_qids,
            right_value=r_qids,
            diff_type=DifferenceType.UNCHANGED if l_qids == r_qids else DifferenceType.CHANGED,
            description="Identical assessment IDs" if l_qids == r_qids else f"Assessment IDs changed ({len(l_qids)} left vs {len(r_qids)} right)",
        )
    )

    return diffs
