"""KAISER — Run Comparison & Experiment Evaluation Engine.

Orchestrates deterministic analytical run comparability checks, parameter diffing,
provenance diffing, quality diffing, and numerical metric delta calculation.

Features:
- Strict comparability categorization (COMPARABLE, PARTIALLY_COMPARABLE, NOT_COMPARABLE).
- Explicit, structured reasons for every incompatibility.
- Directional neutrality with zero subjective "winning strategy" designations.
- Bounded LRU/FIFO comparison cache.
- EventBus telemetry and StateStore synchronization.
"""

from __future__ import annotations

import collections
import copy
import hashlib
import json
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.comparison.diff import (
    diff_configurations,
    diff_provenance,
    diff_quality,
)
from app.analysis.comparison.metrics import (
    compare_metrics_dicts,
    extract_metrics_from_result,
)
from app.analysis.comparison.models import (
    ComparabilityStatus,
    ComparisonDimension,
    MultiRunComparison,
    RunComparison,
)
from app.analysis.runs.models import AnalysisRun, OperationType, RunStatus
from app.analysis.runs.registry import AnalysisRunRegistry
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.quality.status import DataQualityStatus

logger = logging.getLogger("kaiser.analysis.comparison.engine")

DEFAULT_MAX_COMPARISON_CACHE = 500
DEFAULT_MAX_GROUP_RUNS = 20


class RunComparisonEngine:
    """Thread-safe engine for comparing analytical runs and experiment evaluations."""

    def __init__(
        self,
        run_registry: Optional[AnalysisRunRegistry] = None,
        provenance_tracker: Optional[Any] = None,
        quality_monitor: Optional[Any] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        clock: Optional[Callable[[], float]] = None,
        max_cache_size: int = DEFAULT_MAX_COMPARISON_CACHE,
        max_group_runs: int = DEFAULT_MAX_GROUP_RUNS,
    ) -> None:
        """Initialize the run comparison engine.

        Args:
            run_registry: AnalysisRunRegistry providing historical run records.
            provenance_tracker: Optional MarketDataProvenanceTracker for deep provenance diffs.
            quality_monitor: Optional MarketDataQualityMonitor for deep quality diffs.
            event_bus: Optional EventBus for emitting comparison events.
            state_store: Optional StateStore for persisting bounded comparison snapshots.
            clock: Injected POSIX clock for deterministic time simulation.
            max_cache_size: Maximum comparisons retained in memory cache.
            max_group_runs: Maximum runs allowed in a multi-run comparison group.
        """
        self.run_registry = run_registry
        self.provenance_tracker = provenance_tracker
        self.quality_monitor = quality_monitor
        self.event_bus = event_bus
        self.state_store = state_store
        self.clock = clock or time.time
        self.max_cache_size = max(1, min(10000, max_cache_size))
        self.max_group_runs = max(2, min(100, max_group_runs))

        self._lock = threading.RLock()
        self._cache: Dict[Tuple[str, str], RunComparison] = {}
        self._cache_keys: collections.deque[Tuple[str, str]] = collections.deque()

    def evaluate_comparability(
        self,
        left_run: AnalysisRun,
        right_run: AnalysisRun,
    ) -> Tuple[ComparabilityStatus, Tuple[str, ...], Tuple[ComparisonDimension, ...]]:
        """Evaluate whether two runs are directly, partially, or not comparable.

        Evaluates 11 core dimensions:
        - operation_type
        - symbol_universe
        - timeframe_universe
        - data_range
        - data_provenance
        - data_quality
        - configuration
        - strategy
        - analysis_version
        - software_version
        - result_type

        Returns:
            Tuple of (ComparabilityStatus, tuple of string reasons, tuple of changed dimensions).
        """
        reasons: List[str] = []
        changed_dims: List[ComparisonDimension] = []

        is_not_comparable = False
        is_partially_comparable = False

        # 1. Operation Type
        if left_run.operation_type != right_run.operation_type:
            is_not_comparable = True
            changed_dims.append(ComparisonDimension.OPERATION_TYPE)
            reasons.append(
                f"Incompatible operation types: '{left_run.operation_type.value}' vs '{right_run.operation_type.value}'"
            )

        # 2. Result Type
        if left_run.result_type and right_run.result_type and left_run.result_type != right_run.result_type:
            is_not_comparable = True
            changed_dims.append(ComparisonDimension.RESULT_TYPE)
            reasons.append(
                f"Incompatible result types: '{left_run.result_type}' vs '{right_run.result_type}'"
            )

        # 3. Symbol Universe
        l_syms = set(left_run.symbol_universe)
        r_syms = set(right_run.symbol_universe)
        if l_syms != r_syms:
            is_not_comparable = True
            changed_dims.append(ComparisonDimension.SYMBOL_UNIVERSE)
            reasons.append(
                f"Different symbol universes: {tuple(sorted(l_syms))} vs {tuple(sorted(r_syms))}"
            )

        # 4. Timeframe Universe
        l_tfs = set(left_run.timeframe_universe)
        r_tfs = set(right_run.timeframe_universe)
        if l_tfs != r_tfs:
            is_not_comparable = True
            changed_dims.append(ComparisonDimension.TIMEFRAME_UNIVERSE)
            reasons.append(
                f"Different timeframe universes: {tuple(sorted(l_tfs))} vs {tuple(sorted(r_tfs))}"
            )

        # 5. Software / Schema Version
        if left_run.schema_version != right_run.schema_version:
            is_not_comparable = True
            changed_dims.append(ComparisonDimension.SOFTWARE_VERSION)
            reasons.append(
                f"Incompatible schema versions: '{left_run.schema_version}' vs '{right_run.schema_version}'"
            )
        elif left_run.software_version != right_run.software_version:
            is_partially_comparable = True
            changed_dims.append(ComparisonDimension.SOFTWARE_VERSION)
            reasons.append(
                f"Different software versions: '{left_run.software_version}' vs '{right_run.software_version}'"
            )

        # 6. Strategy Identity & Version
        if left_run.strategy_id or right_run.strategy_id:
            if (left_run.strategy_id or "").lower() != (right_run.strategy_id or "").lower():
                is_partially_comparable = True
                changed_dims.append(ComparisonDimension.STRATEGY)
                reasons.append(
                    f"Different strategies evaluated: '{left_run.strategy_id}' vs '{right_run.strategy_id}'"
                )
            elif left_run.strategy_version != right_run.strategy_version:
                is_partially_comparable = True
                changed_dims.append(ComparisonDimension.STRATEGY)
                reasons.append(
                    f"Different strategy versions: '{left_run.strategy_version}' vs '{right_run.strategy_version}'"
                )

        # 7. Analysis Version
        if left_run.analysis_version != right_run.analysis_version:
            is_partially_comparable = True
            changed_dims.append(ComparisonDimension.ANALYSIS_VERSION)
            reasons.append(
                f"Different analysis versions: '{left_run.analysis_version}' vs '{right_run.analysis_version}'"
            )

        # 8. Data Range (start / end)
        if left_run.data_start != right_run.data_start or left_run.data_end != right_run.data_end:
            is_partially_comparable = True
            changed_dims.append(ComparisonDimension.DATA_RANGE)
            reasons.append(
                f"Different historical evaluation date ranges: "
                f"({left_run.data_start}, {left_run.data_end}) vs ({right_run.data_start}, {right_run.data_end})"
            )

        # 9. Data Provenance
        l_prov = set(left_run.input_provenance_ids)
        r_prov = set(right_run.input_provenance_ids)
        if not l_prov or not r_prov:
            is_partially_comparable = True
            changed_dims.append(ComparisonDimension.DATA_PROVENANCE)
            reasons.append("Market data provenance is unrecorded or missing on one or both runs; provenance equivalence cannot be inferred.")
        elif l_prov != r_prov:
            is_partially_comparable = True
            changed_dims.append(ComparisonDimension.DATA_PROVENANCE)
            reasons.append("Different market data provenance inputs or ingestion lineage.")

        # 10. Data Quality Status
        if left_run.quality_status is None or right_run.quality_status is None:
            is_partially_comparable = True
            changed_dims.append(ComparisonDimension.DATA_QUALITY)
            reasons.append("Market data quality status is unrecorded or missing on one or both runs; quality equivalence cannot be inferred.")
        elif left_run.quality_status != right_run.quality_status:
            is_partially_comparable = True
            changed_dims.append(ComparisonDimension.DATA_QUALITY)
            reasons.append(
                f"Different data quality conditions: {left_run.quality_status} vs {right_run.quality_status}"
            )

        # 11. Configuration Fingerprint
        if left_run.configuration_fingerprint != right_run.configuration_fingerprint:
            is_partially_comparable = True
            changed_dims.append(ComparisonDimension.CONFIGURATION)
            reasons.append("Different configuration parameters detected.")

        # Determine overall status
        if is_not_comparable:
            status = ComparabilityStatus.NOT_COMPARABLE
        elif is_partially_comparable:
            status = ComparabilityStatus.PARTIALLY_COMPARABLE
        else:
            status = ComparabilityStatus.COMPARABLE
            reasons.append("Identical operation, data universe, provenance, and configuration.")

        return status, tuple(reasons), tuple(changed_dims)

    def _resolve_run(self, run_or_id: Union[AnalysisRun, str]) -> AnalysisRun:
        """Resolve an AnalysisRun object from an instance or registry ID."""
        if isinstance(run_or_id, AnalysisRun):
            return run_or_id

        if not isinstance(run_or_id, str) or not run_or_id.strip():
            raise ValidationError("run_id must be a non-empty string.")

        clean_id = run_or_id.strip()
        if self.run_registry:
            run = self.run_registry.get_run(clean_id)
            if run is not None:
                return run

        raise ValidationError(f"AnalysisRun '{clean_id}' not found in registry.")

    def _resolve_result_data(self, run: AnalysisRun, explicit_result: Optional[Any] = None) -> Dict[str, Any]:
        """Resolve metric dictionary from explicit result or StateStore."""
        if explicit_result is not None:
            return extract_metrics_from_result(explicit_result, run.execution_metadata)

        # Look up in StateStore if available
        if self.state_store and run.result_id:
            for prefix in ("backtest", "portfolio_simulation", "walk_forward", "robustness", "portfolio_attribution"):
                stored = self.state_store.get(f"{prefix}:{run.result_id}")
                if stored:
                    return extract_metrics_from_result(stored, run.execution_metadata)

        # Fallback to execution metadata
        return extract_metrics_from_result({}, run.execution_metadata)

    def compare_runs(
        self,
        left_run_or_id: Union[AnalysisRun, str],
        right_run_or_id: Union[AnalysisRun, str],
        left_result: Optional[Any] = None,
        right_result: Optional[Any] = None,
        bypass_cache: bool = False,
    ) -> RunComparison:
        """Perform a deterministic comparison between two analytical runs.

        Args:
            left_run_or_id: Baseline run object or run ID.
            right_run_or_id: Candidate run object or run ID.
            left_result: Optional explicit result object/dict for left run.
            right_result: Optional explicit result object/dict for right run.
            bypass_cache: If True, bypasses cache and recomputes comparison.

        Returns:
            RunComparison.
        """
        left_run = self._resolve_run(left_run_or_id)
        right_run = self._resolve_run(right_run_or_id)

        cache_key = (left_run.run_id, right_run.run_id)

        if not bypass_cache and left_result is None and right_result is None:
            with self._lock:
                if cache_key in self._cache:
                    return self._cache[cache_key]

        # 1. Evaluate comparability
        status, reasons, changed_dims = self.evaluate_comparability(left_run, right_run)

        # 2. Compute configuration diff
        config_diffs = diff_configurations(
            left_config=left_run.configuration_summary,
            right_config=right_run.configuration_summary,
        )

        # 3. Compute provenance diff
        prov_diffs = diff_provenance(
            left_run=left_run,
            right_run=right_run,
            provenance_tracker=self.provenance_tracker,
        )

        # 4. Compute quality diff
        qual_diffs = diff_quality(
            left_run=left_run,
            right_run=right_run,
            quality_monitor=self.quality_monitor,
        )

        # 5. Extract and compare metrics
        left_metrics = self._resolve_result_data(left_run, left_result)
        right_metrics = self._resolve_result_data(right_run, right_result)
        metric_diffs = compare_metrics_dicts(left_metrics, right_metrics)

        # 6. Generate comparison ID
        hasher = hashlib.sha256()
        hasher.update(f"{left_run.run_id}:{right_run.run_id}".encode("utf-8"))
        hasher.update(f"{left_run.configuration_fingerprint}:{right_run.configuration_fingerprint}".encode("utf-8"))
        comp_id = f"comp_{hasher.hexdigest()[:16]}"

        now = self.clock()

        # 7. Limitations & Summary
        limitations: List[str] = []
        if status == ComparabilityStatus.NOT_COMPARABLE:
            limitations.append("Runs are fundamentally not comparable; metric differences should not be interpreted.")
        elif status == ComparabilityStatus.PARTIALLY_COMPARABLE:
            limitations.append(
                "Runs differ in experimental setup or data conditions; performance comparisons are contextual only."
            )

        if left_run.status != RunStatus.COMPLETED or right_run.status != RunStatus.COMPLETED:
            limitations.append(
                f"Diagnostic run comparison: non-completed run status detected "
                f"(left: {left_run.status.value}, right: {right_run.status.value}); "
                f"metric availability may be partial or absent."
            )

        if not left_metrics or not right_metrics:
            limitations.append("One or both runs have limited numeric metric data available.")

        summary: Dict[str, Any] = {
            "comparability_status": status.value,
            "changed_dimensions_count": len(changed_dims),
            "configuration_differences_count": len([c for c in config_diffs if c.diff_type.value == "CHANGED"]),
            "metric_differences_count": len(metric_diffs),
            "available_metrics_count": len([m for m in metric_diffs if m.is_available]),
        }

        comparison = RunComparison(
            comparison_id=comp_id,
            generated_at=now,
            left_run_id=left_run.run_id,
            right_run_id=right_run.run_id,
            comparability_status=status,
            comparability_reasons=reasons,
            changed_dimensions=changed_dims,
            metric_differences=tuple(metric_diffs),
            configuration_differences=tuple(config_diffs),
            provenance_differences=tuple(prov_diffs),
            quality_differences=tuple(qual_diffs),
            summary=summary,
            limitations=tuple(limitations),
        )

        # 8. Cache & Telemetry
        with self._lock:
            if cache_key not in self._cache:
                while len(self._cache) >= self.max_cache_size and self._cache_keys:
                    oldest_key = self._cache_keys.popleft()
                    self._cache.pop(oldest_key, None)
            self._cache[cache_key] = comparison
            self._cache_keys.append(cache_key)

        self._publish_event(comparison)
        self._update_state_store(comparison)

        return comparison

    def compare_run_group(
        self,
        runs_or_ids: Sequence[Union[AnalysisRun, str]],
        baseline_run_id: Optional[str] = None,
        max_runs: Optional[int] = None,
    ) -> MultiRunComparison:
        """Compare a bounded group of 2 to 20 analytical runs.

        Args:
            runs_or_ids: Sequence of runs or run IDs.
            baseline_run_id: Optional explicit baseline run ID.
            max_runs: Configurable maximum run bound (defaults to engine max).

        Returns:
            MultiRunComparison.
        """
        effective_max = min(self.max_group_runs, max_runs or self.max_group_runs)

        if not runs_or_ids or len(runs_or_ids) < 2:
            raise ValidationError("compare_run_group requires at least 2 runs.")
        if len(runs_or_ids) > effective_max:
            raise ValidationError(
                f"Requested {len(runs_or_ids)} runs exceeds maximum comparison bound of {effective_max}."
            )

        resolved_runs = [self._resolve_run(r) for r in runs_or_ids]

        # Check baseline
        baseline_run: Optional[AnalysisRun] = None
        if baseline_run_id:
            clean_b_id = baseline_run_id.strip()
            baseline_run = next((r for r in resolved_runs if r.run_id == clean_b_id), None)
            if not baseline_run:
                raise ValidationError(f"Specified baseline_run_id '{clean_b_id}' not found in provided runs.")
        else:
            baseline_run = resolved_runs[0]

        comparisons: List[RunComparison] = []
        aligned_metrics: Dict[str, Dict[str, Optional[float]]] = collections.defaultdict(dict)

        # Populate aligned metrics for all runs
        for r in resolved_runs:
            m_dict = self._resolve_result_data(r)
            for m_name, m_val in m_dict.items():
                aligned_metrics[m_name][r.run_id] = m_val

        # Perform comparisons vs baseline
        for r in resolved_runs:
            if r.run_id == baseline_run.run_id:
                continue
            comp = self.compare_runs(left_run_or_id=baseline_run, right_run_or_id=r)
            comparisons.append(comp)

        hasher = hashlib.sha256()
        hasher.update(":".join(r.run_id for r in resolved_runs).encode("utf-8"))
        group_comp_id = f"mcomp_{hasher.hexdigest()[:16]}"

        status_counts = collections.Counter(c.comparability_status.value for c in comparisons)

        summary = {
            "total_runs": len(resolved_runs),
            "baseline_run_id": baseline_run.run_id,
            "comparisons_count": len(comparisons),
            "status_breakdown": dict(status_counts),
        }

        multi_comp = MultiRunComparison(
            comparison_id=group_comp_id,
            generated_at=self.clock(),
            run_ids=tuple(r.run_id for r in resolved_runs),
            baseline_run_id=baseline_run.run_id,
            comparisons=tuple(comparisons),
            aligned_metrics=aligned_metrics,
            summary=summary,
        )

        return multi_comp

    def get_comparison(self, comparison_id: str) -> Optional[RunComparison]:
        """Look up a cached comparison by comparison_id."""
        if not comparison_id or not isinstance(comparison_id, str):
            return None
        clean_id = comparison_id.strip()
        with self._lock:
            for comp in self._cache.values():
                if comp.comparison_id == clean_id:
                    return comp

        # Check StateStore fallback
        if self.state_store:
            stored = self.state_store.get(f"analysis_comparison:{clean_id}")
            if stored:
                return stored  # Raw dict representation
        return None

    def group_runs_by(
        self,
        runs_or_ids: Sequence[Union[AnalysisRun, str]],
        dimension: ComparisonDimension,
    ) -> Dict[str, List[AnalysisRun]]:
        """Group a collection of runs by a specific comparison dimension."""
        resolved = [self._resolve_run(r) for r in runs_or_ids]
        groups: Dict[str, List[AnalysisRun]] = collections.defaultdict(list)

        for r in resolved:
            if dimension == ComparisonDimension.OPERATION_TYPE:
                groups[r.operation_type.value].append(r)
            elif dimension == ComparisonDimension.STRATEGY:
                groups[r.strategy_id or "NONE"].append(r)
            elif dimension == ComparisonDimension.SYMBOL_UNIVERSE:
                groups[",".join(sorted(r.symbol_universe)) or "NONE"].append(r)
            elif dimension == ComparisonDimension.TIMEFRAME_UNIVERSE:
                groups[",".join(sorted(r.timeframe_universe)) or "NONE"].append(r)
            elif dimension == ComparisonDimension.CONFIGURATION:
                groups[r.configuration_fingerprint].append(r)
            elif dimension == ComparisonDimension.DATA_RANGE:
                groups[f"{r.data_start}:{r.data_end}"].append(r)
            else:
                groups[r.reproducibility_id].append(r)

        return {k: groups[k] for k in sorted(groups.keys())}

    def _publish_event(self, comparison: RunComparison) -> None:
        """Publish ANALYSIS_RUNS_COMPARED lifecycle event."""
        if not self.event_bus:
            return
        try:
            payload = {
                "comparison_id": comparison.comparison_id,
                "left_run_id": comparison.left_run_id,
                "right_run_id": comparison.right_run_id,
                "comparability_status": comparison.comparability_status.value,
                "changed_dimensions": [d.value for d in comparison.changed_dimensions],
                "summary": comparison.summary,
            }
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.ANALYSIS_RUNS_COMPARED,
                    source="run_comparison_engine",
                    payload=payload,
                )
            )
        except Exception as exc:
            logger.warning("Failed to publish ANALYSIS_RUNS_COMPARED event: %s", exc)

    def _update_state_store(self, comparison: RunComparison) -> None:
        """Update StateStore with bounded comparison snapshots."""
        if not self.state_store:
            return
        try:
            c_dict = comparison.to_dict()
            self.state_store.set(f"analysis_comparison:{comparison.comparison_id}", c_dict)
            self.state_store.set("analysis_comparisons:latest", c_dict)

            # Append to bounded history
            history = self.state_store.get("analysis_comparisons:history", [])
            updated_hist = [c_dict] + [h for h in history if h.get("comparison_id") != comparison.comparison_id]
            self.state_store.set("analysis_comparisons:history", updated_hist[:50])
        except Exception as exc:
            logger.warning("Failed to update StateStore with comparison snapshot: %s", exc)
