"""KAISER — Reproducible Analysis Run Registry & Experiment Tracking Engine.

Provides a thread-safe, bounded in-memory registry for tracking analytical run
lifecycles, deterministic reproducibility lookups, EventBus telemetry publishing,
StateStore snapshot synchronization, and optional SQLite persistence.
"""

from __future__ import annotations

import collections
import copy
import json
import logging
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from app.analysis.models import normalize_timeframe
from app.analysis.runs.models import (
    AnalysisRun,
    OperationType,
    RunStatus,
    generate_configuration_fingerprint,
    generate_reproducibility_id,
    validate_lifecycle_transition,
)
from app.core.config import resolve_database_path
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.market.provenance.models import sanitize_provenance_metadata
from app.market.quality.status import DataQualityStatus

logger = logging.getLogger("kaiser.analysis.runs.registry")

DEFAULT_MAX_RUN_HISTORY = 1000


class AnalysisRunRegistry:
    """Thread-safe, bounded analytical run registry and experiment tracking engine."""

    def __init__(
        self,
        max_history: int = DEFAULT_MAX_RUN_HISTORY,
        clock: Optional[Callable[[], float]] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        db_path: Optional[Union[str, Path]] = None,
        name: str = "analysis_run_registry",
    ) -> None:
        """Initialize the analysis run registry.

        Args:
            max_history: Maximum runs retained in memory FIFO history.
            clock: Injected POSIX clock for deterministic time simulation in tests.
            event_bus: Optional EventBus for emitting run lifecycle events.
            state_store: Optional StateStore for persisting bounded run state summaries.
            db_path: Optional SQLite database path for persistent audit retention.
            name: Component identifier.
        """
        self.name = name
        self.max_history = max(1, min(100000, max_history))
        self.clock = clock or time.time
        self.event_bus = event_bus
        self.state_store = state_store
        self.db_path = resolve_database_path(str(db_path)) if db_path else None
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()

        # Primary storage: bounded FIFO history
        self._history: collections.deque[AnalysisRun] = collections.deque(maxlen=self.max_history)

        # High-performance lookup indexes
        self._by_id: Dict[str, AnalysisRun] = {}
        self._by_reproducibility: Dict[str, List[str]] = collections.defaultdict(list)
        self._by_operation: Dict[OperationType, List[str]] = collections.defaultdict(list)
        self._by_status: Dict[RunStatus, List[str]] = collections.defaultdict(list)
        self._by_symbol: Dict[str, List[str]] = collections.defaultdict(list)
        self._by_strategy: Dict[str, List[str]] = collections.defaultdict(list)
        self._children_by_parent: Dict[str, List[str]] = collections.defaultdict(list)

        # Telemetry counters
        self._total_runs_created = 0
        self._total_runs_completed = 0
        self._total_runs_failed = 0
        self._total_runs_cancelled = 0

        if self.db_path:
            self._init_sqlite(self.db_path)

    def _init_sqlite(self, db_path: str) -> None:
        """Initialize minimal, migration-safe SQLite persistence for analysis runs."""
        try:
            if db_path != ":memory:":
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS analysis_runs (
                        run_id TEXT PRIMARY KEY,
                        reproducibility_id TEXT NOT NULL,
                        operation_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        started_at REAL,
                        completed_at REAL,
                        configuration_fingerprint TEXT NOT NULL,
                        configuration_json TEXT,
                        strategy_id TEXT,
                        strategy_version TEXT,
                        analysis_version TEXT NOT NULL,
                        schema_version TEXT NOT NULL,
                        software_version TEXT NOT NULL,
                        symbols_json TEXT,
                        timeframes_json TEXT,
                        data_start REAL,
                        data_end REAL,
                        input_record_count INTEGER NOT NULL DEFAULT 0,
                        input_provenance_json TEXT,
                        parent_runs_json TEXT,
                        quality_assessments_json TEXT,
                        quality_status TEXT,
                        result_id TEXT,
                        result_fingerprint TEXT,
                        result_type TEXT,
                        execution_metadata_json TEXT,
                        error_category TEXT,
                        error_message TEXT,
                        cancellation_reason TEXT,
                        persisted_at REAL NOT NULL
                    );
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_analysis_runs_repro
                    ON analysis_runs (reproducibility_id);
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_analysis_runs_op
                    ON analysis_runs (operation_type);
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_analysis_runs_status
                    ON analysis_runs (status);
                """)
            self._conn = conn
        except Exception as exc:
            logger.warning("Failed to initialize SQLite analysis run database at '%s': %s", db_path, exc)
            self._conn = None

    def close(self) -> None:
        """Close SQLite connection cleanly if opened."""
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    # =========================================================================
    # LIFECYCLE MANAGEMENT
    # =========================================================================

    def create_run(
        self,
        operation_type: Union[OperationType, str],
        configuration: Any,
        run_id: Optional[str] = None,
        input_provenance_ids: Sequence[str] = (),
        parent_run_ids: Sequence[str] = (),
        strategy_id: Optional[str] = None,
        strategy_version: Optional[str] = None,
        symbol_universe: Sequence[str] = (),
        timeframe_universe: Sequence[str] = (),
        data_start: Optional[float] = None,
        data_end: Optional[float] = None,
        input_record_count: int = 0,
        quality_assessment_ids: Sequence[str] = (),
        quality_status: Optional[DataQualityStatus] = None,
        analysis_version: str = "1.0.0",
        schema_version: str = "1.0.0",
        software_version: str = "1.0.0",
        execution_metadata: Optional[Dict[str, Any]] = None,
    ) -> AnalysisRun:
        """Register a new analytical execution in CREATED status."""
        op_enum = OperationType.from_string(operation_type)
        r_id = run_id.strip() if run_id and isinstance(run_id, str) else f"run_{uuid.uuid4().hex[:12]}"
        now = self.clock()

        cfg_fp = generate_configuration_fingerprint(configuration)
        cfg_summary = (
            configuration.to_dict()
            if hasattr(configuration, "to_dict") and callable(getattr(configuration, "to_dict"))
            else (dict(configuration) if isinstance(configuration, Mapping) else {"config": str(configuration)})
        )

        repro_id = generate_reproducibility_id(
            operation_type=op_enum,
            input_provenance_ids=input_provenance_ids,
            configuration_fingerprint=cfg_fp,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            analysis_version=analysis_version,
            software_version=software_version,
            schema_version=schema_version,
        )

        run = AnalysisRun(
            run_id=r_id,
            reproducibility_id=repro_id,
            operation_type=op_enum,
            status=RunStatus.CREATED,
            created_at=now,
            configuration_fingerprint=cfg_fp,
            configuration_summary=cfg_summary,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            analysis_version=analysis_version,
            schema_version=schema_version,
            software_version=software_version,
            symbol_universe=tuple(symbol_universe),
            timeframe_universe=tuple(timeframe_universe),
            data_start=data_start,
            data_end=data_end,
            input_record_count=input_record_count,
            input_provenance_ids=tuple(input_provenance_ids),
            parent_run_ids=tuple(parent_run_ids),
            quality_assessment_ids=tuple(quality_assessment_ids),
            quality_status=quality_status,
            execution_metadata=dict(execution_metadata or {}),
        )

        with self._lock:
            if r_id in self._by_id:
                raise ValidationError(f"AnalysisRun with run_id '{r_id}' already exists in registry.")

            self._record_run_in_memory(run)
            self._total_runs_created += 1
            self._persist_to_sqlite(run)

        self._publish_event(EventType.ANALYSIS_RUN_CREATED, run)
        self._update_state_store(run)
        return run

    def register_run(self, run: AnalysisRun) -> AnalysisRun:
        """Register a pre-constructed AnalysisRun instance in the registry."""
        if not isinstance(run, AnalysisRun):
            raise ValidationError(f"Expected AnalysisRun instance, got {type(run)}")

        with self._lock:
            if run.run_id in self._by_id:
                raise ValidationError(f"AnalysisRun with run_id '{run.run_id}' already exists in registry.")

            self._record_run_in_memory(run)
            self._total_runs_created += 1
            self._persist_to_sqlite(run)

        self._publish_event(EventType.ANALYSIS_RUN_CREATED, run)
        self._update_state_store(run)
        return run

    def start_run(
        self,
        run_id: str,
        started_at: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AnalysisRun:
        """Transition an existing run from CREATED to RUNNING."""
        if not run_id or not isinstance(run_id, str):
            raise ValidationError("run_id must be a non-empty string.")

        with self._lock:
            existing = self._get_run_internal(run_id.strip())
            if not existing:
                raise ValidationError(f"AnalysisRun '{run_id}' not found in registry.")

            validate_lifecycle_transition(existing.status, RunStatus.RUNNING)
            start_ts = started_at if started_at is not None else self.clock()

            merged_metadata = dict(existing.execution_metadata)
            if metadata:
                merged_metadata.update(metadata)

            updated = AnalysisRun(
                run_id=existing.run_id,
                reproducibility_id=existing.reproducibility_id,
                operation_type=existing.operation_type,
                status=RunStatus.RUNNING,
                created_at=existing.created_at,
                configuration_fingerprint=existing.configuration_fingerprint,
                started_at=start_ts,
                completed_at=None,
                input_provenance_ids=existing.input_provenance_ids,
                parent_run_ids=existing.parent_run_ids,
                configuration_summary=existing.configuration_summary,
                strategy_id=existing.strategy_id,
                strategy_version=existing.strategy_version,
                analysis_version=existing.analysis_version,
                schema_version=existing.schema_version,
                software_version=existing.software_version,
                symbol_universe=existing.symbol_universe,
                timeframe_universe=existing.timeframe_universe,
                data_start=existing.data_start,
                data_end=existing.data_end,
                input_record_count=existing.input_record_count,
                quality_assessment_ids=existing.quality_assessment_ids,
                quality_status=existing.quality_status,
                result_id=existing.result_id,
                result_fingerprint=existing.result_fingerprint,
                result_type=existing.result_type,
                execution_metadata=merged_metadata,
            )

            self._update_run_in_memory(existing, updated)
            self._persist_to_sqlite(updated)

        self._publish_event(EventType.ANALYSIS_RUN_STARTED, updated)
        self._update_state_store(updated)
        return updated

    def complete_run(
        self,
        run_id: str,
        result_id: Optional[str] = None,
        result_fingerprint: Optional[str] = None,
        result_type: Optional[str] = None,
        completed_at: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AnalysisRun:
        """Transition a RUNNING run to COMPLETED with linked result identifiers."""
        if not run_id or not isinstance(run_id, str):
            raise ValidationError("run_id must be a non-empty string.")

        with self._lock:
            existing = self._get_run_internal(run_id.strip())
            if not existing:
                raise ValidationError(f"AnalysisRun '{run_id}' not found in registry.")

            validate_lifecycle_transition(existing.status, RunStatus.COMPLETED)
            comp_ts = completed_at if completed_at is not None else self.clock()

            merged_metadata = dict(existing.execution_metadata)
            if metadata:
                merged_metadata.update(metadata)

            updated = AnalysisRun(
                run_id=existing.run_id,
                reproducibility_id=existing.reproducibility_id,
                operation_type=existing.operation_type,
                status=RunStatus.COMPLETED,
                created_at=existing.created_at,
                configuration_fingerprint=existing.configuration_fingerprint,
                started_at=existing.started_at or existing.created_at,
                completed_at=comp_ts,
                input_provenance_ids=existing.input_provenance_ids,
                parent_run_ids=existing.parent_run_ids,
                configuration_summary=existing.configuration_summary,
                strategy_id=existing.strategy_id,
                strategy_version=existing.strategy_version,
                analysis_version=existing.analysis_version,
                schema_version=existing.schema_version,
                software_version=existing.software_version,
                symbol_universe=existing.symbol_universe,
                timeframe_universe=existing.timeframe_universe,
                data_start=existing.data_start,
                data_end=existing.data_end,
                input_record_count=existing.input_record_count,
                quality_assessment_ids=existing.quality_assessment_ids,
                quality_status=existing.quality_status,
                result_id=result_id or existing.result_id,
                result_fingerprint=result_fingerprint or existing.result_fingerprint,
                result_type=result_type or existing.result_type,
                execution_metadata=merged_metadata,
            )

            self._update_run_in_memory(existing, updated)
            self._total_runs_completed += 1
            self._persist_to_sqlite(updated)

        self._publish_event(EventType.ANALYSIS_RUN_COMPLETED, updated)
        self._update_state_store(updated)
        return updated

    def fail_run(
        self,
        run_id: str,
        error_category: Optional[str] = None,
        error_message: Optional[str] = None,
        completed_at: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AnalysisRun:
        """Transition a run to FAILED state with diagnostic error context."""
        if not run_id or not isinstance(run_id, str):
            raise ValidationError("run_id must be a non-empty string.")

        with self._lock:
            existing = self._get_run_internal(run_id.strip())
            if not existing:
                raise ValidationError(f"AnalysisRun '{run_id}' not found in registry.")

            validate_lifecycle_transition(existing.status, RunStatus.FAILED)
            comp_ts = completed_at if completed_at is not None else self.clock()

            merged_metadata = dict(existing.execution_metadata)
            if metadata:
                merged_metadata.update(metadata)

            updated = AnalysisRun(
                run_id=existing.run_id,
                reproducibility_id=existing.reproducibility_id,
                operation_type=existing.operation_type,
                status=RunStatus.FAILED,
                created_at=existing.created_at,
                configuration_fingerprint=existing.configuration_fingerprint,
                started_at=existing.started_at or existing.created_at,
                completed_at=comp_ts,
                input_provenance_ids=existing.input_provenance_ids,
                parent_run_ids=existing.parent_run_ids,
                configuration_summary=existing.configuration_summary,
                strategy_id=existing.strategy_id,
                strategy_version=existing.strategy_version,
                analysis_version=existing.analysis_version,
                schema_version=existing.schema_version,
                software_version=existing.software_version,
                symbol_universe=existing.symbol_universe,
                timeframe_universe=existing.timeframe_universe,
                data_start=existing.data_start,
                data_end=existing.data_end,
                input_record_count=existing.input_record_count,
                quality_assessment_ids=existing.quality_assessment_ids,
                quality_status=existing.quality_status,
                result_id=existing.result_id,
                result_fingerprint=existing.result_fingerprint,
                result_type=existing.result_type,
                execution_metadata=merged_metadata,
                error_category=error_category or "EXECUTION_ERROR",
                error_message=str(error_message)[:1000] if error_message else "Unspecified analytical failure.",
            )

            self._update_run_in_memory(existing, updated)
            self._total_runs_failed += 1
            self._persist_to_sqlite(updated)

        self._publish_event(EventType.ANALYSIS_RUN_FAILED, updated)
        self._update_state_store(updated)
        return updated

    def cancel_run(
        self,
        run_id: str,
        cancellation_reason: Optional[str] = None,
        completed_at: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AnalysisRun:
        """Transition a run to CANCELLED state distinguishing cancellation from failure."""
        if not run_id or not isinstance(run_id, str):
            raise ValidationError("run_id must be a non-empty string.")

        with self._lock:
            existing = self._get_run_internal(run_id.strip())
            if not existing:
                raise ValidationError(f"AnalysisRun '{run_id}' not found in registry.")

            validate_lifecycle_transition(existing.status, RunStatus.CANCELLED)
            comp_ts = completed_at if completed_at is not None else self.clock()

            merged_metadata = dict(existing.execution_metadata)
            if metadata:
                merged_metadata.update(metadata)

            updated = AnalysisRun(
                run_id=existing.run_id,
                reproducibility_id=existing.reproducibility_id,
                operation_type=existing.operation_type,
                status=RunStatus.CANCELLED,
                created_at=existing.created_at,
                configuration_fingerprint=existing.configuration_fingerprint,
                started_at=existing.started_at or existing.created_at,
                completed_at=comp_ts,
                input_provenance_ids=existing.input_provenance_ids,
                parent_run_ids=existing.parent_run_ids,
                configuration_summary=existing.configuration_summary,
                strategy_id=existing.strategy_id,
                strategy_version=existing.strategy_version,
                analysis_version=existing.analysis_version,
                schema_version=existing.schema_version,
                software_version=existing.software_version,
                symbol_universe=existing.symbol_universe,
                timeframe_universe=existing.timeframe_universe,
                data_start=existing.data_start,
                data_end=existing.data_end,
                input_record_count=existing.input_record_count,
                quality_assessment_ids=existing.quality_assessment_ids,
                quality_status=existing.quality_status,
                result_id=existing.result_id,
                result_fingerprint=existing.result_fingerprint,
                result_type=existing.result_type,
                execution_metadata=merged_metadata,
                cancellation_reason=cancellation_reason or "Cancellation requested by operator or token.",
            )

            self._update_run_in_memory(existing, updated)
            self._total_runs_cancelled += 1
            self._persist_to_sqlite(updated)

        self._publish_event(EventType.ANALYSIS_RUN_CANCELLED, updated)
        self._update_state_store(updated)
        return updated

    def invalidate_run(
        self,
        run_id: str,
        reason: Optional[str] = None,
        completed_at: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AnalysisRun:
        """Mark a CREATED run as INVALID before execution starts."""
        if not run_id or not isinstance(run_id, str):
            raise ValidationError("run_id must be a non-empty string.")

        with self._lock:
            existing = self._get_run_internal(run_id.strip())
            if not existing:
                raise ValidationError(f"AnalysisRun '{run_id}' not found in registry.")

            validate_lifecycle_transition(existing.status, RunStatus.INVALID)
            comp_ts = completed_at if completed_at is not None else self.clock()

            merged_metadata = dict(existing.execution_metadata)
            if metadata:
                merged_metadata.update(metadata)

            updated = AnalysisRun(
                run_id=existing.run_id,
                reproducibility_id=existing.reproducibility_id,
                operation_type=existing.operation_type,
                status=RunStatus.INVALID,
                created_at=existing.created_at,
                configuration_fingerprint=existing.configuration_fingerprint,
                started_at=None,
                completed_at=comp_ts,
                input_provenance_ids=existing.input_provenance_ids,
                parent_run_ids=existing.parent_run_ids,
                configuration_summary=existing.configuration_summary,
                strategy_id=existing.strategy_id,
                strategy_version=existing.strategy_version,
                analysis_version=existing.analysis_version,
                schema_version=existing.schema_version,
                software_version=existing.software_version,
                symbol_universe=existing.symbol_universe,
                timeframe_universe=existing.timeframe_universe,
                data_start=existing.data_start,
                data_end=existing.data_end,
                input_record_count=existing.input_record_count,
                quality_assessment_ids=existing.quality_assessment_ids,
                quality_status=existing.quality_status,
                result_id=existing.result_id,
                result_fingerprint=existing.result_fingerprint,
                result_type=existing.result_type,
                execution_metadata=merged_metadata,
                error_category="INVALID_CONFIGURATION",
                error_message=reason or "Run invalidated due to configuration or precondition error.",
            )

            self._update_run_in_memory(existing, updated)
            self._persist_to_sqlite(updated)

        self._publish_event(EventType.ANALYSIS_RUN_FAILED, updated)
        self._update_state_store(updated)
        return updated

    # =========================================================================
    # INTERNAL IN-MEMORY INDEX MANAGEMENT & EVICTION
    # =========================================================================

    def _record_run_in_memory(self, run: AnalysisRun) -> None:
        """Insert new run into FIFO deque and auxiliary indexes, enforcing capacity."""
        if len(self._history) >= self.max_history:
            oldest = self._history[0]
            self._evict_from_indexes(oldest)

        self._history.append(run)
        self._by_id[run.run_id] = run
        self._by_reproducibility[run.reproducibility_id].append(run.run_id)
        self._by_operation[run.operation_type].append(run.run_id)
        self._by_status[run.status].append(run.run_id)

        for sym in run.symbol_universe:
            if run.run_id not in self._by_symbol[sym]:
                self._by_symbol[sym].append(run.run_id)

        if run.strategy_id:
            strat_key = run.strategy_id.lower()
            if run.run_id not in self._by_strategy[strat_key]:
                self._by_strategy[strat_key].append(run.run_id)

        for parent_id in run.parent_run_ids:
            if run.run_id not in self._children_by_parent[parent_id]:
                self._children_by_parent[parent_id].append(run.run_id)

    def _update_run_in_memory(self, old_run: AnalysisRun, new_run: AnalysisRun) -> None:
        """Update a run in place preserving deque order and updating status index."""
        self._by_id[new_run.run_id] = new_run

        # Update in deque
        for idx, item in enumerate(self._history):
            if item.run_id == new_run.run_id:
                self._history[idx] = new_run
                break

        # Move from old status index to new status index
        if old_run.status != new_run.status:
            old_list = self._by_status.get(old_run.status)
            if old_list and new_run.run_id in old_list:
                old_list.remove(new_run.run_id)
            if new_run.run_id not in self._by_status[new_run.status]:
                self._by_status[new_run.status].append(new_run.run_id)

    def _evict_from_indexes(self, evicted: AnalysisRun) -> None:
        """Safely prune evicted run from secondary lookup lists."""
        eid = evicted.run_id
        self._by_id.pop(eid, None)

        repro_list = self._by_reproducibility.get(evicted.reproducibility_id)
        if repro_list and eid in repro_list:
            repro_list.remove(eid)

        op_list = self._by_operation.get(evicted.operation_type)
        if op_list and eid in op_list:
            op_list.remove(eid)

        st_list = self._by_status.get(evicted.status)
        if st_list and eid in st_list:
            st_list.remove(eid)

        for sym in evicted.symbol_universe:
            sym_list = self._by_symbol.get(sym)
            if sym_list and eid in sym_list:
                sym_list.remove(eid)

        if evicted.strategy_id:
            strat_list = self._by_strategy.get(evicted.strategy_id.lower())
            if strat_list and eid in strat_list:
                strat_list.remove(eid)

        for parent_id in evicted.parent_run_ids:
            p_list = self._children_by_parent.get(parent_id)
            if p_list and eid in p_list:
                p_list.remove(eid)

    # =========================================================================
    # QUERY METHODS
    # =========================================================================

    def get_run(self, run_id: str) -> Optional[AnalysisRun]:
        """Look up a run by unique run_id with fallback to SQLite."""
        if not run_id or not isinstance(run_id, str):
            return None
        with self._lock:
            return self._get_run_internal(run_id.strip())

    def _get_run_internal(self, run_id: str) -> Optional[AnalysisRun]:
        """Thread-internal run resolution helper."""
        cached = self._by_id.get(run_id)
        if cached is not None:
            return cached

        if self._conn is not None:
            try:
                cursor = self._conn.execute(
                    "SELECT * FROM analysis_runs WHERE run_id = ?",
                    (run_id,),
                )
                row = cursor.fetchone()
                if row:
                    return self._hydrate_run_from_sqlite(row)
            except Exception as exc:
                logger.warning("Failed to read analysis run from SQLite: %s", exc)
        return None

    def get_runs_by_reproducibility(
        self,
        reproducibility_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AnalysisRun]:
        """Find all executions matching a specific analytical setup fingerprint."""
        if not reproducibility_id or not isinstance(reproducibility_id, str):
            return []

        rid = reproducibility_id.strip()
        clamped_limit = max(1, min(500, limit))
        clamped_offset = max(0, offset)

        with self._lock:
            run_ids = self._by_reproducibility.get(rid, [])
            # Return newest first
            matched = [self._by_id[r] for r in reversed(run_ids) if r in self._by_id]
            return matched[clamped_offset : clamped_offset + clamped_limit]

    def query_runs(
        self,
        operation_type: Optional[Union[OperationType, str]] = None,
        status: Optional[Union[RunStatus, str]] = None,
        symbol: Optional[str] = None,
        strategy_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AnalysisRun]:
        """Query bounded runs with flexible filtering and pagination."""
        clamped_limit = max(1, min(500, limit))
        clamped_offset = max(0, offset)

        op_filter = (
            operation_type
            if isinstance(operation_type, OperationType)
            else OperationType.from_string(str(operation_type))
            if operation_type
            else None
        )
        st_filter = (
            status
            if isinstance(status, RunStatus)
            else RunStatus(str(status).split(".")[-1].strip().upper())
            if status
            else None
        )
        sym_filter = symbol.strip().upper() if symbol else None
        strat_filter = strategy_id.strip().lower() if strategy_id else None
        p_filter = parent_run_id.strip() if parent_run_id else None

        with self._lock:
            # Query newest first from deque
            all_runs = list(self._history)
            all_runs.reverse()

            filtered: List[AnalysisRun] = []
            for r in all_runs:
                if op_filter and r.operation_type != op_filter:
                    continue
                if st_filter and r.status != st_filter:
                    continue
                if sym_filter and sym_filter not in r.symbol_universe:
                    continue
                if strat_filter and (not r.strategy_id or r.strategy_id.lower() != strat_filter):
                    continue
                if p_filter and p_filter not in r.parent_run_ids:
                    continue
                if start_time is not None and r.created_at < start_time:
                    continue
                if end_time is not None and r.created_at > end_time:
                    continue
                filtered.append(r)

            return filtered[clamped_offset : clamped_offset + clamped_limit]

    def get_latest_runs(self, limit: int = 10) -> List[AnalysisRun]:
        """Retrieve the latest registered runs in descending chronological order."""
        clamped_limit = max(1, min(self.max_history, limit))
        with self._lock:
            runs = list(self._history)
            runs.reverse()
            return runs[:clamped_limit]

    def get_summary(self) -> Dict[str, Any]:
        """Return aggregated summary metrics for audit, health, and observability reporting."""
        with self._lock:
            by_op: Dict[str, int] = collections.defaultdict(int)
            by_st: Dict[str, int] = collections.defaultdict(int)

            for r in self._history:
                by_op[r.operation_type.value] += 1
                by_st[r.status.value] += 1

            return {
                "total_runs_created": self._total_runs_created,
                "total_runs_completed": self._total_runs_completed,
                "total_runs_failed": self._total_runs_failed,
                "total_runs_cancelled": self._total_runs_cancelled,
                "current_retained_runs": len(self._history),
                "max_history_capacity": self.max_history,
                "distinct_reproducibility_identities": len(self._by_reproducibility),
                "by_operation_type": dict(by_op),
                "by_status": dict(by_st),
            }

    def reset(self) -> None:
        """Reset internal history and lookup indexes (primarily for testing isolation)."""
        with self._lock:
            self._history.clear()
            self._by_id.clear()
            self._by_reproducibility.clear()
            self._by_operation.clear()
            self._by_status.clear()
            self._by_symbol.clear()
            self._by_strategy.clear()
            self._children_by_parent.clear()
            self._total_runs_created = 0
            self._total_runs_completed = 0
            self._total_runs_failed = 0
            self._total_runs_cancelled = 0

    # =========================================================================
    # SQLITE PERSISTENCE HELPERS
    # =========================================================================

    def _persist_to_sqlite(self, run: AnalysisRun) -> None:
        """Persist or update an AnalysisRun in the optional SQLite audit database."""
        if self._conn is None:
            return
        try:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO analysis_runs (
                        run_id, reproducibility_id, operation_type, status,
                        created_at, started_at, completed_at, configuration_fingerprint,
                        configuration_json, strategy_id, strategy_version,
                        analysis_version, schema_version, software_version,
                        symbols_json, timeframes_json, data_start, data_end,
                        input_record_count, input_provenance_json, parent_runs_json,
                        quality_assessments_json, quality_status, result_id,
                        result_fingerprint, result_type, execution_metadata_json,
                        error_category, error_message, cancellation_reason, persisted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run.run_id,
                        run.reproducibility_id,
                        run.operation_type.value,
                        run.status.value,
                        run.created_at,
                        run.started_at,
                        run.completed_at,
                        run.configuration_fingerprint,
                        json.dumps(run.configuration_summary),
                        run.strategy_id,
                        run.strategy_version,
                        run.analysis_version,
                        run.schema_version,
                        run.software_version,
                        json.dumps(list(run.symbol_universe)),
                        json.dumps(list(run.timeframe_universe)),
                        run.data_start,
                        run.data_end,
                        run.input_record_count,
                        json.dumps(list(run.input_provenance_ids)),
                        json.dumps(list(run.parent_run_ids)),
                        json.dumps(list(run.quality_assessment_ids)),
                        run.quality_status.value if run.quality_status else None,
                        run.result_id,
                        run.result_fingerprint,
                        run.result_type,
                        json.dumps(run.execution_metadata),
                        run.error_category,
                        run.error_message,
                        run.cancellation_reason,
                        time.time(),
                    ),
                )
        except Exception as exc:
            logger.warning("Failed to persist analysis run '%s' to SQLite: %s", run.run_id, exc)

    def _hydrate_run_from_sqlite(self, row: sqlite3.Row) -> AnalysisRun:
        """Construct an immutable AnalysisRun instance from an SQLite record row."""
        q_status = DataQualityStatus(row["quality_status"]) if row["quality_status"] else None
        cfg_summary = json.loads(row["configuration_json"]) if row["configuration_json"] else {}
        symbols = tuple(json.loads(row["symbols_json"])) if row["symbols_json"] else ()
        timeframes = tuple(json.loads(row["timeframes_json"])) if row["timeframes_json"] else ()
        inputs = tuple(json.loads(row["input_provenance_json"])) if row["input_provenance_json"] else ()
        parents = tuple(json.loads(row["parent_runs_json"])) if row["parent_runs_json"] else ()
        qual_ids = tuple(json.loads(row["quality_assessments_json"])) if row["quality_assessments_json"] else ()
        exec_meta = json.loads(row["execution_metadata_json"]) if row["execution_metadata_json"] else {}

        return AnalysisRun(
            run_id=row["run_id"],
            reproducibility_id=row["reproducibility_id"],
            operation_type=OperationType.from_string(row["operation_type"]),
            status=RunStatus(row["status"]),
            created_at=row["created_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            configuration_fingerprint=row["configuration_fingerprint"],
            configuration_summary=cfg_summary,
            strategy_id=row["strategy_id"],
            strategy_version=row["strategy_version"],
            analysis_version=row["analysis_version"],
            schema_version=row["schema_version"],
            software_version=row["software_version"],
            symbol_universe=symbols,
            timeframe_universe=timeframes,
            data_start=row["data_start"],
            data_end=row["data_end"],
            input_record_count=row["input_record_count"],
            input_provenance_ids=inputs,
            parent_run_ids=parents,
            quality_assessment_ids=qual_ids,
            quality_status=q_status,
            result_id=row["result_id"],
            result_fingerprint=row["result_fingerprint"],
            result_type=row["result_type"],
            execution_metadata=exec_meta,
            error_category=row["error_category"],
            error_message=row["error_message"],
            cancellation_reason=row["cancellation_reason"],
        )

    # =========================================================================
    # TELEMETRY & OBSERVABILITY INTEGRATION
    # =========================================================================

    def _publish_event(self, event_type: EventType, run: AnalysisRun) -> None:
        """Publish structured, bounded audit event to EventBus."""
        if not self.event_bus:
            return
        try:
            payload = {
                "run_id": run.run_id,
                "reproducibility_id": run.reproducibility_id,
                "operation_type": run.operation_type.value,
                "status": run.status.value,
                "created_at": run.created_at,
                "started_at": run.started_at,
                "completed_at": run.completed_at,
                "duration_seconds": run.duration_seconds,
                "strategy_id": run.strategy_id,
                "strategy_version": run.strategy_version,
                "symbol_universe": list(run.symbol_universe),
                "timeframe_universe": list(run.timeframe_universe),
                "result_id": run.result_id,
                "result_type": run.result_type,
                "error_category": run.error_category,
                "cancellation_reason": run.cancellation_reason,
            }
            self.event_bus.publish(
                Event.create(
                    event_type=event_type,
                    source=self.name,
                    payload=payload,
                )
            )
        except Exception as exc:
            logger.warning("Failed to publish event '%s' for run '%s': %s", event_type.value, run.run_id, exc)

    def _update_state_store(self, run: AnalysisRun) -> None:
        """Synchronize latest run summary snapshot to StateStore."""
        if not self.state_store:
            return
        try:
            summary = self.get_summary()
            self.state_store.set("analysis_runs:summary", summary)
            self.state_store.set(f"analysis_run:{run.run_id}", run.to_dict())
            latest_list = [r.to_dict() for r in self.get_latest_runs(20)]
            self.state_store.set("analysis_runs:latest", latest_list)
            self.state_store.set("analysis_runs.latest", latest_list)
            latest_key = f"analysis_runs:latest:{run.operation_type.value}"
            self.state_store.set(latest_key, run.to_dict())
        except Exception as exc:
            logger.warning("Failed to update StateStore with analysis run summary: %s", exc)
