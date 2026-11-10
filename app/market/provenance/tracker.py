"""KAISER — Market Data Provenance Tracker & Lineage Engine.

Provides a thread-safe, bounded, zero-secret in-memory audit history and lineage
traversal engine with EventBus and StateStore integration.
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
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union
import uuid

from app.analysis.models import normalize_timeframe
from app.core.config import resolve_database_path
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.provenance.models import (
    DataSourceType,
    LineageGraph,
    LineageNode,
    MarketDataProvenance,
    generate_deterministic_provenance_id,
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)
from app.market.quality.status import DataQualityAssessment, DataQualityStatus

logger = logging.getLogger("kaiser.market.provenance.tracker")

DEFAULT_MAX_PROVENANCE_HISTORY = 1000
DEFAULT_MAX_LINEAGE_DEPTH = 10


class MarketDataProvenanceTracker:
    """Thread-safe, bounded market data provenance tracking and lineage analysis engine."""

    def __init__(
        self,
        max_history: int = DEFAULT_MAX_PROVENANCE_HISTORY,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        db_path: Optional[Union[str, Path]] = None,
        name: str = "market_data_provenance_tracker",
    ) -> None:
        self.name = name
        self.max_history = max(1, min(100000, max_history))
        self.event_bus = event_bus
        self.state_store = state_store
        self.db_path = resolve_database_path(str(db_path)) if db_path else None
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()

        # Primary storage: FIFO ordered history with bounded size
        self._history: collections.deque[MarketDataProvenance] = collections.deque(maxlen=self.max_history)

        # Indexing for O(1) lookups
        self._by_id: Dict[str, MarketDataProvenance] = {}
        self._by_operation: Dict[str, List[str]] = collections.defaultdict(list)
        self._latest_by_stream: Dict[Tuple[str, str], str] = {}  # (symbol, tf) -> provenance_id
        self._children_index: Dict[str, List[str]] = collections.defaultdict(list)  # parent_id -> list of child_ids

        # Aggregated counters
        self._total_records_tracked: int = 0
        self._total_lineage_links: int = 0

        if self.db_path:
            self._init_sqlite(self.db_path)

    def _init_sqlite(self, db_path: str) -> None:
        """Initialize minimal, migration-safe SQLite audit persistence."""
        try:
            if db_path != ":memory:":
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10.0)
            conn.row_factory = sqlite3.Row
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS market_data_provenance (
                        provenance_id TEXT PRIMARY KEY,
                        operation_id TEXT NOT NULL,
                        source_type TEXT NOT NULL,
                        source_identifier TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        data_start REAL,
                        data_end REAL,
                        record_count INTEGER NOT NULL DEFAULT 0,
                        ingestion_timestamp REAL NOT NULL,
                        processing_timestamp REAL NOT NULL,
                        quality_assessment_id TEXT,
                        quality_status TEXT,
                        quality_score REAL,
                        schema_version TEXT NOT NULL DEFAULT '1.0.0',
                        transformation_name TEXT,
                        transformation_version TEXT,
                        input_record_count INTEGER,
                        output_record_count INTEGER,
                        parent_provenance_ids TEXT,
                        replay_id TEXT,
                        metadata_json TEXT,
                        created_at REAL NOT NULL
                    );
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_market_prov_stream
                    ON market_data_provenance (symbol, timeframe);
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_market_prov_op
                    ON market_data_provenance (operation_id);
                """)
            self._conn = conn
        except Exception as exc:
            logger.warning(f"Failed to initialize SQLite provenance database at '{db_path}': {exc}")
            self._conn = None

    def record_provenance(self, provenance: MarketDataProvenance) -> MarketDataProvenance:
        """Record an immutable MarketDataProvenance instance into the audit trail.

        Enforces bounded capacity, updates lookup indexes, emits EventBus telemetry,
        and synchronizes with StateStore.
        """
        if not isinstance(provenance, MarketDataProvenance):
            self._publish_failure_event(
                operation_id="unknown",
                reason=f"Expected MarketDataProvenance instance, got {type(provenance).__name__}",
            )
            raise ValidationError(f"Expected MarketDataProvenance instance, got {type(provenance).__name__}")

        with self._lock:
            # Handle eviction from indexes if history is at maximum capacity
            if len(self._history) >= self.max_history:
                oldest = self._history[0]
                self._evict_from_indexes(oldest.provenance_id)

            # Insert into history
            self._history.append(provenance)
            p_id = provenance.provenance_id

            # Update ID index
            self._by_id[p_id] = provenance

            # Update Operation index
            op_id = provenance.operation_id
            if p_id not in self._by_operation[op_id]:
                self._by_operation[op_id].append(p_id)

            # Update Latest Stream index
            stream_key = (provenance.symbol, provenance.timeframe)
            self._latest_by_stream[stream_key] = p_id

            # Update Child-to-Parent lineage indexes
            for parent_id in provenance.parent_provenance_ids:
                if p_id not in self._children_index[parent_id]:
                    self._children_index[parent_id].append(p_id)
                self._total_lineage_links += 1

            self._total_records_tracked += 1

            # Optional SQLite persistence
            if self._conn is not None:
                try:
                    with self._conn:
                        self._conn.execute(
                            """
                            INSERT OR REPLACE INTO market_data_provenance (
                                provenance_id, operation_id, source_type, source_identifier,
                                symbol, timeframe, data_start, data_end, record_count,
                                ingestion_timestamp, processing_timestamp, quality_assessment_id,
                                quality_status, quality_score, schema_version, transformation_name,
                                transformation_version, input_record_count, output_record_count,
                                parent_provenance_ids, replay_id, metadata_json, created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                provenance.provenance_id,
                                provenance.operation_id,
                                provenance.source_type.value,
                                provenance.source_identifier,
                                provenance.symbol,
                                provenance.timeframe,
                                provenance.data_start,
                                provenance.data_end,
                                provenance.record_count,
                                provenance.ingestion_timestamp,
                                provenance.processing_timestamp,
                                provenance.quality_assessment_id,
                                provenance.quality_status.value if provenance.quality_status else None,
                                provenance.quality_score,
                                provenance.schema_version,
                                provenance.transformation_name,
                                provenance.transformation_version,
                                provenance.input_record_count,
                                provenance.output_record_count,
                                json.dumps(list(provenance.parent_provenance_ids)),
                                provenance.replay_id,
                                json.dumps(provenance.metadata),
                                time.time(),
                            ),
                        )
                except Exception as exc:
                    logger.warning(f"Failed to persist provenance to SQLite: {exc}")

        # Emit asynchronous audit event
        self._publish_event(
            event_type=EventType.DATA_PROVENANCE_CREATED,
            provenance=provenance,
        )

        # If it has parents, emit linkage event
        if provenance.parent_provenance_ids:
            self._publish_linkage_event(provenance)

        # Update bounded StateStore cache if attached
        self._update_state_store(provenance)

        return provenance

    def _evict_from_indexes(self, evicted_id: str) -> None:
        """Prune evicted provenance record from auxiliary lookup tables."""
        prov = self._by_id.pop(evicted_id, None)
        if prov:
            # Remove from operation index
            op_list = self._by_operation.get(prov.operation_id)
            if op_list and evicted_id in op_list:
                op_list.remove(evicted_id)
                if not op_list:
                    self._by_operation.pop(prov.operation_id, None)

            # Remove from latest stream if it still pointed to this record
            stream_key = (prov.symbol, prov.timeframe)
            if self._latest_by_stream.get(stream_key) == evicted_id:
                self._latest_by_stream.pop(stream_key, None)

    def get_provenance(self, provenance_id: str) -> Optional[MarketDataProvenance]:
        """Look up a provenance record by its deterministic identifier."""
        if not provenance_id or not isinstance(provenance_id, str):
            return None
        pid = provenance_id.strip()
        with self._lock:
            cached = self._by_id.get(pid)
            if cached is not None:
                return cached
            if self._conn is not None:
                try:
                    cursor = self._conn.execute(
                        "SELECT * FROM market_data_provenance WHERE provenance_id = ?",
                        (pid,),
                    )
                    row = cursor.fetchone()
                    if row:
                        parents = tuple(json.loads(row["parent_provenance_ids"])) if row["parent_provenance_ids"] else ()
                        meta = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
                        q_status = DataQualityStatus(row["quality_status"]) if row["quality_status"] else None
                        return MarketDataProvenance(
                            provenance_id=row["provenance_id"],
                            operation_id=row["operation_id"],
                            source_type=DataSourceType.from_string(row["source_type"]),
                            source_identifier=row["source_identifier"],
                            symbol=row["symbol"],
                            timeframe=row["timeframe"],
                            data_start=row["data_start"],
                            data_end=row["data_end"],
                            record_count=row["record_count"],
                            ingestion_timestamp=row["ingestion_timestamp"],
                            processing_timestamp=row["processing_timestamp"],
                            quality_assessment_id=row["quality_assessment_id"],
                            quality_status=q_status,
                            quality_score=row["quality_score"],
                            schema_version=row["schema_version"],
                            transformation_name=row["transformation_name"],
                            transformation_version=row["transformation_version"],
                            input_record_count=row["input_record_count"],
                            output_record_count=row["output_record_count"],
                            parent_provenance_ids=parents,
                            replay_id=row["replay_id"],
                            metadata=meta,
                        )
                except Exception as exc:
                    logger.warning(f"Failed to read provenance from SQLite: {exc}")
            return None

    def get_by_operation(self, operation_id: str) -> List[MarketDataProvenance]:
        """Retrieve all provenance records generated during a specific runtime operation."""
        if not operation_id or not isinstance(operation_id, str):
            return []
        with self._lock:
            p_ids = self._by_operation.get(operation_id.strip(), [])
            return [self._by_id[pid] for pid in p_ids if pid in self._by_id]

    def get_latest(self, symbol: str, timeframe: str) -> Optional[MarketDataProvenance]:
        """Retrieve the latest recorded provenance for a symbol and timeframe."""
        if not symbol or not timeframe:
            return None
        norm_sym = symbol.strip().upper()
        try:
            norm_tf = normalize_timeframe(timeframe)
        except ValidationError:
            norm_tf = timeframe.strip()

        with self._lock:
            pid = self._latest_by_stream.get((norm_sym, norm_tf))
            return self._by_id.get(pid) if pid else None

    def get_lineage(
        self,
        provenance_id: str,
        max_depth: int = DEFAULT_MAX_LINEAGE_DEPTH,
        direction: str = "upstream",
    ) -> LineageGraph:
        """Traverse the provenance graph recursively with cycle detection and bounded depth.

        Args:
            provenance_id: Starting root provenance identifier.
            max_depth: Maximum recursive traversal depth (clamped between 1 and 20).
            direction: 'upstream' (follow parents) or 'downstream' (follow children).

        Returns:
            LineageGraph: Structured DAG traversal outcome.
        """
        clamped_depth = max(1, min(20, max_depth))
        dir_clean = "downstream" if direction.lower() == "downstream" else "upstream"

        with self._lock:
            root = self._by_id.get(provenance_id.strip())
            if not root:
                return LineageGraph(
                    root_id=provenance_id,
                    nodes={},
                    edges=(),
                    max_depth_reached=False,
                    cycle_detected=False,
                    direction=dir_clean,
                )

            nodes_map: Dict[str, Dict[str, Any]] = {root.provenance_id: root.to_dict()}
            edges_list: List[Dict[str, str]] = []
            visited_in_path: Set[str] = set()
            max_depth_hit = False
            cycle_hit = False

            if dir_clean == "upstream":
                # Traverse upstream parents
                def traverse_upstream(curr_id: str, current_depth: int) -> None:
                    nonlocal max_depth_hit, cycle_hit
                    if current_depth >= clamped_depth:
                        max_depth_hit = True
                        return

                    visited_in_path.add(curr_id)
                    curr_node = self._by_id.get(curr_id)
                    if curr_node:
                        for parent_id in curr_node.parent_provenance_ids:
                            edges_list.append({"from": parent_id, "to": curr_id, "direction": "parent_to_child"})
                            if parent_id in visited_in_path:
                                cycle_hit = True
                                continue

                            parent_node = self._by_id.get(parent_id)
                            if parent_node:
                                nodes_map[parent_id] = parent_node.to_dict()
                                traverse_upstream(parent_id, current_depth + 1)
                            else:
                                # Known reference not currently in active memory window
                                nodes_map[parent_id] = {
                                    "provenance_id": parent_id,
                                    "status": "UNRESOLVED_IN_MEMORY",
                                }

                    visited_in_path.remove(curr_id)

                traverse_upstream(root.provenance_id, 0)
            else:
                # Traverse downstream children
                def traverse_downstream(curr_id: str, current_depth: int) -> None:
                    nonlocal max_depth_hit, cycle_hit
                    if current_depth >= clamped_depth:
                        max_depth_hit = True
                        return

                    visited_in_path.add(curr_id)
                    child_ids = self._children_index.get(curr_id, [])
                    for child_id in child_ids:
                        edges_list.append({"from": curr_id, "to": child_id, "direction": "parent_to_child"})
                        if child_id in visited_in_path:
                            cycle_hit = True
                            continue

                        child_node = self._by_id.get(child_id)
                        if child_node:
                            nodes_map[child_id] = child_node.to_dict()
                            traverse_downstream(child_id, current_depth + 1)

                    visited_in_path.remove(curr_id)

                traverse_downstream(root.provenance_id, 0)

            return LineageGraph(
                root_id=root.provenance_id,
                nodes=nodes_map,
                edges=tuple(edges_list),
                max_depth_reached=max_depth_hit,
                cycle_detected=cycle_hit,
                direction=dir_clean,
            )

    def get_audit_history(
        self,
        limit: int = 100,
        offset: int = 0,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        source_type: Optional[Union[DataSourceType, str]] = None,
        operation_id: Optional[str] = None,
        parent_provenance_id: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> List[MarketDataProvenance]:
        """Query bounded audit history with flexible filtering and pagination."""
        clamped_limit = max(1, min(500, limit))
        clamped_offset = max(0, offset)

        norm_sym = symbol.strip().upper() if symbol else None
        norm_tf = None
        if timeframe:
            try:
                norm_tf = normalize_timeframe(timeframe)
            except ValidationError:
                norm_tf = timeframe.strip()

        st_filter = DataSourceType.from_string(source_type) if source_type else None
        op_filter = operation_id.strip() if operation_id else None
        parent_filter = parent_provenance_id.strip() if parent_provenance_id else None

        with self._lock:
            # History is stored chronological (oldest to newest); reverse for newest-first display
            records = list(self._history)
            records.reverse()

            filtered: List[MarketDataProvenance] = []
            for p in records:
                if norm_sym and p.symbol != norm_sym:
                    continue
                if norm_tf and p.timeframe != norm_tf:
                    continue
                if st_filter and p.source_type != st_filter:
                    continue
                if op_filter and p.operation_id != op_filter:
                    continue
                if parent_filter and parent_filter not in p.parent_provenance_ids:
                    continue
                if start_time is not None and p.processing_timestamp < start_time:
                    continue
                if end_time is not None and p.processing_timestamp > end_time:
                    continue
                filtered.append(p)

            return filtered[clamped_offset : clamped_offset + clamped_limit]

    def get_summary(self) -> Dict[str, Any]:
        """Return aggregated summary metrics for audit and health reporting."""
        with self._lock:
            by_source: Dict[str, int] = collections.defaultdict(int)
            by_quality: Dict[str, int] = collections.defaultdict(int)

            for p in self._history:
                by_source[p.source_type.value] += 1
                q_status = p.quality_status.value if p.quality_status else "UNKNOWN"
                by_quality[q_status] += 1

            return {
                "total_records_tracked": self._total_records_tracked,
                "current_retained_records": len(self._history),
                "max_history_capacity": self.max_history,
                "total_lineage_links": self._total_lineage_links,
                "monitored_streams_count": len(self._latest_by_stream),
                "by_source_type": dict(by_source),
                "by_quality_status": dict(by_quality),
            }

    def reset(self) -> None:
        """Reset internal history and lookup indexes."""
        with self._lock:
            self._history.clear()
            self._by_id.clear()
            self._by_operation.clear()
            self._latest_by_stream.clear()
            self._children_index.clear()
            self._total_records_tracked = 0
            self._total_lineage_links = 0

    # =========================================================================
    # DOMAIN PROVENANCE FACTORY HELPERS
    # =========================================================================

    def create_provider_provenance(
        self,
        provider_name: str,
        symbol: str,
        timeframe: str,
        bars: Sequence[Any],
        request_start: Optional[float] = None,
        request_end: Optional[float] = None,
        operation_id: Optional[str] = None,
        quality_assessment: Optional[DataQualityAssessment] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MarketDataProvenance:
        """Factory for generating provenance from an external or mock historical data provider query."""
        op_id = operation_id or f"op_prov_{uuid.uuid4().hex[:10]}"
        now = time.time()
        record_count = len(bars)
        data_start = None
        data_end = None

        if bars:
            first_ts = getattr(bars[0], "timestamp", None) or (bars[0].get("timestamp") if isinstance(bars[0], dict) else None)
            last_ts = getattr(bars[-1], "timestamp", None) or (bars[-1].get("timestamp") if isinstance(bars[-1], dict) else None)
            data_start = float(first_ts) if first_ts is not None else request_start
            data_end = float(last_ts) if last_ts is not None else request_end
        else:
            data_start = request_start
            data_end = request_end

        q_id = quality_assessment.assessment_id if quality_assessment else None
        q_status = quality_assessment.quality_status if quality_assessment else None
        q_score = quality_assessment.quality_score if quality_assessment else None

        pid = generate_deterministic_provenance_id(
            source_type=DataSourceType.PROVIDER,
            source_identifier=provider_name,
            symbol=symbol,
            timeframe=timeframe,
            data_start=data_start,
            data_end=data_end,
            record_count=record_count,
            quality_assessment_id=q_id,
        )

        clean_meta = dict(metadata or {})
        clean_meta["provider_name"] = provider_name
        if request_start:
            clean_meta["request_start"] = request_start
        if request_end:
            clean_meta["request_end"] = request_end

        prov = MarketDataProvenance(
            provenance_id=pid,
            operation_id=op_id,
            source_type=DataSourceType.PROVIDER,
            source_identifier=provider_name,
            symbol=symbol,
            timeframe=timeframe,
            data_start=data_start,
            data_end=data_end,
            record_count=record_count,
            ingestion_timestamp=now,
            processing_timestamp=now,
            quality_assessment_id=q_id,
            quality_status=q_status,
            quality_score=q_score,
            transformation_name="provider_load",
            transformation_version="1.0.0",
            output_record_count=record_count,
            metadata=clean_meta,
        )
        return self.record_provenance(prov)

    def create_repository_provenance(
        self,
        repository_name: str,
        symbol: str,
        timeframe: str,
        bars: Sequence[Any],
        query_start: Optional[float] = None,
        query_end: Optional[float] = None,
        operation_id: Optional[str] = None,
        parent_provenance_id: Optional[str] = None,
        quality_assessment: Optional[DataQualityAssessment] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MarketDataProvenance:
        """Factory for generating provenance from a historical SQLite repository read/write."""
        op_id = operation_id or f"op_repo_{uuid.uuid4().hex[:10]}"
        now = time.time()
        record_count = len(bars)
        data_start = None
        data_end = None

        if bars:
            first_ts = getattr(bars[0], "timestamp", None) or (bars[0].get("timestamp") if isinstance(bars[0], dict) else None)
            last_ts = getattr(bars[-1], "timestamp", None) or (bars[-1].get("timestamp") if isinstance(bars[-1], dict) else None)
            data_start = float(first_ts) if first_ts is not None else query_start
            data_end = float(last_ts) if last_ts is not None else query_end
        else:
            data_start = query_start
            data_end = query_end

        parents = (parent_provenance_id,) if parent_provenance_id else ()
        q_id = quality_assessment.assessment_id if quality_assessment else None
        q_status = quality_assessment.quality_status if quality_assessment else None
        q_score = quality_assessment.quality_score if quality_assessment else None

        pid = generate_deterministic_provenance_id(
            source_type=DataSourceType.SQLITE,
            source_identifier=repository_name,
            symbol=symbol,
            timeframe=timeframe,
            data_start=data_start,
            data_end=data_end,
            record_count=record_count,
            parent_provenance_ids=parents,
            quality_assessment_id=q_id,
        )

        clean_meta = dict(metadata or {})
        clean_meta["repository"] = repository_name

        prov = MarketDataProvenance(
            provenance_id=pid,
            operation_id=op_id,
            source_type=DataSourceType.SQLITE,
            source_identifier=repository_name,
            symbol=symbol,
            timeframe=timeframe,
            data_start=data_start,
            data_end=data_end,
            record_count=record_count,
            ingestion_timestamp=now,
            processing_timestamp=now,
            quality_assessment_id=q_id,
            quality_status=q_status,
            quality_score=q_score,
            transformation_name="sqlite_query",
            transformation_version="1.0.0",
            parent_provenance_ids=parents,
            output_record_count=record_count,
            metadata=clean_meta,
        )
        return self.record_provenance(prov)

    def create_replay_provenance(
        self,
        replay_id: str,
        symbol: str,
        timeframe: str,
        bars_replayed: int,
        start_timestamp: Optional[float] = None,
        end_timestamp: Optional[float] = None,
        parent_provenance_ids: Sequence[str] = (),
        quality_assessment: Optional[DataQualityAssessment] = None,
        operation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MarketDataProvenance:
        """Factory for generating provenance from historical replay executions."""
        op_id = operation_id or f"op_rep_{uuid.uuid4().hex[:10]}"
        now = time.time()
        q_id = quality_assessment.assessment_id if quality_assessment else None
        q_status = quality_assessment.quality_status if quality_assessment else None
        q_score = quality_assessment.quality_score if quality_assessment else None

        pid = generate_deterministic_provenance_id(
            source_type=DataSourceType.REPLAY,
            source_identifier=replay_id,
            symbol=symbol,
            timeframe=timeframe,
            data_start=start_timestamp,
            data_end=end_timestamp,
            record_count=bars_replayed,
            transformation_name="historical_replay",
            transformation_version="1.0.0",
            parent_provenance_ids=parent_provenance_ids,
            quality_assessment_id=q_id,
        )

        clean_meta = dict(metadata or {})
        clean_meta["replay_id"] = replay_id

        prov = MarketDataProvenance(
            provenance_id=pid,
            operation_id=op_id,
            source_type=DataSourceType.REPLAY,
            source_identifier=replay_id,
            symbol=symbol,
            timeframe=timeframe,
            data_start=start_timestamp,
            data_end=end_timestamp,
            record_count=bars_replayed,
            ingestion_timestamp=now,
            processing_timestamp=now,
            quality_assessment_id=q_id,
            quality_status=q_status,
            quality_score=q_score,
            transformation_name="historical_replay",
            transformation_version="1.0.0",
            parent_provenance_ids=tuple(parent_provenance_ids),
            replay_id=replay_id,
            output_record_count=bars_replayed,
            metadata=clean_meta,
        )
        return self.record_provenance(prov)

    def create_analysis_provenance(
        self,
        analysis_name: str,
        symbol: str,
        timeframe: str,
        feature_timestamp: float,
        parent_provenance_ids: Sequence[str],
        indicator_count: int,
        bar_count: int,
        operation_id: Optional[str] = None,
        quality_status: Optional[DataQualityStatus] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MarketDataProvenance:
        """Factory for generating derived provenance from technical analysis vector computations."""
        op_id = operation_id or f"op_ana_{uuid.uuid4().hex[:10]}"
        now = time.time()

        pid = generate_deterministic_provenance_id(
            source_type=DataSourceType.DERIVED,
            source_identifier=analysis_name,
            symbol=symbol,
            timeframe=timeframe,
            data_start=feature_timestamp,
            data_end=feature_timestamp,
            record_count=indicator_count,
            transformation_name="technical_analysis",
            transformation_version="1.0.0",
            parent_provenance_ids=parent_provenance_ids,
        )

        clean_meta = dict(metadata or {})
        clean_meta["bar_count_used"] = bar_count
        clean_meta["indicators_computed"] = indicator_count

        prov = MarketDataProvenance(
            provenance_id=pid,
            operation_id=op_id,
            source_type=DataSourceType.DERIVED,
            source_identifier=analysis_name,
            symbol=symbol,
            timeframe=timeframe,
            data_start=feature_timestamp,
            data_end=feature_timestamp,
            record_count=indicator_count,
            ingestion_timestamp=now,
            processing_timestamp=now,
            quality_status=quality_status,
            transformation_name="technical_analysis",
            transformation_version="1.0.0",
            input_record_count=bar_count,
            output_record_count=indicator_count,
            parent_provenance_ids=tuple(parent_provenance_ids),
            metadata=clean_meta,
        )
        return self.record_provenance(prov)

    def create_backtest_provenance(
        self,
        strategy_id: str,
        strategy_version: str,
        symbol: str,
        timeframe: str,
        data_start: float,
        data_end: float,
        bars_evaluated: int,
        trades_simulated: int,
        parent_provenance_ids: Sequence[str],
        run_id: str,
        operation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MarketDataProvenance:
        """Factory for generating backtest simulation market data provenance."""
        op_id = operation_id or f"op_bt_{uuid.uuid4().hex[:10]}"
        now = time.time()

        pid = generate_deterministic_provenance_id(
            source_type=DataSourceType.BACKTEST,
            source_identifier=f"{strategy_id}:{strategy_version}",
            symbol=symbol,
            timeframe=timeframe,
            data_start=data_start,
            data_end=data_end,
            record_count=bars_evaluated,
            transformation_name="backtest_simulation",
            transformation_version=strategy_version,
            parent_provenance_ids=parent_provenance_ids,
        )

        clean_meta = dict(metadata or {})
        clean_meta["run_id"] = run_id
        clean_meta["strategy_id"] = strategy_id
        clean_meta["strategy_version"] = strategy_version
        clean_meta["trades_simulated"] = trades_simulated

        prov = MarketDataProvenance(
            provenance_id=pid,
            operation_id=op_id,
            source_type=DataSourceType.BACKTEST,
            source_identifier=f"{strategy_id}:{strategy_version}",
            symbol=symbol,
            timeframe=timeframe,
            data_start=data_start,
            data_end=data_end,
            record_count=bars_evaluated,
            ingestion_timestamp=now,
            processing_timestamp=now,
            transformation_name="backtest_simulation",
            transformation_version=strategy_version,
            input_record_count=bars_evaluated,
            output_record_count=trades_simulated,
            parent_provenance_ids=tuple(parent_provenance_ids),
            metadata=clean_meta,
        )
        return self.record_provenance(prov)

    def create_quality_provenance(
        self,
        assessment: DataQualityAssessment,
        parent_provenance_ids: Sequence[str] = (),
        operation_id: Optional[str] = None,
        source_type: Union[DataSourceType, str] = DataSourceType.DERIVED,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MarketDataProvenance:
        """Factory for generating provenance from a DataQualityAssessment."""
        op_id = operation_id or assessment.correlation_id or f"op_qual_{uuid.uuid4().hex[:10]}"
        now = time.time()
        src_type = DataSourceType.from_string(source_type)

        anom_summary = {
            "total_anomalies": len(assessment.anomalies) if assessment.anomalies else assessment.anomaly_count,
            "highest_severity": assessment.highest_severity.value if assessment.highest_severity else "INFO",
            "warnings_count": len(assessment.warnings),
            "errors_count": len(assessment.errors),
            "safe_to_consume": assessment.safe_to_consume,
        }

        d_start = assessment.data_start if assessment.data_start is not None else assessment.timestamp
        d_end = assessment.data_end if assessment.data_end is not None else assessment.timestamp
        tf = assessment.timeframe if assessment.timeframe else "UNKNOWN"

        pid = generate_deterministic_provenance_id(
            source_type=src_type,
            source_identifier=assessment.source or "quality_monitor",
            symbol=assessment.symbol,
            timeframe=tf,
            data_start=d_start,
            data_end=d_end,
            record_count=assessment.total_records,
            transformation_name="quality_assessment",
            transformation_version="1.0.0",
            parent_provenance_ids=parent_provenance_ids,
            quality_assessment_id=assessment.assessment_id,
        )

        clean_meta = dict(metadata or {})
        clean_meta["quality_summary"] = anom_summary
        clean_meta["data_type"] = assessment.data_type

        prov = MarketDataProvenance(
            provenance_id=pid,
            operation_id=op_id,
            source_type=src_type,
            source_identifier=assessment.source or "quality_monitor",
            symbol=assessment.symbol,
            timeframe=tf,
            data_start=d_start,
            data_end=d_end,
            record_count=assessment.total_records,
            ingestion_timestamp=assessment.assessment_timestamp or now,
            processing_timestamp=now,
            quality_assessment_id=assessment.assessment_id,
            quality_status=assessment.quality_status,
            quality_score=assessment.quality_score,
            transformation_name="quality_assessment",
            transformation_version="1.0.0",
            input_record_count=assessment.total_records,
            output_record_count=assessment.valid_records,
            parent_provenance_ids=tuple(parent_provenance_ids),
            metadata=clean_meta,
        )
        return self.record_provenance(prov)

    def create_portfolio_simulation_provenance(
        self,
        simulation_id: str,
        symbols: Sequence[str],
        timeframes: Sequence[str],
        data_start: float,
        data_end: float,
        bars_evaluated: int,
        trades_simulated: int,
        parent_provenance_ids: Sequence[str],
        strategy_names: Sequence[str] = (),
        operation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> MarketDataProvenance:
        """Factory for generating provenance from a multi-strategy portfolio simulation."""
        op_id = operation_id or f"op_psim_{uuid.uuid4().hex[:10]}"
        now = time.time()

        primary_sym = symbols[0] if symbols else "PORTFOLIO"
        primary_tf = timeframes[0] if timeframes else "PORTFOLIO"

        clean_meta = dict(metadata or {})
        clean_meta["simulation_id"] = simulation_id
        clean_meta["symbols"] = list(symbols)
        clean_meta["timeframes"] = list(timeframes)
        clean_meta["strategies"] = list(strategy_names)
        clean_meta["trades_simulated"] = trades_simulated

        pid = generate_deterministic_provenance_id(
            source_type=DataSourceType.DERIVED,
            source_identifier=f"portfolio_sim:{simulation_id}",
            symbol=primary_sym,
            timeframe=primary_tf,
            data_start=data_start,
            data_end=data_end,
            record_count=bars_evaluated,
            transformation_name="portfolio_simulation",
            transformation_version="1.0.0",
            parent_provenance_ids=parent_provenance_ids,
        )

        prov = MarketDataProvenance(
            provenance_id=pid,
            operation_id=op_id,
            source_type=DataSourceType.DERIVED,
            source_identifier=f"portfolio_sim:{simulation_id}",
            symbol=primary_sym,
            timeframe=primary_tf,
            data_start=data_start,
            data_end=data_end,
            record_count=bars_evaluated,
            ingestion_timestamp=now,
            processing_timestamp=now,
            transformation_name="portfolio_simulation",
            transformation_version="1.0.0",
            input_record_count=bars_evaluated,
            output_record_count=trades_simulated,
            parent_provenance_ids=tuple(parent_provenance_ids),
            metadata=clean_meta,
        )
        return self.record_provenance(prov)

    # =========================================================================
    # INTERNAL TELEMETRY & PERSISTENCE HELPERS
    # =========================================================================

    def _publish_event(self, event_type: EventType, provenance: MarketDataProvenance) -> None:
        """Publish sanitized event telemetry to EventBus if connected."""
        if not self.event_bus:
            return

        payload = {
            "provenance_id": provenance.provenance_id,
            "operation_id": provenance.operation_id,
            "source_type": provenance.source_type.value,
            "source_identifier": provenance.source_identifier,
            "symbol": provenance.symbol,
            "timeframe": provenance.timeframe,
            "record_count": provenance.record_count,
            "quality_status": provenance.quality_status.value if provenance.quality_status else None,
            "quality_score": provenance.quality_score,
            "has_parents": provenance.parent_count > 0,
            "parent_count": provenance.parent_count,
        }

        try:
            event = Event.create(
                event_type=event_type,
                source=self.name,
                payload=payload,
                correlation_id=provenance.operation_id,
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.warning(f"Failed to publish {event_type.value} event: {exc}")

    def _publish_linkage_event(self, provenance: MarketDataProvenance) -> None:
        """Publish lineage linkage telemetry when parent associations exist."""
        if not self.event_bus:
            return

        payload = {
            "child_provenance_id": provenance.provenance_id,
            "parent_provenance_ids": list(provenance.parent_provenance_ids),
            "symbol": provenance.symbol,
            "timeframe": provenance.timeframe,
            "transformation_name": provenance.transformation_name,
            "operation_id": provenance.operation_id,
        }

        try:
            event = Event.create(
                event_type=EventType.DATA_LINEAGE_LINKED,
                source=self.name,
                payload=payload,
                correlation_id=provenance.operation_id,
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.warning(f"Failed to publish DATA_LINEAGE_LINKED event: {exc}")

    def _publish_failure_event(
        self,
        operation_id: str,
        reason: str,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> None:
        """Publish failure event when provenance recording or resolution fails."""
        if not self.event_bus:
            return

        payload = {
            "operation_id": operation_id,
            "reason": sanitize_provenance_string(reason),
            "symbol": symbol,
            "timeframe": timeframe,
        }

        try:
            event = Event.create(
                event_type=EventType.DATA_PROVENANCE_FAILED,
                source=self.name,
                payload=payload,
                correlation_id=operation_id,
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.warning(f"Failed to publish DATA_PROVENANCE_FAILED event: {exc}")

    def _update_state_store(self, provenance: MarketDataProvenance) -> None:
        """Update bounded StateStore entries with latest provenance snapshot."""
        if not self.state_store:
            return

        try:
            # Store latest per stream
            key = f"market_provenance_latest_{provenance.symbol}_{provenance.timeframe.upper()}"
            self.state_store.set(key, provenance.to_dict())

            # Store aggregated summary
            self.state_store.set("market_provenance_summary", self.get_summary())
        except Exception as exc:
            logger.warning(f"Failed to update StateStore with provenance: {exc}")

    def close(self) -> None:
        """Close SQLite audit connection if open."""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
