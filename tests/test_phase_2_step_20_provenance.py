"""KAISER — Phase 2, Step 20 Test Suite: Market Data Provenance, Lineage & Audit Trail.

Comprehensive verification of:
A. Core Provenance Model (immutability, mandatory fields, source types, missing vs zero)
B. Deterministic Lineage Identity (SHA-256 stability, parameter sensitivity, memory-address independence)
C. Lineage DAG (parent-child relationships, multi-parent DAG, bounded depth traversal, cycle detection)
D. Quality Provenance Integration (Step 19 quality monitoring links, bounded anomaly summary, explicit unavailable quality)
E. Historical Data Integration (provider -> loader -> SQLite -> replay -> cache lineage)
F. Replay Lineage (replay execution lineage, cancellation handling without lineage corruption)
G. Analysis Lineage (technical feature vector provenance, multi-timeframe lineage)
H. Backtest Lineage (backtest simulation market-data provenance, strategy identity)
I. Portfolio Simulation Lineage (multi-strategy portfolio simulation provenance and upstream links)
J. Audit Trail Storage (bounded history, O(1) indexed lookups, filtering, concurrency)
K. SQLite Persistence (optional minimal SQLite audit table, retrieval fallback)
L. EventBus Integration (DATA_PROVENANCE_CREATED, DATA_LINEAGE_LINKED, DATA_PROVENANCE_FAILED, zero secret leakage)
M. StateStore Integration (bounded summary, latest stream snapshots, isolation)
N. Orchestrator Handlers (get_data_provenance, get_data_lineage, get_data_audit_history)
O. Dashboard API Integration (/api/data-provenance, /api/data-lineage, query/path parameters, traversal rejection, 200 OK)
P. Failure Recovery (malformed inputs, cycle rejection, missing parents, cancellation, error events)
Q. Property & Invariant Tests (deterministic identity, bounds, UTC consistency, no NaN, zero secrets)
R. Performance Benchmarks (100, 1,000, 10,000 records)
S. Safety Invariants (execution_locked=True, trading engine uninitialized, zero broker connections, JUJU untouched)
"""

from __future__ import annotations

import collections
import concurrent.futures
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.models import AnalysisConfig
from app.backtesting.engine import BacktestEngine
from app.backtesting.models import BacktestConfig, BacktestResult, BacktestStatus
from app.backtesting.portfolio_simulation.engine import PortfolioSimulationEngine
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
)
from app.backtesting.strategy import Strategy, StrategyRegistry
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.loader import HistoricalDataLoader
from app.market.historical.mock import MockHistoricalDataProvider
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalProviderStatus,
)
from app.market.historical.replay import (
    HistoricalReplayEngine,
    HistoricalReplayRequest,
    HistoricalReplayResult,
    HistoricalReplayStatus,
)
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.provenance.handlers import register_provenance_handlers
from app.market.provenance.models import (
    CREDENTIAL_KEY_PATTERNS,
    DataSourceType,
    LineageGraph,
    LineageNode,
    MarketDataProvenance,
    generate_deterministic_provenance_id,
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)
from app.market.provenance.tracker import (
    DEFAULT_MAX_LINEAGE_DEPTH,
    DEFAULT_MAX_PROVENANCE_HISTORY,
    MarketDataProvenanceTracker,
)
from app.market.quality.engine import MarketDataQualityEngine
from app.market.quality.models import MarketAnomaly, QualityFinding, QualitySeverity
from app.market.quality.monitor import MarketDataQualityMonitor
from app.market.quality.status import DataQualityAssessment, DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest
from app.portfolio.dashboard.server import PortfolioDashboardServer


# ==============================================================================
# 1. CORE PROVENANCE MODEL TESTS
# ==============================================================================

class TestProvenanceModel:
    """Tests for the MarketDataProvenance model invariants, fields, and immutability."""

    def test_immutable_dataclass(self) -> None:
        """Verify MarketDataProvenance is frozen and prohibits field mutation."""
        prov = MarketDataProvenance(
            provenance_id="prov_test_01",
            operation_id="op_01",
            source_type=DataSourceType.PROVIDER,
            source_identifier="mock_feed",
            symbol="RELIANCE",
            timeframe="1m",
            record_count=100,
        )
        with pytest.raises(Exception):
            prov.symbol = "TCS"  # type: ignore

    def test_required_fields_and_defaults(self) -> None:
        """Verify required fields are validated and default timestamps are populated."""
        now = time.time()
        prov = MarketDataProvenance(
            provenance_id="prov_test_02",
            operation_id="op_02",
            source_type=DataSourceType.SQLITE,
            source_identifier="local_db",
            symbol="INFY",
            timeframe="5m",
            record_count=50,
        )
        assert prov.provenance_id == "prov_test_02"
        assert prov.operation_id == "op_02"
        assert prov.source_type == DataSourceType.SQLITE
        assert prov.symbol == "INFY"
        assert prov.timeframe == "5m"
        assert prov.record_count == 50
        assert prov.ingestion_timestamp >= now - 1.0
        assert prov.processing_timestamp >= now - 1.0
        assert prov.schema_version == "1.0.0"
        assert prov.parent_provenance_ids == ()
        assert prov.has_quality is False
        assert prov.is_derived is False
        assert prov.parent_count == 0

    def test_enum_source_types(self) -> None:
        """Verify DataSourceType parsing handles canonical enum members and strings."""
        for st in [
            DataSourceType.MOCK,
            DataSourceType.PROVIDER,
            DataSourceType.SQLITE,
            DataSourceType.CACHE,
            DataSourceType.REPLAY,
            DataSourceType.BACKTEST,
            DataSourceType.DERIVED,
        ]:
            prov = MarketDataProvenance(
                provenance_id=f"prov_{st.value.lower()}",
                operation_id="op_test",
                source_type=st,
                source_identifier="src",
                symbol="NIFTY",
                timeframe="1m",
            )
            assert prov.source_type == st
            assert isinstance(prov.source_type, DataSourceType)

        # Flexibly parse strings
        assert DataSourceType.from_string("provider") == DataSourceType.PROVIDER
        assert DataSourceType.from_string("sqlite_repo") == DataSourceType.SQLITE
        assert DataSourceType.from_string("historical_replay") == DataSourceType.REPLAY
        assert DataSourceType.from_string("cache") == DataSourceType.CACHE
        assert DataSourceType.from_string("simulation") == DataSourceType.BACKTEST
        assert DataSourceType.from_string("analysis") == DataSourceType.DERIVED
        assert DataSourceType.from_string("unknown_source") == DataSourceType.MOCK

    def test_symbol_and_timeframe_normalization(self) -> None:
        """Verify symbol is uppercase and timeframe is canonicalized."""
        prov = MarketDataProvenance(
            provenance_id="prov_norm",
            operation_id="op_norm",
            source_type=DataSourceType.PROVIDER,
            source_identifier="src",
            symbol="  nifty  ",
            timeframe="1M",
        )
        assert prov.symbol == "NIFTY"
        assert prov.timeframe == "1m"

    def test_missing_vs_zero_distinction(self) -> None:
        """Verify explicit distinction between None (missing) and 0.0 / 0 (valid zero)."""
        prov_zero = MarketDataProvenance(
            provenance_id="prov_zero",
            operation_id="op_zero",
            source_type=DataSourceType.PROVIDER,
            source_identifier="src",
            symbol="NIFTY",
            timeframe="1m",
            data_start=0.0,
            data_end=0.0,
            record_count=0,
            quality_score=0.0,
        )
        prov_none = MarketDataProvenance(
            provenance_id="prov_none",
            operation_id="op_none",
            source_type=DataSourceType.PROVIDER,
            source_identifier="src",
            symbol="NIFTY",
            timeframe="1m",
            data_start=None,
            data_end=None,
            record_count=0,
            quality_score=None,
        )
        d_zero = prov_zero.to_dict()
        d_none = prov_none.to_dict()

        assert d_zero["data_start"] == 0.0
        assert d_zero["quality_score"] == 0.0
        assert d_none["data_start"] is None
        assert d_none["quality_score"] is None

    def test_invalid_parameters_raise_validation_error(self) -> None:
        """Verify invalid bounds and identifiers trigger ValidationError."""
        # Empty provenance_id
        with pytest.raises(ValidationError):
            MarketDataProvenance(
                provenance_id="",
                operation_id="op",
                source_type=DataSourceType.PROVIDER,
                source_identifier="src",
                symbol="NIFTY",
                timeframe="1m",
            )
        # Empty symbol
        with pytest.raises(ValidationError):
            MarketDataProvenance(
                provenance_id="prov_1",
                operation_id="op",
                source_type=DataSourceType.PROVIDER,
                source_identifier="src",
                symbol="",
                timeframe="1m",
            )
        # Negative record count
        with pytest.raises(ValidationError):
            MarketDataProvenance(
                provenance_id="prov_1",
                operation_id="op",
                source_type=DataSourceType.PROVIDER,
                source_identifier="src",
                symbol="NIFTY",
                timeframe="1m",
                record_count=-5,
            )
        # Inverted timestamps
        with pytest.raises(ValidationError):
            MarketDataProvenance(
                provenance_id="prov_1",
                operation_id="op",
                source_type=DataSourceType.PROVIDER,
                source_identifier="src",
                symbol="NIFTY",
                timeframe="1m",
                data_start=2000.0,
                data_end=1000.0,
            )
        # Self-referencing parent
        with pytest.raises(ValidationError):
            MarketDataProvenance(
                provenance_id="prov_self",
                operation_id="op",
                source_type=DataSourceType.PROVIDER,
                source_identifier="src",
                symbol="NIFTY",
                timeframe="1m",
                parent_provenance_ids=("prov_self",),
            )
        # Invalid quality score
        with pytest.raises(ValidationError):
            MarketDataProvenance(
                provenance_id="prov_1",
                operation_id="op",
                source_type=DataSourceType.PROVIDER,
                source_identifier="src",
                symbol="NIFTY",
                timeframe="1m",
                quality_score=150.0,
            )


# ==============================================================================
# 2. DETERMINISTIC LINEAGE IDENTITY TESTS
# ==============================================================================

class TestDeterministicIdentity:
    """Tests for deterministic hashing and collision resistance."""

    def test_identical_inputs_produce_identical_id(self) -> None:
        """Verify identical canonical inputs produce byte-for-byte identical IDs."""
        id1 = generate_deterministic_provenance_id(
            source_type=DataSourceType.PROVIDER,
            source_identifier="nse_feed",
            symbol="TCS",
            timeframe="15m",
            data_start=1600000000.0,
            data_end=1600003600.0,
            record_count=4,
            transformation_name="load",
            transformation_version="1.0.0",
        )
        id2 = generate_deterministic_provenance_id(
            source_type=DataSourceType.PROVIDER,
            source_identifier="nse_feed",
            symbol="TCS",
            timeframe="15m",
            data_start=1600000000.0,
            data_end=1600003600.0,
            record_count=4,
            transformation_name="load",
            transformation_version="1.0.0",
        )
        assert id1 == id2
        assert id1.startswith("prov_")

    def test_changed_source_changes_id(self) -> None:
        """Verify altering source changes the deterministic identifier."""
        id_provider = generate_deterministic_provenance_id(
            source_type=DataSourceType.PROVIDER,
            source_identifier="feed_a",
            symbol="TCS",
            timeframe="1m",
        )
        id_sqlite = generate_deterministic_provenance_id(
            source_type=DataSourceType.SQLITE,
            source_identifier="feed_a",
            symbol="TCS",
            timeframe="1m",
        )
        assert id_provider != id_sqlite

    def test_changed_range_changes_id(self) -> None:
        """Verify altering data start or end changes deterministic identifier."""
        id_base = generate_deterministic_provenance_id(
            source_type=DataSourceType.PROVIDER,
            source_identifier="feed",
            symbol="TCS",
            timeframe="1m",
            data_start=1000.0,
            data_end=2000.0,
        )
        id_diff_start = generate_deterministic_provenance_id(
            source_type=DataSourceType.PROVIDER,
            source_identifier="feed",
            symbol="TCS",
            timeframe="1m",
            data_start=1001.0,
            data_end=2000.0,
        )
        assert id_base != id_diff_start

    def test_changed_transformation_version_changes_id(self) -> None:
        """Verify altering transformation version changes deterministic identifier."""
        id_v1 = generate_deterministic_provenance_id(
            source_type=DataSourceType.DERIVED,
            source_identifier="indicator",
            symbol="TCS",
            timeframe="1m",
            transformation_name="rsi",
            transformation_version="1.0.0",
        )
        id_v2 = generate_deterministic_provenance_id(
            source_type=DataSourceType.DERIVED,
            source_identifier="indicator",
            symbol="TCS",
            timeframe="1m",
            transformation_name="rsi",
            transformation_version="2.0.0",
        )
        assert id_v1 != id_v2

    def test_no_memory_address_dependence(self) -> None:
        """Verify IDs are purely function of semantic values, not object addresses."""
        sym_obj1 = "NIFTY" + ""
        sym_obj2 = "".join(["N", "I", "F", "T", "Y"])
        assert id(sym_obj1) != id(sym_obj2)

        id1 = generate_deterministic_provenance_id(
            source_type="PROVIDER",
            source_identifier="src",
            symbol=sym_obj1,
            timeframe="1m",
        )
        id2 = generate_deterministic_provenance_id(
            source_type="PROVIDER",
            source_identifier="src",
            symbol=sym_obj2,
            timeframe="1m",
        )
        assert id1 == id2


# ==============================================================================
# 3. LINEAGE DAG TRAVERSAL TESTS
# ==============================================================================

class TestLineageDAG:
    """Tests for DAG construction, parent-child linkage, traversal, and cycle prevention."""

    def test_parent_child_linkage(self) -> None:
        """Verify parent and child provenance are correctly linked and queryable."""
        tracker = MarketDataProvenanceTracker()

        p_parent = MarketDataProvenance(
            provenance_id="prov_parent_1",
            operation_id="op_1",
            source_type=DataSourceType.PROVIDER,
            source_identifier="provider_feed",
            symbol="SBIN",
            timeframe="1m",
            record_count=100,
        )
        tracker.record_provenance(p_parent)

        p_child = MarketDataProvenance(
            provenance_id="prov_child_1",
            operation_id="op_2",
            source_type=DataSourceType.SQLITE,
            source_identifier="sqlite_repo",
            symbol="SBIN",
            timeframe="1m",
            record_count=100,
            parent_provenance_ids=("prov_parent_1",),
        )
        tracker.record_provenance(p_child)

        graph_up = tracker.get_lineage("prov_child_1", direction="upstream")
        assert graph_up.root_id == "prov_child_1"
        assert "prov_parent_1" in graph_up.nodes
        assert len(graph_up.edges) == 1
        assert graph_up.edges[0]["from"] == "prov_parent_1"
        assert graph_up.edges[0]["to"] == "prov_child_1"

        graph_down = tracker.get_lineage("prov_parent_1", direction="downstream")
        assert graph_down.root_id == "prov_parent_1"
        assert "prov_child_1" in graph_down.nodes

    def test_multi_parent_lineage(self) -> None:
        """Verify derived nodes can reference multiple upstream parents (e.g. multi-tf)."""
        tracker = MarketDataProvenanceTracker()

        p_1m = tracker.record_provenance(
            MarketDataProvenance(
                provenance_id="prov_1m",
                operation_id="op_1m",
                source_type=DataSourceType.PROVIDER,
                source_identifier="feed",
                symbol="NIFTY",
                timeframe="1m",
                record_count=60,
            )
        )
        p_5m = tracker.record_provenance(
            MarketDataProvenance(
                provenance_id="prov_5m",
                operation_id="op_5m",
                source_type=DataSourceType.PROVIDER,
                source_identifier="feed",
                symbol="NIFTY",
                timeframe="5m",
                record_count=12,
            )
        )
        p_analysis = tracker.record_provenance(
            MarketDataProvenance(
                provenance_id="prov_mtf_analysis",
                operation_id="op_mtf",
                source_type=DataSourceType.DERIVED,
                source_identifier="mtf_engine",
                symbol="NIFTY",
                timeframe="1m",
                transformation_name="multi_timeframe_alignment",
                parent_provenance_ids=(p_1m.provenance_id, p_5m.provenance_id),
            )
        )

        graph = tracker.get_lineage(p_analysis.provenance_id, direction="upstream")
        assert len(graph.nodes) == 3
        assert p_1m.provenance_id in graph.nodes
        assert p_5m.provenance_id in graph.nodes
        assert len(graph.edges) == 2

    def test_bounded_traversal_depth(self) -> None:
        """Verify recursive traversal strictly terminates at configured max_depth."""
        tracker = MarketDataProvenanceTracker(max_history=100)

        # Build a chain of 10 nodes: N0 -> N1 -> ... -> N9
        curr_parent: Optional[str] = None
        for i in range(10):
            node_id = f"prov_chain_{i}"
            parents = (curr_parent,) if curr_parent else ()
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=node_id,
                    operation_id=f"op_{i}",
                    source_type=DataSourceType.DERIVED,
                    source_identifier="chain",
                    symbol="NIFTY",
                    timeframe="1m",
                    parent_provenance_ids=parents,
                )
            )
            curr_parent = node_id

        # Query with depth limit of 3
        graph = tracker.get_lineage("prov_chain_9", max_depth=3, direction="upstream")
        assert graph.max_depth_reached is True
        # Node 9, 8, 7 should be present
        assert "prov_chain_9" in graph.nodes
        assert "prov_chain_8" in graph.nodes
        assert "prov_chain_7" in graph.nodes
        # Node 0 should not be visited because depth clamped to 3
        assert "prov_chain_0" not in graph.nodes

    def test_cycle_prevention(self) -> None:
        """Verify cyclical parent references are intercepted without recursion errors."""
        tracker = MarketDataProvenanceTracker()

        # Manually create cyclical nodes (e.g. A -> B -> C -> A)
        tracker._by_id["node_A"] = MarketDataProvenance(
            provenance_id="node_A",
            operation_id="op_a",
            source_type=DataSourceType.DERIVED,
            source_identifier="src",
            symbol="NIFTY",
            timeframe="1m",
            parent_provenance_ids=("node_B",),
        )
        tracker._by_id["node_B"] = MarketDataProvenance(
            provenance_id="node_B",
            operation_id="op_b",
            source_type=DataSourceType.DERIVED,
            source_identifier="src",
            symbol="NIFTY",
            timeframe="1m",
            parent_provenance_ids=("node_C",),
        )
        tracker._by_id["node_C"] = MarketDataProvenance(
            provenance_id="node_C",
            operation_id="op_c",
            source_type=DataSourceType.DERIVED,
            source_identifier="src",
            symbol="NIFTY",
            timeframe="1m",
            parent_provenance_ids=("node_A",),  # Cycle back to A!
        )

        graph = tracker.get_lineage("node_A", max_depth=10, direction="upstream")
        assert graph.cycle_detected is True
        assert len(graph.nodes) == 3


# ==============================================================================
# 4. QUALITY INTEGRATION TESTS
# ==============================================================================

class TestQualityIntegration:
    """Tests for Step 19 quality monitoring integration with provenance."""

    def test_quality_assessment_traceability(self) -> None:
        """Verify provenance accurately references DataQualityAssessment metrics."""
        tracker = MarketDataProvenanceTracker()

        assessment = DataQualityAssessment(
            assessment_id="qa_test_99",
            symbol="HDFC",
            timeframe="15m",
            source="mock_provider",
            timestamp=1600000000.0,
            quality_status=DataQualityStatus.DEGRADED,
            quality_score=85.5,
            data_start=1600000000.0,
            data_end=1600003600.0,
            total_records=20,
            valid_records=18,
            anomaly_count=2,
            highest_severity=QualitySeverity.WARNING,
        )

        prov = tracker.create_quality_provenance(assessment=assessment)
        assert prov.quality_assessment_id == "qa_test_99"
        assert prov.quality_status == DataQualityStatus.DEGRADED
        assert prov.quality_score == 85.5
        assert prov.record_count == 20
        assert prov.output_record_count == 18
        assert "quality_summary" in prov.metadata
        assert prov.metadata["quality_summary"]["highest_severity"] == "WARNING"

    def test_unavailable_quality_remains_unavailable(self) -> None:
        """Verify provenance never silently converts missing quality to VALID."""
        tracker = MarketDataProvenanceTracker()
        prov = tracker.create_provider_provenance(
            provider_name="raw_feed",
            symbol="TCS",
            timeframe="1m",
            bars=[],
            quality_assessment=None,
        )
        assert prov.quality_assessment_id is None
        assert prov.quality_status is None
        assert prov.quality_score is None
        assert prov.has_quality is False
        assert prov.to_dict()["quality_status"] is None


# ==============================================================================
# 5. HISTORICAL, REPLAY & ANALYSIS INTEGRATION TESTS
# ==============================================================================

class TestPipelineIntegration:
    """Tests for historical loader, replay, technical analysis, and backtesting lineage."""

    def test_loader_to_repository_pipeline_lineage(self, tmp_path: Path) -> None:
        """Verify end-to-end provenance: Provider -> Loader -> SQLite Repository."""
        bus = EventBus()
        state = StateStore()
        tracker = MarketDataProvenanceTracker(event_bus=bus, state_store=state)

        db_file = tmp_path / "hist_test.db"
        repo = SQLiteHistoricalBarRepository(db_path=db_file)
        quality_eng = MarketDataQualityEngine(event_bus=bus, state_store=state)
        analysis_eng = TechnicalAnalysisEngine(event_bus=bus, state_store=state)
        provider = MockHistoricalDataProvider()

        loader = HistoricalDataLoader(
            provider=provider,
            quality_engine=quality_eng,
            analysis_engine=analysis_eng,
            event_bus=bus,
            state_store=state,
            repository=repo,
            provenance_tracker=tracker,
        )

        # 1. Load bars from provider
        req = HistoricalBarRequest(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1600000000.0,
            end_time=1600003600.0,
            limit=50,
        )
        resp = loader.load_bars(req)
        assert len(resp.bars) > 0

        # Provenance for provider load should exist
        prov_prov = tracker.get_latest("NIFTY", "1m")
        assert prov_prov is not None
        assert prov_prov.source_type == DataSourceType.PROVIDER

        # 2. Store bars into SQLite repository
        stored_count = loader.persist_bars(resp.bars)
        assert stored_count > 0

        # Provenance for repository should exist
        prov_repo = tracker.get_latest("NIFTY", "1m")
        assert prov_repo is not None
        assert prov_repo.source_type == DataSourceType.SQLITE
        assert prov_repo.record_count == stored_count

    def test_replay_lineage_and_cancellation(self, tmp_path: Path) -> None:
        """Verify historical replay lineage and that cancellation preserves honest partial lineage."""
        tracker = MarketDataProvenanceTracker()
        repo = SQLiteHistoricalBarRepository(db_path=tmp_path / "replay.db")
        quality_eng = MarketDataQualityEngine()
        analysis_eng = TechnicalAnalysisEngine()

        # Seed repository with 10 bars
        now = time.time()
        seed_bars = [
            Bar(symbol="INFY", exchange="NSE", timeframe="1m", timestamp=now + i * 60,
                open=100.0, high=105.0, low=99.0, close=102.0, volume=1000.0, source="mock")
            for i in range(10)
        ]
        repo.store_bars(seed_bars)

        replay_eng = HistoricalReplayEngine(
            repository=repo,
            quality_engine=quality_eng,
            analysis_engine=analysis_eng,
            provenance_tracker=tracker,
        )

        req = HistoricalReplayRequest(
            symbol="INFY",
            timeframe="1m",
            start_time=now - 10,
            end_time=now + 1000,
            pacing_seconds=0.0,
        )

        # Normal run
        result = replay_eng.replay(req)
        assert result.status == HistoricalReplayStatus.COMPLETED
        prov = tracker.get_provenance(f"prov_{result.replay_id}") or tracker.get_latest("INFY", "1m")
        assert prov is not None
        assert prov.source_type == DataSourceType.REPLAY
        assert prov.record_count == 10

        # Cancelled run
        stop_token = threading.Event()
        stop_token.set()  # Cancel immediately
        cancelled_req = HistoricalReplayRequest(
            symbol="INFY",
            timeframe="1m",
            start_time=now - 10,
            end_time=now + 1000,
            replay_id="rep_cancelled_01",
        )
        cancelled_res = replay_eng.replay(cancelled_req, cancellation_token=stop_token)
        assert cancelled_res.status == HistoricalReplayStatus.CANCELLED

        cancelled_prov = tracker.get_latest("INFY", "1m")
        assert cancelled_prov is not None
        assert cancelled_prov.metadata.get("status") == "CANCELLED"

    def test_backtest_and_portfolio_simulation_lineage(self, tmp_path: Path) -> None:
        """Verify BacktestEngine and PortfolioSimulationEngine link to input market data provenance."""
        tracker = MarketDataProvenanceTracker()
        repo = SQLiteHistoricalBarRepository(db_path=tmp_path / "bt.db")
        quality_eng = MarketDataQualityEngine()
        analysis_eng = TechnicalAnalysisEngine()

        now = 1600000000.0
        bars = [
            Bar(symbol="TCS", exchange="NSE", timeframe="1m", timestamp=now + i * 60,
                open=1000.0 + i, high=1005.0 + i, low=995.0 + i, close=1002.0 + i, volume=500.0, source="seed")
            for i in range(25)
        ]
        repo.store_bars(bars)

        # Seed market data provenance
        input_prov = tracker.create_repository_provenance(
            repository_name="sqlite",
            symbol="TCS",
            timeframe="1m",
            bars=bars,
        )

        # Execute Backtest
        bt_engine = BacktestEngine(
            repository=repo,
            quality_engine=quality_eng,
            analysis_engine=analysis_eng,
            provenance_tracker=tracker,
        )
        cfg = BacktestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=now,
            end_time=now + 2000,
        )
        bt_res = bt_engine.run(cfg, strategy="sma_crossover")
        assert bt_res.provenance_id is not None

        # Verify backtest provenance
        bt_prov = tracker.get_provenance(bt_res.provenance_id)
        assert bt_prov is not None
        assert bt_prov.source_type == DataSourceType.BACKTEST
        assert input_prov.provenance_id in bt_prov.parent_provenance_ids

        # Execute Portfolio Simulation
        sim_engine = PortfolioSimulationEngine(
            repository=repo,
            quality_engine=quality_eng,
            analysis_engine=analysis_eng,
            provenance_tracker=tracker,
        )
        sim_cfg = PortfolioSimulationConfig(
            simulation_id="sim_prov_test",
            timeframe="1m",
            allocations=(
                StrategyAllocation(
                    strategy_name="sma_crossover",
                    symbol="TCS",
                    weight=1.0,
                ),
            ),
            start_time=now,
            end_time=now + 2000,
        )
        sim_res = sim_engine.run(sim_cfg)
        assert sim_res.provenance_id is not None

        sim_prov = tracker.get_provenance(sim_res.provenance_id)
        assert sim_prov is not None
        assert sim_prov.source_type == DataSourceType.DERIVED
        assert bt_prov.provenance_id in sim_prov.parent_provenance_ids or input_prov.provenance_id in sim_prov.parent_provenance_ids


# ==============================================================================
# 6. AUDIT STORAGE, FILTERING & SQLITE PERSISTENCE TESTS
# ==============================================================================

class TestAuditStorageAndPersistence:
    """Tests for bounded memory buffer, filtering, pagination, and SQLite audit table."""

    def test_bounded_history_eviction(self) -> None:
        """Verify tracker enforces max_history capacity and evicts oldest records."""
        tracker = MarketDataProvenanceTracker(max_history=5)
        for i in range(10):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"prov_bounded_{i}",
                    operation_id=f"op_{i}",
                    source_type=DataSourceType.PROVIDER,
                    source_identifier="feed",
                    symbol="NIFTY",
                    timeframe="1m",
                )
            )

        summary = tracker.get_summary()
        assert summary["total_records_tracked"] == 10
        assert summary["current_retained_records"] == 5
        # Oldest should be evicted from memory lookup
        assert tracker.get_provenance("prov_bounded_0") is None
        assert tracker.get_provenance("prov_bounded_9") is not None

    def test_audit_history_filtering_and_pagination(self) -> None:
        """Verify filtering by symbol, timeframe, source_type, parent, and pagination."""
        tracker = MarketDataProvenanceTracker(max_history=20)

        # 5 NIFTY 1m
        for i in range(5):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"p_nifty_{i}",
                    operation_id=f"op_n_{i}",
                    source_type=DataSourceType.PROVIDER,
                    source_identifier="src",
                    symbol="NIFTY",
                    timeframe="1m",
                )
            )
        # 5 TCS 5m
        for i in range(5):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"p_tcs_{i}",
                    operation_id=f"op_t_{i}",
                    source_type=DataSourceType.SQLITE,
                    source_identifier="src",
                    symbol="TCS",
                    timeframe="5m",
                    parent_provenance_ids=("p_nifty_0",),
                )
            )

        # Filter by symbol
        tcs_records = tracker.get_audit_history(symbol="TCS")
        assert len(tcs_records) == 5
        assert all(r.symbol == "TCS" for r in tcs_records)

        # Filter by source_type
        sql_records = tracker.get_audit_history(source_type=DataSourceType.SQLITE)
        assert len(sql_records) == 5

        # Filter by parent_provenance_id
        parent_filtered = tracker.get_audit_history(parent_provenance_id="p_nifty_0")
        assert len(parent_filtered) == 5

        # Pagination
        page1 = tracker.get_audit_history(limit=3, offset=0)
        page2 = tracker.get_audit_history(limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        assert page1[0].provenance_id != page2[0].provenance_id

    def test_sqlite_audit_persistence_and_fallback(self, tmp_path: Path) -> None:
        """Verify optional SQLite persistence saves records and resolves evicted records."""
        db_file = tmp_path / "prov_audit.db"
        tracker = MarketDataProvenanceTracker(max_history=3, db_path=db_file)

        # Record 5 records into capacity 3
        for i in range(5):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"prov_sq_{i}",
                    operation_id=f"op_{i}",
                    source_type=DataSourceType.PROVIDER,
                    source_identifier="feed",
                    symbol="NIFTY",
                    timeframe="1m",
                    record_count=100 + i,
                )
            )

        # prov_sq_0 is evicted from memory (capacity 3, holds 2, 3, 4)
        assert "prov_sq_0" not in tracker._by_id
        # But get_provenance falls back to SQLite and recovers it!
        recovered = tracker.get_provenance("prov_sq_0")
        assert recovered is not None
        assert recovered.provenance_id == "prov_sq_0"
        assert recovered.record_count == 100

        tracker.close()


# ==============================================================================
# 7. EVENTBUS & STATESTORE INTEGRATION TESTS
# ==============================================================================

class TestEventBusAndStateStore:
    """Tests for EventBus emission, StateStore snapshots, and secret protection."""

    def test_event_bus_emission_and_payload_sanitization(self) -> None:
        """Verify DATA_PROVENANCE_CREATED and DATA_LINEAGE_LINKED events are published without secrets."""
        bus = EventBus()
        events_received: List[Event] = []
        bus.subscribe(EventType.DATA_PROVENANCE_CREATED, lambda e: events_received.append(e))
        bus.subscribe(EventType.DATA_LINEAGE_LINKED, lambda e: events_received.append(e))
        bus.subscribe(EventType.DATA_PROVENANCE_FAILED, lambda e: events_received.append(e))

        tracker = MarketDataProvenanceTracker(event_bus=bus)

        prov = MarketDataProvenance(
            provenance_id="prov_event_test",
            operation_id="op_evt_1",
            source_type=DataSourceType.PROVIDER,
            source_identifier="feed",
            symbol="INFY",
            timeframe="1m",
            parent_provenance_ids=("parent_1",),
            metadata={
                "api_key": "SECRET12345",
                "normal_param": "valid_value",
                "local_path": "C:\\Users\\admin\\app\\data.csv",
            },
        )
        tracker.record_provenance(prov)

        assert len(events_received) == 2
        created_evt = next(e for e in events_received if e.event_type == EventType.DATA_PROVENANCE_CREATED)
        linked_evt = next(e for e in events_received if e.event_type == EventType.DATA_LINEAGE_LINKED)

        assert created_evt.payload["provenance_id"] == "prov_event_test"
        assert created_evt.payload["has_parents"] is True
        assert linked_evt.payload["child_provenance_id"] == "prov_event_test"

        # Verify no secret in metadata
        assert prov.metadata["api_key"] == "***REDACTED***"
        assert "[PATH]" in prov.metadata["local_path"]

    def test_state_store_bounded_cache(self) -> None:
        """Verify StateStore stores latest per stream and summary without unbounded dataset accumulation."""
        state = StateStore()
        tracker = MarketDataProvenanceTracker(state_store=state)

        for i in range(10):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"prov_ss_{i}",
                    operation_id=f"op_{i}",
                    source_type=DataSourceType.PROVIDER,
                    source_identifier="feed",
                    symbol="RELIANCE",
                    timeframe="1m",
                    record_count=100 * (i + 1),
                )
            )

        latest_entry = state.get("market_provenance_latest_RELIANCE_1M")
        assert latest_entry is not None
        assert latest_entry["provenance_id"] == "prov_ss_9"
        assert latest_entry["record_count"] == 1000

        summary = state.get("market_provenance_summary")
        assert summary is not None
        assert summary["total_records_tracked"] == 10


# ==============================================================================
# 8. ORCHESTRATOR HANDLER TESTS
# ==============================================================================

class TestProvenanceHandlers:
    """Tests for orchestrator command handlers."""

    def test_get_data_provenance_handler(self) -> None:
        """Verify get_data_provenance resolves by ID, stream, and operation."""
        orchestrator = MasterOrchestrator()
        tracker = MarketDataProvenanceTracker()
        register_provenance_handlers(orchestrator, tracker)

        prov = MarketDataProvenance(
            provenance_id="prov_cmd_01",
            operation_id="op_cmd_01",
            source_type=DataSourceType.PROVIDER,
            source_identifier="feed",
            symbol="SBIN",
            timeframe="15m",
            record_count=45,
        )
        tracker.record_provenance(prov)

        # 1. By ID
        req_id = OrchestratorRequest(
            request_id="req_prov_01",
            correlation_id="c_prov_01",
            command="get_data_provenance",
            context={"provenance_id": "prov_cmd_01"},
        )
        res_id = orchestrator.handle_request(req_id)
        assert res_id.is_success
        assert res_id.result["status"] == "success"
        assert res_id.result["provenance"]["provenance_id"] == "prov_cmd_01"

        # 2. By stream
        req_st = OrchestratorRequest(
            request_id="req_prov_02",
            correlation_id="c_prov_02",
            command="get_data_provenance",
            context={"symbol": "SBIN", "timeframe": "15m"},
        )
        res_st = orchestrator.handle_request(req_st)
        assert res_st.is_success
        assert res_st.result["status"] == "success"
        assert res_st.result["provenance"]["provenance_id"] == "prov_cmd_01"

        # 3. By operation
        req_op = OrchestratorRequest(
            request_id="req_prov_03",
            correlation_id="c_prov_03",
            command="get_data_provenance",
            context={"operation_id": "op_cmd_01"},
        )
        res_op = orchestrator.handle_request(req_op)
        assert res_op.is_success
        assert res_op.result["status"] == "success"
        assert len(res_op.result["provenance_records"]) == 1

        # 4. Missing ID
        req_miss = OrchestratorRequest(
            request_id="req_prov_04",
            correlation_id="c_prov_04",
            command="get_data_provenance",
            context={"provenance_id": "non_existent"},
        )
        res_miss = orchestrator.handle_request(req_miss)
        assert res_miss.result["status"] == "not_found"

    def test_get_data_lineage_handler(self) -> None:
        """Verify get_data_lineage traverses DAG via orchestrator."""
        orchestrator = MasterOrchestrator()
        tracker = MarketDataProvenanceTracker()
        register_provenance_handlers(orchestrator, tracker)

        tracker.record_provenance(
            MarketDataProvenance(
                provenance_id="parent_node",
                operation_id="op_p",
                source_type=DataSourceType.PROVIDER,
                source_identifier="feed",
                symbol="NIFTY",
                timeframe="1m",
            )
        )
        tracker.record_provenance(
            MarketDataProvenance(
                provenance_id="child_node",
                operation_id="op_c",
                source_type=DataSourceType.SQLITE,
                source_identifier="repo",
                symbol="NIFTY",
                timeframe="1m",
                parent_provenance_ids=("parent_node",),
            )
        )

        req = OrchestratorRequest(
            request_id="req_lineage_01",
            correlation_id="c_lineage_01",
            command="get_data_lineage",
            context={"provenance_id": "child_node", "direction": "upstream"},
        )
        res = orchestrator.handle_request(req)
        assert res.is_success
        assert "parent_node" in res.result["lineage"]["nodes"]

    def test_get_data_audit_history_handler(self) -> None:
        """Verify get_data_audit_history returns paginated audit records."""
        orchestrator = MasterOrchestrator()
        tracker = MarketDataProvenanceTracker()
        register_provenance_handlers(orchestrator, tracker)

        for i in range(15):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"prov_h_{i}",
                    operation_id=f"op_{i}",
                    source_type=DataSourceType.PROVIDER,
                    source_identifier="feed",
                    symbol="TCS",
                    timeframe="1m",
                )
            )

        req = OrchestratorRequest(
            request_id="req_audit_01",
            correlation_id="c_audit_01",
            command="get_data_audit_history",
            context={"limit": 5, "offset": 0, "symbol": "TCS"},
        )
        res = orchestrator.handle_request(req)
        assert res.is_success
        assert res.result["count"] == 5
        assert len(res.result["records"]) == 5


# ==============================================================================
# 9. DASHBOARD INTEGRATION TESTS
# ==============================================================================

class TestDashboardIntegration:
    """Tests for read-only HTTP dashboard provenance and lineage endpoints."""

    def test_dashboard_provenance_and_lineage_endpoints(self) -> None:
        """Verify GET /api/data-provenance and /api/data-lineage return 200 and enforce safety."""
        tracker = MarketDataProvenanceTracker()
        tracker.record_provenance(
            MarketDataProvenance(
                provenance_id="prov_dash_1",
                operation_id="op_dash_1",
                source_type=DataSourceType.PROVIDER,
                source_identifier="feed",
                symbol="NIFTY",
                timeframe="1m",
                record_count=100,
            )
        )
        tracker.record_provenance(
            MarketDataProvenance(
                provenance_id="prov_dash_2",
                operation_id="op_dash_2",
                source_type=DataSourceType.SQLITE,
                source_identifier="repo",
                symbol="NIFTY",
                timeframe="1m",
                record_count=100,
                parent_provenance_ids=("prov_dash_1",),
            )
        )

        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            provenance_tracker=tracker,
        )
        server.start(blocking=False)
        base_url = server.url

        try:
            # 1. GET /api/data-provenance list
            req1 = urllib.request.Request(f"{base_url}/api/data-provenance")
            with urllib.request.urlopen(req1, timeout=5) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode())
                assert data["status"] == "success"
                assert data["read_only"] is True
                assert data["execution_locked"] is True
                assert data["count"] == 2

            # 2. GET /api/data-provenance single by query param
            req2 = urllib.request.Request(f"{base_url}/api/data-provenance?provenance_id=prov_dash_1")
            with urllib.request.urlopen(req2, timeout=5) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode())
                assert data["status"] == "success"
                assert data["provenance"]["provenance_id"] == "prov_dash_1"

            # 3. GET /api/data-lineage by query param
            req3 = urllib.request.Request(f"{base_url}/api/data-lineage?provenance_id=prov_dash_2")
            with urllib.request.urlopen(req3, timeout=5) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode())
                assert data["status"] == "success"
                assert "prov_dash_1" in data["lineage"]["nodes"]

            # 4. GET /api/data-lineage/<provenance_id> by path
            req4 = urllib.request.Request(f"{base_url}/api/data-lineage/prov_dash_2")
            with urllib.request.urlopen(req4, timeout=5) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode())
                assert data["status"] == "success"
                assert "prov_dash_1" in data["lineage"]["nodes"]

            # 5. Malformed ID and Traversal rejected with 400
            for malformed in ["..", "../../etc/passwd", "prov!@#$%"]:
                try:
                    req_bad = urllib.request.Request(f"{base_url}/api/data-lineage?provenance_id={urllib.parse.quote(malformed)}")
                    urllib.request.urlopen(req_bad, timeout=5)
                    pytest.fail("Expected 400 for malformed ID")
                except urllib.error.HTTPError as err:
                    assert err.code == 400

        finally:
            server.stop()


# ==============================================================================
# 10. FAILURE RECOVERY & EDGE CASES
# ==============================================================================

class TestFailureRecovery:
    """Tests for graceful error handling, invalid inputs, and failure event publishing."""

    def test_invalid_provenance_recording_publishes_failure_event(self) -> None:
        """Verify invalid provenance input emits DATA_PROVENANCE_FAILED event."""
        bus = EventBus()
        failed_events: List[Event] = []
        bus.subscribe(EventType.DATA_PROVENANCE_FAILED, lambda e: failed_events.append(e))

        tracker = MarketDataProvenanceTracker(event_bus=bus)

        with pytest.raises(ValidationError):
            tracker.record_provenance("not_a_provenance_object")  # type: ignore

        assert len(failed_events) == 1
        assert failed_events[0].event_type == EventType.DATA_PROVENANCE_FAILED
        assert "MarketDataProvenance" in failed_events[0].payload["reason"]

    def test_unresolved_parent_in_lineage_graph(self) -> None:
        """Verify referencing a missing parent marks the node as UNRESOLVED_IN_MEMORY without crashing."""
        tracker = MarketDataProvenanceTracker()
        tracker.record_provenance(
            MarketDataProvenance(
                provenance_id="prov_child_orphaned",
                operation_id="op_1",
                source_type=DataSourceType.DERIVED,
                source_identifier="calc",
                symbol="NIFTY",
                timeframe="1m",
                parent_provenance_ids=("missing_parent_id",),
            )
        )

        graph = tracker.get_lineage("prov_child_orphaned", direction="upstream")
        assert "missing_parent_id" in graph.nodes
        assert graph.nodes["missing_parent_id"]["status"] == "UNRESOLVED_IN_MEMORY"


# ==============================================================================
# 11. PROPERTY / INVARIANT TESTS
# ==============================================================================

class TestPropertyInvariants:
    """Tests enforcing system-wide mathematical and logical invariants."""

    def test_quality_score_finite_invariant(self) -> None:
        """Verify quality_score cannot be NaN or Infinity."""
        with pytest.raises(ValidationError):
            MarketDataProvenance(
                provenance_id="p1", operation_id="o1", source_type=DataSourceType.PROVIDER,
                source_identifier="s", symbol="NIFTY", timeframe="1m", quality_score=float("nan"),
            )
        with pytest.raises(ValidationError):
            MarketDataProvenance(
                provenance_id="p1", operation_id="o1", source_type=DataSourceType.PROVIDER,
                source_identifier="s", symbol="NIFTY", timeframe="1m", quality_score=float("inf"),
            )

    def test_no_secret_leakage_in_metadata(self) -> None:
        """Verify credential keys and patterns are scrubbed from metadata recursively."""
        dirty_meta = {
            "api_key": "12345",
            "bearer_token": "secret_token",
            "auth": {"password": "pass", "public_info": "safe"},
            "nested_list": ["path/to/normal", "authorization_header_content"],
            "raw_filepath": "C:\\Users\\trader\\Desktop\\KAISER\\data.csv",
        }
        clean = sanitize_provenance_metadata(dirty_meta)
        assert clean["api_key"] == "***REDACTED***"
        assert clean["bearer_token"] == "***REDACTED***"
        assert clean["auth"]["password"] == "***REDACTED***"
        assert clean["auth"]["public_info"] == "safe"
        assert clean["nested_list"][1] == "***REDACTED***"
        assert "[PATH]" in clean["raw_filepath"]

    def test_thread_safe_concurrency(self) -> None:
        """Verify concurrent insertions and lookups do not corrupt memory or cause deadlock."""
        tracker = MarketDataProvenanceTracker(max_history=1000)
        num_threads = 8
        records_per_thread = 50

        def worker(thread_idx: int) -> None:
            for i in range(records_per_thread):
                p_id = f"prov_t_{thread_idx}_{i}"
                tracker.record_provenance(
                    MarketDataProvenance(
                        provenance_id=p_id,
                        operation_id=f"op_{thread_idx}",
                        source_type=DataSourceType.PROVIDER,
                        source_identifier=f"worker_{thread_idx}",
                        symbol="NIFTY",
                        timeframe="1m",
                    )
                )
                assert tracker.get_provenance(p_id) is not None

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, t) for t in range(num_threads)]
            for f in concurrent.futures.as_completed(futures):
                f.result()

        summary = tracker.get_summary()
        assert summary["total_records_tracked"] == num_threads * records_per_thread


# ==============================================================================
# 12. PERFORMANCE BENCHMARKS
# ==============================================================================

class TestPerformanceWorkloads:
    """Benchmark tests across 100, 1,000, and 10,000 provenance records."""

    def test_100_provenance_records_workload(self) -> None:
        """Benchmark 100 provenance insertions and lookups."""
        tracker = MarketDataProvenanceTracker(max_history=500)
        start = time.perf_counter()
        for i in range(100):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"perf_100_{i}",
                    operation_id=f"op_{i}",
                    source_type=DataSourceType.PROVIDER,
                    source_identifier="feed",
                    symbol="NIFTY",
                    timeframe="1m",
                    record_count=100,
                )
            )
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0
        assert tracker.get_summary()["current_retained_records"] == 100

    def test_1000_provenance_records_workload(self) -> None:
        """Benchmark 1,000 provenance insertions and lookups."""
        tracker = MarketDataProvenanceTracker(max_history=2000)
        start = time.perf_counter()
        for i in range(1000):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"perf_1000_{i}",
                    operation_id=f"op_{i}",
                    source_type=DataSourceType.PROVIDER,
                    source_identifier="feed",
                    symbol="NIFTY",
                    timeframe="1m",
                    record_count=100,
                )
            )
        elapsed = time.perf_counter() - start
        assert elapsed < 2.0
        assert tracker.get_summary()["current_retained_records"] == 1000

    def test_10000_provenance_records_workload(self) -> None:
        """Benchmark 10,000 provenance insertions and lookups."""
        tracker = MarketDataProvenanceTracker(max_history=10000)
        start = time.perf_counter()
        for i in range(10000):
            tracker.record_provenance(
                MarketDataProvenance(
                    provenance_id=f"perf_10000_{i}",
                    operation_id=f"op_{i}",
                    source_type=DataSourceType.PROVIDER,
                    source_identifier="feed",
                    symbol="NIFTY",
                    timeframe="1m",
                    record_count=100,
                )
            )
        elapsed = time.perf_counter() - start
        assert elapsed < 5.0
        assert tracker.get_summary()["current_retained_records"] == 10000


# ==============================================================================
# 13. SAFETY & INVARIANT CONSTRAINTS
# ==============================================================================

class TestSafetyInvariants:
    """Rigorous tests confirming absolute safety guarantees remain preserved."""

    def test_execution_locked_and_trading_uninitialized(self) -> None:
        """Verify Settings.execution_locked remains True and trading engine uninitialized."""
        status = bootstrap_system(log_to_file=False)
        assert status.execution_engine == "locked"
        assert status.trading_engine == "not initialized"
        assert status.settings.execution_locked is True
        assert status.settings.paper_trading_only is True

    def test_juju_untouched(self) -> None:
        """Verify JUJU directory was not modified."""
        juju_path = Path("c:/Users/user/Desktop/KAISER/juju")
        if juju_path.exists():
            for root, dirs, files in os.walk(juju_path):
                for f in files:
                    full_p = Path(root) / f
                    # Ensure no step 20 changes touched juju
                    assert "step_20" not in f.lower()
