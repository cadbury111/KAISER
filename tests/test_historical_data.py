"""KAISER — Historical Market Data Integration Tests.

Deterministic, offline tests verifying:
1. HistoricalBarRequest validation, normalization, and bounds.
2. HistoricalDataProvider interface and MockHistoricalDataProvider capabilities.
3. Timestamp/timezone normalization and timeframe canonicalization.
4. Deterministic synthetic bar generation and OHLC relational invariants.
5. Fault injection: empty responses, provider errors, unsupported requests, rate limits.
6. Zero-volume bar preservation vs missing interval (gap) detection without fabrication.
7. MarketDataQualityEngine integration and strict isolation of rejected bars.
8. Idempotent ingestion and duplicate detection.
9. Pagination and bounded batching.
10. MasterOrchestrator 'load_historical_data' command execution.
11. Offline startup and trading execution safety invariants.
"""

import time
import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.models import SUPPORTED_TIMEFRAMES, normalize_timeframe
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.handlers import register_historical_handlers
from app.market.historical.loader import HistoricalDataLoader
from app.market.historical.mock import MockHistoricalDataProvider, get_base_price_for_symbol
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalIngestionResult,
    HistoricalProviderStatus,
)
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.market.quality.status import DataQualityStatus
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest, RequestStatus


# ==============================================================================
# 1. HistoricalBarRequest & Model Tests
# ==============================================================================

class TestHistoricalBarRequest:
    """Tests for HistoricalBarRequest validation and normalization."""

    def test_valid_request_creation(self):
        req = HistoricalBarRequest(
            symbol="reliance",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            limit=500,
        )
        assert req.symbol == "RELIANCE"
        assert req.timeframe == "15m"
        assert req.start_time == 1700000000.0
        assert req.end_time == 1700003600.0
        assert req.limit == 500
        assert "T" in req.start_iso
        assert "T" in req.end_iso

    def test_timeframe_canonicalization(self):
        req_1m = HistoricalBarRequest(symbol="NIFTY", timeframe="1M", start_time=100.0, end_time=200.0)
        assert req_1m.timeframe == "1m"

        req_1h = HistoricalBarRequest(symbol="NIFTY", timeframe="1H", start_time=100.0, end_time=200.0)
        assert req_1h.timeframe == "1h"

        req_1d = HistoricalBarRequest(symbol="NIFTY", timeframe="1d", start_time=100.0, end_time=200.0)
        assert req_1d.timeframe == "1D"

    def test_invalid_timeframe_raises_validation_error(self):
        with pytest.raises(ValidationError, match="Unsupported timeframe"):
            HistoricalBarRequest(symbol="NIFTY", timeframe="3m", start_time=100.0, end_time=200.0)

    def test_invalid_symbols_raise_validation_error(self):
        with pytest.raises(ValidationError, match="symbol must be a non-empty string"):
            HistoricalBarRequest(symbol="", timeframe="1m", start_time=100.0, end_time=200.0)

        with pytest.raises(ValidationError, match="symbol must be a non-empty string"):
            HistoricalBarRequest(symbol="   ", timeframe="1m", start_time=100.0, end_time=200.0)

    def test_invalid_timestamps_raise_validation_error(self):
        with pytest.raises(ValidationError, match="start_time must be positive"):
            HistoricalBarRequest(symbol="NIFTY", timeframe="1m", start_time=-10.0, end_time=200.0)

        with pytest.raises(ValidationError, match="end_time must be positive"):
            HistoricalBarRequest(symbol="NIFTY", timeframe="1m", start_time=100.0, end_time=0.0)

        with pytest.raises(ValidationError, match="cannot be earlier than start_time"):
            HistoricalBarRequest(symbol="NIFTY", timeframe="1m", start_time=200.0, end_time=100.0)

    def test_bounded_limit_validation(self):
        with pytest.raises(ValidationError, match="limit must be a positive integer"):
            HistoricalBarRequest(symbol="NIFTY", timeframe="1m", start_time=100.0, end_time=200.0, limit=0)

        with pytest.raises(ValidationError, match="limit exceeds maximum bound"):
            HistoricalBarRequest(symbol="NIFTY", timeframe="1m", start_time=100.0, end_time=200.0, limit=6000)

    def test_to_dict_serialization(self):
        req = HistoricalBarRequest(symbol="INFY", timeframe="5m", start_time=100.0, end_time=500.0, limit=100)
        d = req.to_dict()
        assert d["symbol"] == "INFY"
        assert d["timeframe"] == "5m"
        assert d["start_time"] == 100.0
        assert d["end_time"] == 500.0
        assert d["limit"] == 100
        assert "start_iso" in d
        assert "end_iso" in d


# ==============================================================================
# 2. MockHistoricalDataProvider Tests
# ==============================================================================

class TestMockHistoricalDataProvider:
    """Tests for MockHistoricalDataProvider deterministic bar generation and fault injection."""

    def test_deterministic_bar_generation(self):
        provider = MockHistoricalDataProvider(name="test_mock")
        req = HistoricalBarRequest(
            symbol="RELIANCE",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700000600.0,  # 10 minutes = 11 intervals (0 to 10 inclusive)
            limit=50,
        )
        resp = provider.get_historical_bars(req)
        assert resp.status == HistoricalProviderStatus.SUCCESS
        assert resp.total_bars == 11
        assert not resp.is_empty
        assert resp.provider_name == "test_mock"

        # Check OHLC relational invariants on every bar
        for bar in resp.bars:
            assert bar.symbol == "RELIANCE"
            assert bar.timeframe == "1m"
            assert bar.high >= bar.open
            assert bar.high >= bar.close
            assert bar.high >= bar.low
            assert bar.low <= bar.open
            assert bar.low <= bar.close
            assert bar.volume > 0

    def test_unsupported_symbol(self):
        provider = MockHistoricalDataProvider(supported_symbols=("RELIANCE", "NIFTY"))
        req = HistoricalBarRequest(symbol="TCS", timeframe="1m", start_time=100.0, end_time=200.0)
        resp = provider.get_historical_bars(req)
        assert resp.status == HistoricalProviderStatus.UNSUPPORTED_REQUEST
        assert resp.total_bars == 0
        assert "not supported" in (resp.error_message or "")

    def test_unsupported_timeframe(self):
        provider = MockHistoricalDataProvider(supported_timeframes=("1m", "5m"))
        req = HistoricalBarRequest(symbol="NIFTY", timeframe="1D", start_time=100.0, end_time=200.0)
        resp = provider.get_historical_bars(req)
        assert resp.status == HistoricalProviderStatus.UNSUPPORTED_REQUEST
        assert resp.total_bars == 0

    def test_forced_status_fault_injection(self):
        provider = MockHistoricalDataProvider()
        provider.force_status = HistoricalProviderStatus.PROVIDER_ERROR
        provider.force_error_message = "Simulated upstream gateway timeout"

        req = HistoricalBarRequest(symbol="NIFTY", timeframe="1m", start_time=100.0, end_time=200.0)
        resp = provider.get_historical_bars(req)
        assert resp.status == HistoricalProviderStatus.PROVIDER_ERROR
        assert resp.error_message == "Simulated upstream gateway timeout"
        assert resp.total_bars == 0

    def test_forced_empty_fault_injection(self):
        provider = MockHistoricalDataProvider()
        provider.force_empty = True
        req = HistoricalBarRequest(symbol="NIFTY", timeframe="1m", start_time=100.0, end_time=200.0)
        resp = provider.get_historical_bars(req)
        assert resp.status == HistoricalProviderStatus.EMPTY
        assert resp.total_bars == 0
        assert resp.is_empty

    def test_canned_bars_retrieval(self):
        provider = MockHistoricalDataProvider()
        canned = [
            Bar(symbol="NIFTY", exchange="NSE", timeframe="5m", timestamp=1000.0, open=24000.0, high=24100.0, low=23950.0, close=24050.0, volume=500.0),
            Bar(symbol="NIFTY", exchange="NSE", timeframe="5m", timestamp=1300.0, open=24050.0, high=24150.0, low=24000.0, close=24120.0, volume=600.0),
            Bar(symbol="NIFTY", exchange="NSE", timeframe="5m", timestamp=1600.0, open=24120.0, high=24200.0, low=24080.0, close=24180.0, volume=700.0),
        ]
        provider.set_canned_bars("NIFTY", "5m", canned)

        req = HistoricalBarRequest(symbol="NIFTY", timeframe="5m", start_time=1000.0, end_time=1300.0)
        resp = provider.get_historical_bars(req)
        assert resp.status == HistoricalProviderStatus.SUCCESS
        assert resp.total_bars == 2
        assert resp.bars[0].timestamp == 1000.0
        assert resp.bars[1].timestamp == 1300.0

    def test_pagination_simulation(self):
        provider = MockHistoricalDataProvider()
        provider.simulate_pagination = True
        provider.page_size = 5

        req = HistoricalBarRequest(symbol="RELIANCE", timeframe="1m", start_time=1000.0, end_time=2000.0, limit=20)
        resp_page1 = provider.get_historical_bars(req)

        assert resp_page1.status == HistoricalProviderStatus.PARTIAL
        assert resp_page1.total_bars == 5
        assert resp_page1.next_page_token == "offset_5"

        # Fetch page 2
        req_page2 = HistoricalBarRequest(
            symbol="RELIANCE",
            timeframe="1m",
            start_time=1000.0,
            end_time=2000.0,
            limit=20,
            page_token=resp_page1.next_page_token,
        )
        resp_page2 = provider.get_historical_bars(req_page2)
        assert resp_page2.total_bars == 5
        assert resp_page2.bars[0].timestamp > resp_page1.bars[-1].timestamp


# ==============================================================================
# 3. HistoricalDataLoader & Quality Engine Integration Tests
# ==============================================================================

class TestHistoricalDataLoader:
    """Tests for HistoricalDataLoader quality gating, gap detection, and series ingestion."""

    @pytest.fixture
    def setup_pipeline(self):
        event_bus = EventBus(name="test_bus")
        state_store = StateStore(name="test_store")
        quality_engine = MarketDataQualityEngine(event_bus=event_bus, state_store=state_store)
        analysis_engine = TechnicalAnalysisEngine(event_bus=event_bus, state_store=state_store)
        provider = MockHistoricalDataProvider(name="test_provider")
        loader = HistoricalDataLoader(
            provider=provider,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
            event_bus=event_bus,
            state_store=state_store,
        )
        return {
            "loader": loader,
            "provider": provider,
            "quality_engine": quality_engine,
            "analysis_engine": analysis_engine,
            "event_bus": event_bus,
            "state_store": state_store,
        }

    def test_load_and_ingest_happy_path(self, setup_pipeline):
        ctx = setup_pipeline
        loader: HistoricalDataLoader = ctx["loader"]
        analysis_engine: TechnicalAnalysisEngine = ctx["analysis_engine"]
        event_bus: EventBus = ctx["event_bus"]

        events_published = []
        event_bus.subscribe(EventType.HISTORICAL_DATA_INGESTED, lambda e: events_published.append(e))

        req = HistoricalBarRequest(
            symbol="RELIANCE",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700000000.0 + (30 * 900.0),  # 31 bars
            limit=50,
        )

        result = loader.load_and_ingest(req)

        assert result.provider_status == HistoricalProviderStatus.SUCCESS
        assert result.received_bars_count == 31
        assert result.accepted_bars_count == 31
        assert result.rejected_bars_count == 0
        assert result.duplicate_bars_count == 0
        assert result.is_fully_accepted
        assert len(events_published) == 1

        # Verify bars are in the RollingBarSeries
        series = analysis_engine.get_series("RELIANCE", "15m")
        assert series is not None
        assert series.count == 31

        # Verify indicators computed
        vec = analysis_engine.get_latest_vector("RELIANCE", "15m")
        assert vec is not None
        assert vec.bar_count == 31

    def test_zero_volume_bar_preservation(self, setup_pipeline):
        """Zero-volume bars are valid candles with partial completeness; must not be dropped."""
        ctx = setup_pipeline
        loader: HistoricalDataLoader = ctx["loader"]
        provider: MockHistoricalDataProvider = ctx["provider"]
        analysis_engine: TechnicalAnalysisEngine = ctx["analysis_engine"]

        # Inject zero-volume at bar index 2
        provider.inject_zero_volume_indices.add(2)

        req = HistoricalBarRequest(
            symbol="RELIANCE",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700000300.0,  # 6 bars (0, 1, 2, 3, 4, 5)
        )
        result = loader.load_and_ingest(req)

        assert result.received_bars_count == 6
        assert result.accepted_bars_count == 6
        assert result.zero_volume_bars_count == 1
        assert result.rejected_bars_count == 0

        # Bar was accepted into series despite zero volume
        series = analysis_engine.get_series("RELIANCE", "1m")
        assert series is not None
        assert series.count == 6

    def test_missing_interval_detection_without_fabrication(self, setup_pipeline):
        """Gaps in timestamps must be reported as missing intervals without fabricating candles."""
        ctx = setup_pipeline
        loader: HistoricalDataLoader = ctx["loader"]
        provider: MockHistoricalDataProvider = ctx["provider"]
        analysis_engine: TechnicalAnalysisEngine = ctx["analysis_engine"]

        # Inject a missing interval after index 3 (skips 3 intervals)
        provider.inject_gap_after_indices.add(3)

        req = HistoricalBarRequest(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700000600.0,
            limit=10,
        )
        result = loader.load_and_ingest(req)

        assert result.detected_gaps_count >= 1
        assert len(result.gap_details) >= 1
        assert result.gap_details[0]["missed_intervals"] >= 1

        # Check RollingBarSeries recorded the gap without filling candles
        series = analysis_engine.get_series("NIFTY", "1m")
        assert series is not None
        assert series.missing_intervals_count >= 1

    def test_quality_gate_rejects_and_isolates_corrupt_records(self, setup_pipeline):
        """Corrupt OHLC records must be rejected and must NEVER enter RollingBarSeries."""
        ctx = setup_pipeline
        loader: HistoricalDataLoader = ctx["loader"]
        analysis_engine: TechnicalAnalysisEngine = ctx["analysis_engine"]

        # Prepare raw records containing 2 valid bars and 1 corrupt bar (high < low)
        raw_records = [
            {"symbol": "TCS", "timeframe": "1m", "timestamp": 1000.0, "open": 3900.0, "high": 3920.0, "low": 3890.0, "close": 3910.0, "volume": 100.0},
            {"symbol": "TCS", "timeframe": "1m", "timestamp": 1060.0, "open": 3910.0, "high": 3850.0, "low": 3950.0, "close": 3900.0, "volume": 100.0},  # Corrupt: high < low
            {"symbol": "TCS", "timeframe": "1m", "timestamp": 1120.0, "open": 3900.0, "high": 3930.0, "low": 3895.0, "close": 3925.0, "volume": 120.0},
        ]

        req = HistoricalBarRequest(symbol="TCS", timeframe="1m", start_time=1000.0, end_time=1120.0)
        result = loader.ingest_raw_records(raw_records, request=req)

        assert result.received_bars_count == 3
        assert result.accepted_bars_count == 2
        assert result.rejected_bars_count == 1
        assert len(result.rejection_reasons) >= 1
        assert "normalization failed" in result.rejection_reasons[0]

        # Verify only 2 bars entered the series
        series = analysis_engine.get_series("TCS", "1m")
        assert series is not None
        assert series.count == 2

    def test_symbol_and_timeframe_mismatch_rejection(self, setup_pipeline):
        """Bars that do not match the requested stream must be rejected."""
        ctx = setup_pipeline
        loader: HistoricalDataLoader = ctx["loader"]
        provider: MockHistoricalDataProvider = ctx["provider"]

        # Set canned bars with mismatched symbol
        canned = [
            Bar(symbol="INFY", exchange="NSE", timeframe="1m", timestamp=1000.0, open=1800.0, high=1810.0, low=1795.0, close=1805.0, volume=100.0),
        ]
        # Register canned under RELIANCE key
        provider.set_canned_bars("RELIANCE", "1m", canned)

        req = HistoricalBarRequest(symbol="RELIANCE", timeframe="1m", start_time=1000.0, end_time=1200.0)
        result = loader.load_and_ingest(req)

        assert result.received_bars_count == 1
        assert result.accepted_bars_count == 0
        assert result.rejected_bars_count == 1
        assert "Symbol/Timeframe mismatch" in result.rejection_reasons[0]

    def test_idempotent_ingestion(self, setup_pipeline):
        """Ingesting the same historical range twice must be idempotent without duplicating bars."""
        ctx = setup_pipeline
        loader: HistoricalDataLoader = ctx["loader"]
        analysis_engine: TechnicalAnalysisEngine = ctx["analysis_engine"]

        req = HistoricalBarRequest(
            symbol="HDFCBANK",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700000000.0 + (10 * 900.0),  # 11 bars
        )

        # Ingestion 1
        res1 = loader.load_and_ingest(req)
        assert res1.accepted_bars_count == 11
        assert res1.duplicate_bars_count == 0

        series = analysis_engine.get_series("HDFCBANK", "15m")
        assert series is not None
        assert series.count == 11

        # Ingestion 2 (identical request)
        res2 = loader.load_and_ingest(req)
        assert res2.accepted_bars_count == 0
        assert res2.duplicate_bars_count == 11

        # Series is protected by quality gate: count remains unchanged
        assert series.count == 11

        # Verify series-level duplicate rejection directly
        canned_bar = series.get_bars()[-1]
        rejected_by_series = series.add_bar(canned_bar)
        assert rejected_by_series is False
        assert series.duplicate_count == 1

    def test_automatic_pagination_in_load_bars(self, setup_pipeline):
        """Verifies paginate_all=True collects all pages seamlessly."""
        ctx = setup_pipeline
        loader: HistoricalDataLoader = ctx["loader"]
        provider: MockHistoricalDataProvider = ctx["provider"]

        provider.simulate_pagination = True
        provider.page_size = 10

        req = HistoricalBarRequest(
            symbol="RELIANCE",
            timeframe="1m",
            start_time=1000.0,
            end_time=1000.0 + (24 * 60.0),  # 25 bars across 3 pages (10, 10, 5)
            limit=30,
        )

        resp = loader.load_bars(req, paginate_all=True, max_pages=5)
        assert resp.status == HistoricalProviderStatus.SUCCESS
        assert resp.total_bars == 25

    def test_provider_error_handling(self, setup_pipeline):
        ctx = setup_pipeline
        loader: HistoricalDataLoader = ctx["loader"]
        provider: MockHistoricalDataProvider = ctx["provider"]
        event_bus: EventBus = ctx["event_bus"]

        failed_events = []
        event_bus.subscribe(EventType.HISTORICAL_DATA_FAILED, lambda e: failed_events.append(e))

        provider.force_status = HistoricalProviderStatus.PROVIDER_ERROR
        provider.force_error_message = "Rate limit exceeded on external API"

        req = HistoricalBarRequest(symbol="RELIANCE", timeframe="1m", start_time=1000.0, end_time=1200.0)
        result = loader.load_and_ingest(req)

        assert result.provider_status == HistoricalProviderStatus.PROVIDER_ERROR
        assert result.accepted_bars_count == 0
        assert len(failed_events) == 1
        assert "Rate limit exceeded" in result.rejection_reasons[0]


# ==============================================================================
# 4. Master Orchestrator Command Integration Tests
# ==============================================================================

class TestOrchestratorHistoricalIntegration:
    """Tests for MasterOrchestrator 'load_historical_data' command handler."""

    def test_load_historical_data_command_success(self):
        status = bootstrap_system()
        orchestrator: MasterOrchestrator = status.orchestrator
        assert orchestrator is not None

        req = OrchestratorRequest.create(
            command="load_historical_data",
            context={
                "symbol": "RELIANCE",
                "timeframe": "15m",
                "start_time": 1700000000.0,
                "end_time": 1700000000.0 + (10 * 900.0),
                "limit": 50,
            },
        )

        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.COMPLETED
        assert resp.is_success is True
        assert resp.result["status"] == "success"
        assert resp.result["data"]["symbol"] == "RELIANCE"
        assert resp.result["data"]["timeframe"] == "15m"
        assert resp.result["data"]["accepted_bars_count"] == 11
        assert resp.result["data"]["rejected_bars_count"] == 0

    def test_load_historical_data_command_missing_symbol_fails(self):
        status = bootstrap_system()
        orchestrator: MasterOrchestrator = status.orchestrator

        req = OrchestratorRequest.create(
            command="load_historical_data",
            context={"symbol": "", "timeframe": "15m"},
        )

        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.FAILED
        assert any("Field 'symbol' is required" in err for err in resp.errors)

    def test_load_historical_data_iso_timestamp_parsing(self):
        status = bootstrap_system()
        orchestrator: MasterOrchestrator = status.orchestrator

        req = OrchestratorRequest.create(
            command="load_historical_data",
            context={
                "symbol": "NIFTY",
                "timeframe": "1h",
                "start_iso": "2026-10-01T09:15:00Z",
                "end_iso": "2026-10-01T15:30:00Z",
                "limit": 20,
            },
        )

        resp = orchestrator.handle_request(req)
        assert resp.status == RequestStatus.COMPLETED
        assert resp.is_success is True
        assert resp.result["status"] == "success"
        assert resp.result["data"]["accepted_bars_count"] > 0


# ==============================================================================
# 5. Offline & Trading Safety Invariants Tests
# ==============================================================================

class TestSafetyAndOfflineInvariants:
    """Verifies architectural safety constraints and offline startup."""

    def test_offline_startup_without_credentials_or_network(self):
        safe_settings = Settings(
            app_env="test",
            market_data_provider="mock",
            execution_locked=True,
            paper_trading_only=True,
            voice_enabled=False,
        )
        status = bootstrap_system(settings=safe_settings, log_to_file=False)

        assert status.initialized is True
        assert status.execution_engine == "locked"
        assert status.trading_engine == "not initialized"
        assert status.historical_loader is not None
        assert status.historical_provider is not None
        assert status.historical_provider.name == "mock_historical_provider"

    def test_historical_bars_never_trigger_trading_execution(self):
        status = bootstrap_system()
        loader = status.historical_loader
        assert loader is not None

        req = HistoricalBarRequest(
            symbol="RELIANCE",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700000000.0 + 3600.0,
        )
        result = loader.load_and_ingest(req)
        assert result.accepted_bars_count > 0

        # Safety checks
        assert status.settings.execution_locked is True
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.state_store.get("execution_engine") == "locked"
        assert status.state_store.get("trading_engine") == "not initialized"

    def test_historical_bars_labeled_and_distinct_from_live_quotes(self):
        status = bootstrap_system()
        loader = status.historical_loader
        event_bus = status.event_bus

        ingested_events = []
        event_bus.subscribe(EventType.HISTORICAL_DATA_INGESTED, lambda e: ingested_events.append(e))
        live_quote_events = []
        event_bus.subscribe(EventType.MARKET_QUOTE_UPDATED, lambda e: live_quote_events.append(e))

        req = HistoricalBarRequest(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700000120.0,
        )
        loader.load_and_ingest(req)

        # Historical event was published with metadata
        assert len(ingested_events) == 1
        assert ingested_events[0].metadata.get("is_historical") is True

        # No live quote events were generated by historical ingestion
        assert len(live_quote_events) == 0
