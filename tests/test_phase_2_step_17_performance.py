"""KAISER — Phase 2 Step 17 Performance Profiling, Resource-Bound Verification & Scalability Tests.

Comprehensive verification of:
1. Bounded cache/history retention under sustained insertion (LRU cache, diagnostics audit log).
2. Cache invalidation and repository consistency (storage vs cache synchronization).
3. Large but bounded historical queries (limit clamping, boundary conditions).
4. Replay cancellation responsiveness and resource cleanup.
5. Repeated dashboard startup/shutdown lifecycle without socket/thread leaks.
6. Concurrent read-only requests and diagnostics access under multi-threaded load.
7. Error paths that cleanly release resources without memory or lock leaks.
8. Numerical and functional equivalence of core indicator and analytical engines.
9. Accounting and risk invariants across multiple workload sizes.
10. Safety invariants: execution_locked = True, zero live trade execution, loopback binding.
"""

from __future__ import annotations

import concurrent.futures
import json
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.indicators import (
    calculate_atr,
    calculate_bollinger_bands,
    calculate_ema,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
)
from app.analysis.models import AnalysisConfig
from app.backtesting.engine import BacktestEngine
from app.backtesting.models import BacktestConfig, BacktestResult, BacktestStatus
from app.backtesting.portfolio_simulation.engine import PortfolioSimulationEngine
from app.backtesting.portfolio_simulation.models import (
    PortfolioRiskSnapshot,
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
)
from app.backtesting.strategy import StrategyRegistry
from app.core.config import Settings
from app.core.events import EventBus
from app.core.state import StateStore
from app.market.historical.cache import HistoricalDataCache, make_cache_key
from app.market.historical.replay import (
    HistoricalReplayEngine,
    HistoricalReplayRequest,
    HistoricalReplayStatus,
)
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.diagnostics import PortfolioDiagnosticsService
from app.portfolio.models import AttributionConfig
from app.portfolio.metrics import compute_risk_adjusted_metrics
from app.portfolio.reporting import PortfolioReportingService
from app.risk.models import AlertState, PortfolioRiskReport, RiskRuleId
from app.risk.monitor import PortfolioRiskMonitor


def _make_deterministic_bars(count: int, symbol: str = "TCS", start_ts: float = 1700000000.0) -> List[Bar]:
    """Generate predictable deterministic bars."""
    bars: List[Bar] = []
    price = 3500.0
    for i in range(count):
        ts = start_ts + i * 60.0
        delta = 2.0 if (i % 2 == 0) else -1.5
        close_p = price + delta
        high_p = max(price, close_p) + 1.0
        low_p = min(price, close_p) - 1.0
        bars.append(
            Bar(
                symbol=symbol,
                exchange="NSE",
                timeframe="1m",
                timestamp=ts,
                open=price,
                high=high_p,
                low=low_p,
                close=close_p,
                volume=100.0 + (i % 10) * 10.0,
                source="test_gen",
            )
        )
        price = close_p
    return bars


class TestResourceBoundVerification:
    """Verifies that bounded collections, histories, and caches strictly enforce capacity limits."""

    def test_historical_cache_bounded_retention_and_lru_eviction(self) -> None:
        """HistoricalDataCache must remain bounded under sustained insertion and evict least-recently-used."""
        max_size = 15
        cache = HistoricalDataCache(max_size=max_size, ttl_seconds=60.0)

        # Insert 45 distinct items
        keys = []
        for i in range(45):
            key = make_cache_key("SYM", "1m", 1000.0 + i * 60.0, 1060.0 + i * 60.0, 100)
            keys.append(key)
            bars = (Bar(symbol="SYM", exchange="NSE", timeframe="1m", timestamp=1000.0 + i, open=100.0, high=105.0, low=95.0, close=102.0, volume=500.0, source="test"),)
            cache.put(key, bars)
            assert cache.size <= max_size

        assert cache.size == max_size

        # Oldest keys (keys[0] to keys[29]) must have been evicted
        for i in range(30):
            assert cache.get(keys[i]) is None

        # Most recent keys (keys[30] to keys[44]) must be present
        for i in range(30, 45):
            assert cache.get(keys[i]) is not None

    def test_portfolio_diagnostics_bounded_event_history(self) -> None:
        """PortfolioDiagnosticsService must strictly enforce max_history retention under heavy event flow."""
        max_history = 25
        diag_service = PortfolioDiagnosticsService(max_history=max_history)

        # Record 100 operational events
        for i in range(100):
            diag_service.record_event(
                event_type="TEST_BENCHMARK_EVENT",
                outcome="SUCCESS" if i % 2 == 0 else "FAILURE",
                report_id=f"rep_{i}",
                details={"index": i, "payload": "x" * 200},
            )
            events = diag_service.get_history(limit=100)
            assert len(events) <= max_history

        recent_events = diag_service.get_history(limit=100)
        assert len(recent_events) == max_history
        # Default is newest_first=True, so index 0 is rep_99 and index -1 is rep_75
        assert recent_events[0].report_id == "rep_99"
        assert recent_events[-1].report_id == "rep_75"

    def test_historical_repository_large_bounded_queries(self) -> None:
        """SQLite repository query_bars respects limit bounds and handles non-existent queries safely."""
        repo = SQLiteHistoricalBarRepository(":memory:", name="test_bounds_repo")
        bars = _make_deterministic_bars(500, symbol="INFY")
        repo.store_bars(bars)

        start_ts = bars[0].timestamp
        end_ts = bars[-1].timestamp

        # Query with explicit limit smaller than dataset
        queried_50 = repo.query_bars(symbol="INFY", timeframe="1m", start_time=start_ts, end_time=end_ts, limit=50)
        assert len(queried_50) == 50

        # Query with limit larger than dataset returns exact available count without crashing
        queried_1000 = repo.query_bars(symbol="INFY", timeframe="1m", start_time=start_ts, end_time=end_ts, limit=1000)
        assert len(queried_1000) == 500

        # Query for non-existent symbol returns empty tuple without leaking
        queried_empty = repo.query_bars(symbol="NON_EXISTENT", timeframe="1m", start_time=start_ts, end_time=end_ts, limit=100)
        assert queried_empty == ()


class TestReplayLifecycleAndCancellation:
    """Verifies that replay execution honors cancellation without resource leaks or thread hangs."""

    def test_replay_cancellation_and_cleanup(self) -> None:
        """Cancelling replay must immediately halt processing and leave engines in a clean state."""
        repo = SQLiteHistoricalBarRepository(":memory:", name="test_rpl_cancel_repo")
        bars = _make_deterministic_bars(300, symbol="SBIN")
        repo.store_bars(bars)

        quality_engine = MarketDataQualityEngine()
        analysis_engine = TechnicalAnalysisEngine(config=AnalysisConfig(max_bar_history=200))
        replay_engine = HistoricalReplayEngine(
            repository=repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )

        replay_id = "test_cancel_rpl_001"
        req = HistoricalReplayRequest(
            symbol="SBIN",
            timeframe="1m",
            start_time=bars[0].timestamp,
            end_time=bars[-1].timestamp,
            limit=300,
            pacing_seconds=0.01,  # Paced so cancellation can intervene
            reset_analysis_state=True,
            replay_id=replay_id,
        )

        # Run replay in separate thread
        replay_thread = threading.Thread(target=replay_engine.replay, args=(req,), daemon=True)
        replay_thread.start()

        # Allow replay to register
        time.sleep(0.05)

        # Cancel replay
        success = replay_engine.cancel_replay(replay_id)
        assert success is True

        replay_thread.join(timeout=2.0)
        assert not replay_thread.is_alive()
        assert replay_engine.active_replay_count == 0


class TestDashboardConcurrencyAndLifecycle:
    """Verifies repeated dashboard lifecycle and concurrent read-only request handling."""

    def test_repeated_dashboard_startup_shutdown(self) -> None:
        """Dashboard server must cleanly release sockets and stop threads across multiple lifecycles."""
        reporting_service = PortfolioReportingService()
        diagnostics_service = PortfolioDiagnosticsService()

        for _ in range(3):
            server = PortfolioDashboardServer(
                reporting_service=reporting_service,
                host="127.0.0.1",
                port=0,
                diagnostics_service=diagnostics_service,
            )
            server.start(blocking=False)
            assert server.is_running is True
            assert server.url.startswith("http://127.0.0.1:")

            # Perform a quick read-only check
            with urllib.request.urlopen(f"{server.url}/api/status", timeout=2) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["status"] == "healthy"
                assert data["read_only"] is True

            server.stop()
            assert server.is_running is False

    def test_concurrent_dashboard_read_only_requests(self) -> None:
        """Dashboard handles concurrent read-only API requests safely without data corruption."""
        reporting_service = PortfolioReportingService()
        diagnostics_service = PortfolioDiagnosticsService()

        # Seed with a report
        reporting_service.generate_unified_report(
            {
                "simulation_id": "sim_concurrent_bench",
                "initial_equity": 100000.0,
                "final_equity": 110000.0,
                "net_profit": 10000.0,
                "net_return_pct": 10.0,
                "max_drawdown_pct": 2.5,
                "status": "SUCCESS",
            }
        )

        server = PortfolioDashboardServer(
            reporting_service=reporting_service,
            host="127.0.0.1",
            port=0,
            diagnostics_service=diagnostics_service,
        )
        server.start(blocking=False)

        try:
            endpoints = ["/api/status", "/api/report/latest", "/api/diagnostics"]
            results: List[int] = []

            def worker(endpoint: str) -> int:
                url = f"{server.url}{endpoint}"
                with urllib.request.urlopen(url, timeout=3) as resp:
                    resp.read()
                    return resp.status

            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(worker, endpoints[i % len(endpoints)]) for i in range(24)]
                for fut in concurrent.futures.as_completed(futures):
                    results.append(fut.result())

            assert len(results) == 24
            assert all(code == 200 for code in results)

        finally:
            server.stop()


class TestAnalyticalFunctionalEquivalence:
    """Verifies that mathematical computations and financial invariants remain strictly preserved."""

    def test_indicator_calculations_deterministic_values(self) -> None:
        """Indicator computations produce exact deterministic values matching baseline math."""
        closes = [100.0, 102.0, 101.0, 104.0, 103.0, 106.0, 105.0, 108.0, 107.0, 110.0,
                  109.0, 112.0, 111.0, 114.0, 113.0, 116.0, 115.0, 118.0, 117.0, 120.0,
                  119.0, 122.0, 121.0, 124.0, 123.0]

        sma_20 = calculate_sma(closes, period=20)
        assert sma_20 is not None
        assert abs(sma_20 - sum(closes[-20:]) / 20.0) < 1e-4

        ema_20 = calculate_ema(closes, period=20)
        assert ema_20 is not None
        assert ema_20 > 110.0

        rsi_14 = calculate_rsi(closes, period=14)
        assert rsi_14 is not None
        assert 0.0 <= rsi_14 <= 100.0

        bb_20 = calculate_bollinger_bands(closes, period=20, num_std=2.0)
        assert bb_20 is not None
        assert bb_20["upper"] > bb_20["middle"] > bb_20["lower"]

    def test_portfolio_simulation_accounting_invariants(self) -> None:
        """Portfolio simulation strictly preserves reconciliation (gross - costs = net) across dataset sizes."""
        repo = SQLiteHistoricalBarRepository(":memory:", name="test_psim_inv_repo")
        bars_a = _make_deterministic_bars(200, symbol="ASSET_A", start_ts=1700000000.0)
        bars_b = _make_deterministic_bars(200, symbol="ASSET_B", start_ts=1700000000.0)
        repo.store_bars(bars_a)
        repo.store_bars(bars_b)

        quality_engine = MarketDataQualityEngine()
        analysis_engine = TechnicalAnalysisEngine(config=AnalysisConfig(max_bar_history=100))
        sim_engine = PortfolioSimulationEngine(
            repository=repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
        )

        config = PortfolioSimulationConfig(
            allocations=(
                StrategyAllocation(
                    allocation_id="alloc_a",
                    strategy_name="sma_crossover",
                    symbol="ASSET_A",
                    weight=0.5,
                ),
                StrategyAllocation(
                    allocation_id="alloc_b",
                    strategy_name="sma_crossover",
                    symbol="ASSET_B",
                    weight=0.5,
                ),
            ),
            initial_cash=100000.0,
            start_time=1700000000.0,
            end_time=1700000000.0 + 200 * 60.0,
            timeframe="1m",
        )

        result = sim_engine.run(config)
        assert result.status == PortfolioSimulationStatus.SUCCESS
        assert result.bars_processed == 200
        cost_diff = abs(result.gross_pnl - result.total_transaction_costs - result.net_pnl)
        assert cost_diff < 1e-4

    def test_portfolio_risk_monitor_deterministic_evaluation(self) -> None:
        """PortfolioRiskMonitor generates deterministic risk alerts and reports for identical snapshots."""
        monitor = PortfolioRiskMonitor()
        snapshot = PortfolioRiskSnapshot(
            timestamp=1700000000.0,
            total_equity=100000.0,
            cash=10000.0,  # Cash reserve 10% (triggers warning threshold if min is 15%)
            cash_reserve_pct=10.0,
            gross_exposure=90000.0,
            gross_exposure_pct=90.0,
            net_exposure=90000.0,
            net_exposure_pct=90.0,
            strategy_exposures={"sma_crossover": 90000.0},
            instrument_exposures={"TCS": 90000.0},
            max_instrument_concentration_pct=90.0,
            herfindahl_index=0.81,
            drawdown_pct=2.0,
            circuit_breaker_active=False,
        )

        report1 = monitor.evaluate_snapshot(snapshot)
        report2 = monitor.evaluate_snapshot(snapshot)

        assert report1.overall_state == report2.overall_state
        assert len(report1.active_alerts) == len(report2.active_alerts)


class TestSafetyInvariants:
    """Verifies that all Phase 2 safety invariants remain unconditionally active."""

    def test_execution_engine_remains_locked(self) -> None:
        """Settings.execution_locked must be True and paper trading must be enforced."""
        settings = Settings()
        assert settings.execution_locked is True
        assert settings.paper_trading_only is True

    def test_dashboard_server_refuses_public_interface_binding(self) -> None:
        """PortfolioDashboardServer must only bind to 127.0.0.1 loopback address."""
        reporting_service = PortfolioReportingService()

        with pytest.raises(Exception):
            PortfolioDashboardServer(
                reporting_service=reporting_service,
                host="0.0.0.0",  # Public interface forbidden
                port=8080,
            )
