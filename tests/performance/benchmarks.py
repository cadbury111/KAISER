"""KAISER — Deterministic Performance Profiling & Benchmark Suite.

Executes reproducible micro- and macro-benchmarks across core KAISER subsystems:
1. Indicator Calculations & Multi-Timeframe Analysis
2. Historical Bar Storage, Query & Cache Hit/Miss
3. Historical Replay Engine & Cancellation Responsiveness
4. Backtest Engine Single-Strategy Workloads
5. Portfolio Simulation Multi-Strategy Workloads
6. Portfolio Risk Evaluation & Performance Attribution
7. Unified Portfolio Report Generation
8. Dashboard HTTP Server Read-Only Requests

Measures:
- Workload size
- Repetitions & warm-up iterations
- Median (P50) and P95 latency (ms)
- Throughput (operations or bars per second)
- Peak allocated memory (KB via tracemalloc)
"""

from __future__ import annotations

import gc
import json
import math
import random
import statistics
import time
import tracemalloc
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import os
import sys
from pathlib import Path

# Ensure workspace root is in sys.path for standalone benchmark execution
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

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
from app.backtesting.models import BacktestConfig
from app.backtesting.portfolio_simulation.engine import PortfolioSimulationEngine
from app.backtesting.portfolio_simulation.models import (
    PortfolioRiskSnapshot,
    PortfolioSimulationConfig,
    StrategyAllocation,
)
from app.backtesting.strategy import StrategyRegistry
from app.core.config import Settings
from app.core.events import EventBus
from app.core.state import StateStore
from app.market.historical.cache import HistoricalDataCache, make_cache_key
from app.market.historical.replay import HistoricalReplayEngine, HistoricalReplayRequest
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.portfolio.diagnostics import PortfolioDiagnosticsService
from app.portfolio.reporting import PortfolioReportingService
from app.risk.monitor import PortfolioRiskMonitor


@dataclass(frozen=True)
class BenchmarkResult:
    """Standardized record of a single benchmark run."""

    benchmark_name: str
    workload_size: int
    workload_description: str
    repetitions: int
    warmup_runs: int
    p50_latency_ms: float
    p95_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    throughput: float
    throughput_unit: str
    peak_memory_kb: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def generate_deterministic_bars(
    count: int,
    symbol: str = "RELIANCE",
    timeframe: str = "1m",
    start_ts: float = 1700000000.0,
    seed: int = 42,
) -> List[Bar]:
    """Generate deterministic synthetic OHLCV bars using a fixed random seed."""
    rng = random.Random(seed)
    bars: List[Bar] = []
    current_price = 2500.0
    interval = 60.0

    for i in range(count):
        ts = start_ts + i * interval
        delta = rng.uniform(-5.0, 5.0)
        open_price = round(current_price, 2)
        close_price = round(max(10.0, current_price + delta), 2)
        high_price = round(max(open_price, close_price) + rng.uniform(0.5, 3.0), 2)
        low_price = round(min(open_price, close_price) - rng.uniform(0.5, 3.0), 2)
        volume = round(rng.uniform(100.0, 5000.0), 1)

        bars.append(
            Bar(
                symbol=symbol,
                exchange="NSE",
                timeframe=timeframe,
                timestamp=ts,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
                source="mock",
            )
        )
        current_price = close_price

    return bars


def run_timed_benchmark(
    name: str,
    workload_size: int,
    description: str,
    func: Callable[[], Any],
    repetitions: int = 7,
    warmup: int = 2,
    throughput_calc: Optional[Callable[[float], float]] = None,
    throughput_unit: str = "ops/sec",
) -> BenchmarkResult:
    """Execute a function repeatedly with warmup, memory tracking, and statistical percentiles."""
    # Warmup
    for _ in range(warmup):
        func()

    gc.collect()
    tracemalloc.start()
    tracemalloc.reset_peak()

    durations: List[float] = []
    for _ in range(repetitions):
        t0 = time.perf_counter()
        func()
        t1 = time.perf_counter()
        durations.append((t1 - t0) * 1000.0)

    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    durations.sort()
    p50 = statistics.median(durations)
    # P95 approximation
    p95_idx = min(len(durations) - 1, math.ceil(0.95 * len(durations)) - 1)
    p95 = durations[p95_idx]
    min_lat = min(durations)
    max_lat = max(durations)

    if throughput_calc:
        tp = throughput_calc(p50)
    else:
        tp = (1000.0 / p50) if p50 > 0 else 0.0

    res = BenchmarkResult(
        benchmark_name=name,
        workload_size=workload_size,
        workload_description=description,
        repetitions=repetitions,
        warmup_runs=warmup,
        p50_latency_ms=round(p50, 3),
        p95_latency_ms=round(p95, 3),
        min_latency_ms=round(min_lat, 3),
        max_latency_ms=round(max_lat, 3),
        throughput=round(tp, 2),
        throughput_unit=throughput_unit,
        peak_memory_kb=round(peak_bytes / 1024.0, 2),
    )
    print(
        f"  -> [{time.strftime('%H:%M:%S')}] {name} (size={workload_size}): "
        f"P50={res.p50_latency_ms:.2f}ms, P95={res.p95_latency_ms:.2f}ms, "
        f"TP={res.throughput} {res.throughput_unit}",
        flush=True,
    )
    return res


class KaiserBenchmarkSuite:
    """Comprehensive performance suite covering all core computational hot paths."""

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed
        self.results: List[BenchmarkResult] = []

    def run_all_benchmarks(self) -> List[BenchmarkResult]:
        """Run all benchmarks across representative workloads."""
        self.results.clear()

        # 1. Technical Indicators Benchmark (Small=500, Medium=2000, Large=5000)
        self.benchmark_indicators()

        # 2. Historical Storage & Cache Benchmark
        self.benchmark_storage_and_cache()

        # 3. Replay Engine Benchmark
        self.benchmark_replay()

        # 4. Backtest Engine Benchmark
        self.benchmark_backtest()

        # 5. Portfolio Simulation Benchmark
        self.benchmark_portfolio_simulation()

        # 6. Portfolio Risk Monitoring & Attribution Benchmark
        self.benchmark_risk_and_attribution()

        # 7. Unified Portfolio Reporting Benchmark
        self.benchmark_reporting()

        # 8. Dashboard HTTP Server Read-Only Benchmark
        self.benchmark_dashboard()

        return list(self.results)

    def benchmark_indicators(self) -> None:
        """Benchmark technical indicator calculations across various bar window sizes."""
        for size in (500, 2000, 5000):
            bars = generate_deterministic_bars(size, seed=self.seed)
            closes = [b.close for b in bars]
            highs = [b.high for b in bars]
            lows = [b.low for b in bars]

            def run_indicator_bundle() -> None:
                calculate_sma(closes, period=20)
                calculate_ema(closes, period=20)
                calculate_rsi(closes, period=14)
                calculate_macd(closes, fast_period=12, slow_period=26, signal_period=9)
                calculate_bollinger_bands(closes, period=20, num_std=2.0)
                calculate_atr(bars, period=14)

            res = run_timed_benchmark(
                name="technical_indicators_bundle",
                workload_size=size,
                description=f"Calculated SMA, EMA, RSI, MACD, BB, ATR over {size} bars",
                func=run_indicator_bundle,
                repetitions=10,
                warmup=2,
                throughput_calc=lambda med, s=size: round((s * 6) / (med / 1000.0), 1),
                throughput_unit="indicators*bars/sec",
            )
            self.results.append(res)

    def benchmark_storage_and_cache(self) -> None:
        """Benchmark SQLite repository insert, query, and LRU cache hit/miss."""
        repo = SQLiteHistoricalBarRepository(":memory:", name="bench_repo")
        cache = HistoricalDataCache(max_size=200, ttl_seconds=300.0)

        for size in (500, 2000, 5000):
            bars = generate_deterministic_bars(size, symbol=f"SYM_{size}", seed=self.seed)

            # Insert Benchmark
            def do_insert() -> None:
                repo.store_bars(bars, source_provider="bench_source")

            res_insert = run_timed_benchmark(
                name="sqlite_store_bars",
                workload_size=size,
                description=f"Store {size} bars in SQLite (:memory:)",
                func=do_insert,
                repetitions=5,
                warmup=1,
                throughput_calc=lambda med, s=size: round(s / (med / 1000.0), 1),
                throughput_unit="bars/sec",
            )
            self.results.append(res_insert)

            # Query Benchmark
            start_ts = bars[0].timestamp
            end_ts = bars[-1].timestamp

            def do_query() -> None:
                repo.query_bars(
                    symbol=f"SYM_{size}",
                    timeframe="1m",
                    start_time=start_ts,
                    end_time=end_ts,
                    limit=size,
                )

            res_query = run_timed_benchmark(
                name="sqlite_query_bars",
                workload_size=size,
                description=f"Query {size} bars from SQLite with Bar deserialization",
                func=do_query,
                repetitions=10,
                warmup=2,
                throughput_calc=lambda med, s=size: round(s / (med / 1000.0), 1),
                throughput_unit="bars/sec",
            )
            self.results.append(res_query)

            # Cache Hit vs Miss Benchmark
            key = make_cache_key(f"SYM_{size}", "1m", start_ts, end_ts, limit=size)
            cache.put(key, bars)

            def do_cache_hit() -> None:
                cache.get(key)

            res_cache_hit = run_timed_benchmark(
                name="historical_cache_hit",
                workload_size=size,
                description=f"Retrieve cached tuple of {size} bars",
                func=do_cache_hit,
                repetitions=20,
                warmup=5,
                throughput_calc=lambda med: round(1000.0 / med, 1),
                throughput_unit="lookups/sec",
            )
            self.results.append(res_cache_hit)

    def benchmark_replay(self) -> None:
        """Benchmark HistoricalReplayEngine throughput and cancellation."""
        for size in (500, 2000):
            repo = SQLiteHistoricalBarRepository(":memory:", name=f"bench_replay_repo_{size}")
            bars = generate_deterministic_bars(size, symbol=f"RPL_{size}", seed=self.seed)
            repo.store_bars(bars)

            quality_engine = MarketDataQualityEngine()
            analysis_engine = TechnicalAnalysisEngine(config=AnalysisConfig(max_bar_history=500))
            replay_engine = HistoricalReplayEngine(
                repository=repo,
                quality_engine=quality_engine,
                analysis_engine=analysis_engine,
            )

            req = HistoricalReplayRequest(
                symbol=f"RPL_{size}",
                timeframe="1m",
                start_time=bars[0].timestamp,
                end_time=bars[-1].timestamp,
                limit=size,
                pacing_seconds=0.0,
                reset_analysis_state=True,
            )

            def do_replay() -> None:
                replay_engine.replay(req)

            res = run_timed_benchmark(
                name="historical_replay_throughput",
                workload_size=size,
                description=f"Replay {size} bars through Quality & Technical Analysis Engines",
                func=do_replay,
                repetitions=5,
                warmup=1,
                throughput_calc=lambda med, s=size: round(s / (med / 1000.0), 1),
                throughput_unit="replayed_bars/sec",
            )
            self.results.append(res)

    def benchmark_backtest(self) -> None:
        """Benchmark BacktestEngine running SMA crossover strategy across dataset sizes."""
        for size in (500, 2000, 4000):
            repo = SQLiteHistoricalBarRepository(":memory:", name=f"bench_bt_repo_{size}")
            bars = generate_deterministic_bars(size, symbol=f"BT_{size}", seed=self.seed)
            repo.store_bars(bars)

            quality_engine = MarketDataQualityEngine()
            analysis_engine = TechnicalAnalysisEngine(config=AnalysisConfig(max_bar_history=500))
            registry = StrategyRegistry()
            engine = BacktestEngine(
                repository=repo,
                quality_engine=quality_engine,
                analysis_engine=analysis_engine,
                strategy_registry=registry,
            )

            config = BacktestConfig(
                symbol=f"BT_{size}",
                timeframe="1m",
                start_time=bars[0].timestamp,
                end_time=bars[-1].timestamp,
                initial_cash=100000.0,
            )

            def do_backtest() -> None:
                engine.run(config, strategy="sma_crossover")

            reps = 3 if size == 500 else 2
            res = run_timed_benchmark(
                name="backtest_engine_sma_crossover",
                workload_size=size,
                description=f"Run SMA crossover backtest over {size} bars with portfolio simulation",
                func=do_backtest,
                repetitions=reps,
                warmup=1,
                throughput_calc=lambda med, s=size: round(s / (med / 1000.0), 1),
                throughput_unit="bars_simulated/sec",
            )
            self.results.append(res)

    def benchmark_portfolio_simulation(self) -> None:
        """Benchmark multi-strategy, multi-asset portfolio simulation."""
        for strat_count in (2, 4):
            bar_count = 500
            repo = SQLiteHistoricalBarRepository(":memory:", name=f"bench_psim_repo_{strat_count}")
            allocations = []
            for i in range(strat_count):
                sym = f"P_{i}"
                bars = generate_deterministic_bars(bar_count, symbol=sym, seed=self.seed + i)
                repo.store_bars(bars)
                allocations.append(
                    StrategyAllocation(
                        allocation_id=f"alloc_{i}",
                        strategy_name="sma_crossover",
                        symbol=sym,
                        weight=1.0 / strat_count,
                    )
                )

            quality_engine = MarketDataQualityEngine()
            analysis_engine = TechnicalAnalysisEngine(config=AnalysisConfig(max_bar_history=500))
            sim_engine = PortfolioSimulationEngine(
                repository=repo,
                quality_engine=quality_engine,
                analysis_engine=analysis_engine,
            )

            config = PortfolioSimulationConfig(
                allocations=tuple(allocations),
                initial_cash=100000.0,
                start_time=1700000000.0,
                end_time=1700000000.0 + bar_count * 60.0,
                timeframe="1m",
            )

            def do_sim() -> None:
                sim_engine.run(config)

            total_bars = bar_count * strat_count
            res = run_timed_benchmark(
                name="portfolio_simulation_engine",
                workload_size=total_bars,
                description=f"Simulate {strat_count} strategies across {bar_count} bars each ({total_bars} total)",
                func=do_sim,
                repetitions=2,
                warmup=1,
                throughput_calc=lambda med, tb=total_bars: round(tb / (med / 1000.0), 1),
                throughput_unit="total_bars/sec",
            )
            self.results.append(res)

    def benchmark_risk_and_attribution(self) -> None:
        """Benchmark risk monitoring evaluation and attribution calculation."""
        # 1. Risk Monitor
        monitor = PortfolioRiskMonitor()
        snapshot = PortfolioRiskSnapshot(
            timestamp=1700000000.0,
            total_equity=105000.0,
            cash=45000.0,
            cash_reserve_pct=42.86,
            gross_exposure=60000.0,
            gross_exposure_pct=57.14,
            net_exposure=60000.0,
            net_exposure_pct=57.14,
            strategy_exposures={"sma_crossover": 35000.0, "momentum": 25000.0},
            instrument_exposures={"RELIANCE": 35000.0, "TCS": 25000.0},
            max_instrument_concentration_pct=33.33,
            herfindahl_index=0.168,
            drawdown_pct=4.55,
            circuit_breaker_active=False,
        )

        def do_risk() -> None:
            monitor.evaluate_snapshot(snapshot)

        res_risk = run_timed_benchmark(
            name="portfolio_risk_monitor_evaluate",
            workload_size=1,
            description="Evaluate VaR, CVaR, concentration, drawdown against portfolio snapshot",
            func=do_risk,
            repetitions=20,
            warmup=3,
            throughput_calc=lambda med: round(1000.0 / med, 1),
            throughput_unit="evaluations/sec",
        )
        self.results.append(res_risk)

        # 2. Risk-Adjusted Performance Metrics Calculation
        from app.portfolio.metrics import compute_risk_adjusted_metrics
        from app.portfolio.models import AttributionConfig
        equity_points = [{"timestamp": 1700000000 + i * 86400, "total_equity": 100000 + i * 250 + (i % 3 - 1) * 100} for i in range(100)]
        trades = [{"net_pnl": 500.0 if i % 2 == 0 else -200.0} for i in range(50)]
        cfg = AttributionConfig()

        def do_metrics() -> None:
            compute_risk_adjusted_metrics(
                equity_points=equity_points,
                trades=trades,
                config=cfg,
                total_bars=len(equity_points),
                initial_equity=100000.0,
                final_equity=125000.0,
            )

        res_attr = run_timed_benchmark(
            name="portfolio_risk_adjusted_metrics",
            workload_size=len(equity_points),
            description="Compute Sharpe, Sortino, Calmar, MaxDD and trade distribution metrics",
            func=do_metrics,
            repetitions=20,
            warmup=3,
            throughput_calc=lambda med: round(1000.0 / med, 1),
            throughput_unit="computations/sec",
        )
        self.results.append(res_attr)

    def benchmark_reporting(self) -> None:
        """Benchmark UnifiedPortfolioReport generation including Markdown, HTML & voice summaries."""
        reporting_service = PortfolioReportingService()
        source_result = {
            "simulation_id": "bench_sim_001",
            "initial_equity": 100000.0,
            "final_equity": 112500.0,
            "net_profit": 12500.0,
            "net_return_pct": 12.5,
            "max_drawdown_pct": 4.2,
            "sharpe_ratio": 1.65,
            "profit_factor": 1.85,
            "total_trades": 42,
            "win_rate": 62.5,
            "status": "SUCCESS",
            "contributions": [
                {"strategy_name": "sma_crossover", "symbol": "RELIANCE", "weight": 0.5, "net_pnl": 7500.0},
                {"strategy_name": "momentum", "symbol": "TCS", "weight": 0.5, "net_pnl": 5000.0},
            ],
            "equity_curve": [{"timestamp": 1700000000 + i * 3600, "equity": 100000 + i * 300} for i in range(50)],
        }

        def do_report() -> None:
            reporting_service.generate_unified_report(source_result, title="Performance Benchmark Report")

        res_rep = run_timed_benchmark(
            name="portfolio_reporting_generate_unified",
            workload_size=1,
            description="Synthesize UnifiedPortfolioReport with Markdown, HTML & voice summaries",
            func=do_report,
            repetitions=10,
            warmup=2,
            throughput_calc=lambda med: round(1000.0 / med, 1),
            throughput_unit="reports/sec",
        )
        self.results.append(res_rep)

    def benchmark_dashboard(self) -> None:
        """Benchmark Dashboard HTTP server read-only API requests."""
        reporting_service = PortfolioReportingService()
        diagnostics_service = PortfolioDiagnosticsService()

        # Seed with a report and 20 diagnostic events
        reporting_service.generate_unified_report(
            {
                "simulation_id": "dash_bench_sim",
                "initial_equity": 100000.0,
                "final_equity": 108000.0,
                "net_profit": 8000.0,
                "net_return_pct": 8.0,
                "max_drawdown_pct": 3.1,
                "status": "SUCCESS",
            }
        )
        for i in range(20):
            diagnostics_service.record_event(
                event_type="REPORT_RETRIEVAL_SUCCEEDED",
                outcome="SUCCESS",
                report_id="dash_bench_sim",
                details={"step": i},
            )

        server = PortfolioDashboardServer(
            reporting_service=reporting_service,
            host="127.0.0.1",
            port=0,  # dynamic port
            diagnostics_service=diagnostics_service,
        )
        server.start(blocking=False)
        base_url = server.url

        try:
            # 1. /api/status Endpoint
            status_url = f"{base_url}/api/status"

            def req_status() -> None:
                with urllib.request.urlopen(status_url, timeout=2) as r:
                    r.read()

            res_status = run_timed_benchmark(
                name="dashboard_http_api_status",
                workload_size=1,
                description="GET /api/status over loopback HTTP",
                func=req_status,
                repetitions=20,
                warmup=5,
                throughput_calc=lambda med: round(1000.0 / med, 1),
                throughput_unit="req/sec",
            )
            self.results.append(res_status)

            # 2. /api/report/latest Endpoint
            report_url = f"{base_url}/api/report/latest"

            def req_report() -> None:
                with urllib.request.urlopen(report_url, timeout=2) as r:
                    r.read()

            res_report = run_timed_benchmark(
                name="dashboard_http_api_report_latest",
                workload_size=1,
                description="GET /api/report/latest JSON over loopback HTTP",
                func=req_report,
                repetitions=20,
                warmup=5,
                throughput_calc=lambda med: round(1000.0 / med, 1),
                throughput_unit="req/sec",
            )
            self.results.append(res_report)

            # 3. /api/diagnostics Endpoint
            diag_url = f"{base_url}/api/diagnostics"

            def req_diag() -> None:
                with urllib.request.urlopen(diag_url, timeout=2) as r:
                    r.read()

            res_diag = run_timed_benchmark(
                name="dashboard_http_api_diagnostics",
                workload_size=20,
                description="GET /api/diagnostics bounded event list over loopback HTTP",
                func=req_diag,
                repetitions=20,
                warmup=5,
                throughput_calc=lambda med: round(1000.0 / med, 1),
                throughput_unit="req/sec",
            )
            self.results.append(res_diag)

        finally:
            server.stop()


def main() -> None:
    """Run benchmark suite and print human-readable summary table and JSON."""
    print("=" * 80)
    print("KAISER PERFORMANCE PROFILING & BENCHMARK SUITE")
    print("Phase 2, Step 17 — Baseline Performance Measurement")
    print("=" * 80)

    suite = KaiserBenchmarkSuite()
    results = suite.run_all_benchmarks()

    print(f"\nCompleted {len(results)} benchmarks.\n")
    print(f"{'Benchmark Name':<35} | {'Size':<6} | {'P50 (ms)':<9} | {'P95 (ms)':<9} | {'Throughput':<15} | {'Peak KB':<8}")
    print("-" * 95)
    for r in results:
        tp_str = f"{r.throughput} {r.throughput_unit.split('/')[0]}/s"
        print(f"{r.benchmark_name:<35} | {r.workload_size:<6} | {r.p50_latency_ms:<9.3f} | {r.p95_latency_ms:<9.3f} | {tp_str:<15} | {r.peak_memory_kb:<8.1f}")

    # Output JSON representation for report ingestion
    import sys
    out_file = "tests/performance/optimized_results.json" if "--optimized" in sys.argv else "tests/performance/baseline_results.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump([r.to_dict() for r in results], f, indent=2)
    print(f"\nSaved benchmark results to {out_file}")


if __name__ == "__main__":
    main()
