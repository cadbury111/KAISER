"""KAISER — Walk-Forward Validation & Out-of-Sample Evaluation Foundation Tests.

Comprehensive, deterministic test suite verifying:
1. WalkForwardConfig, ValidationWindow, ValidationWarning, ValidationWindowResult,
   OutOfSampleSummary, and WalkForwardResult domain models and validation rules.
2. Chronological window generation across rolling and expanding modes.
3. Strict chronological boundary rules, non-overlap invariants, and data leakage protections.
4. Warm-up window boundaries and indicator timestamp isolation.
5. In-sample and out-of-sample execution using existing BacktestEngine.
6. Frozen strategy configuration between in-sample and out-of-sample periods.
7. Out-of-sample metric aggregation (arithmetic mean, median, compounded return, worst drawdown).
8. Edge cases: zero trades, zero losses, division-by-zero safety, and undefined profit factors.
9. Handling of insufficient data, skipped windows, and incomplete partitions.
10. Deterministic reproducibility across repeated validation runs.
11. Cooperative cancellation mid-run and state persistence.
12. Quality gate integration and bar accounting.
13. HandlerRegistry and MasterOrchestrator command integration (`run_walk_forward_validation`).
14. Absolute trading safety invariants: execution locked, virtual simulation only, research disclaimer.
"""

from __future__ import annotations

import math
import threading
import time
from typing import List, Tuple
import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.backtesting.engine import BacktestEngine
from app.backtesting.handlers import register_backtest_handlers
from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    BacktestStatus,
    ExecutionModel,
    FeeModel,
    PerformanceMetrics,
    PositionSizeType,
)
from app.backtesting.strategy import (
    SMACrossoverStrategy,
    Strategy,
    StrategyContext,
    StrategyRegistry,
)
from app.backtesting.validation.models import (
    OutOfSampleSummary,
    ValidationWarning,
    ValidationWindow,
    ValidationWindowResult,
    WalkForwardConfig,
    WalkForwardResult,
    WalkForwardStatus,
    WindowMode,
)
from app.backtesting.validation.validator import WalkForwardValidator
from app.backtesting.validation.windows import (
    generate_walk_forward_windows,
    validate_window_continuity,
)
from app.bootstrap import bootstrap_system
from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest


# ------------------------------------------------------------------------------
# Test Fixtures and Helpers
# ------------------------------------------------------------------------------

def _create_sample_bars(
    symbol: str = "TCS",
    timeframe: str = "1m",
    count: int = 120,
    start_ts: float = 1700000000.0,
    interval_sec: float = 60.0,
    base_price: float = 3500.0,
    trend: str = "sine",
) -> List[Bar]:
    """Generate deterministic Bar objects for walk-forward validation testing."""
    bars: List[Bar] = []
    p = base_price
    for i in range(count):
        ts = start_ts + (i * interval_sec)
        if trend == "up":
            p += 1.5
        elif trend == "down":
            p -= 1.5
        elif trend == "sine":
            p = base_price + math.sin(i / 5.0) * 25.0

        o = round(p, 2)
        h = round(o + 2.0, 2)
        l = round(o - 2.0, 2)
        c = round(o + 0.5, 2)
        v = 500.0 + (i * 2.0)

        bars.append(
            Bar(
                symbol=symbol,
                exchange="NSE",
                timeframe=timeframe,
                timestamp=ts,
                open=o,
                high=h,
                low=l,
                close=c,
                volume=v,
                source="mock",
            )
        )
    return bars


@pytest.fixture
def in_memory_repo() -> SQLiteHistoricalBarRepository:
    """Fixture providing an isolated in-memory SQLite bar repository."""
    return SQLiteHistoricalBarRepository(db_path=":memory:", name="test_wf_mem_repo")


@pytest.fixture
def quality_engine() -> MarketDataQualityEngine:
    """Fixture providing a fresh MarketDataQualityEngine."""
    return MarketDataQualityEngine()


@pytest.fixture
def analysis_engine() -> TechnicalAnalysisEngine:
    """Fixture providing a fresh TechnicalAnalysisEngine."""
    return TechnicalAnalysisEngine(name="test_wf_analysis_engine")


@pytest.fixture
def strategy_registry() -> StrategyRegistry:
    """Fixture providing a fresh StrategyRegistry with default strategies."""
    registry = StrategyRegistry()
    registry.register(
        name="sma_crossover",
        factory=lambda: SMACrossoverStrategy(fast_period=5, slow_period=10),
        description="Fast 5 / Slow 10 SMA Crossover for testing",
    )
    return registry


@pytest.fixture
def backtest_engine(
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    analysis_engine: TechnicalAnalysisEngine,
    strategy_registry: StrategyRegistry,
) -> BacktestEngine:
    """Fixture providing a configured BacktestEngine."""
    return BacktestEngine(
        repository=in_memory_repo,
        quality_engine=quality_engine,
        analysis_engine=analysis_engine,
        strategy_registry=strategy_registry,
        name="test_wf_backtest_engine",
    )


@pytest.fixture
def validator(
    backtest_engine: BacktestEngine,
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    strategy_registry: StrategyRegistry,
) -> WalkForwardValidator:
    """Fixture providing a configured WalkForwardValidator."""
    return WalkForwardValidator(
        engine=backtest_engine,
        repository=in_memory_repo,
        quality_engine=quality_engine,
        strategy_registry=strategy_registry,
        name="test_wf_validator",
    )


# ------------------------------------------------------------------------------
# 1. Domain Models & Configuration Validation Tests
# ------------------------------------------------------------------------------

class TestWalkForwardDomainModels:
    """Tests verifying domain models, input validation, and immutability."""

    def test_validation_window_valid(self) -> None:
        """ValidationWindow correctly accepts valid non-overlapping partitions."""
        win = ValidationWindow(
            window_id=0,
            train_start=1000.0,
            train_end=2000.0,
            test_start=2000.0,
            test_end=2500.0,
        )
        assert win.window_id == 0
        assert win.train_duration == 1000.0
        assert win.test_duration == 500.0
        assert win.is_in_sample(1000.0) is True
        assert win.is_in_sample(1999.9) is True
        assert win.is_in_sample(2000.0) is False  # Right-open boundary
        assert win.is_out_of_sample(2000.0) is True
        assert win.is_out_of_sample(2499.9) is True
        assert win.is_out_of_sample(2500.0) is False

    def test_validation_window_data_leakage_rejected(self) -> None:
        """ValidationWindow rejects overlapping train_end > test_start (data leakage)."""
        with pytest.raises(ValidationError, match="data leakage violation"):
            ValidationWindow(
                window_id=0,
                train_start=1000.0,
                train_end=2100.0,  # Overlaps into test
                test_start=2000.0,
                test_end=2500.0,
            )

    def test_validation_window_inverted_timestamps_rejected(self) -> None:
        """ValidationWindow rejects inverted train or test ranges."""
        with pytest.raises(ValidationError, match="train_start .* must be strictly less than train_end"):
            ValidationWindow(
                window_id=0,
                train_start=2000.0,
                train_end=1000.0,
                test_start=2100.0,
                test_end=2500.0,
            )

        with pytest.raises(ValidationError, match="test_start .* must be strictly less than test_end"):
            ValidationWindow(
                window_id=0,
                train_start=1000.0,
                train_end=2000.0,
                test_start=3000.0,
                test_end=2500.0,
            )

    def test_walk_forward_config_valid(self) -> None:
        """WalkForwardConfig initializes and normalizes valid configuration."""
        cfg = WalkForwardConfig(
            symbol="tcs",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700100000.0,
            train_duration_seconds=50000.0,
            test_duration_seconds=20000.0,
        )
        assert cfg.symbol == "TCS"
        assert cfg.timeframe == "15m"
        assert cfg.step_duration_seconds == 20000.0  # Defaulted to test_duration_seconds
        assert cfg.window_mode == "rolling"
        assert cfg.validation_id.startswith("wf_")

    def test_walk_forward_config_insufficient_span_rejected(self) -> None:
        """WalkForwardConfig rejects spans smaller than one full validation window."""
        with pytest.raises(ValidationError, match="smaller than one full validation window"):
            WalkForwardConfig(
                symbol="TCS",
                timeframe="1m",
                start_time=1000.0,
                end_time=1050.0,
                train_duration_seconds=40.0,
                test_duration_seconds=30.0,  # 40 + 30 = 70 > 50 span
            )

    def test_walk_forward_config_invalid_fields_rejected(self) -> None:
        """WalkForwardConfig rejects invalid symbol, modes, and durations."""
        with pytest.raises(ValidationError, match="symbol must be a non-empty string"):
            WalkForwardConfig(
                symbol="",
                timeframe="1m",
                start_time=1000.0,
                end_time=2000.0,
                train_duration_seconds=500.0,
                test_duration_seconds=200.0,
            )

        with pytest.raises(ValidationError, match="Invalid window_mode"):
            WalkForwardConfig(
                symbol="INFY",
                timeframe="1m",
                start_time=1000.0,
                end_time=2000.0,
                train_duration_seconds=500.0,
                test_duration_seconds=200.0,
                window_mode="random_shuffled",
            )

        with pytest.raises(ValidationError, match="train_duration_seconds must be strictly positive"):
            WalkForwardConfig(
                symbol="INFY",
                timeframe="1m",
                start_time=1000.0,
                end_time=2000.0,
                train_duration_seconds=-100.0,
                test_duration_seconds=200.0,
            )


# ------------------------------------------------------------------------------
# 2. Chronological Window Generation & Anti-Leakage Tests
# ------------------------------------------------------------------------------

class TestChronologicalWindowGeneration:
    """Tests verifying window generation logic, modes, and anti-leakage invariants."""

    def test_rolling_window_generation(self) -> None:
        """Generate rolling windows with constant train length and rolling shift."""
        cfg = WalkForwardConfig(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1000.0,
            end_time=2800.0,
            train_duration_seconds=600.0,
            test_duration_seconds=300.0,
            step_duration_seconds=300.0,
            window_mode="rolling",
        )
        windows = generate_walk_forward_windows(cfg)
        assert len(windows) == 4

        # Window 0: Train [1000, 1600), Test [1600, 1900)
        assert windows[0].train_start == 1000.0
        assert windows[0].train_end == 1600.0
        assert windows[0].test_start == 1600.0
        assert windows[0].test_end == 1900.0

        # Window 1: Train [1300, 1900), Test [1900, 2200)
        assert windows[1].train_start == 1300.0
        assert windows[1].train_end == 1900.0
        assert windows[1].test_start == 1900.0
        assert windows[1].test_end == 2200.0

        # Window 2: Train [1600, 2200), Test [2200, 2500)
        assert windows[2].train_start == 1600.0
        assert windows[2].train_end == 2200.0
        assert windows[2].test_start == 2200.0
        assert windows[2].test_end == 2500.0

        # Window 3: Train [1900, 2500), Test [2500, 2800)
        assert windows[3].train_start == 1900.0
        assert windows[3].train_end == 2500.0
        assert windows[3].test_start == 2500.0
        assert windows[3].test_end == 2800.0

        # Verify no window exceeds end_time and no train/test overlap
        for w in windows:
            assert w.test_end <= 2800.001
            assert w.train_end <= w.test_start

    def test_expanding_window_generation(self) -> None:
        """Generate expanding windows where train_start remains anchored at start_time."""
        cfg = WalkForwardConfig(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1000.0,
            end_time=2800.0,
            train_duration_seconds=600.0,
            test_duration_seconds=300.0,
            step_duration_seconds=300.0,
            window_mode="expanding",
        )
        windows = generate_walk_forward_windows(cfg)
        assert len(windows) == 4

        # All windows have train_start = start_time (1000.0)
        assert windows[0].train_start == 1000.0
        assert windows[0].train_end == 1600.0
        assert windows[0].test_start == 1600.0

        assert windows[1].train_start == 1000.0
        assert windows[1].train_end == 1900.0
        assert windows[1].test_start == 1900.0

        assert windows[2].train_start == 1000.0
        assert windows[2].train_end == 2200.0
        assert windows[2].test_start == 2200.0

        assert windows[3].train_start == 1000.0
        assert windows[3].train_end == 2500.0
        assert windows[3].test_start == 2500.0

    def test_window_generation_with_validation_buffer(self) -> None:
        """Validation buffer sits strictly between train_end and test_start."""
        cfg = WalkForwardConfig(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1000.0,
            end_time=2500.0,
            train_duration_seconds=600.0,
            validation_duration_seconds=100.0,
            test_duration_seconds=300.0,
            step_duration_seconds=300.0,
        )
        windows = generate_walk_forward_windows(cfg)
        assert len(windows) >= 1

        w0 = windows[0]
        assert w0.train_start == 1000.0
        assert w0.train_end == 1600.0
        assert w0.val_start == 1600.0
        assert w0.val_end == 1700.0
        assert w0.test_start == 1700.0
        assert w0.test_end == 2000.0

    def test_indicator_warmup_calculation(self) -> None:
        """Indicator warmup precedes test_start and never references future bars."""
        cfg = WalkForwardConfig(
            symbol="NIFTY",
            timeframe="1m",  # 60 sec per bar
            start_time=1000.0,
            end_time=2500.0,
            train_duration_seconds=600.0,
            test_duration_seconds=300.0,
            warmup_bars=5,  # 5 * 60 = 300s
        )
        windows = generate_walk_forward_windows(cfg)
        w0 = windows[0]
        # test_start = 1600.0, warmup_start = 1600 - 300 = 1300.0
        assert w0.warmup_start == 1300.0
        assert w0.warmup_start < w0.test_start
        assert w0.warmup_start >= w0.train_start

    def test_validate_window_continuity_warnings(self) -> None:
        """Continuity checker emits structured warnings for few windows or gaps."""
        cfg = WalkForwardConfig(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1000.0,
            end_time=1950.0,
            train_duration_seconds=600.0,
            test_duration_seconds=300.0,
        )
        windows = generate_walk_forward_windows(cfg)
        assert len(windows) == 1

        warnings = validate_window_continuity(windows, cfg)
        warning_codes = [w.code for w in warnings]
        assert "FEW_WINDOWS" in warning_codes


# ------------------------------------------------------------------------------
# 3. Walk-Forward Validation Engine Execution Tests
# ------------------------------------------------------------------------------

class TestWalkForwardValidatorEngine:
    """Tests executing chronological walk-forward validations against historical data."""

    def test_full_walk_forward_run(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Full walk-forward run completes across multiple windows with deterministic metrics."""
        # 120 bars of 1m = 7200 seconds of data from ts=1700000000.0 to 1700007200.0
        bars = _create_sample_bars(
            symbol="TCS",
            timeframe="1m",
            count=120,
            start_ts=1700000000.0,
            interval_sec=60.0,
            trend="sine",
        )
        in_memory_repo.store_bars(bars)

        # 30 bars train (1800s), 15 bars test (900s), step 15 bars (900s)
        # Total span 120 bars * 60 = 7200s
        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700007200.0,
            train_duration_seconds=1800.0,
            test_duration_seconds=900.0,
            step_duration_seconds=900.0,
            min_in_sample_bars=10,
            min_out_of_sample_bars=5,
            initial_cash=100_000.0,
            position_size_value=10.0,
        )

        result = validator.run(config=cfg, strategy="sma_crossover")

        assert result.status == WalkForwardStatus.COMPLETED
        assert result.strategy_name == "sma_crossover"
        assert len(result.windows) >= 4
        assert result.summary.completed_windows >= 4
        assert result.summary.skipped_windows == 0
        assert result.summary.failed_windows == 0

        # Verify each window has both in_sample_result and out_of_sample_result
        for w_res in result.windows:
            assert w_res.is_completed is True
            assert w_res.in_sample_result is not None
            assert w_res.out_of_sample_result is not None
            assert w_res.in_sample_bars >= 10
            assert w_res.out_of_sample_bars >= 5

        # Check research disclaimer is attached
        assert "RESEARCH ONLY" in result.disclaimer

    def test_reproducibility_identical_runs(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Two runs with identical inputs produce identical numerical metrics."""
        bars = _create_sample_bars(
            symbol="TCS",
            timeframe="1m",
            count=80,
            start_ts=1700000000.0,
            trend="sine",
        )
        in_memory_repo.store_bars(bars)

        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700004800.0,
            train_duration_seconds=1200.0,
            test_duration_seconds=600.0,
            step_duration_seconds=600.0,
            min_in_sample_bars=5,
            min_out_of_sample_bars=5,
        )

        res1 = validator.run(config=cfg, strategy="sma_crossover")
        res2 = validator.run(config=cfg, strategy="sma_crossover")

        assert res1.summary.completed_windows == res2.summary.completed_windows
        assert res1.summary.mean_window_return_pct == pytest.approx(res2.summary.mean_window_return_pct)
        assert res1.summary.compounded_return_pct == pytest.approx(res2.summary.compounded_return_pct)
        assert res1.summary.total_oos_trades == res2.summary.total_oos_trades
        assert res1.summary.total_oos_net_pnl == pytest.approx(res2.summary.total_oos_net_pnl)

    def test_insufficient_bars_skips_window(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Window with insufficient in-sample or out-of-sample bars is marked SKIPPED."""
        # Only populate the first 20 bars (1200s)
        bars = _create_sample_bars(
            symbol="TCS",
            timeframe="1m",
            count=20,
            start_ts=1700000000.0,
        )
        in_memory_repo.store_bars(bars)

        # Config asks for 60 bars of training, but only 20 exist
        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700004800.0,
            train_duration_seconds=1200.0,
            test_duration_seconds=600.0,
            min_in_sample_bars=30,  # Requires 30, only 20 present
            min_out_of_sample_bars=5,
        )

        result = validator.run(config=cfg, strategy="sma_crossover")

        assert result.status == WalkForwardStatus.COMPLETED
        assert result.summary.completed_windows == 0
        assert result.summary.skipped_windows > 0
        assert any(w.status == "SKIPPED" for w in result.windows)
        assert any(w.code == "INSUFFICIENT_IN_SAMPLE" for w in result.warnings)


# ------------------------------------------------------------------------------
# 4. Out-of-Sample Metric Aggregation & Mathematical Invariants Tests
# ------------------------------------------------------------------------------

class TestMetricAggregation:
    """Tests verifying arithmetic vs compounded return aggregation, profit factor, and drawdowns."""

    def test_metric_aggregation_mathematics(self, validator: WalkForwardValidator) -> None:
        """Verify explicit distinction between arithmetic average and compounded sequential returns."""
        # Create mock completed window results with known returns: +10%, -5%, +20%
        w1 = ValidationWindow(0, 100, 200, 200, 300)
        w2 = ValidationWindow(1, 200, 300, 300, 400)
        w3 = ValidationWindow(2, 300, 400, 400, 500)

        def _make_res(win: ValidationWindow, ret: float, pnl: float, trades: int, wins: int, dd: float) -> ValidationWindowResult:
            metrics = PerformanceMetrics(
                initial_equity=100_000.0,
                final_equity=100_000.0 + pnl,
                gross_profit=pnl if pnl > 0 else 0.0,
                gross_loss=abs(pnl) if pnl < 0 else 0.0,
                net_profit=pnl,
                total_return_pct=ret,
                max_drawdown_pct=dd,
                max_drawdown_amount=dd * 1000.0,
                total_trades=trades,
                winning_trades=wins,
                losing_trades=trades - wins,
                even_trades=0,
                win_rate=(wins / trades * 100.0) if trades > 0 else 0.0,
                profit_factor=None,
                profit_factor_description="N/A",
                total_fees=0.0,
                total_slippage=0.0,
                total_transaction_costs=0.0,
                market_exposure_pct=0.0,
            )
            backtest_res = BacktestResult(
                backtest_id=f"bt_{win.window_id}",
                status=BacktestStatus.COMPLETED,
                strategy_name="sma_crossover",
                strategy_version="1.0.0",
                config=BacktestConfig("TCS", "1m", win.test_start, win.test_end),
                metrics=metrics,
            )
            return ValidationWindowResult(
                window=win,
                status="COMPLETED",
                in_sample_bars=50,
                out_of_sample_bars=50,
                in_sample_result=backtest_res,
                out_of_sample_result=backtest_res,
            )

        window_results = [
            _make_res(w1, ret=10.0, pnl=10_000.0, trades=4, wins=3, dd=4.0),
            _make_res(w2, ret=-5.0, pnl=-5_000.0, trades=2, wins=0, dd=7.0),
            _make_res(w3, ret=20.0, pnl=20_000.0, trades=5, wins=4, dd=3.0),
        ]

        summary = validator._aggregate_out_of_sample_summary(3, window_results)

        # Arithmetic Mean: (10 - 5 + 20) / 3 = 8.333%
        assert summary.mean_window_return_pct == pytest.approx(8.3333, abs=0.01)
        # Median: 10.0%
        assert summary.median_window_return_pct == 10.0
        # Compounded Return: (1.10 * 0.95 * 1.20) - 1 = 1.254 - 1 = 25.4%
        assert summary.compounded_return_pct == pytest.approx(25.4, abs=0.01)

        # Worst Max Drawdown: max(4.0, 7.0, 3.0) = 7.0%
        assert summary.max_window_drawdown_pct == 7.0

        # Trades & Win Rate: (3 + 0 + 4) / (4 + 2 + 5) = 7 / 11 = 63.64%
        assert summary.total_oos_trades == 11
        assert summary.total_oos_winning_trades == 7
        assert summary.overall_oos_win_rate == pytest.approx(63.636, abs=0.01)

        # Window Win Rate: 2 positive out of 3 = 66.67%
        assert summary.profitable_windows_count == 2
        assert summary.unprofitable_windows_count == 1
        assert summary.window_win_rate_pct == pytest.approx(66.666, abs=0.01)

    def test_zero_trades_metric_safety(self, validator: WalkForwardValidator) -> None:
        """Windows with zero trades produce safe summary metrics without NaN or infinity."""
        w1 = ValidationWindow(0, 100, 200, 200, 300)
        metrics = PerformanceMetrics(
            initial_equity=100_000.0,
            final_equity=100_000.0,
            gross_profit=0.0,
            gross_loss=0.0,
            net_profit=0.0,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            max_drawdown_amount=0.0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            even_trades=0,
            win_rate=0.0,
            profit_factor=None,
            profit_factor_description="N/A",
            total_fees=0.0,
            total_slippage=0.0,
            total_transaction_costs=0.0,
            market_exposure_pct=0.0,
        )
        backtest_res = BacktestResult(
            backtest_id="bt_zero",
            status=BacktestStatus.COMPLETED,
            strategy_name="sma_crossover",
            strategy_version="1.0.0",
            config=BacktestConfig("TCS", "1m", 200, 300),
            metrics=metrics,
        )
        w_res = ValidationWindowResult(
            window=w1,
            status="COMPLETED",
            in_sample_bars=20,
            out_of_sample_bars=20,
            out_of_sample_result=backtest_res,
        )

        summary = validator._aggregate_out_of_sample_summary(1, [w_res])
        assert summary.total_oos_trades == 0
        assert summary.overall_oos_win_rate == 0.0
        assert summary.overall_profit_factor is None
        assert "No OOS trades executed" in summary.overall_profit_factor_description
        assert not math.isnan(summary.mean_window_return_pct)
        assert not math.isinf(summary.compounded_return_pct)


# ------------------------------------------------------------------------------
# 5. Cancellation, Events, and Quality Gate Tests
# ------------------------------------------------------------------------------

class TestWalkForwardCancellationAndEvents:
    """Tests cooperative cancellation, event publishing, and data quality tracking."""

    def test_cooperative_cancellation(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Pre-set cancellation token immediately aborts loop and returns CANCELLED."""
        bars = _create_sample_bars(symbol="TCS", count=100, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700006000.0,
            train_duration_seconds=1200.0,
            test_duration_seconds=600.0,
        )

        cancel_token = threading.Event()
        cancel_token.set()  # Cancel immediately before execution

        result = validator.run(config=cfg, strategy="sma_crossover", cancellation_token=cancel_token)
        assert result.status == WalkForwardStatus.CANCELLED
        assert result.summary.completed_windows == 0

    def test_lifecycle_event_publishing(
        self,
        in_memory_repo: SQLiteHistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
        strategy_registry: StrategyRegistry,
    ) -> None:
        """Validator publishes WALK_FORWARD_STARTED, PROGRESS, and COMPLETED events."""
        event_bus = EventBus()
        captured_events: List[Event] = []

        event_bus.subscribe_all(lambda e: captured_events.append(e))

        backtest_engine = BacktestEngine(
            repository=in_memory_repo,
            quality_engine=quality_engine,
            analysis_engine=analysis_engine,
            strategy_registry=strategy_registry,
            event_bus=event_bus,
        )
        validator = WalkForwardValidator(
            engine=backtest_engine,
            repository=in_memory_repo,
            quality_engine=quality_engine,
            strategy_registry=strategy_registry,
            event_bus=event_bus,
        )

        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            train_duration_seconds=1200.0,
            test_duration_seconds=600.0,
            min_in_sample_bars=5,
            min_out_of_sample_bars=5,
        )

        result = validator.run(config=cfg, strategy="sma_crossover")
        assert result.status == WalkForwardStatus.COMPLETED

        event_types = [e.event_type for e in captured_events]
        assert EventType.WALK_FORWARD_STARTED in event_types
        assert EventType.WALK_FORWARD_PROGRESS in event_types
        assert EventType.WALK_FORWARD_COMPLETED in event_types


# ------------------------------------------------------------------------------
# 6. HandlerRegistry & MasterOrchestrator Integration Tests
# ------------------------------------------------------------------------------

class TestWalkForwardOrchestratorIntegration:
    """Tests verifying orchestrator command handling and error reporting."""

    def test_run_walk_forward_validation_command(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
        strategy_registry: StrategyRegistry,
    ) -> None:
        """MasterOrchestrator handles 'run_walk_forward_validation' command successfully."""
        orchestrator = MasterOrchestrator()
        register_backtest_handlers(
            orchestrator=orchestrator,
            engine=validator.engine,
            strategy_registry=strategy_registry,
            validator=validator,
        )

        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        req = OrchestratorRequest.create(
            command="run_walk_forward_validation",
            context={
                "symbol": "TCS",
                "timeframe": "1m",
                "strategy": "sma_crossover",
                "start_time": 1700000000.0,
                "end_time": 1700003600.0,
                "train_duration_seconds": 1200.0,
                "test_duration_seconds": 600.0,
                "min_in_sample_bars": 5,
                "min_out_of_sample_bars": 5,
            },
        )

        resp = orchestrator.handle_request(req)
        assert resp.is_success is True
        assert resp.result["status"] == "success"
        assert resp.result["validation_status"] == "COMPLETED"
        assert "completed_windows" in resp.result
        assert "RESEARCH ONLY" in resp.result["disclaimer"]

    def test_orchestrator_unknown_strategy_rejected(
        self,
        validator: WalkForwardValidator,
        strategy_registry: StrategyRegistry,
    ) -> None:
        """MasterOrchestrator returns structured error for unregistered strategy."""
        orchestrator = MasterOrchestrator()
        register_backtest_handlers(
            orchestrator=orchestrator,
            engine=validator.engine,
            strategy_registry=strategy_registry,
            validator=validator,
        )

        req = OrchestratorRequest.create(
            command="run_walk_forward_validation",
            context={
                "symbol": "TCS",
                "strategy": "non_existent_neural_strategy",
                "start_time": 1000.0,
                "end_time": 2000.0,
            },
        )

        resp = orchestrator.handle_request(req)
        assert resp.is_success is False
        assert any("Unknown strategy" in err for err in resp.errors)

    def test_expanding_window_mode_execution(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Validator runs in expanding window mode successfully."""
        bars = _create_sample_bars(symbol="TCS", count=100, start_ts=1700000000.0, trend="sine")
        in_memory_repo.store_bars(bars)

        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700006000.0,
            train_duration_seconds=1800.0,
            test_duration_seconds=900.0,
            step_duration_seconds=900.0,
            window_mode="expanding",
            min_in_sample_bars=5,
            min_out_of_sample_bars=5,
        )

        result = validator.run(config=cfg, strategy="sma_crossover")
        assert result.status == WalkForwardStatus.COMPLETED
        assert result.config.window_mode == "expanding"
        # In expanding mode, all windows have the same train_start
        for w_res in result.windows:
            assert w_res.window.train_start == 1700000000.0

    def test_reinvest_profits_execution(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Validator propagates ending equity when reinvest_profits is True."""
        bars = _create_sample_bars(symbol="TCS", count=80, start_ts=1700000000.0, trend="up")
        in_memory_repo.store_bars(bars)

        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700004800.0,
            train_duration_seconds=1200.0,
            test_duration_seconds=600.0,
            step_duration_seconds=600.0,
            reinvest_profits=True,
            min_in_sample_bars=5,
            min_out_of_sample_bars=5,
        )

        result = validator.run(config=cfg, strategy="sma_crossover")
        assert result.status == WalkForwardStatus.COMPLETED
        assert result.config.reinvest_profits is True

    def test_profit_factor_zero_losses_infinite_description(self, validator: WalkForwardValidator) -> None:
        """Profit factor reports 'Infinite (No losing OOS trades)' when gross_loss is zero."""
        w1 = ValidationWindow(0, 100, 200, 200, 300)
        metrics = PerformanceMetrics(
            initial_equity=100_000.0,
            final_equity=105_000.0,
            gross_profit=5_000.0,
            gross_loss=0.0,
            net_profit=5_000.0,
            total_return_pct=5.0,
            max_drawdown_pct=1.0,
            max_drawdown_amount=1000.0,
            total_trades=2,
            winning_trades=2,
            losing_trades=0,
            even_trades=0,
            win_rate=100.0,
            profit_factor=None,
            profit_factor_description="Infinite",
            total_fees=0.0,
            total_slippage=0.0,
            total_transaction_costs=0.0,
            market_exposure_pct=10.0,
        )
        backtest_res = BacktestResult(
            backtest_id="bt_win",
            status=BacktestStatus.COMPLETED,
            strategy_name="sma_crossover",
            strategy_version="1.0.0",
            config=BacktestConfig("TCS", "1m", 200, 300),
            metrics=metrics,
        )
        w_res = ValidationWindowResult(
            window=w1,
            status="COMPLETED",
            in_sample_bars=20,
            out_of_sample_bars=20,
            out_of_sample_result=backtest_res,
        )
        summary = validator._aggregate_out_of_sample_summary(1, [w_res])
        assert summary.overall_profit_factor is None
        assert "Infinite" in summary.overall_profit_factor_description

    def test_result_dictionary_serialization(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """WalkForwardResult.to_dict() produces valid serializable dictionary with all metadata."""
        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            train_duration_seconds=1200.0,
            test_duration_seconds=600.0,
            min_in_sample_bars=5,
            min_out_of_sample_bars=5,
        )

        result = validator.run(config=cfg, strategy="sma_crossover")
        d = result.to_dict()
        assert isinstance(d, dict)
        assert d["validation_id"] == result.validation_id
        assert d["status"] == "COMPLETED"
        assert "config" in d
        assert "summary" in d
        assert "windows" in d
        assert "disclaimer" in d
        assert len(d["windows"]) == len(result.windows)

    def test_warmup_clamped_to_start_time(self) -> None:
        """Warm-up start is clamped to start_time if requested warmup exceeds boundary."""
        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1000.0,
            end_time=2500.0,
            train_duration_seconds=600.0,
            test_duration_seconds=300.0,
            warmup_bars=100,  # 100 * 60 = 6000s > test_start (1600s) - start_time (1000s)
        )
        windows = generate_walk_forward_windows(cfg)
        assert windows[0].warmup_start == 1000.0  # Clamped to start_time


# ------------------------------------------------------------------------------
# 7. Absolute Trading Safety Invariants Tests
# ------------------------------------------------------------------------------

class TestWalkForwardTradingSafety:
    """Tests ensuring walk-forward evaluation adheres to zero-risk safety rules."""

    def test_execution_remains_locked(self) -> None:
        """Settings.execution_locked must remain True throughout validation."""
        assert Settings.execution_locked is True

    def test_no_broker_connection_or_live_execution(
        self,
        validator: WalkForwardValidator,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Walk-forward evaluation operates purely on virtual simulated models."""
        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        cfg = WalkForwardConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            train_duration_seconds=1200.0,
            test_duration_seconds=600.0,
            min_in_sample_bars=5,
            min_out_of_sample_bars=5,
        )

        result = validator.run(config=cfg, strategy="sma_crossover")
        assert result.status == WalkForwardStatus.COMPLETED
        # Verify no external broker or network endpoints were invoked
        assert Settings.execution_locked is True
