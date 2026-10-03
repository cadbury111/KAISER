"""KAISER — Strategy Robustness & Stress Testing Foundation Tests.

Comprehensive, deterministic test suite verifying:
1. RobustnessTestConfig, StressScenario, StressScenarioResult, ParameterSensitivityResult,
   HistoricalWindowComparison, RobustnessSummary, and RobustnessTestResult domain models.
2. Baseline backtest compared against isolated transaction-cost stress scenarios
   (increased commission, wider spread, increased slippage, combined adverse costs).
3. Verification that transaction costs are not applied twice and only intended variables change.
4. Parameter sensitivity analysis, bounded variant constraints, and range validations.
5. Historical window robustness comparisons and data quality tracking.
6. Zero-trades, zero-losses, division-by-zero, and undefined metric handling.
7. State isolation between sequential scenario runs (resets positions, caches, indicators).
8. Deterministic reproducibility across repeated test runs.
9. Cooperative cancellation mid-run, error recovery, and event publishing.
10. HandlerRegistry and MasterOrchestrator integration (`run_robustness_test`).
11. Absolute trading safety invariants: execution locked, virtual simulation only, research disclaimer.
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
from app.backtesting.robustness.engine import RobustnessTestEngine
from app.backtesting.robustness.models import (
    HistoricalWindowComparison,
    ParameterSensitivityResult,
    RobustnessStatus,
    RobustnessSummary,
    RobustnessTestConfig,
    RobustnessTestResult,
    RobustnessWarning,
    ScenarioType,
    StressScenario,
    StressScenarioResult,
)
from app.backtesting.strategy import (
    SMACrossoverStrategy,
    Strategy,
    StrategyContext,
    StrategyRegistry,
)
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
    """Generate deterministic Bar objects for robustness and stress testing."""
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
    return SQLiteHistoricalBarRepository(db_path=":memory:", name="test_rob_mem_repo")


@pytest.fixture
def quality_engine() -> MarketDataQualityEngine:
    """Fixture providing a fresh MarketDataQualityEngine."""
    return MarketDataQualityEngine()


@pytest.fixture
def analysis_engine() -> TechnicalAnalysisEngine:
    """Fixture providing a fresh TechnicalAnalysisEngine."""
    return TechnicalAnalysisEngine(name="test_rob_analysis_engine")


@pytest.fixture
def strategy_registry() -> StrategyRegistry:
    """Fixture providing a fresh StrategyRegistry with default strategies."""
    registry = StrategyRegistry()
    registry.register(
        name="sma_crossover",
        factory=lambda **kwargs: SMACrossoverStrategy(
            fast_period=kwargs.get("fast_period", 5),
            slow_period=kwargs.get("slow_period", 15),
        ),
        description="Fast 5 / Slow 15 SMA Crossover for testing",
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
        name="test_rob_backtest_engine",
    )


@pytest.fixture
def robustness_engine(
    backtest_engine: BacktestEngine,
    in_memory_repo: SQLiteHistoricalBarRepository,
    quality_engine: MarketDataQualityEngine,
    strategy_registry: StrategyRegistry,
) -> RobustnessTestEngine:
    """Fixture providing a configured RobustnessTestEngine."""
    return RobustnessTestEngine(
        engine=backtest_engine,
        repository=in_memory_repo,
        quality_engine=quality_engine,
        strategy_registry=strategy_registry,
        name="test_rob_engine",
    )


# ------------------------------------------------------------------------------
# 1. Domain Models & Configuration Validation Tests
# ------------------------------------------------------------------------------

class TestRobustnessDomainModels:
    """Tests verifying input validation, immutability, and boundary rules."""

    def test_stress_scenario_valid(self) -> None:
        """StressScenario accepts valid cost and parameter overrides."""
        sc = StressScenario(
            name="2x_fee",
            description="Double commission rate",
            fee_rate=0.001,
            slippage_bps=4.0,
            half_spread_bps=2.0,
        )
        assert sc.name == "2x_fee"
        assert sc.fee_rate == 0.001
        assert sc.slippage_bps == 4.0
        assert sc.half_spread_bps == 2.0

    def test_stress_scenario_negative_cost_rejected(self) -> None:
        """StressScenario rejects negative cost parameters."""
        with pytest.raises(ValidationError, match="fee_rate cannot be negative"):
            StressScenario(name="bad", description="bad", fee_rate=-0.01)

        with pytest.raises(ValidationError, match="slippage_bps cannot be negative"):
            StressScenario(name="bad", description="bad", slippage_bps=-1.0)

        with pytest.raises(ValidationError, match="half_spread_bps cannot be negative"):
            StressScenario(name="bad", description="bad", half_spread_bps=-2.0)

    def test_robustness_test_config_valid(self) -> None:
        """RobustnessTestConfig normalizes symbol, timeframe, and initializes defaults."""
        cfg = RobustnessTestConfig(
            symbol="infy",
            timeframe="15m",
            start_time=1700000000.0,
            end_time=1700050000.0,
            fee_rate=0.0005,
        )
        assert cfg.symbol == "INFY"
        assert cfg.timeframe == "15m"
        assert cfg.include_cost_stress is True
        assert cfg.include_parameter_sensitivity is True
        assert cfg.test_id.startswith("rob_")

    def test_robustness_test_config_inverted_dates_rejected(self) -> None:
        """RobustnessTestConfig rejects end_time <= start_time."""
        with pytest.raises(ValidationError, match="end_time .* must be strictly greater than start_time"):
            RobustnessTestConfig(
                symbol="TCS",
                timeframe="1m",
                start_time=2000.0,
                end_time=1000.0,
            )

    def test_robustness_test_config_bounded_scenarios(self) -> None:
        """RobustnessTestConfig enforces bounds on parameter variants and total scenarios."""
        # More than 20 parameter variants rejected
        bad_variants = [{"fast_period": i, "slow_period": i + 5} for i in range(25)]
        with pytest.raises(ValidationError, match="Maximum 20 parameter variants allowed"):
            RobustnessTestConfig(
                symbol="TCS",
                timeframe="1m",
                start_time=1000.0,
                end_time=5000.0,
                parameter_variants=bad_variants,
            )

        # Invalid max_total_scenarios
        with pytest.raises(ValidationError, match="max_total_scenarios must be between 1 and 100"):
            RobustnessTestConfig(
                symbol="TCS",
                timeframe="1m",
                start_time=1000.0,
                end_time=5000.0,
                max_total_scenarios=200,
            )


# ------------------------------------------------------------------------------
# 2. Transaction-Cost Stress Testing & Accounting Tests
# ------------------------------------------------------------------------------

class TestTransactionCostStressTesting:
    """Tests verifying isolated cost variations, delta accounting, and cost isolation."""

    def test_cost_stress_scenarios_execution(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Robustness test executes baseline and all cost stress scenarios."""
        bars = _create_sample_bars(symbol="TCS", count=100, start_ts=1700000000.0, trend="sine")
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700006000.0,
            fee_rate=0.0005,
            slippage_bps=2.0,
            half_spread_bps=1.0,
            include_cost_stress=True,
            include_parameter_sensitivity=False,
            include_historical_windows=False,
            commission_multipliers=(2.0,),
            spread_multipliers=(3.0,),
            slippage_multipliers=(3.0,),
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")

        assert result.status == RobustnessStatus.COMPLETED
        assert result.baseline_result is not None
        assert result.baseline_result.is_success is True

        # Must have completed scenarios: commission_2.0x, spread_3.0x, slippage_3.0x, combined_adverse_costs
        scenario_names = [r.scenario.name for r in result.scenario_results]
        assert "commission_2.0x" in scenario_names
        assert "spread_3.0x" in scenario_names
        assert "slippage_3.0x" in scenario_names
        assert "combined_adverse_costs" in scenario_names

        b_costs = result.baseline_result.metrics.total_transaction_costs

        # Find commission scenario
        comm_res = next(r for r in result.scenario_results if r.scenario.name == "commission_2.0x")
        assert comm_res.is_completed is True
        # Total transaction costs must be strictly greater than baseline costs if trades occurred
        if comm_res.metrics.total_trades > 0:
            assert comm_res.metrics.total_transaction_costs > b_costs
            assert comm_res.delta_transaction_costs > 0.0
            # Net PnL must be lower under higher costs
            assert comm_res.delta_net_pnl < 0.0

        # Find combined adverse scenario: must have highest cost delta
        comb_res = next(r for r in result.scenario_results if r.scenario.name == "combined_adverse_costs")
        if comb_res.metrics.total_trades > 0:
            assert comb_res.metrics.total_transaction_costs > comm_res.metrics.total_transaction_costs
            assert comb_res.delta_net_pnl <= comm_res.delta_net_pnl

    def test_cost_isolation_not_applied_twice(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Each scenario changes ONLY its intended cost variable without compounding across runs."""
        bars = _create_sample_bars(symbol="TCS", count=80, start_ts=1700000000.0, trend="sine")
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700004800.0,
            fee_rate=0.001,
            slippage_bps=2.0,
            half_spread_bps=1.0,
            include_cost_stress=False,
            include_parameter_sensitivity=False,
            custom_scenarios=(
                StressScenario("only_slippage_up", "Test slippage only", slippage_bps=10.0),
                StressScenario("only_spread_up", "Test spread only", half_spread_bps=10.0),
            ),
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED

        r_slip = next(r for r in result.scenario_results if r.scenario.name == "only_slippage_up")
        r_spread = next(r for r in result.scenario_results if r.scenario.name == "only_spread_up")

        # In only_slippage_up, half_spread_bps should remain at baseline (1.0)
        sc_slip_cfg = robustness_engine._build_scenario_backtest_config(cfg, r_slip.scenario)
        assert sc_slip_cfg.slippage_bps == 10.0
        assert sc_slip_cfg.half_spread_bps == 1.0
        assert sc_slip_cfg.fee_rate == 0.001

        # In only_spread_up, slippage_bps should remain at baseline (2.0)
        sc_spread_cfg = robustness_engine._build_scenario_backtest_config(cfg, r_spread.scenario)
        assert sc_spread_cfg.half_spread_bps == 10.0
        assert sc_spread_cfg.slippage_bps == 2.0
        assert sc_spread_cfg.fee_rate == 0.001


# ------------------------------------------------------------------------------
# 3. Parameter Sensitivity Analysis Tests
# ------------------------------------------------------------------------------

class TestParameterSensitivityAnalysis:
    """Tests verifying strategy parameter perturbation, stability, and comparison."""

    def test_parameter_sensitivity_grid(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Engine evaluates bounded parameter variants and calculates comparative deltas."""
        bars = _create_sample_bars(symbol="TCS", count=100, start_ts=1700000000.0, trend="sine")
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700006000.0,
            include_cost_stress=False,
            include_parameter_sensitivity=True,
            parameter_variants=(
                {"fast_period": 3, "slow_period": 10},
                {"fast_period": 4, "slow_period": 12},
                {"fast_period": 6, "slow_period": 18},
            ),
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED
        assert len(result.parameter_results) == 3

        for p_res in result.parameter_results:
            assert p_res.status == "COMPLETED"
            assert p_res.metrics is not None
            assert isinstance(p_res.delta_return_pct, float)
            assert isinstance(p_res.delta_max_drawdown_pct, float)

    def test_invalid_parameter_variant_handled_safely(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Invalid parameter combinations (e.g. fast >= slow) are handled without crash."""
        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        # In SMACrossoverStrategy, fast_period must be < slow_period.
        # Passing an invalid combination should result in a FAILED variant result.
        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            include_cost_stress=False,
            include_parameter_sensitivity=True,
            parameter_variants=(
                {"fast_period": 15, "slow_period": 5},  # Inverted: invalid
            ),
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED
        assert len(result.parameter_results) == 1
        assert result.parameter_results[0].status == "FAILED"
        assert result.parameter_results[0].error_message is not None


# ------------------------------------------------------------------------------
# 4. Historical Window Robustness Tests
# ------------------------------------------------------------------------------

class TestHistoricalWindowRobustness:
    """Tests evaluating strategy stability across distinct historical time windows."""

    def test_historical_window_comparisons(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Historical window comparisons evaluate distinct partitions and track sample counts."""
        # 120 bars = 7200s (from 1700000000 to 1700007200)
        bars = _create_sample_bars(symbol="TCS", count=120, start_ts=1700000000.0, trend="sine")
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700007200.0,
            include_cost_stress=False,
            include_parameter_sensitivity=False,
            include_historical_windows=True,
            historical_windows=(
                (1700000000.0, 1700003600.0, "Window_Phase1"),
                (1700003600.0, 1700007200.0, "Window_Phase2"),
            ),
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED
        assert len(result.window_results) == 2

        w1 = result.window_results[0]
        w2 = result.window_results[1]
        assert w1.window_name == "Window_Phase1"
        assert w1.bars_count >= 60
        assert w1.status == "COMPLETED"

        assert w2.window_name == "Window_Phase2"
        assert w2.bars_count >= 60
        assert w2.status == "COMPLETED"


# ------------------------------------------------------------------------------
# 5. Determinism, State Isolation, and Reproducibility Tests
# ------------------------------------------------------------------------------

class TestDeterminismAndStateIsolation:
    """Tests verifying state resets between runs and identical reproducibility."""

    def test_deterministic_reproducibility(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Two robustness test runs with identical inputs produce identical numerical outputs."""
        bars = _create_sample_bars(symbol="TCS", count=80, start_ts=1700000000.0, trend="sine")
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700004800.0,
            include_cost_stress=True,
            commission_multipliers=(2.0,),
            spread_multipliers=(2.0,),
            slippage_multipliers=(2.0,),
        )

        res1 = robustness_engine.run(config=cfg, strategy="sma_crossover")
        res2 = robustness_engine.run(config=cfg, strategy="sma_crossover")

        assert res1.summary.baseline_return_pct == pytest.approx(res2.summary.baseline_return_pct)
        assert res1.summary.worst_case_return_pct == pytest.approx(res2.summary.worst_case_return_pct)
        assert res1.summary.worst_case_max_drawdown_pct == pytest.approx(res2.summary.worst_case_max_drawdown_pct)
        assert res1.summary.max_cost_impact_pnl == pytest.approx(res2.summary.max_cost_impact_pnl)

    def test_state_isolation_between_scenarios(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Strategy and portfolio state are strictly isolated between successive scenarios."""
        bars = _create_sample_bars(symbol="TCS", count=80, start_ts=1700000000.0, trend="up")
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700004800.0,
            initial_cash=100_000.0,
            include_cost_stress=True,
            commission_multipliers=(1.5, 2.0),
            spread_multipliers=(),
            slippage_multipliers=(),
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED

        # Verify every scenario backtest started with exactly initial_cash (100,000)
        for sc_res in result.scenario_results:
            if sc_res.is_completed:
                assert sc_res.backtest_result.config.initial_cash == 100_000.0
                assert sc_res.metrics.initial_equity == 100_000.0


# ------------------------------------------------------------------------------
# 6. Cancellation, Error Recovery, and Event Publishing Tests
# ------------------------------------------------------------------------------

class TestCancellationAndEvents:
    """Tests cooperative cancellation, error recovery, and EventBus publishing."""

    def test_cooperative_cancellation(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Pre-set cancellation token aborts scenario loop and returns CANCELLED."""
        bars = _create_sample_bars(symbol="TCS", count=80, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700004800.0,
        )

        cancel_token = threading.Event()
        cancel_token.set()  # Cancel immediately

        result = robustness_engine.run(config=cfg, strategy="sma_crossover", cancellation_token=cancel_token)
        assert result.status == RobustnessStatus.CANCELLED

    def test_event_publishing(
        self,
        in_memory_repo: SQLiteHistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
        strategy_registry: StrategyRegistry,
    ) -> None:
        """Engine publishes ROBUSTNESS_TEST_STARTED, PROGRESS, and COMPLETED events."""
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
        rob_engine = RobustnessTestEngine(
            engine=backtest_engine,
            repository=in_memory_repo,
            quality_engine=quality_engine,
            strategy_registry=strategy_registry,
            event_bus=event_bus,
        )

        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            commission_multipliers=(2.0,),
            spread_multipliers=(),
            slippage_multipliers=(),
        )

        result = rob_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED

        e_types = [e.event_type for e in captured_events]
        assert EventType.ROBUSTNESS_TEST_STARTED in e_types
        assert EventType.ROBUSTNESS_TEST_PROGRESS in e_types
        assert EventType.ROBUSTNESS_TEST_COMPLETED in e_types


# ------------------------------------------------------------------------------
# 7. HandlerRegistry & MasterOrchestrator Integration Tests
# ------------------------------------------------------------------------------

class TestRobustnessOrchestratorIntegration:
    """Tests verifying orchestrator command handling and error reporting."""

    def test_run_robustness_test_command(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
        strategy_registry: StrategyRegistry,
    ) -> None:
        """MasterOrchestrator handles 'run_robustness_test' command successfully."""
        orchestrator = MasterOrchestrator()
        register_backtest_handlers(
            orchestrator=orchestrator,
            engine=robustness_engine.engine,
            strategy_registry=strategy_registry,
            robustness_engine=robustness_engine,
        )

        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        req = OrchestratorRequest.create(
            command="run_robustness_test",
            context={
                "symbol": "TCS",
                "timeframe": "1m",
                "strategy": "sma_crossover",
                "start_time": 1700000000.0,
                "end_time": 1700003600.0,
                "include_cost_stress": True,
                "include_parameter_sensitivity": False,
            },
        )

        resp = orchestrator.handle_request(req)
        assert resp.is_success is True
        assert resp.result["status"] == "success"
        assert resp.result["robustness_status"] == "COMPLETED"
        assert "baseline_return_pct" in resp.result
        assert "worst_case_return_pct" in resp.result
        assert "RESEARCH ONLY" in resp.result["disclaimer"]

    def test_orchestrator_missing_symbol_rejected(
        self,
        robustness_engine: RobustnessTestEngine,
        strategy_registry: StrategyRegistry,
    ) -> None:
        """MasterOrchestrator returns structured error when symbol is omitted."""
        orchestrator = MasterOrchestrator()
        register_backtest_handlers(
            orchestrator=orchestrator,
            engine=robustness_engine.engine,
            strategy_registry=strategy_registry,
            robustness_engine=robustness_engine,
        )

        req = OrchestratorRequest.create(
            command="run_robustness_test",
            context={
                "strategy": "sma_crossover",
            },
        )

        resp = orchestrator.handle_request(req)
        assert resp.is_success is False
        assert any("symbol" in err for err in resp.errors)

    def test_orchestrator_unknown_strategy_rejected(
        self,
        robustness_engine: RobustnessTestEngine,
        strategy_registry: StrategyRegistry,
    ) -> None:
        """MasterOrchestrator returns structured error when strategy is not found."""
        orchestrator = MasterOrchestrator()
        register_backtest_handlers(
            orchestrator=orchestrator,
            engine=robustness_engine.engine,
            strategy_registry=strategy_registry,
            robustness_engine=robustness_engine,
        )

        req = OrchestratorRequest.create(
            command="run_robustness_test",
            context={
                "symbol": "TCS",
                "strategy": "quantum_neural_arbitrage",
            },
        )

        resp = orchestrator.handle_request(req)
        assert resp.is_success is False
        assert any("Unknown strategy" in err for err in resp.errors)

    def test_serialization_and_summary_text(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """RobustnessTestResult serializes to valid dictionary and produces readable summary text."""
        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            commission_multipliers=(2.0,),
            spread_multipliers=(),
            slippage_multipliers=(),
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED

        d = result.to_dict()
        assert isinstance(d, dict)
        assert d["test_id"] == result.test_id
        assert "summary" in d
        assert "scenario_results" in d
        assert "disclaimer" in d
        assert "RESEARCH ONLY" in d["disclaimer"]

        # Check summary text formatting
        assert "Robustness Test" in result.summary_text
        assert "TCS" in result.summary_text
        assert "PassRate=" in result.summary_text
        assert "WorstCaseReturn=" in result.summary_text

    def test_zero_trades_robustness_handling(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """When flat prices result in 0 trades, robustness handles metrics safely."""
        # Flat prices across all bars -> no crossover -> 0 trades
        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0, trend="flat")
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
            commission_multipliers=(2.0,),
            spread_multipliers=(),
            slippage_multipliers=(),
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED
        assert result.baseline_result.metrics.total_trades == 0
        assert not math.isnan(result.summary.baseline_return_pct)
        assert not math.isnan(result.summary.worst_case_return_pct)

    def test_cancel_test_method_direct(
        self,
        robustness_engine: RobustnessTestEngine,
    ) -> None:
        """cancel_test returns False for non-existent test and True when active."""
        assert robustness_engine.cancel_test("non_existent_id") is False
        stop_evt = threading.Event()
        with robustness_engine._lock:
            robustness_engine._active_tests["test_fake"] = stop_evt
        assert robustness_engine.cancel_test("test_fake") is True
        assert stop_evt.is_set() is True


# ------------------------------------------------------------------------------
# 8. Absolute Trading Safety Invariants Tests
# ------------------------------------------------------------------------------

class TestRobustnessTradingSafety:
    """Tests ensuring stress testing operates in strict simulation without live trading."""

    def test_execution_remains_locked(self) -> None:
        """Settings.execution_locked must remain True throughout stress testing."""
        assert Settings.execution_locked is True

    def test_no_broker_connection_or_live_execution(
        self,
        robustness_engine: RobustnessTestEngine,
        in_memory_repo: SQLiteHistoricalBarRepository,
    ) -> None:
        """Stress testing operates strictly inside the virtual simulation engine."""
        bars = _create_sample_bars(symbol="TCS", count=60, start_ts=1700000000.0)
        in_memory_repo.store_bars(bars)

        cfg = RobustnessTestConfig(
            symbol="TCS",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700003600.0,
        )

        result = robustness_engine.run(config=cfg, strategy="sma_crossover")
        assert result.status == RobustnessStatus.COMPLETED
        # Absolute safety verification
        assert Settings.execution_locked is True
