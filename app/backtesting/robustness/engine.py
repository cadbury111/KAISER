"""KAISER — Strategy Robustness & Stress Testing Engine.

Orchestrates deterministic research-only stress testing across transaction-cost variations,
parameter sensitivity perturbations, and historical time partitions using the existing
BacktestEngine and quality gates.

SAFETY INVARIANTS:
1. Research and evaluation only: Evaluates simulated strategy behavior under adverse conditions.
   Never executes real orders or connects to a broker.
2. Anti-leakage and anti-look-ahead: Stress scenarios use only permitted historical data.
3. State isolation: Resets strategy and engine states between sequential scenario runs.
4. No profitability claims: Stress results highlight vulnerability and risk;
   they do not predict future market performance.
"""

from __future__ import annotations

import copy
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from app.backtesting.engine import BacktestEngine
from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    BacktestStatus,
    PerformanceMetrics,
)
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
from app.backtesting.strategy import Strategy, StrategyRegistry
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.historical.storage import HistoricalBarRepository
from app.market.quality.engine import MarketDataQualityEngine

logger = get_logger("backtesting.robustness.engine")


class RobustnessTestEngine:
    """Orchestrates comprehensive stress testing and parameter sensitivity analysis."""

    def __init__(
        self,
        engine: BacktestEngine,
        repository: HistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        strategy_registry: Optional[StrategyRegistry] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        run_registry: Optional[Any] = None,
        name: str = "robustness_test_engine",
    ) -> None:
        self.name = name
        self.engine = engine
        self.repository = repository
        self.quality_engine = quality_engine
        self.strategy_registry = strategy_registry or getattr(engine, "strategy_registry", StrategyRegistry())
        self.event_bus = event_bus
        self.state_store = state_store
        self.run_registry = run_registry

        self._lock = threading.RLock()
        self._active_tests: Dict[str, threading.Event] = {}

    @property
    def active_test_count(self) -> int:
        """Return the number of robustness test sessions currently executing."""
        with self._lock:
            return len(self._active_tests)

    def cancel_test(self, test_id: str) -> bool:
        """Signal cooperative cancellation to an active robustness test session."""
        with self._lock:
            evt = self._active_tests.get(test_id)
            if evt:
                evt.set()
                logger.info("Cancellation requested for robustness test '%s'", test_id)
                return True
            return False

    def run(
        self,
        config: RobustnessTestConfig,
        strategy: Optional[Union[Strategy, str]] = None,
        cancellation_token: Optional[threading.Event] = None,
    ) -> RobustnessTestResult:
        """Execute a full robustness and stress-testing evaluation.

        Args:
            config: Validated RobustnessTestConfig specification.
            strategy: Optional Strategy instance or registered identifier (defaults to config.strategy_name).
            cancellation_token: Optional cooperative cancellation token.

        Returns:
            RobustnessTestResult: Detailed comparative report across baseline and all scenarios.
        """
        start_time_perf = time.perf_counter()
        test_id = config.test_id or f"rob_{uuid.uuid4().hex[:12]}"
        cid = config.correlation_id or uuid.uuid4().hex

        # 1. Resolve Strategy
        strat_key = strategy or config.strategy_name
        resolved_strategy: Strategy
        if isinstance(strat_key, str):
            resolved_strategy = self.strategy_registry.get(strat_key)
        elif isinstance(strat_key, Strategy):
            resolved_strategy = strat_key
        else:
            raise ValidationError(f"Invalid strategy type: {type(strat_key)}. Expected Strategy or str.")

        stop_event = cancellation_token or threading.Event()
        with self._lock:
            self._active_tests[test_id] = stop_event

        warnings: List[RobustnessWarning] = []

        # Setup Analysis Run Registry
        analysis_run = None
        if getattr(self, "run_registry", None) is not None:
            try:
                from app.analysis.runs.models import OperationType
                analysis_run = self.run_registry.create_run(
                    operation_type=OperationType.ROBUSTNESS,
                    configuration=config,
                    strategy_id=resolved_strategy.name,
                    strategy_version=resolved_strategy.version,
                    symbol_universe=(config.symbol,),
                    timeframe_universe=(config.timeframe,),
                    data_start=config.start_time,
                    data_end=config.end_time,
                    execution_metadata={"test_id": test_id},
                )
                self.run_registry.start_run(analysis_run.run_id)
            except Exception as exc:
                logger.warning("Failed to initialize analysis run for robustness [%s]: %s", test_id, exc)
                analysis_run = None

        # 2. Publish Started Event
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.ROBUSTNESS_TEST_STARTED,
                    source=self.name,
                    correlation_id=cid,
                    payload={
                        "test_id": test_id,
                        "strategy": resolved_strategy.name,
                        "symbol": config.symbol,
                        "timeframe": config.timeframe,
                    },
                )
            )

        # 3. Execute Baseline Backtest
        baseline_cfg = config.create_baseline_backtest_config()
        baseline_res = self.engine.run(
            config=baseline_cfg,
            strategy=resolved_strategy,
            cancellation_token=stop_event,
        )

        if not baseline_res.is_success:
            logger.error("Baseline backtest failed for robustness test [%s]: %s", test_id, baseline_res.error_message)
            duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
            empty_summary = self._create_empty_summary()
            is_cancelled = stop_event.is_set() or baseline_res.status == BacktestStatus.CANCELLED
            status = RobustnessStatus.CANCELLED if is_cancelled else RobustnessStatus.FAILED
            msg = "Baseline run cancelled" if is_cancelled else f"Baseline run failed: {baseline_res.error_message}"
            result = RobustnessTestResult(
                test_id=test_id,
                status=status,
                strategy_name=resolved_strategy.name,
                strategy_version=resolved_strategy.version,
                config=config,
                baseline_result=baseline_res,
                summary=empty_summary,
                error_message=msg,
                duration_ms=duration_ms,
                run_id=analysis_run.run_id if analysis_run else None,
            )
            self._cleanup_and_finish(result, cid)
            return result

        b_metrics = baseline_res.metrics
        if b_metrics.total_trades < 5:
            warnings.append(
                RobustnessWarning(
                    code="LOW_BASELINE_TRADES",
                    message=(
                        f"Baseline strategy executed only {b_metrics.total_trades} trades. "
                        "Sensitivity calculations carry elevated uncertainty due to small sample size."
                    ),
                    severity="WARNING",
                )
            )

        # 4. Generate Scenarios
        scenarios = self._generate_scenarios(config)
        if len(scenarios) > config.max_total_scenarios:
            warnings.append(
                RobustnessWarning(
                    code="SCENARIO_COUNT_CAPPED",
                    message=f"Scenario count capped at max_total_scenarios ({config.max_total_scenarios}).",
                    severity="INFO",
                )
            )
            scenarios = scenarios[: config.max_total_scenarios]

        # 5. Execute Stress Scenarios
        scenario_results: List[StressScenarioResult] = []
        for sc in scenarios:
            if stop_event.is_set():
                logger.warning("Robustness test [%s] cancelled during scenario execution", test_id)
                duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
                summary = self._synthesize_summary(baseline_res, scenario_results, (), ())
                result = RobustnessTestResult(
                    test_id=test_id,
                    status=RobustnessStatus.CANCELLED,
                    strategy_name=resolved_strategy.name,
                    strategy_version=resolved_strategy.version,
                    config=config,
                    baseline_result=baseline_res,
                    summary=summary,
                    scenario_results=tuple(scenario_results),
                    warnings=tuple(warnings),
                    duration_ms=duration_ms,
                )
                self._cleanup_and_finish(result, cid)
                return result

            sc_start_perf = time.perf_counter()
            sc_cfg = self._build_scenario_backtest_config(config, sc)

            sc_res = self.engine.run(
                config=sc_cfg,
                strategy=resolved_strategy,
                cancellation_token=stop_event,
            )
            sc_duration = (time.perf_counter() - sc_start_perf) * 1000.0

            sc_warnings: List[RobustnessWarning] = []
            if sc_res.is_success and sc_res.metrics:
                m = sc_res.metrics
                delta_pnl = m.net_profit - b_metrics.net_profit
                delta_ret = m.total_return_pct - b_metrics.total_return_pct
                delta_dd = m.max_drawdown_pct - b_metrics.max_drawdown_pct
                delta_trades = m.total_trades - b_metrics.total_trades
                delta_win = m.win_rate - b_metrics.win_rate
                delta_costs = m.total_transaction_costs - b_metrics.total_transaction_costs

                # Relative degradation calculations
                ret_deg: Optional[float] = None
                if b_metrics.net_profit > 0:
                    ret_deg = max(0.0, (1.0 - (m.net_profit / b_metrics.net_profit)) * 100.0)

                dd_inc: Optional[float] = None
                if b_metrics.max_drawdown_pct > 0:
                    dd_inc = max(0.0, ((m.max_drawdown_pct - b_metrics.max_drawdown_pct) / b_metrics.max_drawdown_pct) * 100.0)

                # Diagnostic warnings
                if b_metrics.net_profit > 0 and m.net_profit < 0:
                    w = RobustnessWarning(
                        code="PROFITABILITY_INVERSION",
                        message=f"Scenario '{sc.name}' converted a net profitable strategy into net loss ({m.net_profit:+.2f}).",
                        scenario_name=sc.name,
                        severity="WARNING",
                    )
                    sc_warnings.append(w)
                    warnings.append(w)

                if dd_inc is not None and dd_inc > 50.0:
                    w = RobustnessWarning(
                        code="SEVERE_DRAWDOWN_DEGRADATION",
                        message=f"Scenario '{sc.name}' increased max drawdown by {dd_inc:.1f}% ({m.max_drawdown_pct:.2f}% vs {b_metrics.max_drawdown_pct:.2f}%).",
                        scenario_name=sc.name,
                        severity="WARNING",
                    )
                    sc_warnings.append(w)
                    warnings.append(w)

                s_result = StressScenarioResult(
                    scenario=sc,
                    status="COMPLETED",
                    backtest_result=sc_res,
                    delta_net_pnl=delta_pnl,
                    delta_return_pct=delta_ret,
                    delta_max_drawdown_pct=delta_dd,
                    delta_trades=delta_trades,
                    delta_win_rate=delta_win,
                    delta_transaction_costs=delta_costs,
                    return_degradation_pct=ret_deg,
                    drawdown_increase_pct=dd_inc,
                    warnings=tuple(sc_warnings),
                    duration_ms=sc_duration,
                )
            else:
                s_result = StressScenarioResult(
                    scenario=sc,
                    status="FAILED",
                    backtest_result=sc_res,
                    error_message=sc_res.error_message or "Scenario backtest execution failed",
                    duration_ms=sc_duration,
                )

            scenario_results.append(s_result)

            if self.event_bus:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.ROBUSTNESS_TEST_PROGRESS,
                        source=self.name,
                        correlation_id=cid,
                        payload={
                            "test_id": test_id,
                            "completed_scenarios": len(scenario_results),
                            "total_scenarios": len(scenarios),
                            "latest_scenario": sc.name,
                            "latest_status": s_result.status,
                        },
                    )
                )

        # 6. Parameter Sensitivity Analysis (Bounded Variants)
        param_results: List[ParameterSensitivityResult] = []
        if config.include_parameter_sensitivity:
            param_variants = self._resolve_parameter_variants(config, resolved_strategy)
            for p_dict in param_variants:
                if stop_event.is_set():
                    break
                p_start = time.perf_counter()
                v_name = "_".join(f"{k}{v}" for k, v in sorted(p_dict.items()))
                try:
                    # Instantiate strategy variant through registry
                    v_strategy = self.strategy_registry.get(config.strategy_name, **p_dict)
                    v_res = self.engine.run(
                        config=baseline_cfg,
                        strategy=v_strategy,
                        cancellation_token=stop_event,
                    )
                    p_dur = (time.perf_counter() - p_start) * 1000.0

                    if v_res.is_success and v_res.metrics:
                        vm = v_res.metrics
                        param_results.append(
                            ParameterSensitivityResult(
                                variant_name=v_name,
                                parameters=p_dict,
                                status="COMPLETED",
                                metrics=vm,
                                delta_return_pct=vm.total_return_pct - b_metrics.total_return_pct,
                                delta_max_drawdown_pct=vm.max_drawdown_pct - b_metrics.max_drawdown_pct,
                                delta_trades=vm.total_trades - b_metrics.total_trades,
                                delta_win_rate=vm.win_rate - b_metrics.win_rate,
                                duration_ms=p_dur,
                            )
                        )
                    else:
                        param_results.append(
                            ParameterSensitivityResult(
                                variant_name=v_name,
                                parameters=p_dict,
                                status="FAILED",
                                error_message=v_res.error_message,
                                duration_ms=p_dur,
                            )
                        )
                except Exception as err:
                    logger.warning("Parameter sensitivity variant [%s] failed: %s", v_name, err)
                    param_results.append(
                        ParameterSensitivityResult(
                            variant_name=v_name,
                            parameters=p_dict,
                            status="FAILED",
                            error_message=str(err),
                            duration_ms=(time.perf_counter() - p_start) * 1000.0,
                        )
                    )

        # 7. Historical Window Comparisons
        window_results: List[HistoricalWindowComparison] = []
        if config.include_historical_windows and config.historical_windows:
            for w_start, w_end, w_label in config.historical_windows:
                if stop_event.is_set():
                    break
                w_start_perf = time.perf_counter()
                w_cfg = BacktestConfig(
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    start_time=w_start,
                    end_time=w_end,
                    initial_cash=config.initial_cash,
                    position_size_type=config.position_size_type,
                    position_size_value=config.position_size_value,
                    fee_model=config.fee_model,
                    fee_rate=config.fee_rate,
                    fixed_fee=config.fixed_fee,
                    slippage_bps=config.slippage_bps,
                    half_spread_bps=config.half_spread_bps,
                    execution_model=config.execution_model,
                    reset_state_between_runs=True,
                    correlation_id=cid,
                )
                bars = self.repository.query_bars(
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    start_time=w_start,
                    end_time=w_end,
                )
                w_res = self.engine.run(
                    config=w_cfg,
                    strategy=resolved_strategy,
                    cancellation_token=stop_event,
                )
                w_dur = (time.perf_counter() - w_start_perf) * 1000.0

                w_warns: List[RobustnessWarning] = []
                if len(bars) < 20:
                    w_warns.append(
                        RobustnessWarning(
                            code="FEW_WINDOW_OBSERVATIONS",
                            message=f"Historical window '{w_label}' has only {len(bars)} bars.",
                            scenario_name=w_label,
                            severity="WARNING",
                        )
                    )

                if w_res.is_success and w_res.metrics:
                    wm = w_res.metrics
                    window_results.append(
                        HistoricalWindowComparison(
                            window_name=w_label,
                            start_time=w_start,
                            end_time=w_end,
                            bars_count=len(bars),
                            status="COMPLETED",
                            metrics=wm,
                            delta_return_pct=wm.total_return_pct - b_metrics.total_return_pct,
                            delta_max_drawdown_pct=wm.max_drawdown_pct - b_metrics.max_drawdown_pct,
                            warnings=tuple(w_warns),
                            duration_ms=w_dur,
                        )
                    )
                else:
                    window_results.append(
                        HistoricalWindowComparison(
                            window_name=w_label,
                            start_time=w_start,
                            end_time=w_end,
                            bars_count=len(bars),
                            status="FAILED",
                            warnings=tuple(w_warns),
                            duration_ms=w_dur,
                        )
                    )

        # 8. Synthesize Overall Summary
        summary = self._synthesize_summary(baseline_res, scenario_results, param_results, window_results)

        duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
        final_result = RobustnessTestResult(
            test_id=test_id,
            status=RobustnessStatus.COMPLETED,
            strategy_name=resolved_strategy.name,
            strategy_version=resolved_strategy.version,
            config=config,
            baseline_result=baseline_res,
            summary=summary,
            scenario_results=tuple(scenario_results),
            parameter_results=tuple(param_results),
            window_results=tuple(window_results),
            warnings=tuple(warnings),
            duration_ms=duration_ms,
            run_id=analysis_run.run_id if analysis_run else None,
        )

        self._cleanup_and_finish(final_result, cid)
        return final_result

    def _generate_scenarios(self, config: RobustnessTestConfig) -> List[StressScenario]:
        """Generate full list of stress scenarios based on config specifications."""
        scenarios: List[StressScenario] = []

        if config.include_cost_stress:
            # 1. Commission Stress
            for mult in config.commission_multipliers:
                scenarios.append(
                    StressScenario(
                        name=f"commission_{mult}x",
                        description=f"Commission rate elevated to {mult}x baseline",
                        scenario_type=ScenarioType.COMMISSION_STRESS.value,
                        fee_rate=config.fee_rate * mult,
                        fixed_fee=config.fixed_fee * mult,
                    )
                )

            # 2. Spread Stress
            for mult in config.spread_multipliers:
                scenarios.append(
                    StressScenario(
                        name=f"spread_{mult}x",
                        description=f"Bid-ask half spread widened to {mult}x baseline",
                        scenario_type=ScenarioType.SPREAD_STRESS.value,
                        half_spread_bps=config.half_spread_bps * mult,
                    )
                )

            # 3. Slippage Stress
            for mult in config.slippage_multipliers:
                scenarios.append(
                    StressScenario(
                        name=f"slippage_{mult}x",
                        description=f"Execution slippage increased to {mult}x baseline",
                        scenario_type=ScenarioType.SLIPPAGE_STRESS.value,
                        slippage_bps=config.slippage_bps * mult,
                    )
                )

            # 4. Combined Adverse Cost Stress
            scenarios.append(
                StressScenario(
                    name="combined_adverse_costs",
                    description="Severe adverse combination: 2x commission, 3x spread, and 3x slippage",
                    scenario_type=ScenarioType.COMBINED_COST_STRESS.value,
                    fee_rate=config.fee_rate * 2.0,
                    fixed_fee=config.fixed_fee * 2.0,
                    half_spread_bps=config.half_spread_bps * 3.0,
                    slippage_bps=config.slippage_bps * 3.0,
                )
            )

        # Include custom scenarios
        for custom in config.custom_scenarios:
            scenarios.append(custom)

        return scenarios

    def _build_scenario_backtest_config(
        self,
        base_config: RobustnessTestConfig,
        scenario: StressScenario,
    ) -> BacktestConfig:
        """Construct BacktestConfig for a specific scenario by applying isolated overrides."""
        start_t = scenario.window_start if scenario.window_start is not None else base_config.start_time
        end_t = scenario.window_end if scenario.window_end is not None else base_config.end_time

        f_rate = scenario.fee_rate if scenario.fee_rate is not None else base_config.fee_rate
        f_fixed = scenario.fixed_fee if scenario.fixed_fee is not None else base_config.fixed_fee
        slip = scenario.slippage_bps if scenario.slippage_bps is not None else base_config.slippage_bps
        spread = scenario.half_spread_bps if scenario.half_spread_bps is not None else base_config.half_spread_bps

        return BacktestConfig(
            symbol=base_config.symbol,
            timeframe=base_config.timeframe,
            start_time=start_t,
            end_time=end_t,
            initial_cash=base_config.initial_cash,
            position_size_type=base_config.position_size_type,
            position_size_value=base_config.position_size_value,
            fee_model=base_config.fee_model,
            fee_rate=f_rate,
            fixed_fee=f_fixed,
            slippage_bps=slip,
            half_spread_bps=spread,
            execution_model=base_config.execution_model,
            reset_state_between_runs=True,
            correlation_id=base_config.correlation_id,
        )

    def _resolve_parameter_variants(
        self,
        config: RobustnessTestConfig,
        strategy: Strategy,
    ) -> List[Dict[str, Any]]:
        """Resolve and validate parameter variants for sensitivity testing."""
        if config.parameter_variants:
            variants = list(config.parameter_variants)
        elif strategy.name == "sma_crossover":
            # Default bounded grid for demonstration SMA crossover strategy
            variants = [
                {"fast_period": 3, "slow_period": 10},
                {"fast_period": 4, "slow_period": 12},
                {"fast_period": 5, "slow_period": 15},  # baseline
                {"fast_period": 7, "slow_period": 20},
                {"fast_period": 10, "slow_period": 30},
            ]
        else:
            variants = []

        # Pass bounded parameter variants for evaluation
        valid_variants: List[Dict[str, Any]] = [dict(p) for p in variants[:20]]
        return valid_variants

    def _synthesize_summary(
        self,
        baseline_result: BacktestResult,
        scenario_results: Sequence[StressScenarioResult],
        param_results: Sequence[ParameterSensitivityResult],
        window_results: Sequence[HistoricalWindowComparison],
    ) -> RobustnessSummary:
        """Mathematically aggregate findings across baseline and all evaluated scenarios."""
        completed = [r for r in scenario_results if r.is_completed and r.metrics is not None]
        failed = [r for r in scenario_results if r.status == "FAILED"]
        skipped = [r for r in scenario_results if r.status == "SKIPPED"]

        b_metrics = baseline_result.metrics
        b_ret = b_metrics.total_return_pct
        b_dd = b_metrics.max_drawdown_pct
        b_pnl = b_metrics.net_profit
        b_trades = b_metrics.total_trades

        if not completed:
            return RobustnessSummary(
                total_scenarios_evaluated=len(scenario_results),
                completed_scenarios=0,
                failed_scenarios=len(failed),
                skipped_scenarios=len(skipped),
                baseline_return_pct=b_ret,
                baseline_max_drawdown_pct=b_dd,
                baseline_net_pnl=b_pnl,
                baseline_trades=b_trades,
                worst_case_return_pct=b_ret,
                worst_case_scenario_name="baseline",
                worst_case_max_drawdown_pct=b_dd,
                worst_case_drawdown_scenario_name="baseline",
                mean_scenario_return_pct=b_ret,
                max_cost_impact_pnl=0.0,
                max_cost_scenario_name="none",
                profitable_scenarios_count=1 if b_pnl > 0 else 0,
                unprofitable_scenarios_count=0 if b_pnl > 0 else 1,
                scenario_pass_rate_pct=100.0 if b_pnl > 0 else 0.0,
                parameter_variants_count=len(param_results),
                historical_windows_count=len(window_results),
            )

        returns = [r.metrics.total_return_pct for r in completed]
        drawdowns = [r.metrics.max_drawdown_pct for r in completed]
        cost_impacts = [r.delta_net_pnl for r in completed]

        # Worst Return Scenario
        worst_ret = min(returns)
        worst_ret_idx = returns.index(worst_ret)
        worst_ret_sc_name = completed[worst_ret_idx].scenario.name

        # Worst Drawdown Scenario
        worst_dd = max(drawdowns)
        worst_dd_idx = drawdowns.index(worst_dd)
        worst_dd_sc_name = completed[worst_dd_idx].scenario.name

        # Max Negative Cost Impact
        max_cost_impact = min(cost_impacts)  # Most negative delta
        max_cost_idx = cost_impacts.index(max_cost_impact)
        max_cost_sc_name = completed[max_cost_idx].scenario.name

        mean_return = sum(returns) / len(returns)
        profitable_count = sum(1 for r in completed if r.metrics.net_profit > 0.0)
        unprofitable_count = sum(1 for r in completed if r.metrics.net_profit <= 0.0)
        pass_rate = (profitable_count / len(completed) * 100.0) if completed else 0.0

        return RobustnessSummary(
            total_scenarios_evaluated=len(scenario_results),
            completed_scenarios=len(completed),
            failed_scenarios=len(failed),
            skipped_scenarios=len(skipped),
            baseline_return_pct=b_ret,
            baseline_max_drawdown_pct=b_dd,
            baseline_net_pnl=b_pnl,
            baseline_trades=b_trades,
            worst_case_return_pct=worst_ret,
            worst_case_scenario_name=worst_ret_sc_name,
            worst_case_max_drawdown_pct=worst_dd,
            worst_case_drawdown_scenario_name=worst_dd_sc_name,
            mean_scenario_return_pct=mean_return,
            max_cost_impact_pnl=max_cost_impact,
            max_cost_scenario_name=max_cost_sc_name,
            profitable_scenarios_count=profitable_count,
            unprofitable_scenarios_count=unprofitable_count,
            scenario_pass_rate_pct=pass_rate,
            parameter_variants_count=len(param_results),
            historical_windows_count=len(window_results),
        )

    def _create_empty_summary(self) -> RobustnessSummary:
        """Create a safe empty summary when baseline execution fails."""
        return RobustnessSummary(
            total_scenarios_evaluated=0,
            completed_scenarios=0,
            failed_scenarios=0,
            skipped_scenarios=0,
            baseline_return_pct=0.0,
            baseline_max_drawdown_pct=0.0,
            baseline_net_pnl=0.0,
            baseline_trades=0,
            worst_case_return_pct=0.0,
            worst_case_scenario_name="none",
            worst_case_max_drawdown_pct=0.0,
            worst_case_drawdown_scenario_name="none",
            mean_scenario_return_pct=0.0,
            max_cost_impact_pnl=0.0,
            max_cost_scenario_name="none",
            profitable_scenarios_count=0,
            unprofitable_scenarios_count=0,
            scenario_pass_rate_pct=0.0,
        )

    def _cleanup_and_finish(self, result: RobustnessTestResult, cid: str) -> None:
        """Clean up active test tracking and broadcast terminal lifecycle events."""
        with self._lock:
            self._active_tests.pop(result.test_id, None)

        if self.state_store:
            self.state_store.set(
                "robustness.latest",
                {
                    "test_id": result.test_id,
                    "status": result.status.value,
                    "strategy": result.strategy_name,
                    "symbol": result.config.symbol,
                    "timeframe": result.config.timeframe,
                    "completed_scenarios": result.summary.completed_scenarios,
                    "total_scenarios": result.summary.total_scenarios_evaluated,
                    "baseline_return_pct": result.summary.baseline_return_pct,
                    "worst_case_return_pct": result.summary.worst_case_return_pct,
                    "worst_case_scenario": result.summary.worst_case_scenario_name,
                    "pass_rate_pct": result.summary.scenario_pass_rate_pct,
                    "duration_ms": result.duration_ms,
                },
            )

        if getattr(self, "run_registry", None) is not None and getattr(result, "run_id", None):
            try:
                if result.status == RobustnessStatus.COMPLETED:
                    self.run_registry.complete_run(
                        run_id=result.run_id,
                        result_id=result.test_id,
                        result_type="RobustnessTestResult",
                    )
                elif result.status == RobustnessStatus.CANCELLED:
                    self.run_registry.cancel_run(
                        run_id=result.run_id,
                        cancellation_reason="Robustness test cancelled",
                    )
                else:
                    self.run_registry.fail_run(
                        run_id=result.run_id,
                        error_category="ROBUSTNESS_ERROR",
                        error_message=result.error_message or "Robustness test failure",
                    )
            except Exception as exc:
                logger.warning("Failed to finalize analysis run in robustness cleanup: %s", exc)

        if self.event_bus:
            evt_type = (
                EventType.ROBUSTNESS_TEST_COMPLETED
                if result.status == RobustnessStatus.COMPLETED
                else EventType.ROBUSTNESS_TEST_CANCELLED
                if result.status == RobustnessStatus.CANCELLED
                else EventType.ROBUSTNESS_TEST_FAILED
            )
            self.event_bus.publish(
                Event.create(
                    event_type=evt_type,
                    source=self.name,
                    correlation_id=cid,
                    payload=result.to_dict(),
                )
            )
