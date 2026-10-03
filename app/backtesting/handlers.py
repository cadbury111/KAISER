"""KAISER — Backtesting Orchestrator Command Handlers.

Registers `run_backtest` and `list_backtest_strategies` commands with
the MasterOrchestrator, providing structured parameter parsing, validation,
and voice/CLI compatible response formats.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from app.analysis.models import TIMEFRAME_SECONDS, normalize_timeframe
from app.backtesting.engine import BacktestEngine
from app.backtesting.models import BacktestConfig
from app.backtesting.robustness.engine import RobustnessTestEngine
from app.backtesting.robustness.models import RobustnessTestConfig
from app.backtesting.strategy import StrategyRegistry
from app.backtesting.validation.models import WalkForwardConfig
from app.backtesting.validation.validator import WalkForwardValidator
from app.core.exceptions import ValidationError
from app.market.normalizer import parse_timestamp
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator


def register_backtest_handlers(
    orchestrator: MasterOrchestrator,
    engine: BacktestEngine,
    strategy_registry: Optional[StrategyRegistry] = None,
    validator: Optional[WalkForwardValidator] = None,
    robustness_engine: Optional[RobustnessTestEngine] = None,
) -> None:
    """Register backtesting, validation, and robustness commands with the Master Orchestrator.

    Commands registered:
    - `run_backtest`: Execute deterministic backtest across stored historical data.
    - `list_backtest_strategies`: List all available research strategies.
    - `run_walk_forward_validation`: Execute chronological in-sample/out-of-sample validation.
    - `run_robustness_test`: Execute transaction-cost stress testing and parameter sensitivity analysis.
    """
    registry = strategy_registry or getattr(engine, "strategy_registry", StrategyRegistry())

    def handle_run_backtest(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'run_backtest' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        symbol_raw = p.get("symbol")
        if not symbol_raw or not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ValidationError("Field 'symbol' is required for backtesting.")

        timeframe_raw = str(p.get("timeframe", "15m")).strip()
        timeframe = normalize_timeframe(timeframe_raw)

        strategy_name = str(p.get("strategy") or p.get("strategy_name", "sma_crossover")).strip()
        if not registry.has_strategy(strategy_name):
            available = [s["name"] for s in registry.list_strategies()]
            raise ValidationError(
                f"Unknown strategy '{strategy_name}'. Available strategies: {available}"
            )

        # Parse timestamps
        end_val = p.get("end_time") or p.get("end_iso")
        end_time = parse_timestamp(end_val) if end_val is not None else time.time()

        interval = TIMEFRAME_SECONDS.get(timeframe, 60)
        start_val = p.get("start_time") or p.get("start_iso")
        start_time = parse_timestamp(start_val) if start_val is not None else end_time - (200 * interval)

        initial_cash = float(p.get("initial_cash", 100_000.0))
        size_type = str(p.get("position_size_type", "fixed_quantity")).strip()
        size_val = float(p.get("position_size_value", 10.0))
        fee_model = str(p.get("fee_model", "percentage")).strip()
        fee_rate = float(p.get("fee_rate", 0.0005))
        fixed_fee = float(p.get("fixed_fee", 0.0))
        slippage_bps = float(p.get("slippage_bps", 2.0))
        half_spread_bps = float(p.get("half_spread_bps", 1.0))
        max_drawdown_limit = float(p["max_drawdown_limit_pct"]) if "max_drawdown_limit_pct" in p else None
        execution_model = str(p.get("execution_model", "next_bar_open")).strip()
        limit = int(p.get("limit", 5000))
        reset_state = bool(p.get("reset_state_between_runs", True))

        config = BacktestConfig(
            symbol=symbol_raw.strip().upper(),
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            initial_cash=initial_cash,
            position_size_type=size_type,
            position_size_value=size_val,
            fee_model=fee_model,
            fee_rate=fee_rate,
            fixed_fee=fixed_fee,
            slippage_bps=slippage_bps,
            half_spread_bps=half_spread_bps,
            max_drawdown_limit_pct=max_drawdown_limit,
            execution_model=execution_model,
            limit=limit,
            reset_state_between_runs=reset_state,
            correlation_id=req.correlation_id,
        )

        result = engine.run(config=config, strategy=strategy_name)

        return {
            "status": "success" if result.is_success else "failed",
            "backtest_status": result.status.value,
            "message": result.summary_text,
            "summary": result.summary_text,
            "data": result.to_dict(),
            "metrics": result.metrics.to_dict(),
            "trades_count": len(result.trades),
            "disclaimer": result.disclaimer,
        }

    orchestrator.register_handler(
        command="run_backtest",
        handler=handle_run_backtest,
        capability="backtesting",
        description="Run deterministic historical strategy backtest in virtual portfolio",
        allow_override=True,
    )

    def handle_list_backtest_strategies(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_backtest_strategies' command."""
        strategies = registry.list_strategies()
        return {
            "status": "success",
            "count": len(strategies),
            "strategies": strategies,
        }

    orchestrator.register_handler(
        command="list_backtest_strategies",
        handler=handle_list_backtest_strategies,
        capability="backtesting",
        description="List registered research strategies available for backtesting",
        allow_override=True,
    )

    def handle_run_walk_forward_validation(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'run_walk_forward_validation' command."""
        val_engine = validator
        if not val_engine:
            val_engine = WalkForwardValidator(
                engine=engine,
                repository=engine.repository,
                quality_engine=engine.quality_engine,
                strategy_registry=registry,
                event_bus=engine.event_bus,
                state_store=engine.state_store,
            )

        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        symbol_raw = p.get("symbol")
        if not symbol_raw or not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ValidationError("Field 'symbol' is required for walk-forward validation.")

        timeframe_raw = str(p.get("timeframe", "15m")).strip()
        timeframe = normalize_timeframe(timeframe_raw)

        strategy_name = str(p.get("strategy") or p.get("strategy_name", "sma_crossover")).strip()
        if not registry.has_strategy(strategy_name):
            available = [s["name"] for s in registry.list_strategies()]
            raise ValidationError(
                f"Unknown strategy '{strategy_name}'. Available strategies: {available}"
            )

        interval = TIMEFRAME_SECONDS.get(timeframe, 60)

        # Parse start and end times
        end_val = p.get("end_time") or p.get("end_iso")
        end_time = parse_timestamp(end_val) if end_val is not None else time.time()

        train_dur = float(p.get("train_duration_seconds", 300 * interval))
        test_dur = float(p.get("test_duration_seconds", 100 * interval))
        step_dur = float(p["step_duration_seconds"]) if "step_duration_seconds" in p else test_dur
        val_dur = float(p.get("validation_duration_seconds", 0.0))

        start_val = p.get("start_time") or p.get("start_iso")
        start_time = parse_timestamp(start_val) if start_val is not None else end_time - (train_dur + test_dur + 50 * interval)

        window_mode = str(p.get("window_mode", "rolling")).strip()
        min_is_bars = int(p.get("min_in_sample_bars", 20))
        min_oos_bars = int(p.get("min_out_of_sample_bars", 10))
        warmup_bars = int(p.get("warmup_bars", 0))

        initial_cash = float(p.get("initial_cash", 100_000.0))
        size_type = str(p.get("position_size_type", "fixed_quantity")).strip()
        size_val = float(p.get("position_size_value", 10.0))
        fee_model = str(p.get("fee_model", "percentage")).strip()
        fee_rate = float(p.get("fee_rate", 0.0005))
        fixed_fee = float(p.get("fixed_fee", 0.0))
        slippage_bps = float(p.get("slippage_bps", 2.0))
        half_spread_bps = float(p.get("half_spread_bps", 1.0))
        execution_model = str(p.get("execution_model", "next_bar_open")).strip()
        reinvest = bool(p.get("reinvest_profits", False))

        wf_config = WalkForwardConfig(
            symbol=symbol_raw.strip().upper(),
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            train_duration_seconds=train_dur,
            test_duration_seconds=test_dur,
            step_duration_seconds=step_dur,
            validation_duration_seconds=val_dur,
            window_mode=window_mode,
            min_in_sample_bars=min_is_bars,
            min_out_of_sample_bars=min_oos_bars,
            warmup_bars=warmup_bars,
            initial_cash=initial_cash,
            position_size_type=size_type,
            position_size_value=size_val,
            fee_model=fee_model,
            fee_rate=fee_rate,
            fixed_fee=fixed_fee,
            slippage_bps=slippage_bps,
            half_spread_bps=half_spread_bps,
            execution_model=execution_model,
            reinvest_profits=reinvest,
            correlation_id=req.correlation_id,
        )

        result = val_engine.run(config=wf_config, strategy=strategy_name)

        return {
            "status": "success" if result.is_success else "failed",
            "validation_status": result.status.value,
            "message": result.summary_text,
            "summary": result.summary_text,
            "data": result.to_dict(),
            "summary_metrics": result.summary.to_dict(),
            "windows_count": len(result.windows),
            "completed_windows": result.summary.completed_windows,
            "disclaimer": result.disclaimer,
        }

    orchestrator.register_handler(
        command="run_walk_forward_validation",
        handler=handle_run_walk_forward_validation,
        capability="backtesting",
        description="Run chronological in-sample/out-of-sample walk-forward validation",
        allow_override=True,
    )

    def handle_run_robustness_test(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'run_robustness_test' command."""
        rob_engine = robustness_engine
        if not rob_engine:
            raise ValidationError(
                "Robustness test engine is not configured in this environment."
            )

        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        symbol_raw = p.get("symbol")
        if not symbol_raw or not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ValidationError("Field 'symbol' is required for robustness testing.")

        timeframe_raw = str(p.get("timeframe", "15m")).strip()
        timeframe = normalize_timeframe(timeframe_raw)

        strategy_name = str(p.get("strategy") or p.get("strategy_name", "sma_crossover")).strip()
        if not registry.has_strategy(strategy_name):
            available = [s["name"] for s in registry.list_strategies()]
            raise ValidationError(
                f"Unknown strategy '{strategy_name}'. Available strategies: {available}"
            )

        interval = TIMEFRAME_SECONDS.get(timeframe, 60)

        end_val = p.get("end_time") or p.get("end_iso")
        end_time = parse_timestamp(end_val) if end_val is not None else time.time()

        start_val = p.get("start_time") or p.get("start_iso")
        start_time = parse_timestamp(start_val) if start_val is not None else end_time - (500 * interval)

        initial_cash = float(p.get("initial_cash", 100_000.0))
        size_type = str(p.get("position_size_type", "fixed_quantity")).strip()
        size_val = float(p.get("position_size_value", 10.0))
        fee_model = str(p.get("fee_model", "percentage")).strip()
        fee_rate = float(p.get("fee_rate", 0.0005))
        fixed_fee = float(p.get("fixed_fee", 0.0))
        slippage_bps = float(p.get("slippage_bps", 2.0))
        half_spread_bps = float(p.get("half_spread_bps", 1.0))
        execution_model = str(p.get("execution_model", "next_bar_open")).strip()

        include_costs = bool(p.get("include_cost_stress", True))
        include_params = bool(p.get("include_parameter_sensitivity", True))
        include_windows = bool(p.get("include_historical_windows", False))

        param_variants = list(p.get("parameter_variants", []) or [])
        custom_scenarios = list(p.get("custom_scenarios", []) or [])
        historical_windows = list(p.get("historical_windows", []) or [])

        rob_config = RobustnessTestConfig(
            symbol=symbol_raw.strip().upper(),
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            strategy_name=strategy_name,
            initial_cash=initial_cash,
            position_size_type=size_type,
            position_size_value=size_val,
            fee_model=fee_model,
            fee_rate=fee_rate,
            fixed_fee=fixed_fee,
            slippage_bps=slippage_bps,
            half_spread_bps=half_spread_bps,
            execution_model=execution_model,
            include_cost_stress=include_costs,
            include_parameter_sensitivity=include_params,
            include_historical_windows=include_windows,
            parameter_variants=param_variants,
            historical_windows=historical_windows,
            correlation_id=req.correlation_id,
        )

        result = rob_engine.run(config=rob_config, strategy=strategy_name)

        return {
            "status": "success" if result.is_success else "failed",
            "robustness_status": result.status.value,
            "message": result.summary_text,
            "summary": result.summary_text,
            "data": result.to_dict(),
            "summary_metrics": result.summary.to_dict(),
            "scenarios_count": len(result.scenario_results),
            "completed_scenarios": result.summary.completed_scenarios,
            "baseline_return_pct": result.summary.baseline_return_pct,
            "worst_case_return_pct": result.summary.worst_case_return_pct,
            "worst_case_scenario": result.summary.worst_case_scenario_name,
            "disclaimer": result.disclaimer,
        }

    orchestrator.register_handler(
        command="run_robustness_test",
        handler=handle_run_robustness_test,
        capability="backtesting",
        description="Run transaction-cost stress testing and parameter sensitivity analysis",
        allow_override=True,
    )

