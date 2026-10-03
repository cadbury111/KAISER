"""KAISER — Walk-Forward Validator Engine.

Executes deterministic, chronological in-sample training and out-of-sample testing
across sequential market partitions using the existing BacktestEngine,
historical data repository, and market data quality gates.

SAFETY INVARIANTS:
1. Research and evaluation only: Never executes real trades or connects to a broker.
2. Segregation of concerns: Strategy evaluates in-sample and out-of-sample data sequentially.
3. Frozen strategy state: Parameters and logic are frozen before evaluating each test period.
4. Overfitting warning: Out-of-sample validation provides historical robustness checks
   but cannot guarantee future profitability.
"""

from __future__ import annotations

import statistics
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Union

from app.backtesting.engine import BacktestEngine
from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    BacktestStatus,
    PerformanceMetrics,
)
from app.backtesting.strategy import Strategy, StrategyRegistry
from app.backtesting.validation.models import (
    OutOfSampleSummary,
    ValidationWarning,
    ValidationWindow,
    ValidationWindowResult,
    WalkForwardConfig,
    WalkForwardResult,
    WalkForwardStatus,
)
from app.backtesting.validation.windows import (
    generate_walk_forward_windows,
    validate_window_continuity,
)
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.historical.storage import HistoricalBarRepository
from app.market.quality.engine import MarketDataQualityEngine

logger = get_logger("backtesting.validation.validator")


class WalkForwardValidator:
    """Orchestrates chronological in-sample and out-of-sample walk-forward evaluation."""

    def __init__(
        self,
        engine: BacktestEngine,
        repository: HistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        strategy_registry: Optional[StrategyRegistry] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        name: str = "walk_forward_validator",
    ) -> None:
        self.name = name
        self.engine = engine
        self.repository = repository
        self.quality_engine = quality_engine
        self.strategy_registry = strategy_registry or getattr(engine, "strategy_registry", StrategyRegistry())
        self.event_bus = event_bus
        self.state_store = state_store

        self._lock = threading.RLock()
        self._active_validations: Dict[str, threading.Event] = {}

    @property
    def active_validation_count(self) -> int:
        """Return the number of validation jobs currently executing."""
        with self._lock:
            return len(self._active_validations)

    def cancel_validation(self, validation_id: str) -> bool:
        """Signal cooperative cancellation to an active validation session."""
        with self._lock:
            evt = self._active_validations.get(validation_id)
            if evt:
                evt.set()
                logger.info("Cancellation requested for walk-forward validation '%s'", validation_id)
                return True
            return False

    def run(
        self,
        config: WalkForwardConfig,
        strategy: Union[Strategy, str],
        cancellation_token: Optional[threading.Event] = None,
    ) -> WalkForwardResult:
        """Execute a full walk-forward validation across chronological windows.

        Args:
            config: Validated WalkForwardConfig parameters.
            strategy: Strategy instance or registered identifier.
            cancellation_token: Optional cooperative cancellation token.

        Returns:
            WalkForwardResult: Detailed per-window and aggregate evaluation report.
        """
        start_time_perf = time.perf_counter()
        val_id = config.validation_id or f"wf_{uuid.uuid4().hex[:12]}"
        cid = config.correlation_id or uuid.uuid4().hex

        # 1. Resolve Strategy
        resolved_strategy: Strategy
        if isinstance(strategy, str):
            resolved_strategy = self.strategy_registry.get(strategy)
        elif isinstance(strategy, Strategy):
            resolved_strategy = strategy
        else:
            raise ValidationError(f"Invalid strategy type: {type(strategy)}. Expected Strategy or str.")

        # Setup cancellation tracking
        stop_event = cancellation_token or threading.Event()
        with self._lock:
            self._active_validations[val_id] = stop_event

        # 2. Generate Chronological Windows
        try:
            windows = generate_walk_forward_windows(config)
        except Exception as err:
            logger.error("Failed to generate windows for validation [%s]: %s", val_id, err)
            duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
            empty_summary = self._create_empty_summary(0)
            result = WalkForwardResult(
                validation_id=val_id,
                status=WalkForwardStatus.FAILED,
                strategy_name=resolved_strategy.name,
                strategy_version=resolved_strategy.version,
                config=config,
                summary=empty_summary,
                error_message=str(err),
                duration_ms=duration_ms,
            )
            self._cleanup_and_finish(result, cid)
            return result

        warnings: List[ValidationWarning] = list(validate_window_continuity(windows, config))

        # 3. Publish Started Event
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.WALK_FORWARD_STARTED,
                    source=self.name,
                    correlation_id=cid,
                    payload={
                        "validation_id": val_id,
                        "strategy": resolved_strategy.name,
                        "symbol": config.symbol,
                        "timeframe": config.timeframe,
                        "total_windows": len(windows),
                        "window_mode": config.window_mode,
                    },
                )
            )

        # 4. Sequential Window Evaluation Loop
        window_results: List[ValidationWindowResult] = []
        last_ending_equity: float = config.initial_cash
        total_bars_queried = 0
        total_bars_accepted = 0
        total_bars_rejected = 0

        for w in windows:
            # Check cooperative cancellation
            if stop_event.is_set():
                logger.warning("Walk-forward validation [%s] cancelled after %d windows", val_id, len(window_results))
                duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
                summary = self._aggregate_out_of_sample_summary(len(windows), window_results)
                result = WalkForwardResult(
                    validation_id=val_id,
                    status=WalkForwardStatus.CANCELLED,
                    strategy_name=resolved_strategy.name,
                    strategy_version=resolved_strategy.version,
                    config=config,
                    summary=summary,
                    windows=tuple(window_results),
                    warnings=tuple(warnings),
                    duration_ms=duration_ms,
                )
                self._cleanup_and_finish(result, cid)
                return result

            w_start_perf = time.perf_counter()

            # A. Query bars in in-sample and out-of-sample partitions
            train_bars = self.repository.query_bars(
                symbol=config.symbol,
                timeframe=config.timeframe,
                start_time=w.train_start,
                end_time=w.train_end,
                limit=10_000,
            )
            test_bars = self.repository.query_bars(
                symbol=config.symbol,
                timeframe=config.timeframe,
                start_time=w.test_start,
                end_time=w.test_end,
                limit=10_000,
            )

            total_bars_queried += len(train_bars) + len(test_bars)

            # B. Eligibility checks for observations
            if len(train_bars) < config.min_in_sample_bars:
                w_duration = (time.perf_counter() - w_start_perf) * 1000.0
                skip_w = ValidationWarning(
                    code="INSUFFICIENT_IN_SAMPLE",
                    message=(
                        f"Window {w.window_id} skipped: in-sample bars ({len(train_bars)}) "
                        f"< required minimum ({config.min_in_sample_bars})"
                    ),
                    window_id=w.window_id,
                    severity="WARNING",
                )
                warnings.append(skip_w)
                window_results.append(
                    ValidationWindowResult(
                        window=w,
                        status="SKIPPED",
                        in_sample_bars=len(train_bars),
                        out_of_sample_bars=len(test_bars),
                        skip_or_fail_reason=skip_w.message,
                        warnings=(skip_w,),
                        duration_ms=w_duration,
                    )
                )
                continue

            if len(test_bars) < config.min_out_of_sample_bars:
                w_duration = (time.perf_counter() - w_start_perf) * 1000.0
                skip_w = ValidationWarning(
                    code="INSUFFICIENT_OUT_OF_SAMPLE",
                    message=(
                        f"Window {w.window_id} skipped: out-of-sample bars ({len(test_bars)}) "
                        f"< required minimum ({config.min_out_of_sample_bars})"
                    ),
                    window_id=w.window_id,
                    severity="WARNING",
                )
                warnings.append(skip_w)
                window_results.append(
                    ValidationWindowResult(
                        window=w,
                        status="SKIPPED",
                        in_sample_bars=len(train_bars),
                        out_of_sample_bars=len(test_bars),
                        skip_or_fail_reason=skip_w.message,
                        warnings=(skip_w,),
                        duration_ms=w_duration,
                    )
                )
                continue

            # C. In-Sample Training Period Backtest
            is_config = BacktestConfig(
                symbol=config.symbol,
                timeframe=config.timeframe,
                start_time=w.train_start,
                end_time=w.train_end,
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

            is_result = self.engine.run(
                config=is_config,
                strategy=resolved_strategy,
                cancellation_token=stop_event,
            )

            # D. Freeze Strategy Configuration
            # Note: Strategy logic and parameters are kept strictly identical for out-of-sample testing.
            # Fixed-configuration walk-forward evaluation avoids ungrounded fitting.

            # E. Out-of-Sample Testing Period Backtest
            oos_initial_cash = last_ending_equity if config.reinvest_profits else config.initial_cash
            oos_config = BacktestConfig(
                symbol=config.symbol,
                timeframe=config.timeframe,
                start_time=w.test_start,
                end_time=w.test_end,
                initial_cash=oos_initial_cash,
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

            oos_result = self.engine.run(
                config=oos_config,
                strategy=resolved_strategy,
                cancellation_token=stop_event,
            )

            if oos_result.is_success and config.reinvest_profits:
                last_ending_equity = oos_result.metrics.final_equity

            total_bars_accepted += is_result.bars_accepted + oos_result.bars_accepted
            total_bars_rejected += is_result.bars_rejected + oos_result.bars_rejected

            w_duration = (time.perf_counter() - w_start_perf) * 1000.0
            w_res = ValidationWindowResult(
                window=w,
                status="COMPLETED" if (is_result.is_success and oos_result.is_success) else "FAILED",
                in_sample_bars=len(train_bars),
                out_of_sample_bars=len(test_bars),
                in_sample_result=is_result,
                out_of_sample_result=oos_result,
                duration_ms=w_duration,
            )
            window_results.append(w_res)

            # Publish Progress Event
            if self.event_bus:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.WALK_FORWARD_PROGRESS,
                        source=self.name,
                        correlation_id=cid,
                        payload={
                            "validation_id": val_id,
                            "symbol": config.symbol,
                            "completed_windows": sum(1 for r in window_results if r.is_completed),
                            "total_windows": len(windows),
                            "latest_window_id": w.window_id,
                            "latest_oos_return": oos_result.metrics.total_return_pct if oos_result else 0.0,
                        },
                    )
                )

        # 5. Out-of-Sample Metrics Aggregation
        summary = self._aggregate_out_of_sample_summary(len(windows), window_results)

        # Evaluate Global Warnings
        if summary.completed_windows < 3:
            warnings.append(
                ValidationWarning(
                    code="FEW_COMPLETED_WINDOWS",
                    message=(
                        f"Only {summary.completed_windows} out of {summary.total_windows_generated} "
                        "windows completed successfully."
                    ),
                    severity="WARNING",
                )
            )

        if summary.total_oos_trades < 10:
            warnings.append(
                ValidationWarning(
                    code="FEW_OOS_TRADES",
                    message=(
                        f"Only {summary.total_oos_trades} total trades completed across all out-of-sample "
                        "windows. Statistical validity is constrained."
                    ),
                    severity="WARNING",
                )
            )

        if summary.skipped_windows > 0:
            warnings.append(
                ValidationWarning(
                    code="SKIPPED_WINDOWS_PRESENT",
                    message=f"{summary.skipped_windows} validation window(s) were skipped due to insufficient data.",
                    severity="INFO",
                )
            )

        duration_ms = (time.perf_counter() - start_time_perf) * 1000.0

        quality_summary = {
            "total_windows": len(windows),
            "completed_windows": summary.completed_windows,
            "skipped_windows": summary.skipped_windows,
            "failed_windows": summary.failed_windows,
            "total_bars_queried": total_bars_queried,
            "total_bars_accepted": total_bars_accepted,
            "total_bars_rejected": total_bars_rejected,
        }

        result = WalkForwardResult(
            validation_id=val_id,
            status=WalkForwardStatus.COMPLETED,
            strategy_name=resolved_strategy.name,
            strategy_version=resolved_strategy.version,
            config=config,
            summary=summary,
            windows=tuple(window_results),
            warnings=tuple(warnings),
            data_quality_summary=quality_summary,
            duration_ms=duration_ms,
        )

        self._cleanup_and_finish(result, cid)
        return result

    def _aggregate_out_of_sample_summary(
        self,
        total_windows: int,
        window_results: List[ValidationWindowResult],
    ) -> OutOfSampleSummary:
        """Mathematically aggregate out-of-sample results across completed windows."""
        completed = [w for w in window_results if w.is_completed and w.out_of_sample_metrics is not None]
        skipped = [w for w in window_results if w.status == "SKIPPED"]
        failed = [w for w in window_results if w.status == "FAILED"]

        if not completed:
            return self._create_empty_summary(total_windows, skipped=len(skipped), failed=len(failed))

        returns = [w.out_of_sample_metrics.total_return_pct for w in completed]
        pnls = [w.out_of_sample_metrics.net_profit for w in completed]
        trades = sum(w.out_of_sample_metrics.total_trades for w in completed)
        wins = sum(w.out_of_sample_metrics.winning_trades for w in completed)
        losses = sum(w.out_of_sample_metrics.losing_trades for w in completed)
        costs = sum(w.out_of_sample_metrics.total_transaction_costs for w in completed)
        drawdowns = [w.out_of_sample_metrics.max_drawdown_pct for w in completed]
        dd_amounts = [w.out_of_sample_metrics.max_drawdown_amount for w in completed]

        mean_return = sum(returns) / len(returns)
        median_return = statistics.median(returns) if len(returns) > 1 else returns[0]

        # Compounded Return: product(1 + r_i / 100) - 1
        compounded = 1.0
        for r in returns:
            compounded *= (1.0 + (r / 100.0))
        compounded_return_pct = (compounded - 1.0) * 100.0

        total_pnl = sum(pnls)
        overall_win_rate = (wins / trades * 100.0) if trades > 0 else 0.0

        win_rates = [w.out_of_sample_metrics.win_rate for w in completed]
        mean_win_rate = sum(win_rates) / len(win_rates)

        # Profit factor across all OOS windows
        gross_profit = sum(w.out_of_sample_metrics.gross_profit for w in completed)
        gross_loss = sum(w.out_of_sample_metrics.gross_loss for w in completed)

        overall_pf: Optional[float] = None
        if trades == 0:
            pf_desc = "N/A (No OOS trades executed)"
        elif gross_loss == 0.0:
            if gross_profit > 0.0:
                pf_desc = "Infinite (No losing OOS trades)"
            else:
                overall_pf = 0.0
                pf_desc = "0.0 (Zero gross profit and loss)"
        else:
            overall_pf = round(gross_profit / gross_loss, 4)
            pf_desc = f"{overall_pf:.2f}"

        # Mean of window profit factors
        pfs = [w.out_of_sample_metrics.profit_factor for w in completed if w.out_of_sample_metrics.profit_factor is not None]
        mean_pf = (sum(pfs) / len(pfs)) if pfs else None

        max_dd = max(drawdowns) if drawdowns else 0.0
        worst_dd_amt = max(dd_amounts) if dd_amounts else 0.0

        profitable_wins = sum(1 for r in returns if r > 0.0)
        unprofitable_wins = sum(1 for r in returns if r <= 0.0)
        win_rate_pct = (profitable_wins / len(completed) * 100.0) if completed else 0.0

        return OutOfSampleSummary(
            total_windows_generated=total_windows,
            eligible_windows=len(completed) + len(failed),
            completed_windows=len(completed),
            skipped_windows=len(skipped),
            failed_windows=len(failed),
            mean_window_return_pct=mean_return,
            median_window_return_pct=median_return,
            compounded_return_pct=compounded_return_pct,
            total_oos_net_pnl=total_pnl,
            total_oos_trades=trades,
            total_oos_winning_trades=wins,
            total_oos_losing_trades=losses,
            overall_oos_win_rate=overall_win_rate,
            mean_window_win_rate=mean_win_rate,
            mean_profit_factor=mean_pf,
            overall_profit_factor=overall_pf,
            overall_profit_factor_description=pf_desc,
            max_window_drawdown_pct=max_dd,
            worst_window_drawdown_amount=worst_dd_amt,
            total_transaction_costs=costs,
            profitable_windows_count=profitable_wins,
            unprofitable_windows_count=unprofitable_wins,
            window_win_rate_pct=win_rate_pct,
        )

    def _create_empty_summary(
        self,
        total_windows: int,
        skipped: int = 0,
        failed: int = 0,
    ) -> OutOfSampleSummary:
        """Create a zero-division safe empty summary container."""
        return OutOfSampleSummary(
            total_windows_generated=total_windows,
            eligible_windows=0,
            completed_windows=0,
            skipped_windows=skipped,
            failed_windows=failed,
            mean_window_return_pct=0.0,
            median_window_return_pct=0.0,
            compounded_return_pct=0.0,
            total_oos_net_pnl=0.0,
            total_oos_trades=0,
            total_oos_winning_trades=0,
            total_oos_losing_trades=0,
            overall_oos_win_rate=0.0,
            mean_window_win_rate=0.0,
            mean_profit_factor=None,
            overall_profit_factor=None,
            overall_profit_factor_description="N/A (No completed windows)",
            max_window_drawdown_pct=0.0,
            worst_window_drawdown_amount=0.0,
            total_transaction_costs=0.0,
            profitable_windows_count=0,
            unprofitable_windows_count=0,
            window_win_rate_pct=0.0,
        )

    def _cleanup_and_finish(self, result: WalkForwardResult, cid: str) -> None:
        """Clean up active validation state and broadcast terminal events."""
        with self._lock:
            self._active_validations.pop(result.validation_id, None)

        if self.state_store:
            self.state_store.set(
                "walk_forward.latest",
                {
                    "validation_id": result.validation_id,
                    "status": result.status.value,
                    "strategy": result.strategy_name,
                    "symbol": result.config.symbol,
                    "timeframe": result.config.timeframe,
                    "completed_windows": result.summary.completed_windows,
                    "total_windows": result.summary.total_windows_generated,
                    "mean_return_pct": result.summary.mean_window_return_pct,
                    "compounded_return_pct": result.summary.compounded_return_pct,
                    "total_oos_trades": result.summary.total_oos_trades,
                    "window_win_rate_pct": result.summary.window_win_rate_pct,
                    "duration_ms": result.duration_ms,
                },
            )

        if self.event_bus:
            evt_type = (
                EventType.WALK_FORWARD_COMPLETED
                if result.status == WalkForwardStatus.COMPLETED
                else EventType.WALK_FORWARD_CANCELLED
                if result.status == WalkForwardStatus.CANCELLED
                else EventType.WALK_FORWARD_FAILED
            )
            self.event_bus.publish(
                Event.create(
                    event_type=evt_type,
                    source=self.name,
                    correlation_id=cid,
                    payload=result.to_dict(),
                )
            )
