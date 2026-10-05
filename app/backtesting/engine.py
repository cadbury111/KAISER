"""KAISER — Deterministic Backtest Engine.

Executes reproducible, research-only strategy backtests over stored historical
market data through the MarketDataQualityEngine, TechnicalAnalysisEngine,
and SimulatedPortfolio.

SAFETY INVARIANTS:
1. Research only: Strictly simulated fills in virtual portfolio. No real orders or broker connections.
2. Anti-look-ahead: Signals generated at bar close are scheduled and executed at the NEXT bar's open.
3. Total state isolation: Stream tracking, feature vectors, and strategy state are reset between runs.
4. Comprehensive auditing: Logs every signal, order, fill, and quality-gated rejection.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Union

from app.analysis.models import TIMEFRAME_SECONDS, normalize_timeframe
from app.backtesting.metrics import calculate_performance_metrics
from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    BacktestStatus,
    ExecutionModel,
    OrderSide,
    OrderStatus,
    OrderType,
    PerformanceMetrics,
    SignalType,
    SimulatedOrder,
)
from app.backtesting.portfolio import SimulatedPortfolio
from app.backtesting.strategy import Strategy, StrategyContext, StrategyRegistry
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.historical.storage import HistoricalBarRepository
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine

if TYPE_CHECKING:
    from app.analysis.engine import TechnicalAnalysisEngine

logger = get_logger("backtesting.engine")


class BacktestEngine:
    """Central orchestrator for executing deterministic historical backtests."""

    def __init__(
        self,
        repository: HistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
        strategy_registry: Optional[StrategyRegistry] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        provenance_tracker: Optional[Any] = None,
        run_registry: Optional[Any] = None,
        name: str = "backtest_engine",
    ) -> None:
        self.name = name
        self.repository = repository
        self.quality_engine = quality_engine
        self.analysis_engine = analysis_engine
        self.strategy_registry = strategy_registry or StrategyRegistry()
        self.event_bus = event_bus
        self.state_store = state_store
        self.provenance_tracker = provenance_tracker
        self.run_registry = run_registry

        self._lock = threading.RLock()
        self._active_backtests: Dict[str, threading.Event] = {}

    @property
    def active_backtest_count(self) -> int:
        """Number of backtests currently executing."""
        with self._lock:
            return len(self._active_backtests)

    def cancel_backtest(self, backtest_id: str) -> bool:
        """Signal cooperative cancellation to an active backtest run."""
        with self._lock:
            stop_evt = self._active_backtests.get(backtest_id)
            if stop_evt:
                stop_evt.set()
                logger.info("Cancellation requested for backtest '%s'", backtest_id)
                return True
            return False

    def run(
        self,
        config: BacktestConfig,
        strategy: Union[Strategy, str],
        cancellation_token: Optional[threading.Event] = None,
    ) -> BacktestResult:
        """Execute a deterministic backtest run.

        Args:
            config: Validated BacktestConfig parameters.
            strategy: Strategy instance or registered strategy identifier.
            cancellation_token: Optional external cancellation event.

        Returns:
            BacktestResult: Comprehensive audit and performance result.
        """
        start_time_perf = time.perf_counter()
        bt_id = config.backtest_id or f"bt_{uuid.uuid4().hex[:12]}"
        cid = config.correlation_id or uuid.uuid4().hex

        # 1. Resolve Strategy
        resolved_strategy: Strategy
        if isinstance(strategy, str):
            resolved_strategy = self.strategy_registry.get(strategy)
        elif isinstance(strategy, Strategy):
            resolved_strategy = strategy
        else:
            raise ValidationError(f"Invalid strategy type: {type(strategy)}. Expected Strategy or str.")

        # Setup cancellation token
        stop_event = cancellation_token or threading.Event()
        with self._lock:
            self._active_backtests[bt_id] = stop_event

        # Setup Analysis Run Registry
        analysis_run = None
        if getattr(self, "run_registry", None) is not None:
            try:
                from app.analysis.runs.models import OperationType
                parent_ids = ()
                if getattr(self, "provenance_tracker", None) is not None:
                    p = self.provenance_tracker.get_latest(config.symbol, config.timeframe)
                    if p:
                        parent_ids = (p.provenance_id,)
                analysis_run = self.run_registry.create_run(
                    operation_type=OperationType.BACKTEST,
                    configuration=config,
                    input_provenance_ids=parent_ids,
                    strategy_id=resolved_strategy.name,
                    strategy_version=resolved_strategy.version,
                    symbol_universe=(config.symbol,),
                    timeframe_universe=(config.timeframe,),
                    data_start=config.start_time,
                    data_end=config.end_time,
                    execution_metadata={"backtest_id": bt_id},
                )
                self.run_registry.start_run(analysis_run.run_id)
            except Exception as exc:
                logger.warning("Failed to initialize analysis run for backtest [%s]: %s", bt_id, exc)
                analysis_run = None

        # 2. State Isolation
        if config.reset_state_between_runs:
            self.analysis_engine.reset_stream(config.symbol, config.timeframe)
            if hasattr(self.quality_engine, "reset_stream"):
                self.quality_engine.reset_stream(config.symbol, source="replay")
            elif hasattr(self.quality_engine, "reset"):
                self.quality_engine.reset()
            resolved_strategy.reset()
            logger.info("Isolated and reset state for %s ahead of backtest [%s]", config.symbol, bt_id)

        # 3. Publish Backtest Started Event
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.BACKTEST_STARTED,
                    source=self.name,
                    correlation_id=cid,
                    payload={
                        "backtest_id": bt_id,
                        "strategy": resolved_strategy.name,
                        "symbol": config.symbol,
                        "timeframe": config.timeframe,
                        "start_time": config.start_time,
                        "end_time": config.end_time,
                        "initial_cash": config.initial_cash,
                    },
                )
            )

        resolved_strategy.on_start(config)

        # 4. Query Historical Bars
        try:
            stored_bars = self.repository.query_bars(
                symbol=config.symbol,
                timeframe=config.timeframe,
                start_time=config.start_time,
                end_time=config.end_time,
                limit=config.limit or 5000,
            )
        except Exception as err:
            logger.error("Failed to query historical bars for backtest [%s]: %s", bt_id, err)
            duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
            empty_metrics = calculate_performance_metrics(
                initial_equity=config.initial_cash,
                final_equity=config.initial_cash,
                trades=[],
                equity_curve=[],
            )
            result = BacktestResult(
                backtest_id=bt_id,
                status=BacktestStatus.FAILED,
                strategy_name=resolved_strategy.name,
                strategy_version=resolved_strategy.version,
                config=config,
                metrics=empty_metrics,
                error_message=str(err),
                duration_ms=duration_ms,
                run_id=analysis_run.run_id if analysis_run else None,
            )
            self._cleanup_and_finish(result, cid)
            return result

        total_available = len(stored_bars)
        if total_available == 0:
            duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
            empty_metrics = calculate_performance_metrics(
                initial_equity=config.initial_cash,
                final_equity=config.initial_cash,
                trades=[],
                equity_curve=[],
            )
            result = BacktestResult(
                backtest_id=bt_id,
                status=BacktestStatus.COMPLETED,
                strategy_name=resolved_strategy.name,
                strategy_version=resolved_strategy.version,
                config=config,
                metrics=empty_metrics,
                total_bars_available=0,
                duration_ms=duration_ms,
                warnings=("No historical bars found in repository matching criteria.",),
                run_id=analysis_run.run_id if analysis_run else None,
            )
            self._cleanup_and_finish(result, cid)
            return result

        # 5. Initialize Virtual Portfolio
        portfolio = SimulatedPortfolio(initial_cash=config.initial_cash)

        # 6. Chronological Simulation Loop
        bars_processed = 0
        bars_accepted = 0
        bars_rejected = 0
        signals_generated = 0
        bars_with_position = 0
        zero_vol_count = 0
        gaps_detected = 0
        rejection_reasons: List[str] = []
        warnings: List[str] = []
        accepted_bars: List[Bar] = []

        if config.execution_model == ExecutionModel.CURRENT_BAR_CLOSE.value:
            warnings.append(
                "ExecutionModel.CURRENT_BAR_CLOSE carries potential look-ahead latency bias. "
                "Default is NEXT_BAR_OPEN."
            )

        interval_sec = TIMEFRAME_SECONDS.get(config.timeframe, 60.0)
        last_ts: Optional[float] = None
        pending_order: Optional[SimulatedOrder] = None

        first_bar_open = stored_bars[0].open
        last_bar_close = stored_bars[-1].close

        for bar in stored_bars:
            # Check cooperative cancellation
            if stop_event.is_set():
                logger.warning("Backtest [%s] cancelled after %d bars", bt_id, bars_processed)
                duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
                metrics = calculate_performance_metrics(
                    initial_equity=config.initial_cash,
                    final_equity=portfolio.total_equity,
                    trades=portfolio.trades,
                    equity_curve=portfolio.equity_curve,
                    total_bars=bars_processed,
                    bars_with_position=bars_with_position,
                    total_fees=portfolio.total_fees_paid,
                    total_slippage=portfolio.total_slippage_cost,
                    first_bar_price=first_bar_open,
                    last_bar_price=bar.close,
                )
                result = BacktestResult(
                    backtest_id=bt_id,
                    status=BacktestStatus.CANCELLED,
                    strategy_name=resolved_strategy.name,
                    strategy_version=resolved_strategy.version,
                    config=config,
                    metrics=metrics,
                    trades=tuple(portfolio.trades),
                    equity_curve=tuple(portfolio.equity_curve),
                    total_bars_available=total_available,
                    bars_processed=bars_processed,
                    bars_accepted=bars_accepted,
                    bars_rejected=bars_rejected,
                    signals_generated=signals_generated,
                    orders_submitted=len(portfolio.orders),
                    orders_accepted=len(portfolio.fills),
                    orders_rejected=len(portfolio.rejected_orders),
                    fills_completed=len(portfolio.fills),
                    start_timestamp=stored_bars[0].timestamp,
                    end_timestamp=bar.timestamp,
                    duration_ms=duration_ms,
                    rejection_reasons=tuple(rejection_reasons),
                    warnings=tuple(warnings),
                )
                self._cleanup_and_finish(result, cid)
                return result

            # Pacing sleep if requested
            if config.pacing_seconds > 0.0:
                time.sleep(config.pacing_seconds)

            bars_processed += 1

            # Tag with source='replay'
            if bar.source != "replay":
                bar_to_ingest = Bar(
                    symbol=bar.symbol,
                    exchange=bar.exchange,
                    timeframe=bar.timeframe,
                    timestamp=bar.timestamp,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    source="replay",
                )
            else:
                bar_to_ingest = bar

            # Gap & Volume tracking
            if last_ts is not None:
                diff = bar_to_ingest.timestamp - last_ts
                if diff > interval_sec * 1.5:
                    gaps_detected += 1
            last_ts = bar_to_ingest.timestamp

            if bar_to_ingest.volume == 0.0:
                zero_vol_count += 1

            # Quality Gate Evaluation
            assessment = self.quality_engine.evaluate_bar(
                bar=bar_to_ingest,
                correlation_id=cid,
                reference_time=bar_to_ingest.timestamp,
            )

            if not assessment.safe_to_consume:
                bars_rejected += 1
                rejection_reasons.append(
                    f"Quarantined by quality engine at ts={bar_to_ingest.timestamp}: {assessment.quality_status.value}"
                )
                continue

            # Ingest into Analysis Engine
            vector = self.analysis_engine.add_bar(
                bar=bar_to_ingest,
                safe_to_consume=True,
                correlation_id=cid,
            )

            if vector is None:
                bars_rejected += 1
                rejection_reasons.append(f"Rejected by series (duplicate/out-of-order) at ts={bar_to_ingest.timestamp}")
                continue

            bars_accepted += 1
            accepted_bars.append(bar_to_ingest)

            # ------------------------------------------------------------------
            # Execution Phase: Fill pending orders from previous bar
            # ------------------------------------------------------------------
            if config.execution_model == ExecutionModel.NEXT_BAR_OPEN.value and pending_order is not None:
                # Fill pending order at current bar's OPEN price
                fill = portfolio.execute_order(
                    order=pending_order,
                    base_price=bar_to_ingest.open,
                    timestamp=bar_to_ingest.timestamp,
                    config=config,
                )
                pending_order = None

            # Mark portfolio to market at current bar's close
            portfolio.mark_to_market(
                timestamp=bar_to_ingest.timestamp,
                current_prices={config.symbol: bar_to_ingest.close},
                config=config,
            )

            if portfolio.get_position_quantity(config.symbol) > 0.000001:
                bars_with_position += 1

            # ------------------------------------------------------------------
            # Strategy Evaluation Phase: Generate signals on candle close
            # ------------------------------------------------------------------
            pos = portfolio.get_position(config.symbol)
            pos_qty = pos.quantity if pos else 0.0
            pos_cost = pos.avg_cost if pos else 0.0
            pos_val = pos.market_value if pos else 0.0

            ctx = StrategyContext(
                symbol=config.symbol,
                timeframe=config.timeframe,
                current_bar=bar_to_ingest,
                timestamp=bar_to_ingest.timestamp,
                feature_vector=vector,
                portfolio_cash=portfolio.cash,
                position_quantity=pos_qty,
                position_avg_cost=pos_cost,
                position_market_value=pos_val,
                portfolio_equity=portfolio.total_equity,
                bars_history=tuple(accepted_bars),
            )

            signal = resolved_strategy.on_bar(ctx)

            if signal is not None:
                signals_generated += 1

                # Process BUY signal
                if signal.signal_type == SignalType.BUY:
                    if ctx.is_flat:
                        target_qty = (
                            signal.suggested_quantity
                            if signal.suggested_quantity is not None
                            else portfolio.calculate_target_quantity(
                                config=config,
                                current_price=bar_to_ingest.close,
                                current_equity=portfolio.total_equity,
                            )
                        )
                        if target_qty > 0:
                            order = SimulatedOrder(
                                order_id=f"ord_{uuid.uuid4().hex[:10]}",
                                symbol=config.symbol,
                                side=OrderSide.BUY,
                                order_type=OrderType.MARKET,
                                quantity=target_qty,
                                price=bar_to_ingest.close,
                                created_at=bar_to_ingest.timestamp,
                                correlation_id=cid,
                            )
                            if config.execution_model == ExecutionModel.NEXT_BAR_OPEN.value:
                                pending_order = order
                            else:
                                portfolio.execute_order(
                                    order=order,
                                    base_price=bar_to_ingest.close,
                                    timestamp=bar_to_ingest.timestamp,
                                    config=config,
                                )

                # Process SELL or CLOSE signal
                elif signal.signal_type in (SignalType.SELL, SignalType.CLOSE):
                    if ctx.has_position:
                        sell_qty = (
                            min(ctx.position_quantity, signal.suggested_quantity)
                            if signal.suggested_quantity is not None
                            else ctx.position_quantity
                        )
                        if sell_qty > 0:
                            order = SimulatedOrder(
                                order_id=f"ord_{uuid.uuid4().hex[:10]}",
                                symbol=config.symbol,
                                side=OrderSide.SELL,
                                order_type=OrderType.MARKET,
                                quantity=sell_qty,
                                price=bar_to_ingest.close,
                                created_at=bar_to_ingest.timestamp,
                                correlation_id=cid,
                            )
                            if config.execution_model == ExecutionModel.NEXT_BAR_OPEN.value:
                                pending_order = order
                            else:
                                portfolio.execute_order(
                                    order=order,
                                    base_price=bar_to_ingest.close,
                                    timestamp=bar_to_ingest.timestamp,
                                    config=config,
                                )

            # Progress event distribution
            if self.event_bus and (bars_processed % 100 == 0 or bars_processed == total_available):
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.BACKTEST_PROGRESS,
                        source=self.name,
                        correlation_id=cid,
                        payload={
                            "backtest_id": bt_id,
                            "symbol": config.symbol,
                            "bars_processed": bars_processed,
                            "total_bars": total_available,
                            "percent_complete": round((bars_processed / total_available) * 100.0, 1),
                            "equity": round(portfolio.total_equity, 2),
                            "trades_count": len(portfolio.trades),
                        },
                    )
                )

        resolved_strategy.on_finish()

        # 7. Close Open Positions at Final Bar Close (realizes full backtest PnL)
        if portfolio.get_position_quantity(config.symbol) > 0.000001:
            portfolio.close_all_positions(
                timestamp=last_bar_close,
                current_prices={config.symbol: last_bar_close},
                config=config,
            )

        duration_ms = (time.perf_counter() - start_time_perf) * 1000.0

        # 8. Calculate Final Performance Metrics
        metrics = calculate_performance_metrics(
            initial_equity=config.initial_cash,
            final_equity=portfolio.total_equity,
            trades=portfolio.trades,
            equity_curve=portfolio.equity_curve,
            total_bars=bars_processed,
            bars_with_position=bars_with_position,
            total_fees=portfolio.total_fees_paid,
            total_slippage=portfolio.total_slippage_cost,
            first_bar_price=first_bar_open,
            last_bar_price=last_bar_close,
        )

        quality_summary = {
            "total_bars_available": total_available,
            "bars_processed": bars_processed,
            "bars_accepted": bars_accepted,
            "bars_rejected": bars_rejected,
            "zero_volume_bars": zero_vol_count,
            "gaps_detected": gaps_detected,
        }

        result = BacktestResult(
            backtest_id=bt_id,
            status=BacktestStatus.COMPLETED,
            strategy_name=resolved_strategy.name,
            strategy_version=resolved_strategy.version,
            config=config,
            metrics=metrics,
            trades=tuple(portfolio.trades),
            equity_curve=tuple(portfolio.equity_curve),
            total_bars_available=total_available,
            bars_processed=bars_processed,
            bars_accepted=bars_accepted,
            bars_rejected=bars_rejected,
            signals_generated=signals_generated,
            orders_submitted=len(portfolio.orders),
            orders_accepted=len(portfolio.fills),
            orders_rejected=len(portfolio.rejected_orders),
            fills_completed=len(portfolio.fills),
            rejection_reasons=tuple(rejection_reasons),
            warnings=tuple(warnings),
            data_quality_summary=quality_summary,
            start_timestamp=stored_bars[0].timestamp if stored_bars else None,
            end_timestamp=stored_bars[-1].timestamp if stored_bars else None,
            duration_ms=duration_ms,
            run_id=analysis_run.run_id if analysis_run else None,
        )

        self._cleanup_and_finish(result, cid)
        return result

    def _cleanup_and_finish(self, result: BacktestResult, cid: str) -> None:
        """Internal cleanup, StateStore update, and terminal event publishing."""
        with self._lock:
            self._active_backtests.pop(result.backtest_id, None)

        if self.state_store:
            self.state_store.set(
                "backtest.latest",
                {
                    "backtest_id": result.backtest_id,
                    "status": result.status.value,
                    "strategy": result.strategy_name,
                    "symbol": result.config.symbol,
                    "timeframe": result.config.timeframe,
                    "net_pnl": result.metrics.net_profit,
                    "return_pct": result.metrics.total_return_pct,
                    "trades": result.metrics.total_trades,
                    "win_rate": result.metrics.win_rate,
                    "max_drawdown_pct": result.metrics.max_drawdown_pct,
                    "duration_ms": result.duration_ms,
                },
            )

        if getattr(self, "provenance_tracker", None) is not None:
            try:
                parent_prov = self.provenance_tracker.get_latest(result.config.symbol, result.config.timeframe)
                parent_ids = (parent_prov.provenance_id,) if parent_prov else ()
                prov = self.provenance_tracker.create_backtest_provenance(
                    strategy_id=result.strategy_name,
                    strategy_version=result.strategy_version,
                    symbol=result.config.symbol,
                    timeframe=result.config.timeframe,
                    data_start=result.start_timestamp or result.config.start_time,
                    data_end=result.end_timestamp or result.config.end_time,
                    bars_evaluated=result.bars_processed,
                    trades_simulated=len(result.trades),
                    parent_provenance_ids=parent_ids,
                    run_id=result.backtest_id,
                    operation_id=cid,
                )
                if hasattr(result, "provenance_id"):
                    object.__setattr__(result, "provenance_id", prov.provenance_id)
            except Exception as exc:
                logger.warning("Failed to record backtest provenance: %s", exc)

        if getattr(self, "run_registry", None) is not None and getattr(result, "run_id", None):
            try:
                if result.status == BacktestStatus.COMPLETED:
                    self.run_registry.complete_run(
                        run_id=result.run_id,
                        result_id=result.backtest_id,
                        result_type="BacktestResult",
                    )
                elif result.status == BacktestStatus.CANCELLED:
                    reason = result.warnings[0] if result.warnings else "Backtest cancelled by operator"
                    self.run_registry.cancel_run(
                        run_id=result.run_id,
                        cancellation_reason=str(reason),
                    )
                else:
                    self.run_registry.fail_run(
                        run_id=result.run_id,
                        error_category="BACKTEST_ERROR",
                        error_message=result.error_message or "Backtest execution failure",
                    )
            except Exception as exc:
                logger.warning("Failed to finalize analysis run in backtest cleanup: %s", exc)

        if self.event_bus:
            evt_type = (
                EventType.BACKTEST_COMPLETED
                if result.status == BacktestStatus.COMPLETED
                else EventType.BACKTEST_CANCELLED
                if result.status == BacktestStatus.CANCELLED
                else EventType.BACKTEST_FAILED
            )
            self.event_bus.publish(
                Event.create(
                    event_type=evt_type,
                    source=self.name,
                    correlation_id=cid,
                    payload=result.to_dict(),
                )
            )
