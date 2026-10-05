"""KAISER — Portfolio-Level Risk Analytics & Multi-Strategy Simulation Engine.

Coordinates multiple historical strategies inside a single, shared virtual portfolio.
Enforces central cash constraints, risk limits, order priorities, and anti-look-ahead
next-bar execution timing across multiple instruments.

SAFETY INVARIANTS:
1. Research-only simulation: strictly virtual accounting with NO real broker connectivity.
2. Complete balance integrity: cash + position_value == total_equity at every step.
3. Central risk controls: enforces cash reserve, concentration, and exposure bounds deterministically.
4. Strategy isolation: strategies receive strictly time-bounded context and cannot see each other's state.
5. Deterministic priority: simultaneous signals resolve using explicit priority rules (SELL before BUY, weight priority).
"""

from __future__ import annotations

import math
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Set, Tuple

from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.models import normalize_timeframe
from app.backtesting.models import (
    BacktestConfig,
    EquityPoint,
    ExecutionModel,
    FeeModel,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSizeType,
    SimulatedFill,
    SimulatedOrder,
    StrategySignal,
    TradeRecord,
)
from app.backtesting.portfolio_simulation.analytics import compute_diversification_metrics
from app.backtesting.portfolio_simulation.models import (
    DiversificationMetrics,
    PortfolioPosition,
    PortfolioRiskSnapshot,
    PortfolioRiskWarning,
    PortfolioSimulationConfig,
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
    StrategyAllocation,
    StrategyContribution,
)
from app.backtesting.strategy import Strategy, StrategyContext, StrategyRegistry
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state.store import StateStore
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.market.historical.storage import HistoricalBarRepository

logger = get_logger("backtesting.portfolio_simulation")


class PortfolioSimulationEngine:
    """Multi-strategy virtual portfolio simulation and risk analytics coordinator."""

    def __init__(
        self,
        repository: HistoricalBarRepository,
        quality_engine: Optional[MarketDataQualityEngine] = None,
        analysis_engine: Optional[TechnicalAnalysisEngine] = None,
        strategy_registry: Optional[StrategyRegistry] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        provenance_tracker: Optional[Any] = None,
        run_registry: Optional[Any] = None,
        name: str = "portfolio_simulation_engine",
    ) -> None:
        self.repository = repository
        self.quality_engine = quality_engine
        self.analysis_engine = analysis_engine
        self.strategy_registry = strategy_registry or StrategyRegistry()
        self.event_bus = event_bus
        self.state_store = state_store
        self.provenance_tracker = provenance_tracker
        self.run_registry = run_registry
        self.name = name

        self._active_simulations: Dict[str, threading.Event] = {}
        self._lock = threading.RLock()

    def cancel_simulation(self, simulation_id: str) -> bool:
        """Signal cooperative cancellation for an active portfolio simulation."""
        with self._lock:
            token = self._active_simulations.get(simulation_id)
            if token:
                token.set()
                logger.info("Signalled cancellation for portfolio simulation [%s]", simulation_id)
                return True
        return False

    def run(
        self,
        config: PortfolioSimulationConfig,
        cancellation_token: Optional[threading.Event] = None,
    ) -> PortfolioSimulationResult:
        """Execute a deterministic multi-strategy virtual portfolio simulation."""
        start_time_perf = time.perf_counter()
        sim_id = config.simulation_id
        cid = config.correlation_id or f"sim_{sim_id}"

        # 1. Setup Cancellation Token
        stop_event = cancellation_token or threading.Event()
        with self._lock:
            self._active_simulations[sim_id] = stop_event

        # Setup Analysis Run Registry
        analysis_run = None
        if getattr(self, "run_registry", None) is not None:
            try:
                from app.analysis.runs.models import OperationType
                symbols = list(config.symbols) if hasattr(config, "symbols") and config.symbols else [alloc.symbol for alloc in config.allocations]
                parent_ids = []
                if getattr(self, "provenance_tracker", None) is not None:
                    for s in symbols:
                        p = self.provenance_tracker.get_latest(s, getattr(config, "timeframe", "1m"))
                        if p:
                            parent_ids.append(p.provenance_id)
                analysis_run = self.run_registry.create_run(
                    operation_type=OperationType.PORTFOLIO_SIMULATION,
                    configuration=config,
                    input_provenance_ids=parent_ids,
                    symbol_universe=tuple(symbols),
                    timeframe_universe=(getattr(config, "timeframe", "1m"),),
                    data_start=config.start_time,
                    data_end=config.end_time,
                    execution_metadata={"simulation_id": sim_id},
                )
                self.run_registry.start_run(analysis_run.run_id)
            except Exception as exc:
                logger.warning("Failed to initialize analysis run for portfolio simulation [%s]: %s", sim_id, exc)
                analysis_run = None

        if stop_event.is_set():
            return self._build_terminal_result(
                config=config,
                status=PortfolioSimulationStatus.CANCELLED,
                error_message="Simulation cancelled before execution started.",
                duration_ms=0.0,
                analysis_run_id=analysis_run.run_id if analysis_run else None,
            )

        # 2. State Isolation & Reset
        if config.reset_state_between_runs:
            for sym in config.symbols:
                if self.analysis_engine:
                    self.analysis_engine.reset_stream(sym, config.timeframe)
                if self.quality_engine and hasattr(self.quality_engine, "reset_stream"):
                    self.quality_engine.reset_stream(sym, source="replay")

        # 3. Instantiate and Verify Strategies
        strategies: Dict[str, Strategy] = {}
        for alloc in config.allocations:
            try:
                strat_inst = self.strategy_registry.get(alloc.strategy_name, **alloc.parameters)
                strat_inst.reset()
                strategies[alloc.allocation_id] = strat_inst
            except Exception as err:
                msg = f"Failed to instantiate strategy '{alloc.strategy_name}' for allocation '{alloc.allocation_id}': {err}"
                logger.error(msg)
                return self._build_terminal_result(
                    config=config,
                    status=PortfolioSimulationStatus.FAILED,
                    error_message=msg,
                    duration_ms=(time.perf_counter() - start_time_perf) * 1000.0,
                    analysis_run_id=analysis_run.run_id if analysis_run else None,
                )

        # 4. Publish Started Event
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.PORTFOLIO_SIMULATION_STARTED,
                    source=self.name,
                    correlation_id=cid,
                    payload={
                        "simulation_id": sim_id,
                        "initial_cash": config.initial_cash,
                        "strategies_count": len(config.allocations),
                        "symbols": list(config.symbols),
                        "timeframe": config.timeframe,
                        "start_time": config.start_time,
                        "end_time": config.end_time,
                    },
                )
            )

        # 5. Load Historical Bars for All Symbols
        stored_bars_by_symbol: Dict[str, List[Bar]] = {}
        for sym in config.symbols:
            try:
                bars = self.repository.query_bars(
                    symbol=sym,
                    timeframe=config.timeframe,
                    start_time=config.start_time,
                    end_time=config.end_time,
                    limit=20000,
                )
                stored_bars_by_symbol[sym] = bars
            except Exception as err:
                msg = f"Failed to query historical bars for symbol '{sym}': {err}"
                logger.error(msg)
                return self._build_terminal_result(
                    config=config,
                    status=PortfolioSimulationStatus.FAILED,
                    error_message=msg,
                    duration_ms=(time.perf_counter() - start_time_perf) * 1000.0,
                    analysis_run_id=analysis_run.run_id if analysis_run else None,
                )

        # Check that we have bars
        total_bars_count = sum(len(b) for b in stored_bars_by_symbol.values())
        if total_bars_count == 0:
            return self._build_terminal_result(
                config=config,
                status=PortfolioSimulationStatus.FAILED,
                error_message="No historical bars found within requested date range.",
                duration_ms=(time.perf_counter() - start_time_perf) * 1000.0,
                analysis_run_id=analysis_run.run_id if analysis_run else None,
            )

        # 6. Quality Filter & Build Chronological Timeline
        timeline: Dict[float, Dict[str, Bar]] = {}
        data_quality_warnings: List[PortfolioRiskWarning] = []

        for sym, bars in stored_bars_by_symbol.items():
            for b in bars:
                bar_to_ingest = Bar(
                    symbol=b.symbol,
                    exchange=b.exchange,
                    timeframe=b.timeframe,
                    timestamp=b.timestamp,
                    open=b.open,
                    high=b.high,
                    low=b.low,
                    close=b.close,
                    volume=b.volume,
                    source="replay",
                )

                if self.quality_engine:
                    assessment = self.quality_engine.evaluate_bar(
                        bar=bar_to_ingest,
                        correlation_id=cid,
                        reference_time=bar_to_ingest.timestamp,
                    )
                    if not assessment.safe_to_consume:
                        data_quality_warnings.append(
                            PortfolioRiskWarning(
                                timestamp=b.timestamp,
                                warning_type="DATA_QUALITY_REJECTION",
                                message=f"Bar for {sym} at ts={b.timestamp} quarantined: {assessment.quality_status.value}",
                                details={"symbol": sym, "timestamp": b.timestamp},
                            )
                        )
                        continue

                if self.analysis_engine:
                    self.analysis_engine.add_bar(bar_to_ingest, safe_to_consume=True, correlation_id=cid)

                ts = bar_to_ingest.timestamp
                if ts not in timeline:
                    timeline[ts] = {}
                timeline[ts][sym] = bar_to_ingest

        timestamps = sorted(timeline.keys())
        if not timestamps:
            return self._build_terminal_result(
                config=config,
                status=PortfolioSimulationStatus.FAILED,
                error_message="All historical bars were quarantined or invalid.",
                duration_ms=(time.perf_counter() - start_time_perf) * 1000.0,
                analysis_run_id=analysis_run.run_id if analysis_run else None,
            )

        # 7. Initialize Central Portfolio State
        cash: float = float(config.initial_cash)
        peak_equity: float = float(config.initial_cash)
        max_drawdown_pct: float = 0.0
        max_drawdown_amount: float = 0.0
        max_drawdown_duration_bars: int = 0
        current_drawdown_bars: int = 0
        circuit_breaker_triggered: bool = False

        positions: Dict[str, PortfolioPosition] = {}
        pending_orders: List[Tuple[StrategyAllocation, SimulatedOrder]] = []

        orders_submitted: int = 0
        orders_accepted: int = 0
        orders_rejected: int = 0
        rejected_orders: List[Dict[str, Any]] = []

        total_fees: float = 0.0
        total_slippage: float = 0.0

        trades_by_strategy: Dict[str, List[TradeRecord]] = {alloc.allocation_id: [] for alloc in config.allocations}
        fees_by_strategy: Dict[str, float] = {alloc.allocation_id: 0.0 for alloc in config.allocations}
        slippage_by_strategy: Dict[str, float] = {alloc.allocation_id: 0.0 for alloc in config.allocations}
        orders_sub_by_strat: Dict[str, int] = {alloc.allocation_id: 0 for alloc in config.allocations}
        orders_acc_by_strat: Dict[str, int] = {alloc.allocation_id: 0 for alloc in config.allocations}
        orders_rej_by_strat: Dict[str, int] = {alloc.allocation_id: 0 for alloc in config.allocations}

        equity_curve: List[EquityPoint] = []
        risk_snapshots: List[PortfolioRiskSnapshot] = []
        warnings: List[PortfolioRiskWarning] = list(data_quality_warnings)

        last_known_prices: Dict[str, float] = {}
        bars_history_by_symbol: Dict[str, List[Bar]] = {s: [] for s in config.symbols}

        instrument_bar_returns: Dict[str, Dict[float, float]] = {s: {} for s in config.symbols}
        prev_bar_close: Dict[str, float] = {}

        strategy_bar_returns: Dict[str, Dict[float, float]] = {alloc.allocation_id: {} for alloc in config.allocations}
        prev_strategy_equity: Dict[str, float] = {
            alloc.allocation_id: alloc.weight * config.initial_cash for alloc in config.allocations
        }

        # 8. Chronological Simulation Loop
        for step_idx, ts in enumerate(timestamps):
            if stop_event.is_set():
                logger.warning("Portfolio simulation [%s] cancelled after %d timestamps", sim_id, step_idx)
                return self._finalize_simulation(
                    config=config,
                    status=PortfolioSimulationStatus.CANCELLED,
                    cash=cash,
                    positions=positions,
                    trades_by_strategy=trades_by_strategy,
                    fees_by_strategy=fees_by_strategy,
                    slippage_by_strategy=slippage_by_strategy,
                    orders_sub_by_strat=orders_sub_by_strat,
                    orders_acc_by_strat=orders_acc_by_strat,
                    orders_rej_by_strat=orders_rej_by_strat,
                    orders_submitted=orders_submitted,
                    orders_accepted=orders_accepted,
                    orders_rejected=orders_rejected,
                    rejected_orders=rejected_orders,
                    equity_curve=equity_curve,
                    risk_snapshots=risk_snapshots,
                    warnings=warnings,
                    peak_equity=peak_equity,
                    max_drawdown_pct=max_drawdown_pct,
                    max_drawdown_amount=max_drawdown_amount,
                    max_drawdown_duration_bars=max_drawdown_duration_bars,
                    bars_processed=step_idx,
                    start_ts=timestamps[0],
                    end_ts=ts,
                    duration_ms=(time.perf_counter() - start_time_perf) * 1000.0,
                    instrument_bar_returns=instrument_bar_returns,
                    strategy_bar_returns=strategy_bar_returns,
                    cid=cid,
                    liquidate_remaining=False,
                )

            # Update prices and bar histories for current timestamp
            current_bars = timeline[ts]
            for sym, bar in current_bars.items():
                bars_history_by_symbol[sym].append(bar)
                if sym in prev_bar_close and prev_bar_close[sym] > 0:
                    ret = (bar.close - prev_bar_close[sym]) / prev_bar_close[sym]
                    instrument_bar_returns[sym][ts] = ret
                prev_bar_close[sym] = bar.close
                last_known_prices[sym] = bar.close

            # ------------------------------------------------------------------
            # Phase 1: Fill Pending Orders from previous bar at Current Bar OPEN
            # ------------------------------------------------------------------
            if pending_orders:
                remaining_pending = []
                for alloc, order in pending_orders:
                    if order.symbol not in current_bars:
                        # Wait for next bar of this symbol
                        remaining_pending.append((alloc, order))
                        continue

                    open_price = current_bars[order.symbol].open
                    exec_price, slip_amount, _ = self._calculate_execution_price(
                        side=order.side,
                        base_price=open_price,
                        slippage_bps=config.slippage_bps,
                        half_spread_bps=config.half_spread_bps,
                    )
                    fee = self._calculate_fee(
                        quantity=order.quantity,
                        price=exec_price,
                        fee_model=config.fee_model,
                        fee_rate=config.fee_rate,
                        fixed_fee=config.fixed_fee,
                    )
                    slip_cost = slip_amount * order.quantity

                    if order.side == OrderSide.BUY:
                        total_out = (order.quantity * exec_price) + fee
                        if cash < total_out:
                            # Rejection at execution time due to insufficient cash
                            orders_rejected += 1
                            orders_acc_by_strat[alloc.allocation_id] = max(0, orders_acc_by_strat[alloc.allocation_id] - 1)
                            orders_rej_by_strat[alloc.allocation_id] += 1
                            rejected_orders.append({
                                "order_id": order.order_id,
                                "strategy_id": alloc.allocation_id,
                                "symbol": order.symbol,
                                "side": order.side.value,
                                "reason": f"INSUFFICIENT_CASH_ON_FILL: Cash ${cash:.2f} < ${total_out:.2f}",
                                "timestamp": ts,
                            })
                            continue

                        cash -= total_out
                        total_fees += fee
                        total_slippage += slip_cost
                        fees_by_strategy[alloc.allocation_id] += fee
                        slippage_by_strategy[alloc.allocation_id] += slip_cost

                        existing = positions.get(alloc.allocation_id)
                        if existing:
                            new_qty = existing.quantity + order.quantity
                            new_avg_cost = ((existing.quantity * existing.avg_cost) + (order.quantity * exec_price)) / new_qty
                            positions[alloc.allocation_id] = PortfolioPosition(
                                strategy_id=alloc.allocation_id,
                                symbol=alloc.symbol,
                                quantity=new_qty,
                                avg_cost=new_avg_cost,
                                current_price=exec_price,
                                entry_timestamp=existing.entry_timestamp,
                                accumulated_fees=existing.accumulated_fees + fee,
                            )
                        else:
                            positions[alloc.allocation_id] = PortfolioPosition(
                                strategy_id=alloc.allocation_id,
                                symbol=alloc.symbol,
                                quantity=order.quantity,
                                avg_cost=exec_price,
                                current_price=exec_price,
                                entry_timestamp=ts,
                                accumulated_fees=fee,
                            )

                    elif order.side == OrderSide.SELL:
                        existing = positions.get(alloc.allocation_id)
                        if not existing or existing.quantity <= 0.000001:
                            continue

                        qty_to_close = min(existing.quantity, order.quantity)
                        gross_pnl = (exec_price - existing.avg_cost) * qty_to_close
                        entry_fee_share = (qty_to_close / existing.quantity) * existing.accumulated_fees
                        trade_fees = entry_fee_share + fee
                        net_pnl = gross_pnl - trade_fees
                        duration = max(0.0, ts - existing.entry_timestamp)
                        ret_pct = ((exec_price - existing.avg_cost) / existing.avg_cost) * 100.0 if existing.avg_cost > 0 else 0.0

                        total_in = (qty_to_close * exec_price) - fee
                        cash += total_in
                        total_fees += fee
                        total_slippage += slip_cost
                        fees_by_strategy[alloc.allocation_id] += fee
                        slippage_by_strategy[alloc.allocation_id] += slip_cost

                        trade = TradeRecord(
                            trade_id=f"trade_{uuid.uuid4().hex[:10]}",
                            symbol=alloc.symbol,
                            side=OrderSide.BUY,
                            entry_time=existing.entry_timestamp,
                            exit_time=ts,
                            entry_price=existing.avg_cost,
                            exit_price=exec_price,
                            quantity=qty_to_close,
                            gross_pnl=gross_pnl,
                            net_pnl=net_pnl,
                            fees=trade_fees,
                            slippage=slip_cost,
                            return_pct=ret_pct,
                            duration_seconds=duration,
                            is_win=(net_pnl > 0),
                        )
                        trades_by_strategy[alloc.allocation_id].append(trade)

                        rem_qty = existing.quantity - qty_to_close
                        if rem_qty <= 0.000001:
                            del positions[alloc.allocation_id]
                        else:
                            positions[alloc.allocation_id] = PortfolioPosition(
                                strategy_id=alloc.allocation_id,
                                symbol=alloc.symbol,
                                quantity=rem_qty,
                                avg_cost=existing.avg_cost,
                                current_price=exec_price,
                                entry_timestamp=existing.entry_timestamp,
                                accumulated_fees=existing.accumulated_fees - entry_fee_share,
                            )

                pending_orders = remaining_pending

            # ------------------------------------------------------------------
            # Phase 2: Mark Portfolio to Market at Current Bar CLOSE
            # ------------------------------------------------------------------
            for alloc_id, pos in list(positions.items()):
                if pos.symbol in last_known_prices:
                    positions[alloc_id] = PortfolioPosition(
                        strategy_id=pos.strategy_id,
                        symbol=pos.symbol,
                        quantity=pos.quantity,
                        avg_cost=pos.avg_cost,
                        current_price=last_known_prices[pos.symbol],
                        entry_timestamp=pos.entry_timestamp,
                        accumulated_fees=pos.accumulated_fees,
                    )

            total_position_val = sum(p.market_value for p in positions.values())
            total_equity = cash + total_position_val

            if total_equity > peak_equity:
                peak_equity = total_equity
                current_drawdown_bars = 0
            else:
                current_drawdown_bars += 1

            if current_drawdown_bars > max_drawdown_duration_bars:
                max_drawdown_duration_bars = current_drawdown_bars

            dd_amount = max(0.0, peak_equity - total_equity)
            dd_pct = (dd_amount / peak_equity) * 100.0 if peak_equity > 0 else 0.0
            if dd_pct > max_drawdown_pct:
                max_drawdown_pct = dd_pct
            if dd_amount > max_drawdown_amount:
                max_drawdown_amount = dd_amount

            # Circuit breaker check
            if config.max_drawdown_limit_pct is not None and dd_pct >= config.max_drawdown_limit_pct:
                if not circuit_breaker_triggered:
                    circuit_breaker_triggered = True
                    warnings.append(
                        PortfolioRiskWarning(
                            timestamp=ts,
                            warning_type="CIRCUIT_BREAKER_TRIGGERED",
                            message=f"Drawdown {dd_pct:.2f}% breached circuit-breaker threshold {config.max_drawdown_limit_pct:.2f}%. Halting new long entries.",
                            details={"drawdown_pct": dd_pct, "peak_equity": peak_equity, "total_equity": total_equity},
                        )
                    )

            # Record EquityPoint
            eq_pt = EquityPoint(
                timestamp=ts,
                cash=cash,
                position_value=total_position_val,
                total_equity=total_equity,
                drawdown_pct=dd_pct,
                drawdown_amount=dd_amount,
            )
            equity_curve.append(eq_pt)

            # Record PortfolioRiskSnapshot
            gross_exp = sum(abs(p.market_value) for p in positions.values())
            gross_exp_pct = (gross_exp / total_equity) * 100.0 if total_equity > 0 else 0.0
            net_exp = sum(p.market_value for p in positions.values())
            net_exp_pct = (net_exp / total_equity) * 100.0 if total_equity > 0 else 0.0
            cash_res_pct = (cash / total_equity) * 100.0 if total_equity > 0 else 0.0

            strat_exps = {p.strategy_id: p.market_value for p in positions.values()}
            inst_exps: Dict[str, float] = {}
            for p in positions.values():
                inst_exps[p.symbol] = inst_exps.get(p.symbol, 0.0) + p.market_value

            max_inst_conc = (max(inst_exps.values()) / total_equity * 100.0) if (inst_exps and total_equity > 0) else 0.0
            inst_hhi = sum((v / total_equity) ** 2 for v in inst_exps.values()) if (inst_exps and total_equity > 0) else 0.0

            risk_snap = PortfolioRiskSnapshot(
                timestamp=ts,
                total_equity=total_equity,
                cash=cash,
                cash_reserve_pct=cash_res_pct,
                gross_exposure=gross_exp,
                gross_exposure_pct=gross_exp_pct,
                net_exposure=net_exp,
                net_exposure_pct=net_exp_pct,
                strategy_exposures=strat_exps,
                instrument_exposures=inst_exps,
                max_instrument_concentration_pct=max_inst_conc,
                herfindahl_index=inst_hhi,
                drawdown_pct=dd_pct,
                circuit_breaker_active=circuit_breaker_triggered,
            )
            risk_snapshots.append(risk_snap)

            # Record periodic strategy return for correlation
            for alloc in config.allocations:
                alloc_id = alloc.allocation_id
                strat_pos = positions.get(alloc_id)
                strat_realized = sum(t.net_pnl for t in trades_by_strategy[alloc_id])
                strat_unrealized = strat_pos.unrealized_pnl if strat_pos else 0.0
                curr_strat_val = (alloc.weight * config.initial_cash) + strat_realized + strat_unrealized
                prev_val = prev_strategy_equity[alloc_id]
                if prev_val > 0:
                    strategy_bar_returns[alloc_id][ts] = (curr_strat_val - prev_val) / prev_val
                prev_strategy_equity[alloc_id] = curr_strat_val

            # ------------------------------------------------------------------
            # Phase 3: Strategy Evaluation on Candle Close
            # ------------------------------------------------------------------
            candidate_signals: List[Tuple[StrategyAllocation, StrategySignal, Bar]] = []
            for alloc in config.allocations:
                if alloc.symbol not in current_bars:
                    continue

                bar = current_bars[alloc.symbol]
                strat = strategies[alloc.allocation_id]
                pos = positions.get(alloc.allocation_id)
                pos_qty = pos.quantity if pos else 0.0
                pos_cost = pos.avg_cost if pos else 0.0
                pos_val = pos.market_value if pos else 0.0
                allocated_equity = alloc.weight * total_equity

                feature_vec = None
                if self.analysis_engine:
                    feature_vec = self.analysis_engine.get_feature_vector(alloc.symbol, config.timeframe)

                ctx = StrategyContext(
                    symbol=alloc.symbol,
                    timeframe=config.timeframe,
                    current_bar=bar,
                    timestamp=ts,
                    feature_vector=feature_vec,
                    portfolio_cash=cash,
                    position_quantity=pos_qty,
                    position_avg_cost=pos_cost,
                    position_market_value=pos_val,
                    portfolio_equity=allocated_equity,
                    bars_history=tuple(bars_history_by_symbol[alloc.symbol]),
                )

                try:
                    sig = strat.on_bar(ctx)
                    if sig is not None:
                        candidate_signals.append((alloc, sig, bar))
                except Exception as err:
                    logger.error("Strategy '%s' raised exception on bar ts=%f: %s", alloc.allocation_id, ts, err)
                    warnings.append(
                        PortfolioRiskWarning(
                            timestamp=ts,
                            warning_type="STRATEGY_EXECUTION_ERROR",
                            message=f"Strategy '{alloc.allocation_id}' raised error: {err}",
                            details={"strategy_id": alloc.allocation_id, "error": str(err)},
                        )
                    )

            # ------------------------------------------------------------------
            # Phase 4: Deterministic Order Prioritization & Central Risk Validation
            # ------------------------------------------------------------------
            if candidate_signals:
                # Deterministic Sorting Rule:
                # 1. SELL signals first (free cash and reduce exposure)
                # 2. Allocation weight descending
                # 3. Signal strength descending
                # 4. Strategy allocation_id ascending (alphabetical tie-breaker)
                # 5. Symbol ascending
                def signal_sort_key(item: Tuple[StrategyAllocation, StrategySignal, Bar]) -> Tuple[int, float, float, str, str]:
                    a, s, _ = item
                    side_rank = 0 if s.signal_type.value == OrderSide.SELL.value else 1
                    return (
                        side_rank,
                        -a.weight,
                        -float(s.strength or 1.0),
                        a.allocation_id,
                        a.symbol,
                    )

                sorted_signals = sorted(candidate_signals, key=signal_sort_key)

                # Simulated available cash buffer for order sizing across simultaneous signals
                sim_free_cash = cash

                for alloc, sig, bar in sorted_signals:
                    orders_submitted += 1
                    orders_sub_by_strat[alloc.allocation_id] += 1

                    if sig.signal_type.value == OrderSide.SELL.value:
                        pos = positions.get(alloc.allocation_id)
                        if not pos or pos.quantity <= 0.000001:
                            orders_rejected += 1
                            orders_rej_by_strat[alloc.allocation_id] += 1
                            rejected_orders.append({
                                "order_id": f"ord_rej_{uuid.uuid4().hex[:8]}",
                                "strategy_id": alloc.allocation_id,
                                "symbol": alloc.symbol,
                                "side": OrderSide.SELL.value,
                                "reason": "NO_POSITION_TO_SELL: Strategy holds 0 units",
                                "timestamp": ts,
                            })
                            continue

                        sell_qty = pos.quantity if sig.suggested_quantity is None else min(pos.quantity, sig.suggested_quantity)
                        order = SimulatedOrder(
                            order_id=f"ord_{uuid.uuid4().hex[:10]}",
                            symbol=alloc.symbol,
                            side=OrderSide.SELL,
                            order_type=OrderType.MARKET,
                            quantity=sell_qty,
                            price=bar.close,
                            created_at=ts,
                            status=OrderStatus.PENDING,
                            correlation_id=cid,
                        )
                        pending_orders.append((alloc, order))
                        orders_accepted += 1
                        orders_acc_by_strat[alloc.allocation_id] += 1

                    elif sig.signal_type.value == OrderSide.BUY.value:
                        # 1. Circuit Breaker Check
                        if circuit_breaker_triggered:
                            orders_rejected += 1
                            orders_rej_by_strat[alloc.allocation_id] += 1
                            rejected_orders.append({
                                "order_id": f"ord_rej_{uuid.uuid4().hex[:8]}",
                                "strategy_id": alloc.allocation_id,
                                "symbol": alloc.symbol,
                                "side": OrderSide.BUY.value,
                                "reason": "CIRCUIT_BREAKER_ACTIVE: Drawdown limit breached",
                                "timestamp": ts,
                            })
                            continue

                        # Sizing calculation
                        allocated_equity = alloc.weight * total_equity
                        if alloc.position_size_type == PositionSizeType.FIXED_QUANTITY.value:
                            qty = alloc.position_size_value
                        elif alloc.position_size_type == PositionSizeType.FIXED_CASH.value:
                            qty = alloc.position_size_value / bar.close if bar.close > 0 else 0.0
                        else:  # percent_equity
                            qty = (allocated_equity * alloc.position_size_value) / bar.close if bar.close > 0 else 0.0

                        if sig.suggested_quantity is not None:
                            qty = sig.suggested_quantity

                        qty = round(qty, 4)
                        if qty <= 0.0:
                            orders_rejected += 1
                            orders_rej_by_strat[alloc.allocation_id] += 1
                            rejected_orders.append({
                                "order_id": f"ord_rej_{uuid.uuid4().hex[:8]}",
                                "strategy_id": alloc.allocation_id,
                                "symbol": alloc.symbol,
                                "side": OrderSide.BUY.value,
                                "reason": f"INVALID_QUANTITY: Computed quantity {qty} <= 0",
                                "timestamp": ts,
                            })
                            continue

                        # Estimated Cost
                        est_price, _, _ = self._calculate_execution_price(
                            OrderSide.BUY, bar.close, config.slippage_bps, config.half_spread_bps
                        )
                        est_fee = self._calculate_fee(
                            qty, est_price, config.fee_model, config.fee_rate, config.fixed_fee
                        )
                        req_cash = (qty * est_price) + est_fee

                        # 2. Free Cash & Cash Reserve Gate
                        min_cash_reserve = config.initial_cash * config.min_cash_reserve_pct
                        available_for_trading = max(0.0, sim_free_cash - min_cash_reserve)
                        if req_cash > available_for_trading:
                            reason = (
                                "CASH_RESERVE_VIOLATION"
                                if (sim_free_cash - req_cash) < min_cash_reserve and sim_free_cash >= req_cash
                                else "INSUFFICIENT_CASH"
                            )
                            orders_rejected += 1
                            orders_rej_by_strat[alloc.allocation_id] += 1
                            rejected_orders.append({
                                "order_id": f"ord_rej_{uuid.uuid4().hex[:8]}",
                                "strategy_id": alloc.allocation_id,
                                "symbol": alloc.symbol,
                                "side": OrderSide.BUY.value,
                                "reason": f"{reason}: Required ${req_cash:.2f} > Available ${available_for_trading:.2f}",
                                "timestamp": ts,
                            })
                            continue

                        # 3. Maximum Portfolio Exposure Gate
                        current_gross_exp = sum(abs(p.market_value) for p in positions.values())
                        new_port_exp = current_gross_exp + (qty * est_price)
                        max_allowed_port_exp = total_equity * config.max_portfolio_exposure_pct
                        if new_port_exp > max_allowed_port_exp * 1.0001:
                            orders_rejected += 1
                            orders_rej_by_strat[alloc.allocation_id] += 1
                            rejected_orders.append({
                                "order_id": f"ord_rej_{uuid.uuid4().hex[:8]}",
                                "strategy_id": alloc.allocation_id,
                                "symbol": alloc.symbol,
                                "side": OrderSide.BUY.value,
                                "reason": f"MAX_PORTFOLIO_EXPOSURE_EXCEEDED: ${new_port_exp:.2f} > max ${max_allowed_port_exp:.2f}",
                                "timestamp": ts,
                            })
                            continue

                        # 4. Per-Strategy Exposure Gate
                        strat_cap_pct = alloc.max_exposure_pct or config.max_strategy_exposure_pct
                        curr_strat_pos = positions.get(alloc.allocation_id)
                        curr_strat_exp = curr_strat_pos.market_value if curr_strat_pos else 0.0
                        new_strat_exp = curr_strat_exp + (qty * est_price)
                        max_strat_exp = total_equity * strat_cap_pct
                        if new_strat_exp > max_strat_exp * 1.0001:
                            orders_rejected += 1
                            orders_rej_by_strat[alloc.allocation_id] += 1
                            rejected_orders.append({
                                "order_id": f"ord_rej_{uuid.uuid4().hex[:8]}",
                                "strategy_id": alloc.allocation_id,
                                "symbol": alloc.symbol,
                                "side": OrderSide.BUY.value,
                                "reason": f"STRATEGY_EXPOSURE_LIMIT_EXCEEDED: ${new_strat_exp:.2f} > max ${max_strat_exp:.2f}",
                                "timestamp": ts,
                            })
                            continue

                        # 5. Instrument Concentration Gate
                        curr_inst_exp = sum(p.market_value for p in positions.values() if p.symbol == alloc.symbol)
                        new_inst_exp = curr_inst_exp + (qty * est_price)
                        max_inst_exp = total_equity * config.max_instrument_concentration_pct
                        if new_inst_exp > max_inst_exp * 1.0001:
                            orders_rejected += 1
                            orders_rej_by_strat[alloc.allocation_id] += 1
                            rejected_orders.append({
                                "order_id": f"ord_rej_{uuid.uuid4().hex[:8]}",
                                "strategy_id": alloc.allocation_id,
                                "symbol": alloc.symbol,
                                "side": OrderSide.BUY.value,
                                "reason": f"INSTRUMENT_CONCENTRATION_EXCEEDED: Instrument {alloc.symbol} ${new_inst_exp:.2f} > max ${max_inst_exp:.2f}",
                                "timestamp": ts,
                            })
                            continue

                        # Order Accepted and Queued for Next-Bar Execution
                        order = SimulatedOrder(
                            order_id=f"ord_{uuid.uuid4().hex[:10]}",
                            symbol=alloc.symbol,
                            side=OrderSide.BUY,
                            order_type=OrderType.MARKET,
                            quantity=qty,
                            price=bar.close,
                            created_at=ts,
                            status=OrderStatus.PENDING,
                            correlation_id=cid,
                        )
                        pending_orders.append((alloc, order))
                        sim_free_cash -= req_cash
                        orders_accepted += 1
                        orders_acc_by_strat[alloc.allocation_id] += 1

            if config.pacing_seconds > 0:
                time.sleep(config.pacing_seconds)

        # 9. Simulation Completed - Finalize Result
        duration_ms = (time.perf_counter() - start_time_perf) * 1000.0
        return self._finalize_simulation(
            config=config,
            status=PortfolioSimulationStatus.SUCCESS,
            cash=cash,
            positions=positions,
            trades_by_strategy=trades_by_strategy,
            fees_by_strategy=fees_by_strategy,
            slippage_by_strategy=slippage_by_strategy,
            orders_sub_by_strat=orders_sub_by_strat,
            orders_acc_by_strat=orders_acc_by_strat,
            orders_rej_by_strat=orders_rej_by_strat,
            orders_submitted=orders_submitted,
            orders_accepted=orders_accepted,
            orders_rejected=orders_rejected,
            rejected_orders=rejected_orders,
            equity_curve=equity_curve,
            risk_snapshots=risk_snapshots,
            warnings=warnings,
            peak_equity=peak_equity,
            max_drawdown_pct=max_drawdown_pct,
            max_drawdown_amount=max_drawdown_amount,
            max_drawdown_duration_bars=max_drawdown_duration_bars,
            bars_processed=len(timestamps),
            start_ts=timestamps[0] if timestamps else config.start_time,
            end_ts=timestamps[-1] if timestamps else config.end_time,
            duration_ms=duration_ms,
            instrument_bar_returns=instrument_bar_returns,
            strategy_bar_returns=strategy_bar_returns,
            cid=cid,
            liquidate_remaining=True,
            last_known_prices=last_known_prices,
            analysis_run_id=analysis_run.run_id if analysis_run else None,
        )

    def _finalize_simulation(
        self,
        config: PortfolioSimulationConfig,
        status: PortfolioSimulationStatus,
        cash: float,
        positions: Dict[str, PortfolioPosition],
        trades_by_strategy: Dict[str, List[TradeRecord]],
        fees_by_strategy: Dict[str, float],
        slippage_by_strategy: Dict[str, float],
        orders_sub_by_strat: Dict[str, int],
        orders_acc_by_strat: Dict[str, int],
        orders_rej_by_strat: Dict[str, int],
        orders_submitted: int,
        orders_accepted: int,
        orders_rejected: int,
        rejected_orders: List[Dict[str, Any]],
        equity_curve: List[EquityPoint],
        risk_snapshots: List[PortfolioRiskSnapshot],
        warnings: List[PortfolioRiskWarning],
        peak_equity: float,
        max_drawdown_pct: float,
        max_drawdown_amount: float,
        max_drawdown_duration_bars: int,
        bars_processed: int,
        start_ts: float,
        end_ts: float,
        duration_ms: float,
        instrument_bar_returns: Dict[str, Dict[float, float]],
        strategy_bar_returns: Dict[str, Dict[float, float]],
        cid: str,
        liquidate_remaining: bool = True,
        last_known_prices: Optional[Dict[str, float]] = None,
        analysis_run_id: Optional[str] = None,
    ) -> PortfolioSimulationResult:
        """Close open positions, assemble strategy contributions, compute metrics, and publish events."""
        final_cash = cash
        last_prices = last_known_prices or {}

        # Liquidate remaining open positions at final close price for complete accounting
        if liquidate_remaining and positions:
            for alloc_id, pos in list(positions.items()):
                alloc = next((a for a in config.allocations if a.allocation_id == alloc_id), None)
                if not alloc:
                    continue
                exit_price = last_prices.get(pos.symbol, pos.avg_cost)
                exec_price, slip_amount, _ = self._calculate_execution_price(
                    side=OrderSide.SELL,
                    base_price=exit_price,
                    slippage_bps=config.slippage_bps,
                    half_spread_bps=config.half_spread_bps,
                )
                fee = self._calculate_fee(
                    quantity=pos.quantity,
                    price=exec_price,
                    fee_model=config.fee_model,
                    fee_rate=config.fee_rate,
                    fixed_fee=config.fixed_fee,
                )
                slip_cost = slip_amount * pos.quantity

                gross_pnl = (exec_price - pos.avg_cost) * pos.quantity
                trade_fees = pos.accumulated_fees + fee
                net_pnl = gross_pnl - trade_fees
                duration = max(0.0, end_ts - pos.entry_timestamp)
                ret_pct = ((exec_price - pos.avg_cost) / pos.avg_cost) * 100.0 if pos.avg_cost > 0 else 0.0

                trade = TradeRecord(
                    trade_id=f"trade_close_{uuid.uuid4().hex[:10]}",
                    symbol=pos.symbol,
                    side=OrderSide.BUY,
                    entry_time=pos.entry_timestamp,
                    exit_time=end_ts,
                    entry_price=pos.avg_cost,
                    exit_price=exec_price,
                    quantity=pos.quantity,
                    gross_pnl=gross_pnl,
                    net_pnl=net_pnl,
                    fees=trade_fees,
                    slippage=slip_cost,
                    return_pct=ret_pct,
                    duration_seconds=duration,
                    is_win=(net_pnl > 0),
                )
                trades_by_strategy[alloc_id].append(trade)
                fees_by_strategy[alloc_id] += fee
                slippage_by_strategy[alloc_id] += slip_cost
                final_cash += (pos.quantity * exec_price) - fee

            positions.clear()

        final_equity = final_cash if liquidate_remaining else (final_cash + sum(p.market_value for p in positions.values()))
        initial_equity = config.initial_cash
        net_profit = final_equity - initial_equity
        net_return_pct = (net_profit / initial_equity) * 100.0 if initial_equity > 0 else 0.0

        all_trades: List[TradeRecord] = []
        for tlist in trades_by_strategy.values():
            all_trades.extend(tlist)

        total_trades = len(all_trades)
        winning_trades = sum(1 for t in all_trades if t.is_win)
        losing_trades = sum(1 for t in all_trades if not t.is_win and t.net_pnl < 0)
        win_rate = (winning_trades / total_trades) * 100.0 if total_trades > 0 else 0.0

        gross_profit = sum(t.gross_pnl for t in all_trades if t.gross_pnl > 0)
        gross_loss = abs(sum(t.gross_pnl for t in all_trades if t.gross_pnl < 0))
        total_fees = sum(fees_by_strategy.values())
        total_slippage = sum(slippage_by_strategy.values())
        gross_pnl = net_profit + total_fees + total_slippage

        profit_factor, pf_desc = self._calculate_profit_factor(total_trades, gross_profit, gross_loss)

        # Strategy Contributions
        contributions: List[StrategyContribution] = []
        for alloc in config.allocations:
            alloc_id = alloc.allocation_id
            strats_trades = trades_by_strategy.get(alloc_id, [])
            s_trades_count = len(strats_trades)
            s_wins = sum(1 for t in strats_trades if t.is_win)
            s_losses = sum(1 for t in strats_trades if not t.is_win and t.net_pnl < 0)
            s_win_rate = (s_wins / s_trades_count) * 100.0 if s_trades_count > 0 else 0.0

            s_gross_prof = sum(t.gross_pnl for t in strats_trades if t.gross_pnl > 0)
            s_gross_loss = abs(sum(t.gross_pnl for t in strats_trades if t.gross_pnl < 0))
            s_pf, s_pf_desc = self._calculate_profit_factor(s_trades_count, s_gross_prof, s_gross_loss)

            s_net_pnl = sum(t.net_pnl for t in strats_trades)
            s_gross_pnl = s_net_pnl + fees_by_strategy[alloc_id] + slippage_by_strategy[alloc_id]
            s_allocated = alloc.weight * initial_equity
            s_net_ret = (s_net_pnl / s_allocated) * 100.0 if s_allocated > 0 else 0.0
            pnl_share = (s_net_pnl / net_profit) * 100.0 if abs(net_profit) > 1e-6 else 0.0

            sc = StrategyContribution(
                strategy_id=alloc_id,
                strategy_name=alloc.strategy_name,
                symbol=alloc.symbol,
                weight=alloc.weight,
                allocated_capital=s_allocated,
                gross_pnl=s_gross_pnl,
                net_pnl=s_net_pnl,
                net_return_pct=s_net_ret,
                portfolio_pnl_share_pct=pnl_share,
                total_trades=s_trades_count,
                winning_trades=s_wins,
                losing_trades=s_losses,
                win_rate=s_win_rate,
                profit_factor=s_pf,
                profit_factor_description=s_pf_desc,
                total_fees=fees_by_strategy[alloc_id],
                total_slippage=slippage_by_strategy[alloc_id],
                orders_submitted=orders_sub_by_strat[alloc_id],
                orders_accepted=orders_acc_by_strat[alloc_id],
                orders_rejected=orders_rej_by_strat[alloc_id],
                trades=tuple(strats_trades),
            )
            contributions.append(sc)

        # Diversification Analytics
        diversification = compute_diversification_metrics(
            allocations=config.allocations,
            instrument_returns=instrument_bar_returns,
            strategy_returns=strategy_bar_returns,
            min_observations=config.min_correlation_observations,
        )

        result = PortfolioSimulationResult(
            simulation_id=config.simulation_id,
            status=status,
            config=config,
            initial_equity=initial_equity,
            final_equity=final_equity,
            gross_pnl=gross_pnl,
            net_pnl=net_profit,
            net_return_pct=net_return_pct,
            max_drawdown_pct=max_drawdown_pct,
            max_drawdown_amount=max_drawdown_amount,
            max_drawdown_duration_bars=max_drawdown_duration_bars,
            total_fees=total_fees,
            total_slippage=total_slippage,
            total_transaction_costs=total_fees + total_slippage,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            profit_factor_description=pf_desc,
            equity_curve=tuple(equity_curve),
            risk_snapshots=tuple(risk_snapshots),
            strategy_contributions=tuple(contributions),
            diversification=diversification,
            orders_submitted=orders_submitted,
            orders_accepted=orders_accepted,
            orders_rejected=orders_rejected,
            rejected_orders=tuple(rejected_orders),
            warnings=tuple(warnings),
            bars_processed=bars_processed,
            start_timestamp=start_ts,
            end_timestamp=end_ts,
            duration_ms=duration_ms,
            run_id=analysis_run_id,
        )

        # Cleanup active simulation registry
        with self._lock:
            self._active_simulations.pop(config.simulation_id, None)

        # Record Portfolio Simulation Provenance
        if getattr(self, "provenance_tracker", None) is not None:
            try:
                symbols = list(config.symbols) if hasattr(config, "symbols") and config.symbols else [alloc.symbol for alloc in config.allocations]
                timeframes = [getattr(config, "timeframe", "1m")]
                parent_ids = []
                for s in symbols:
                    p = self.provenance_tracker.get_latest(s, timeframes[0])
                    if p:
                        parent_ids.append(p.provenance_id)
                strat_names = [alloc.strategy_name for alloc in config.allocations]
                prov = self.provenance_tracker.create_portfolio_simulation_provenance(
                    simulation_id=result.simulation_id,
                    symbols=symbols,
                    timeframes=timeframes,
                    data_start=result.start_timestamp or config.start_time,
                    data_end=result.end_timestamp or config.end_time,
                    bars_evaluated=result.bars_processed,
                    trades_simulated=result.total_trades,
                    parent_provenance_ids=parent_ids,
                    strategy_names=strat_names,
                    operation_id=result.simulation_id,
                )
                if hasattr(result, "provenance_id"):
                    object.__setattr__(result, "provenance_id", prov.provenance_id)
            except Exception as exc:
                logger.warning("Failed to record portfolio simulation provenance: %s", exc)

        # StateStore persistence
        if self.state_store:
            try:
                self.state_store.set(f"portfolio_simulation:{config.simulation_id}", result.to_dict())
                self.state_store.set(f"portfolio_simulation_obj:{config.simulation_id}", result)
            except Exception as e:
                logger.warning("Failed to store portfolio simulation in state store: %s", e)

        # Finalize Analysis Run in Registry
        if getattr(self, "run_registry", None) is not None and getattr(result, "run_id", None):
            try:
                if status == PortfolioSimulationStatus.SUCCESS:
                    self.run_registry.complete_run(
                        run_id=result.run_id,
                        result_id=result.simulation_id,
                        result_type="PortfolioSimulationResult",
                    )
                elif status == PortfolioSimulationStatus.CANCELLED:
                    self.run_registry.cancel_run(
                        run_id=result.run_id,
                        cancellation_reason="Simulation cancelled by operator or token",
                    )
                else:
                    self.run_registry.fail_run(
                        run_id=result.run_id,
                        error_category="PORTFOLIO_SIMULATION_ERROR",
                        error_message=result.error_message or "Portfolio simulation failure",
                    )
            except Exception as exc:
                logger.warning("Failed to finalize analysis run in portfolio simulation: %s", exc)

        # Publish Completion / Cancellation Event
        if self.event_bus:
            ev_type = (
                EventType.PORTFOLIO_SIMULATION_COMPLETED
                if status == PortfolioSimulationStatus.SUCCESS
                else (
                    EventType.PORTFOLIO_SIMULATION_CANCELLED
                    if status == PortfolioSimulationStatus.CANCELLED
                    else EventType.PORTFOLIO_SIMULATION_FAILED
                )
            )
            self.event_bus.publish(
                Event.create(
                    event_type=ev_type,
                    source=self.name,
                    correlation_id=cid,
                    payload={
                        "simulation_id": config.simulation_id,
                        "status": status.value,
                        "initial_equity": initial_equity,
                        "final_equity": final_equity,
                        "net_pnl": net_profit,
                        "net_return_pct": net_return_pct,
                        "max_drawdown_pct": max_drawdown_pct,
                        "total_trades": total_trades,
                        "strategies_count": len(contributions),
                    },
                )
            )

        return result

    def _build_terminal_result(
        self,
        config: PortfolioSimulationConfig,
        status: PortfolioSimulationStatus,
        error_message: str,
        duration_ms: float,
        analysis_run_id: Optional[str] = None,
    ) -> PortfolioSimulationResult:
        """Construct an immediate terminal failed or cancelled result."""
        with self._lock:
            self._active_simulations.pop(config.simulation_id, None)

        if getattr(self, "run_registry", None) is not None and analysis_run_id:
            try:
                if status == PortfolioSimulationStatus.CANCELLED:
                    self.run_registry.cancel_run(
                        run_id=analysis_run_id,
                        cancellation_reason=error_message,
                    )
                else:
                    self.run_registry.fail_run(
                        run_id=analysis_run_id,
                        error_category="PORTFOLIO_SIMULATION_ERROR",
                        error_message=error_message,
                    )
            except Exception as exc:
                logger.warning("Failed to finalize terminal analysis run: %s", exc)

        empty_contribs = tuple(
            StrategyContribution(
                strategy_id=alloc.allocation_id,
                strategy_name=alloc.strategy_name,
                symbol=alloc.symbol,
                weight=alloc.weight,
                allocated_capital=alloc.weight * config.initial_cash,
                gross_pnl=0.0,
                net_pnl=0.0,
                net_return_pct=0.0,
                portfolio_pnl_share_pct=0.0,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                win_rate=0.0,
                profit_factor=None,
                profit_factor_description="N/A",
                total_fees=0.0,
                total_slippage=0.0,
                orders_submitted=0,
                orders_accepted=0,
                orders_rejected=0,
            )
            for alloc in config.allocations
        )

        res = PortfolioSimulationResult(
            simulation_id=config.simulation_id,
            status=status,
            config=config,
            initial_equity=config.initial_cash,
            final_equity=config.initial_cash,
            gross_pnl=0.0,
            net_pnl=0.0,
            net_return_pct=0.0,
            max_drawdown_pct=0.0,
            max_drawdown_amount=0.0,
            max_drawdown_duration_bars=0,
            total_fees=0.0,
            total_slippage=0.0,
            total_transaction_costs=0.0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            win_rate=0.0,
            profit_factor=None,
            profit_factor_description="N/A",
            strategy_contributions=empty_contribs,
            duration_ms=duration_ms,
            error_message=error_message,
            run_id=analysis_run_id,
        )

        if self.event_bus:
            ev_type = (
                EventType.PORTFOLIO_SIMULATION_CANCELLED
                if status == PortfolioSimulationStatus.CANCELLED
                else EventType.PORTFOLIO_SIMULATION_FAILED
            )
            self.event_bus.publish(
                Event.create(
                    event_type=ev_type,
                    source=self.name,
                    correlation_id=config.correlation_id or config.simulation_id,
                    payload={"simulation_id": config.simulation_id, "status": status.value, "error": error_message},
                )
            )

        return res

    def _calculate_execution_price(
        self,
        side: OrderSide,
        base_price: float,
        slippage_bps: float,
        half_spread_bps: float,
    ) -> Tuple[float, float, float]:
        """Compute execution price, slippage amount, and half-spread adjustment."""
        slip_pct = slippage_bps / 10_000.0
        spread_pct = half_spread_bps / 10_000.0
        tot_adj = slip_pct + spread_pct

        if side == OrderSide.BUY:
            exec_price = base_price * (1.0 + tot_adj)
        else:
            exec_price = max(0.0001, base_price * (1.0 - tot_adj))

        return exec_price, base_price * slip_pct, base_price * spread_pct

    def _calculate_fee(
        self,
        quantity: float,
        price: float,
        fee_model: str,
        fee_rate: float,
        fixed_fee: float,
    ) -> float:
        """Calculate transaction fee for a simulated fill."""
        if fee_model == FeeModel.FIXED.value:
            return max(0.0, fixed_fee)
        trade_val = quantity * price
        return max(0.0, (trade_val * fee_rate) + fixed_fee)

    def _calculate_profit_factor(
        self, total_trades: int, gross_profit: float, gross_loss: float
    ) -> Tuple[Optional[float], str]:
        """Safely compute profit factor without division by zero or infinite values."""
        if total_trades == 0:
            return None, "N/A (No trades)"
        if gross_loss <= 1e-12:
            if gross_profit > 0:
                return None, "Infinite (Zero loss)"
            return 0.0, "0.00"
        pf = round(gross_profit / gross_loss, 4)
        return pf, f"{pf:.2f}"
