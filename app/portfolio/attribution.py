"""KAISER — Portfolio Performance Attribution & Risk-Adjusted Analytics Service.

Provides a unified, deterministic analytics engine for:
1. Multi-strategy and standalone portfolio performance attribution.
2. Instrument-level attribution.
3. Accounting reconciliation (portfolio vs strategy PnL, gross vs net performance).
4. Multi-entity comparisons:
   - Portfolio simulation runs.
   - In-Sample vs Out-of-Sample walk-forward windows.
   - Robustness baseline vs stress scenarios.
5. EventBus publication and StateStore persistence.

SAFETY INVARIANTS:
1. Read-only observational analytics: Never places orders, liquidates positions, or connects to live brokers.
2. Incomplete or missing data results in explicit `is_available=False` with clear diagnostic reasons.
3. All accounting discrepancies are audited and reported deterministically.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from app.backtesting.models import BacktestResult
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationResult,
    StrategyContribution,
)
from app.backtesting.robustness.models import RobustnessTestResult
from app.backtesting.validation.models import WalkForwardResult
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state.store import StateStore
from app.portfolio.metrics import (
    calculate_trade_metrics,
    compute_risk_adjusted_metrics,
)
from app.portfolio.models import (
    AttributionConfig,
    ComparisonEntity,
    ComparisonReport,
    ComparisonType,
    InstrumentAttribution,
    MetricValue,
    PortfolioAttributionReport,
    ReconciliationReport,
    ReconciliationStatus,
    RiskAdjustedMetrics,
    StrategyAttribution,
    TransactionCostAttribution,
)

logger = logging.getLogger("kaiser.portfolio.attribution")


class PortfolioAttributionService:
    """Core analytics service for portfolio performance attribution and risk-adjusted metrics."""

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        clock_fn: Optional[Callable[[], float]] = None,
        run_registry: Optional[Any] = None,
    ) -> None:
        self.event_bus = event_bus
        self.state_store = state_store
        self._clock_fn = clock_fn or time.time
        self.run_registry = run_registry
        self._latest_report: Optional[PortfolioAttributionReport] = None
        self._latest_comparison: Optional[ComparisonReport] = None

    def analyze_simulation_result(
        self,
        result: PortfolioSimulationResult,
        config: Optional[AttributionConfig] = None,
        correlation_id: Optional[str] = None,
    ) -> PortfolioAttributionReport:
        """Compute complete performance attribution and risk-adjusted metrics for a portfolio simulation.

        Args:
            result: Completed PortfolioSimulationResult.
            config: Optional AttributionConfig overrides.
            correlation_id: Optional tracing correlation ID.

        Returns:
            PortfolioAttributionReport.
        """
        if not isinstance(result, PortfolioSimulationResult):
            raise ValidationError(
                f"Expected PortfolioSimulationResult, got {type(result).__name__}"
            )

        cfg = config or AttributionConfig()
        cid = correlation_id or uuid.uuid4().hex
        now = self._clock_fn()

        report_id = f"attr_{uuid.uuid4().hex[:10]}"

        # Setup Analysis Run Registry
        analysis_run = None
        if getattr(self, "run_registry", None) is not None:
            try:
                from app.analysis.runs.models import OperationType
                parents = [result.run_id] if getattr(result, "run_id", None) else []
                prov_ids = [result.provenance_id] if getattr(result, "provenance_id", None) else []
                symbols = [a.symbol for a in result.strategy_contributions] if result.strategy_contributions else []
                analysis_run = self.run_registry.create_run(
                    operation_type=OperationType.PORTFOLIO_ATTRIBUTION,
                    configuration=cfg,
                    input_provenance_ids=prov_ids,
                    parent_run_ids=parents,
                    symbol_universe=tuple(symbols),
                    data_start=result.start_timestamp,
                    data_end=result.end_timestamp,
                    execution_metadata={"source_id": result.simulation_id, "source_type": "portfolio_simulation"},
                )
                self.run_registry.start_run(analysis_run.run_id)
            except Exception as exc:
                logger.warning("Failed to initialize attribution run: %s", exc)
                analysis_run = None

        try:
            # 1. Transaction cost attribution
            total_costs = TransactionCostAttribution(
                total_costs=result.total_transaction_costs,
                commissions_fees=result.total_fees,
                slippage_costs=result.total_slippage,
                spread_costs=0.0,
                pct_of_gross_pnl=(
                    (result.total_transaction_costs / abs(result.gross_pnl)) * 100.0
                    if abs(result.gross_pnl) > 0
                    else None
                ),
                pct_of_initial_equity=(
                    (result.total_transaction_costs / result.initial_equity) * 100.0
                    if result.initial_equity > 0
                    else 0.0
                ),
            )

            # 2. Portfolio-level risk-adjusted metrics
            all_trades = [t for c in result.strategy_contributions for t in c.trades]
            risk_metrics = compute_risk_adjusted_metrics(
                equity_points=result.equity_curve,
                trades=all_trades,
                config=cfg,
                total_bars=result.bars_processed,
                initial_equity=result.initial_equity,
                final_equity=result.final_equity,
            )

            # 3. Realized vs Unrealized P&L
            # In KAISER PortfolioSimulationEngine, when liquidate_remaining=True (default),
            # all positions are closed on final bar, so all PnL is realized.
            # If positions remained open, unrealized PnL is tracked.
            realized_pnl = sum(t.net_pnl for t in all_trades)
            unrealized_pnl = result.net_pnl - realized_pnl
            if abs(unrealized_pnl) < 1e-4:
                unrealized_pnl = 0.0
                realized_pnl = result.net_pnl

            # 4. Strategy-level attribution
            strat_attributions: List[StrategyAttribution] = []
            for c in result.strategy_contributions:
                c_realized = sum(t.net_pnl for t in c.trades)
                c_unrealized = c.net_pnl - c_realized
                if abs(c_unrealized) < 1e-4:
                    c_unrealized = 0.0
                    c_realized = c.net_pnl

                c_costs = c.total_fees + c.total_slippage
                c_gross_ret = (c.gross_pnl / c.allocated_capital) * 100.0 if c.allocated_capital > 0 else 0.0

                strat_attributions.append(
                    StrategyAttribution(
                        strategy_id=c.strategy_id,
                        strategy_name=c.strategy_name,
                        symbol=c.symbol,
                        weight=c.weight,
                        allocated_capital=c.allocated_capital,
                        gross_pnl=c.gross_pnl,
                        net_pnl=c.net_pnl,
                        realized_pnl=c_realized,
                        unrealized_pnl=c_unrealized,
                        total_costs=c_costs,
                        gross_return_pct=c_gross_ret,
                        net_return_pct=c.net_return_pct,
                        portfolio_pnl_share_pct=c.portfolio_pnl_share_pct,
                        trades_count=c.total_trades,
                        win_rate=c.win_rate,
                        profit_factor=c.profit_factor,
                    )
                )

            # 5. Instrument-level attribution
            inst_attributions: List[InstrumentAttribution] = []
            if result.strategy_contributions:
                by_symbol: Dict[str, List[StrategyContribution]] = {}
                for c in result.strategy_contributions:
                    by_symbol.setdefault(c.symbol, []).append(c)

                for sym, clist in sorted(by_symbol.items()):
                    s_gross = sum(sc.gross_pnl for sc in clist)
                    s_net = sum(sc.net_pnl for sc in clist)
                    s_costs = sum(sc.total_fees + sc.total_slippage for sc in clist)
                    s_trades = sum(sc.total_trades for sc in clist)
                    s_trades_objs = [t for sc in clist for t in sc.trades]
                    s_realized = sum(t.net_pnl for t in s_trades_objs)
                    s_unrealized = s_net - s_realized
                    if abs(s_unrealized) < 1e-4:
                        s_unrealized = 0.0
                        s_realized = s_net

                    pnl_share = (s_net / result.net_pnl) * 100.0 if abs(result.net_pnl) > 0 else 0.0

                    inst_attributions.append(
                        InstrumentAttribution(
                            symbol=sym,
                            gross_pnl=s_gross,
                            net_pnl=s_net,
                            realized_pnl=s_realized,
                            unrealized_pnl=s_unrealized,
                            total_costs=s_costs,
                            trades_count=s_trades,
                            portfolio_pnl_share_pct=pnl_share,
                            is_available=True,
                            unavailable_reason=None,
                        )
                    )
            else:
                inst_attributions.append(
                    InstrumentAttribution(
                        symbol="N/A",
                        gross_pnl=0.0,
                        net_pnl=0.0,
                        realized_pnl=0.0,
                        unrealized_pnl=0.0,
                        total_costs=0.0,
                        trades_count=0,
                        portfolio_pnl_share_pct=0.0,
                        is_available=False,
                        unavailable_reason="Source simulation result does not contain strategy or instrument contribution data.",
                    )
                )

            # 6. Accounting reconciliation
            reconciliation = self._reconcile_portfolio_accounting(
                portfolio_gross_pnl=result.gross_pnl,
                portfolio_net_pnl=result.net_pnl,
                portfolio_costs=result.total_transaction_costs,
                strategy_contributions=result.strategy_contributions,
                tolerance=cfg.reconciliation_tolerance,
            )

            # Final cash balance from last equity point
            cash_balance = (
                result.equity_curve[-1].cash
                if result.equity_curve
                else result.final_equity
            )

            report = PortfolioAttributionReport(
                report_id=report_id,
                source_type="portfolio_simulation",
                source_id=result.simulation_id,
                created_at=now,
                initial_equity=result.initial_equity,
                final_equity=result.final_equity,
                gross_pnl=result.gross_pnl,
                net_pnl=result.net_pnl,
                realized_pnl=realized_pnl,
                unrealized_pnl=unrealized_pnl,
                gross_return_pct=(
                    (result.gross_pnl / result.initial_equity) * 100.0
                    if result.initial_equity > 0
                    else 0.0
                ),
                net_return_pct=result.net_return_pct,
                cash_balance=cash_balance,
                total_costs=total_costs,
                risk_adjusted_metrics=risk_metrics,
                strategy_attributions=tuple(strat_attributions),
                instrument_attributions=tuple(inst_attributions),
                reconciliation=reconciliation,
                run_id=analysis_run.run_id if analysis_run else None,
            )

            if getattr(self, "run_registry", None) is not None and analysis_run:
                try:
                    self.run_registry.complete_run(
                        run_id=analysis_run.run_id,
                        result_id=report.report_id,
                        result_type="PortfolioAttributionReport",
                    )
                except Exception as exc:
                    logger.warning("Failed to complete attribution run in registry: %s", exc)

            self._latest_report = report
            self._persist_report(report)
            self._publish_completed_event(report, cid)

            logger.info("Portfolio attribution generated successfully: %s", report.summary_text)
            return report

        except Exception as e:
            if getattr(self, "run_registry", None) is not None and analysis_run:
                try:
                    self.run_registry.fail_run(
                        run_id=analysis_run.run_id,
                        error_category="ATTRIBUTION_ERROR",
                        error_message=str(e),
                    )
                except Exception:
                    pass
            logger.error("Portfolio attribution failed: %s", str(e), exc_info=True)
            self._publish_failed_event(result.simulation_id, "portfolio_simulation", str(e), cid)
            raise

    def analyze_backtest_result(
        self,
        result: BacktestResult,
        config: Optional[AttributionConfig] = None,
        correlation_id: Optional[str] = None,
    ) -> PortfolioAttributionReport:
        """Compute performance attribution and risk-adjusted metrics for a standalone backtest result.

        Args:
            result: Completed BacktestResult.
            config: Optional AttributionConfig.
            correlation_id: Optional tracing correlation ID.

        Returns:
            PortfolioAttributionReport.
        """
        if not isinstance(result, BacktestResult):
            raise ValidationError(f"Expected BacktestResult, got {type(result).__name__}")

        cfg = config or AttributionConfig()
        cid = correlation_id or uuid.uuid4().hex
        now = self._clock_fn()

        report_id = f"attr_{uuid.uuid4().hex[:10]}"

        # Setup Analysis Run Registry
        analysis_run = None
        if getattr(self, "run_registry", None) is not None:
            try:
                from app.analysis.runs.models import OperationType
                parents = [result.run_id] if getattr(result, "run_id", None) else []
                prov_ids = [result.provenance_id] if getattr(result, "provenance_id", None) else []
                sym = [result.config.symbol] if getattr(result, "config", None) and hasattr(result.config, "symbol") else []
                analysis_run = self.run_registry.create_run(
                    operation_type=OperationType.PORTFOLIO_ATTRIBUTION,
                    configuration=cfg,
                    input_provenance_ids=prov_ids,
                    parent_run_ids=parents,
                    symbol_universe=tuple(sym),
                    data_start=result.start_timestamp,
                    data_end=result.end_timestamp,
                    execution_metadata={"source_id": result.backtest_id, "source_type": "backtest"},
                )
                self.run_registry.start_run(analysis_run.run_id)
            except Exception as exc:
                logger.warning("Failed to initialize attribution run for backtest: %s", exc)
                analysis_run = None

        m = result.metrics

        total_costs = TransactionCostAttribution(
            total_costs=m.total_transaction_costs,
            commissions_fees=m.total_fees,
            slippage_costs=m.total_slippage,
            spread_costs=0.0,
            pct_of_gross_pnl=(
                (m.total_transaction_costs / abs(m.gross_profit - m.gross_loss)) * 100.0
                if abs(m.gross_profit - m.gross_loss) > 0
                else None
            ),
            pct_of_initial_equity=(
                (m.total_transaction_costs / m.initial_equity) * 100.0
                if m.initial_equity > 0
                else 0.0
            ),
        )

        risk_metrics = compute_risk_adjusted_metrics(
            equity_points=result.equity_curve,
            trades=result.trades,
            config=cfg,
            total_bars=result.bars_processed,
            initial_equity=m.initial_equity,
            final_equity=m.final_equity,
        )

        realized_pnl = sum(t.net_pnl for t in result.trades)
        unrealized_pnl = m.net_profit - realized_pnl
        if abs(unrealized_pnl) < 1e-4:
            unrealized_pnl = 0.0
            realized_pnl = m.net_profit

        gross_pnl = m.gross_profit - m.gross_loss

        # Standalone strategy attribution
        strat_attr = StrategyAttribution(
            strategy_id=result.strategy_name,
            strategy_name=result.strategy_name,
            symbol=result.config.symbol,
            weight=1.0,
            allocated_capital=m.initial_equity,
            gross_pnl=gross_pnl,
            net_pnl=m.net_profit,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            total_costs=m.total_transaction_costs,
            gross_return_pct=(gross_pnl / m.initial_equity) * 100.0 if m.initial_equity > 0 else 0.0,
            net_return_pct=m.total_return_pct,
            portfolio_pnl_share_pct=100.0,
            trades_count=m.total_trades,
            win_rate=m.win_rate,
            profit_factor=m.profit_factor,
            risk_adjusted_metrics=risk_metrics,
        )

        # Standalone instrument attribution
        inst_attr = InstrumentAttribution(
            symbol=result.config.symbol,
            gross_pnl=gross_pnl,
            net_pnl=m.net_profit,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            total_costs=m.total_transaction_costs,
            trades_count=m.total_trades,
            portfolio_pnl_share_pct=100.0,
            is_available=True,
            unavailable_reason=None,
        )

        # Reconciliation
        gross_to_net_diff = abs(gross_pnl - m.total_transaction_costs - m.net_profit)
        status = (
            ReconciliationStatus.EXACT
            if gross_to_net_diff == 0.0
            else (
                ReconciliationStatus.RECONCILED_WITHIN_TOLERANCE
                if gross_to_net_diff <= cfg.reconciliation_tolerance
                else ReconciliationStatus.DISCREPANCY_DETECTED
            )
        )

        reconciliation = ReconciliationReport(
            status=status,
            portfolio_net_pnl=m.net_profit,
            sum_strategy_net_pnl=m.net_profit,
            strategy_discrepancy=0.0,
            portfolio_gross_pnl=gross_pnl,
            portfolio_total_costs=m.total_transaction_costs,
            gross_minus_costs=gross_pnl - m.total_transaction_costs,
            gross_to_net_discrepancy=gross_to_net_diff,
            is_reconciled=(status in (ReconciliationStatus.EXACT, ReconciliationStatus.RECONCILED_WITHIN_TOLERANCE)),
            tolerance=cfg.reconciliation_tolerance,
            details={"type": "standalone_backtest_reconciliation"},
        )

        cash_balance = (
            result.equity_curve[-1].cash
            if result.equity_curve
            else m.final_equity
        )

        report = PortfolioAttributionReport(
            report_id=report_id,
            source_type="backtest",
            source_id=result.backtest_id,
            created_at=now,
            initial_equity=m.initial_equity,
            final_equity=m.final_equity,
            gross_pnl=gross_pnl,
            net_pnl=m.net_profit,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            gross_return_pct=(gross_pnl / m.initial_equity) * 100.0 if m.initial_equity > 0 else 0.0,
            net_return_pct=m.total_return_pct,
            cash_balance=cash_balance,
            total_costs=total_costs,
            risk_adjusted_metrics=risk_metrics,
            strategy_attributions=(strat_attr,),
            instrument_attributions=(inst_attr,),
            reconciliation=reconciliation,
            run_id=analysis_run.run_id if analysis_run else None,
        )

        if getattr(self, "run_registry", None) is not None and analysis_run:
            try:
                self.run_registry.complete_run(
                    run_id=analysis_run.run_id,
                    result_id=report.report_id,
                    result_type="PortfolioAttributionReport",
                )
            except Exception as exc:
                logger.warning("Failed to complete attribution run in registry: %s", exc)

        self._latest_report = report
        self._persist_report(report)
        self._publish_completed_event(report, cid)
        return report

    def compare_simulation_runs(
        self,
        results: Sequence[PortfolioSimulationResult],
        comparison_id: Optional[str] = None,
    ) -> ComparisonReport:
        """Compare multiple portfolio simulation runs side-by-side.

        Args:
            results: Sequence of PortfolioSimulationResult instances.
            comparison_id: Optional identifier.

        Returns:
            ComparisonReport.
        """
        if not results:
            raise ValidationError("Cannot compare an empty list of simulation results.")

        cid = comparison_id or f"comp_sim_{uuid.uuid4().hex[:10]}"
        now = self._clock_fn()

        entities: List[ComparisonEntity] = []
        for r in results:
            entities.append(
                ComparisonEntity(
                    entity_id=r.simulation_id,
                    entity_name=f"Simulation {r.simulation_id}",
                    entity_type="portfolio_simulation",
                    metrics={
                        "initial_equity": r.initial_equity,
                        "final_equity": r.final_equity,
                        "net_pnl": r.net_pnl,
                        "net_return_pct": r.net_return_pct,
                        "max_drawdown_pct": r.max_drawdown_pct,
                        "total_trades": r.total_trades,
                        "win_rate": r.win_rate,
                        "profit_factor": r.profit_factor,
                        "total_costs": r.total_transaction_costs,
                        "strategy_count": len(r.strategy_contributions),
                    },
                )
            )

        best_by_return = max(results, key=lambda x: x.net_return_pct)
        lowest_dd = min(results, key=lambda x: x.max_drawdown_pct)

        summary = {
            "headline": (
                f"Best return: {best_by_return.simulation_id} ({best_by_return.net_return_pct:+.2f}%), "
                f"Lowest drawdown: {lowest_dd.simulation_id} ({lowest_dd.max_drawdown_pct:.2f}%)."
            ),
            "best_return_simulation_id": best_by_return.simulation_id,
            "best_net_return_pct": best_by_return.net_return_pct,
            "lowest_drawdown_simulation_id": lowest_dd.simulation_id,
            "lowest_max_drawdown_pct": lowest_dd.max_drawdown_pct,
            "total_runs_compared": len(results),
        }

        report = ComparisonReport(
            comparison_id=cid,
            comparison_type=ComparisonType.SIMULATION_RUNS,
            created_at=now,
            entities=tuple(entities),
            summary=summary,
        )

        self._latest_comparison = report
        self._persist_comparison(report)
        return report

    def compare_walk_forward_windows(
        self,
        wf_result: WalkForwardResult,
        comparison_id: Optional[str] = None,
    ) -> ComparisonReport:
        """Compare In-Sample (training) vs Out-of-Sample (validation) metrics across walk-forward windows.

        Args:
            wf_result: WalkForwardResult to analyze.
            comparison_id: Optional identifier.

        Returns:
            ComparisonReport.
        """
        if not isinstance(wf_result, WalkForwardResult):
            raise ValidationError(f"Expected WalkForwardResult, got {type(wf_result).__name__}")

        cid = comparison_id or f"comp_wf_{uuid.uuid4().hex[:10]}"
        now = self._clock_fn()

        entities: List[ComparisonEntity] = []
        is_returns: List[float] = []
        oos_returns: List[float] = []

        for win in wf_result.windows:
            if not win.in_sample_metrics or not win.out_of_sample_metrics:
                continue
            is_ret = win.in_sample_metrics.total_return_pct
            oos_ret = win.out_of_sample_metrics.total_return_pct
            is_returns.append(is_ret)
            oos_returns.append(oos_ret)

            # Efficiency ratio (OOS / IS)
            eff_ratio: Optional[float] = None
            if is_ret > 0:
                eff_ratio = oos_ret / is_ret

            w_idx = (
                getattr(win.window, "window_id", getattr(win, "window_index", 0))
                if hasattr(win, "window")
                else getattr(win, "window_index", 0)
            )
            w_mode = (
                win.window.window_mode.value
                if hasattr(win, "window") and hasattr(win.window, "window_mode")
                else "window"
            )
            is_profitable = win.out_of_sample_metrics.net_profit > 0

            entities.append(
                ComparisonEntity(
                    entity_id=f"window_{w_idx}",
                    entity_name=f"Window {w_idx} ({w_mode})",
                    entity_type="walk_forward_window",
                    metrics={
                        "window_index": w_idx,
                        "is_return_pct": is_ret,
                        "oos_return_pct": oos_ret,
                        "oos_to_is_efficiency_ratio": eff_ratio,
                        "is_win_rate": win.in_sample_metrics.win_rate,
                        "oos_win_rate": win.out_of_sample_metrics.win_rate,
                        "is_max_dd_pct": win.in_sample_metrics.max_drawdown_pct,
                        "oos_max_dd_pct": win.out_of_sample_metrics.max_drawdown_pct,
                        "oos_trades": win.out_of_sample_metrics.total_trades,
                        "is_profitable_oos": is_profitable,
                    },
                )
            )

        mean_is = sum(is_returns) / len(is_returns) if is_returns else 0.0
        mean_oos = sum(oos_returns) / len(oos_returns) if oos_returns else 0.0
        agg_eff = (mean_oos / mean_is) if mean_is > 0 else None
        eff_str = f"{agg_eff:.2f}" if agg_eff is not None else "N/A"

        summary = {
            "headline": (
                f"Walk-Forward {wf_result.validation_id}: Mean IS Return={mean_is:+.2f}%, "
                f"Mean OOS Return={mean_oos:+.2f}% (Walk-Forward Efficiency={eff_str})."
            ),
            "validation_id": wf_result.validation_id,
            "strategy_name": wf_result.strategy_name,
            "total_windows": len(wf_result.windows),
            "mean_in_sample_return_pct": mean_is,
            "mean_out_of_sample_return_pct": mean_oos,
            "aggregate_efficiency_ratio": agg_eff,
            "oos_win_rate_pct": wf_result.summary.window_win_rate_pct,
        }

        report = ComparisonReport(
            comparison_id=cid,
            comparison_type=ComparisonType.WALK_FORWARD,
            created_at=now,
            entities=tuple(entities),
            summary=summary,
        )

        self._latest_comparison = report
        self._persist_comparison(report)
        return report

    def compare_robustness_runs(
        self,
        robustness_result: RobustnessTestResult,
        comparison_id: Optional[str] = None,
    ) -> ComparisonReport:
        """Compare baseline strategy performance against adverse stress scenarios.

        Args:
            robustness_result: RobustnessTestResult to analyze.
            comparison_id: Optional identifier.

        Returns:
            ComparisonReport.
        """
        if not isinstance(robustness_result, RobustnessTestResult):
            raise ValidationError(f"Expected RobustnessTestResult, got {type(robustness_result).__name__}")

        cid = comparison_id or f"comp_rob_{uuid.uuid4().hex[:10]}"
        now = self._clock_fn()

        base = robustness_result.baseline_result
        entities: List[ComparisonEntity] = []

        # 1. Baseline entity
        if base:
            entities.append(
                ComparisonEntity(
                    entity_id="baseline",
                    entity_name="Baseline Simulation",
                    entity_type="robustness_baseline",
                    metrics={
                        "scenario_type": "baseline",
                        "net_profit": base.metrics.net_profit,
                        "total_return_pct": base.metrics.total_return_pct,
                        "max_drawdown_pct": base.metrics.max_drawdown_pct,
                        "win_rate": base.metrics.win_rate,
                        "profit_factor": base.metrics.profit_factor,
                        "total_costs": base.metrics.total_transaction_costs,
                        "is_profitable": base.metrics.net_profit > 0,
                        "profit_degradation_pct": 0.0,
                    },
                )
            )

        # 2. Stress scenarios
        for scen in robustness_result.scenario_results:
            m = scen.metrics
            entities.append(
                ComparisonEntity(
                    entity_id=scen.scenario.name,
                    entity_name=f"Stress: {scen.scenario.name}",
                    entity_type="robustness_scenario",
                    metrics={
                        "scenario_type": scen.scenario.scenario_type.value,
                        "net_profit": m.net_profit if m else scen.delta_net_pnl,
                        "total_return_pct": m.total_return_pct if m else scen.delta_return_pct,
                        "max_drawdown_pct": m.max_drawdown_pct if m else scen.delta_max_drawdown_pct,
                        "win_rate": m.win_rate if m else 0.0,
                        "profit_factor": m.profit_factor if m else None,
                        "total_costs": m.total_transaction_costs if m else scen.delta_transaction_costs,
                        "is_profitable": (m.net_profit > 0) if m else False,
                        "return_degradation_pct": scen.return_degradation_pct,
                        "drawdown_increase_pct": scen.drawdown_increase_pct,
                    },
                )
            )

        base_ret = base.metrics.total_return_pct if base else 0.0
        summary = {
            "headline": (
                f"Robustness {robustness_result.test_id}: Baseline Return={base_ret:+.2f}%, "
                f"Pass Rate={robustness_result.summary.scenario_pass_rate_pct:.1f}% across {len(robustness_result.scenario_results)} scenarios."
            ),
            "test_id": robustness_result.test_id,
            "strategy_name": robustness_result.strategy_name,
            "baseline_return_pct": base_ret,
            "pass_rate_pct": robustness_result.summary.scenario_pass_rate_pct,
            "worst_scenario_drawdown_pct": robustness_result.summary.worst_case_max_drawdown_pct,
            "scenarios_tested_count": len(robustness_result.scenario_results),
        }

        report = ComparisonReport(
            comparison_id=cid,
            comparison_type=ComparisonType.ROBUSTNESS,
            created_at=now,
            entities=tuple(entities),
            summary=summary,
        )

        self._latest_comparison = report
        self._persist_comparison(report)
        return report

    def get_latest_report(self) -> Optional[PortfolioAttributionReport]:
        """Retrieve the most recently generated attribution report."""
        if self._latest_report:
            return self._latest_report
        if self.state_store:
            data = self.state_store.get("portfolio_attribution:latest")
            if data and isinstance(data, dict):
                return self._deserialize_report(data)
        return None

    def get_report(self, report_id: str) -> Optional[PortfolioAttributionReport]:
        """Retrieve a stored attribution report by report_id."""
        if self._latest_report and self._latest_report.report_id == report_id:
            return self._latest_report
        if self.state_store:
            data = self.state_store.get(f"portfolio_attribution:{report_id}")
            if data and isinstance(data, dict):
                return self._deserialize_report(data)
        return None

    def get_latest_comparison(self) -> Optional[ComparisonReport]:
        """Retrieve the most recently generated comparison report."""
        return self._latest_comparison

    # --------------------------------------------------------------------------
    # Internal Helpers
    # --------------------------------------------------------------------------

    def _reconcile_portfolio_accounting(
        self,
        portfolio_gross_pnl: float,
        portfolio_net_pnl: float,
        portfolio_costs: float,
        strategy_contributions: Sequence[StrategyContribution],
        tolerance: float = 0.05,
    ) -> ReconciliationReport:
        """Reconcile strategy contributions and transaction costs against portfolio totals."""
        if not strategy_contributions:
            return ReconciliationReport(
                status=ReconciliationStatus.INSUFFICIENT_DATA,
                portfolio_net_pnl=portfolio_net_pnl,
                sum_strategy_net_pnl=0.0,
                strategy_discrepancy=abs(portfolio_net_pnl),
                portfolio_gross_pnl=portfolio_gross_pnl,
                portfolio_total_costs=portfolio_costs,
                gross_minus_costs=portfolio_gross_pnl - portfolio_costs,
                gross_to_net_discrepancy=abs(portfolio_gross_pnl - portfolio_costs - portfolio_net_pnl),
                is_reconciled=False,
                tolerance=tolerance,
                details={"reason": "No strategy contributions recorded in simulation result."},
            )

        sum_strat_net = sum(c.net_pnl for c in strategy_contributions)
        sum_strat_gross = sum(c.gross_pnl for c in strategy_contributions)
        sum_strat_costs = sum((c.total_fees + c.total_slippage) for c in strategy_contributions)

        strat_discrepancy = abs(portfolio_net_pnl - sum_strat_net)
        gross_to_net_calc = portfolio_gross_pnl - portfolio_costs
        cost_discrepancy = abs(gross_to_net_calc - portfolio_net_pnl)

        is_reconciled = (strat_discrepancy <= tolerance) and (cost_discrepancy <= tolerance)

        if strat_discrepancy == 0.0 and cost_discrepancy == 0.0:
            status = ReconciliationStatus.EXACT
        elif is_reconciled:
            status = ReconciliationStatus.RECONCILED_WITHIN_TOLERANCE
        else:
            status = ReconciliationStatus.DISCREPANCY_DETECTED

        return ReconciliationReport(
            status=status,
            portfolio_net_pnl=portfolio_net_pnl,
            sum_strategy_net_pnl=sum_strat_net,
            strategy_discrepancy=strat_discrepancy,
            portfolio_gross_pnl=portfolio_gross_pnl,
            portfolio_total_costs=portfolio_costs,
            gross_minus_costs=gross_to_net_calc,
            gross_to_net_discrepancy=cost_discrepancy,
            is_reconciled=is_reconciled,
            tolerance=tolerance,
            details={
                "sum_strategy_gross": round(sum_strat_gross, 2),
                "sum_strategy_costs": round(sum_strat_costs, 4),
            },
        )

    def _persist_report(self, report: PortfolioAttributionReport) -> None:
        """Persist report dictionary in StateStore."""
        if not self.state_store:
            return
        rep_dict = report.to_dict()
        self.state_store.set("portfolio_attribution:latest", rep_dict)
        self.state_store.set(f"portfolio_attribution:{report.report_id}", rep_dict)
        self.state_store.set(f"portfolio_attribution:source:{report.source_id}", rep_dict)

    def _persist_comparison(self, report: ComparisonReport) -> None:
        """Persist comparison report dictionary in StateStore."""
        if not self.state_store:
            return
        c_dict = report.to_dict()
        self.state_store.set("portfolio_attribution:comparison:latest", c_dict)
        self.state_store.set(f"portfolio_attribution:comparison:{report.comparison_id}", c_dict)

    def _publish_completed_event(self, report: PortfolioAttributionReport, cid: str) -> None:
        """Publish PORTFOLIO_ATTRIBUTION_COMPLETED event to EventBus."""
        if not self.event_bus:
            return
        try:
            ev = Event.create(
                event_type=EventType.PORTFOLIO_ATTRIBUTION_COMPLETED,
                source="portfolio_attribution_service",
                correlation_id=cid,
                payload={
                    "report_id": report.report_id,
                    "source_type": report.source_type,
                    "source_id": report.source_id,
                    "initial_equity": report.initial_equity,
                    "final_equity": report.final_equity,
                    "net_pnl": report.net_pnl,
                    "net_return_pct": report.net_return_pct,
                    "sharpe_ratio": report.risk_adjusted_metrics.sharpe_ratio.value,
                    "reconciliation_status": report.reconciliation.status.value if report.reconciliation else "N/A",
                    "summary_text": report.summary_text,
                },
            )
            self.event_bus.publish(ev)
        except Exception as e:
            logger.warning("Failed to publish PORTFOLIO_ATTRIBUTION_COMPLETED event: %s", str(e))

    def _publish_failed_event(self, source_id: str, source_type: str, error: str, cid: str) -> None:
        """Publish PORTFOLIO_ATTRIBUTION_FAILED event to EventBus."""
        if not self.event_bus:
            return
        try:
            ev = Event.create(
                event_type=EventType.PORTFOLIO_ATTRIBUTION_FAILED,
                source="portfolio_attribution_service",
                correlation_id=cid,
                payload={
                    "source_id": source_id,
                    "source_type": source_type,
                    "error": error,
                },
            )
            self.event_bus.publish(ev)
        except Exception as e:
            logger.warning("Failed to publish PORTFOLIO_ATTRIBUTION_FAILED event: %s", str(e))

    def _deserialize_report(self, data: Dict[str, Any]) -> PortfolioAttributionReport:
        """Reconstruct minimal PortfolioAttributionReport from StateStore dict."""
        from app.portfolio.models import (
            MetricValue,
            ReconciliationReport,
            ReconciliationStatus,
            RiskAdjustedMetrics,
            TransactionCostAttribution,
        )

        costs_data = data.get("total_costs", {})
        total_costs = TransactionCostAttribution(
            total_costs=float(costs_data.get("total_costs", 0.0)),
            commissions_fees=float(costs_data.get("commissions_fees", 0.0)),
            slippage_costs=float(costs_data.get("slippage_costs", 0.0)),
            spread_costs=float(costs_data.get("spread_costs", 0.0)),
            pct_of_gross_pnl=costs_data.get("pct_of_gross_pnl"),
            pct_of_initial_equity=float(costs_data.get("pct_of_initial_equity", 0.0)),
        )

        m_data = data.get("risk_adjusted_metrics", {})
        def _parse_mv(d: Any, default_unit: str = "") -> MetricValue:
            if isinstance(d, dict):
                return MetricValue(
                    value=d.get("value"),
                    unit=d.get("unit", default_unit),
                    is_available=bool(d.get("is_available", True)),
                    unavailable_reason=d.get("unavailable_reason"),
                    description=d.get("description", ""),
                )
            return MetricValue(value=None, unit=default_unit, is_available=False)

        risk_metrics = RiskAdjustedMetrics(
            annualized_return_pct=_parse_mv(m_data.get("annualized_return_pct"), "%"),
            annualized_volatility_pct=_parse_mv(m_data.get("annualized_volatility_pct"), "%"),
            sharpe_ratio=_parse_mv(m_data.get("sharpe_ratio"), "ratio"),
            sortino_ratio=_parse_mv(m_data.get("sortino_ratio"), "ratio"),
            max_drawdown_pct=_parse_mv(m_data.get("max_drawdown_pct"), "%"),
            max_drawdown_amount=_parse_mv(m_data.get("max_drawdown_amount"), "USD"),
            max_drawdown_duration_bars=_parse_mv(m_data.get("max_drawdown_duration_bars"), "bars"),
            calmar_ratio=_parse_mv(m_data.get("calmar_ratio"), "ratio"),
            profit_factor=_parse_mv(m_data.get("profit_factor"), "ratio"),
            win_rate=_parse_mv(m_data.get("win_rate"), "%"),
            win_loss_ratio=_parse_mv(m_data.get("win_loss_ratio"), "ratio"),
            average_win=_parse_mv(m_data.get("average_win"), "USD"),
            average_loss=_parse_mv(m_data.get("average_loss"), "USD"),
            total_trades=_parse_mv(m_data.get("total_trades"), "count"),
            market_exposure_pct=_parse_mv(m_data.get("market_exposure_pct"), "%"),
            annualization_factor=float(m_data.get("annualization_factor", 252.0)),
            risk_free_rate=float(m_data.get("risk_free_rate", 0.0)),
        )

        r_data = data.get("reconciliation")
        reconciliation = None
        if r_data and isinstance(r_data, dict):
            reconciliation = ReconciliationReport(
                status=ReconciliationStatus(r_data.get("status", "EXACT")),
                portfolio_net_pnl=float(r_data.get("portfolio_net_pnl", 0.0)),
                sum_strategy_net_pnl=float(r_data.get("sum_strategy_net_pnl", 0.0)),
                strategy_discrepancy=float(r_data.get("strategy_discrepancy", 0.0)),
                portfolio_gross_pnl=float(r_data.get("portfolio_gross_pnl", 0.0)),
                portfolio_total_costs=float(r_data.get("portfolio_total_costs", 0.0)),
                gross_minus_costs=float(r_data.get("gross_minus_costs", 0.0)),
                gross_to_net_discrepancy=float(r_data.get("gross_to_net_discrepancy", 0.0)),
                is_reconciled=bool(r_data.get("is_reconciled", True)),
                tolerance=float(r_data.get("tolerance", 0.05)),
                details=dict(r_data.get("details", {})),
            )

        return PortfolioAttributionReport(
            report_id=str(data.get("report_id", "")),
            source_type=str(data.get("source_type", "portfolio_simulation")),
            source_id=str(data.get("source_id", "")),
            created_at=float(data.get("created_at", 0.0)),
            initial_equity=float(data.get("initial_equity", 100_000.0)),
            final_equity=float(data.get("final_equity", 100_000.0)),
            gross_pnl=float(data.get("gross_pnl", 0.0)),
            net_pnl=float(data.get("net_pnl", 0.0)),
            realized_pnl=float(data.get("realized_pnl", 0.0)),
            unrealized_pnl=float(data.get("unrealized_pnl", 0.0)),
            gross_return_pct=float(data.get("gross_return_pct", 0.0)),
            net_return_pct=float(data.get("net_return_pct", 0.0)),
            cash_balance=float(data.get("cash_balance", 100_000.0)),
            total_costs=total_costs,
            risk_adjusted_metrics=risk_metrics,
            strategy_attributions=(),
            instrument_attributions=(),
            reconciliation=reconciliation,
        )
