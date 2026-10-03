"""KAISER — Portfolio Analytics Reporting & Read-Only Dashboard Data API Service.

Provides a unified, deterministic reporting service aggregating performance attribution,
risk monitoring, transaction costs, and portfolio exposure into dashboard-ready data contracts.

SAFETY & RESEARCH INVARIANTS:
1. Purely informational, research-only reporting.
2. Read-only service: never mutates input simulation results or risk snapshots.
3. Complete provenance and data integrity: unavailable metrics are flagged explicitly, never zeroed.
4. Strictly decoupled from execution: never places trades, modifies orders, or contacts brokers.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from app.backtesting.models import BacktestResult, BacktestStatus, EquityPoint
from app.backtesting.portfolio_simulation.models import (
    PortfolioSimulationResult,
    PortfolioSimulationStatus,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.state.store import StateStore
from app.core.exceptions import ValidationError
from app.portfolio.attribution import PortfolioAttributionService
from app.portfolio.models import (
    ComparisonEntity,
    ComparisonReport,
    ComparisonType,
    PortfolioAttributionReport,
    ReconciliationStatus,
)
from app.portfolio.report_models import (
    DashboardSeriesPoint,
    DashboardTimeSeries,
    HistoricalReportSummary,
    ReportDataProvenance,
    ReportDataStatus,
    UnifiedPortfolioReport,
    VoiceSummary,
    format_iso_timestamp,
    sanitize_dict_for_export,
    sanitize_numeric,
)
from app.risk.models import AlertState, PortfolioRiskReport
from app.risk.monitor import PortfolioRiskMonitor

logger = logging.getLogger("kaiser.portfolio.reporting")


class PortfolioReportingService:
    """Read-only portfolio analytics reporting and dashboard data API service.

    Aggregates multi-strategy simulations, backtests, walk-forward results,
    risk monitoring, and performance attribution into unified, deterministic reports.
    """

    STATE_LATEST_KEY = "portfolio_reporting:latest"
    STATE_REPORTS_PREFIX = "portfolio_reporting:reports:"
    STATE_INDEX_KEY = "portfolio_reporting:index"

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        attribution_service: Optional[PortfolioAttributionService] = None,
        risk_monitor: Optional[PortfolioRiskMonitor] = None,
        max_history_size: int = 50,
        engine_version: str = "1.0.0",
    ) -> None:
        """Initialize the reporting service.

        Args:
            event_bus: Optional system EventBus for lifecycle event publishing.
            state_store: Optional StateStore for persistence and caching.
            attribution_service: Optional attribution service for computing attribution if needed.
            risk_monitor: Optional risk monitor for evaluating snapshot risk status.
            max_history_size: Maximum number of historical reports retained in memory.
            engine_version: Version identifier of the analytics reporting engine.
        """
        if max_history_size <= 0:
            raise ValidationError(f"max_history_size must be positive, got {max_history_size}")

        self.event_bus = event_bus
        self.state_store = state_store
        self.attribution_service = attribution_service
        self.risk_monitor = risk_monitor
        self.max_history_size = max_history_size
        self.engine_version = engine_version

        # In-memory bounded cache
        self._reports: Dict[str, UnifiedPortfolioReport] = {}
        self._run_to_report_map: Dict[str, str] = {}

    def generate_unified_report(
        self,
        source_result: Union[PortfolioSimulationResult, BacktestResult, Dict[str, Any]],
        risk_report: Optional[PortfolioRiskReport] = None,
        attribution_report: Optional[PortfolioAttributionReport] = None,
        title: Optional[str] = None,
        provenance_overrides: Optional[Dict[str, Any]] = None,
        walk_forward_summary: Optional[Dict[str, Any]] = None,
        robustness_summary: Optional[Dict[str, Any]] = None,
    ) -> UnifiedPortfolioReport:
        """Synthesize a complete UnifiedPortfolioReport from available source data.

        Args:
            source_result: PortfolioSimulationResult, BacktestResult, or dictionary.
            risk_report: Optional pre-evaluated PortfolioRiskReport.
            attribution_report: Optional pre-calculated PortfolioAttributionReport.
            title: Optional custom report title.
            provenance_overrides: Optional overrides for data provenance metadata.
            walk_forward_summary: Optional walk-forward validation summary dict.
            robustness_summary: Optional robustness stress-testing summary dict.

        Returns:
            UnifiedPortfolioReport: Typed, immutable unified report.
        """
        now = time.time()
        report_id = f"report_{uuid.uuid4().hex[:12]}"

        try:
            # 1. Resolve source identifiers and type
            source_id, source_type = self._extract_source_meta(source_result)
            report_title = title or f"Portfolio Analytics Report [{source_id}]"

            # 2. Extract or compute attribution report
            attr = attribution_report
            if attr is None and self.attribution_service is not None:
                if isinstance(source_result, PortfolioSimulationResult):
                    attr = self.attribution_service.analyze_simulation_result(source_result)
                elif isinstance(source_result, BacktestResult):
                    attr = self.attribution_service.analyze_backtest_result(source_result)

            # 3. Extract or evaluate risk monitoring report
            risk = risk_report
            if risk is None and self.risk_monitor is not None:
                if isinstance(source_result, PortfolioSimulationResult) and source_result.risk_snapshots:
                    risk = self.risk_monitor.evaluate_snapshot(source_result.risk_snapshots[-1])

            # 4. Extract core financials, exposures, and curves
            (
                initial_equity,
                final_equity,
                gross_pnl,
                net_pnl,
                gross_return_pct,
                net_return_pct,
                realized_pnl,
                unrealized_pnl,
                cash_balance,
                gross_exposure,
                gross_exposure_pct,
                net_exposure,
                net_exposure_pct,
                cash_reserve_pct,
                total_costs,
                cost_breakdown,
                equity_points,
                max_dd_pct,
                max_dd_amt,
                max_dd_duration,
                raw_strat_attrs,
                raw_inst_attrs,
                inst_available,
            ) = self._extract_financials_and_curves(source_result, attr)

            # 5. Extract risk-adjusted metrics
            risk_metrics_dict: Dict[str, Any] = {}
            if attr is not None and attr.risk_adjusted_metrics is not None:
                risk_metrics_dict = attr.risk_adjusted_metrics.to_dict()

            # 6. Audit data freshness, completeness, and warnings
            data_warnings: List[str] = []
            status = ReportDataStatus.VALID

            # Check source status
            if isinstance(source_result, PortfolioSimulationResult):
                if source_result.status != PortfolioSimulationStatus.SUCCESS:
                    status = ReportDataStatus.INCOMPLETE
                    data_warnings.append(f"Source simulation status is '{source_result.status.value}'.")
                if source_result.warnings:
                    for w in source_result.warnings:
                        data_warnings.append(f"Simulation warning [{w.rule_name}]: {w.message}")
            elif isinstance(source_result, BacktestResult):
                if source_result.status != BacktestStatus.COMPLETED:
                    status = ReportDataStatus.INCOMPLETE
                    data_warnings.append(f"Source backtest status is '{source_result.status.value}'.")
                if source_result.warnings:
                    for w in source_result.warnings:
                        data_warnings.append(f"Backtest warning: {w}")

            # Check risk freshness
            if risk is not None:
                if risk.is_snapshot_stale:
                    status = ReportDataStatus.STALE
                    data_warnings.append("Underlying portfolio risk snapshot is stale.")
                if risk.overall_state in (AlertState.WARNING, AlertState.CRITICAL):
                    data_warnings.append(
                        f"Portfolio risk monitor flagged state '{risk.overall_state.value}' "
                        f"with {len(risk.active_alerts)} active alerts."
                    )

            # Check attribution reconciliation
            recon_dict: Optional[Dict[str, Any]] = None
            if attr is not None and attr.reconciliation is not None:
                recon_dict = attr.reconciliation.to_dict()
                if attr.reconciliation.status == ReconciliationStatus.DISCREPANCY_DETECTED:
                    data_warnings.append(
                        f"Accounting discrepancy detected: strategy delta=${attr.reconciliation.strategy_discrepancy:.2f}, "
                        f"gross-to-net delta=${attr.reconciliation.gross_to_net_discrepancy:.2f}"
                    )
                elif attr.reconciliation.status == ReconciliationStatus.INSUFFICIENT_DATA:
                    data_warnings.append("Insufficient data for full strategy contribution reconciliation.")

            if not inst_available:
                data_warnings.append("Instrument-level attribution is unavailable in source records.")

            if not equity_points:
                status = ReportDataStatus.INCOMPLETE
                data_warnings.append("No equity curve observations available in source records.")

            # 7. Construct Provenance
            prov_dict = provenance_overrides or {}
            provenance = ReportDataProvenance(
                source_run_id=source_id,
                source_type=source_type,
                generated_at=now,
                generated_at_iso=format_iso_timestamp(now),
                engine_version=self.engine_version,
                git_commit=prov_dict.get("git_commit"),
                timeframe=prov_dict.get("timeframe", "1d"),
                start_time=prov_dict.get("start_time"),
                end_time=prov_dict.get("end_time"),
                parameters=prov_dict.get("parameters", {}),
            )

            # 8. Synthesize Voice Summary
            voice_summary = self._synthesize_voice_summary(
                source_id=source_id,
                net_pnl=net_pnl,
                net_return_pct=net_return_pct,
                max_dd_pct=max_dd_pct,
                max_dd_amt=max_dd_amt,
                max_dd_duration=max_dd_duration,
                risk_report=risk,
                data_warnings=data_warnings,
                attr=attr,
            )

            # 9. Build Unified Report instance
            report = UnifiedPortfolioReport(
                report_id=report_id,
                title=report_title,
                provenance=provenance,
                status=status,
                data_warnings=tuple(data_warnings),
                initial_equity=initial_equity,
                final_equity=final_equity,
                gross_pnl=gross_pnl,
                net_pnl=net_pnl,
                gross_return_pct=gross_return_pct,
                net_return_pct=net_return_pct,
                realized_pnl=realized_pnl,
                unrealized_pnl=unrealized_pnl,
                cash_balance=cash_balance,
                gross_exposure=gross_exposure,
                gross_exposure_pct=gross_exposure_pct,
                net_exposure=net_exposure,
                net_exposure_pct=net_exposure_pct,
                cash_reserve_pct=cash_reserve_pct,
                total_transaction_costs=total_costs,
                cost_breakdown=cost_breakdown,
                equity_curve=tuple(equity_points),
                max_drawdown_pct=max_dd_pct,
                max_drawdown_amount=max_dd_amt,
                max_drawdown_duration_bars=max_dd_duration,
                risk_adjusted_metrics=risk_metrics_dict,
                strategy_attributions=tuple(raw_strat_attrs),
                instrument_attributions=tuple(raw_inst_attrs),
                instrument_attribution_available=inst_available,
                reconciliation=recon_dict,
                risk_status=risk.to_dict() if risk else None,
                active_alerts_count=len(risk.active_alerts) if risk else 0,
                walk_forward_summary=walk_forward_summary,
                robustness_summary=robustness_summary,
                voice_summary=voice_summary,
            )

            # 10. Cache and persist
            self._persist_report(report)

            # 11. Emit EventBus Event
            if self.event_bus is not None:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.PORTFOLIO_REPORT_GENERATED,
                        source="portfolio_reporting_service",
                        payload={
                            "report_id": report.report_id,
                            "source_run_id": source_id,
                            "source_type": source_type,
                            "status": report.status.value,
                            "initial_equity": report.initial_equity,
                            "final_equity": report.final_equity,
                            "net_pnl": report.net_pnl,
                            "net_return_pct": report.net_return_pct,
                            "max_drawdown_pct": report.max_drawdown_pct,
                            "active_alerts_count": report.active_alerts_count,
                            "summary_text": report.summary_text,
                        },
                    )
                )

            logger.info("Generated unified portfolio report: %s", report.summary_text)
            return report

        except Exception as exc:
            logger.error("Failed to generate unified portfolio report: %s", exc, exc_info=True)
            if self.event_bus is not None:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.PORTFOLIO_REPORT_FAILED,
                        source="portfolio_reporting_service",
                        payload={
                            "report_id": report_id,
                            "error": str(exc),
                        },
                    )
                )
            raise

    def get_latest_unified_report(self) -> Optional[UnifiedPortfolioReport]:
        """Retrieve the most recently generated unified report."""
        if self._reports:
            # Return newest by insertion
            latest_id = list(self._reports.keys())[-1]
            return self._reports[latest_id]

        # Check StateStore
        if self.state_store is not None:
            latest_dict = self.state_store.get(self.STATE_LATEST_KEY)
            if latest_dict and isinstance(latest_dict, dict):
                return self._deserialize_report(latest_dict)

        return None

    def get_report_by_id(self, report_id: str) -> Optional[UnifiedPortfolioReport]:
        """Retrieve a unified report by its unique report identifier.

        Args:
            report_id: Unique report identifier.

        Returns:
            Optional[UnifiedPortfolioReport]: Report if found, else None.
        """
        if not report_id or not isinstance(report_id, str) or not report_id.strip():
            raise ValidationError("report_id must be a non-empty string.")

        clean_id = report_id.strip()
        if clean_id in self._reports:
            return self._reports[clean_id]

        if self.state_store is not None:
            raw = self.state_store.get(f"{self.STATE_REPORTS_PREFIX}{clean_id}")
            if raw and isinstance(raw, dict):
                report = self._deserialize_report(raw)
                self._reports[clean_id] = report
                return report

        return None

    def get_report_by_run_id(self, run_id: str) -> Optional[UnifiedPortfolioReport]:
        """Retrieve the report generated for a specific source simulation or backtest run ID.

        Args:
            run_id: Source simulation ID or backtest run ID.

        Returns:
            Optional[UnifiedPortfolioReport]: Report if found, else None.
        """
        if not run_id or not isinstance(run_id, str) or not run_id.strip():
            raise ValidationError("run_id must be a non-empty string.")

        clean_run_id = run_id.strip()
        if clean_run_id in self._run_to_report_map:
            report_id = self._run_to_report_map[clean_run_id]
            return self.get_report_by_id(report_id)

        # Scan cached reports
        for rep in reversed(list(self._reports.values())):
            if rep.provenance.source_run_id == clean_run_id:
                return rep

        return None

    def list_historical_reports(
        self,
        limit: int = 20,
        offset: int = 0,
        sort_desc: bool = True,
    ) -> Tuple[HistoricalReportSummary, ...]:
        """Retrieve a bounded, paginated list of historical report summaries.

        Args:
            limit: Maximum items to return (1 <= limit <= 100).
            offset: Number of items to skip (offset >= 0).
            sort_desc: If True, newest reports appear first.

        Returns:
            Tuple[HistoricalReportSummary, ...]: Bounded summary items.
        """
        if not isinstance(limit, int) or limit <= 0 or limit > 100:
            raise ValidationError(f"limit must be an integer between 1 and 100, got {limit}")
        if not isinstance(offset, int) or offset < 0:
            raise ValidationError(f"offset must be a non-negative integer, got {offset}")

        all_reports = list(self._reports.values())
        if sort_desc:
            all_reports.sort(key=lambda r: r.provenance.generated_at, reverse=True)
        else:
            all_reports.sort(key=lambda r: r.provenance.generated_at, reverse=False)

        sliced = all_reports[offset : offset + limit]

        summaries: List[HistoricalReportSummary] = []
        for rep in sliced:
            sharpe_val: Optional[float] = None
            if rep.risk_adjusted_metrics and "sharpe_ratio" in rep.risk_adjusted_metrics:
                sharpe_val = rep.risk_adjusted_metrics["sharpe_ratio"].get("value")

            risk_state_str = rep.risk_status.get("overall_state", "UNKNOWN") if rep.risk_status else "UNKNOWN"
            is_recon = rep.reconciliation.get("is_reconciled", True) if rep.reconciliation else True

            summaries.append(
                HistoricalReportSummary(
                    report_id=rep.report_id,
                    source_id=rep.provenance.source_run_id,
                    source_type=rep.provenance.source_type,
                    created_at=rep.provenance.generated_at,
                    created_at_iso=rep.provenance.generated_at_iso,
                    initial_equity=rep.initial_equity,
                    final_equity=rep.final_equity,
                    net_pnl=rep.net_pnl,
                    net_return_pct=rep.net_return_pct,
                    max_drawdown_pct=rep.max_drawdown_pct,
                    sharpe_ratio=sharpe_val,
                    risk_state=risk_state_str,
                    is_reconciled=is_recon,
                    status=rep.status,
                    title=rep.title,
                )
            )

        return tuple(summaries)

    def get_equity_and_drawdown_series(
        self,
        report_id: str,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Optional[DashboardTimeSeries]:
        """Retrieve bounded, ordered equity and drawdown timeline series for charting.

        Args:
            report_id: Report identifier.
            limit: Optional maximum number of points to return (1 <= limit <= 5000).
            offset: Number of initial points to skip.

        Returns:
            Optional[DashboardTimeSeries]: Formatted chart series container, or None if report not found.
        """
        report = self.get_report_by_id(report_id)
        if report is None:
            return None

        if limit is not None:
            if not isinstance(limit, int) or limit <= 0 or limit > 5000:
                raise ValidationError(f"limit must be an integer between 1 and 5000, got {limit}")
        if not isinstance(offset, int) or offset < 0:
            raise ValidationError(f"offset must be a non-negative integer, got {offset}")

        curve = report.equity_curve
        total_count = len(curve)

        if total_count == 0:
            return DashboardTimeSeries(
                series_id=f"series_{report.report_id}",
                points=(),
                total_points=0,
                timeframe=report.provenance.timeframe or "1d",
                is_empty=True,
                peak_equity=0.0,
                trough_equity=0.0,
                max_drawdown_pct=0.0,
            )

        sliced_points = curve[offset : (offset + limit) if limit is not None else total_count]
        peak = max(p.total_equity for p in curve)
        trough = min(p.total_equity for p in curve)

        return DashboardTimeSeries(
            series_id=f"series_{report.report_id}",
            points=tuple(sliced_points),
            total_points=total_count,
            timeframe=report.provenance.timeframe or "1d",
            is_empty=len(sliced_points) == 0,
            peak_equity=peak,
            trough_equity=trough,
            max_drawdown_pct=report.max_drawdown_pct,
        )

    def get_strategy_attribution(
        self,
        report_id: str,
    ) -> Optional[Tuple[Dict[str, Any], ...]]:
        """Retrieve strategy performance attribution breakdown for a report."""
        report = self.get_report_by_id(report_id)
        if report is None:
            return None
        return report.strategy_attributions

    def get_risk_status_and_alerts(
        self,
        report_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Retrieve portfolio risk status and active alerts for a report."""
        report = self.get_report_by_id(report_id)
        if report is None:
            return None
        return report.risk_status

    def compare_simulation_runs(
        self,
        run_ids: Sequence[str],
    ) -> ComparisonReport:
        """Compare multiple historical simulation runs side-by-side.

        Args:
            run_ids: Sequence of run or report IDs to compare (min 2, max 20).

        Returns:
            ComparisonReport: Comparative performance report.
        """
        if not run_ids or len(run_ids) < 2:
            raise ValidationError("At least two run identifiers are required for comparison.")
        if len(run_ids) > 20:
            raise ValidationError(f"Maximum 20 runs can be compared at once, got {len(run_ids)}")

        entities: List[ComparisonEntity] = []
        for rid in run_ids:
            if not isinstance(rid, str) or not rid.strip():
                raise ValidationError(f"Invalid run identifier: {rid}")
            clean_id = rid.strip()

            # Lookup report by report_id or run_id
            rep = self.get_report_by_id(clean_id) or self.get_report_by_run_id(clean_id)
            if rep is None:
                raise ValidationError(f"Report for run '{clean_id}' not found.")

            sharpe_val = None
            sortino_val = None
            vol_val = None
            if rep.risk_adjusted_metrics:
                sharpe_val = rep.risk_adjusted_metrics.get("sharpe_ratio", {}).get("value")
                sortino_val = rep.risk_adjusted_metrics.get("sortino_ratio", {}).get("value")
                vol_val = rep.risk_adjusted_metrics.get("annualized_volatility_pct", {}).get("value")

            metrics = {
                "initial_equity": rep.initial_equity,
                "final_equity": rep.final_equity,
                "net_pnl": rep.net_pnl,
                "net_return_pct": rep.net_return_pct,
                "gross_pnl": rep.gross_pnl,
                "total_transaction_costs": rep.total_transaction_costs,
                "max_drawdown_pct": rep.max_drawdown_pct,
                "sharpe_ratio": sharpe_val,
                "sortino_ratio": sortino_val,
                "annualized_volatility_pct": vol_val,
                "risk_state": rep.risk_status.get("overall_state", "UNKNOWN") if rep.risk_status else "UNKNOWN",
                "active_alerts_count": rep.active_alerts_count,
            }

            entities.append(
                ComparisonEntity(
                    entity_id=rep.report_id,
                    entity_name=rep.title,
                    entity_type=rep.provenance.source_type,
                    metrics=metrics,
                )
            )

        # Sort entities deterministically by net_return_pct descending
        entities.sort(key=lambda e: e.metrics.get("net_return_pct", 0.0), reverse=True)

        best_entity = entities[0]
        headline = (
            f"Top performer: '{best_entity.entity_name}' with net return {best_entity.metrics['net_return_pct']:.2f}% "
            f"and MaxDD {best_entity.metrics['max_drawdown_pct']:.2f}%."
        )

        return ComparisonReport(
            comparison_id=f"comp_{uuid.uuid4().hex[:12]}",
            comparison_type=ComparisonType.SIMULATION_RUNS,
            created_at=time.time(),
            entities=tuple(entities),
            summary={
                "headline": headline,
                "runs_compared": len(entities),
                "best_run_id": best_entity.entity_id,
            },
        )

    def clear_history(self) -> None:
        """Clear all in-memory report history and reset StateStore index."""
        self._reports.clear()
        self._run_to_report_map.clear()
        if self.state_store is not None:
            self.state_store.delete(self.STATE_LATEST_KEY)
            self.state_store.delete(self.STATE_INDEX_KEY)
        logger.info("Portfolio reporting history cleared.")

    # -------------------------------------------------------------------------
    # Private Extraction and Transformation Helpers
    # -------------------------------------------------------------------------

    def _extract_source_meta(
        self,
        source: Union[PortfolioSimulationResult, BacktestResult, Dict[str, Any]],
    ) -> Tuple[str, str]:
        """Extract source ID and source type from input object."""
        if isinstance(source, PortfolioSimulationResult):
            return source.simulation_id, "portfolio_simulation"
        if isinstance(source, BacktestResult):
            return source.backtest_id, "backtest"
        if isinstance(source, dict):
            sid = source.get("simulation_id") or source.get("backtest_id") or source.get("run_id") or "unknown_run"
            stype = source.get("source_type", "synthetic")
            return str(sid), str(stype)
        return "unknown_run", "unknown"

    def _extract_financials_and_curves(
        self,
        source: Union[PortfolioSimulationResult, BacktestResult, Dict[str, Any]],
        attr: Optional[PortfolioAttributionReport],
    ) -> Tuple[
        float, float, float, float, float, float,
        Optional[float], Optional[float],
        float, float, float, float, float, float,
        float, Dict[str, float],
        List[DashboardSeriesPoint],
        float, float, int,
        List[Dict[str, Any]], List[Dict[str, Any]], bool,
    ]:
        """Extract core financial amounts, curve points, and attributions."""
        initial_equity = 0.0
        final_equity = 0.0
        gross_pnl = 0.0
        net_pnl = 0.0
        gross_return_pct = 0.0
        net_return_pct = 0.0
        realized_pnl: Optional[float] = None
        unrealized_pnl: Optional[float] = None
        cash_balance = 0.0
        gross_exposure = 0.0
        gross_exposure_pct = 0.0
        net_exposure = 0.0
        net_exposure_pct = 0.0
        cash_reserve_pct = 0.0
        total_costs = 0.0
        cost_breakdown: Dict[str, float] = {}
        raw_equity_points: Sequence[Any] = ()
        max_dd_pct = 0.0
        max_dd_amt = 0.0
        max_dd_duration = 0
        raw_strat_attrs: List[Dict[str, Any]] = []
        raw_inst_attrs: List[Dict[str, Any]] = []
        inst_available = False

        if isinstance(source, PortfolioSimulationResult):
            initial_equity = source.initial_equity
            final_equity = source.final_equity
            gross_pnl = source.gross_pnl
            net_pnl = source.net_pnl
            net_return_pct = source.net_return_pct
            gross_return_pct = (gross_pnl / initial_equity * 100.0) if initial_equity > 0 else 0.0
            total_costs = source.total_transaction_costs
            cost_breakdown = {
                "fees": source.total_fees,
                "slippage": source.total_slippage,
            }
            raw_equity_points = source.equity_curve
            max_dd_pct = source.max_drawdown_pct
            max_dd_amt = source.max_drawdown_amount
            max_dd_duration = source.max_drawdown_duration_bars

            # Snapshots for exposures and cash balance
            if source.risk_snapshots:
                latest_snap = source.risk_snapshots[-1]
                cash_balance = latest_snap.cash_balance
                gross_exposure = latest_snap.gross_exposure
                gross_exposure_pct = latest_snap.gross_exposure_pct
                net_exposure = latest_snap.net_exposure
                net_exposure_pct = latest_snap.net_exposure_pct
                cash_reserve_pct = latest_snap.cash_reserve_pct
            elif source.equity_curve:
                latest_eq = source.equity_curve[-1]
                cash_balance = latest_eq.cash
                pos_val = latest_eq.position_value
                gross_exposure = abs(pos_val)
                gross_exposure_pct = (gross_exposure / latest_eq.total_equity * 100.0) if latest_eq.total_equity > 0 else 0.0
                cash_reserve_pct = (cash_balance / latest_eq.total_equity * 100.0) if latest_eq.total_equity > 0 else 0.0

            # Strategies
            for c in source.strategy_contributions:
                raw_strat_attrs.append(c.to_dict())

        elif isinstance(source, BacktestResult):
            initial_equity = source.config.initial_cash
            final_equity = source.metrics.final_equity
            gross_pnl = source.metrics.gross_profit - source.metrics.gross_loss
            net_pnl = source.metrics.net_profit
            net_return_pct = source.metrics.total_return_pct
            gross_return_pct = (gross_pnl / initial_equity * 100.0) if initial_equity > 0 else 0.0
            total_costs = source.metrics.total_transaction_costs
            cost_breakdown = {
                "fees": source.metrics.total_fees,
                "slippage": source.metrics.total_slippage,
            }
            raw_equity_points = source.equity_curve
            max_dd_pct = source.metrics.max_drawdown_pct
            max_dd_amt = source.metrics.max_drawdown_amount
            max_dd_duration = 0
            cash_balance = source.metrics.final_equity
            gross_exposure = (source.metrics.market_exposure_pct / 100.0) * final_equity
            gross_exposure_pct = source.metrics.market_exposure_pct
            cash_reserve_pct = max(0.0, 100.0 - gross_exposure_pct)

            # Single strategy attribution
            raw_strat_attrs.append({
                "strategy_id": source.strategy_name,
                "strategy_name": source.strategy_name,
                "weight_pct": 100.0,
                "gross_pnl": gross_pnl,
                "net_pnl": net_pnl,
                "realized_pnl": net_pnl,
                "unrealized_pnl": 0.0,
                "total_costs": total_costs,
                "trades_count": source.metrics.total_trades,
                "winning_trades": source.metrics.winning_trades,
                "win_rate": source.metrics.win_rate,
                "portfolio_pnl_share_pct": 100.0,
            })

        elif isinstance(source, dict):
            initial_equity = float(source.get("initial_equity", 100000.0))
            final_equity = float(source.get("final_equity", initial_equity))
            gross_pnl = float(source.get("gross_pnl", 0.0))
            net_pnl = float(source.get("net_pnl", final_equity - initial_equity))
            gross_return_pct = float(source.get("gross_return_pct", (gross_pnl / initial_equity * 100.0) if initial_equity > 0 else 0.0))
            net_return_pct = float(source.get("net_return_pct", (net_pnl / initial_equity * 100.0) if initial_equity > 0 else 0.0))
            total_costs = float(source.get("total_transaction_costs", 0.0))
            cost_breakdown = dict(source.get("cost_breakdown", {}))
            cash_balance = float(source.get("cash_balance", final_equity))
            max_dd_pct = float(source.get("max_drawdown_pct", 0.0))
            max_dd_amt = float(source.get("max_drawdown_amount", 0.0))
            max_dd_duration = int(source.get("max_drawdown_duration_bars", 0))
            raw_equity_points = source.get("equity_curve", ())
            raw_strat_attrs = list(source.get("strategy_attributions", []))
            raw_inst_attrs = list(source.get("instrument_attributions", []))

        # Reconcile or refine with attribution report if available
        if attr is not None:
            realized_pnl = attr.realized_pnl
            unrealized_pnl = attr.unrealized_pnl
            if attr.strategy_attributions:
                raw_strat_attrs = [s.to_dict() for s in attr.strategy_attributions]
            if attr.instrument_attributions:
                raw_inst_attrs = [i.to_dict() for i in attr.instrument_attributions]
                inst_available = any(i.is_available for i in attr.instrument_attributions)
            if attr.total_costs:
                total_costs = attr.total_costs.total_costs
                cost_breakdown = {
                    "commissions_fees": attr.total_costs.commissions_fees,
                    "slippage_costs": attr.total_costs.slippage_costs,
                    "spread_costs": attr.total_costs.spread_costs,
                }

        # Transform and sort equity curve points chronologically
        dashboard_points = self._build_dashboard_points(raw_equity_points, initial_equity)

        return (
            initial_equity, final_equity, gross_pnl, net_pnl, gross_return_pct, net_return_pct,
            realized_pnl, unrealized_pnl,
            cash_balance, gross_exposure, gross_exposure_pct, net_exposure, net_exposure_pct, cash_reserve_pct,
            total_costs, cost_breakdown,
            dashboard_points,
            max_dd_pct, max_dd_amt, max_dd_duration,
            raw_strat_attrs, raw_inst_attrs, inst_available,
        )

    def _build_dashboard_points(
        self,
        raw_points: Sequence[Any],
        initial_equity: float,
    ) -> List[DashboardSeriesPoint]:
        """Convert heterogeneous equity curve observations into strictly sorted DashboardSeriesPoint instances."""
        points: List[DashboardSeriesPoint] = []
        for p in raw_points:
            ts = 0.0
            cash = 0.0
            pos = 0.0
            equity = 0.0
            dd_pct = 0.0
            dd_amt = 0.0

            if isinstance(p, EquityPoint):
                ts = p.timestamp
                cash = p.cash
                pos = p.position_value
                equity = p.total_equity
                dd_pct = p.drawdown_pct
                dd_amt = p.drawdown_amount
            elif isinstance(p, dict):
                ts = float(p.get("timestamp", 0.0))
                cash = float(p.get("cash", 0.0))
                pos = float(p.get("position_value", 0.0))
                equity = float(p.get("total_equity", cash + pos))
                dd_pct = float(p.get("drawdown_pct", 0.0))
                dd_amt = float(p.get("drawdown_amount", 0.0))

            cum_ret = ((equity - initial_equity) / initial_equity * 100.0) if initial_equity > 0 else 0.0

            points.append(
                DashboardSeriesPoint(
                    timestamp=ts,
                    timestamp_iso=format_iso_timestamp(ts),
                    cash=sanitize_numeric(cash, precision=2, default=0.0) or 0.0,
                    position_value=sanitize_numeric(pos, precision=2, default=0.0) or 0.0,
                    total_equity=sanitize_numeric(equity, precision=2, default=0.0) or 0.0,
                    drawdown_pct=sanitize_numeric(dd_pct, precision=2, default=0.0) or 0.0,
                    drawdown_amount=sanitize_numeric(dd_amt, precision=2, default=0.0) or 0.0,
                    cumulative_return_pct=sanitize_numeric(cum_ret, precision=4, default=0.0) or 0.0,
                )
            )

        # Sort chronologically by timestamp ascending
        points.sort(key=lambda pt: pt.timestamp)
        return points

    def _synthesize_voice_summary(
        self,
        source_id: str,
        net_pnl: float,
        net_return_pct: float,
        max_dd_pct: float,
        max_dd_amt: float,
        max_dd_duration: int,
        risk_report: Optional[PortfolioRiskReport],
        data_warnings: Sequence[str],
        attr: Optional[PortfolioAttributionReport],
    ) -> VoiceSummary:
        """Formulate concise structured speech fields for voice synthesis."""
        pnl_sign = "profit" if net_pnl >= 0 else "loss"
        abs_pnl = abs(net_pnl)
        sign_word = "plus" if net_return_pct >= 0 else "minus"
        abs_ret = abs(net_return_pct)

        headline = (
            f"Portfolio evaluation for run {source_id} concluded with a net {pnl_sign} of "
            f"${abs_pnl:,.2f}, representing a {sign_word} {abs_ret:.1f} percent return."
        )

        net_pnl_speech = (
            f"Net performance is a {pnl_sign} of {abs_pnl:,.0f} dollars, or {net_return_pct:.2f} percent."
        )

        drawdown_speech = (
            f"Maximum peak-to-trough drawdown reached {max_dd_pct:.1f} percent, "
            f"or {max_dd_amt:,.0f} dollars, over a span of {max_dd_duration} bars."
        )

        if risk_report is not None:
            if risk_report.overall_state == AlertState.NORMAL:
                risk_speech = "Portfolio risk monitor status is NORMAL with zero active breaches."
            else:
                alert_count = len(risk_report.active_alerts)
                risk_speech = (
                    f"Portfolio risk monitor is currently in {risk_report.overall_state.value} state "
                    f"with {alert_count} active alerts."
                )
        else:
            risk_speech = "Portfolio risk monitoring report was not evaluated."

        # Sharpe ratio speech
        sharpe_info = ""
        if attr is not None and attr.risk_adjusted_metrics is not None:
            sh = attr.risk_adjusted_metrics.sharpe_ratio
            if sh.is_available and sh.value is not None:
                sharpe_info = f" The annualized Sharpe ratio was {sh.value:.2f}."
            else:
                sharpe_info = " Sharpe ratio is unavailable due to insufficient return variance."

        if data_warnings:
            warning_speech = f"Attention: {len(data_warnings)} data warnings were flagged during report audit."
        else:
            warning_speech = "All accounting data and reconciliation checks passed with zero discrepancies."

        full_speech = (
            f"{headline} {net_pnl_speech} {drawdown_speech}{sharpe_info} {risk_speech} {warning_speech}"
        )

        return VoiceSummary(
            headline=headline,
            net_pnl_speech=net_pnl_speech,
            drawdown_speech=drawdown_speech,
            risk_status_speech=risk_speech,
            warning_speech=warning_speech,
            full_speech_script=full_speech,
        )

    def _persist_report(self, report: UnifiedPortfolioReport) -> None:
        """Cache report in memory (bounded) and persist into StateStore."""
        # Enforce memory bounded history
        while len(self._reports) >= self.max_history_size:
            oldest_id = next(iter(self._reports))
            oldest_rep = self._reports.pop(oldest_id, None)
            if oldest_rep and oldest_rep.provenance.source_run_id in self._run_to_report_map:
                self._run_to_report_map.pop(oldest_rep.provenance.source_run_id, None)

        self._reports[report.report_id] = report
        self._run_to_report_map[report.provenance.source_run_id] = report.report_id

        # Persist to StateStore
        if self.state_store is not None:
            try:
                rep_dict = report.to_dict()
                self.state_store.set(self.STATE_LATEST_KEY, rep_dict)
                self.state_store.set(f"{self.STATE_REPORTS_PREFIX}{report.report_id}", rep_dict)
                self.state_store.set(self.STATE_INDEX_KEY, list(self._reports.keys()))
            except Exception as exc:
                logger.warning("Failed to persist report into StateStore: %s", exc)

    def _deserialize_report(self, data: Dict[str, Any]) -> UnifiedPortfolioReport:
        """Safely deserialize a dictionary back into a UnifiedPortfolioReport."""
        prov_data = data.get("provenance", {})
        provenance = ReportDataProvenance(
            source_run_id=prov_data.get("source_run_id", "unknown"),
            source_type=prov_data.get("source_type", "synthetic"),
            generated_at=float(prov_data.get("generated_at", time.time())),
            generated_at_iso=prov_data.get("generated_at_iso", "1970-01-01T00:00:00Z"),
            engine_version=prov_data.get("engine_version", "1.0.0"),
            git_commit=prov_data.get("git_commit"),
            timeframe=prov_data.get("timeframe", "1d"),
            start_time=prov_data.get("start_time"),
            end_time=prov_data.get("end_time"),
            parameters=prov_data.get("parameters", {}),
        )

        status_str = data.get("status", "VALID")
        status = ReportDataStatus(status_str) if status_str in ReportDataStatus._value2member_map_ else ReportDataStatus.VALID

        points: List[DashboardSeriesPoint] = []
        for pt in data.get("equity_curve", []):
            points.append(
                DashboardSeriesPoint(
                    timestamp=float(pt.get("timestamp", 0.0)),
                    timestamp_iso=pt.get("timestamp_iso", "1970-01-01T00:00:00Z"),
                    cash=float(pt.get("cash", 0.0)),
                    position_value=float(pt.get("position_value", 0.0)),
                    total_equity=float(pt.get("total_equity", 0.0)),
                    drawdown_pct=float(pt.get("drawdown_pct", 0.0)),
                    drawdown_amount=float(pt.get("drawdown_amount", 0.0)),
                    cumulative_return_pct=float(pt.get("cumulative_return_pct", 0.0)),
                )
            )

        voice_data = data.get("voice_summary")
        voice_summary = (
            VoiceSummary(
                headline=voice_data.get("headline", ""),
                net_pnl_speech=voice_data.get("net_pnl_speech", ""),
                drawdown_speech=voice_data.get("drawdown_speech", ""),
                risk_status_speech=voice_data.get("risk_status_speech", ""),
                warning_speech=voice_data.get("warning_speech", ""),
                full_speech_script=voice_data.get("full_speech_script", ""),
            )
            if voice_data
            else None
        )

        return UnifiedPortfolioReport(
            report_id=data.get("report_id", "rep_unknown"),
            title=data.get("title", "Portfolio Report"),
            provenance=provenance,
            status=status,
            data_warnings=tuple(data.get("data_warnings", ())),
            initial_equity=float(data.get("initial_equity", 0.0)),
            final_equity=float(data.get("final_equity", 0.0)),
            gross_pnl=float(data.get("gross_pnl", 0.0)),
            net_pnl=float(data.get("net_pnl", 0.0)),
            gross_return_pct=float(data.get("gross_return_pct", 0.0)),
            net_return_pct=float(data.get("net_return_pct", 0.0)),
            realized_pnl=data.get("realized_pnl"),
            unrealized_pnl=data.get("unrealized_pnl"),
            cash_balance=float(data.get("cash_balance", 0.0)),
            gross_exposure=float(data.get("gross_exposure", 0.0)),
            gross_exposure_pct=float(data.get("gross_exposure_pct", 0.0)),
            net_exposure=float(data.get("net_exposure", 0.0)),
            net_exposure_pct=float(data.get("net_exposure_pct", 0.0)),
            cash_reserve_pct=float(data.get("cash_reserve_pct", 0.0)),
            total_transaction_costs=float(data.get("total_transaction_costs", 0.0)),
            cost_breakdown=dict(data.get("cost_breakdown", {})),
            equity_curve=tuple(points),
            max_drawdown_pct=float(data.get("max_drawdown_pct", 0.0)),
            max_drawdown_amount=float(data.get("max_drawdown_amount", 0.0)),
            max_drawdown_duration_bars=int(data.get("max_drawdown_duration_bars", 0)),
            risk_adjusted_metrics=dict(data.get("risk_adjusted_metrics", {})),
            strategy_attributions=tuple(data.get("strategy_attributions", ())),
            instrument_attributions=tuple(data.get("instrument_attributions", ())),
            instrument_attribution_available=bool(data.get("instrument_attribution_available", False)),
            reconciliation=data.get("reconciliation"),
            risk_status=data.get("risk_status"),
            active_alerts_count=int(data.get("active_alerts_count", 0)),
            walk_forward_summary=data.get("walk_forward_summary"),
            robustness_summary=data.get("robustness_summary"),
            voice_summary=voice_summary,
            disclaimer=data.get("disclaimer", ""),
        )
