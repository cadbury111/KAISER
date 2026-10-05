"""KAISER — Market Data Quality Orchestrator Handlers.

Exposes user, agent, and system commands on the MasterOrchestrator for evaluating
market data reliability, querying quality assessments, and inspecting detected anomalies.

SAFETY INVARIANTS:
1. Purely informational, read-only quality monitoring.
2. Settings.execution_locked = True unconditionally preserved; zero order or broker routes exist.
3. Strict parameter validation and deterministic error handling.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from app.analysis.models import TIMEFRAME_SECONDS, normalize_timeframe
from app.core.exceptions import ValidationError
from app.market.historical.storage import HistoricalBarRepository
from app.market.normalizer import parse_timestamp
from app.market.quality.models import QualitySeverity
from app.market.quality.monitor import MarketDataQualityMonitor
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator


def register_quality_handlers(
    orchestrator: MasterOrchestrator,
    quality_monitor: MarketDataQualityMonitor,
    repository: Optional[HistoricalBarRepository] = None,
) -> None:
    """Register market data quality commands with the Master Orchestrator.

    Commands registered:
    - `assess_market_data_quality`: Run quality evaluation on a bar series or stored data.
    - `get_market_data_quality`: Retrieve latest quality assessment or stream summary.
    - `get_market_data_anomalies`: Query bounded anomaly history with optional filters.
    """

    def handle_assess_market_data_quality(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'assess_market_data_quality' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        symbol_raw = p.get("symbol")
        if not symbol_raw or not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ValidationError("Field 'symbol' is required for market data quality assessment.")

        symbol = symbol_raw.strip().upper()
        timeframe_raw = str(p.get("timeframe", "15m")).strip()
        timeframe = normalize_timeframe(timeframe_raw)

        source = str(p.get("source", "orchestrator")).strip()

        # Injected reference time for deterministic testing if provided
        ref_time_val = p.get("reference_time")
        ref_time: Optional[float] = float(ref_time_val) if ref_time_val is not None else None

        # Check if bars were passed directly in payload
        bars = p.get("bars")
        if bars is None and repository is not None:
            # Query from historical repository if start/end time provided
            end_val = p.get("end_time") or p.get("end_iso")
            end_time = parse_timestamp(end_val) if end_val is not None else (ref_time or time.time())

            interval = TIMEFRAME_SECONDS.get(timeframe, 60.0)
            start_val = p.get("start_time") or p.get("start_iso")
            start_time = parse_timestamp(start_val) if start_val is not None else (end_time - (100 * interval))

            limit_val = p.get("limit", 1000)
            limit = int(limit_val) if limit_val is not None else 1000

            try:
                bars = repository.query_bars(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit,
                )
            except Exception as err:
                raise ValidationError(f"Failed to query stored bars for assessment: {err}") from err

        if bars is None:
            bars = ()

        assessment = quality_monitor.evaluate_bar_series(
            bars=bars,
            symbol=symbol,
            timeframe=timeframe,
            source=source,
            reference_time=ref_time,
            correlation_id=req.correlation_id,
        )

        return {
            "status": "success",
            "assessment": assessment.to_dict(),
        }

    def handle_get_market_data_quality(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_market_data_quality' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        symbol_raw = p.get("symbol")
        timeframe_raw = p.get("timeframe")

        if symbol_raw and isinstance(symbol_raw, str) and symbol_raw.strip():
            symbol = symbol_raw.strip().upper()
            timeframe = normalize_timeframe(str(timeframe_raw).strip()) if timeframe_raw else None
            assessment = quality_monitor.get_latest_assessment(symbol=symbol, timeframe=timeframe)
            return {
                "status": "success",
                "symbol": symbol,
                "timeframe": timeframe,
                "found": assessment is not None,
                "assessment": assessment.to_dict() if assessment else None,
                "summary": quality_monitor.get_quality_summary(),
            }

        # Return snapshot of all monitored streams
        return {
            "status": "success",
            "all_streams": quality_monitor.get_all_latest_assessments(),
            "summary": quality_monitor.get_quality_summary(),
        }

    def handle_get_market_data_anomalies(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_market_data_anomalies' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        symbol_raw = p.get("symbol")
        symbol = symbol_raw.strip().upper() if symbol_raw and isinstance(symbol_raw, str) and symbol_raw.strip() else None

        severity_raw = p.get("severity")
        severity: Optional[QualitySeverity] = None
        if severity_raw and isinstance(severity_raw, str) and severity_raw.strip():
            try:
                severity = QualitySeverity(severity_raw.strip().upper())
            except ValueError:
                raise ValidationError(
                    f"Invalid severity filter '{severity_raw}'. Must be one of: "
                    f"{', '.join(s.value for s in QualitySeverity)}"
                )

        limit_val = p.get("limit", 50)
        try:
            limit = int(limit_val)
        except (ValueError, TypeError):
            limit = 50

        anomalies = quality_monitor.get_anomalies(symbol=symbol, severity=severity, limit=limit)

        return {
            "status": "success",
            "count": len(anomalies),
            "limit": limit,
            "filter_symbol": symbol,
            "filter_severity": severity.value if severity else None,
            "anomalies": [a.to_dict() for a in anomalies],
        }

    orchestrator.register_handler(
        command="assess_market_data_quality",
        handler=handle_assess_market_data_quality,
        description="Run deterministic quality evaluation and anomaly detection on a market data series",
        capability="market_quality",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="get_market_data_quality",
        handler=handle_get_market_data_quality,
        description="Retrieve latest market data quality assessment or monitored stream summaries",
        capability="market_quality",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="get_market_data_anomalies",
        handler=handle_get_market_data_anomalies,
        description="Query bounded in-memory market anomaly detections with optional symbol/severity filters",
        capability="market_quality",
        allow_override=True,
    )
