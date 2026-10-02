"""KAISER — Historical Market Data Orchestrator Handlers.

Exposes user and agent-invocable commands on the MasterOrchestrator for loading
and ingesting historical OHLCV market data.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional, Union

from app.analysis.models import TIMEFRAME_SECONDS, normalize_timeframe
from app.core.exceptions import ValidationError
from app.market.historical.loader import HistoricalDataLoader
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalProviderStatus,
)
from app.market.normalizer import parse_timestamp
from app.orchestrator.master import MasterOrchestrator
from app.orchestrator.models import OrchestratorRequest


def register_historical_handlers(
    orchestrator: MasterOrchestrator,
    loader: HistoricalDataLoader,
) -> None:
    """Register historical data loading commands with the Master Orchestrator.

    Commands registered:
    - `load_historical_data`: Fetch, validate, and ingest historical market bars.
    """

    def handle_load_historical_data(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'load_historical_data' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        symbol_raw = p.get("symbol")
        if not symbol_raw or not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ValidationError("Field 'symbol' is required for historical data loading.")

        timeframe_raw = str(p.get("timeframe", "15m")).strip()
        timeframe = normalize_timeframe(timeframe_raw)

        # Parse end_time (defaults to current time if omitted)
        end_val = p.get("end_time") or p.get("end_iso")
        if end_val is not None:
            end_time = parse_timestamp(end_val)
        else:
            end_time = time.time()

        # Parse start_time (defaults to 100 intervals before end_time if omitted)
        interval = TIMEFRAME_SECONDS.get(timeframe, 60)
        start_val = p.get("start_time") or p.get("start_iso")
        if start_val is not None:
            start_time = parse_timestamp(start_val)
        else:
            start_time = end_time - (100 * interval)

        limit_val = p.get("limit", 1000)
        limit = int(limit_val) if limit_val is not None else 1000

        paginate_all = bool(p.get("paginate_all", False))
        exchange = str(p.get("exchange", "NSE")).strip().upper()

        request = HistoricalBarRequest(
            symbol=symbol_raw.strip().upper(),
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            exchange=exchange,
        )

        result = loader.load_and_ingest(
            request=request,
            correlation_id=req.correlation_id,
            paginate_all=paginate_all,
        )

        success = result.provider_status in (
            HistoricalProviderStatus.SUCCESS,
            HistoricalProviderStatus.PARTIAL,
            HistoricalProviderStatus.EMPTY,
        )

        return {
            "status": "success" if success else "failed",
            "message": result.summary_text,
            "symbol": result.request.symbol,
            "timeframe": result.request.timeframe,
            "provider_status": result.provider_status.value,
            "received_bars_count": result.received_bars_count,
            "accepted_bars_count": result.accepted_bars_count,
            "rejected_bars_count": result.rejected_bars_count,
            "duplicate_bars_count": result.duplicate_bars_count,
            "zero_volume_bars_count": result.zero_volume_bars_count,
            "detected_gaps_count": result.detected_gaps_count,
            "rejection_reasons": list(result.rejection_reasons),
            "gap_details": list(result.gap_details),
            "data": result.to_dict(),
        }

    orchestrator.register_handler(
        command="load_historical_data",
        handler=handle_load_historical_data,
        capability="historical_data",
        allow_override=True,
    )
