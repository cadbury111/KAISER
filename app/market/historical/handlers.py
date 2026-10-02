"""KAISER — Historical Market Data Orchestrator Handlers.

Exposes user, agent, and CLI commands on the MasterOrchestrator for loading,
storing, querying, and replaying historical OHLCV market data.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Union

from app.analysis.models import TIMEFRAME_SECONDS, normalize_timeframe
from app.core.exceptions import ValidationError
from app.market.historical.loader import HistoricalDataLoader
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalProviderStatus,
)
from app.market.historical.replay import (
    HistoricalReplayEngine,
    HistoricalReplayRequest,
)
from app.market.historical.storage import HistoricalBarRepository
from app.market.models import Bar
from app.market.normalizer import parse_timestamp
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator


def register_historical_handlers(
    orchestrator: MasterOrchestrator,
    loader: HistoricalDataLoader,
    repository: Optional[HistoricalBarRepository] = None,
    replay_engine: Optional[HistoricalReplayEngine] = None,
) -> None:
    """Register historical market data commands with the Master Orchestrator.

    Commands registered:
    - `load_historical_data`: Fetch, validate, and ingest historical market bars.
    - `store_historical_bars`: Persist bars into local SQLite repository.
    - `query_historical_bars`: Query stored historical bars from local repository.
    - `replay_historical_bars`: Execute deterministic replay through analysis pipeline.
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

    def handle_store_historical_bars(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'store_historical_bars' command."""
        repo = repository or getattr(loader, "repository", None)
        if not repo:
            raise ValidationError("No storage repository is configured for storing historical bars.")

        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        raw_bars = p.get("bars")
        symbol_raw = p.get("symbol")
        if not symbol_raw and raw_bars and isinstance(raw_bars, (list, tuple)) and len(raw_bars) > 0:
            first_b = raw_bars[0]
            if isinstance(first_b, Bar):
                symbol_raw = first_b.symbol
            elif isinstance(first_b, (dict, Mapping)):
                symbol_raw = first_b.get("symbol")

        if not symbol_raw or not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ValidationError("Field 'symbol' is required for storing historical bars.")

        timeframe_raw = str(p.get("timeframe", "15m")).strip()
        timeframe = normalize_timeframe(timeframe_raw)
        source = str(p.get("source", "orchestrator_store"))

        bars_to_store: List[Bar] = []

        if raw_bars and isinstance(raw_bars, (list, tuple)):
            for b in raw_bars:
                if isinstance(b, Bar):
                    bars_to_store.append(b)
                elif isinstance(b, (dict, Mapping)):
                    bar = loader.normalizer.normalize_bar(dict(b), default_source=source)
                    bars_to_store.append(bar)
        else:
            # If bars are not directly supplied, fetch via loader and store
            end_val = p.get("end_time") or p.get("end_iso")
            end_time = parse_timestamp(end_val) if end_val is not None else time.time()
            interval = TIMEFRAME_SECONDS.get(timeframe, 60)
            start_val = p.get("start_time") or p.get("start_iso")
            start_time = parse_timestamp(start_val) if start_val is not None else end_time - (100 * interval)
            limit = int(p.get("limit", 1000))

            req_spec = HistoricalBarRequest(
                symbol=symbol_raw.strip().upper(),
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
            resp = loader.load_bars(req_spec, paginate_all=bool(p.get("paginate_all", False)))
            bars_to_store = list(resp.bars)

        stored_count = loader.persist_bars(bars_to_store, source_provider=source) if hasattr(loader, "persist_bars") else repo.store_bars(bars_to_store, source_provider=source)

        return {
            "status": "success",
            "message": f"Successfully stored {stored_count} bars for {symbol_raw.strip().upper()} ({timeframe})",
            "symbol": symbol_raw.strip().upper(),
            "timeframe": timeframe,
            "stored_count": stored_count,
        }

    orchestrator.register_handler(
        command="store_historical_bars",
        handler=handle_store_historical_bars,
        capability="historical_data",
        allow_override=True,
    )

    def handle_query_historical_bars(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'query_historical_bars' command."""
        repo = repository or getattr(loader, "repository", None)
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        symbol_raw = p.get("symbol")
        if not symbol_raw or not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ValidationError("Field 'symbol' is required for querying historical bars.")

        timeframe_raw = str(p.get("timeframe", "15m")).strip()
        timeframe = normalize_timeframe(timeframe_raw)

        end_val = p.get("end_time") or p.get("end_iso")
        end_time = parse_timestamp(end_val) if end_val is not None else time.time()
        interval = TIMEFRAME_SECONDS.get(timeframe, 60)
        start_val = p.get("start_time") or p.get("start_iso")
        start_time = parse_timestamp(start_val) if start_val is not None else end_time - (100 * interval)
        limit = int(p.get("limit", 1000))

        if repo:
            bars = repo.query_bars(
                symbol=symbol_raw.strip().upper(),
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
        else:
            bar_req = HistoricalBarRequest(
                symbol=symbol_raw.strip().upper(),
                timeframe=timeframe,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
            resp = loader.load_bars(bar_req)
            bars = resp.bars

        first_ts = bars[0].timestamp if bars else None
        last_ts = bars[-1].timestamp if bars else None

        return {
            "status": "success",
            "symbol": symbol_raw.strip().upper(),
            "timeframe": timeframe,
            "bars_count": len(bars),
            "count": len(bars),
            "first_timestamp": first_ts,
            "last_timestamp": last_ts,
            "start_time": start_time,
            "end_time": end_time,
            "bars": [b.to_dict() for b in bars],
        }

    orchestrator.register_handler(
        command="query_historical_bars",
        handler=handle_query_historical_bars,
        capability="historical_data",
        allow_override=True,
    )

    def handle_replay_historical_bars(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'replay_historical_bars' command."""
        engine = replay_engine or getattr(loader, "replay_engine", None)
        if not engine:
            raise ValidationError("No HistoricalReplayEngine is configured for replaying bars.")

        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})
        symbol_raw = p.get("symbol")
        if not symbol_raw or not isinstance(symbol_raw, str) or not symbol_raw.strip():
            raise ValidationError("Field 'symbol' is required for replaying historical data.")

        timeframe_raw = str(p.get("timeframe", "15m")).strip()
        timeframe = normalize_timeframe(timeframe_raw)

        end_val = p.get("end_time") or p.get("end_iso")
        end_time = parse_timestamp(end_val) if end_val is not None else time.time()
        interval = TIMEFRAME_SECONDS.get(timeframe, 60)
        start_val = p.get("start_time") or p.get("start_iso")
        start_time = parse_timestamp(start_val) if start_val is not None else end_time - (100 * interval)
        limit = int(p.get("limit", 5000))
        pacing = float(p.get("pacing_seconds", 0.0))
        reset_state = bool(p.get("reset_analysis_state", True))

        replay_req = HistoricalReplayRequest(
            symbol=symbol_raw.strip().upper(),
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            pacing_seconds=pacing,
            reset_analysis_state=reset_state,
            correlation_id=req.correlation_id,
        )

        result = engine.replay(replay_req)

        return {
            "status": "success" if result.is_success else "failed",
            "replay_status": result.status.value,
            "message": result.summary_text,
            "data": result.to_dict(),
            "bars_replayed": result.bars_replayed,
            "bars_accepted": result.bars_accepted,
        }

    orchestrator.register_handler(
        command="replay_historical_bars",
        handler=handle_replay_historical_bars,
        capability="historical_data",
        allow_override=True,
    )
