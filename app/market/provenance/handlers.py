"""KAISER — Market Data Provenance & Lineage Orchestrator Handlers.

Provides read-only query handlers for market data provenance records,
lineage DAG traversal, and bounded audit history.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from app.analysis.models import normalize_timeframe
from app.core.exceptions import ValidationError
from app.market.provenance.models import DataSourceType
from app.market.provenance.tracker import MarketDataProvenanceTracker
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.market.provenance.handlers")


def register_provenance_handlers(
    orchestrator: MasterOrchestrator,
    provenance_tracker: MarketDataProvenanceTracker,
) -> None:
    """Register market data provenance and lineage commands with MasterOrchestrator.

    Commands registered:
    - `get_data_provenance`: Retrieve a provenance record by ID or stream (symbol + timeframe).
    - `get_data_lineage`: Traverse upstream/downstream lineage DAG with cycle detection.
    - `get_data_audit_history`: Query bounded audit history with filtering and pagination.
    """

    def handle_get_data_provenance(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_data_provenance' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        provenance_id = p.get("provenance_id")
        if provenance_id and isinstance(provenance_id, str) and provenance_id.strip():
            prov = provenance_tracker.get_provenance(provenance_id.strip())
            if not prov:
                return {
                    "status": "not_found",
                    "provenance_id": provenance_id.strip(),
                    "message": f"Provenance record with ID '{provenance_id.strip()}' was not found in audit history.",
                    "read_only": True,
                }
            return {
                "status": "success",
                "provenance": prov.to_dict(),
                "read_only": True,
            }

        # Alternatively lookup by stream (symbol + timeframe)
        symbol = p.get("symbol")
        timeframe = p.get("timeframe")
        if symbol and timeframe:
            norm_sym = str(symbol).strip().upper()
            try:
                norm_tf = normalize_timeframe(str(timeframe).strip())
            except ValidationError:
                norm_tf = str(timeframe).strip()

            prov = provenance_tracker.get_latest(symbol=norm_sym, timeframe=norm_tf)
            if not prov:
                return {
                    "status": "not_found",
                    "symbol": norm_sym,
                    "timeframe": norm_tf,
                    "message": f"No provenance recorded for stream '{norm_sym}_{norm_tf}'.",
                    "read_only": True,
                }
            return {
                "status": "success",
                "symbol": norm_sym,
                "timeframe": norm_tf,
                "provenance": prov.to_dict(),
                "read_only": True,
            }

        # Alternatively lookup by operation_id
        operation_id = p.get("operation_id")
        if operation_id and isinstance(operation_id, str) and operation_id.strip():
            records = provenance_tracker.get_by_operation(operation_id.strip())
            return {
                "status": "success",
                "operation_id": operation_id.strip(),
                "count": len(records),
                "provenance_records": [r.to_dict() for r in records],
                "read_only": True,
            }

        raise ValidationError(
            "Command 'get_data_provenance' requires either 'provenance_id', ('symbol' and 'timeframe'), or 'operation_id'."
        )

    def handle_get_data_lineage(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_data_lineage' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        provenance_id = p.get("provenance_id")
        if not provenance_id or not isinstance(provenance_id, str) or not provenance_id.strip():
            raise ValidationError("Field 'provenance_id' is required for lineage traversal.")

        pid_clean = provenance_id.strip()
        direction = str(p.get("direction", "upstream")).strip().lower()
        max_depth = int(p.get("max_depth", 10))

        graph = provenance_tracker.get_lineage(
            provenance_id=pid_clean,
            max_depth=max_depth,
            direction=direction,
        )

        if not graph.nodes:
            return {
                "status": "not_found",
                "provenance_id": pid_clean,
                "message": f"Provenance record with ID '{pid_clean}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "provenance_id": pid_clean,
            "lineage": graph.to_dict(),
            "read_only": True,
        }

    def handle_get_data_audit_history(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_data_audit_history' command."""
        p = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or getattr(req, "payload", {}) or {})

        limit = int(p.get("limit", 100))
        offset = int(p.get("offset", 0))
        symbol = str(p.get("symbol")).strip() if p.get("symbol") else None
        timeframe = str(p.get("timeframe")).strip() if p.get("timeframe") else None
        source_type = str(p.get("source_type")).strip() if p.get("source_type") else None
        operation_id = str(p.get("operation_id")).strip() if p.get("operation_id") else None
        parent_provenance_id = str(p.get("parent_provenance_id")).strip() if p.get("parent_provenance_id") else None
        start_time = float(p["start_time"]) if "start_time" in p and p["start_time"] is not None else None
        end_time = float(p["end_time"]) if "end_time" in p and p["end_time"] is not None else None

        records = provenance_tracker.get_audit_history(
            limit=limit,
            offset=offset,
            symbol=symbol,
            timeframe=timeframe,
            source_type=source_type,
            operation_id=operation_id,
            parent_provenance_id=parent_provenance_id,
            start_time=start_time,
            end_time=end_time,
        )
        summary = provenance_tracker.get_summary()

        return {
            "status": "success",
            "count": len(records),
            "limit": limit,
            "offset": offset,
            "summary": summary,
            "records": [r.to_dict() for r in records],
            "read_only": True,
        }

    orchestrator.register_handler(
        command="get_data_provenance",
        handler=handle_get_data_provenance,
        capability="market_provenance",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_data_lineage",
        handler=handle_get_data_lineage,
        capability="market_provenance",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_data_audit_history",
        handler=handle_get_data_audit_history,
        capability="market_provenance",
        allow_override=True,
    )
    logger.info("Registered market data provenance and lineage orchestrator handlers.")
