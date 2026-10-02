"""KAISER — Technical Analysis Orchestrator Handlers.

Exposes deterministic analysis capabilities to the MasterOrchestrator, CLI,
and Voice Service interfaces without touching broker execution engines.
"""

from __future__ import annotations

from typing import Any, Dict

from app.analysis.engine import TechnicalAnalysisEngine
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest, OrchestratorResponse, RequestStatus


def register_analysis_handlers(
    orchestrator: Any,
    engine: TechnicalAnalysisEngine,
) -> None:
    """Register technical analysis command handlers on the MasterOrchestrator."""

    def handle_technical_analysis(req: OrchestratorRequest) -> Dict[str, Any]:
        """Produce full technical indicator summary for a given symbol."""
        params = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or {})
        symbol = params.get("symbol")
        timeframe = params.get("timeframe", "1m")

        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("Missing or invalid 'symbol' parameter for technical_analysis.")

        vector = engine.get_feature_vector(symbol=symbol.strip().upper(), timeframe=str(timeframe).strip())
        if not vector:
            return {
                "status": "unavailable",
                "message": f"No technical analysis available for {symbol.strip().upper()} ({timeframe}).",
                "symbol": symbol.strip().upper(),
                "timeframe": timeframe,
                "data": None,
            }

        # Format concise human & voice friendly summary
        rsi_val = vector.indicators.get(f"RSI_{engine.config.rsi_period}")
        macd_val = vector.indicators.get(
            f"MACD_{engine.config.macd_fast}_{engine.config.macd_slow}_{engine.config.macd_signal}"
        )
        bb_val = vector.indicators.get(
            f"BB_{engine.config.bollinger_period}_{engine.config.bollinger_std_dev}"
        )

        parts = [f"Technical analysis for {vector.symbol}: Last close {vector.last_close:.2f}."]
        if rsi_val and rsi_val.value is not None:
            parts.append(f"RSI is {rsi_val.value:.1f} ({vector.signals.get('rsi', 'neutral')}).")
        if macd_val and macd_val.values:
            hist = macd_val.values.get("histogram", 0.0)
            parts.append(f"MACD histogram is {hist:.2f} ({vector.signals.get('macd', 'neutral')}).")
        if bb_val and bb_val.values:
            parts.append(f"Bollinger Band status is {vector.signals.get('bollinger', 'neutral')}.")

        summary_text = " ".join(parts)

        return {
            "status": "success",
            "message": summary_text,
            "symbol": vector.symbol,
            "timeframe": vector.timeframe,
            "last_close": vector.last_close,
            "bar_count": vector.bar_count,
            "signals": vector.signals,
            "data": vector.to_dict(),
        }

    def handle_calculate_indicator(req: OrchestratorRequest) -> Dict[str, Any]:
        """Query a specific technical indicator for a symbol."""
        params = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or {})
        symbol = params.get("symbol")
        indicator = params.get("indicator")
        timeframe = params.get("timeframe", "1m")

        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("Missing or invalid 'symbol' parameter.")
        if not indicator or not isinstance(indicator, str) or not indicator.strip():
            raise ValidationError("Missing or invalid 'indicator' parameter.")

        clean_symbol = symbol.strip().upper()
        clean_ind = indicator.strip().upper()

        vector = engine.get_feature_vector(symbol=clean_symbol, timeframe=str(timeframe).strip())
        if not vector:
            return {
                "status": "unavailable",
                "message": f"No data available to calculate {clean_ind} for {clean_symbol}.",
                "symbol": clean_symbol,
                "indicator": clean_ind,
                "data": None,
            }

        # Resolve indicator query (supports generic 'RSI', 'MACD', or specific 'SMA_20', etc.)
        matched_value = None
        for name, ind_val in vector.indicators.items():
            if name.upper() == clean_ind or name.upper().startswith(clean_ind):
                matched_value = ind_val
                break

        if not matched_value:
            return {
                "status": "not_found",
                "message": f"Indicator '{clean_ind}' not found for {clean_symbol}.",
                "symbol": clean_symbol,
                "indicator": clean_ind,
                "data": None,
            }

        if not matched_value.is_valid:
            return {
                "status": "insufficient_data",
                "message": f"Insufficient historical bars to compute {matched_value.name} for {clean_symbol}.",
                "symbol": clean_symbol,
                "indicator": matched_value.name,
                "data": matched_value.to_dict(),
            }

        val_repr = (
            f"{matched_value.value:.2f}"
            if matched_value.value is not None
            else str(matched_value.values)
        )
        msg = f"{matched_value.name} for {clean_symbol} is {val_repr}."

        return {
            "status": "success",
            "message": msg,
            "symbol": clean_symbol,
            "indicator": matched_value.name,
            "value": matched_value.value,
            "values": matched_value.values,
            "data": matched_value.to_dict(),
        }

    orchestrator.register_handler(
        command="technical_analysis",
        handler=handle_technical_analysis,
        capability="analysis",
        allow_override=True,
    )

    orchestrator.register_handler(
        command="calculate_indicator",
        handler=handle_calculate_indicator,
        capability="analysis",
        allow_override=True,
    )
