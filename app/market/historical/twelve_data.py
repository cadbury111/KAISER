"""KAISER — Twelve Data Historical Market Data Provider.

Implements the HistoricalDataProvider interface for querying historical OHLCV
candlestick records via Twelve Data /time_series API. Adheres to candle timestamp
conventions (POSIX UTC open time), timeframe canonicalization, zero-volume
preservation, and isolation from live execution.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import math
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from app.analysis.models import SUPPORTED_TIMEFRAMES, normalize_timeframe
from app.core.exceptions import ValidationError
from app.market.feeds.twelve_data import (
    StandardHttpTransport,
    TwelveDataConfig,
    TwelveDataTransport,
    parse_twelve_data_timestamp,
)
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalProviderStatus,
)
from app.market.historical.provider import HistoricalDataProvider
from app.market.models import Bar

logger = logging.getLogger("market.historical.twelvedata")

# Mapping from KAISER canonical timeframe to Twelve Data interval string
TIMEFRAME_TO_TWELVE_DATA_INTERVAL: Dict[str, str] = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1h": "1h",
    "1D": "1day",
}


class TwelveDataHistoricalProvider(HistoricalDataProvider):
    """Production historical market data provider integrating Twelve Data."""

    def __init__(
        self,
        config: TwelveDataConfig,
        transport: Optional[TwelveDataTransport] = None,
        name: str = "twelve_data_historical_provider",
        supported_symbols: Optional[Tuple[str, ...]] = None,
    ) -> None:
        super().__init__(
            name=name,
            supported_timeframes=tuple(TIMEFRAME_TO_TWELVE_DATA_INTERVAL.keys()),
            supported_symbols=supported_symbols or config.symbols,
        )
        self.config = config
        self.transport: TwelveDataTransport = transport or StandardHttpTransport(
            base_url=config.base_url,
        )

    def _resolve_exchange_tz(self, exchange: str) -> str:
        """Resolve timezone for exchange."""
        if exchange.upper() in ("NSE", "BSE"):
            return "Asia/Kolkata"
        return "UTC"

    def _format_datetime_for_query(self, timestamp: float, exchange: str) -> str:
        """Format POSIX UTC timestamp into Twelve Data date-time query format in exchange timezone."""
        tz = ZoneInfo(self._resolve_exchange_tz(exchange))
        dt = datetime.fromtimestamp(timestamp, tz=tz)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def get_historical_bars(self, request: HistoricalBarRequest) -> HistoricalBarResponse:
        """Fetch historical candlestick bars from Twelve Data /time_series API.

        Args:
            request: Validated HistoricalBarRequest specifying symbol, timeframe,
                     time range, exchange, and limit.

        Returns:
            HistoricalBarResponse: Structured response containing provider status,
            chronological typed Bar records, and diagnostics.
        """
        # 1. Validate timeframe support
        if not self.supports_timeframe(request.timeframe):
            logger.warning(
                "Unsupported timeframe '%s' for provider '%s'",
                request.timeframe,
                self.name,
            )
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.UNSUPPORTED_REQUEST,
                error_message=f"Timeframe '{request.timeframe}' is not supported by Twelve Data",
                provider_name=self.name,
            )

        interval = TIMEFRAME_TO_TWELVE_DATA_INTERVAL[request.timeframe]
        symbol = request.symbol.strip().upper()
        exchange = request.exchange.strip().upper()

        mapped_symbol = symbol
        if self.config.symbol_map and symbol in self.config.symbol_map:
            mapped_symbol = self.config.symbol_map[symbol]

        start_str = self._format_datetime_for_query(request.start_time, exchange)
        end_str = self._format_datetime_for_query(request.end_time, exchange)

        output_size = min(request.limit or 1000, 5000)

        params: Dict[str, Any] = {
            "symbol": mapped_symbol,
            "exchange": exchange,
            "interval": interval,
            "start_date": start_str,
            "end_date": end_str,
            "outputsize": output_size,
            "apikey": self.config.api_key,
        }

        logger.debug(
            "Querying Twelve Data /time_series for %s:%s [%s to %s, interval=%s]",
            exchange,
            symbol,
            start_str,
            end_str,
            interval,
        )

        status_code, body = self.transport.get(
            endpoint="time_series",
            params=params,
            timeout=self.config.timeout_seconds,
        )

        # 2. Handle HTTP/API Error Statuses
        if status_code == 401 or body.get("code") == 401:
            err_msg = body.get("message", "Invalid API key or unauthorized (HTTP 401)")
            logger.error("Twelve Data historical provider auth error: %s", err_msg)
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.PROVIDER_ERROR,
                error_message=err_msg,
                provider_name=self.name,
            )

        if status_code == 429 or body.get("code") == 429:
            err_msg = body.get("message", "Rate limit exceeded (HTTP 429)")
            logger.warning("Twelve Data historical provider rate limited: %s", err_msg)
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.RATE_LIMITED,
                error_message=err_msg,
                provider_name=self.name,
            )

        if status_code == 400 or body.get("code") == 400:
            err_msg = body.get("message", "Bad request or symbol not found")
            logger.warning("Twelve Data historical request unsupported: %s", err_msg)
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.UNSUPPORTED_REQUEST,
                error_message=err_msg,
                provider_name=self.name,
            )

        if status_code != 200 or body.get("status") == "error":
            err_msg = body.get("message", f"Provider error (HTTP {status_code})")
            logger.error("Twelve Data historical request failed: %s", err_msg)
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.PROVIDER_ERROR,
                error_message=err_msg,
                provider_name=self.name,
            )

        # 3. Parse and Validate 'values' array
        raw_values = body.get("values")
        if not raw_values or not isinstance(raw_values, list):
            logger.info(
                "No historical bars returned from Twelve Data for %s [%s to %s]",
                symbol,
                start_str,
                end_str,
            )
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.EMPTY,
                bars=(),
                provider_name=self.name,
            )

        meta = body.get("meta", {})
        meta_tz = meta.get("exchange_timezone") or self._resolve_exchange_tz(exchange)
        currency = meta.get("currency") or "INR"

        parsed_bars: List[Bar] = []
        for item in raw_values:
            try:
                dt_str = item.get("datetime")
                if not dt_str:
                    continue

                ts = parse_twelve_data_timestamp(
                    raw_ts=item.get("timestamp"),
                    raw_datetime=dt_str,
                    exchange_tz=meta_tz,
                )

                # Filter strictly within requested bounds
                if ts < request.start_time or ts > request.end_time:
                    continue

                o_val = float(item["open"])
                h_val = float(item["high"])
                l_val = float(item["low"])
                c_val = float(item["close"])
                v_val = float(item.get("volume") or 0.0)

                # Validate domain invariants
                if not (math.isfinite(o_val) and math.isfinite(h_val) and math.isfinite(l_val) and math.isfinite(c_val)):
                    continue
                if o_val < 0 or h_val < 0 or l_val < 0 or c_val < 0 or v_val < 0:
                    continue

                # Ensure high and low bounds conform to invariants
                h_val = max(h_val, o_val, c_val, l_val)
                l_val = min(l_val, o_val, c_val, h_val)

                bar = Bar(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=request.timeframe,
                    timestamp=ts,
                    open=round(o_val, 4),
                    high=round(h_val, 4),
                    low=round(l_val, 4),
                    close=round(c_val, 4),
                    volume=round(v_val, 2),
                    source="twelvedata",
                )
                parsed_bars.append(bar)
            except Exception as item_err:
                logger.debug("Skipping invalid bar in Twelve Data response: %s", item_err)
                continue

        if not parsed_bars:
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.EMPTY,
                bars=(),
                provider_name=self.name,
            )

        # Sort chronologically (oldest candle open time first)
        parsed_bars.sort(key=lambda b: b.timestamp)

        # Truncate to limit if needed
        if request.limit and len(parsed_bars) > request.limit:
            parsed_bars = parsed_bars[:request.limit]

        return HistoricalBarResponse(
            request=request,
            status=HistoricalProviderStatus.SUCCESS,
            bars=tuple(parsed_bars),
            provider_name=self.name,
        )
