"""KAISER — Twelve Data Market Feed Adapter.

Integrates Twelve Data REST/streaming API for live and delayed market data ingestion.
Adheres strictly to the MarketFeed contract, providing robust connection lifecycle
tracking, rate-limit backoff, symbol mapping, timezone normalization, and safe
provenance tagging without trading execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
import logging
import math
import time
from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

from app.core.exceptions import ConfigurationError, ValidationError
from app.market.feeds.base import MarketFeed, MarketRecord
from app.market.models import Quote

logger = logging.getLogger("market.feeds.twelvedata")


class TwelveDataFeedState(str, Enum):
    """Categorical connection and operational states for Twelve Data feed."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    DELAYED = "DELAYED"
    STALE = "STALE"
    RATE_LIMITED = "RATE_LIMITED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class TwelveDataTransport(Protocol):
    """Protocol for HTTP/REST communication with Twelve Data API."""

    def get(
        self,
        endpoint: str,
        params: Dict[str, Any],
        timeout: float,
    ) -> Tuple[int, Dict[str, Any]]:
        """Execute a GET request against Twelve Data endpoint.

        Args:
            endpoint: API path (e.g. '/quote', '/time_series').
            params: Query string parameters.
            timeout: Request timeout in seconds.

        Returns:
            Tuple[int, Dict[str, Any]]: HTTP status code and parsed JSON body.
        """
        ...


class StandardHttpTransport:
    """Production HTTP transport using Python standard library urllib."""

    def __init__(
        self,
        base_url: str = "https://api.twelvedata.com",
        user_agent: str = "KAISER-MarketFeed/1.0",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent

    def get(
        self,
        endpoint: str,
        params: Dict[str, Any],
        timeout: float,
    ) -> Tuple[int, Dict[str, Any]]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        full_url = f"{url}?{query}" if query else url

        req = urllib.request.Request(
            full_url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            },
            method="GET",
        )

        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                status_code = response.status
                body = response.read().decode("utf-8")
                try:
                    data = json.loads(body)
                except Exception as parse_err:
                    data = {"status": "error", "message": f"Malformed JSON: {parse_err}"}
                return status_code, data
        except urllib.error.HTTPError as http_err:
            body = http_err.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(body)
            except Exception:
                data = {"status": "error", "code": http_err.code, "message": str(http_err)}
            return http_err.code, data
        except urllib.error.URLError as url_err:
            return 0, {"status": "error", "code": 0, "message": f"Network error: {url_err.reason}"}
        except Exception as generic_err:
            return 0, {"status": "error", "code": 0, "message": f"Transport failure: {generic_err}"}


class MockTwelveDataTransport:
    """Deterministic in-memory mock transport for unit and integration testing."""

    def __init__(self) -> None:
        self._handlers: Dict[str, Any] = {}
        self._call_history: List[Dict[str, Any]] = []

    @property
    def call_history(self) -> List[Dict[str, Any]]:
        return list(self._call_history)

    def register_response(
        self,
        endpoint: str,
        status_code: int,
        body: Dict[str, Any],
    ) -> None:
        """Register a canned response for an endpoint."""
        self._handlers[endpoint.lstrip("/")] = (status_code, body)

    def get(
        self,
        endpoint: str,
        params: Dict[str, Any],
        timeout: float,
    ) -> Tuple[int, Dict[str, Any]]:
        clean_ep = endpoint.lstrip("/")
        self._call_history.append({"endpoint": clean_ep, "params": dict(params), "timeout": timeout})
        if clean_ep in self._handlers:
            resp = self._handlers[clean_ep]
            if callable(resp):
                return resp(params)
            return resp
        return 404, {"status": "error", "code": 404, "message": f"Mock endpoint not found: {clean_ep}"}


@dataclass(frozen=True)
class TwelveDataConfig:
    """Configuration container for Twelve Data feed and historical access."""

    api_key: str
    symbols: Tuple[str, ...] = ("RELIANCE", "TCS", "INFY")
    exchange: str = "NSE"
    base_url: str = "https://api.twelvedata.com"
    poll_interval_seconds: float = 15.0
    timeout_seconds: float = 10.0
    max_retries: int = 3
    backoff_factor: float = 2.0
    max_backoff_seconds: float = 60.0
    rate_limit_per_minute: int = 8
    is_delayed: bool = True
    freshness_threshold_seconds: float = 900.0  # 15 minutes for delayed data
    symbol_map: Optional[Dict[str, str]] = None

    def __post_init__(self) -> None:
        if not self.api_key or not isinstance(self.api_key, str) or not self.api_key.strip():
            raise ConfigurationError("TwelveDataConfig requires a non-empty api_key.")

        if not self.symbols:
            raise ConfigurationError("TwelveDataConfig requires at least one symbol.")

        cleaned_symbols = tuple(s.strip().upper() for s in self.symbols if s.strip())
        object.__setattr__(self, "symbols", cleaned_symbols)

        norm_exchange = self.exchange.strip().upper()
        if not norm_exchange:
            raise ConfigurationError("TwelveDataConfig requires a non-empty exchange.")
        object.__setattr__(self, "exchange", norm_exchange)

        if self.poll_interval_seconds <= 0:
            raise ConfigurationError("poll_interval_seconds must be positive.")

        if self.timeout_seconds <= 0:
            raise ConfigurationError("timeout_seconds must be positive.")


def parse_twelve_data_timestamp(
    raw_ts: Any,
    raw_datetime: Optional[str] = None,
    exchange_tz: str = "Asia/Kolkata",
) -> float:
    """Parse Twelve Data timestamp or datetime string into a POSIX UTC float."""
    # 1. Prefer explicit POSIX timestamp if available
    if raw_ts is not None and isinstance(raw_ts, (int, float)):
        ts = float(raw_ts)
        if ts > 4102444800:
            ts /= 1000.0
        if ts > 0 and math.isfinite(ts):
            return ts

    # 2. Parse datetime string
    if raw_datetime and isinstance(raw_datetime, str):
        cleaned = raw_datetime.strip()
        if cleaned:
            # Handle ISO with Z or offset
            try:
                iso_str = cleaned[:-1] + "+00:00" if cleaned.endswith("Z") else cleaned
                dt = datetime.fromisoformat(iso_str)
                if dt.tzinfo is None:
                    # Apply exchange timezone
                    tz = ZoneInfo(exchange_tz)
                    dt = dt.replace(tzinfo=tz)
                return dt.timestamp()
            except Exception:
                pass

            # Handle format "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(cleaned, fmt)
                    tz = ZoneInfo(exchange_tz)
                    dt = dt.replace(tzinfo=tz)
                    return dt.timestamp()
                except Exception:
                    continue

    raise ValidationError(
        f"Unable to parse Twelve Data timestamp (raw_ts={raw_ts}, raw_datetime={raw_datetime})"
    )


class TwelveDataMarketFeed(MarketFeed):
    """Production market data feed integrating Twelve Data API.

    Adheres strictly to the MarketFeed contract. Cycles through configured
    symbols, enforces rate-limiting backoff, parses live/delayed quote payloads,
    normalizes timezone timestamps, and tracks feed operational health.
    """

    def __init__(
        self,
        config: TwelveDataConfig,
        transport: Optional[TwelveDataTransport] = None,
        name: str = "twelve_data_feed",
        auto_start: bool = True,
    ) -> None:
        super().__init__(name=name, source="twelvedata")
        self.config = config
        self.transport: TwelveDataTransport = transport or StandardHttpTransport(
            base_url=config.base_url,
        )
        self._state: TwelveDataFeedState = TwelveDataFeedState.DISCONNECTED
        self._symbol_index: int = 0
        self._error_count: int = 0
        self._consecutive_rate_limits: int = 0
        self._backoff_until: float = 0.0
        self._last_poll_time: Optional[float] = None
        self._last_quote_time: Optional[float] = None
        self._last_error: Optional[str] = None
        self._cached_quotes: Dict[str, Quote] = {}

        if auto_start:
            self.start()

    @property
    def state(self) -> TwelveDataFeedState:
        """Return the current categorical feed state."""
        return self._state

    @property
    def error_count(self) -> int:
        """Return cumulative error count."""
        return self._error_count

    @property
    def is_rate_limited(self) -> bool:
        """Return whether feed is currently within a rate-limit backoff window."""
        return time.time() < self._backoff_until

    def start(self) -> None:
        """Start the feed and transition state to CONNECTING."""
        super().start()
        self._state = TwelveDataFeedState.CONNECTING
        logger.info(
            "Twelve Data feed '%s' started [exchange=%s, symbols=%s, delayed=%s]",
            self.name,
            self.config.exchange,
            self.config.symbols,
            self.config.is_delayed,
        )

    def stop(self) -> None:
        """Stop the feed and transition state to STOPPED."""
        super().stop()
        self._state = TwelveDataFeedState.STOPPED
        logger.info("Twelve Data feed '%s' stopped.", self.name)

    def reset(self) -> None:
        """Reset feed cursor, error metrics, and backoff timers."""
        self._symbol_index = 0
        self._records_emitted = 0
        self._error_count = 0
        self._consecutive_rate_limits = 0
        self._backoff_until = 0.0
        self._last_poll_time = None
        self._last_quote_time = None
        self._last_error = None
        self._cached_quotes.clear()
        if self._is_running:
            self._state = TwelveDataFeedState.CONNECTING
        else:
            self._state = TwelveDataFeedState.DISCONNECTED
        logger.debug("Twelve Data feed '%s' reset.", self.name)

    def _map_symbol(self, symbol: str) -> str:
        """Map internal symbol to Twelve Data symbol query format."""
        if self.config.symbol_map and symbol in self.config.symbol_map:
            return self.config.symbol_map[symbol]
        return symbol

    def _resolve_exchange_tz(self) -> str:
        """Determine exchange timezone for datetime parsing."""
        if self.config.exchange in ("NSE", "BSE"):
            return "Asia/Kolkata"
        return "UTC"

    def next_record(self) -> Optional[MarketRecord]:
        """Fetch the next market quote from Twelve Data.

        Returns:
            Optional[MarketRecord]: Normalized Quote model or None if stopped,
            in rate-limit backoff, or temporary network failure.
        """
        if not self._is_running:
            return None

        now = time.time()
        # Enforce rate-limit backoff if active
        if now < self._backoff_until:
            logger.debug(
                "Twelve Data feed is in backoff until %.1fs (%.1fs remaining)",
                self._backoff_until,
                self._backoff_until - now,
            )
            return None

        # Select symbol in round-robin sequence
        symbol = self.config.symbols[self._symbol_index % len(self.config.symbols)]
        mapped_symbol = self._map_symbol(symbol)

        params = {
            "symbol": mapped_symbol,
            "exchange": self.config.exchange,
            "apikey": self.config.api_key,
        }

        self._last_poll_time = now
        status_code, body = self.transport.get(
            endpoint="quote",
            params=params,
            timeout=self.config.timeout_seconds,
        )

        # 1. Handle HTTP 401 Unauthorized / Invalid API Key
        if status_code == 401 or body.get("code") == 401:
            self._state = TwelveDataFeedState.FAILED
            self._last_error = "Invalid API key or unauthorized (HTTP 401)"
            self._error_count += 1
            logger.error("Twelve Data authentication failed: %s", self._last_error)
            return None

        # 2. Handle HTTP 429 Rate Limit Exceeded
        if status_code == 429 or body.get("code") == 429:
            self._state = TwelveDataFeedState.RATE_LIMITED
            self._consecutive_rate_limits += 1
            backoff_duration = min(
                self.config.max_backoff_seconds,
                (self.config.backoff_factor ** self._consecutive_rate_limits) * 2.0,
            )
            self._backoff_until = now + backoff_duration
            self._last_error = f"Rate limited (code 429). Backing off for {backoff_duration:.1f}s"
            self._error_count += 1
            logger.warning("Twelve Data rate limit reached: %s", self._last_error)
            return None

        # 3. Handle Other API Errors (e.g. 400 Bad Request, 404 Symbol Not Found)
        if status_code != 200 or body.get("status") == "error":
            err_msg = body.get("message", f"HTTP {status_code}")
            self._state = TwelveDataFeedState.DISCONNECTED
            self._last_error = f"API error for {symbol}: {err_msg}"
            self._error_count += 1
            # Apply minor pause on error before retrying next symbol
            self._backoff_until = now + 1.0
            logger.warning("Twelve Data quote request failed: %s", self._last_error)
            self._symbol_index += 1
            return None

        # 4. Parse and Normalize Successful Quote Payload
        try:
            quote = self._normalize_quote_payload(body, requested_symbol=symbol)
        except Exception as norm_err:
            self._state = TwelveDataFeedState.DISCONNECTED
            self._last_error = f"Normalization failure for {symbol}: {norm_err}"
            self._error_count += 1
            logger.error("Twelve Data quote payload invalid: %s", self._last_error)
            self._symbol_index += 1
            return None

        # Successful record parsing: reset consecutive rate limits and update state
        self._consecutive_rate_limits = 0
        self._last_quote_time = quote.timestamp
        self._cached_quotes[quote.symbol] = quote
        self._records_emitted += 1
        self._symbol_index += 1

        # Check freshness to determine whether state is DELAYED, STALE, or CONNECTED
        age = max(0.0, now - quote.timestamp)
        if age > self.config.freshness_threshold_seconds:
            self._state = TwelveDataFeedState.STALE
        elif self.config.is_delayed:
            self._state = TwelveDataFeedState.DELAYED
        else:
            self._state = TwelveDataFeedState.CONNECTED

        return quote

    def _normalize_quote_payload(
        self,
        body: Dict[str, Any],
        requested_symbol: str,
    ) -> Quote:
        """Normalize raw Twelve Data /quote JSON payload into Quote domain model."""
        resp_symbol = body.get("symbol") or requested_symbol
        resp_exchange = body.get("exchange") or self.config.exchange

        # Parse prices
        raw_price = body.get("close") or body.get("last_price") or body.get("price")
        if raw_price is None:
            raise ValidationError(f"Missing last_price in Twelve Data quote: {body}")
        last_price = float(raw_price)

        # Parse bid/ask if provided by provider
        raw_bid = body.get("bid")
        bid = float(raw_bid) if raw_bid is not None else None

        raw_ask = body.get("ask")
        ask = float(raw_ask) if raw_ask is not None else None

        # Parse volume
        raw_volume = body.get("volume") or 0.0
        volume = float(raw_volume)

        # Parse currency
        currency = body.get("currency") or "INR"

        # Timestamp normalization
        raw_ts = body.get("timestamp")
        raw_datetime = body.get("datetime")
        ts = parse_twelve_data_timestamp(
            raw_ts=raw_ts,
            raw_datetime=raw_datetime,
            exchange_tz=self._resolve_exchange_tz(),
        )

        return Quote(
            symbol=resp_symbol,
            exchange=resp_exchange,
            timestamp=ts,
            last_price=last_price,
            bid=bid,
            ask=ask,
            volume=volume,
            currency=currency,
            source="twelvedata",
        )

    def get_status(self) -> Dict[str, Any]:
        """Return diagnostic status metadata without leaking secrets."""
        base_status = super().get_status()
        base_status.update({
            "feed_state": self._state.value,
            "exchange": self.config.exchange,
            "symbols": list(self.config.symbols),
            "is_delayed": self.config.is_delayed,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "last_poll_time": self._last_poll_time,
            "last_quote_time": self._last_quote_time,
            "is_rate_limited": self.is_rate_limited,
            "backoff_until": self._backoff_until if self.is_rate_limited else None,
            "cached_symbols": list(self._cached_quotes.keys()),
        })
        return base_status
