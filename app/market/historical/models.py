"""KAISER — Historical Market Data Models.

Provides strongly-typed, immutable data structures for historical OHLCV bar
requests, provider responses, ingestion tracking, and status representations.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from app.analysis.models import SUPPORTED_TIMEFRAMES, normalize_timeframe
from app.core.exceptions import ValidationError
from app.market.models import Bar


class HistoricalProviderStatus(str, Enum):
    """Categorical outcome of a historical data provider query."""

    SUCCESS = "SUCCESS"
    EMPTY = "EMPTY"
    PARTIAL = "PARTIAL"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    UNSUPPORTED_REQUEST = "UNSUPPORTED_REQUEST"
    RATE_LIMITED = "RATE_LIMITED"


@dataclass(frozen=True)
class HistoricalBarRequest:
    """Immutable request specification for querying historical OHLCV bars.

    Attributes:
        symbol: Ticker symbol (e.g. 'NIFTY', 'RELIANCE').
        timeframe: Bar timeframe ('1m', '5m', '15m', '1h', '1D').
        start_time: Start POSIX UTC timestamp (inclusive).
        end_time: End POSIX UTC timestamp (inclusive).
        limit: Maximum number of bars requested (bounded 1 to 5000).
        page_token: Pagination cursor token for subsequent pages.
        exchange: Exchange identifier (e.g. 'NSE', 'BSE').
    """

    symbol: str
    timeframe: str
    start_time: float
    end_time: float
    limit: Optional[int] = 1000
    page_token: Optional[str] = None
    exchange: str = "NSE"

    def __post_init__(self) -> None:
        """Validate request invariants upon instantiation."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("HistoricalBarRequest symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not self.exchange or not isinstance(self.exchange, str) or not self.exchange.strip():
            raise ValidationError("HistoricalBarRequest exchange must be a non-empty string.")
        object.__setattr__(self, "exchange", self.exchange.strip().upper())

        # Canonicalize timeframe against supported set ('1m', '5m', '15m', '1h', '1D')
        canon_tf = normalize_timeframe(self.timeframe)
        object.__setattr__(self, "timeframe", canon_tf)

        if self.start_time <= 0:
            raise ValidationError(f"HistoricalBarRequest start_time must be positive, got {self.start_time}")

        if self.end_time <= 0:
            raise ValidationError(f"HistoricalBarRequest end_time must be positive, got {self.end_time}")

        if self.end_time < self.start_time:
            raise ValidationError(
                f"HistoricalBarRequest end_time ({self.end_time}) cannot be earlier than start_time ({self.start_time})"
            )

        if self.limit is not None:
            if not isinstance(self.limit, int) or self.limit <= 0:
                raise ValidationError(f"HistoricalBarRequest limit must be a positive integer, got {self.limit}")
            if self.limit > 5000:
                raise ValidationError(f"HistoricalBarRequest limit exceeds maximum bound of 5000, got {self.limit}")

    @property
    def start_iso(self) -> str:
        """ISO-8601 UTC representation of start timestamp."""
        return datetime.fromtimestamp(self.start_time, tz=timezone.utc).isoformat()

    @property
    def end_iso(self) -> str:
        """ISO-8601 UTC representation of end timestamp."""
        return datetime.fromtimestamp(self.end_time, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert request to dictionary."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timeframe": self.timeframe,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "start_iso": self.start_iso,
            "end_iso": self.end_iso,
            "limit": self.limit,
            "page_token": self.page_token,
        }


@dataclass(frozen=True)
class HistoricalBarResponse:
    """Immutable response payload returned by a historical data provider.

    Attributes:
        request: The originating request specification.
        status: Status of the retrieval outcome.
        bars: Tuple of typed, normalized Bar records.
        next_page_token: Pagination cursor for additional records if available.
        error_message: Diagnostic error description if status is not SUCCESS.
        provider_name: Identifier of the responding provider.
        response_timestamp: POSIX UTC timestamp when response was generated.
    """

    request: HistoricalBarRequest
    status: HistoricalProviderStatus
    bars: Tuple[Bar, ...] = ()
    next_page_token: Optional[str] = None
    error_message: Optional[str] = None
    provider_name: str = "mock"
    response_timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        """Ensure bars is an immutable tuple."""
        if not isinstance(self.bars, tuple):
            object.__setattr__(self, "bars", tuple(self.bars or ()))

    @property
    def is_empty(self) -> bool:
        """True if no bars were returned."""
        return len(self.bars) == 0

    @property
    def total_bars(self) -> int:
        """Number of bars returned in this response batch."""
        return len(self.bars)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize response to dictionary."""
        return {
            "request": self.request.to_dict(),
            "status": self.status.value,
            "total_bars": self.total_bars,
            "has_next_page": bool(self.next_page_token),
            "next_page_token": self.next_page_token,
            "error_message": self.error_message,
            "provider_name": self.provider_name,
            "response_timestamp": self.response_timestamp,
        }


@dataclass(frozen=True)
class HistoricalIngestionResult:
    """Structured audit outcome of a historical data ingestion cycle.

    Tracks requested range, provider status, count of received/accepted/rejected
    bars, duplicate rejections, zero-volume bars, and detected missing gaps.
    """

    request: HistoricalBarRequest
    provider_status: HistoricalProviderStatus
    received_bars_count: int
    accepted_bars_count: int
    rejected_bars_count: int
    duplicate_bars_count: int
    zero_volume_bars_count: int
    detected_gaps_count: int
    gap_details: Tuple[Dict[str, Any], ...] = ()
    rejection_reasons: Tuple[str, ...] = ()
    first_bar_timestamp: Optional[float] = None
    last_bar_timestamp: Optional[float] = None
    correlation_id: str = ""
    execution_time_ms: float = 0.0

    def __post_init__(self) -> None:
        """Ensure tuples for immutable sequence attributes."""
        object.__setattr__(self, "gap_details", tuple(self.gap_details or ()))
        object.__setattr__(self, "rejection_reasons", tuple(self.rejection_reasons or ()))

    @property
    def is_fully_accepted(self) -> bool:
        """True if all received bars were accepted without rejections."""
        return self.received_bars_count > 0 and self.rejected_bars_count == 0 and self.duplicate_bars_count == 0

    @property
    def summary_text(self) -> str:
        """Human-readable ingestion summary string."""
        return (
            f"Historical ingestion for {self.request.symbol} ({self.request.timeframe}): "
            f"Status={self.provider_status.value}, "
            f"Received={self.received_bars_count}, "
            f"Accepted={self.accepted_bars_count}, "
            f"Rejected={self.rejected_bars_count}, "
            f"Duplicates={self.duplicate_bars_count}, "
            f"Zero-Volume={self.zero_volume_bars_count}, "
            f"Gaps={self.detected_gaps_count}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize result to dictionary."""
        return {
            "symbol": self.request.symbol,
            "timeframe": self.request.timeframe,
            "provider_status": self.provider_status.value,
            "received_bars_count": self.received_bars_count,
            "accepted_bars_count": self.accepted_bars_count,
            "rejected_bars_count": self.rejected_bars_count,
            "duplicate_bars_count": self.duplicate_bars_count,
            "zero_volume_bars_count": self.zero_volume_bars_count,
            "detected_gaps_count": self.detected_gaps_count,
            "gap_details": list(self.gap_details),
            "rejection_reasons": list(self.rejection_reasons),
            "first_bar_timestamp": self.first_bar_timestamp,
            "last_bar_timestamp": self.last_bar_timestamp,
            "correlation_id": self.correlation_id,
            "execution_time_ms": round(self.execution_time_ms, 2),
            "summary": self.summary_text,
        }
