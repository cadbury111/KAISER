"""KAISER — Deterministic Mock Historical Market Data Provider.

Provides an offline, deterministic, zero-network implementation of HistoricalDataProvider
for local development, CI testing, and scenario simulation. Supports realistic
synthetic candlestick generation and controllable fault injection.
"""

from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.analysis.models import SUPPORTED_TIMEFRAMES, TIMEFRAME_SECONDS, normalize_timeframe
from app.core.exceptions import ValidationError
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalProviderStatus,
)
from app.market.historical.provider import HistoricalDataProvider
from app.market.models import Bar


def get_base_price_for_symbol(symbol: str) -> float:
    """Deterministic base price generator seeded by symbol ticker."""
    known_prices = {
        "NIFTY": 24000.0,
        "BANKNIFTY": 51000.0,
        "RELIANCE": 2500.0,
        "TCS": 3900.0,
        "INFY": 1800.0,
        "HDFCBANK": 1650.0,
    }
    sym = symbol.strip().upper()
    if sym in known_prices:
        return known_prices[sym]
    # Deterministic fallback based on char ordinals
    hash_val = sum(ord(c) for c in sym)
    return float(500 + (hash_val % 2000))


class MockHistoricalDataProvider(HistoricalDataProvider):
    """Deterministic, configurable mock historical data provider.

    Attributes:
        name: Provider name identifier.
        supported_timeframes: Supported timeframe strings.
        supported_symbols: Optional set of allowed symbols. None means all.
    """

    def __init__(
        self,
        name: str = "mock_historical_provider",
        supported_timeframes: Optional[Tuple[str, ...]] = None,
        supported_symbols: Optional[Tuple[str, ...]] = None,
    ) -> None:
        super().__init__(
            name=name,
            supported_timeframes=supported_timeframes,
            supported_symbols=supported_symbols,
        )
        # Canned data store: key is (symbol, timeframe) -> List[Bar]
        self._canned_bars: Dict[Tuple[str, str], List[Bar]] = {}

        # Controllable fault injection switches
        self.force_status: Optional[HistoricalProviderStatus] = None
        self.force_error_message: Optional[str] = None
        self.force_empty: bool = False
        self.simulate_pagination: bool = False
        self.page_size: int = 50

        # Fault injection indices
        self.inject_zero_volume_indices: Set[int] = set()
        self.inject_gap_after_indices: Set[int] = set()  # skips 2 intervals after this index
        self.inject_duplicate_indices: Set[int] = set()  # duplicates bar at this index
        self.inject_out_of_order: bool = False  # reverses bar order
        self.inject_raw_corrupt: bool = False  # includes invalid raw bars

    def set_canned_bars(self, symbol: str, timeframe: str, bars: Sequence[Bar]) -> None:
        """Register explicit pre-canned bars for deterministic tests."""
        canon_tf = normalize_timeframe(timeframe)
        key = (symbol.strip().upper(), canon_tf)
        self._canned_bars[key] = list(bars)

    def clear_canned_bars(self) -> None:
        """Clear all registered canned bars."""
        self._canned_bars.clear()

    def reset_faults(self) -> None:
        """Reset all fault injection switches to healthy defaults."""
        self.force_status = None
        self.force_error_message = None
        self.force_empty = False
        self.simulate_pagination = False
        self.page_size = 50
        self.inject_zero_volume_indices.clear()
        self.inject_gap_after_indices.clear()
        self.inject_duplicate_indices.clear()
        self.inject_out_of_order = False
        self.inject_raw_corrupt = False

    def get_historical_bars(self, request: HistoricalBarRequest) -> HistoricalBarResponse:
        """Retrieve synthetic or canned historical bars matching the request."""
        # 1. Check Symbol Support
        if not self.supports_symbol(request.symbol):
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.UNSUPPORTED_REQUEST,
                bars=(),
                error_message=f"Symbol '{request.symbol}' is not supported by {self.name}",
                provider_name=self.name,
            )

        # 2. Check Timeframe Support
        if not self.supports_timeframe(request.timeframe):
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.UNSUPPORTED_REQUEST,
                bars=(),
                error_message=f"Timeframe '{request.timeframe}' is not supported by {self.name}",
                provider_name=self.name,
            )

        # 3. Check Forced Status Fault Injection
        if self.force_status is not None:
            return HistoricalBarResponse(
                request=request,
                status=self.force_status,
                bars=(),
                error_message=self.force_error_message or f"Forced status {self.force_status.value}",
                provider_name=self.name,
            )

        # 4. Check Forced Empty Fault Injection
        if self.force_empty:
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.EMPTY,
                bars=(),
                provider_name=self.name,
            )

        # 5. Fetch Canned Bars if Available
        key = (request.symbol, request.timeframe)
        if key in self._canned_bars:
            all_canned = self._canned_bars[key]
            # Filter by time range
            filtered = [
                b for b in all_canned
                if request.start_time <= b.timestamp <= request.end_time
            ]
            return self._build_paginated_response(request, filtered)

        # 6. Generate Synthetic Deterministic Bars
        bars = self._generate_synthetic_bars(request)

        if not bars:
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.EMPTY,
                bars=(),
                provider_name=self.name,
            )

        return self._build_paginated_response(request, bars)

    def _generate_synthetic_bars(self, request: HistoricalBarRequest) -> List[Bar]:
        """Generate deterministic, mathematically continuous OHLCV candles."""
        interval = TIMEFRAME_SECONDS.get(request.timeframe, 60)
        base_price = get_base_price_for_symbol(request.symbol)

        # Determine start aligned to interval
        curr_time = float(request.start_time)
        bars: List[Bar] = []
        bar_idx = 0
        limit = 5000 if self.simulate_pagination else (request.limit or 1000)

        while curr_time <= request.end_time and len(bars) < limit:
            # Check gap injection
            if bar_idx in self.inject_gap_after_indices:
                curr_time += interval * 3.0  # Skip intervals to produce gap
                bar_idx += 1
                continue

            # Deterministic wave generation
            phase = bar_idx * 0.15
            drift = 0.0001 * bar_idx
            mid = base_price * (1.0 + 0.015 * math.sin(phase) + 0.008 * math.cos(phase * 0.5) + drift)

            open_p = round(mid, 2)
            close_p = round(mid + (0.003 * math.sin(phase * 1.5) * base_price), 2)
            high_p = round(max(open_p, close_p) + abs(0.002 * base_price), 2)
            low_p = round(min(open_p, close_p) - abs(0.002 * base_price), 2)

            # Volume calculation
            if bar_idx in self.inject_zero_volume_indices:
                vol = 0.0
            else:
                vol = round(1000.0 + 350.0 * math.sin(phase), 1)

            bar = Bar(
                symbol=request.symbol,
                exchange=request.exchange,
                timeframe=request.timeframe,
                timestamp=curr_time,
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                volume=vol,
                source=f"{self.name}",
            )
            bars.append(bar)

            # Check duplicate injection
            if bar_idx in self.inject_duplicate_indices:
                # Add duplicate with exact same timestamp
                bars.append(bar)

            curr_time += interval
            bar_idx += 1

        if self.inject_out_of_order and len(bars) > 1:
            bars.reverse()

        return bars

    def _build_paginated_response(
        self,
        request: HistoricalBarRequest,
        bars: List[Bar],
    ) -> HistoricalBarResponse:
        """Handle optional pagination chunking based on page_token and page_size."""
        if not self.simulate_pagination:
            return HistoricalBarResponse(
                request=request,
                status=HistoricalProviderStatus.SUCCESS,
                bars=tuple(bars),
                next_page_token=None,
                provider_name=self.name,
            )

        # Parse page token offset
        offset = 0
        if request.page_token and request.page_token.startswith("offset_"):
            try:
                offset = int(request.page_token.replace("offset_", ""))
            except ValueError:
                offset = 0

        page_limit = min(self.page_size, request.limit or self.page_size)
        page_bars = bars[offset : offset + page_limit]
        next_offset = offset + len(page_bars)
        next_token = f"offset_{next_offset}" if next_offset < len(bars) else None

        status = HistoricalProviderStatus.SUCCESS
        if next_token and not page_bars:
            status = HistoricalProviderStatus.EMPTY
        elif next_token:
            status = HistoricalProviderStatus.PARTIAL

        return HistoricalBarResponse(
            request=request,
            status=status,
            bars=tuple(page_bars),
            next_page_token=next_token,
            provider_name=self.name,
        )
