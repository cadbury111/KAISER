"""KAISER — Bounded Rolling Bar Series.

Maintains an in-memory, thread-safe, bounded rolling window of OHLCV Bar records
for a specific (symbol, timeframe) stream to power indicator calculations.
"""

from __future__ import annotations

import collections
import threading
from typing import List, Optional, Tuple

from app.core.exceptions import ValidationError
from app.market.models import Bar


class RollingBarSeries:
    """Thread-safe, bounded rolling window container for historical Bar records.

    Tracks duplicate bars, out-of-order sequences, and detected missing intervals (gaps)
    without ever fabricating synthetic candles.

    Attributes:
        symbol: Ticker symbol.
        timeframe: Bar timeframe.
        max_capacity: Maximum number of bars retained in memory.
    """

    def __init__(self, symbol: str, timeframe: str, max_capacity: int = 500) -> None:
        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("RollingBarSeries symbol must be a non-empty string.")
        if not timeframe or not isinstance(timeframe, str) or not timeframe.strip():
            raise ValidationError("RollingBarSeries timeframe must be a non-empty string.")
        if max_capacity < 20:
            raise ValidationError(f"max_capacity must be at least 20, got {max_capacity}")

        self.symbol = symbol.strip().upper()
        # Normalization with fallback for non-standard timeframes
        from app.analysis.models import TIMEFRAME_SECONDS, normalize_timeframe
        try:
            self.timeframe = normalize_timeframe(timeframe)
        except ValidationError:
            self.timeframe = timeframe.strip()

        self.max_capacity = max_capacity
        self._interval_seconds = TIMEFRAME_SECONDS.get(self.timeframe)

        self._lock = threading.RLock()
        self._bars: collections.deque[Bar] = collections.deque(maxlen=max_capacity)
        self._closes: collections.deque[float] = collections.deque(maxlen=max_capacity)
        self._highs: collections.deque[float] = collections.deque(maxlen=max_capacity)
        self._lows: collections.deque[float] = collections.deque(maxlen=max_capacity)
        self._opens: collections.deque[float] = collections.deque(maxlen=max_capacity)
        self._volumes: collections.deque[float] = collections.deque(maxlen=max_capacity)
        self._last_timestamp: Optional[float] = None

        # Data Quality & Synchronization Tracking
        self._duplicate_count: int = 0
        self._out_of_order_count: int = 0
        self._missing_intervals_count: int = 0
        self._gap_records: List[Tuple[float, float, int]] = []

    def add_bar(self, bar: Bar) -> bool:
        """Append a new Bar to the series if it matches symbol, timeframe, and timestamp order.

        Args:
            bar: The Bar instance to add.

        Returns:
            bool: True if bar was accepted and added, False if rejected (wrong stream,
                  duplicate, or non-increasing timestamp).
        """
        with self._lock:
            # Check symbol and timeframe isolation
            bar_sym = bar.symbol.strip().upper()
            bar_tf = bar.timeframe.strip()
            if bar_sym != self.symbol or (bar_tf != self.timeframe and bar_tf.upper() != self.timeframe.upper()):
                return False

            if self._last_timestamp is not None:
                if bar.timestamp == self._last_timestamp:
                    self._duplicate_count += 1
                    return False
                elif bar.timestamp < self._last_timestamp:
                    self._out_of_order_count += 1
                    return False

                # Chronological advance: check for missing intervals (gaps)
                if self._interval_seconds is not None:
                    elapsed = bar.timestamp - self._last_timestamp
                    if elapsed > self._interval_seconds * 1.5:
                        missed_bars = max(1, int(round(elapsed / self._interval_seconds)) - 1)
                        self._missing_intervals_count += missed_bars
                        self._gap_records.append((self._last_timestamp, bar.timestamp, missed_bars))

            self._bars.append(bar)
            self._closes.append(bar.close)
            self._highs.append(bar.high)
            self._lows.append(bar.low)
            self._opens.append(bar.open)
            self._volumes.append(bar.volume)
            self._last_timestamp = bar.timestamp
            return True

    @property
    def count(self) -> int:
        """Return current number of bars stored."""
        with self._lock:
            return len(self._bars)

    @property
    def latest_bar(self) -> Optional[Bar]:
        """Return the most recently added bar, or None if empty."""
        with self._lock:
            return self._bars[-1] if self._bars else None

    def get_bars(self) -> List[Bar]:
        """Return an immutable snapshot copy of all bars in chronological order."""
        with self._lock:
            return list(self._bars)

    def get_closes(self) -> List[float]:
        """Extract closing prices sequence."""
        with self._lock:
            return list(self._closes)

    def get_highs(self) -> List[float]:
        """Extract high prices sequence."""
        with self._lock:
            return list(self._highs)

    def get_lows(self) -> List[float]:
        """Extract low prices sequence."""
        with self._lock:
            return list(self._lows)

    def get_opens(self) -> List[float]:
        """Extract open prices sequence."""
        with self._lock:
            return list(self._opens)

    def get_volumes(self) -> List[float]:
        """Extract volumes sequence."""
        with self._lock:
            return list(self._volumes)

    @property
    def missing_intervals_count(self) -> int:
        """Return total detected missing bar intervals (gaps)."""
        with self._lock:
            return self._missing_intervals_count

    @property
    def has_gaps(self) -> bool:
        """True if any missing bar intervals were detected in chronological history."""
        with self._lock:
            return self._missing_intervals_count > 0

    @property
    def gap_records(self) -> List[Tuple[float, float, int]]:
        """Return snapshot of recorded gaps (start_ts, end_ts, missing_count)."""
        with self._lock:
            return list(self._gap_records)

    @property
    def duplicate_count(self) -> int:
        """Return count of rejected duplicate bars."""
        with self._lock:
            return self._duplicate_count

    @property
    def out_of_order_count(self) -> int:
        """Return count of rejected out-of-order bars."""
        with self._lock:
            return self._out_of_order_count

    def clear(self) -> None:
        """Clear all stored bars and reset timestamp and quality tracking."""
        with self._lock:
            self._bars.clear()
            self._closes.clear()
            self._highs.clear()
            self._lows.clear()
            self._opens.clear()
            self._volumes.clear()
            self._last_timestamp = None
            self._duplicate_count = 0
            self._out_of_order_count = 0
            self._missing_intervals_count = 0
            self._gap_records.clear()

