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
        self.timeframe = timeframe.strip()
        self.max_capacity = max_capacity

        self._lock = threading.RLock()
        self._bars: collections.deque[Bar] = collections.deque(maxlen=max_capacity)
        self._last_timestamp: Optional[float] = None

    def add_bar(self, bar: Bar) -> bool:
        """Append a new Bar to the series if it matches symbol, timeframe, and timestamp order.

        Args:
            bar: The Bar instance to add.

        Returns:
            bool: True if bar was accepted and added, False if rejected (wrong stream,
                  duplicate, or non-increasing timestamp).
        """
        with self._lock:
            if bar.symbol.upper() != self.symbol or bar.timeframe != self.timeframe:
                return False

            if self._last_timestamp is not None and bar.timestamp <= self._last_timestamp:
                # Reject duplicate or out-of-order bar
                return False

            self._bars.append(bar)
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
            return [b.close for b in self._bars]

    def get_highs(self) -> List[float]:
        """Extract high prices sequence."""
        with self._lock:
            return [b.high for b in self._bars]

    def get_lows(self) -> List[float]:
        """Extract low prices sequence."""
        with self._lock:
            return [b.low for b in self._bars]

    def get_opens(self) -> List[float]:
        """Extract open prices sequence."""
        with self._lock:
            return [b.open for b in self._bars]

    def get_volumes(self) -> List[float]:
        """Extract volumes sequence."""
        with self._lock:
            return [b.volume for b in self._bars]

    def clear(self) -> None:
        """Clear all stored bars and reset timestamp tracking."""
        with self._lock:
            self._bars.clear()
            self._last_timestamp = None
