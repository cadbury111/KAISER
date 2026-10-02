"""KAISER — Historical Market Data Bounded LRU Cache.

Provides deterministic, thread-safe caching of historical bar queries with
configurable LRU eviction, TTL expiration, stream-level invalidation, and observable telemetry.
"""

from __future__ import annotations

import collections
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.analysis.models import normalize_timeframe
from app.market.models import Bar


CacheKey = Tuple[str, str, float, float, int]


def make_cache_key(
    symbol: str,
    timeframe: str,
    start_time: float,
    end_time: float,
    limit: int = 1000,
) -> CacheKey:
    """Construct a deterministic cache key for historical queries."""
    norm_sym = symbol.strip().upper()
    norm_tf = normalize_timeframe(timeframe)
    return (norm_sym, norm_tf, round(float(start_time), 3), round(float(end_time), 3), int(limit))


class HistoricalDataCache:
    """Thread-safe, bounded LRU cache for historical market bars."""

    def __init__(
        self,
        max_size: int = 200,
        ttl_seconds: Optional[float] = 300.0,
    ) -> None:
        self.max_size = max(1, max_size)
        self.ttl_seconds = ttl_seconds
        self._lock = threading.RLock()
        # Maps CacheKey -> (created_at_timestamp, tuple_of_bars)
        self._entries: collections.OrderedDict[CacheKey, Tuple[float, Tuple[Bar, ...]]] = (
            collections.OrderedDict()
        )

        # Operational metrics
        self._hits: int = 0
        self._misses: int = 0
        self._evictions: int = 0
        self._invalidations: int = 0

    @property
    def size(self) -> int:
        """Current number of cached query entries."""
        with self._lock:
            return len(self._entries)

    @property
    def hits(self) -> int:
        """Count of successful cache hits."""
        with self._lock:
            return self._hits

    @property
    def misses(self) -> int:
        """Count of cache misses."""
        with self._lock:
            return self._misses

    @property
    def evictions(self) -> int:
        """Count of items evicted due to capacity bounds."""
        with self._lock:
            return self._evictions

    @property
    def invalidations(self) -> int:
        """Count of items invalidated due to new data writes."""
        with self._lock:
            return self._invalidations

    @property
    def hit_rate(self) -> float:
        """Cache hit ratio [0.0, 1.0]."""
        with self._lock:
            total = self._hits + self._misses
            return round(self._hits / total, 4) if total > 0 else 0.0

    def get(self, key: CacheKey) -> Optional[Tuple[Bar, ...]]:
        """Retrieve cached bars if present and not expired."""
        with self._lock:
            if key not in self._entries:
                self._misses += 1
                return None

            created_at, bars = self._entries[key]
            now = time.time()

            # Check TTL expiry
            if self.ttl_seconds is not None and (now - created_at) > self.ttl_seconds:
                del self._entries[key]
                self._misses += 1
                return None

            # Mark as recently used (move to end)
            self._entries.move_to_end(key)
            self._hits += 1
            return bars

    def put(
        self,
        key: CacheKey,
        bars: Sequence[Bar],
        is_complete: bool = True,
    ) -> None:
        """Store query results in cache with LRU eviction.

        Only complete, non-corrupt datasets should be cached.
        """
        if not is_complete:
            return

        with self._lock:
            now = time.time()
            if key in self._entries:
                self._entries[key] = (now, tuple(bars))
                self._entries.move_to_end(key)
                return

            # Check bounded capacity
            if len(self._entries) >= self.max_size:
                self._entries.popitem(last=False)
                self._evictions += 1

            self._entries[key] = (now, tuple(bars))

    def invalidate_stream(
        self,
        symbol: str,
        timeframe: Optional[str] = None,
    ) -> int:
        """Invalidate all cached queries for a given symbol and optional timeframe.

        Called whenever new bars are persisted to maintain consistency.
        """
        norm_sym = symbol.strip().upper()
        norm_tf = normalize_timeframe(timeframe) if timeframe else None

        with self._lock:
            keys_to_remove: List[CacheKey] = []
            for k in self._entries:
                k_sym, k_tf, _, _, _ = k
                if k_sym == norm_sym and (norm_tf is None or k_tf == norm_tf):
                    keys_to_remove.append(k)

            for k in keys_to_remove:
                del self._entries[k]
                self._invalidations += 1

            return len(keys_to_remove)

    def clear(self) -> None:
        """Clear all entries and reset operational metrics."""
        with self._lock:
            self._entries.clear()

    def get_metrics(self) -> Dict[str, Any]:
        """Return diagnostic metrics snapshot."""
        with self._lock:
            return {
                "size": len(self._entries),
                "max_size": self.max_size,
                "ttl_seconds": self.ttl_seconds,
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "invalidations": self._invalidations,
                "hit_rate": self.hit_rate,
            }
