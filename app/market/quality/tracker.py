"""KAISER — Market Data Source Health Tracking.

Maintains operational metrics for each market data feed source (records received,
accepted, rejected, duplicates, stale counts, out-of-order occurrences, and timeouts).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.market.quality.status import DataQualityStatus


@dataclass(frozen=True)
class SourceHealth:
    """Operational reliability snapshot for a data provider source."""

    source: str
    last_received_timestamp: Optional[float]
    last_processed_timestamp: Optional[float]
    records_received: int
    records_accepted: int
    records_rejected: int
    duplicate_count: int
    stale_count: int
    out_of_order_count: int
    validation_error_count: int
    degraded_count: int

    @property
    def is_healthy(self) -> bool:
        """Returns True if the source has received records and error/rejection rate is low."""
        if self.records_received == 0:
            return True
        error_total = self.records_rejected + self.validation_error_count
        return (error_total / self.records_received) < 0.20

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a serializable dictionary."""
        return {
            "source": self.source,
            "last_received_timestamp": self.last_received_timestamp,
            "last_processed_timestamp": self.last_processed_timestamp,
            "records_received": self.records_received,
            "records_accepted": self.records_accepted,
            "records_rejected": self.records_rejected,
            "duplicate_count": self.duplicate_count,
            "stale_count": self.stale_count,
            "out_of_order_count": self.out_of_order_count,
            "validation_error_count": self.validation_error_count,
            "degraded_count": self.degraded_count,
            "is_healthy": self.is_healthy,
        }


class SourceHealthTracker:
    """Thread-safe operational telemetry tracker for data sources."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._metrics: Dict[str, Dict[str, Any]] = {}

    def _ensure_source(self, source: str) -> Dict[str, Any]:
        if source not in self._metrics:
            self._metrics[source] = {
                "last_received_timestamp": None,
                "last_processed_timestamp": None,
                "records_received": 0,
                "records_accepted": 0,
                "records_rejected": 0,
                "duplicate_count": 0,
                "stale_count": 0,
                "out_of_order_count": 0,
                "validation_error_count": 0,
                "degraded_count": 0,
            }
        return self._metrics[source]

    def record_observation(
        self,
        source: str,
        status: DataQualityStatus,
        record_timestamp: float,
        processing_time: Optional[float] = None,
    ) -> None:
        """Update source metrics with a new assessment outcome."""
        now = time.time() if processing_time is None else processing_time
        with self._lock:
            data = self._ensure_source(source)
            data["records_received"] += 1
            data["last_received_timestamp"] = record_timestamp
            data["last_processed_timestamp"] = now

            if status in (DataQualityStatus.VALID, DataQualityStatus.DEGRADED):
                data["records_accepted"] += 1
            else:
                data["records_rejected"] += 1

            if status == DataQualityStatus.DEGRADED:
                data["degraded_count"] += 1
            elif status == DataQualityStatus.DUPLICATE:
                data["duplicate_count"] += 1
            elif status == DataQualityStatus.STALE:
                data["stale_count"] += 1
            elif status == DataQualityStatus.OUT_OF_ORDER:
                data["out_of_order_count"] += 1
            elif status == DataQualityStatus.INVALID:
                data["validation_error_count"] += 1

    def get_health(self, source: str) -> SourceHealth:
        """Get an immutable health snapshot for a single source."""
        with self._lock:
            data = self._ensure_source(source)
            return SourceHealth(source=source, **data)

    def get_all_health(self) -> Dict[str, SourceHealth]:
        """Get immutable health snapshots for all tracked sources."""
        with self._lock:
            return {src: self.get_health(src) for src in self._metrics}

    def check_source_timeout(
        self,
        source: str,
        timeout_seconds: float,
        current_time: Optional[float] = None,
    ) -> bool:
        """Returns True if the source has stopped producing data for longer than timeout_seconds."""
        now = time.time() if current_time is None else current_time
        with self._lock:
            data = self._ensure_source(source)
            last_ts = data["last_processed_timestamp"]
            if last_ts is None:
                return True
            return (now - last_ts) > timeout_seconds
