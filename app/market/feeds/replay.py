"""KAISER — Deterministic Replay Market Data Feed.

Plays back historical or pre-recorded synthetic market records in exact chronological
order for backtesting and agent evaluation without external network connectivity.
All replayed records are explicitly stamped with source="replay".
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from app.core.exceptions import ValidationError
from app.market.feeds.base import MarketFeed, MarketRecord
from app.market.models import Bar, OrderBookSnapshot, Quote
from app.market.normalizer import MarketDataNormalizer

logger = logging.getLogger("market.feeds.replay")


class ReplayMarketFeed(MarketFeed):
    """Deterministic replay feed for sequenced market data playback."""

    def __init__(
        self,
        name: str = "replay_feed",
        records: Optional[Sequence[Union[MarketRecord, Dict[str, Any]]]] = None,
        auto_start: bool = True,
        strict_chronological: bool = False,
    ) -> None:
        super().__init__(name=name, source="replay")
        self.strict_chronological = strict_chronological
        self._raw_records: List[Union[MarketRecord, Dict[str, Any]]] = list(records or [])
        self._current_index: int = 0
        self._records_rejected: int = 0
        self._last_emitted_timestamp: Optional[float] = None
        if auto_start:
            self.start()

    @property
    def current_index(self) -> int:
        """Current zero-indexed playback cursor."""
        return self._current_index

    @property
    def total_records(self) -> int:
        """Total number of records available for replay."""
        return len(self._raw_records)

    @property
    def records_rejected(self) -> int:
        """Total number of records rejected due to malformed schema or out-of-order timestamps."""
        return self._records_rejected

    def has_next(self) -> bool:
        """Check whether more records remain in the replay buffer."""
        return self._current_index < len(self._raw_records)

    def reset(self) -> None:
        """Reset playback cursor back to start."""
        self._current_index = 0
        self._records_emitted = 0
        self._records_rejected = 0
        self._last_emitted_timestamp = None

    def load_records(self, records: Sequence[Union[MarketRecord, Dict[str, Any]]]) -> None:
        """Load a new batch of records and reset cursor."""
        self._raw_records = list(records)
        self.reset()

    def load_from_json_file(self, file_path: Union[str, Path]) -> int:
        """Load records from a local JSON file containing an array of raw records."""
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Replay data file not found: {path}")

        content = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(content, list):
            raise ValueError(f"Expected JSON array of records, got {type(content).__name__}")

        self.load_records(content)
        return len(self._raw_records)

    def _normalize_record(self, raw: Union[MarketRecord, Dict[str, Any]]) -> MarketRecord:
        """Ensure record is normalized and tagged with source='replay'."""
        if isinstance(raw, (Quote, Bar, OrderBookSnapshot)):
            if raw.source != "replay":
                # Stamp with replay source
                d = raw.to_dict()
                d["source"] = "replay"
                if isinstance(raw, Quote):
                    return MarketDataNormalizer.normalize_quote(d, default_source="replay")
                elif isinstance(raw, Bar):
                    return MarketDataNormalizer.normalize_bar(d, default_source="replay")
                else:
                    return MarketDataNormalizer.normalize_order_book(d, default_source="replay")
            return raw

        if isinstance(raw, dict):
            record_dict = copy.deepcopy(raw)
            record_dict["source"] = "replay"
            # Deduce model type
            if "open" in record_dict and "close" in record_dict:
                return MarketDataNormalizer.normalize_bar(record_dict, default_source="replay")
            elif "bids" in record_dict or "asks" in record_dict:
                return MarketDataNormalizer.normalize_order_book(record_dict, default_source="replay")
            else:
                return MarketDataNormalizer.normalize_quote(record_dict, default_source="replay")

        raise TypeError(f"Unsupported record format in replay feed: {type(raw).__name__}")

    def next_record(self) -> Optional[MarketRecord]:
        """Advance cursor and return the next normalized market record."""
        if not self._is_running or not self.has_next():
            return None

        raw = self._raw_records[self._current_index]
        try:
            record = self._normalize_record(raw)
        except Exception as err:
            self._records_rejected += 1
            self._current_index += 1
            if self.strict_chronological:
                raise ValidationError(f"Malformed replay record at index {self._current_index - 1}: {err}") from err
            logger.warning("Rejected malformed replay record at index %d: %s", self._current_index - 1, err)
            return None

        if self.strict_chronological and self._last_emitted_timestamp is not None:
            if record.timestamp < self._last_emitted_timestamp:
                self._records_rejected += 1
                self._current_index += 1
                raise ValidationError(
                    f"Out-of-order replay record detected at index {self._current_index - 1}: "
                    f"timestamp {record.timestamp} is earlier than previous timestamp {self._last_emitted_timestamp}"
                )

        self._last_emitted_timestamp = record.timestamp
        self._current_index += 1
        self._records_emitted += 1
        return record

    def step(self) -> Optional[MarketRecord]:
        """Convenience alias for next_record."""
        return self.next_record()

    def play_all(self) -> List[MarketRecord]:
        """Drain and return all remaining records until end of feed."""
        emitted: List[MarketRecord] = []
        while self.has_next() and self._is_running:
            rec = self.next_record()
            if rec is not None:
                emitted.append(rec)
        return emitted

    def compute_input_digest(self) -> str:
        """Compute a deterministic SHA-256 digest of all records in the replay buffer.

        Provides a reproducible cryptographic fingerprint of the input data stream.
        """
        hasher = hashlib.sha256()
        for idx, raw in enumerate(self._raw_records):
            try:
                rec = self._normalize_record(raw)
                if isinstance(rec, Quote):
                    line = f"QUOTE:{rec.symbol}:{rec.timestamp:.6f}:{rec.last_price:.4f}:{rec.volume:.2f}\n"
                elif isinstance(rec, Bar):
                    line = f"BAR:{rec.symbol}:{rec.timestamp:.6f}:{rec.open:.4f}:{rec.high:.4f}:{rec.low:.4f}:{rec.close:.4f}:{rec.volume:.2f}\n"
                else:
                    line = f"BOOK:{rec.symbol}:{rec.timestamp:.6f}\n"
                hasher.update(line.encode("utf-8"))
            except Exception:
                hasher.update(f"CORRUPT:{idx}\n".encode("utf-8"))
        return hasher.hexdigest()

    def get_replay_stats(self) -> Dict[str, Any]:
        """Return audit and operational statistics for the replay feed."""
        return {
            "name": self.name,
            "total_records": self.total_records,
            "records_emitted": self._records_emitted,
            "records_rejected": self._records_rejected,
            "records_remaining": max(0, self.total_records - self._current_index),
            "current_index": self._current_index,
            "last_emitted_timestamp": self._last_emitted_timestamp,
            "strict_chronological": self.strict_chronological,
            "input_digest": self.compute_input_digest(),
        }
