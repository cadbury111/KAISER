"""KAISER — Deterministic Replay Market Data Feed.

Plays back historical or pre-recorded synthetic market records in exact chronological
order for backtesting and agent evaluation without external network connectivity.
All replayed records are explicitly stamped with source="replay".
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from app.market.feeds.base import MarketFeed, MarketRecord
from app.market.models import Bar, OrderBookSnapshot, Quote
from app.market.normalizer import MarketDataNormalizer


class ReplayMarketFeed(MarketFeed):
    """Deterministic replay feed for sequenced market data playback."""

    def __init__(
        self,
        name: str = "replay_feed",
        records: Optional[Sequence[Union[MarketRecord, Dict[str, Any]]]] = None,
        auto_start: bool = True,
    ) -> None:
        super().__init__(name=name, source="replay")
        self._raw_records: List[Union[MarketRecord, Dict[str, Any]]] = list(records or [])
        self._current_index: int = 0
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

    def has_next(self) -> bool:
        """Check whether more records remain in the replay buffer."""
        return self._current_index < len(self._raw_records)

    def reset(self) -> None:
        """Reset playback cursor back to start."""
        self._current_index = 0
        self._records_emitted = 0

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
        record = self._normalize_record(raw)
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
