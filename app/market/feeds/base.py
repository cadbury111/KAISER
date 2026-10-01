"""KAISER — Market Data Feed Base Interface.

Defines the common protocol and lifecycle methods for all market data providers
(Mock, Replay, and future live feeds).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union

from app.market.models import Bar, OrderBookSnapshot, Quote

MarketRecord = Union[Quote, Bar, OrderBookSnapshot]


class MarketFeed(ABC):
    """Abstract base class for all market data ingestion feeds."""

    def __init__(self, name: str, source: str) -> None:
        self.name = name
        self.source = source
        self._is_running: bool = False
        self._records_emitted: int = 0

    @property
    def is_running(self) -> bool:
        """Return whether the feed is currently active."""
        return self._is_running

    @property
    def records_emitted(self) -> int:
        """Return the total number of records successfully emitted."""
        return self._records_emitted

    def start(self) -> None:
        """Start the feed for record generation or playback."""
        self._is_running = True

    def stop(self) -> None:
        """Stop the feed."""
        self._is_running = False

    @abstractmethod
    def reset(self) -> None:
        """Reset feed state, cursor, and metrics back to initial state."""

    @abstractmethod
    def next_record(self) -> Optional[MarketRecord]:
        """Fetch or generate the next market record.

        Returns:
            Optional[MarketRecord]: Normalized Quote, Bar, or OrderBookSnapshot,
                                    or None if feed has terminated or is stopped.
        """

    def get_status(self) -> Dict[str, Any]:
        """Return diagnostic status metadata for the feed."""
        return {
            "name": self.name,
            "source": self.source,
            "is_running": self._is_running,
            "records_emitted": self._records_emitted,
        }
