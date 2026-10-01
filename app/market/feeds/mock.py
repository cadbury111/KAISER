"""KAISER — Deterministic Mock Market Data Feed.

Generates predefined, synthetic market records for predictable testing scenarios
(normal quote, price surge, drop, volume spike, missing fields, stale data, multiple symbols).
All generated records are explicitly stamped with source="mock".
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence

from app.market.feeds.base import MarketFeed, MarketRecord
from app.market.models import Quote


class MockScenario(str, Enum):
    """Predefined test scenarios for mock feed generation."""

    NORMAL_QUOTE = "NORMAL_QUOTE"
    PRICE_INCREASE = "PRICE_INCREASE"
    PRICE_DECREASE = "PRICE_DECREASE"
    VOLUME_SPIKE = "VOLUME_SPIKE"
    MISSING_OPTIONAL = "MISSING_OPTIONAL"
    INVALID_RECORD = "INVALID_RECORD"
    STALE_TIMESTAMP = "STALE_TIMESTAMP"
    MULTIPLE_SYMBOLS = "MULTIPLE_SYMBOLS"


class MockMarketFeed(MarketFeed):
    """Deterministic synthetic market feed for testing without live connections."""

    DEFAULT_SYMBOLS = ["NIFTY", "RELIANCE", "TCS", "INFY"]

    def __init__(
        self,
        name: str = "mock_feed",
        symbols: Optional[Sequence[str]] = None,
        base_prices: Optional[Dict[str, float]] = None,
        auto_start: bool = True,
    ) -> None:
        super().__init__(name=name, source="mock")
        self.symbols = list(symbols or self.DEFAULT_SYMBOLS)
        self.base_prices = base_prices or {
            "NIFTY": 24500.0,
            "RELIANCE": 2980.0,
            "TCS": 4150.0,
            "INFY": 1820.0,
        }
        self._current_index: int = 0
        self._scenario_queue: List[Quote] = []
        if auto_start:
            self.start()

    def reset(self) -> None:
        """Reset feed index and clear scenario queue."""
        self._current_index = 0
        self._scenario_queue.clear()
        self._records_emitted = 0

    def generate_scenario_quote(
        self,
        scenario: MockScenario,
        symbol: str = "NIFTY",
        reference_time: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Generate a raw scenario dictionary based on the requested test scenario.

        Returns raw dictionary representation so normalizer and validator can be exercised.
        """
        now = reference_time if reference_time is not None else time.time()
        base_price = self.base_prices.get(symbol, 1000.0)

        if scenario == MockScenario.NORMAL_QUOTE:
            return {
                "symbol": symbol,
                "exchange": "NSE",
                "timestamp": now,
                "last_price": base_price,
                "bid": round(base_price - 0.5, 2),
                "ask": round(base_price + 0.5, 2),
                "bid_size": 100.0,
                "ask_size": 150.0,
                "volume": 50000.0,
                "currency": "INR",
                "source": "mock",
            }

        elif scenario == MockScenario.PRICE_INCREASE:
            jump_price = round(base_price * 1.05, 2)
            return {
                "symbol": symbol,
                "exchange": "NSE",
                "timestamp": now,
                "last_price": jump_price,
                "bid": round(jump_price - 0.5, 2),
                "ask": round(jump_price + 0.5, 2),
                "bid_size": 250.0,
                "ask_size": 80.0,
                "volume": 75000.0,
                "currency": "INR",
                "source": "mock",
            }

        elif scenario == MockScenario.PRICE_DECREASE:
            drop_price = round(base_price * 0.95, 2)
            return {
                "symbol": symbol,
                "exchange": "NSE",
                "timestamp": now,
                "last_price": drop_price,
                "bid": round(drop_price - 0.5, 2),
                "ask": round(drop_price + 0.5, 2),
                "bid_size": 50.0,
                "ask_size": 300.0,
                "volume": 90000.0,
                "currency": "INR",
                "source": "mock",
            }

        elif scenario == MockScenario.VOLUME_SPIKE:
            return {
                "symbol": symbol,
                "exchange": "NSE",
                "timestamp": now,
                "last_price": base_price,
                "bid": round(base_price - 0.5, 2),
                "ask": round(base_price + 0.5, 2),
                "bid_size": 5000.0,
                "ask_size": 5000.0,
                "volume": 1000000.0,  # 1M surge
                "currency": "INR",
                "source": "mock",
            }

        elif scenario == MockScenario.MISSING_OPTIONAL:
            # Bid and ask sizes missing
            return {
                "symbol": symbol,
                "exchange": "NSE",
                "timestamp": now,
                "last_price": base_price,
                "bid": round(base_price - 0.5, 2),
                "ask": round(base_price + 0.5, 2),
                "volume": 12000.0,
                "currency": "INR",
                "source": "mock",
            }

        elif scenario == MockScenario.INVALID_RECORD:
            # Negative price
            return {
                "symbol": symbol,
                "exchange": "NSE",
                "timestamp": now,
                "last_price": -100.0,
                "volume": 100.0,
                "source": "mock",
            }

        elif scenario == MockScenario.STALE_TIMESTAMP:
            # Timestamp from 2 hours ago
            return {
                "symbol": symbol,
                "exchange": "NSE",
                "timestamp": now - 7200.0,
                "last_price": base_price,
                "bid": round(base_price - 0.5, 2),
                "ask": round(base_price + 0.5, 2),
                "volume": 20000.0,
                "source": "mock",
            }

        elif scenario == MockScenario.MULTIPLE_SYMBOLS:
            return {
                "symbol": symbol,
                "exchange": "NSE",
                "timestamp": now,
                "last_price": base_price,
                "volume": 10000.0,
                "source": "mock",
            }

        raise ValueError(f"Unknown mock scenario: {scenario}")

    def enqueue_quote(self, quote: Quote) -> None:
        """Enqueue an explicit Quote for the next record call."""
        self._scenario_queue.append(quote)

    def next_record(self) -> Optional[MarketRecord]:
        """Produce the next deterministic mock quote."""
        if not self._is_running:
            return None

        # If explicit queue has items, pop first
        if self._scenario_queue:
            record = self._scenario_queue.pop(0)
            self._records_emitted += 1
            return record

        # Deterministically cycle across symbols
        sym = self.symbols[self._current_index % len(self.symbols)]
        base_price = self.base_prices.get(sym, 1000.0)
        # Apply a deterministic minor price drift based on iteration count
        drift = round(((self._current_index % 7) - 3) * 0.25, 2)
        price = max(1.0, round(base_price + drift, 2))

        quote = Quote(
            symbol=sym,
            exchange="NSE",
            timestamp=time.time(),
            last_price=price,
            bid=round(price - 0.25, 2),
            ask=round(price + 0.25, 2),
            bid_size=100.0,
            ask_size=100.0,
            volume=float(10000 + (self._current_index * 250)),
            currency="INR",
            source="mock",
        )

        self._current_index += 1
        self._records_emitted += 1
        return quote
