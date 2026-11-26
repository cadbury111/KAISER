"""KAISER — Level 2 Order Book Depth Delta Reconstruction Engine.

Provides deterministic, thread-safe, fail-closed Level 2 order book reconstruction:
1. Maintains discrete bid and ask price ladders sorted by price (bids descending, asks ascending).
2. Applies incremental OrderBookDelta updates: ADD, UPDATE, DELETE, and zero-quantity removals.
3. Enforces sequence continuity, detecting duplicate, out-of-order, and missing sequence numbers.
4. Detects crossed order books (best_bid >= best_ask) and prevents silent publication of invalid state.
5. Emits immutable, frozen OrderBookSnapshot domain models.
6. Multi-instrument L2OrderBookManager coordinating builders across symbols.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.market.models import BookSide, DeltaAction, OrderBookDelta, OrderBookLevel, OrderBookSnapshot

logger = get_logger("market.depth")


class L2OrderBookBuilder:
    """Deterministic, thread-safe Level 2 order book reconstructor for a single instrument.

    Maintains bid and ask price levels, applies delta modifications, guards against crossed
    books, validates sequence continuity, and outputs immutable OrderBookSnapshot models.
    """

    def __init__(
        self,
        symbol: str,
        exchange: str = "NSE",
        depth_limit: int = 20,
        source: str = "websocket",
        strict_sequence: bool = True,
        max_allowed_gap: int = 100,
    ) -> None:
        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("L2OrderBookBuilder requires a non-empty symbol.")
        self.symbol = symbol.strip().upper()
        self.exchange = exchange.strip().upper()
        if depth_limit <= 0:
            raise ValidationError(f"depth_limit must be a positive integer, got {depth_limit}")
        self.depth_limit = depth_limit
        self.source = source
        self.strict_sequence = strict_sequence
        self.max_allowed_gap = max_allowed_gap

        self._lock = threading.RLock()
        self._bids: Dict[float, OrderBookLevel] = {}
        self._asks: Dict[float, OrderBookLevel] = {}

        self._last_sequence_number: Optional[int] = None
        self._last_timestamp: float = 0.0
        self._deltas_processed: int = 0
        self._deltas_rejected: int = 0
        self._sequence_gap_count: int = 0
        self._crossed_book_count: int = 0

    @property
    def best_bid(self) -> Optional[float]:
        """Price of current highest bid."""
        with self._lock:
            return max(self._bids.keys()) if self._bids else None

    @property
    def best_ask(self) -> Optional[float]:
        """Price of current lowest ask."""
        with self._lock:
            return min(self._asks.keys()) if self._asks else None

    @property
    def spread(self) -> Optional[float]:
        """Spread between best ask and best bid."""
        with self._lock:
            bb = self.best_bid
            ba = self.best_ask
            if bb is not None and ba is not None:
                return round(ba - bb, 4)
            return None

    @property
    def is_crossed(self) -> bool:
        """True if the book is inverted or locked (best_bid >= best_ask)."""
        with self._lock:
            bb = self.best_bid
            ba = self.best_ask
            if bb is not None and ba is not None:
                return bb >= ba
            return False

    @property
    def bids_count(self) -> int:
        """Total distinct bid price levels."""
        with self._lock:
            return len(self._bids)

    @property
    def asks_count(self) -> int:
        """Total distinct ask price levels."""
        with self._lock:
            return len(self._asks)

    @property
    def last_sequence_number(self) -> Optional[int]:
        """Most recent successfully processed sequence number."""
        with self._lock:
            return self._last_sequence_number

    @property
    def deltas_processed(self) -> int:
        """Cumulative count of successfully applied deltas."""
        with self._lock:
            return self._deltas_processed

    @property
    def deltas_rejected(self) -> int:
        """Cumulative count of rejected deltas (schema, sequence, or crossed book)."""
        with self._lock:
            return self._deltas_rejected

    @property
    def sequence_gap_count(self) -> int:
        """Cumulative count of sequence gap anomalies detected."""
        with self._lock:
            return self._sequence_gap_count

    @property
    def crossed_book_count(self) -> int:
        """Cumulative count of crossed book events caught and prevented."""
        with self._lock:
            return self._crossed_book_count

    def reset(self) -> None:
        """Clear all price levels and reset sequence counters."""
        with self._lock:
            self._bids.clear()
            self._asks.clear()
            self._last_sequence_number = None
            self._last_timestamp = 0.0
            self._deltas_processed = 0
            self._deltas_rejected = 0
            self._sequence_gap_count = 0
            self._crossed_book_count = 0
            logger.debug("L2OrderBookBuilder reset for %s", self.symbol)

    def apply_snapshot(self, snapshot: OrderBookSnapshot) -> None:
        """Bootstrap or overwrite order book state directly from an existing snapshot."""
        with self._lock:
            if snapshot.symbol.upper() != self.symbol:
                raise ValidationError(
                    f"Snapshot symbol '{snapshot.symbol}' does not match builder symbol '{self.symbol}'."
                )

            # Check snapshot for crossed condition
            if snapshot.best_bid is not None and snapshot.best_ask is not None and snapshot.best_bid >= snapshot.best_ask:
                raise ValidationError(
                    f"Cannot apply crossed snapshot for {self.symbol}: best_bid ({snapshot.best_bid}) >= best_ask ({snapshot.best_ask})"
                )

            self._bids.clear()
            self._asks.clear()

            for level in snapshot.bids:
                if level.quantity > 0:
                    self._bids[level.price] = level

            for level in snapshot.asks:
                if level.quantity > 0:
                    self._asks[level.price] = level

            self._last_timestamp = snapshot.timestamp
            if snapshot.sequence_number is not None:
                self._last_sequence_number = snapshot.sequence_number

            logger.info(
                "Bootstrapped L2 book for %s with %d bids and %d asks [ts=%.3f]",
                self.symbol,
                len(self._bids),
                len(self._asks),
                snapshot.timestamp,
            )

    def apply_delta(self, delta: OrderBookDelta) -> OrderBookSnapshot:
        """Apply an incremental OrderBookDelta update and return the resulting OrderBookSnapshot.

        Enforces:
        1. Symbol matching.
        2. Sequence monotonicity (rejects duplicates and out-of-order deltas).
        3. Sequence gap telemetry.
        4. Level mutation (ADD, UPDATE, DELETE, and zero-quantity removal).
        5. Crossed book prevention: rolls back mutation and raises ValidationError if crossed.

        Args:
            delta: Validated OrderBookDelta instance.

        Returns:
            OrderBookSnapshot: The resulting immutable snapshot.

        Raises:
            ValidationError: If delta is invalid, out-of-order, or causes crossed book.
        """
        with self._lock:
            # 1. Symbol verification
            if delta.symbol.upper() != self.symbol:
                self._deltas_rejected += 1
                raise ValidationError(
                    f"Delta symbol '{delta.symbol}' does not match builder symbol '{self.symbol}'."
                )

            # 2. Sequence continuity verification
            if delta.sequence_number is not None:
                if self._last_sequence_number is not None:
                    if delta.sequence_number <= self._last_sequence_number:
                        self._deltas_rejected += 1
                        raise ValidationError(
                            f"Out-of-order or duplicate sequence number for {self.symbol}: "
                            f"received {delta.sequence_number}, last was {self._last_sequence_number}."
                        )
                    if delta.sequence_number > self._last_sequence_number + 1:
                        gap = delta.sequence_number - (self._last_sequence_number + 1)
                        self._sequence_gap_count += 1
                        logger.warning(
                            "Sequence gap detected for %s: missing %d sequences (last=%d, curr=%d)",
                            self.symbol,
                            gap,
                            self._last_sequence_number,
                            delta.sequence_number,
                        )
                        if self.strict_sequence and gap > self.max_allowed_gap:
                            self._deltas_rejected += 1
                            raise ValidationError(
                                f"Sequence gap ({gap}) exceeded max allowed threshold ({self.max_allowed_gap}) for {self.symbol}."
                            )

            # 3. Apply mutation with rollback preservation
            target_dict = self._bids if delta.side == BookSide.BID else self._asks
            prev_level = target_dict.get(delta.price)

            if delta.action == DeltaAction.DELETE or delta.quantity == 0:
                target_dict.pop(delta.price, None)
            elif delta.action in (DeltaAction.ADD, DeltaAction.UPDATE):
                target_dict[delta.price] = OrderBookLevel(
                    price=delta.price,
                    quantity=delta.quantity,
                    orders_count=delta.orders_count,
                )

            # 4. Invariant Check: Crossed Book Detection
            if self.is_crossed:
                # Rollback mutation so builder state remains clean and uncorrupted
                if prev_level is not None:
                    target_dict[delta.price] = prev_level
                else:
                    target_dict.pop(delta.price, None)

                self._crossed_book_count += 1
                self._deltas_rejected += 1
                raise ValidationError(
                    f"Crossed order book invariant violated for {self.symbol}: best bid ({self.best_bid}) "
                    f">= best ask ({self.best_ask}) after applying delta at price {delta.price}."
                )

            # Mutation committed successfully
            self._deltas_processed += 1
            if delta.sequence_number is not None:
                self._last_sequence_number = delta.sequence_number
            self._last_timestamp = delta.timestamp

            return self.get_snapshot(timestamp=delta.timestamp, sequence_number=delta.sequence_number)

    def get_snapshot(
        self,
        timestamp: Optional[float] = None,
        sequence_number: Optional[int] = None,
    ) -> OrderBookSnapshot:
        """Generate an immutable OrderBookSnapshot representing the current top-N depth ladder.

        Bids are sorted descending by price (highest bid first).
        Asks are sorted ascending by price (lowest ask first).
        """
        with self._lock:
            # Sort bids descending
            sorted_bid_prices = sorted(self._bids.keys(), reverse=True)[: self.depth_limit]
            bids = tuple(self._bids[p] for p in sorted_bid_prices)

            # Sort asks ascending
            sorted_ask_prices = sorted(self._asks.keys())[: self.depth_limit]
            asks = tuple(self._asks[p] for p in sorted_ask_prices)

            ts = timestamp if timestamp is not None else self._last_timestamp
            seq = sequence_number if sequence_number is not None else self._last_sequence_number

            return OrderBookSnapshot(
                symbol=self.symbol,
                exchange=self.exchange,
                timestamp=ts,
                bids=bids,
                asks=asks,
                source=self.source,
                sequence_number=seq,
                is_valid=not self.is_crossed,
            )

    def get_status(self) -> Dict[str, Any]:
        """Return diagnostic operational metadata for the order book builder."""
        with self._lock:
            return {
                "symbol": self.symbol,
                "exchange": self.exchange,
                "depth_limit": self.depth_limit,
                "best_bid": self.best_bid,
                "best_ask": self.best_ask,
                "spread": self.spread,
                "bids_count": len(self._bids),
                "asks_count": len(self._asks),
                "is_crossed": self.is_crossed,
                "last_sequence_number": self._last_sequence_number,
                "deltas_processed": self._deltas_processed,
                "deltas_rejected": self._deltas_rejected,
                "sequence_gap_count": self._sequence_gap_count,
                "crossed_book_count": self._crossed_book_count,
            }


class L2OrderBookManager:
    """Thread-safe multi-instrument manager coordinating L2OrderBookBuilder instances."""

    def __init__(
        self,
        exchange: str = "NSE",
        depth_limit: int = 20,
        source: str = "websocket",
        strict_sequence: bool = True,
    ) -> None:
        self.exchange = exchange.strip().upper()
        self.depth_limit = depth_limit
        self.source = source
        self.strict_sequence = strict_sequence
        self._builders: Dict[str, L2OrderBookBuilder] = {}
        self._lock = threading.RLock()

    def get_builder(self, symbol: str) -> L2OrderBookBuilder:
        """Retrieve or lazily instantiate an L2OrderBookBuilder for the requested symbol."""
        norm_sym = symbol.strip().upper()
        with self._lock:
            if norm_sym not in self._builders:
                self._builders[norm_sym] = L2OrderBookBuilder(
                    symbol=norm_sym,
                    exchange=self.exchange,
                    depth_limit=self.depth_limit,
                    source=self.source,
                    strict_sequence=self.strict_sequence,
                )
            return self._builders[norm_sym]

    def apply_delta(self, delta: OrderBookDelta) -> OrderBookSnapshot:
        """Route an OrderBookDelta to its corresponding symbol builder and return the new snapshot."""
        builder = self.get_builder(delta.symbol)
        return builder.apply_delta(delta)

    def get_snapshot(self, symbol: str) -> Optional[OrderBookSnapshot]:
        """Return current snapshot for symbol if builder exists and has levels, else None."""
        norm_sym = symbol.strip().upper()
        with self._lock:
            builder = self._builders.get(norm_sym)
            if builder and (builder.bids_count > 0 or builder.asks_count > 0):
                return builder.get_snapshot()
            return None

    def get_all_snapshots(self) -> Dict[str, OrderBookSnapshot]:
        """Return current snapshots for all tracked instruments."""
        with self._lock:
            return {
                sym: builder.get_snapshot()
                for sym, builder in self._builders.items()
                if builder.bids_count > 0 or builder.asks_count > 0
            }

    def reset(self, symbol: Optional[str] = None) -> None:
        """Reset builder for specific symbol or all builders if symbol is None."""
        with self._lock:
            if symbol:
                norm_sym = symbol.strip().upper()
                if norm_sym in self._builders:
                    self._builders[norm_sym].reset()
            else:
                for builder in self._builders.values():
                    builder.reset()
