"""KAISER — Market Data Domain Models.

Provides standardized, strongly-typed, immutable market data structures:
- Quote (real-time price and BBO quotes)
- Bar (OHLCV candlestick records)
- OrderBookSnapshot & OrderBookLevel (market depth snapshots)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import math
from typing import Any, Dict, List, Optional, Tuple

from app.core.exceptions import ValidationError


@dataclass(frozen=True)
class Quote:
    """Immutable market quote representation.

    Attributes:
        symbol: Ticker symbol (e.g., 'NIFTY', 'RELIANCE').
        exchange: Exchange identifier (e.g., 'NSE', 'BSE', 'MOCK').
        timestamp: POSIX UTC timestamp when quote occurred.
        last_price: Most recent traded price.
        bid: Current best bid price (optional).
        ask: Current best ask price (optional).
        bid_size: Quantity available at best bid (optional).
        ask_size: Quantity available at best ask (optional).
        volume: Cumulative traded volume.
        currency: Pricing currency (default 'INR').
        source: Data source tag (e.g. 'mock', 'replay').
    """

    symbol: str
    exchange: str
    timestamp: float
    last_price: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    volume: float = 0.0
    currency: str = "INR"
    source: str = "mock"

    def __post_init__(self) -> None:
        """Validate core invariants upon instantiation."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("Quote symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not self.exchange or not isinstance(self.exchange, str) or not self.exchange.strip():
            raise ValidationError("Quote exchange must be a non-empty string.")
        object.__setattr__(self, "exchange", self.exchange.strip().upper())

        if not self.source or not isinstance(self.source, str) or not self.source.strip():
            raise ValidationError("Quote source must be a non-empty string.")

        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"Quote timestamp must be a positive finite float, got {self.timestamp}")

        if not math.isfinite(self.last_price) or self.last_price < 0:
            raise ValidationError(f"Quote last_price cannot be negative or non-finite, got {self.last_price}")

        if not math.isfinite(self.volume) or self.volume < 0:
            raise ValidationError(f"Quote volume cannot be negative or non-finite, got {self.volume}")

        if self.bid is not None and (not math.isfinite(self.bid) or self.bid < 0):
            raise ValidationError(f"Quote bid cannot be negative or non-finite, got {self.bid}")

        if self.ask is not None and (not math.isfinite(self.ask) or self.ask < 0):
            raise ValidationError(f"Quote ask cannot be negative or non-finite, got {self.ask}")

        if self.bid_size is not None and (not math.isfinite(self.bid_size) or self.bid_size < 0):
            raise ValidationError(f"Quote bid_size cannot be negative or non-finite, got {self.bid_size}")

        if self.ask_size is not None and (not math.isfinite(self.ask_size) or self.ask_size < 0):
            raise ValidationError(f"Quote ask_size cannot be negative or non-finite, got {self.ask_size}")

        # Invariant: Ask cannot be lower than bid if both exist and are non-zero
        if (
            self.bid is not None
            and self.ask is not None
            and self.bid > 0
            and self.ask > 0
            and self.ask < self.bid
        ):
            raise ValidationError(
                f"Inverted spread invariant violated: ask ({self.ask}) cannot be less than bid ({self.bid})"
            )

    @property
    def spread(self) -> Optional[float]:
        """Calculate the bid-ask spread if both are available."""
        if self.bid is not None and self.ask is not None:
            return round(self.ask - self.bid, 4)
        return None

    @property
    def mid_price(self) -> float:
        """Calculate the mid-market price or return last_price as fallback."""
        if self.bid is not None and self.ask is not None:
            return round((self.ask + self.bid) / 2.0, 4)
        return self.last_price

    @property
    def iso_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert Quote into a clean serializable dictionary."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "last_price": self.last_price,
            "bid": self.bid,
            "ask": self.ask,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "volume": self.volume,
            "currency": self.currency,
            "source": self.source,
            "spread": self.spread,
            "mid_price": self.mid_price,
        }


@dataclass(frozen=True)
class Bar:
    """Immutable OHLCV candlestick bar.

    Attributes:
        symbol: Ticker symbol.
        exchange: Exchange identifier.
        timeframe: Bar timeframe (e.g. '1m', '5m', '1D').
        timestamp: Bar open or closing POSIX UTC timestamp.
        open: Opening price.
        high: Highest traded price.
        low: Lowest traded price.
        close: Closing traded price.
        volume: Total volume traded in the bar.
        source: Data source tag.
    """

    symbol: str
    exchange: str
    timeframe: str
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    source: str = "mock"

    def __post_init__(self) -> None:
        """Validate OHLC price relations and boundaries."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("Bar symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not self.exchange or not isinstance(self.exchange, str) or not self.exchange.strip():
            raise ValidationError("Bar exchange must be a non-empty string.")
        object.__setattr__(self, "exchange", self.exchange.strip().upper())

        if not self.timeframe or not isinstance(self.timeframe, str) or not self.timeframe.strip():
            raise ValidationError("Bar timeframe must be a non-empty string.")

        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"Bar timestamp must be a positive finite float, got {self.timestamp}")

        # Check non-negative finite prices
        for price_name, val in [("open", self.open), ("high", self.high), ("low", self.low), ("close", self.close)]:
            if not math.isfinite(val) or val < 0:
                raise ValidationError(f"Bar {price_name} price must be a non-negative finite float, got {val}")

        if not math.isfinite(self.volume) or self.volume < 0:
            raise ValidationError(f"Bar volume must be a non-negative finite float, got {self.volume}")

        # OHLC Relational Invariants
        if self.high < self.open or self.high < self.close or self.high < self.low:
            raise ValidationError(
                f"Invalid Bar high price ({self.high}): high must be >= open ({self.open}), "
                f"close ({self.close}), and low ({self.low})"
            )

        if self.low > self.open or self.low > self.close or self.low > self.high:
            raise ValidationError(
                f"Invalid Bar low price ({self.low}): low must be <= open ({self.open}), "
                f"close ({self.close}), and high ({self.high})"
            )

    @property
    def is_bullish(self) -> bool:
        """True if closing price is greater than or equal to open price."""
        return self.close >= self.open

    @property
    def range(self) -> float:
        """Bar price range (high - low)."""
        return round(self.high - self.low, 4)

    @property
    def iso_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert Bar into a clean serializable dictionary."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timeframe": self.timeframe,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "source": self.source,
            "is_bullish": self.is_bullish,
            "range": self.range,
        }


@dataclass(frozen=True)
class OrderBookLevel:
    """Immutable single price level in an order book."""

    price: float
    quantity: float
    orders_count: Optional[int] = None

    def __post_init__(self) -> None:
        if self.price < 0:
            raise ValidationError(f"OrderBookLevel price cannot be negative, got {self.price}")
        if self.quantity < 0:
            raise ValidationError(f"OrderBookLevel quantity cannot be negative, got {self.quantity}")
        if self.orders_count is not None and self.orders_count < 0:
            raise ValidationError(f"OrderBookLevel orders_count cannot be negative, got {self.orders_count}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "price": self.price,
            "quantity": self.quantity,
            "orders_count": self.orders_count,
        }


@dataclass(frozen=True)
class OrderBookSnapshot:
    """Immutable market depth snapshot.

    Attributes:
        symbol: Ticker symbol.
        exchange: Exchange identifier.
        timestamp: POSIX UTC timestamp.
        bids: Tuple of bid levels, sorted by price descending.
        asks: Tuple of ask levels, sorted by price ascending.
        source: Data source tag.
        sequence_number: Optional sequence number from upstream feed.
        is_valid: Boolean indicating whether snapshot is consistent and uncrossed.
    """

    symbol: str
    exchange: str
    timestamp: float
    bids: Tuple[OrderBookLevel, ...] = ()
    asks: Tuple[OrderBookLevel, ...] = ()
    source: str = "mock"
    sequence_number: Optional[int] = None
    is_valid: bool = True

    def __post_init__(self) -> None:
        """Validate and freeze depth levels."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("OrderBookSnapshot symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not self.exchange or not isinstance(self.exchange, str) or not self.exchange.strip():
            raise ValidationError("OrderBookSnapshot exchange must be a non-empty string.")
        object.__setattr__(self, "exchange", self.exchange.strip().upper())

        if self.timestamp <= 0:
            raise ValidationError(f"OrderBookSnapshot timestamp must be a positive float, got {self.timestamp}")

        # Ensure bids and asks are tuples of OrderBookLevel
        clean_bids = tuple(
            level if isinstance(level, OrderBookLevel) else OrderBookLevel(**level)
            for level in self.bids
        )
        clean_asks = tuple(
            level if isinstance(level, OrderBookLevel) else OrderBookLevel(**level)
            for level in self.asks
        )
        object.__setattr__(self, "bids", clean_bids)
        object.__setattr__(self, "asks", clean_asks)

    @property
    def best_bid(self) -> Optional[float]:
        """Price of highest bid."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        """Price of lowest ask."""
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> Optional[float]:
        """Spread between best ask and best bid."""
        if self.best_bid is not None and self.best_ask is not None:
            return round(self.best_ask - self.best_bid, 4)
        return None

    @property
    def iso_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert OrderBookSnapshot into a clean serializable dictionary."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "bids": [b.to_dict() for b in self.bids],
            "asks": [a.to_dict() for a in self.asks],
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread": self.spread,
            "source": self.source,
            "sequence_number": self.sequence_number,
            "is_valid": self.is_valid,
        }


class DeltaAction(str, Enum):
    """Operation types for Level 2 order book updates."""

    ADD = "ADD"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class BookSide(str, Enum):
    """Order book sides."""

    BID = "BID"
    ASK = "ASK"


@dataclass(frozen=True)
class OrderBookDelta:
    """Immutable Level 2 order book price level delta.

    Attributes:
        symbol: Ticker symbol.
        side: Order book side (BID or ASK).
        price: Price level (strictly finite and positive).
        quantity: Available volume at price level (strictly finite and non-negative; 0 removes level).
        action: Operation type (ADD, UPDATE, DELETE).
        timestamp: POSIX UTC timestamp.
        sequence_number: Optional monotonic sequence number for gap/duplicate detection.
        orders_count: Optional count of resting orders at price level.
        exchange: Exchange identifier.
        source: Data source tag.
    """

    symbol: str
    side: BookSide
    price: float
    quantity: float
    action: DeltaAction
    timestamp: float
    sequence_number: Optional[int] = None
    orders_count: Optional[int] = None
    exchange: str = "NSE"
    source: str = "websocket"

    def __post_init__(self) -> None:
        """Validate delta invariants fail-closed."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("OrderBookDelta symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not self.exchange or not isinstance(self.exchange, str) or not self.exchange.strip():
            raise ValidationError("OrderBookDelta exchange must be a non-empty string.")
        object.__setattr__(self, "exchange", self.exchange.strip().upper())

        # Normalize side
        if isinstance(self.side, str):
            clean_side = self.side.strip().upper()
            if clean_side not in ("BID", "ASK"):
                raise ValidationError(f"Invalid OrderBookDelta side: {self.side}. Must be 'BID' or 'ASK'.")
            object.__setattr__(self, "side", BookSide(clean_side))
        elif not isinstance(self.side, BookSide):
            raise ValidationError(f"OrderBookDelta side must be BookSide or str, got {type(self.side).__name__}")

        # Normalize action
        if isinstance(self.action, str):
            clean_act = self.action.strip().upper()
            if clean_act not in ("ADD", "UPDATE", "DELETE"):
                raise ValidationError(f"Invalid OrderBookDelta action: {self.action}. Must be ADD, UPDATE, or DELETE.")
            object.__setattr__(self, "action", DeltaAction(clean_act))
        elif not isinstance(self.action, DeltaAction):
            raise ValidationError(f"OrderBookDelta action must be DeltaAction or str, got {type(self.action).__name__}")

        # Price validation: finite and positive
        if not math.isfinite(self.price) or self.price <= 0:
            raise ValidationError(f"OrderBookDelta price must be a finite positive float, got {self.price}")

        # Quantity validation: finite and non-negative
        if not math.isfinite(self.quantity) or self.quantity < 0:
            raise ValidationError(f"OrderBookDelta quantity must be a finite non-negative float, got {self.quantity}")

        # Timestamp validation: finite and positive
        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"OrderBookDelta timestamp must be a finite positive float, got {self.timestamp}")

        # Sequence number validation
        if self.sequence_number is not None:
            if not isinstance(self.sequence_number, int) or self.sequence_number < 0:
                raise ValidationError(
                    f"OrderBookDelta sequence_number must be a non-negative integer, got {self.sequence_number}"
                )

        # Orders count validation
        if self.orders_count is not None:
            if not isinstance(self.orders_count, int) or self.orders_count < 0:
                raise ValidationError(
                    f"OrderBookDelta orders_count must be a non-negative integer, got {self.orders_count}"
                )

    @property
    def iso_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert OrderBookDelta into a clean serializable dictionary."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "side": self.side.value,
            "price": self.price,
            "quantity": self.quantity,
            "action": self.action.value,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "sequence_number": self.sequence_number,
            "orders_count": self.orders_count,
            "source": self.source,
        }


class AggressorSide(str, Enum):
    """Categorical classification of trade initiator (aggressor side)."""

    BUY_INITIATED = "BUY_INITIATED"
    SELL_INITIATED = "SELL_INITIATED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class TradeTick:
    """Immutable executed market trade tick.

    Attributes:
        symbol: Ticker symbol (e.g. 'RELIANCE', 'TCS').
        price: Executed trade price (finite, strictly positive).
        volume: Executed trade quantity / volume (finite, strictly positive).
        timestamp: Exchange execution POSIX UTC timestamp.
        received_timestamp: Local system receipt POSIX UTC timestamp.
        trade_id: Unique trade execution identifier from upstream feed.
        sequence_number: Optional monotonic sequence number.
        aggressor_side: Party classification (BUY_INITIATED, SELL_INITIATED, UNKNOWN).
        exchange: Exchange identifier.
        source: Data source tag.
    """

    symbol: str
    price: float
    volume: float
    timestamp: float
    received_timestamp: float
    trade_id: str
    sequence_number: Optional[int] = None
    aggressor_side: AggressorSide = AggressorSide.UNKNOWN
    exchange: str = "NSE"
    source: str = "trade_feed"

    def __post_init__(self) -> None:
        """Validate trade tick invariants fail-closed."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("TradeTick symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not self.exchange or not isinstance(self.exchange, str) or not self.exchange.strip():
            raise ValidationError("TradeTick exchange must be a non-empty string.")
        object.__setattr__(self, "exchange", self.exchange.strip().upper())

        if not self.trade_id or not isinstance(self.trade_id, str) or not str(self.trade_id).strip():
            raise ValidationError("TradeTick trade_id must be a non-empty string.")
        object.__setattr__(self, "trade_id", str(self.trade_id).strip())

        # Price validation: finite and strictly positive
        if not math.isfinite(self.price) or self.price <= 0:
            raise ValidationError(f"TradeTick price must be a finite positive float, got {self.price}")

        # Volume validation: finite and strictly positive
        if not math.isfinite(self.volume) or self.volume <= 0:
            raise ValidationError(f"TradeTick volume must be a finite positive float, got {self.volume}")

        # Timestamps validation
        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"TradeTick timestamp must be a positive float, got {self.timestamp}")

        if not math.isfinite(self.received_timestamp) or self.received_timestamp <= 0:
            raise ValidationError(
                f"TradeTick received_timestamp must be a positive float, got {self.received_timestamp}"
            )

        # Sequence number validation
        if self.sequence_number is not None:
            if not isinstance(self.sequence_number, int) or self.sequence_number < 0:
                raise ValidationError(
                    f"TradeTick sequence_number must be a non-negative integer, got {self.sequence_number}"
                )

        # Normalize aggressor_side
        if isinstance(self.aggressor_side, str):
            clean_side = self.aggressor_side.strip().upper()
            if clean_side not in ("BUY_INITIATED", "SELL_INITIATED", "UNKNOWN"):
                raise ValidationError(
                    f"Invalid TradeTick aggressor_side: '{self.aggressor_side}'. "
                    f"Must be BUY_INITIATED, SELL_INITIATED, or UNKNOWN."
                )
            object.__setattr__(self, "aggressor_side", AggressorSide(clean_side))
        elif not isinstance(self.aggressor_side, AggressorSide):
            raise ValidationError(
                f"TradeTick aggressor_side must be AggressorSide or str, got {type(self.aggressor_side).__name__}"
            )

    @property
    def quantity(self) -> float:
        """Alias for volume to satisfy quantity-based callers."""
        return self.volume

    @property
    def iso_timestamp(self) -> str:
        """Return the exchange execution ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    @property
    def iso_received_timestamp(self) -> str:
        """Return the local receipt ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.received_timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert TradeTick into a clean serializable dictionary."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "trade_id": self.trade_id,
            "price": self.price,
            "volume": self.volume,
            "quantity": self.volume,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "received_timestamp": self.received_timestamp,
            "iso_received_timestamp": self.iso_received_timestamp,
            "sequence_number": self.sequence_number,
            "aggressor_side": self.aggressor_side.value,
            "source": self.source,
        }
