"""KAISER — Market Data Normalization Layer.

Converts diverse, provider-specific, or mock raw records into standardized
immutable KAISER domain models (Quote, Bar, OrderBookSnapshot) with strict
domain validation and type coercion.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from app.core.exceptions import ValidationError
from app.market.models import (
    Bar,
    BookSide,
    DeltaAction,
    OrderBookDelta,
    OrderBookLevel,
    OrderBookSnapshot,
    Quote,
)
from app.market.validators import validate_raw_bar_dict, validate_raw_quote_dict


def parse_timestamp(value: Any) -> float:
    """Safely parse various timestamp formats into a POSIX UTC float.

    Supports:
    - POSIX int or float (seconds or milliseconds)
    - ISO-8601 strings (e.g., '2026-10-01T10:00:00Z', '2026-10-01 10:00:00')

    Args:
        value: Timestamp in raw format.

    Returns:
        float: POSIX UTC timestamp in seconds.

    Raises:
        ValidationError: If timestamp cannot be parsed or is invalid.
    """
    if value is None:
        raise ValidationError("Missing timestamp: market data timestamps cannot be omitted or faked.")

    if isinstance(value, (int, float)):
        ts = float(value)
        # If timestamp is in milliseconds (greater than year 2100 in seconds, e.g. > 4102444800)
        if ts > 4102444800:
            ts = ts / 1000.0
        if ts <= 0:
            raise ValidationError(f"Invalid timestamp value: {value}")
        return ts

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            raise ValidationError("Empty timestamp string.")

        # Check if it's a numeric string first
        try:
            return parse_timestamp(float(cleaned))
        except ValueError:
            pass

        # Try parsing ISO format
        try:
            # Replace trailing 'Z' for fromisoformat compatibility
            iso_str = cleaned[:-1] + "+00:00" if cleaned.endswith("Z") else cleaned
            dt = datetime.fromisoformat(iso_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError as err:
            raise ValidationError(f"Could not parse timestamp string '{value}': {err}") from err

    raise ValidationError(f"Unsupported timestamp type: {type(value).__name__}")


def parse_float(value: Any, field_name: str, allow_none: bool = False) -> Optional[float]:
    """Parse a numeric value into a float, preserving None when allowed."""
    if value is None or (isinstance(value, str) and not value.strip()):
        if allow_none:
            return None
        raise ValidationError(f"Field '{field_name}' cannot be None or empty.")

    try:
        return float(value)
    except (ValueError, TypeError) as err:
        raise ValidationError(f"Field '{field_name}' must be numeric, got '{value}': {err}") from err


class MarketDataNormalizer:
    """Normalizes raw feed records into standardized domain models."""

    @staticmethod
    def normalize_quote(raw: Dict[str, Any], default_source: str = "unknown") -> Quote:
        """Normalize a raw dictionary into a standardized Quote model.

        Args:
            raw: Dictionary containing raw quote fields.
            default_source: Fallback source name if not provided in raw record.

        Returns:
            Quote: Validated immutable quote model.

        Raises:
            ValidationError: If schema, types, or price relations are invalid.
        """
        validate_raw_quote_dict(raw)

        # Extract symbol
        symbol = str(
            raw.get("symbol")
            or raw.get("Symbol")
            or raw.get("ticker")
            or raw.get("instrument")
        )

        exchange = str(raw.get("exchange") or raw.get("Exchange") or "NSE")
        source = str(raw.get("source") or default_source)
        currency = str(raw.get("currency") or "INR")

        # Timestamp
        raw_ts = raw.get("timestamp") or raw.get("Timestamp") or raw.get("time")
        timestamp = parse_timestamp(raw_ts)

        # Prices
        raw_price = (
            raw.get("last_price")
            if "last_price" in raw
            else raw.get("lastPrice")
            if "lastPrice" in raw
            else raw.get("price")
            if "price" in raw
            else raw.get("ltp")
            if "ltp" in raw
            else raw.get("close")
        )
        last_price = parse_float(raw_price, "last_price", allow_none=False)
        assert last_price is not None

        bid = parse_float(raw.get("bid") or raw.get("best_bid"), "bid", allow_none=True)
        ask = parse_float(raw.get("ask") or raw.get("best_ask"), "ask", allow_none=True)
        bid_size = parse_float(raw.get("bid_size") or raw.get("bidSize"), "bid_size", allow_none=True)
        ask_size = parse_float(raw.get("ask_size") or raw.get("askSize"), "ask_size", allow_none=True)
        volume = parse_float(raw.get("volume") or raw.get("Volume") or 0.0, "volume", allow_none=False)
        assert volume is not None

        return Quote(
            symbol=symbol,
            exchange=exchange,
            timestamp=timestamp,
            last_price=last_price,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            volume=volume,
            currency=currency,
            source=source,
        )

    @staticmethod
    def normalize_bar(raw: Dict[str, Any], default_source: str = "unknown") -> Bar:
        """Normalize a raw dictionary into a standardized Bar model.

        Args:
            raw: Dictionary containing raw candlestick fields.
            default_source: Fallback source name.

        Returns:
            Bar: Validated immutable OHLCV bar.

        Raises:
            ValidationError: If OHLC relationships or schema are violated.
        """
        validate_raw_bar_dict(raw)

        symbol = str(
            raw.get("symbol")
            or raw.get("Symbol")
            or raw.get("ticker")
            or raw.get("instrument")
        )
        exchange = str(raw.get("exchange") or raw.get("Exchange") or "NSE")
        timeframe = str(raw.get("timeframe") or raw.get("resolution") or "1m")
        source = str(raw.get("source") or default_source)

        raw_ts = raw.get("timestamp") or raw.get("Timestamp") or raw.get("time")
        timestamp = parse_timestamp(raw_ts)

        open_price = parse_float(raw.get("open") or raw.get("Open"), "open", allow_none=False)
        high_price = parse_float(raw.get("high") or raw.get("High"), "high", allow_none=False)
        low_price = parse_float(raw.get("low") or raw.get("Low"), "low", allow_none=False)
        close_price = parse_float(raw.get("close") or raw.get("Close"), "close", allow_none=False)
        volume = parse_float(raw.get("volume") or raw.get("Volume") or 0.0, "volume", allow_none=False)

        assert open_price is not None and high_price is not None and low_price is not None and close_price is not None and volume is not None

        return Bar(
            symbol=symbol,
            exchange=exchange,
            timeframe=timeframe,
            timestamp=timestamp,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            source=source,
        )

    @staticmethod
    def normalize_order_book(raw: Dict[str, Any], default_source: str = "unknown") -> OrderBookSnapshot:
        """Normalize a raw dictionary into a standardized OrderBookSnapshot.

        Args:
            raw: Dictionary containing raw market depth data.
            default_source: Fallback source tag.

        Returns:
            OrderBookSnapshot: Validated immutable depth snapshot.
        """
        if not isinstance(raw, dict):
            raise ValidationError(f"Raw order book data must be a dictionary, got {type(raw).__name__}")

        symbol = str(raw.get("symbol") or raw.get("Symbol") or "")
        exchange = str(raw.get("exchange") or "NSE")
        source = str(raw.get("source") or default_source)
        raw_ts = raw.get("timestamp") or raw.get("time")
        timestamp = parse_timestamp(raw_ts)

        raw_bids = raw.get("bids", [])
        raw_asks = raw.get("asks", [])

        def parse_levels(levels: Any) -> Tuple[OrderBookLevel, ...]:
            parsed = []
            for item in levels:
                if isinstance(item, OrderBookLevel):
                    parsed.append(item)
                elif isinstance(item, dict):
                    p = parse_float(item.get("price"), "level.price")
                    q = parse_float(item.get("quantity") or item.get("size") or item.get("qty"), "level.quantity")
                    c = int(item["orders_count"]) if "orders_count" in item else None
                    assert p is not None and q is not None
                    parsed.append(OrderBookLevel(price=p, quantity=q, orders_count=c))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    p = parse_float(item[0], "level.price")
                    q = parse_float(item[1], "level.quantity")
                    c = int(item[2]) if len(item) > 2 else None
                    assert p is not None and q is not None
                    parsed.append(OrderBookLevel(price=p, quantity=q, orders_count=c))
                else:
                    raise ValidationError(f"Unrecognized order book level format: {item}")
            return tuple(parsed)

        return OrderBookSnapshot(
            symbol=symbol,
            exchange=exchange,
            timestamp=timestamp,
            bids=parse_levels(raw_bids),
            asks=parse_levels(raw_asks),
            source=source,
        )

    @staticmethod
    def normalize_order_book_delta(raw: Dict[str, Any], default_source: str = "unknown") -> OrderBookDelta:
        """Normalize a raw dictionary into a validated immutable OrderBookDelta.

        Args:
            raw: Dictionary containing raw order book delta fields.
            default_source: Fallback source name.

        Returns:
            OrderBookDelta: Validated immutable delta.
        """
        if not isinstance(raw, dict):
            raise ValidationError(f"Raw delta data must be a dictionary, got {type(raw).__name__}")

        symbol = str(raw.get("symbol") or raw.get("Symbol") or raw.get("ticker") or "")
        exchange = str(raw.get("exchange") or "NSE")
        source = str(raw.get("source") or default_source)

        raw_side = str(raw.get("side") or raw.get("Side") or "BID").strip().upper()
        raw_action = str(raw.get("action") or raw.get("operation") or raw.get("type") or "UPDATE").strip().upper()

        raw_price = raw.get("price") if "price" in raw else raw.get("level")
        price = parse_float(raw_price, "price")
        assert price is not None

        raw_qty = raw.get("quantity") if "quantity" in raw else raw.get("size") if "size" in raw else raw.get("qty", 0.0)
        quantity = parse_float(raw_qty, "quantity")
        assert quantity is not None

        raw_ts = raw.get("timestamp") or raw.get("time") or time.time()
        timestamp = parse_timestamp(raw_ts)

        seq = int(raw["sequence_number"]) if "sequence_number" in raw and raw["sequence_number"] is not None else None
        if seq is None and "sequence" in raw and raw["sequence"] is not None:
            seq = int(raw["sequence"])
        if seq is None and "seq" in raw and raw["seq"] is not None:
            seq = int(raw["seq"])

        orders_count = int(raw["orders_count"]) if "orders_count" in raw and raw["orders_count"] is not None else None

        return OrderBookDelta(
            symbol=symbol,
            side=BookSide(raw_side),
            price=price,
            quantity=quantity,
            action=DeltaAction(raw_action),
            timestamp=timestamp,
            sequence_number=seq,
            orders_count=orders_count,
            exchange=exchange,
            source=source,
        )
