"""KAISER — Level 2 Market Depth Analytics & Imbalance Indicators.

Provides deterministic, mathematical microstructure analytics computed from
reconstructed Level 2 order book snapshots:
1. Order Book Imbalance (OBI): Configurable depth levels (k=1, 5, 10, all),
   bounded strictly in [-1.0, 1.0], zero-division protected, categorical pressure classification.
2. Cumulative Depth Analytics & Depth Slope: Quantity accumulation distributions and
   price-slope metrics (Delta Q / Delta P) on bid and ask ladders.
3. Micro-Price (Stoikov): Volume-weighted top-of-book fair price, crossed/locked book rejection,
   safe zero-liquidity handling.
4. Weighted Spread Analytics: Absolute spread, relative spread in basis points (bps),
   and volume-weighted average price (VWAP) spreads across depth levels.
5. MarketDepthAnalytics container: Comprehensive, immutable snapshot analytics.
6. L2DepthAnalyticsEngine: Thread-safe, event-driven depth analytics coordinator.

STRICT INVARIANTS:
- Analytical only: Zero trade recommendations, order placement, or execution fields.
- Mathematical safety: Bounded in valid intervals, never produces NaN, Inf, or ZeroDivisionError.
- Fail-closed: Crossed or invalid books are rejected and clearly marked is_valid=False.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.market.models import OrderBookLevel, OrderBookSnapshot

logger = get_logger("market.depth_analytics")


class ImbalanceInterpretation(str, Enum):
    """Categorical classification of order book imbalance."""

    BUY_PRESSURE = "BUY_PRESSURE"
    SELL_PRESSURE = "SELL_PRESSURE"
    NEUTRAL = "NEUTRAL"


@dataclass(frozen=True)
class OrderBookImbalance:
    """Immutable order book imbalance (OBI) metric.

    Formula:
        OBI = (Sum(Q_bids) - Sum(Q_asks)) / (Sum(Q_bids) + Sum(Q_asks))

    Attributes:
        symbol: Ticker symbol.
        value: Imbalance value in range [-1.0, 1.0].
        bid_quantity: Cumulative bid quantity across evaluated depth levels.
        ask_quantity: Cumulative ask quantity across evaluated depth levels.
        depth_levels: Number of price levels evaluated.
        interpretation: BUY_PRESSURE, SELL_PRESSURE, or NEUTRAL.
        timestamp: POSIX UTC calculation timestamp.
    """

    symbol: str
    value: float
    bid_quantity: float
    ask_quantity: float
    depth_levels: int
    interpretation: ImbalanceInterpretation
    timestamp: float

    def __post_init__(self) -> None:
        """Validate bounds and invariants fail-closed."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("OrderBookImbalance symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not math.isfinite(self.value) or self.value < -1.0 or self.value > 1.0:
            raise ValidationError(f"OrderBookImbalance value must be a finite float in [-1.0, 1.0], got {self.value}")

        if not math.isfinite(self.bid_quantity) or self.bid_quantity < 0:
            raise ValidationError(f"OrderBookImbalance bid_quantity cannot be negative, got {self.bid_quantity}")

        if not math.isfinite(self.ask_quantity) or self.ask_quantity < 0:
            raise ValidationError(f"OrderBookImbalance ask_quantity cannot be negative, got {self.ask_quantity}")

        if self.depth_levels <= 0:
            raise ValidationError(f"OrderBookImbalance depth_levels must be positive, got {self.depth_levels}")

        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"OrderBookImbalance timestamp must be positive, got {self.timestamp}")

        if isinstance(self.interpretation, str):
            clean_interp = self.interpretation.strip().upper()
            if clean_interp not in ("BUY_PRESSURE", "SELL_PRESSURE", "NEUTRAL"):
                raise ValidationError(f"Invalid interpretation: {self.interpretation}")
            object.__setattr__(self, "interpretation", ImbalanceInterpretation(clean_interp))
        elif not isinstance(self.interpretation, ImbalanceInterpretation):
            raise ValidationError(f"interpretation must be ImbalanceInterpretation, got {type(self.interpretation)}")

    @property
    def is_buy_pressure(self) -> bool:
        """True if order book indicates net buying pressure."""
        return self.interpretation == ImbalanceInterpretation.BUY_PRESSURE

    @property
    def is_sell_pressure(self) -> bool:
        """True if order book indicates net selling pressure."""
        return self.interpretation == ImbalanceInterpretation.SELL_PRESSURE

    @property
    def is_neutral(self) -> bool:
        """True if order book is balanced."""
        return self.interpretation == ImbalanceInterpretation.NEUTRAL

    @property
    def total_quantity(self) -> float:
        """Total evaluated depth quantity (bids + asks)."""
        return round(self.bid_quantity + self.ask_quantity, 4)

    @property
    def iso_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a JSON-serializable dictionary."""
        return {
            "symbol": self.symbol,
            "value": self.value,
            "bid_quantity": self.bid_quantity,
            "ask_quantity": self.ask_quantity,
            "total_quantity": self.total_quantity,
            "depth_levels": self.depth_levels,
            "interpretation": self.interpretation.value,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
        }


@dataclass(frozen=True)
class DepthSlope:
    """Cumulative depth slope metrics representing price-elasticity of liquidity.

    Formula:
        bid_slope = Sum(Q_bids) / |P_best_bid - P_deepest_bid|
        ask_slope = Sum(Q_asks) / |P_deepest_ask - P_best_ask|
        ratio = (bid_slope - ask_slope) / (bid_slope + ask_slope)
    """

    bid_slope: float
    ask_slope: float
    ratio: float
    bid_depth_span: float
    ask_depth_span: float

    def __post_init__(self) -> None:
        """Validate depth slope invariants."""
        if not math.isfinite(self.bid_slope) or self.bid_slope < 0:
            raise ValidationError(f"bid_slope must be non-negative finite float, got {self.bid_slope}")
        if not math.isfinite(self.ask_slope) or self.ask_slope < 0:
            raise ValidationError(f"ask_slope must be non-negative finite float, got {self.ask_slope}")
        if not math.isfinite(self.ratio) or self.ratio < -1.0 or self.ratio > 1.0:
            raise ValidationError(f"ratio must be finite float in [-1.0, 1.0], got {self.ratio}")
        if not math.isfinite(self.bid_depth_span) or self.bid_depth_span < 0:
            raise ValidationError(f"bid_depth_span must be non-negative, got {self.bid_depth_span}")
        if not math.isfinite(self.ask_depth_span) or self.ask_depth_span < 0:
            raise ValidationError(f"ask_depth_span must be non-negative, got {self.ask_depth_span}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a serializable dictionary."""
        return {
            "bid_slope": self.bid_slope,
            "ask_slope": self.ask_slope,
            "ratio": self.ratio,
            "bid_depth_span": self.bid_depth_span,
            "ask_depth_span": self.ask_depth_span,
        }


@dataclass(frozen=True)
class MicroPrice:
    """Volume-weighted top-of-book fair price (Stoikov micro-price).

    Formula:
        MicroPrice = (P_ask * Q_bid + P_bid * Q_ask) / (Q_bid + Q_ask)

    Attributes:
        price: Micro-price value, or None if book is invalid/crossed.
        best_bid: Top of book bid price.
        best_ask: Top of book ask price.
        bid_weight: Normalized weight of bid quantity in [0.0, 1.0].
        ask_weight: Normalized weight of ask quantity in [0.0, 1.0].
        spread: Absolute top-of-book spread.
        mid_price: Simple mid price (best_bid + best_ask) / 2.
        is_valid: False if crossed, locked, or zero-liquidity prevented calculation.
    """

    price: Optional[float]
    best_bid: Optional[float]
    best_ask: Optional[float]
    bid_weight: float
    ask_weight: float
    spread: Optional[float]
    mid_price: Optional[float]
    is_valid: bool = True

    def __post_init__(self) -> None:
        """Validate micro-price invariants."""
        if self.price is not None and (not math.isfinite(self.price) or self.price <= 0):
            raise ValidationError(f"MicroPrice price must be positive finite float or None, got {self.price}")
        if not math.isfinite(self.bid_weight) or self.bid_weight < 0 or self.bid_weight > 1.0:
            raise ValidationError(f"bid_weight must be in [0.0, 1.0], got {self.bid_weight}")
        if not math.isfinite(self.ask_weight) or self.ask_weight < 0 or self.ask_weight > 1.0:
            raise ValidationError(f"ask_weight must be in [0.0, 1.0], got {self.ask_weight}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a serializable dictionary."""
        return {
            "price": self.price,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "bid_weight": self.bid_weight,
            "ask_weight": self.ask_weight,
            "spread": self.spread,
            "mid_price": self.mid_price,
            "is_valid": self.is_valid,
        }


@dataclass(frozen=True)
class WeightedSpread:
    """Absolute, relative, and volume-weighted spread metrics across depth levels.

    Attributes:
        absolute_spread: Top-of-book spread (best_ask - best_bid).
        relative_spread_bps: Relative spread in basis points ((best_ask - best_bid) / mid) * 10000.
        volume_weighted_spread: Difference between ask VWAP and bid VWAP across evaluated levels.
        vwap_bid: Volume-weighted average bid price across levels.
        vwap_ask: Volume-weighted average ask price across levels.
        depth_levels: Number of price levels included in VWAP calculations.
    """

    absolute_spread: Optional[float]
    relative_spread_bps: Optional[float]
    volume_weighted_spread: Optional[float]
    vwap_bid: Optional[float]
    vwap_ask: Optional[float]
    depth_levels: int

    def __post_init__(self) -> None:
        """Validate spread metrics."""
        if self.depth_levels <= 0:
            raise ValidationError(f"depth_levels must be positive, got {self.depth_levels}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a serializable dictionary."""
        return {
            "absolute_spread": self.absolute_spread,
            "relative_spread_bps": self.relative_spread_bps,
            "volume_weighted_spread": self.volume_weighted_spread,
            "vwap_bid": self.vwap_bid,
            "vwap_ask": self.vwap_ask,
            "depth_levels": self.depth_levels,
        }


@dataclass(frozen=True)
class MarketDepthAnalytics:
    """Immutable aggregate depth analytics model for a single order book snapshot.

    Attributes:
        symbol: Ticker symbol.
        exchange: Exchange identifier.
        timestamp: Calculation timestamp (POSIX UTC).
        depth_levels: Number of price levels evaluated.
        imbalance: Calculated OrderBookImbalance.
        micro_price: Calculated MicroPrice if available.
        depth_slope: Calculated DepthSlope if available.
        weighted_spread: Calculated WeightedSpread if available.
        cumulative_bid_quantity: Total bid quantity evaluated.
        cumulative_ask_quantity: Total ask quantity evaluated.
        is_valid: False if snapshot was uncrossed, invalid, or corrupted.
        error: Diagnostic error message if calculation failed or degraded.
    """

    symbol: str
    exchange: str
    timestamp: float
    depth_levels: int
    imbalance: OrderBookImbalance
    micro_price: Optional[MicroPrice] = None
    depth_slope: Optional[DepthSlope] = None
    weighted_spread: Optional[WeightedSpread] = None
    cumulative_bid_quantity: float = 0.0
    cumulative_ask_quantity: float = 0.0
    is_valid: bool = True
    error: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate depth analytics aggregate invariants."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("MarketDepthAnalytics symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not self.exchange or not isinstance(self.exchange, str) or not self.exchange.strip():
            raise ValidationError("MarketDepthAnalytics exchange must be a non-empty string.")
        object.__setattr__(self, "exchange", self.exchange.strip().upper())

        if self.timestamp <= 0:
            raise ValidationError(f"MarketDepthAnalytics timestamp must be positive, got {self.timestamp}")

        if self.depth_levels <= 0:
            raise ValidationError(f"MarketDepthAnalytics depth_levels must be positive, got {self.depth_levels}")

    @property
    def iso_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert into a JSON-serializable dictionary."""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "depth_levels": self.depth_levels,
            "imbalance": self.imbalance.to_dict(),
            "micro_price": self.micro_price.to_dict() if self.micro_price else None,
            "depth_slope": self.depth_slope.to_dict() if self.depth_slope else None,
            "weighted_spread": self.weighted_spread.to_dict() if self.weighted_spread else None,
            "cumulative_bid_quantity": self.cumulative_bid_quantity,
            "cumulative_ask_quantity": self.cumulative_ask_quantity,
            "is_valid": self.is_valid,
            "error": self.error,
        }


# =============================================================================
# Functional Calculation Engines (Deterministic & Pure Math)
# =============================================================================

def calculate_order_book_imbalance(
    snapshot: OrderBookSnapshot,
    depth_limit: Optional[int] = None,
    imbalance_threshold: float = 0.05,
) -> OrderBookImbalance:
    """Calculate Order Book Imbalance (OBI) across top-k price levels.

    Formula:
        OBI = (Sum(Q_bids) - Sum(Q_asks)) / (Sum(Q_bids) + Sum(Q_asks))

    Guarantees:
    - Strictly bounded in [-1.0, 1.0].
    - Zero liquidity safely returns 0.0 and NEUTRAL (no ZeroDivisionError).
    - Rejects non-finite quantities.

    Args:
        snapshot: Validated OrderBookSnapshot.
        depth_limit: Number of levels from top of book (default: all available, minimum 1).
        imbalance_threshold: Threshold above/below which pressure is classified (default 0.05).

    Returns:
        OrderBookImbalance: Immutable domain model.
    """
    levels = depth_limit if depth_limit is not None and depth_limit > 0 else max(len(snapshot.bids), len(snapshot.asks), 1)
    
    bids_slice = snapshot.bids[:levels] if snapshot.bids else ()
    asks_slice = snapshot.asks[:levels] if snapshot.asks else ()

    sum_bids = sum(b.quantity for b in bids_slice if math.isfinite(b.quantity) and b.quantity > 0)
    sum_asks = sum(a.quantity for a in asks_slice if math.isfinite(a.quantity) and a.quantity > 0)
    total_qty = sum_bids + sum_asks

    if total_qty <= 0.0 or not math.isfinite(total_qty):
        val = 0.0
        interp = ImbalanceInterpretation.NEUTRAL
    else:
        raw_val = (sum_bids - sum_asks) / total_qty
        val = round(max(-1.0, min(1.0, raw_val)), 6)
        if val > imbalance_threshold:
            interp = ImbalanceInterpretation.BUY_PRESSURE
        elif val < -imbalance_threshold:
            interp = ImbalanceInterpretation.SELL_PRESSURE
        else:
            interp = ImbalanceInterpretation.NEUTRAL

    return OrderBookImbalance(
        symbol=snapshot.symbol,
        value=val,
        bid_quantity=round(sum_bids, 4),
        ask_quantity=round(sum_asks, 4),
        depth_levels=levels,
        interpretation=interp,
        timestamp=snapshot.timestamp,
    )


def calculate_micro_price(snapshot: OrderBookSnapshot) -> MicroPrice:
    """Calculate Stoikov volume-weighted micro-price at top-of-book.

    Formula:
        MicroPrice = (P_ask * Q_bid + P_bid * Q_ask) / (Q_bid + Q_ask)

    Guarantees:
    - Fails closed on crossed or locked books (best_bid >= best_ask).
    - Safe zero-liquidity handling (falls back to mid-price or marks invalid).
    - Returns weights in [0.0, 1.0].

    Args:
        snapshot: OrderBookSnapshot to evaluate.

    Returns:
        MicroPrice: Immutable domain model with validity flag.
    """
    best_bid = snapshot.best_bid
    best_ask = snapshot.best_ask

    # Guard: Missing book sides or snapshot flagged invalid
    if not snapshot.is_valid or best_bid is None or best_ask is None:
        return MicroPrice(
            price=None,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_weight=0.5,
            ask_weight=0.5,
            spread=round(best_ask - best_bid, 4) if (best_bid is not None and best_ask is not None) else None,
            mid_price=round((best_bid + best_ask) / 2.0, 4) if (best_bid is not None and best_ask is not None) else None,
            is_valid=False,
        )

    # Guard: Crossed or locked book (best_bid >= best_ask)
    if best_bid >= best_ask:
        logger.warning(
            "MicroPrice rejected crossed/locked book for %s: bid=%.4f >= ask=%.4f",
            snapshot.symbol,
            best_bid,
            best_ask,
        )
        return MicroPrice(
            price=None,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_weight=0.5,
            ask_weight=0.5,
            spread=round(best_ask - best_bid, 4),
            mid_price=round((best_bid + best_ask) / 2.0, 4),
            is_valid=False,
        )

    bid_qty = snapshot.bids[0].quantity if snapshot.bids else 0.0
    ask_qty = snapshot.asks[0].quantity if snapshot.asks else 0.0
    total_qty = bid_qty + ask_qty
    mid = round((best_bid + best_ask) / 2.0, 4)
    spread = round(best_ask - best_bid, 4)

    if total_qty <= 0.0 or not math.isfinite(total_qty):
        # Zero top-of-book liquidity fallback to mid-price
        return MicroPrice(
            price=mid,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_weight=0.5,
            ask_weight=0.5,
            spread=spread,
            mid_price=mid,
            is_valid=True,
        )

    bid_w = bid_qty / total_qty
    ask_w = ask_qty / total_qty
    micro_p = (best_ask * bid_qty + best_bid * ask_qty) / total_qty

    return MicroPrice(
        price=round(micro_p, 4),
        best_bid=best_bid,
        best_ask=best_ask,
        bid_weight=round(bid_w, 4),
        ask_weight=round(ask_w, 4),
        spread=spread,
        mid_price=mid,
        is_valid=True,
    )


def calculate_depth_slope(
    snapshot: OrderBookSnapshot,
    depth_limit: Optional[int] = None,
) -> DepthSlope:
    """Calculate cumulative depth slope (Delta Q / Delta P) across bid and ask ladders.

    Represents book resilience and liquidity density away from top of book.

    Args:
        snapshot: OrderBookSnapshot to evaluate.
        depth_limit: Levels to evaluate from top of book.

    Returns:
        DepthSlope: Calculated bid slope, ask slope, and normalized ratio.
    """
    levels = depth_limit if depth_limit is not None and depth_limit > 0 else 10

    bids_slice = snapshot.bids[:levels] if snapshot.bids else ()
    asks_slice = snapshot.asks[:levels] if snapshot.asks else ()

    # Bid slope
    bid_slope = 0.0
    bid_span = 0.0
    if len(bids_slice) >= 2:
        best_bid = bids_slice[0].price
        deepest_bid = bids_slice[-1].price
        bid_span = round(best_bid - deepest_bid, 4)
        total_bid_qty = sum(b.quantity for b in bids_slice)
        if bid_span > 0:
            bid_slope = round(total_bid_qty / bid_span, 4)

    # Ask slope
    ask_slope = 0.0
    ask_span = 0.0
    if len(asks_slice) >= 2:
        best_ask = asks_slice[0].price
        deepest_ask = asks_slice[-1].price
        ask_span = round(deepest_ask - best_ask, 4)
        total_ask_qty = sum(a.quantity for a in asks_slice)
        if ask_span > 0:
            ask_slope = round(total_ask_qty / ask_span, 4)

    # Normalized slope ratio
    total_slope = bid_slope + ask_slope
    if total_slope > 0:
        ratio = round((bid_slope - ask_slope) / total_slope, 6)
        ratio = max(-1.0, min(1.0, ratio))
    else:
        ratio = 0.0

    return DepthSlope(
        bid_slope=bid_slope,
        ask_slope=ask_slope,
        ratio=ratio,
        bid_depth_span=bid_span,
        ask_depth_span=ask_span,
    )


def calculate_weighted_spread(
    snapshot: OrderBookSnapshot,
    depth_limit: Optional[int] = None,
) -> WeightedSpread:
    """Calculate absolute, relative (bps), and volume-weighted depth spreads.

    Formula:
        absolute_spread = best_ask - best_bid
        relative_spread_bps = (absolute_spread / mid_price) * 10000
        vwap_spread = vwap_ask - vwap_bid

    Args:
        snapshot: OrderBookSnapshot to evaluate.
        depth_limit: Levels to evaluate from top of book.

    Returns:
        WeightedSpread: Comprehensive spread metrics.
    """
    levels = depth_limit if depth_limit is not None and depth_limit > 0 else 5

    best_bid = snapshot.best_bid
    best_ask = snapshot.best_ask

    abs_spread = round(best_ask - best_bid, 4) if (best_bid is not None and best_ask is not None) else None
    rel_spread_bps = None
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0
        if mid > 0:
            rel_spread_bps = round(((best_ask - best_bid) / mid) * 10000.0, 2)

    # Compute VWAP across levels
    bids_slice = snapshot.bids[:levels] if snapshot.bids else ()
    asks_slice = snapshot.asks[:levels] if snapshot.asks else ()

    total_bid_val = sum(b.price * b.quantity for b in bids_slice)
    total_bid_qty = sum(b.quantity for b in bids_slice)
    vwap_bid = round(total_bid_val / total_bid_qty, 4) if total_bid_qty > 0 else best_bid

    total_ask_val = sum(a.price * a.quantity for a in asks_slice)
    total_ask_qty = sum(a.quantity for a in asks_slice)
    vwap_ask = round(total_ask_val / total_ask_qty, 4) if total_ask_qty > 0 else best_ask

    vwap_spread = None
    if vwap_bid is not None and vwap_ask is not None:
        vwap_spread = round(vwap_ask - vwap_bid, 4)

    return WeightedSpread(
        absolute_spread=abs_spread,
        relative_spread_bps=rel_spread_bps,
        volume_weighted_spread=vwap_spread,
        vwap_bid=vwap_bid,
        vwap_ask=vwap_ask,
        depth_levels=levels,
    )


def compute_depth_analytics(
    snapshot: OrderBookSnapshot,
    depth_limit: int = 10,
    imbalance_threshold: float = 0.05,
) -> MarketDepthAnalytics:
    """Compute comprehensive Level 2 market depth analytics for a snapshot.

    Fail-closed:
    If snapshot is crossed or invalid, marks output is_valid=False with diagnostic error.

    Args:
        snapshot: Reconstructed OrderBookSnapshot.
        depth_limit: Depth evaluation horizon (default 10).
        imbalance_threshold: Sensitivity threshold for OBI pressure categorization.

    Returns:
        MarketDepthAnalytics: Immutable aggregate depth analytics record.
    """
    levels = depth_limit if depth_limit > 0 else 10
    calc_ts = snapshot.timestamp if snapshot.timestamp > 0 else time.time()

    # Fail-closed check: snapshot validity and crossed book detection
    best_bid = snapshot.best_bid
    best_ask = snapshot.best_ask
    is_crossed = (best_bid is not None and best_ask is not None and best_bid >= best_ask)

    if not snapshot.is_valid or is_crossed:
        err_msg = "Order book is crossed or marked invalid" if is_crossed else "OrderBookSnapshot is marked invalid"
        imbalance = calculate_order_book_imbalance(snapshot, depth_limit=levels, imbalance_threshold=imbalance_threshold)
        micro_price = calculate_micro_price(snapshot)
        depth_slope = calculate_depth_slope(snapshot, depth_limit=levels)
        weighted_spread = calculate_weighted_spread(snapshot, depth_limit=levels)

        return MarketDepthAnalytics(
            symbol=snapshot.symbol,
            exchange=snapshot.exchange,
            timestamp=calc_ts,
            depth_levels=levels,
            imbalance=imbalance,
            micro_price=micro_price,
            depth_slope=depth_slope,
            weighted_spread=weighted_spread,
            cumulative_bid_quantity=imbalance.bid_quantity,
            cumulative_ask_quantity=imbalance.ask_quantity,
            is_valid=False,
            error=err_msg,
        )

    # Compute valid analytics
    imbalance = calculate_order_book_imbalance(snapshot, depth_limit=levels, imbalance_threshold=imbalance_threshold)
    micro_price = calculate_micro_price(snapshot)
    depth_slope = calculate_depth_slope(snapshot, depth_limit=levels)
    weighted_spread = calculate_weighted_spread(snapshot, depth_limit=levels)

    return MarketDepthAnalytics(
        symbol=snapshot.symbol,
        exchange=snapshot.exchange,
        timestamp=calc_ts,
        depth_levels=levels,
        imbalance=imbalance,
        micro_price=micro_price,
        depth_slope=depth_slope,
        weighted_spread=weighted_spread,
        cumulative_bid_quantity=imbalance.bid_quantity,
        cumulative_ask_quantity=imbalance.ask_quantity,
        is_valid=True,
        error=None,
    )


class AdaptiveDepthProfile(int, Enum):
    """Predefined standard depth horizons for adaptive analytical evaluation."""

    TOP_OF_BOOK = 1
    SHALLOW = 5
    MEDIUM = 10
    DEEP = 20
    MAXIMUM = 50


def compute_adaptive_depth_analytics(
    snapshot: OrderBookSnapshot,
    requested_levels: int = 10,
    min_levels: int = 1,
    imbalance_threshold: float = 0.05,
) -> MarketDepthAnalytics:
    """Compute market depth analytics with adaptive level selection.

    Safely handles order books with fewer levels than requested:
    - If requested levels exceed available levels in snapshot, adapts gracefully
      to the maximum available levels (at least min_levels).
    - Preserves all snapshot validation, crossed-book detection, and fail-closed guarantees.
    - Read-only: Never modifies the underlying snapshot or generates orders.

    Args:
        snapshot: OrderBookSnapshot to evaluate.
        requested_levels: Target depth horizon.
        min_levels: Minimum levels to evaluate (default 1).
        imbalance_threshold: Categorical pressure threshold.

    Returns:
        MarketDepthAnalytics: Evaluated analytics with adaptive depth horizon.
    """
    avail_bids = len(snapshot.bids) if snapshot.bids else 0
    avail_asks = len(snapshot.asks) if snapshot.asks else 0
    max_avail = max(avail_bids, avail_asks)

    if requested_levels <= 0:
        effective_levels = max(max_avail, min_levels)
    elif max_avail > 0:
        effective_levels = max(min(requested_levels, max_avail), min_levels)
    else:
        effective_levels = max(min_levels, 1)

    return compute_depth_analytics(
        snapshot=snapshot,
        depth_limit=effective_levels,
        imbalance_threshold=imbalance_threshold,
    )


# =============================================================================
# Stateful & Event-Driven Depth Analytics Engine
# =============================================================================

class L2DepthAnalyticsEngine:
    """Thread-safe, deterministic Level 2 market depth analytics coordinator.

    Maintains the latest evaluated MarketDepthAnalytics per instrument,
    optionally emits depth analytics events over the KAISER EventBus,
    and exposes a read-only query interface for downstream analysis engines.
    """

    def __init__(
        self,
        default_depth_limit: int = 10,
        imbalance_threshold: float = 0.05,
        event_bus: Optional[Any] = None,
        name: str = "l2_depth_analytics_engine",
    ) -> None:
        self.name = name
        if default_depth_limit <= 0:
            raise ValidationError(f"default_depth_limit must be positive, got {default_depth_limit}")
        self.default_depth_limit = default_depth_limit
        self.imbalance_threshold = imbalance_threshold
        self.event_bus = event_bus

        self._lock = threading.RLock()
        self._cache: Dict[str, MarketDepthAnalytics] = {}
        self._history: Dict[str, List[MarketDepthAnalytics]] = {}
        self._max_history = 100
        self._total_evaluated = 0

    @property
    def total_evaluated(self) -> int:
        """Count of total snapshots evaluated."""
        with self._lock:
            return self._total_evaluated

    def get_latest(self, symbol: str) -> Optional[MarketDepthAnalytics]:
        """Retrieve most recent MarketDepthAnalytics for symbol."""
        with self._lock:
            return self._cache.get(symbol.strip().upper())

    def get_history(self, symbol: str, limit: int = 20) -> List[MarketDepthAnalytics]:
        """Retrieve historical analytics for symbol."""
        with self._lock:
            sym = symbol.strip().upper()
            hist = self._history.get(sym, [])
            return list(hist[-limit:])

    def evaluate_snapshot(
        self,
        snapshot: OrderBookSnapshot,
        depth_limit: Optional[int] = None,
    ) -> MarketDepthAnalytics:
        """Evaluate depth analytics for an incoming OrderBookSnapshot.

        Thread-safe, updates in-memory cache, and dispatches event if bus attached.

        Args:
            snapshot: OrderBookSnapshot to process.
            depth_limit: Optional override for depth horizon.

        Returns:
            MarketDepthAnalytics: Computed analytics model.
        """
        levels = depth_limit if (depth_limit is not None and depth_limit > 0) else self.default_depth_limit
        analytics = compute_depth_analytics(
            snapshot=snapshot,
            depth_limit=levels,
            imbalance_threshold=self.imbalance_threshold,
        )

        with self._lock:
            sym = analytics.symbol
            self._cache[sym] = analytics
            if sym not in self._history:
                self._history[sym] = []
            self._history[sym].append(analytics)
            if len(self._history[sym]) > self._max_history:
                self._history[sym].pop(0)
            self._total_evaluated += 1

        # Publish event if event bus attached
        if self.event_bus is not None:
            try:
                from app.core.events import Event, EventType
                # Use MARKET_DEPTH_UPDATED with analytics payload or MARKET_DEPTH_ANALYTICS_UPDATED
                evt_type = getattr(EventType, "MARKET_DEPTH_ANALYTICS_UPDATED", EventType.MARKET_DEPTH_UPDATED)
                event = Event.create(
                    event_type=evt_type,
                    payload=analytics.to_dict(),
                    source=self.name,
                )
                self.event_bus.publish(event)
            except Exception as err:
                logger.error("Failed to publish depth analytics event: %s", err)

        return analytics

    def evaluate_adaptive_snapshot(
        self,
        snapshot: OrderBookSnapshot,
        requested_levels: int = 10,
        min_levels: int = 1,
    ) -> MarketDepthAnalytics:
        """Evaluate depth analytics using adaptive depth selection based on book depth.

        Args:
            snapshot: OrderBookSnapshot to process.
            requested_levels: Desired depth horizon.
            min_levels: Minimum depth levels to evaluate (default 1).

        Returns:
            MarketDepthAnalytics: Computed analytics model.
        """
        analytics = compute_adaptive_depth_analytics(
            snapshot=snapshot,
            requested_levels=requested_levels,
            min_levels=min_levels,
            imbalance_threshold=self.imbalance_threshold,
        )

        with self._lock:
            sym = analytics.symbol
            self._cache[sym] = analytics
            if sym not in self._history:
                self._history[sym] = []
            self._history[sym].append(analytics)
            if len(self._history[sym]) > self._max_history:
                self._history[sym].pop(0)
            self._total_evaluated += 1

        if self.event_bus is not None:
            try:
                from app.core.events import Event, EventType
                evt_type = getattr(EventType, "MARKET_DEPTH_ANALYTICS_UPDATED", EventType.MARKET_DEPTH_UPDATED)
                event = Event.create(
                    event_type=evt_type,
                    payload=analytics.to_dict(),
                    source=self.name,
                )
                self.event_bus.publish(event)
            except Exception as err:
                logger.error("Failed to publish depth analytics event: %s", err)

        return analytics

    def clear(self) -> None:
        """Clear all in-memory analytics state."""
        with self._lock:
            self._cache.clear()
            self._history.clear()
            self._total_evaluated = 0
