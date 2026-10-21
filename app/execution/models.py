"""KAISER — Paper Execution Domain Models.

Provides strongly-typed, immutable data structures for simulated orders,
fills, execution results, rejection codes, and risk configuration.

SAFETY INVARIANTS:
1. PURELY SIMULATED: Never connects to a live broker, exchange, or trading API.
2. EXECUTION LOCKED: Live execution remains locked by default; paper mode is isolated.
3. FAIL CLOSED: Missing, invalid, or stale data leads to rejection, never synthetic fills.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import math
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid

from app.core.exceptions import ValidationError


class PaperOrderSide(str, Enum):
    """Side of a paper trading order."""

    BUY = "BUY"
    SELL = "SELL"


class PaperOrderType(str, Enum):
    """Execution type of a paper order."""

    MARKET = "MARKET"
    LIMIT = "LIMIT"


class PaperOrderStatus(str, Enum):
    """Lifecycle state of a paper order."""

    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class PaperRejectionCode(str, Enum):
    """Structured rejection reason codes for paper order validation and risk gates."""

    PAPER_EXECUTION_DISABLED = "PAPER_EXECUTION_DISABLED"
    EXECUTION_LOCKED = "EXECUTION_LOCKED"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    INVALID_SIDE = "INVALID_SIDE"
    INVALID_ORDER_TYPE = "INVALID_ORDER_TYPE"
    INVALID_QUANTITY = "INVALID_QUANTITY"
    LIMIT_PRICE_REQUIRED = "LIMIT_PRICE_REQUIRED"
    INVALID_LIMIT_PRICE = "INVALID_LIMIT_PRICE"
    INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
    INSUFFICIENT_POSITION = "INSUFFICIENT_POSITION"
    SHORT_SELLING_PROHIBITED = "SHORT_SELLING_PROHIBITED"
    SHORT_SELLING_DISABLED = "SHORT_SELLING_PROHIBITED"
    EXCEEDS_MAX_NOTIONAL = "EXCEEDS_MAX_NOTIONAL"
    MAX_ORDER_NOTIONAL_EXCEEDED = "EXCEEDS_MAX_NOTIONAL"
    EXCEEDS_MAX_POSITION_EXPOSURE = "EXCEEDS_MAX_POSITION_EXPOSURE"
    MAX_POSITION_EXPOSURE_EXCEEDED = "EXCEEDS_MAX_POSITION_EXPOSURE"
    MISSING_MARKET_DATA = "MISSING_MARKET_DATA"
    MARKET_DATA_UNAVAILABLE = "MISSING_MARKET_DATA"
    STALE_MARKET_DATA = "STALE_MARKET_DATA"
    INVALID_MARKET_DATA = "INVALID_MARKET_DATA"
    LIMIT_PRICE_NOT_MET = "LIMIT_PRICE_NOT_MET"
    DUPLICATE_ORDER = "DUPLICATE_ORDER"
    ORDER_NOT_FOUND = "ORDER_NOT_FOUND"
    ALREADY_TERMINAL = "ALREADY_TERMINAL"


@dataclass(frozen=True)
class PaperRiskConfig:
    """Configurable risk and fee limits for paper execution."""

    max_order_notional: float = 50_000.0
    max_position_exposure_pct: float = 50.0  # Max % of total portfolio equity in one symbol
    fee_rate: float = 0.0005  # 5 bps (0.05%)
    slippage_rate: float = 0.0002  # 2 bps (0.02%)
    half_spread_rate: float = 0.0005  # Half-spread estimation when BBO is missing
    freshness_threshold_seconds: float = 60.0
    allow_short_selling: bool = False  # Long-only by default

    def __post_init__(self) -> None:
        if self.max_order_notional <= 0:
            raise ValidationError("max_order_notional must be strictly positive.")
        if self.max_position_exposure_pct <= 0 or self.max_position_exposure_pct > 100.0:
            raise ValidationError("max_position_exposure_pct must be between 0 and 100.")
        if self.fee_rate < 0:
            raise ValidationError("fee_rate cannot be negative.")
        if self.slippage_rate < 0:
            raise ValidationError("slippage_rate cannot be negative.")
        if self.freshness_threshold_seconds <= 0:
            raise ValidationError("freshness_threshold_seconds must be positive.")


@dataclass(frozen=True)
class PaperOrderRequest:
    """Immutable client request to submit a paper trading order."""

    symbol: str
    side: PaperOrderSide
    order_type: PaperOrderType
    quantity: float
    limit_price: Optional[float] = None
    client_order_id: Optional[str] = None
    correlation_id: Optional[str] = None
    notes: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("PaperOrderRequest symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not isinstance(self.side, PaperOrderSide):
            try:
                object.__setattr__(self, "side", PaperOrderSide(str(self.side).upper()))
            except Exception:
                raise ValidationError(f"Invalid PaperOrderSide: {self.side}")

        if not isinstance(self.order_type, PaperOrderType):
            try:
                object.__setattr__(self, "order_type", PaperOrderType(str(self.order_type).upper()))
            except Exception:
                raise ValidationError(f"Invalid PaperOrderType: {self.order_type}")

        if not math.isfinite(self.quantity) or self.quantity <= 0:
            raise ValidationError(f"PaperOrderRequest quantity must be a positive finite float, got {self.quantity}")

        if self.order_type == PaperOrderType.LIMIT:
            if self.limit_price is None or not math.isfinite(self.limit_price) or self.limit_price <= 0:
                raise ValidationError("Limit orders require a positive finite limit_price.")

        if self.client_order_id is None:
            object.__setattr__(self, "client_order_id", f"pord_{uuid.uuid4().hex[:12]}")


@dataclass(frozen=True)
class PaperFill:
    """Immutable execution record for a simulated fill."""

    fill_id: str
    order_id: str
    client_order_id: str
    symbol: str
    side: PaperOrderSide
    quantity: float
    price: float
    fee: float
    slippage: float
    timestamp: float
    market_data_timestamp: float
    market_data_source: str
    correlation_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.fill_id:
            raise ValidationError("PaperFill fill_id is required.")
        if not self.order_id:
            raise ValidationError("PaperFill order_id is required.")
        if not math.isfinite(self.quantity) or self.quantity <= 0:
            raise ValidationError(f"PaperFill quantity must be positive finite, got {self.quantity}")
        if not math.isfinite(self.price) or self.price <= 0:
            raise ValidationError(f"PaperFill price must be positive finite, got {self.price}")
        if not math.isfinite(self.fee) or self.fee < 0:
            raise ValidationError(f"PaperFill fee cannot be negative, got {self.fee}")
        if not math.isfinite(self.slippage) or self.slippage < 0:
            raise ValidationError(f"PaperFill slippage cannot be negative or non-finite, got {self.slippage}")
        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"PaperFill timestamp must be positive finite, got {self.timestamp}")
        if not math.isfinite(self.market_data_timestamp) or self.market_data_timestamp <= 0:
            raise ValidationError(f"PaperFill market_data_timestamp must be positive finite, got {self.market_data_timestamp}")

    @property
    def fill_price(self) -> float:
        """Alias for execution price."""
        return self.price

    @property
    def gross_notional(self) -> float:
        """Gross traded value before fees."""
        return round(self.quantity * self.price, 4)

    @property
    def net_cash_impact(self) -> float:
        """Net cash flow: negative for BUY (cash spent), positive for SELL (cash received)."""
        if self.side == PaperOrderSide.BUY:
            return round(-(self.gross_notional + self.fee), 4)
        else:
            return round(self.gross_notional - self.fee, 4)

    def to_dict(self) -> Dict[str, Any]:
        """Convert fill to dictionary."""
        return {
            "fill_id": self.fill_id,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "quantity": self.quantity,
            "price": self.price,
            "fee": self.fee,
            "slippage": self.slippage,
            "gross_notional": self.gross_notional,
            "net_cash_impact": self.net_cash_impact,
            "timestamp": self.timestamp,
            "market_data_timestamp": self.market_data_timestamp,
            "market_data_source": self.market_data_source,
            "correlation_id": self.correlation_id,
        }


@dataclass(frozen=True)
class PaperOrder:
    """Immutable state representation of a paper trading order."""

    order_id: str
    client_order_id: str
    symbol: str
    side: PaperOrderSide
    order_type: PaperOrderType
    quantity: float
    limit_price: Optional[float]
    status: PaperOrderStatus
    created_at: float
    updated_at: float
    filled_at: Optional[float] = None
    fill_price: Optional[float] = None
    filled_quantity: float = 0.0
    fee: float = 0.0
    slippage: float = 0.0
    rejection_code: Optional[PaperRejectionCode] = None
    rejection_reason: Optional[str] = None
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary."""
        return {
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "limit_price": self.limit_price,
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "filled_at": self.filled_at,
            "fill_price": self.fill_price,
            "filled_quantity": self.filled_quantity,
            "fee": self.fee,
            "slippage": self.slippage,
            "rejection_code": self.rejection_code.value if self.rejection_code else None,
            "rejection_reason": self.rejection_reason,
            "correlation_id": self.correlation_id,
        }


@dataclass(frozen=True)
class PaperExecutionResult:
    """Outcome container returned by PaperExecutionEngine."""

    success: bool
    order: PaperOrder
    fill: Optional[PaperFill] = None
    rejection_code: Optional[PaperRejectionCode] = None
    rejection_reason: Optional[str] = None
    event_delivery_failed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert execution result to dictionary."""
        return {
            "success": self.success,
            "order": self.order.to_dict(),
            "fill": self.fill.to_dict() if self.fill else None,
            "rejection_code": self.rejection_code.value if self.rejection_code else None,
            "rejection_reason": self.rejection_reason,
            "event_delivery_failed": self.event_delivery_failed,
        }
