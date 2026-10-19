"""KAISER — Paper Portfolio & Position Accounting.

Provides deterministic, decimal-accurate accounting for simulated holdings,
available cash, mark-to-market positions, realized/unrealized P&L, and fees.

CONVENTIONS:
1. Long-Only: Short selling is prohibited; sell orders cannot exceed current position quantity.
2. Weighted Average Cost: Buy fills update the position's average entry price weighted by quantity.
3. Realized PnL: Calculated on closed/reduced quantities minus transaction fees.
4. Cash Invariant: available_cash decreases on BUY (by gross + fee) and increases on SELL (by gross - fee).
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any, Dict, List, Optional

from app.core.exceptions import ValidationError
from app.execution.models import PaperFill, PaperOrderSide
from app.market.models import Quote


@dataclass
class PaperPosition:
    """Holding representation for a single symbol inside the paper portfolio."""

    symbol: str
    quantity: float
    avg_entry_price: float
    current_price: float
    accumulated_fees: float = 0.0
    realized_pnl: float = 0.0

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValidationError("PaperPosition symbol is required.")
        if self.quantity < 0:
            raise ValidationError(f"PaperPosition quantity cannot be negative, got {self.quantity}")
        if self.avg_entry_price < 0:
            raise ValidationError(f"PaperPosition avg_entry_price cannot be negative, got {self.avg_entry_price}")

    @property
    def cost_basis(self) -> float:
        """Total invested cost basis for current position quantity."""
        return round(self.quantity * self.avg_entry_price, 4)

    @property
    def market_value(self) -> float:
        """Current mark-to-market value of the position."""
        return round(self.quantity * self.current_price, 4)

    @property
    def current_market_price(self) -> float:
        """Alias for current_price."""
        return self.current_price

    @property
    def unrealized_pnl(self) -> float:
        """Unrealized profit/loss based on current price."""
        return round(self.market_value - self.cost_basis, 4)

    @property
    def unrealized_return_pct(self) -> float:
        """Unrealized percentage return relative to cost basis."""
        if self.cost_basis <= 0:
            return 0.0
        return round((self.unrealized_pnl / self.cost_basis) * 100.0, 2)

    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary."""
        return {
            "symbol": self.symbol,
            "quantity": self.quantity,
            "avg_entry_price": self.avg_entry_price,
            "current_price": self.current_price,
            "cost_basis": self.cost_basis,
            "market_value": self.market_value,
            "unrealized_pnl": self.unrealized_pnl,
            "unrealized_return_pct": self.unrealized_return_pct,
            "realized_pnl": round(self.realized_pnl, 4),
            "accumulated_fees": round(self.accumulated_fees, 4),
        }


class PaperPortfolio:
    """Simulated account portfolio managing cash, positions, and P&L accounting."""

    def __init__(self, initial_cash: float = 100_000.0) -> None:
        if initial_cash <= 0 or not math.isfinite(initial_cash):
            raise ValidationError(f"initial_cash must be a positive finite number, got {initial_cash}")

        self.initial_cash: float = round(float(initial_cash), 4)
        self.available_cash: float = round(float(initial_cash), 4)
        self.realized_pnl: float = 0.0
        self.total_fees_paid: float = 0.0
        self.positions: Dict[str, PaperPosition] = {}

    @property
    def total_position_value(self) -> float:
        """Sum of market values of all open positions."""
        return round(sum(pos.market_value for pos in self.positions.values()), 4)

    @property
    def total_equity(self) -> float:
        """Total portfolio equity: available cash + mark-to-market positions."""
        return round(self.available_cash + self.total_position_value, 4)

    @property
    def unrealized_pnl(self) -> float:
        """Sum of unrealized P&L across all open positions."""
        return round(sum(pos.unrealized_pnl for pos in self.positions.values()), 4)

    @property
    def total_pnl(self) -> float:
        """Total P&L: realized P&L + unrealized P&L."""
        return round(self.realized_pnl + self.unrealized_pnl, 4)

    def get_position(self, symbol: str) -> Optional[PaperPosition]:
        """Retrieve open position for symbol, if any."""
        return self.positions.get(symbol.strip().upper())

    def apply_fill(self, fill: PaperFill) -> float:
        """Apply a verified simulated fill to portfolio positions and cash.

        Returns:
            float: Realized P&L from this fill (0.0 for BUY orders).
        """
        symbol = fill.symbol.strip().upper()
        gross = fill.gross_notional
        fee = fill.fee

        if fill.side == PaperOrderSide.BUY:
            total_cash_required = round(gross + fee, 4)
            if self.available_cash < total_cash_required:
                raise ValidationError(
                    f"Insufficient cash for fill: required {total_cash_required}, available {self.available_cash}"
                )

            self.available_cash = round(self.available_cash - total_cash_required, 4)
            self.total_fees_paid = round(self.total_fees_paid + fee, 4)

            existing = self.positions.get(symbol)
            if existing is None:
                self.positions[symbol] = PaperPosition(
                    symbol=symbol,
                    quantity=fill.quantity,
                    avg_entry_price=fill.price,
                    current_price=fill.price,
                    accumulated_fees=fee,
                )
            else:
                new_qty = round(existing.quantity + fill.quantity, 6)
                total_cost = (existing.quantity * existing.avg_entry_price) + (fill.quantity * fill.price)
                new_avg = round(total_cost / new_qty, 4)
                existing.quantity = new_qty
                existing.avg_entry_price = new_avg
                existing.current_price = fill.price
                existing.accumulated_fees = round(existing.accumulated_fees + fee, 4)

            return 0.0

        elif fill.side == PaperOrderSide.SELL:
            existing = self.positions.get(symbol)
            if existing is None or existing.quantity < fill.quantity:
                current_qty = existing.quantity if existing else 0.0
                raise ValidationError(
                    f"Cannot execute SELL for {symbol}: position {current_qty} is less than fill quantity {fill.quantity}"
                )

            net_proceeds = round(gross - fee, 4)
            self.available_cash = round(self.available_cash + net_proceeds, 4)
            self.total_fees_paid = round(self.total_fees_paid + fee, 4)

            # Trade Realized PnL = (fill_price - avg_entry) * qty - fee
            trade_realized = round(((fill.price - existing.avg_entry_price) * fill.quantity) - fee, 4)
            self.realized_pnl = round(self.realized_pnl + trade_realized, 4)
            existing.realized_pnl = round(existing.realized_pnl + trade_realized, 4)
            existing.accumulated_fees = round(existing.accumulated_fees + fee, 4)

            new_qty = round(existing.quantity - fill.quantity, 6)
            if new_qty <= 1e-9:
                # Fully closed position
                del self.positions[symbol]
            else:
                existing.quantity = new_qty
                existing.current_price = fill.price

            return trade_realized

        return 0.0

    def mark_to_market(self, quote_or_symbol: Quote | str, price: Optional[float] = None) -> None:
        """Update mark-to-market valuations for open positions with latest quote or symbol/price."""
        if isinstance(quote_or_symbol, Quote):
            symbol = quote_or_symbol.symbol.strip().upper()
            target_price = quote_or_symbol.last_price
        else:
            symbol = quote_or_symbol.strip().upper()
            target_price = price if price is not None else 0.0

        pos = self.positions.get(symbol)
        if pos is not None and target_price > 0:
            pos.current_price = target_price

    def to_dict(self) -> Dict[str, Any]:
        """Convert portfolio status to dictionary."""
        return {
            "initial_cash": self.initial_cash,
            "available_cash": self.available_cash,
            "total_position_value": self.total_position_value,
            "total_equity": self.total_equity,
            "realized_pnl": round(self.realized_pnl, 4),
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl,
            "total_fees_paid": round(self.total_fees_paid, 4),
            "positions": {sym: pos.to_dict() for sym, pos in self.positions.items()},
        }
