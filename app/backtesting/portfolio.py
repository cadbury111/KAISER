"""KAISER — Simulated Virtual Portfolio and Accounting Engine.

Manages virtual cash, positions, trade accounting, fee deductions, slippage,
exposure limits, and peak-to-trough drawdown tracking for historical backtesting.

SAFETY INVARIANTS:
1. Virtual accounting only: no real funds, accounts, or brokers are touched.
2. Complete balance integrity: cash + position_value == total_equity at all times.
3. Explicit rejection: orders with insufficient cash or violating risk limits are rejected.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.backtesting.models import (
    BacktestConfig,
    EquityPoint,
    FeeModel,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSizeType,
    SimulatedFill,
    SimulatedOrder,
    TradeRecord,
)
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger

logger = get_logger("backtesting.portfolio")


@dataclass
class Position:
    """Mutable holding representation inside the simulated portfolio."""

    symbol: str
    quantity: float
    avg_cost: float
    current_price: float
    entry_timestamp: float
    accumulated_fees: float = 0.0

    @property
    def market_value(self) -> float:
        """Current market value of the position."""
        return self.quantity * self.current_price

    @property
    def cost_basis(self) -> float:
        """Total cost basis invested in the position."""
        return self.quantity * self.avg_cost

    @property
    def unrealized_pnl(self) -> float:
        """Unrealized dollar profit/loss."""
        return self.market_value - self.cost_basis

    @property
    def unrealized_return_pct(self) -> float:
        """Unrealized percentage return on cost basis."""
        if self.cost_basis <= 0:
            return 0.0
        return (self.unrealized_pnl / self.cost_basis) * 100.0


class SimulatedPortfolio:
    """Virtual deterministic portfolio accounting engine for backtesting."""

    def __init__(self, initial_cash: float = 100_000.0) -> None:
        if initial_cash <= 0:
            raise ValidationError(f"initial_cash must be strictly positive, got {initial_cash}")

        self.initial_cash: float = float(initial_cash)
        self.cash: float = float(initial_cash)
        self.positions: Dict[str, Position] = {}
        self.equity_curve: List[EquityPoint] = []
        self.orders: List[SimulatedOrder] = []
        self.fills: List[SimulatedFill] = []
        self.trades: List[TradeRecord] = []
        self.rejected_orders: List[SimulatedOrder] = []

        self.peak_equity: float = float(initial_cash)
        self.max_drawdown_pct: float = 0.0
        self.max_drawdown_amount: float = 0.0
        self.circuit_breaker_triggered: bool = False
        self.total_fees_paid: float = 0.0
        self.total_slippage_cost: float = 0.0

    @property
    def total_position_value(self) -> float:
        """Sum of market values across all held positions."""
        return sum(pos.market_value for pos in self.positions.values())

    @property
    def total_equity(self) -> float:
        """Total portfolio equity (cash + mark-to-market positions)."""
        return self.cash + self.total_position_value

    def get_position(self, symbol: str) -> Optional[Position]:
        """Retrieve active position for a symbol if any."""
        return self.positions.get(symbol.strip().upper())

    def get_position_quantity(self, symbol: str) -> float:
        """Return held quantity for a symbol."""
        pos = self.get_position(symbol)
        return pos.quantity if pos else 0.0

    def calculate_execution_price(
        self,
        side: OrderSide,
        base_price: float,
        slippage_bps: float = 2.0,
        half_spread_bps: float = 1.0,
    ) -> Tuple[float, float, float]:
        """Calculate execution price including slippage and spread assumptions.

        Returns:
            Tuple[exec_price, slippage_amount, spread_amount]
        """
        slip_pct = slippage_bps / 10_000.0
        spread_pct = half_spread_bps / 10_000.0
        total_adjustment_pct = slip_pct + spread_pct

        if side == OrderSide.BUY:
            exec_price = base_price * (1.0 + total_adjustment_pct)
        else:
            exec_price = max(0.0001, base_price * (1.0 - total_adjustment_pct))

        slippage_amount = base_price * slip_pct
        spread_amount = base_price * spread_pct

        return exec_price, slippage_amount, spread_amount

    def calculate_fee(
        self,
        quantity: float,
        price: float,
        fee_model: str = "percentage",
        fee_rate: float = 0.0005,
        fixed_fee: float = 0.0,
    ) -> float:
        """Calculate transaction fee for an order."""
        if fee_model == FeeModel.FIXED.value:
            return max(0.0, fixed_fee)
        trade_val = quantity * price
        return max(0.0, (trade_val * fee_rate) + fixed_fee)

    def calculate_target_quantity(
        self,
        config: BacktestConfig,
        current_price: float,
        current_equity: Optional[float] = None,
    ) -> float:
        """Determine quantity to order based on sizing rules."""
        if current_price <= 0:
            return 0.0

        equity = current_equity if current_equity is not None else self.total_equity

        if config.position_size_type == PositionSizeType.FIXED_QUANTITY.value:
            qty = config.position_size_value
        elif config.position_size_type == PositionSizeType.FIXED_CASH.value:
            qty = config.position_size_value / current_price
        elif config.position_size_type == PositionSizeType.PERCENT_EQUITY.value:
            allocated = equity * config.position_size_value
            qty = allocated / current_price
        else:
            qty = config.position_size_value

        if config.max_position_size is not None:
            qty = min(qty, config.max_position_size)

        return max(0.0, round(qty, 4))

    def validate_order(
        self,
        order: SimulatedOrder,
        current_price: float,
        config: BacktestConfig,
    ) -> Tuple[bool, Optional[str]]:
        """Validate order against cash, position constraints, and risk limits."""
        if self.circuit_breaker_triggered:
            return False, "CIRCUIT_BREAKER_ACTIVE: Maximum drawdown limit exceeded"

        if order.quantity <= 0:
            return False, f"INVALID_QUANTITY: Quantity must be positive, got {order.quantity}"

        if current_price <= 0:
            return False, f"INVALID_PRICE: Reference price must be positive, got {current_price}"

        exec_price, _, _ = self.calculate_execution_price(
            side=order.side,
            base_price=current_price,
            slippage_bps=config.slippage_bps,
            half_spread_bps=config.half_spread_bps,
        )

        estimated_fee = self.calculate_fee(
            quantity=order.quantity,
            price=exec_price,
            fee_model=config.fee_model,
            fee_rate=config.fee_rate,
            fixed_fee=config.fixed_fee,
        )

        if order.side == OrderSide.BUY:
            required_cash = (order.quantity * exec_price) + estimated_fee
            if self.cash < required_cash:
                return (
                    False,
                    f"INSUFFICIENT_CASH: Required {required_cash:.2f} exceeds available cash {self.cash:.2f}",
                )

            # Check maximum exposure constraint
            existing_pos = self.get_position(order.symbol)
            existing_val = existing_pos.market_value if existing_pos else 0.0
            new_position_val = existing_val + (order.quantity * exec_price)
            max_allowed_val = self.total_equity * config.max_exposure_pct
            if new_position_val > max_allowed_val * 1.0001:  # small floating-point tolerance
                return (
                    False,
                    f"EXPOSURE_LIMIT_EXCEEDED: Position value {new_position_val:.2f} exceeds max allowed {max_allowed_val:.2f}",
                )

        elif order.side == OrderSide.SELL:
            if not config.allow_short:
                held_qty = self.get_position_quantity(order.symbol)
                if held_qty <= 0.000001:
                    return False, f"NO_POSITION_TO_SELL: Holding 0 shares of {order.symbol}"
                if order.quantity > held_qty * 1.0001:
                    return (
                        False,
                        f"INSUFFICIENT_POSITION: Cannot sell {order.quantity} shares, only {held_qty} held",
                    )

        return True, None

    def execute_order(
        self,
        order: SimulatedOrder,
        base_price: float,
        timestamp: float,
        config: BacktestConfig,
    ) -> Optional[SimulatedFill]:
        """Execute a simulated order, mutating virtual cash and positions."""
        self.orders.append(order)

        # 1. Pre-trade risk validation
        valid, reason = self.validate_order(order, base_price, config)
        if not valid:
            rejected = SimulatedOrder(
                order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=base_price,
                created_at=order.created_at,
                status=OrderStatus.REJECTED,
                rejection_reason=reason,
                correlation_id=order.correlation_id,
            )
            self.rejected_orders.append(rejected)
            logger.debug("Simulated order rejected: %s", reason)
            return None

        # 2. Compute execution price with slippage and half-spread
        exec_price, slippage_amount, _ = self.calculate_execution_price(
            side=order.side,
            base_price=base_price,
            slippage_bps=config.slippage_bps,
            half_spread_bps=config.half_spread_bps,
        )

        fee = self.calculate_fee(
            quantity=order.quantity,
            price=exec_price,
            fee_model=config.fee_model,
            fee_rate=config.fee_rate,
            fixed_fee=config.fixed_fee,
        )

        total_slippage_cost = slippage_amount * order.quantity
        self.total_fees_paid += fee
        self.total_slippage_cost += total_slippage_cost

        # 3. Apply Fill Accounting
        fill_id = f"fill_{uuid.uuid4().hex[:12]}"
        fill = SimulatedFill(
            fill_id=fill_id,
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=exec_price,
            fee=fee,
            slippage_cost=total_slippage_cost,
            timestamp=timestamp,
            correlation_id=order.correlation_id,
        )
        self.fills.append(fill)

        if order.side == OrderSide.BUY:
            total_cash_out = (order.quantity * exec_price) + fee
            self.cash -= total_cash_out

            existing = self.positions.get(order.symbol)
            if existing:
                new_qty = existing.quantity + order.quantity
                new_avg_cost = ((existing.quantity * existing.avg_cost) + (order.quantity * exec_price)) / new_qty
                existing.quantity = new_qty
                existing.avg_cost = new_avg_cost
                existing.current_price = exec_price
                existing.accumulated_fees += fee
            else:
                self.positions[order.symbol] = Position(
                    symbol=order.symbol,
                    quantity=order.quantity,
                    avg_cost=exec_price,
                    current_price=exec_price,
                    entry_timestamp=timestamp,
                    accumulated_fees=fee,
                )

        elif order.side == OrderSide.SELL:
            total_cash_in = (order.quantity * exec_price) - fee
            self.cash += total_cash_in

            existing = self.positions.get(order.symbol)
            if existing:
                entry_price = existing.avg_cost
                qty_to_close = min(existing.quantity, order.quantity)
                gross_pnl = (exec_price - entry_price) * qty_to_close
                # Total trade fees include allocated entry fee + current exit fee
                entry_fee_share = (qty_to_close / existing.quantity) * existing.accumulated_fees
                total_trade_fees = entry_fee_share + fee
                net_pnl = gross_pnl - total_trade_fees
                duration = max(0.0, timestamp - existing.entry_timestamp)
                ret_pct = ((exec_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0

                trade = TradeRecord(
                    trade_id=f"trade_{uuid.uuid4().hex[:12]}",
                    symbol=order.symbol,
                    side=OrderSide.BUY,  # Long roundtrip
                    entry_time=existing.entry_timestamp,
                    exit_time=timestamp,
                    entry_price=entry_price,
                    exit_price=exec_price,
                    quantity=qty_to_close,
                    gross_pnl=gross_pnl,
                    net_pnl=net_pnl,
                    fees=total_trade_fees,
                    slippage=total_slippage_cost,
                    return_pct=ret_pct,
                    duration_seconds=duration,
                    is_win=(net_pnl > 0),
                )
                self.trades.append(trade)

                remaining = existing.quantity - qty_to_close
                if remaining <= 0.000001:
                    del self.positions[order.symbol]
                else:
                    existing.quantity = remaining
                    existing.current_price = exec_price
                    existing.accumulated_fees -= entry_fee_share

        return fill

    def mark_to_market(
        self,
        timestamp: float,
        current_prices: Dict[str, float],
        config: Optional[BacktestConfig] = None,
    ) -> EquityPoint:
        """Update market values and record an equity observation point."""
        for sym, pos in self.positions.items():
            if sym in current_prices:
                pos.current_price = current_prices[sym]

        equity = self.total_equity
        if equity > self.peak_equity:
            self.peak_equity = equity

        dd_amount = max(0.0, self.peak_equity - equity)
        dd_pct = (dd_amount / self.peak_equity) * 100.0 if self.peak_equity > 0 else 0.0

        if dd_pct > self.max_drawdown_pct:
            self.max_drawdown_pct = dd_pct
        if dd_amount > self.max_drawdown_amount:
            self.max_drawdown_amount = dd_amount

        # Check circuit breaker
        if config and config.max_drawdown_limit_pct is not None:
            if dd_pct >= config.max_drawdown_limit_pct:
                if not self.circuit_breaker_triggered:
                    self.circuit_breaker_triggered = True
                    logger.warning(
                        "Circuit breaker triggered! Drawdown %.2f%% reached limit %.2f%%",
                        dd_pct,
                        config.max_drawdown_limit_pct,
                    )

        point = EquityPoint(
            timestamp=timestamp,
            cash=self.cash,
            position_value=self.total_position_value,
            total_equity=equity,
            drawdown_pct=dd_pct,
            drawdown_amount=dd_amount,
        )
        self.equity_curve.append(point)
        return point

    def close_all_positions(
        self,
        timestamp: float,
        current_prices: Dict[str, float],
        config: BacktestConfig,
    ) -> List[TradeRecord]:
        """Liquidate all open positions at current market prices at end of backtest."""
        initial_trades_count = len(self.trades)
        for sym in list(self.positions.keys()):
            pos = self.positions[sym]
            price = current_prices.get(sym, pos.current_price)
            order = SimulatedOrder(
                order_id=f"order_close_{uuid.uuid4().hex[:8]}",
                symbol=sym,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=pos.quantity,
                price=price,
                created_at=timestamp,
                status=OrderStatus.PENDING,
            )
            self.execute_order(order=order, base_price=price, timestamp=timestamp, config=config)

        return self.trades[initial_trades_count:]
