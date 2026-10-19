"""KAISER — Simulated Broker Adapter.

In-process simulated broker adapter executing paper orders against verified market data.
Contains ZERO external network calls, zero broker SDK imports, and zero live execution paths.

DOCUMENTED ELIGIBLE FILL-PRICE POLICY:
1. BUY Market Order:
   - If Quote has valid best ask (> 0): base price = quote.ask
   - Else fallback: base price = quote.last_price * (1 + half_spread_rate)
   - Fill price = base price * (1 + slippage_rate), rounded to 4 decimals.
2. SELL Market Order:
   - If Quote has valid best bid (> 0): base price = quote.bid
   - Else fallback: base price = quote.last_price * (1 - half_spread_rate)
   - Fill price = base price * (1 - slippage_rate), rounded to 4 decimals.
3. BUY Limit Order:
   - Market ask price must be <= order.limit_price. If higher, returns LIMIT_PRICE_NOT_MET.
   - Fill price = min(order.limit_price, market_ask_price).
4. SELL Limit Order:
   - Market bid price must be >= order.limit_price. If lower, returns LIMIT_PRICE_NOT_MET.
   - Fill price = max(order.limit_price, market_bid_price).
5. Freshness Constraint:
   - Fails closed if market quote timestamp exceeds freshness_threshold_seconds.
"""

from __future__ import annotations

import math
import time
from typing import Optional, Tuple
import uuid

from app.execution.models import (
    PaperFill,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectionCode,
    PaperRiskConfig,
)
from app.market.models import Quote


class SimulatedBrokerAdapter:
    """In-process broker simulator executing deterministic paper fills."""

    def __init__(
        self,
        config: Optional[PaperRiskConfig] = None,
        fee_rate: Optional[float] = None,
        slippage_rate: Optional[float] = None,
        freshness_threshold_seconds: Optional[float] = None,
    ) -> None:
        if config is not None:
            self.config = config
        else:
            self.config = PaperRiskConfig(
                fee_rate=fee_rate if fee_rate is not None else 0.0005,
                slippage_rate=slippage_rate if slippage_rate is not None else 0.0002,
                freshness_threshold_seconds=freshness_threshold_seconds if freshness_threshold_seconds is not None else 60.0,
            )

    def simulate_fill(
        self,
        request_or_order: PaperOrderRequest | PaperOrder,
        quote: Quote,
        reference_time: Optional[float] = None,
        max_age_seconds: Optional[float] = None,
    ) -> Tuple[bool, Optional[PaperFill], Optional[PaperRejectionCode], Optional[str]]:
        """Simulate fill for an order request or order against a quote."""
        if max_age_seconds is not None and max_age_seconds != self.config.freshness_threshold_seconds:
            custom_config = PaperRiskConfig(
                max_order_notional=self.config.max_order_notional,
                max_position_exposure_pct=self.config.max_position_exposure_pct,
                fee_rate=self.config.fee_rate,
                slippage_rate=self.config.slippage_rate,
                half_spread_rate=self.config.half_spread_rate,
                freshness_threshold_seconds=max_age_seconds,
                allow_short_selling=self.config.allow_short_selling,
            )
            adapter = SimulatedBrokerAdapter(config=custom_config)
            return adapter.simulate_fill(request_or_order, quote, reference_time=reference_time)

        if isinstance(request_or_order, PaperOrderRequest):
            order = PaperOrder(
                order_id=f"sim_{uuid.uuid4().hex[:8]}",
                client_order_id=request_or_order.client_order_id,
                symbol=request_or_order.symbol,
                side=request_or_order.side,
                order_type=request_or_order.order_type,
                quantity=request_or_order.quantity,
                limit_price=request_or_order.limit_price,
                status=PaperOrderStatus.ACCEPTED,
                created_at=time.time(),
                updated_at=time.time(),
                correlation_id=request_or_order.correlation_id,
            )
        else:
            order = request_or_order

        return self.execute_order(order=order, quote=quote, reference_time=reference_time)

    def execute_order(
        self,
        order: PaperOrder,
        quote: Quote,
        reference_time: Optional[float] = None,
    ) -> Tuple[bool, Optional[PaperFill], Optional[PaperRejectionCode], Optional[str]]:
        """Simulate order execution against the provided market quote.

        Returns:
            Tuple[bool, Optional[PaperFill], Optional[PaperRejectionCode], Optional[str]]:
            (is_filled, fill_record, rejection_code, rejection_reason)
        """
        now = reference_time if reference_time is not None else time.time()

        # 1. Validate Symbol Alignment
        if quote.symbol.upper() != order.symbol.upper():
            return (
                False,
                None,
                PaperRejectionCode.INVALID_MARKET_DATA,
                f"Market quote symbol '{quote.symbol}' does not match order symbol '{order.symbol}'",
            )

        # 2. Check Freshness Threshold
        age = max(0.0, now - quote.timestamp)
        if age > self.config.freshness_threshold_seconds:
            return (
                False,
                None,
                PaperRejectionCode.STALE_MARKET_DATA,
                f"Market data for '{order.symbol}' is stale (age={age:.1f}s > threshold={self.config.freshness_threshold_seconds:.1f}s)",
            )

        # 3. Check Price Bounds and Inversion
        if not math.isfinite(quote.last_price) or quote.last_price <= 0:
            return (
                False,
                None,
                PaperRejectionCode.INVALID_MARKET_DATA,
                f"Market quote has invalid price: {quote.last_price}",
            )

        if quote.bid is not None and quote.ask is not None and quote.bid > 0 and quote.ask > 0:
            if quote.ask < quote.bid:
                return (
                    False,
                    None,
                    PaperRejectionCode.INVALID_MARKET_DATA,
                    f"Market quote has inverted spread (ask {quote.ask} < bid {quote.bid})",
                )

        # 4. Compute Fill Price by Order Type & Side
        fill_price: float = 0.0

        if order.order_type == PaperOrderType.MARKET:
            if order.side == PaperOrderSide.BUY:
                base_price = (
                    quote.ask
                    if (quote.ask is not None and quote.ask > 0)
                    else quote.last_price * (1.0 + self.config.half_spread_rate)
                )
                fill_price = round(base_price * (1.0 + self.config.slippage_rate), 4)

            elif order.side == PaperOrderSide.SELL:
                base_price = (
                    quote.bid
                    if (quote.bid is not None and quote.bid > 0)
                    else quote.last_price * (1.0 - self.config.half_spread_rate)
                )
                fill_price = round(base_price * (1.0 - self.config.slippage_rate), 4)

        elif order.order_type == PaperOrderType.LIMIT:
            if order.limit_price is None or order.limit_price <= 0:
                return (
                    False,
                    None,
                    PaperRejectionCode.LIMIT_PRICE_REQUIRED,
                    "Limit order requires a positive limit_price",
                )

            if order.side == PaperOrderSide.BUY:
                market_price = (
                    quote.ask
                    if (quote.ask is not None and quote.ask > 0)
                    else quote.last_price
                )
                if market_price > order.limit_price:
                    return (
                        False,
                        None,
                        PaperRejectionCode.LIMIT_PRICE_NOT_MET,
                        f"Current ask price {market_price:.2f} is above buy limit {order.limit_price:.2f}",
                    )
                fill_price = round(min(order.limit_price, market_price), 4)

            elif order.side == PaperOrderSide.SELL:
                market_price = (
                    quote.bid
                    if (quote.bid is not None and quote.bid > 0)
                    else quote.last_price
                )
                if market_price < order.limit_price:
                    return (
                        False,
                        None,
                        PaperRejectionCode.LIMIT_PRICE_NOT_MET,
                        f"Current bid price {market_price:.2f} is below sell limit {order.limit_price:.2f}",
                    )
                fill_price = round(max(order.limit_price, market_price), 4)

        else:
            return (
                False,
                None,
                PaperRejectionCode.INVALID_ORDER_TYPE,
                f"Unsupported order type: {order.order_type}",
            )

        # 5. Compute Fees and Slippage
        gross_notional = round(order.quantity * fill_price, 4)
        fee = round(gross_notional * self.config.fee_rate, 4)
        slippage_cost = round(abs(fill_price - quote.last_price) * order.quantity, 4)

        fill_id = f"fill_{order.order_id}_{uuid.uuid4().hex[:8]}"

        fill = PaperFill(
            fill_id=fill_id,
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=fill_price,
            fee=fee,
            slippage=slippage_cost,
            timestamp=now,
            market_data_timestamp=quote.timestamp,
            market_data_source=quote.source,
            correlation_id=order.correlation_id,
        )

        return True, fill, None, None
