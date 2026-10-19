"""KAISER — Paper Execution Risk Gate.

Enforces pre-trade risk checks for simulated paper trading:
1. Symbol, side, quantity, and order type validation.
2. Market data presence, validity, and freshness thresholds.
3. Cash sufficiency for buy orders (including estimated fees and slippage).
4. Long-only constraint: short selling is prohibited unless explicitly configured.
5. Maximum order notional limit.
6. Maximum single-instrument position exposure (% of total portfolio equity).

SAFETY INVARIANTS:
1. A passing risk check only authorizes a simulated operation; it NEVER unlocks live execution.
2. Fails closed with structured rejection reason codes on any missing or stale market data.
"""

from __future__ import annotations

import math
import time
from typing import Optional, Tuple

from app.execution.models import (
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderType,
    PaperRejectionCode,
    PaperRiskConfig,
)
from app.execution.portfolio import PaperPortfolio
from app.market.models import Quote


class PaperRiskGate:
    """Pre-trade risk evaluation engine for simulated paper trading."""

    def __init__(self, config: Optional[PaperRiskConfig] = None) -> None:
        self.config = config or PaperRiskConfig()

    def evaluate_order(
        self,
        request: PaperOrderRequest,
        portfolio: PaperPortfolio,
        quote: Optional[Quote] = None,
        reference_time: Optional[float] = None,
    ) -> Tuple[bool, Optional[PaperRejectionCode], Optional[str]]:
        """Validate order request against portfolio state, risk limits, and market data.

        Returns:
            Tuple[bool, Optional[PaperRejectionCode], Optional[str]]:
            (is_approved, rejection_code, rejection_reason)
        """
        now = reference_time if reference_time is not None else time.time()

        # 1. Quantity Validation
        if not math.isfinite(request.quantity) or request.quantity <= 0:
            return (
                False,
                PaperRejectionCode.INVALID_QUANTITY,
                f"Order quantity must be positive finite, got {request.quantity}",
            )

        # 2. Limit Price Validation for Limit Orders
        if request.order_type == PaperOrderType.LIMIT:
            if request.limit_price is None or not math.isfinite(request.limit_price) or request.limit_price <= 0:
                return (
                    False,
                    PaperRejectionCode.INVALID_LIMIT_PRICE,
                    f"Limit order requires positive finite limit_price, got {request.limit_price}",
                )

        # 3. Market Data Availability & Freshness Check
        if quote is None:
            return (
                False,
                PaperRejectionCode.MISSING_MARKET_DATA,
                f"No market quote available for symbol '{request.symbol}'",
            )

        if quote.symbol.upper() != request.symbol.upper():
            return (
                False,
                PaperRejectionCode.INVALID_MARKET_DATA,
                f"Market quote symbol '{quote.symbol}' does not match requested symbol '{request.symbol}'",
            )

        if not math.isfinite(quote.last_price) or quote.last_price <= 0:
            return (
                False,
                PaperRejectionCode.INVALID_MARKET_DATA,
                f"Market quote has non-positive or non-finite price: {quote.last_price}",
            )

        # Check for spread inversion
        if quote.bid is not None and quote.ask is not None and quote.bid > 0 and quote.ask > 0:
            if quote.ask < quote.bid:
                return (
                    False,
                    PaperRejectionCode.INVALID_MARKET_DATA,
                    f"Market quote has inverted spread (ask {quote.ask} < bid {quote.bid})",
                )

        # Check freshness
        data_age = max(0.0, now - quote.timestamp)
        if data_age > self.config.freshness_threshold_seconds:
            return (
                False,
                PaperRejectionCode.STALE_MARKET_DATA,
                f"Market quote for '{request.symbol}' is stale (age={data_age:.1f}s, threshold={self.config.freshness_threshold_seconds:.1f}s)",
            )

        # 4. Reference Price Estimation
        if request.side == PaperOrderSide.BUY:
            est_price = quote.ask if (quote.ask is not None and quote.ask > 0) else quote.last_price
        else:
            est_price = quote.bid if (quote.bid is not None and quote.bid > 0) else quote.last_price

        # 5. Maximum Order Notional Limit
        order_notional = request.quantity * est_price
        if order_notional > self.config.max_order_notional:
            return (
                False,
                PaperRejectionCode.EXCEEDS_MAX_NOTIONAL,
                f"Order notional ({order_notional:.2f}) exceeds configured max_order_notional ({self.config.max_order_notional:.2f})",
            )

        # 6. Cash Sufficiency for BUY orders
        if request.side == PaperOrderSide.BUY:
            est_fee = round(order_notional * self.config.fee_rate, 4)
            est_slippage = round(order_notional * self.config.slippage_rate, 4)
            est_total_cost = round(order_notional + est_fee + est_slippage, 4)

            if portfolio.available_cash < est_total_cost:
                return (
                    False,
                    PaperRejectionCode.INSUFFICIENT_CASH,
                    f"Insufficient available cash: required {est_total_cost:.2f}, available {portfolio.available_cash:.2f}",
                )

            # 7. Maximum Single-Instrument Position Exposure
            current_pos = portfolio.get_position(request.symbol)
            current_pos_val = current_pos.market_value if current_pos else 0.0
            projected_pos_val = current_pos_val + order_notional
            total_equity = portfolio.total_equity

            if total_equity > 0:
                exposure_pct = (projected_pos_val / total_equity) * 100.0
                if exposure_pct > self.config.max_position_exposure_pct:
                    return (
                        False,
                        PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE,
                        f"Projected position exposure for '{request.symbol}' ({exposure_pct:.1f}%) exceeds limit ({self.config.max_position_exposure_pct:.1f}%)",
                    )

        # 8. Position Sufficiency for SELL orders (Long-Only Enforcement)
        elif request.side == PaperOrderSide.SELL:
            if not self.config.allow_short_selling:
                pos = portfolio.get_position(request.symbol)
                held_qty = pos.quantity if pos else 0.0
                if held_qty <= 0 or held_qty < request.quantity:
                    return (
                        False,
                        PaperRejectionCode.INSUFFICIENT_POSITION,
                        f"Cannot SELL {request.quantity} of '{request.symbol}': held quantity is {held_qty} (short-selling prohibited)",
                    )

        return True, None, None
