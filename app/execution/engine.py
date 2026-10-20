"""KAISER — Paper Execution Engine.

Central engine coordinating simulated order submission, pre-trade risk evaluation,
deterministic broker execution, paper portfolio accounting, auditable ledger recording,
and event bus distribution.

CRITICAL SAFETY INVARIANTS:
1. PURELY ISOLATED: No connection to any real broker, exchange API, or live order path.
2. EXECUTION LOCKED: Settings.execution_locked remains True; paper execution never unlocks live.
3. FAIL CLOSED: Orders are rejected with explicit reason codes if market data or risk parameters are violated.
4. AUDITABLE: All submissions, acceptances, rejections, fills, and cancellations are recorded in the ledger.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any, Dict, List, Optional
import uuid

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.execution.models import (
    PaperExecutionResult,
    PaperFill,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectionCode,
    PaperRiskConfig,
)
from app.execution.portfolio import PaperPortfolio
from app.execution.risk import PaperRiskGate
from app.execution.simulated_broker import SimulatedBrokerAdapter
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.market.models import Quote

logger = get_logger("execution.paper")


class PaperExecutionEngine:
    """Deterministic, isolated paper trading execution engine."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        ledger: Optional[PaperLedger] = None,
        portfolio: Optional[PaperPortfolio] = None,
        risk_config: Optional[PaperRiskConfig] = None,
        broker_adapter: Optional[SimulatedBrokerAdapter] = None,
        name: str = "paper_execution_engine",
        enabled: Optional[bool] = None,
        initial_cash: Optional[float] = None,
        fee_rate: Optional[float] = None,
        slippage_rate: Optional[float] = None,
    ) -> None:
        self.name = name
        self.settings = settings or Settings()
        self.event_bus = event_bus
        self.state_store = state_store

        # Operational enablement flag
        self._enabled = enabled if enabled is not None else getattr(self.settings, "paper_execution_enabled", False)

        # Construct portfolio and ledger
        init_cash = (
            initial_cash
            if initial_cash is not None
            else getattr(self.settings, "paper_initial_cash", 100_000.0)
        )
        self.portfolio = portfolio or PaperPortfolio(initial_cash=init_cash)
        self.ledger = ledger or PaperLedger(name="paper_ledger", initial_balance=init_cash)

        # Initialize risk gate and broker adapter
        if risk_config is None:
            max_notional = getattr(self.settings, "paper_max_order_notional", 50_000.0)
            max_exposure = getattr(self.settings, "paper_max_position_exposure_pct", 50.0)
            f_rate = fee_rate if fee_rate is not None else getattr(self.settings, "paper_fee_rate", 0.0005)
            s_rate = slippage_rate if slippage_rate is not None else getattr(self.settings, "paper_slippage_rate", 0.0002)
            freshness_threshold = getattr(self.settings, "paper_freshness_threshold_seconds", 60.0)
            risk_config = PaperRiskConfig(
                max_order_notional=max_notional,
                max_position_exposure_pct=max_exposure,
                fee_rate=f_rate,
                slippage_rate=s_rate,
                freshness_threshold_seconds=freshness_threshold,
                allow_short_selling=False,
            )
        self.risk_config = risk_config
        self.risk_gate = PaperRiskGate(config=self.risk_config)
        self.broker_adapter = broker_adapter or SimulatedBrokerAdapter(config=self.risk_config)

        self._orders: Dict[str, PaperOrder] = {}
        self._client_order_map: Dict[str, str] = {}
        self._fills: Dict[str, PaperFill] = {}
        self._quotes: Dict[str, Quote] = {}
        self._lock = threading.RLock()

        # Subscribe to market quote updates if EventBus is attached
        if self.event_bus:
            self._subscribe_to_market_events()

        # If ledger already contains records (e.g. SQLite recovery), restore state
        if self.ledger.count() > 0:
            self.recover_from_ledger()
        else:
            # Record initial capital in ledger if ledger is empty
            init_entry = PaperLedgerEntry(
                entry_id=f"ledg_init_{uuid.uuid4().hex[:8]}",
                timestamp=time.time(),
                entry_type=LedgerEntryType.INITIAL_CAPITAL,
                cash_change=self.portfolio.initial_cash,
                cash_balance_after=self.portfolio.available_cash,
                idempotency_key=f"init_deposit_{self.name}_{self.portfolio.initial_cash}",
                metadata={"initial_cash": self.portfolio.initial_cash, "description": "Initial paper capital allocation"},
            )
            self.ledger.record_entry(init_entry)

        self._update_state_store()

    def _subscribe_to_market_events(self) -> None:
        """Subscribe to market data quote events from event bus."""
        assert self.event_bus is not None

        def _on_market_quote_event(event: Event) -> None:
            try:
                p = event.payload
                quote = Quote(
                    symbol=str(p["symbol"]),
                    exchange=str(p.get("exchange", "MOCK")),
                    timestamp=float(p.get("timestamp", time.time())),
                    last_price=float(p["last_price"]),
                    bid=float(p["bid"]) if p.get("bid") is not None else None,
                    ask=float(p["ask"]) if p.get("ask") is not None else None,
                    bid_size=float(p["bid_size"]) if p.get("bid_size") is not None else None,
                    ask_size=float(p["ask_size"]) if p.get("ask_size") is not None else None,
                    volume=float(p.get("volume", 0.0)),
                    currency=str(p.get("currency", "INR")),
                    source=str(p.get("source", "mock")),
                )
                self.on_market_quote(quote)
            except Exception as err:
                logger.error("Failed to parse quote from event: %s", err)

        self.event_bus.subscribe(EventType.MARKET_QUOTE_UPDATED, _on_market_quote_event)

    @property
    def is_enabled(self) -> bool:
        """Check whether paper execution is explicitly enabled."""
        return self._enabled

    def set_enabled(self, value: bool) -> None:
        """Explicitly enable or disable paper execution."""
        with self._lock:
            self._enabled = bool(value)
            self._update_state_store()

    def _publish_event(
        self,
        event_type: EventType,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> None:
        """Safely publish an immutable event across EventBus if attached."""
        if self.event_bus:
            event = Event.create(
                event_type=event_type,
                source="paper_execution",
                correlation_id=correlation_id or uuid.uuid4().hex,
                payload=payload,
                metadata={"execution_mode": "PAPER", "locked": self.settings.execution_locked},
            )
            self.event_bus.publish(event)

    def _update_state_store(self) -> None:
        """Update StateStore with latest paper portfolio and operational metrics."""
        if self.state_store:
            self.state_store.set("paper_portfolio", self.portfolio.to_dict())
            self.state_store.set("paper_orders_count", len(self._orders))
            self.state_store.set("paper_fills_count", len(self._fills))
            self.state_store.set("paper_status", "active" if self.is_enabled else "disabled")

    def recover_from_ledger(self) -> Dict[str, Any]:
        """Recover portfolio, orders, fills, and cash balances from the ledger audit trail.

        Guarantees:
        1. Idempotency: replaying multiple times yields identical financial state.
        2. Zero cash duplication: initial cash is reset and not double-credited.
        3. Reconstructs all open, filled, rejected, and cancelled orders.
        4. Fails safely with explicit ValidationError on corrupted or inconsistent records.
        """
        with self._lock:
            entries = self.ledger.get_entries(limit=1_000_000)
            if not entries:
                return {
                    "recovered_orders": 0,
                    "recovered_fills": 0,
                    "available_cash": self.portfolio.available_cash,
                    "positions": 0,
                }

            # 1. Inspect initial capital
            init_entries = [e for e in entries if e.entry_type == LedgerEntryType.INITIAL_CAPITAL]
            if init_entries:
                first_init_cash = init_entries[0].cash_change
                for ie in init_entries[1:]:
                    if abs(ie.cash_change - first_init_cash) > 0.0001:
                        raise ValidationError(
                            f"Conflicting initial capital records in ledger: {ie.cash_change} vs {first_init_cash}"
                        )
                init_cash = first_init_cash
            else:
                init_cash = self.portfolio.initial_cash

            # Reset portfolio cleanly
            self.portfolio.reset(initial_cash=init_cash)

            # Clear engine in-memory indices
            self._orders.clear()
            self._client_order_map.clear()
            self._fills.clear()

            # 2. Replay entries in chronological order
            for entry in entries:
                if not math.isfinite(entry.cash_change) or not math.isfinite(entry.cash_balance_after):
                    raise ValidationError(f"Corrupted ledger entry {entry.entry_id}: non-finite cash values")

                if entry.entry_type == LedgerEntryType.INITIAL_CAPITAL:
                    continue

                elif entry.entry_type == LedgerEntryType.ORDER_ACCEPTANCE:
                    if not entry.order_id or not entry.symbol or not entry.side or entry.quantity is None or entry.quantity <= 0:
                        raise ValidationError(f"Corrupted ORDER_ACCEPTANCE entry: {entry.entry_id}")

                    order_type_str = entry.metadata.get("order_type")
                    if not order_type_str:
                        order_type_str = "LIMIT" if entry.price and entry.price > 0 else "MARKET"
                    try:
                        otype = PaperOrderType(order_type_str)
                        oside = PaperOrderSide(entry.side)
                    except Exception as err:
                        raise ValidationError(f"Invalid order enum in ledger entry {entry.entry_id}: {err}") from err

                    acc_order = PaperOrder(
                        order_id=entry.order_id,
                        client_order_id=entry.client_order_id or entry.order_id,
                        symbol=entry.symbol,
                        side=oside,
                        order_type=otype,
                        quantity=entry.quantity,
                        limit_price=entry.price if entry.price and entry.price > 0 else None,
                        status=PaperOrderStatus.ACCEPTED,
                        created_at=entry.timestamp,
                        updated_at=entry.timestamp,
                        correlation_id=entry.correlation_id,
                    )
                    self._orders[entry.order_id] = acc_order
                    if entry.client_order_id:
                        self._client_order_map[entry.client_order_id] = entry.order_id

                elif entry.entry_type == LedgerEntryType.ORDER_FILL:
                    if (
                        not entry.order_id
                        or not entry.symbol
                        or not entry.side
                        or entry.quantity is None
                        or entry.quantity <= 0
                        or entry.price is None
                        or entry.price <= 0
                    ):
                        raise ValidationError(f"Corrupted ORDER_FILL entry: {entry.entry_id}")

                    fill_id = entry.metadata.get("fill_id") or (
                        entry.idempotency_key.replace("order_fill_", "")
                        if entry.idempotency_key.startswith("order_fill_")
                        else f"fill_{entry.order_id}"
                    )
                    try:
                        oside = PaperOrderSide(entry.side)
                    except Exception as err:
                        raise ValidationError(f"Invalid side enum in ORDER_FILL {entry.entry_id}: {err}") from err

                    fill = PaperFill(
                        fill_id=fill_id,
                        order_id=entry.order_id,
                        client_order_id=entry.client_order_id or entry.order_id,
                        symbol=entry.symbol,
                        side=oside,
                        quantity=entry.quantity,
                        price=entry.price,
                        fee=entry.fee,
                        slippage=float(entry.metadata.get("slippage", 0.0)),
                        timestamp=entry.timestamp,
                        market_data_timestamp=float(entry.metadata.get("market_data_timestamp", entry.timestamp)),
                        market_data_source=str(entry.metadata.get("market_data_source", "replayed")),
                        correlation_id=entry.correlation_id,
                    )
                    self.portfolio.apply_fill(fill)
                    self._fills[fill_id] = fill

                    existing = self._orders.get(entry.order_id)
                    order_type_str = entry.metadata.get("order_type")
                    if existing:
                        otype = existing.order_type
                        created_at = existing.created_at
                        limit_price = existing.limit_price
                    else:
                        otype = PaperOrderType(order_type_str) if order_type_str else (
                            PaperOrderType.LIMIT if entry.price and entry.price > 0 else PaperOrderType.MARKET
                        )
                        created_at = entry.timestamp
                        limit_price = entry.price if otype == PaperOrderType.LIMIT else None

                    filled_order = PaperOrder(
                        order_id=entry.order_id,
                        client_order_id=entry.client_order_id or entry.order_id,
                        symbol=entry.symbol,
                        side=oside,
                        order_type=otype,
                        quantity=entry.quantity,
                        limit_price=limit_price,
                        status=PaperOrderStatus.FILLED,
                        created_at=created_at,
                        updated_at=entry.timestamp,
                        filled_at=entry.timestamp,
                        fill_price=entry.price,
                        filled_quantity=entry.quantity,
                        fee=entry.fee,
                        slippage=fill.slippage,
                        correlation_id=entry.correlation_id,
                    )
                    self._orders[entry.order_id] = filled_order
                    if entry.client_order_id:
                        self._client_order_map[entry.client_order_id] = entry.order_id

                elif entry.entry_type == LedgerEntryType.ORDER_REJECTION:
                    if not entry.order_id:
                        continue
                    rej_code_str = entry.metadata.get("rejection_code")
                    rej_code = PaperRejectionCode(rej_code_str) if rej_code_str else None
                    rej_reason = entry.metadata.get("rejection_reason")
                    order_type_str = entry.metadata.get("order_type", "MARKET")
                    try:
                        otype = PaperOrderType(order_type_str)
                        oside = PaperOrderSide(entry.side) if entry.side else PaperOrderSide.BUY
                    except Exception:
                        otype = PaperOrderType.MARKET
                        oside = PaperOrderSide.BUY

                    rej_order = PaperOrder(
                        order_id=entry.order_id,
                        client_order_id=entry.client_order_id or entry.order_id,
                        symbol=entry.symbol or "UNKNOWN",
                        side=oside,
                        order_type=otype,
                        quantity=entry.quantity or 0.0,
                        limit_price=entry.price,
                        status=PaperOrderStatus.REJECTED,
                        created_at=entry.timestamp,
                        updated_at=entry.timestamp,
                        rejection_code=rej_code,
                        rejection_reason=rej_reason,
                        correlation_id=entry.correlation_id,
                    )
                    self._orders[entry.order_id] = rej_order
                    if entry.client_order_id:
                        self._client_order_map[entry.client_order_id] = entry.order_id

                elif entry.entry_type == LedgerEntryType.ORDER_CANCELLATION:
                    if not entry.order_id:
                        continue
                    existing = self._orders.get(entry.order_id)
                    can_reason = entry.metadata.get("cancellation_reason", "Cancelled")
                    if existing:
                        can_order = PaperOrder(
                            order_id=existing.order_id,
                            client_order_id=existing.client_order_id,
                            symbol=existing.symbol,
                            side=existing.side,
                            order_type=existing.order_type,
                            quantity=existing.quantity,
                            limit_price=existing.limit_price,
                            status=PaperOrderStatus.CANCELLED,
                            created_at=existing.created_at,
                            updated_at=entry.timestamp,
                            rejection_reason=can_reason,
                            correlation_id=existing.correlation_id,
                        )
                        self._orders[entry.order_id] = can_order

            # 3. Post-recovery reconciliation
            recon = self.ledger.reconcile(expected_current_cash=self.portfolio.available_cash)
            if not recon["is_balanced"]:
                raise ValidationError(
                    f"Ledger reconciliation failed during recovery: computed={recon['computed_balance']} vs portfolio={self.portfolio.available_cash}, discrepancy={recon['discrepancy']}"
                )

            self._update_state_store()
            logger.info(
                "Paper execution engine recovered from ledger [orders=%d, fills=%d, cash=%.2f, positions=%d]",
                len(self._orders),
                len(self._fills),
                self.portfolio.available_cash,
                len(self.portfolio.positions),
            )
            return {
                "recovered_orders": len(self._orders),
                "recovered_fills": len(self._fills),
                "available_cash": self.portfolio.available_cash,
                "positions": len(self.portfolio.positions),
            }

    def submit_order(
        self,
        request: PaperOrderRequest,
        market_quote: Optional[Quote] = None,
        reference_time: Optional[float] = None,
    ) -> PaperExecutionResult:
        """Submit a paper order for risk validation and simulated execution.

        Args:
            request: Validated PaperOrderRequest.
            market_quote: Current Quote for symbol (falls back to cached quote).
            reference_time: Clock reference.

        Returns:
            PaperExecutionResult: Structured outcome with order, fill, or rejection details.
        """
        now = reference_time if reference_time is not None else time.time()
        order_id = f"pord_{uuid.uuid4().hex[:12]}"
        client_order_id = request.client_order_id or order_id
        cid = request.correlation_id or uuid.uuid4().hex

        with self._lock:
            # Look up cached market quote if not explicitly supplied
            if market_quote is None:
                market_quote = self._quotes.get(request.symbol.strip().upper())

            # 1. Publish Order Submitted Event
            self._publish_event(
                EventType.PAPER_ORDER_SUBMITTED,
                {
                    "client_order_id": client_order_id,
                    "symbol": request.symbol,
                    "side": request.side.value,
                    "order_type": request.order_type.value,
                    "quantity": request.quantity,
                    "limit_price": request.limit_price,
                },
                correlation_id=cid,
            )

            # 2. Configuration Gate Check
            if not self.is_enabled:
                rejected_order = PaperOrder(
                    order_id=order_id,
                    client_order_id=client_order_id,
                    symbol=request.symbol,
                    side=request.side,
                    order_type=request.order_type,
                    quantity=request.quantity,
                    limit_price=request.limit_price,
                    status=PaperOrderStatus.REJECTED,
                    created_at=now,
                    updated_at=now,
                    rejection_code=PaperRejectionCode.PAPER_EXECUTION_DISABLED,
                    rejection_reason="Paper execution is disabled in system settings (PAPER_EXECUTION_ENABLED=false)",
                    correlation_id=cid,
                )
                self._orders[order_id] = rejected_order
                self._client_order_map[client_order_id] = order_id
                self._record_rejection(rejected_order, cid)
                return PaperExecutionResult(
                    success=False,
                    order=rejected_order,
                    rejection_code=PaperRejectionCode.PAPER_EXECUTION_DISABLED,
                    rejection_reason=rejected_order.rejection_reason,
                )

            # 3. Idempotency Check on client_order_id
            if client_order_id in self._client_order_map:
                existing_order_id = self._client_order_map[client_order_id]
                existing_order = self._orders.get(existing_order_id)
                if existing_order and existing_order.status in (
                    PaperOrderStatus.ACCEPTED,
                    PaperOrderStatus.PENDING,
                    PaperOrderStatus.FILLED,
                ):
                    return PaperExecutionResult(
                        success=False,
                        order=existing_order,
                        rejection_code=PaperRejectionCode.DUPLICATE_ORDER,
                        rejection_reason=f"Order with client_order_id '{client_order_id}' has already been processed",
                    )

            # 4. Pre-Trade Risk Evaluation
            is_approved, rej_code, rej_reason = self.risk_gate.evaluate_order(
                request=request,
                portfolio=self.portfolio,
                quote=market_quote,
                reference_time=now,
            )

            if not is_approved:
                rejected_order = PaperOrder(
                    order_id=order_id,
                    client_order_id=client_order_id,
                    symbol=request.symbol,
                    side=request.side,
                    order_type=request.order_type,
                    quantity=request.quantity,
                    limit_price=request.limit_price,
                    status=PaperOrderStatus.REJECTED,
                    created_at=now,
                    updated_at=now,
                    rejection_code=rej_code,
                    rejection_reason=rej_reason,
                    correlation_id=cid,
                )
                self._orders[order_id] = rejected_order
                self._client_order_map[client_order_id] = order_id
                self._record_rejection(rejected_order, cid)
                return PaperExecutionResult(
                    success=False,
                    order=rejected_order,
                    rejection_code=rej_code,
                    rejection_reason=rej_reason,
                )

            # 5. Order Accepted by Risk Gate
            accepted_order = PaperOrder(
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=request.symbol,
                side=request.side,
                order_type=request.order_type,
                quantity=request.quantity,
                limit_price=request.limit_price,
                status=PaperOrderStatus.ACCEPTED,
                created_at=now,
                updated_at=now,
                correlation_id=cid,
            )
            self._orders[order_id] = accepted_order
            self._client_order_map[client_order_id] = order_id

            # Record acceptance in ledger & event bus
            accept_entry = PaperLedgerEntry(
                entry_id=f"ledg_acc_{uuid.uuid4().hex[:8]}",
                timestamp=now,
                entry_type=LedgerEntryType.ORDER_ACCEPTANCE,
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=request.symbol,
                side=request.side.value,
                quantity=request.quantity,
                price=request.limit_price,
                cash_change=0.0,
                cash_balance_after=self.portfolio.available_cash,
                idempotency_key=f"order_accept_{order_id}",
                correlation_id=cid,
                metadata={"order_type": request.order_type.value},
            )
            self.ledger.record_entry(accept_entry)
            self._publish_event(EventType.PAPER_ORDER_ACCEPTED, accepted_order.to_dict(), correlation_id=cid)

            # 6. Simulated Broker Execution
            assert market_quote is not None
            is_filled, fill, fill_rej_code, fill_rej_reason = self.broker_adapter.execute_order(
                order=accepted_order,
                quote=market_quote,
                reference_time=now,
            )

            # If order cannot fill immediately (e.g. limit order price not met)
            if not is_filled:
                if fill_rej_code == PaperRejectionCode.LIMIT_PRICE_NOT_MET:
                    return PaperExecutionResult(
                        success=False,
                        order=accepted_order,
                        fill=None,
                        rejection_code=fill_rej_code,
                        rejection_reason=fill_rej_reason,
                    )
                else:
                    rej_order = PaperOrder(
                        order_id=order_id,
                        client_order_id=client_order_id,
                        symbol=request.symbol,
                        side=request.side,
                        order_type=request.order_type,
                        quantity=request.quantity,
                        limit_price=request.limit_price,
                        status=PaperOrderStatus.REJECTED,
                        created_at=now,
                        updated_at=now,
                        rejection_code=fill_rej_code,
                        rejection_reason=fill_rej_reason,
                        correlation_id=cid,
                    )
                    self._orders[order_id] = rej_order
                    self._record_rejection(rej_order, cid)
                    return PaperExecutionResult(
                        success=False,
                        order=rej_order,
                        rejection_code=fill_rej_code,
                        rejection_reason=fill_rej_reason,
                    )

            # 7. Apply Fill to Portfolio & Update Ledger
            assert fill is not None
            self.portfolio.apply_fill(fill)
            self._fills[fill.fill_id] = fill

            filled_order = PaperOrder(
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=request.symbol,
                side=request.side,
                order_type=request.order_type,
                quantity=request.quantity,
                limit_price=request.limit_price,
                status=PaperOrderStatus.FILLED,
                created_at=accepted_order.created_at,
                updated_at=now,
                filled_at=fill.timestamp,
                fill_price=fill.price,
                filled_quantity=fill.quantity,
                fee=fill.fee,
                slippage=fill.slippage,
                correlation_id=cid,
            )
            self._orders[order_id] = filled_order

            # Record Fill in Ledger
            fill_entry = PaperLedgerEntry(
                entry_id=f"ledg_fill_{uuid.uuid4().hex[:8]}",
                timestamp=now,
                entry_type=LedgerEntryType.ORDER_FILL,
                order_id=order_id,
                client_order_id=client_order_id,
                symbol=request.symbol,
                side=request.side.value,
                quantity=fill.quantity,
                price=fill.price,
                fee=fill.fee,
                cash_change=fill.net_cash_impact,
                cash_balance_after=self.portfolio.available_cash,
                realized_pnl=self.portfolio.realized_pnl,
                idempotency_key=f"order_fill_{fill.fill_id}",
                correlation_id=cid,
                metadata={
                    "fill_id": fill.fill_id,
                    "slippage": fill.slippage,
                    "order_type": request.order_type.value,
                    "market_data_timestamp": fill.market_data_timestamp,
                    "market_data_source": fill.market_data_source,
                },
            )
            self.ledger.record_entry(fill_entry)

            # Publish Events & Update StateStore
            self._publish_event(EventType.PAPER_ORDER_FILLED, filled_order.to_dict(), correlation_id=cid)
            self._publish_event(EventType.PAPER_PORTFOLIO_UPDATED, self.portfolio.to_dict(), correlation_id=cid)
            self._update_state_store()

            logger.info(
                "Paper order filled [order_id=%s, symbol=%s, side=%s, qty=%.2f, price=%.2f, fee=%.2f]",
                order_id,
                request.symbol,
                request.side.value,
                fill.quantity,
                fill.price,
                fill.fee,
            )

            return PaperExecutionResult(
                success=True,
                order=filled_order,
                fill=fill,
            )

    def _record_rejection(self, order: PaperOrder, cid: str) -> None:
        """Record order rejection in ledger and publish rejection event."""
        rej_entry = PaperLedgerEntry(
            entry_id=f"ledg_rej_{uuid.uuid4().hex[:8]}",
            timestamp=order.updated_at,
            entry_type=LedgerEntryType.ORDER_REJECTION,
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side.value,
            quantity=order.quantity,
            cash_change=0.0,
            cash_balance_after=self.portfolio.available_cash,
            idempotency_key=f"order_reject_{order.order_id}",
            correlation_id=cid,
            metadata={
                "order_type": order.order_type.value,
                "rejection_code": order.rejection_code.value if order.rejection_code else None,
                "rejection_reason": order.rejection_reason,
            },
        )
        self.ledger.record_entry(rej_entry)
        self._publish_event(EventType.PAPER_ORDER_REJECTED, order.to_dict(), correlation_id=cid)
        logger.warning(
            "Paper order rejected [order_id=%s, code=%s, reason=%s]",
            order.order_id,
            order.rejection_code,
            order.rejection_reason,
        )

    def cancel_order(self, order_id: str, reason: str = "User cancellation") -> PaperExecutionResult:
        """Cancel an open/pending paper order."""
        now = time.time()
        with self._lock:
            order = self._orders.get(order_id)
            if not order:
                return PaperExecutionResult(
                    success=False,
                    order=PaperOrder(
                        order_id=order_id,
                        client_order_id=order_id,
                        symbol="UNKNOWN",
                        side=PaperOrderSide.BUY,
                        order_type=PaperOrderType.MARKET,
                        quantity=0.0,
                        limit_price=None,
                        status=PaperOrderStatus.REJECTED,
                        created_at=now,
                        updated_at=now,
                        rejection_code=PaperRejectionCode.ORDER_NOT_FOUND,
                        rejection_reason=f"Order '{order_id}' was not found",
                    ),
                    rejection_code=PaperRejectionCode.ORDER_NOT_FOUND,
                    rejection_reason=f"Order '{order_id}' was not found",
                )

            if order.status in (PaperOrderStatus.FILLED, PaperOrderStatus.CANCELLED, PaperOrderStatus.REJECTED):
                return PaperExecutionResult(
                    success=False,
                    order=order,
                    rejection_code=PaperRejectionCode.ALREADY_TERMINAL,
                    rejection_reason=f"Order '{order_id}' is already in terminal state {order.status.value}",
                )

            cancelled_order = PaperOrder(
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                limit_price=order.limit_price,
                status=PaperOrderStatus.CANCELLED,
                created_at=order.created_at,
                updated_at=now,
                rejection_reason=reason,
                correlation_id=order.correlation_id,
            )
            self._orders[order_id] = cancelled_order

            cancel_entry = PaperLedgerEntry(
                entry_id=f"ledg_can_{uuid.uuid4().hex[:8]}",
                timestamp=now,
                entry_type=LedgerEntryType.ORDER_CANCELLATION,
                order_id=order_id,
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                side=order.side.value,
                quantity=order.quantity,
                cash_change=0.0,
                cash_balance_after=self.portfolio.available_cash,
                idempotency_key=f"order_cancel_{order_id}",
                correlation_id=order.correlation_id,
                metadata={"order_type": order.order_type.value, "cancellation_reason": reason},
            )
            self.ledger.record_entry(cancel_entry)
            self._publish_event(EventType.PAPER_ORDER_CANCELLED, cancelled_order.to_dict(), correlation_id=order.correlation_id)
            self._update_state_store()

            logger.info("Paper order cancelled [order_id=%s, reason=%s]", order_id, reason)
            return PaperExecutionResult(success=True, order=cancelled_order)

    def _process_pending_limit_orders(self, quote: Quote) -> List[PaperExecutionResult]:
        """Check all open (ACCEPTED) limit orders for quote.symbol and execute if eligible."""
        if not self.is_enabled:
            return []

        norm_sym = quote.symbol.strip().upper()
        results: List[PaperExecutionResult] = []

        # Find orders that are ACCEPTED, LIMIT, and match the symbol
        pending_orders = [
            o
            for o in list(self._orders.values())
            if o.symbol == norm_sym
            and o.status == PaperOrderStatus.ACCEPTED
            and o.order_type == PaperOrderType.LIMIT
        ]

        for order in pending_orders:
            # Check broker execution against the incoming quote
            is_filled, fill, fill_rej_code, fill_rej_reason = self.broker_adapter.execute_order(
                order=order,
                quote=quote,
                reference_time=quote.timestamp,
            )
            if not is_filled:
                continue

            assert fill is not None
            # Check cash sufficiency before applying fill for BUY orders
            if order.side == PaperOrderSide.BUY:
                cash_needed = round(fill.gross_notional + fill.fee, 4)
                if self.portfolio.available_cash < cash_needed:
                    logger.warning(
                        "Pending limit order %s could not fill due to insufficient cash (%s < %s)",
                        order.order_id,
                        self.portfolio.available_cash,
                        cash_needed,
                    )
                    continue

            # Apply fill to portfolio
            self.portfolio.apply_fill(fill)
            self._fills[fill.fill_id] = fill

            now = quote.timestamp
            cid = order.correlation_id or uuid.uuid4().hex
            filled_order = PaperOrder(
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                limit_price=order.limit_price,
                status=PaperOrderStatus.FILLED,
                created_at=order.created_at,
                updated_at=now,
                filled_at=fill.timestamp,
                fill_price=fill.price,
                filled_quantity=fill.quantity,
                fee=fill.fee,
                slippage=fill.slippage,
                correlation_id=cid,
            )
            self._orders[order.order_id] = filled_order

            # Record Fill in Ledger
            fill_entry = PaperLedgerEntry(
                entry_id=f"ledg_fill_{uuid.uuid4().hex[:8]}",
                timestamp=now,
                entry_type=LedgerEntryType.ORDER_FILL,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                symbol=order.symbol,
                side=order.side.value,
                quantity=fill.quantity,
                price=fill.price,
                fee=fill.fee,
                cash_change=fill.net_cash_impact,
                cash_balance_after=self.portfolio.available_cash,
                realized_pnl=self.portfolio.realized_pnl,
                idempotency_key=f"order_fill_{fill.fill_id}",
                correlation_id=cid,
                metadata={
                    "fill_id": fill.fill_id,
                    "slippage": fill.slippage,
                    "order_type": order.order_type.value,
                    "market_data_timestamp": fill.market_data_timestamp,
                    "market_data_source": fill.market_data_source,
                },
            )
            self.ledger.record_entry(fill_entry)

            # Publish events
            self._publish_event(EventType.PAPER_ORDER_FILLED, filled_order.to_dict(), correlation_id=cid)
            self._publish_event(EventType.PAPER_PORTFOLIO_UPDATED, self.portfolio.to_dict(), correlation_id=cid)

            logger.info(
                "Pending limit order filled on market update [order_id=%s, symbol=%s, price=%.2f, qty=%.2f]",
                order.order_id,
                order.symbol,
                fill.price,
                fill.quantity,
            )
            results.append(PaperExecutionResult(success=True, order=filled_order, fill=fill))

        return results

    def on_market_quote(self, quote: Quote) -> List[PaperExecutionResult]:
        """Handle incoming market quote to update mark-to-market position valuations,
        cache quotes, and check open pending limit orders for execution."""
        with self._lock:
            self._quotes[quote.symbol.strip().upper()] = quote
            self.portfolio.mark_to_market(quote)
            results = self._process_pending_limit_orders(quote)
            self._update_state_store()
            return results

    def get_quote(self, symbol: str) -> Optional[Quote]:
        """Retrieve cached quote for symbol if available."""
        with self._lock:
            return self._quotes.get(symbol.strip().upper())

    def get_order(self, order_id: str) -> Optional[PaperOrder]:
        """Retrieve an order by its unique order_id."""
        with self._lock:
            return self._orders.get(order_id)

    def get_order_by_client_id(self, client_order_id: str) -> Optional[PaperOrder]:
        """Retrieve an order by its client_order_id."""
        with self._lock:
            oid = self._client_order_map.get(client_order_id)
            return self._orders.get(oid) if oid else None

    def list_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[PaperOrderStatus] = None,
    ) -> List[PaperOrder]:
        """List recorded paper orders filtered by symbol or status."""
        with self._lock:
            orders = list(self._orders.values())
            if symbol:
                norm_sym = symbol.strip().upper()
                orders = [o for o in orders if o.symbol.upper() == norm_sym]
            if status:
                orders = [o for o in orders if o.status == status]
            return orders

    def get_portfolio(self) -> PaperPortfolio:
        """Return the current paper portfolio instance."""
        return self.portfolio

    def get_ledger(self) -> PaperLedger:
        """Return the audit ledger instance."""
        return self.ledger

    def get_status(self) -> Dict[str, Any]:
        """Return diagnostic operational status of the paper execution engine."""
        with self._lock:
            return {
                "name": self.name,
                "is_enabled": self.is_enabled,
                "execution_locked": self.settings.execution_locked,
                "broker_provider": self.settings.broker_provider,
                "total_orders": len(self._orders),
                "total_fills": len(self._fills),
                "portfolio": self.portfolio.to_dict(),
                "ledger_entries_count": self.ledger.count(),
            }
