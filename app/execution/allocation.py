"""KAISER — Multi-Asset Portfolio Allocation & Cross-Strategy Capital Partitioning.

Provides a strongly-typed, auditable portfolio-allocation layer for paper trading
that partitions available simulated capital across multiple assets and strategy sleeves.

MANDATORY SAFETY INVARIANTS:
1. PURELY SIMULATED: Never connects to a live broker, exchange, or trading API.
2. EXECUTION LOCKED: Live execution remains locked by default; paper mode is strictly isolated.
3. FAIL CLOSED: Missing, invalid, non-finite, or out-of-budget requests lead to explicit rejection.
4. AUDITABLE: All allocation partitions, orders, fills, rejections, and transfers are recorded in the ledger.
5. ISOLATION: A strategy sleeve cannot spend another sleeve's reserved capital.
6. NO AGGREGATE BYPASS: Multi-sleeve allocations cannot bypass aggregate portfolio risk limits.
7. NOT STRATEGY ADVICE: Simulation results and allocations are strictly operational foundations,
   not recommendations or guarantees of market profitability.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import copy
import hashlib
import json
import logging
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple
import uuid

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
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
from app.execution.outbox_dispatcher import OutboxDispatcher
from app.execution.portfolio import PaperPortfolio, PaperPosition
from app.execution.risk import PaperRiskGate
from app.execution.simulated_broker import SimulatedBrokerAdapter
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.ledger.outbox import OutboxRecord, TransactionalOutbox
from app.market.models import Quote

logger = logging.getLogger("execution.allocation")

# Pattern for safe, valid sleeve identifiers
SAFE_SLEEVE_ID_REGEX = re.compile(r"^[A-Za-z0-9_\-\.]{2,64}$")
SUPPORTED_ASSET_CLASSES = frozenset({"EQUITY"})


class AllocationStatus(str, Enum):
    """Lifecycle status of a portfolio capital allocation."""

    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ACTIVE = "ACTIVE"
    TERMINATED = "TERMINATED"


class AllocationRejectionCode(str, Enum):
    """Structured rejection reasons for capital allocation and sleeve operations."""

    INVALID_CAPITAL = "INVALID_CAPITAL"
    INVALID_WEIGHTS = "INVALID_WEIGHTS"
    WEIGHT_SUM_EXCEEDED = "WEIGHT_SUM_EXCEEDED"
    CAPITAL_EXCEEDS_PORTFOLIO = "CAPITAL_EXCEEDS_PORTFOLIO"
    DUPLICATE_SLEEVE_ID = "DUPLICATE_SLEEVE_ID"
    INVALID_SLEEVE_ID = "INVALID_SLEEVE_ID"
    NON_FINITE_VALUE = "NON_FINITE_VALUE"
    UNSUPPORTED_ASSET_CLASS = "UNSUPPORTED_ASSET_CLASS"
    UNSUPPORTED_ORDER_TYPE = "UNSUPPORTED_ORDER_TYPE"
    UNAUTHORIZED_TRANSFER = "UNAUTHORIZED_TRANSFER"
    INSUFFICIENT_SLEEVE_CASH = "INSUFFICIENT_SLEEVE_CASH"
    SLEEVE_NOT_FOUND = "SLEEVE_NOT_FOUND"
    SLEEVE_ORDER_NOTIONAL_EXCEEDED = "SLEEVE_ORDER_NOTIONAL_EXCEEDED"
    SLEEVE_POSITION_EXPOSURE_EXCEEDED = "SLEEVE_POSITION_EXPOSURE_EXCEEDED"
    PORTFOLIO_EXPOSURE_EXCEEDED = "PORTFOLIO_EXPOSURE_EXCEEDED"
    PORTFOLIO_NOTIONAL_EXCEEDED = "PORTFOLIO_NOTIONAL_EXCEEDED"
    DISALLOWED_SYMBOL = "DISALLOWED_SYMBOL"
    DUPLICATE_REQUEST_ID = "DUPLICATE_REQUEST_ID"
    DUPLICATE_CLIENT_ORDER_ID = "DUPLICATE_CLIENT_ORDER_ID"
    INSUFFICIENT_POSITION = "INSUFFICIENT_POSITION"
    SHORT_SELLING_PROHIBITED = "SHORT_SELLING_PROHIBITED"
    INVALID_MARKET_DATA = "INVALID_MARKET_DATA"
    MISSING_MARKET_DATA = "MISSING_MARKET_DATA"
    STALE_MARKET_DATA = "STALE_MARKET_DATA"


@dataclass(frozen=True)
class SleeveRiskBudget:
    """Configurable risk and exposure limits specific to a strategy sleeve."""

    max_order_notional: float = 25_000.0
    max_position_exposure_pct: float = 30.0  # Max % of sleeve equity in any single symbol
    max_sleeve_exposure_pct: float = 100.0   # Max % of sleeve equity in total positions
    allow_short_selling: bool = False        # Strictly False in paper simulation
    fee_rate: float = 0.0005                 # 5 bps
    slippage_rate: float = 0.0002            # 2 bps
    half_spread_rate: float = 0.0005         # 5 bps
    freshness_threshold_seconds: float = 60.0

    def __post_init__(self) -> None:
        if not math.isfinite(self.max_order_notional) or self.max_order_notional <= 0:
            raise ValidationError(f"max_order_notional must be a positive finite float, got {self.max_order_notional}")
        if not math.isfinite(self.max_position_exposure_pct) or self.max_position_exposure_pct <= 0 or self.max_position_exposure_pct > 100.0:
            raise ValidationError(f"max_position_exposure_pct must be in (0, 100], got {self.max_position_exposure_pct}")
        if not math.isfinite(self.max_sleeve_exposure_pct) or self.max_sleeve_exposure_pct <= 0 or self.max_sleeve_exposure_pct > 100.0:
            raise ValidationError(f"max_sleeve_exposure_pct must be in (0, 100], got {self.max_sleeve_exposure_pct}")
        if self.allow_short_selling:
            raise ValidationError("Short selling is strictly prohibited in paper trading simulation.")
        if not math.isfinite(self.fee_rate) or self.fee_rate < 0:
            raise ValidationError(f"fee_rate cannot be negative, got {self.fee_rate}")
        if not math.isfinite(self.slippage_rate) or self.slippage_rate < 0:
            raise ValidationError(f"slippage_rate cannot be negative, got {self.slippage_rate}")
        if not math.isfinite(self.freshness_threshold_seconds) or self.freshness_threshold_seconds <= 0:
            raise ValidationError(f"freshness_threshold_seconds must be positive, got {self.freshness_threshold_seconds}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_order_notional": self.max_order_notional,
            "max_position_exposure_pct": self.max_position_exposure_pct,
            "max_sleeve_exposure_pct": self.max_sleeve_exposure_pct,
            "allow_short_selling": self.allow_short_selling,
            "fee_rate": self.fee_rate,
            "slippage_rate": self.slippage_rate,
            "half_spread_rate": self.half_spread_rate,
            "freshness_threshold_seconds": self.freshness_threshold_seconds,
        }


@dataclass(frozen=True)
class StrategySleeveDefinition:
    """Immutable specification defining a strategy sleeve and its capital target."""

    sleeve_id: str
    name: str
    target_weight: Optional[float] = None
    target_budget: Optional[float] = None
    allowed_symbols: Optional[Sequence[str]] = None
    allowed_asset_classes: Sequence[str] = field(default_factory=lambda: ("EQUITY",))
    risk_budget: SleeveRiskBudget = field(default_factory=SleeveRiskBudget)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.sleeve_id or not isinstance(self.sleeve_id, str):
            raise ValidationError("sleeve_id must be a non-empty string.")
        clean_id = self.sleeve_id.strip()
        if not SAFE_SLEEVE_ID_REGEX.match(clean_id):
            raise ValidationError(f"Invalid sleeve_id '{self.sleeve_id}'. Must be alphanumeric with '-' or '_', length 2-64.")
        object.__setattr__(self, "sleeve_id", clean_id)

        if not self.name or not isinstance(self.name, str) or not self.name.strip():
            raise ValidationError("Sleeve name must be a non-empty string.")
        object.__setattr__(self, "name", self.name.strip())

        if self.target_weight is None and self.target_budget is None:
            raise ValidationError(f"Sleeve '{self.sleeve_id}' requires at least one of target_weight or target_budget.")

        if self.target_weight is not None:
            if not math.isfinite(self.target_weight) or self.target_weight <= 0 or self.target_weight > 1.0:
                raise ValidationError(f"target_weight for sleeve '{self.sleeve_id}' must be in (0, 1.0], got {self.target_weight}")

        if self.target_budget is not None:
            if not math.isfinite(self.target_budget) or self.target_budget <= 0:
                raise ValidationError(f"target_budget for sleeve '{self.sleeve_id}' must be a positive finite number, got {self.target_budget}")

        if self.allowed_symbols is not None:
            clean_syms = tuple(s.strip().upper() for s in self.allowed_symbols if s and isinstance(s, str))
            if not clean_syms:
                raise ValidationError(f"allowed_symbols for sleeve '{self.sleeve_id}' cannot be empty when provided.")
            object.__setattr__(self, "allowed_symbols", clean_syms)

        # Asset class validation: fail closed on unsupported asset classes
        for ac in self.allowed_asset_classes:
            norm_ac = str(ac).strip().upper()
            if norm_ac not in SUPPORTED_ASSET_CLASSES:
                raise ValidationError(
                    f"Unsupported asset class '{ac}' in sleeve '{self.sleeve_id}'. "
                    f"Supported classes: {sorted(SUPPORTED_ASSET_CLASSES)}."
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sleeve_id": self.sleeve_id,
            "name": self.name,
            "target_weight": self.target_weight,
            "target_budget": self.target_budget,
            "allowed_symbols": list(self.allowed_symbols) if self.allowed_symbols else None,
            "allowed_asset_classes": list(self.allowed_asset_classes),
            "risk_budget": self.risk_budget.to_dict(),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class PortfolioAllocationRequest:
    """Explicit, strongly-typed request to partition simulated capital among strategy sleeves."""

    request_id: str
    timestamp: float = field(default_factory=time.time)
    portfolio_id: str = "default_paper_portfolio"
    total_capital: Optional[float] = None
    sleeves: Sequence[StrategySleeveDefinition] = field(default_factory=list)
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.request_id or not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValidationError("PortfolioAllocationRequest request_id must be a non-empty string.")
        object.__setattr__(self, "request_id", self.request_id.strip())

        if not self.portfolio_id or not isinstance(self.portfolio_id, str) or not self.portfolio_id.strip():
            raise ValidationError("portfolio_id must be a non-empty string.")
        object.__setattr__(self, "portfolio_id", self.portfolio_id.strip())

        if self.total_capital is not None:
            if not math.isfinite(self.total_capital) or self.total_capital <= 0:
                raise ValidationError(f"total_capital must be positive finite float, got {self.total_capital}")

        if not self.sleeves:
            raise ValidationError("PortfolioAllocationRequest must include at least one strategy sleeve.")

        # Check unique sleeve IDs
        seen_ids: Set[str] = set()
        for slv in self.sleeves:
            if slv.sleeve_id in seen_ids:
                raise ValidationError(f"Duplicate sleeve_id '{slv.sleeve_id}' in allocation request.")
            seen_ids.add(slv.sleeve_id)

        # Check weight normalization if all sleeves specify target_weight
        weights = [s.target_weight for s in self.sleeves if s.target_weight is not None]
        if len(weights) == len(self.sleeves):
            total_weight = sum(weights)
            if total_weight > 1.00001:
                raise ValidationError(f"Sum of sleeve target_weights ({total_weight:.4f}) exceeds 1.0")

        # Check budget sum if all sleeves specify target_budget and total_capital is provided
        budgets = [s.target_budget for s in self.sleeves if s.target_budget is not None]
        if len(budgets) == len(self.sleeves) and self.total_capital is not None:
            total_budget = sum(budgets)
            if total_budget > self.total_capital + 0.0001:
                raise ValidationError(
                    f"Sum of sleeve target_budgets ({total_budget:.2f}) exceeds total_capital ({self.total_capital:.2f})"
                )

        if self.correlation_id is None:
            object.__setattr__(self, "correlation_id", uuid.uuid4().hex)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "timestamp": self.timestamp,
            "portfolio_id": self.portfolio_id,
            "total_capital": self.total_capital,
            "sleeves": [s.to_dict() for s in self.sleeves],
            "correlation_id": self.correlation_id,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class SleeveAllocationResult:
    """Allocation outcome for a single strategy sleeve."""

    sleeve_id: str
    name: str
    allocated_cash: float
    effective_weight: float
    status: AllocationStatus
    risk_budget: SleeveRiskBudget
    allowed_symbols: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sleeve_id": self.sleeve_id,
            "name": self.name,
            "allocated_cash": self.allocated_cash,
            "effective_weight": self.effective_weight,
            "status": self.status.value,
            "risk_budget": self.risk_budget.to_dict(),
            "allowed_symbols": self.allowed_symbols,
        }


@dataclass(frozen=True)
class PortfolioAllocationResult:
    """Immutable result of a portfolio allocation request."""

    request_id: str
    status: AllocationStatus
    success: bool
    total_portfolio_capital: float
    allocated_capital: float
    unallocated_capital: float
    sleeve_allocations: Dict[str, SleeveAllocationResult]
    rejection_code: Optional[AllocationRejectionCode] = None
    rejection_reason: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    deterministic_digest: str = ""
    event_delivery_failed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "status": self.status.value,
            "success": self.success,
            "total_portfolio_capital": self.total_portfolio_capital,
            "allocated_capital": self.allocated_capital,
            "unallocated_capital": self.unallocated_capital,
            "sleeve_allocations": {k: v.to_dict() for k, v in self.sleeve_allocations.items()},
            "rejection_code": self.rejection_code.value if self.rejection_code else None,
            "rejection_reason": self.rejection_reason,
            "timestamp": self.timestamp,
            "deterministic_digest": self.deterministic_digest,
            "event_delivery_failed": self.event_delivery_failed,
            "advisory_disclaimer": "SIMULATION ONLY: Allocation results reflect deterministic partition models and do not predict market returns.",
        }


class StrategySleeve:
    """Active runtime state and accounting container for an individual strategy sleeve."""

    def __init__(
        self,
        sleeve_id: str,
        name: str,
        allocated_capital: float,
        risk_budget: Optional[SleeveRiskBudget] = None,
        allowed_symbols: Optional[Sequence[str]] = None,
        allowed_asset_classes: Optional[Sequence[str]] = None,
    ) -> None:
        self.sleeve_id = sleeve_id
        self.name = name
        self.allocated_capital = round(float(allocated_capital), 4)
        self.available_cash = round(float(allocated_capital), 4)
        self.reserved_cash = 0.0
        self.realized_pnl = 0.0
        self.total_fees_paid = 0.0
        self.positions: Dict[str, PaperPosition] = {}
        self.risk_budget = risk_budget or SleeveRiskBudget()
        self.allowed_symbols: Optional[Set[str]] = (
            set(s.strip().upper() for s in allowed_symbols) if allowed_symbols else None
        )
        self.allowed_asset_classes: Set[str] = set(
            str(ac).strip().upper() for ac in (allowed_asset_classes or ["EQUITY"])
        )

    @property
    def total_position_value(self) -> float:
        """Current mark-to-market value of all positions in this sleeve."""
        return round(sum(pos.market_value for pos in self.positions.values()), 4)

    @property
    def total_equity(self) -> float:
        """Total sleeve equity = available cash + reserved cash + position value."""
        return round(self.available_cash + self.reserved_cash + self.total_position_value, 4)

    @property
    def unrealized_pnl(self) -> float:
        """Sum of unrealized P&L across all open positions in this sleeve."""
        return round(sum(pos.unrealized_pnl for pos in self.positions.values()), 4)

    @property
    def total_pnl(self) -> float:
        """Total P&L = realized + unrealized."""
        return round(self.realized_pnl + self.unrealized_pnl, 4)

    @property
    def utilization_pct(self) -> float:
        """Percentage of total sleeve equity deployed in open positions."""
        eq = self.total_equity
        if eq <= 0:
            return 0.0
        return round((self.total_position_value / eq) * 100.0, 2)

    def get_position(self, symbol: str) -> Optional[PaperPosition]:
        return self.positions.get(symbol.strip().upper())

    def apply_fill(self, fill: PaperFill) -> float:
        """Apply a simulated fill to this sleeve's positions and cash.

        Returns:
            float: Realized P&L from this fill (0.0 for BUY).
        """
        symbol = fill.symbol.strip().upper()
        gross = fill.gross_notional
        fee = fill.fee

        if fill.side == PaperOrderSide.BUY:
            total_cash_required = round(gross + fee, 4)
            if self.available_cash < total_cash_required:
                raise ValidationError(
                    f"Sleeve '{self.sleeve_id}' has insufficient cash for fill: "
                    f"required {total_cash_required:.2f}, available {self.available_cash:.2f}"
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
                    f"Cannot execute SELL for {symbol} in sleeve '{self.sleeve_id}': "
                    f"position {current_qty} is less than fill quantity {fill.quantity}"
                )

            net_proceeds = round(gross - fee, 4)
            self.available_cash = round(self.available_cash + net_proceeds, 4)
            self.total_fees_paid = round(self.total_fees_paid + fee, 4)

            trade_realized = round(((fill.price - existing.avg_entry_price) * fill.quantity) - fee, 4)
            self.realized_pnl = round(self.realized_pnl + trade_realized, 4)
            existing.realized_pnl = round(existing.realized_pnl + trade_realized, 4)
            existing.accumulated_fees = round(existing.accumulated_fees + fee, 4)

            new_qty = round(existing.quantity - fill.quantity, 6)
            if new_qty <= 1e-9:
                del self.positions[symbol]
            else:
                existing.quantity = new_qty
                existing.current_price = fill.price

            return trade_realized

        return 0.0

    def mark_to_market(self, quote_or_symbol: Quote | str, price: Optional[float] = None) -> None:
        """Update mark-to-market position price."""
        if isinstance(quote_or_symbol, Quote):
            symbol = quote_or_symbol.symbol.strip().upper()
            target_price = quote_or_symbol.last_price
        else:
            symbol = quote_or_symbol.strip().upper()
            target_price = price if price is not None else 0.0

        pos = self.positions.get(symbol)
        if pos is not None and math.isfinite(target_price) and target_price > 0:
            pos.current_price = target_price

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sleeve_id": self.sleeve_id,
            "name": self.name,
            "allocated_capital": self.allocated_capital,
            "available_cash": self.available_cash,
            "reserved_cash": self.reserved_cash,
            "total_position_value": self.total_position_value,
            "total_equity": self.total_equity,
            "realized_pnl": round(self.realized_pnl, 4),
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl,
            "total_fees_paid": round(self.total_fees_paid, 4),
            "utilization_pct": self.utilization_pct,
            "positions": {sym: pos.to_dict() for sym, pos in self.positions.items()},
            "risk_budget": self.risk_budget.to_dict(),
            "allowed_symbols": sorted(self.allowed_symbols) if self.allowed_symbols else None,
        }

    def snapshot(self) -> Dict[str, Any]:
        """Capture point-in-time state snapshot for transactional rollback."""
        return {
            "available_cash": self.available_cash,
            "reserved_cash": self.reserved_cash,
            "realized_pnl": self.realized_pnl,
            "total_fees_paid": self.total_fees_paid,
            "positions": {
                sym: copy.copy(pos) for sym, pos in self.positions.items()
            },
        }

    def restore_snapshot(self, snap: Dict[str, Any]) -> None:
        """Restore point-in-time state from snapshot."""
        self.available_cash = snap["available_cash"]
        self.reserved_cash = snap["reserved_cash"]
        self.realized_pnl = snap["realized_pnl"]
        self.total_fees_paid = snap["total_fees_paid"]
        self.positions = snap["positions"]

    def reserve_cash(self, amount: float) -> bool:
        """Reserve cash within the sleeve for an in-flight order."""
        if not math.isfinite(amount) or amount <= 0:
            return False
        amt = round(float(amount), 4)
        if self.available_cash < amt:
            return False
        self.available_cash = round(self.available_cash - amt, 4)
        self.reserved_cash = round(self.reserved_cash + amt, 4)
        return True

    def release_reserved_cash(self, amount: float) -> bool:
        """Release reserved cash back to available sleeve cash."""
        if not math.isfinite(amount) or amount <= 0:
            return False
        amt = round(float(amount), 4)
        if self.reserved_cash <= 0:
            return False
        actual_release = min(self.reserved_cash, amt)
        self.reserved_cash = round(self.reserved_cash - actual_release, 4)
        self.available_cash = round(self.available_cash + actual_release, 4)
        return True


@dataclass(frozen=True)
class SleeveExposureSnapshot:
    """Point-in-time exposure and accounting snapshot of a single strategy sleeve."""

    sleeve_id: str
    name: str
    allocated_capital: float
    available_cash: float
    reserved_cash: float
    total_position_value: float
    total_equity: float
    realized_pnl: float
    unrealized_pnl: float
    total_fees_paid: float
    positions: Dict[str, Dict[str, Any]]
    utilization_pct: float
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sleeve_id": self.sleeve_id,
            "name": self.name,
            "allocated_capital": self.allocated_capital,
            "available_cash": self.available_cash,
            "reserved_cash": self.reserved_cash,
            "total_position_value": self.total_position_value,
            "total_equity": self.total_equity,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_fees_paid": self.total_fees_paid,
            "positions": self.positions,
            "utilization_pct": self.utilization_pct,
            "timestamp": self.timestamp,
        }


@dataclass(frozen=True)
class PortfolioExposureSnapshot:
    """Point-in-time consolidated exposure snapshot across all sleeves and portfolio assets."""

    total_portfolio_equity: float
    total_allocated_capital: float
    unallocated_cash: float
    total_position_value: float
    total_cash: float
    realized_pnl: float
    unrealized_pnl: float
    total_fees_paid: float
    sleeves: Dict[str, SleeveExposureSnapshot]
    aggregate_symbol_exposures: Dict[str, Dict[str, Any]]
    is_reconciled: bool
    reconciliation_discrepancy: float
    timestamp: float
    deterministic_digest: str
    event_delivery_failures: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_portfolio_equity": self.total_portfolio_equity,
            "total_allocated_capital": self.total_allocated_capital,
            "unallocated_cash": self.unallocated_cash,
            "total_position_value": self.total_position_value,
            "total_cash": self.total_cash,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_fees_paid": self.total_fees_paid,
            "sleeves": {k: v.to_dict() for k, v in self.sleeves.items()},
            "aggregate_symbol_exposures": self.aggregate_symbol_exposures,
            "is_reconciled": self.is_reconciled,
            "reconciliation_discrepancy": self.reconciliation_discrepancy,
            "timestamp": self.timestamp,
            "deterministic_digest": self.deterministic_digest,
            "event_delivery_failures": self.event_delivery_failures,
            "advisory_disclaimer": "SIMULATION ONLY: Capital partitioning and exposure metrics are offline simulation tools.",
        }


@dataclass(frozen=True)
class PortfolioEquitySnapshot:
    """Immutable point-in-time portfolio equity and exposure snapshot."""

    snapshot_id: str
    timestamp: float
    currency: str
    total_equity: float
    available_cash: float = 0.0
    reserved_cash: float = 0.0
    unallocated_cash: float = 0.0
    allocated_capital: float = 0.0
    total_position_value: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_fees_paid: float = 0.0
    sleeve_equities: Dict[str, float] = field(default_factory=dict)
    positions_summary: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    schema_version: int = 1
    correlation_id: Optional[str] = None
    source: str = "manual"
    is_reconciled: bool = True
    reconciliation_discrepancy: float = 0.0
    deterministic_digest: str = ""

    @property
    def cash(self) -> float:
        return self.available_cash

    @property
    def positions_valuation(self) -> float:
        return self.total_position_value

    @property
    def sleeve_allocations(self) -> Dict[str, float]:
        return self.sleeve_equities

    def __post_init__(self) -> None:
        if not self.snapshot_id or not isinstance(self.snapshot_id, str) or not self.snapshot_id.strip():
            raise ValidationError("PortfolioEquitySnapshot snapshot_id must be a non-empty string.")
        clean_id = self.snapshot_id.strip()
        object.__setattr__(self, "snapshot_id", clean_id)

        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"timestamp must be a positive finite float, got {self.timestamp}")

        if self.available_cash < 0:
            raise ValidationError(f"available_cash must be non-negative, got {self.available_cash}")
        if self.reserved_cash < 0:
            raise ValidationError(f"reserved_cash must be non-negative, got {self.reserved_cash}")

        for field_name in (
            "total_equity",
            "available_cash",
            "reserved_cash",
            "unallocated_cash",
            "allocated_capital",
            "total_position_value",
            "realized_pnl",
            "unrealized_pnl",
            "total_fees_paid",
            "reconciliation_discrepancy",
        ):
            val = getattr(self, field_name)
            if not isinstance(val, (int, float)) or not math.isfinite(val):
                raise ValidationError(f"{field_name} must be a finite number, got {val}")

        for sid, eq in self.sleeve_equities.items():
            if not isinstance(eq, (int, float)) or not math.isfinite(eq):
                raise ValidationError(f"Sleeve '{sid}' equity must be finite, got {eq}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp,
            "currency": self.currency,
            "total_equity": self.total_equity,
            "available_cash": self.available_cash,
            "reserved_cash": self.reserved_cash,
            "unallocated_cash": self.unallocated_cash,
            "allocated_capital": self.allocated_capital,
            "total_position_value": self.total_position_value,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_fees_paid": self.total_fees_paid,
            "sleeve_equities": dict(self.sleeve_equities),
            "positions_summary": dict(self.positions_summary),
            "schema_version": self.schema_version,
            "correlation_id": self.correlation_id,
            "source": self.source,
            "is_reconciled": self.is_reconciled,
            "reconciliation_discrepancy": self.reconciliation_discrepancy,
            "deterministic_digest": self.deterministic_digest,
            "advisory_disclaimer": "SIMULATION ONLY: Portfolio equity snapshots are offline paper-trading metrics.",
        }


@dataclass(frozen=True)
class ReconciliationCheckDetail:
    """Detail for a specific reconciliation dimension."""

    dimension: str
    expected_value: float
    actual_value: float
    discrepancy: float
    is_balanced: bool
    tolerance: float = 1e-4
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension": self.dimension,
            "expected_value": round(self.expected_value, 4),
            "actual_value": round(self.actual_value, 4),
            "discrepancy": round(self.discrepancy, 6),
            "is_balanced": self.is_balanced,
            "tolerance": self.tolerance,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class PortfolioSleeveReconciliationResult:
    """Structured, actionable mathematical reconciliation report between sleeves and portfolio."""

    is_reconciled: bool
    max_discrepancy: float
    tolerance: float
    discrepancies: List[str]
    checks: Dict[str, ReconciliationCheckDetail]
    portfolio_cash: float
    sum_sleeve_cash: float
    unallocated_cash: float
    portfolio_equity: float
    sum_sleeve_equity: float
    timestamp: float = field(default_factory=time.time)
    deterministic_digest: str = ""

    def __getitem__(self, key: str) -> Any:
        """Support dictionary-style access for backwards compatibility."""
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        """Safe dictionary-style get."""
        return getattr(self, key, default)

    def __contains__(self, key: str) -> bool:
        """Support 'in' operator."""
        return hasattr(self, key)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_reconciled": self.is_reconciled,
            "max_discrepancy": round(self.max_discrepancy, 6),
            "tolerance": self.tolerance,
            "discrepancies": list(self.discrepancies),
            "checks": {k: v.to_dict() for k, v in self.checks.items()},
            "portfolio_cash": round(self.portfolio_cash, 4),
            "sum_sleeve_cash": round(self.sum_sleeve_cash, 4),
            "unallocated_cash": round(self.unallocated_cash, 4),
            "portfolio_equity": round(self.portfolio_equity, 4),
            "sum_sleeve_equity": round(self.sum_sleeve_equity, 4),
            "timestamp": self.timestamp,
            "deterministic_digest": self.deterministic_digest,
            "advisory_disclaimer": "SIMULATION ONLY: Reconciliation metrics verify simulated accounting invariants and do not guarantee trading returns.",
        }


def _snapshot_portfolio(portfolio: PaperPortfolio) -> Dict[str, Any]:
    """Capture portfolio point-in-time state for rollback."""
    return {
        "available_cash": portfolio.available_cash,
        "realized_pnl": portfolio.realized_pnl,
        "total_fees_paid": portfolio.total_fees_paid,
        "positions": {
            sym: copy.copy(pos) for sym, pos in portfolio.positions.items()
        },
    }


def _restore_portfolio_snapshot(portfolio: PaperPortfolio, snap: Dict[str, Any]) -> None:
    """Restore portfolio state from snapshot."""
    portfolio.available_cash = snap["available_cash"]
    portfolio.realized_pnl = snap["realized_pnl"]
    portfolio.total_fees_paid = snap["total_fees_paid"]
    portfolio.positions = snap["positions"]


def compute_allocation_digest(result: PortfolioAllocationResult) -> str:
    """Compute a deterministic SHA-256 fingerprint of an allocation result."""
    parts = [
        f"req={result.request_id}",
        f"status={result.status.value}",
        f"total={result.total_portfolio_capital:.4f}",
        f"allocated={result.allocated_capital:.4f}",
        f"unallocated={result.unallocated_capital:.4f}",
    ]
    for sid in sorted(result.sleeve_allocations.keys()):
        sa = result.sleeve_allocations[sid]
        parts.append(f"sleeve:{sid}:cash={sa.allocated_cash:.4f}:weight={sa.effective_weight:.4f}")
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_multi_sleeve_financial_digest(
    portfolio: PaperPortfolio,
    sleeves: Mapping[str, StrategySleeve],
    unallocated_cash: float,
) -> str:
    """Compute a deterministic SHA-256 fingerprint of the multi-sleeve financial state."""
    parts = [
        f"port_cash={portfolio.available_cash:.4f}",
        f"port_equity={portfolio.total_equity:.4f}",
        f"port_fees={portfolio.total_fees_paid:.4f}",
        f"port_realized={portfolio.realized_pnl:.4f}",
        f"unallocated={unallocated_cash:.4f}",
    ]
    for sid in sorted(sleeves.keys()):
        s = sleeves[sid]
        parts.append(
            f"sleeve:{sid}:cash={s.available_cash:.4f}:equity={s.total_equity:.4f}:"
            f"realized={s.realized_pnl:.4f}:fees={s.total_fees_paid:.4f}"
        )
        for sym in sorted(s.positions.keys()):
            pos = s.positions[sym]
            parts.append(
                f"sleeve:{sid}:pos:{sym}:qty={pos.quantity:.6f}:avg={pos.avg_entry_price:.4f}:"
                f"cur={pos.current_price:.4f}"
            )
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class PortfolioAllocationManager:
    """Orchestrates capital partitioning, sleeve isolation, and multi-asset exposure controls."""

    def __init__(
        self,
        portfolio: PaperPortfolio,
        ledger: PaperLedger,
        risk_config: Optional[PaperRiskConfig] = None,
        event_bus: Optional[EventBus] = None,
        broker_adapter: Optional[SimulatedBrokerAdapter] = None,
        outbox: Optional[TransactionalOutbox] = None,
        outbox_dispatcher: Optional[OutboxDispatcher] = None,
        db_path: Optional[str] = None,
    ) -> None:
        self.portfolio = portfolio
        self.ledger = ledger
        self.db_path = db_path or getattr(self.ledger, "db_path", None)
        self.risk_config = risk_config or PaperRiskConfig()
        self.event_bus = event_bus
        self.broker_adapter = broker_adapter or SimulatedBrokerAdapter(config=self.risk_config)
        self.outbox: Optional[TransactionalOutbox] = outbox or getattr(self.ledger, "outbox", None)
        if outbox_dispatcher is not None:
            self.outbox_dispatcher = outbox_dispatcher
        elif self.outbox is not None:
            self.outbox_dispatcher = OutboxDispatcher(
                outbox=self.outbox,
                event_bus=self.event_bus,
                auto_start=False,
            )
        else:
            self.outbox_dispatcher = None

        self._sleeves: Dict[str, StrategySleeve] = {}
        self._unallocated_cash: float = portfolio.available_cash
        self._allocation_history: List[PortfolioAllocationResult] = []
        self._allocation_requests: Dict[str, PortfolioAllocationRequest] = {}
        self._allocation_results: Dict[str, PortfolioAllocationResult] = {}
        self._order_requests: Dict[str, Tuple[str, PaperOrderRequest]] = {}  # client_order_id -> (sleeve_id, request)
        self._order_results: Dict[str, PaperExecutionResult] = {}            # client_order_id -> result
        self._snapshots: Dict[str, PortfolioEquitySnapshot] = {}
        self._event_delivery_failures: List[Dict[str, Any]] = []
        self._failed_event_deliveries: int = 0
        self._lock = threading.RLock()

        # Initialize SQLite snapshot persistence if ledger has db_path
        if getattr(self.ledger, "db_path", None):
            self._init_snapshot_db()
            self.recover_snapshots()

        # If ledger already contains allocation entries, attempt recovery
        if self.ledger.count() > 0:
            self.recover_from_ledger(self.ledger)

    @property
    def unallocated_cash(self) -> float:
        """Available cash remaining at the portfolio level not reserved to any sleeve."""
        with self._lock:
            return round(self._unallocated_cash, 4)

    @property
    def event_delivery_failures_count(self) -> int:
        """Total count of event publication failures caught post-commit."""
        with self._lock:
            return self._failed_event_deliveries

    @property
    def event_delivery_failures(self) -> List[Dict[str, Any]]:
        """Audit list of event delivery failures captured post-commit."""
        with self._lock:
            return list(self._event_delivery_failures)

    @property
    def total_allocated_capital(self) -> float:
        """Sum of initial capital partitioned across all strategy sleeves."""
        with self._lock:
            return round(sum(s.allocated_capital for s in self._sleeves.values()), 4)

    @property
    def outbox_stats(self) -> Dict[str, Any]:
        """Aggregate statistics from the attached transactional outbox."""
        with self._lock:
            return self.outbox.get_stats() if self.outbox else {}

    def reserve_sleeve_capital(self, sleeve_id: str, amount: float) -> bool:
        """Reserve capital within a specific sleeve for pending execution."""
        with self._lock:
            sleeve = self._sleeves.get(sleeve_id.strip())
            if sleeve is None:
                return False
            return sleeve.reserve_cash(amount)

    def release_sleeve_capital(self, sleeve_id: str, amount: float) -> bool:
        """Release previously reserved capital back to sleeve available cash."""
        with self._lock:
            sleeve = self._sleeves.get(sleeve_id.strip())
            if sleeve is not None:
                return sleeve.release_reserved_cash(amount)
            return False

    def get_sleeve(self, sleeve_id: str) -> Optional[StrategySleeve]:
        """Retrieve a sleeve by its unique ID."""
        with self._lock:
            return self._sleeves.get(sleeve_id.strip())

    def list_sleeves(self) -> List[StrategySleeve]:
        """List all currently active strategy sleeves."""
        with self._lock:
            return list(self._sleeves.values())

    def apply_allocation(self, request: PortfolioAllocationRequest) -> PortfolioAllocationResult:
        """Partition simulated portfolio capital across specified strategy sleeves.

        Enforces:
        1. Idempotency: repeated requests with identical request_id return cached result.
        2. Finite, positive capital and weight limits.
        3. Total allocated capital cannot exceed available portfolio cash.
        4. Rejections leave portfolio and sleeve state untouched.
        5. Atomic ledger recording of sleeve capital partitions.
        """
        if request is None or not isinstance(request, PortfolioAllocationRequest):
            raise ValidationError("A valid PortfolioAllocationRequest is required.")

        with self._lock:
            # 1. Idempotency Check
            if request.request_id in self._allocation_results:
                existing_res = self._allocation_results[request.request_id]
                existing_req = self._allocation_requests.get(request.request_id)
                # If identical request, return cached result
                if existing_req and existing_req.to_dict() == request.to_dict():
                    return existing_res
                # Duplicate ID with different parameters fails
                return PortfolioAllocationResult(
                    request_id=request.request_id,
                    status=AllocationStatus.REJECTED,
                    success=False,
                    total_portfolio_capital=self.portfolio.available_cash,
                    allocated_capital=0.0,
                    unallocated_capital=self._unallocated_cash,
                    sleeve_allocations={},
                    rejection_code=AllocationRejectionCode.DUPLICATE_REQUEST_ID,
                    rejection_reason=f"Allocation request_id '{request.request_id}' has already been processed with different parameters.",
                    timestamp=time.time(),
                )

            # 2. Capital Bounds Check
            available_port_cash = self.portfolio.available_cash
            total_target_cap = (
                request.total_capital
                if request.total_capital is not None
                else available_port_cash
            )

            if total_target_cap > available_port_cash:
                rej_res = PortfolioAllocationResult(
                    request_id=request.request_id,
                    status=AllocationStatus.REJECTED,
                    success=False,
                    total_portfolio_capital=available_port_cash,
                    allocated_capital=0.0,
                    unallocated_capital=self._unallocated_cash,
                    sleeve_allocations={},
                    rejection_code=AllocationRejectionCode.CAPITAL_EXCEEDS_PORTFOLIO,
                    rejection_reason=f"Requested allocation capital ({total_target_cap:.2f}) exceeds available portfolio cash ({available_port_cash:.2f}).",
                    timestamp=time.time(),
                )
                self._allocation_results[request.request_id] = rej_res
                return rej_res

            # 3. Compute Sleeve Partitions
            planned_sleeve_cash: Dict[str, Tuple[float, float]] = {}  # sleeve_id -> (allocated_cash, effective_weight)
            total_planned_cash = 0.0

            for slv_def in request.sleeves:
                if slv_def.target_weight is not None:
                    cash_for_sleeve = round(total_target_cap * slv_def.target_weight, 4)
                    eff_weight = slv_def.target_weight
                elif slv_def.target_budget is not None:
                    cash_for_sleeve = round(slv_def.target_budget, 4)
                    eff_weight = round(cash_for_sleeve / total_target_cap, 6) if total_target_cap > 0 else 0.0
                else:
                    cash_for_sleeve = 0.0
                    eff_weight = 0.0

                if cash_for_sleeve <= 0 or not math.isfinite(cash_for_sleeve):
                    rej_res = PortfolioAllocationResult(
                        request_id=request.request_id,
                        status=AllocationStatus.REJECTED,
                        success=False,
                        total_portfolio_capital=available_port_cash,
                        allocated_capital=0.0,
                        unallocated_capital=self._unallocated_cash,
                        sleeve_allocations={},
                        rejection_code=AllocationRejectionCode.INVALID_CAPITAL,
                        rejection_reason=f"Sleeve '{slv_def.sleeve_id}' received invalid capital amount {cash_for_sleeve}.",
                        timestamp=time.time(),
                    )
                    self._allocation_results[request.request_id] = rej_res
                    return rej_res

                planned_sleeve_cash[slv_def.sleeve_id] = (cash_for_sleeve, eff_weight)
                total_planned_cash = round(total_planned_cash + cash_for_sleeve, 4)

            if total_planned_cash > available_port_cash:
                rej_res = PortfolioAllocationResult(
                    request_id=request.request_id,
                    status=AllocationStatus.REJECTED,
                    success=False,
                    total_portfolio_capital=available_port_cash,
                    allocated_capital=0.0,
                    unallocated_capital=self._unallocated_cash,
                    sleeve_allocations={},
                    rejection_code=AllocationRejectionCode.CAPITAL_EXCEEDS_PORTFOLIO,
                    rejection_reason=f"Sum of sleeve allocations ({total_planned_cash:.2f}) exceeds available cash ({available_port_cash:.2f}).",
                    timestamp=time.time(),
                )
                self._allocation_results[request.request_id] = rej_res
                return rej_res

            # 4. Atomically Apply Partitions with Rollback Protection
            sleeves_backup = {sid: copy.copy(s) for sid, s in self._sleeves.items()}
            unalloc_backup = self._unallocated_cash

            now = time.time()
            try:
                self._sleeves.clear()
                sleeve_results: Dict[str, SleeveAllocationResult] = {}
                alloc_entries: List[PaperLedgerEntry] = []

                for slv_def in request.sleeves:
                    cash_alloc, eff_w = planned_sleeve_cash[slv_def.sleeve_id]
                    new_sleeve = StrategySleeve(
                        sleeve_id=slv_def.sleeve_id,
                        name=slv_def.name,
                        allocated_capital=cash_alloc,
                        risk_budget=slv_def.risk_budget,
                        allowed_symbols=slv_def.allowed_symbols,
                        allowed_asset_classes=slv_def.allowed_asset_classes,
                    )
                    self._sleeves[slv_def.sleeve_id] = new_sleeve
                    sleeve_results[slv_def.sleeve_id] = SleeveAllocationResult(
                        sleeve_id=slv_def.sleeve_id,
                        name=slv_def.name,
                        allocated_cash=cash_alloc,
                        effective_weight=eff_w,
                        status=AllocationStatus.APPROVED,
                        risk_budget=slv_def.risk_budget,
                        allowed_symbols=list(slv_def.allowed_symbols) if slv_def.allowed_symbols else None,
                    )

                    # Record in durable ledger (cash_change=0.0 because it's an internal capital partition)
                    alloc_entry = PaperLedgerEntry(
                        entry_id=f"alloc_{uuid.uuid4().hex[:8]}",
                        timestamp=now,
                        entry_type=LedgerEntryType.CAPITAL_ALLOCATION,
                        cash_change=0.0,
                        cash_balance_after=self.portfolio.available_cash,
                        idempotency_key=f"alloc_{request.request_id}_{slv_def.sleeve_id}",
                        correlation_id=request.correlation_id,
                        metadata={
                            "request_id": request.request_id,
                            "sleeve_id": slv_def.sleeve_id,
                            "name": slv_def.name,
                            "allocated_cash": cash_alloc,
                            "effective_weight": eff_w,
                            "description": f"Capital partitioned to sleeve {slv_def.sleeve_id}",
                        },
                    )
                    alloc_entries.append(alloc_entry)

                temp_unalloc = round(available_port_cash - total_planned_cash, 4)
                alloc_outbox_rec = OutboxRecord(
                    event_id=f"outbox_alloc_{request.request_id}_{int(now * 1000)}",
                    event_type=EventType.PAPER_ALLOCATION_UPDATED.value,
                    schema_version=1,
                    correlation_id=request.correlation_id or request.request_id,
                    causation_id=request.request_id,
                    payload={
                        "request_id": request.request_id,
                        "status": AllocationStatus.APPROVED.value,
                        "total_portfolio_capital": available_port_cash,
                        "allocated_capital": total_planned_cash,
                        "unallocated_capital": temp_unalloc,
                        "sleeve_count": len(sleeve_results),
                        "timestamp": now,
                    },
                    created_at=now,
                )

                if self.outbox and hasattr(self.ledger, "record_entries_with_outbox"):
                    self.ledger.record_entries_with_outbox(alloc_entries, [alloc_outbox_rec])
                else:
                    for entry in alloc_entries:
                        self.ledger.record_entry(entry)

                self._unallocated_cash = temp_unalloc
            except Exception as exc:
                self._sleeves = sleeves_backup
                self._unallocated_cash = unalloc_backup
                logger.error("Transactional rollback in apply_allocation: %s", exc)
                raise

            # 5. Build Result & Digest
            success_result = PortfolioAllocationResult(
                request_id=request.request_id,
                status=AllocationStatus.APPROVED,
                success=True,
                total_portfolio_capital=available_port_cash,
                allocated_capital=total_planned_cash,
                unallocated_capital=self._unallocated_cash,
                sleeve_allocations=sleeve_results,
                timestamp=now,
            )
            digest = compute_allocation_digest(success_result)
            final_result = PortfolioAllocationResult(
                request_id=success_result.request_id,
                status=success_result.status,
                success=True,
                total_portfolio_capital=success_result.total_portfolio_capital,
                allocated_capital=success_result.allocated_capital,
                unallocated_capital=success_result.unallocated_capital,
                sleeve_allocations=success_result.sleeve_allocations,
                timestamp=now,
                deterministic_digest=digest,
            )

            self._allocation_requests[request.request_id] = request
            self._allocation_results[request.request_id] = final_result
            self._allocation_history.append(final_result)

            # 6. Post-Commit Event Publication (Non-Transactional Notification)
            # Transaction Boundary Definition:
            # Capital partitions and durable ledger records are ALREADY COMMITTED.
            # EventBus is an in-memory decoupled broker; publication failure does NOT roll back
            # the committed financial ledger state (which would cause fatal divergence between
            # ledger and memory). Instead, delivery failure is surfaced on the result container
            # and tracked in audit metrics and transactional outbox.
            event_failed = False
            if self.event_bus:
                try:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.PAPER_ALLOCATION_UPDATED,
                            source="portfolio_allocation_manager",
                            correlation_id=request.correlation_id or request.request_id,
                            payload=final_result.to_dict(),
                        )
                    )
                    if self.outbox:
                        self.outbox.mark_delivered(alloc_outbox_rec.event_id, delivered_at=now)
                except Exception as err:
                    event_failed = True
                    self._failed_event_deliveries += 1
                    self._event_delivery_failures.append({
                        "operation": "apply_allocation",
                        "request_id": request.request_id,
                        "error": str(err),
                        "timestamp": now,
                    })
                    if self.outbox:
                        self.outbox.record_failure(alloc_outbox_rec.event_id, error=str(err), next_retry_at=now + 2.0)
                    logger.warning(
                        "Post-commit event publication failed for allocation '%s': %s (financial state remains committed)",
                        request.request_id,
                        err,
                    )

            if event_failed:
                final_result = PortfolioAllocationResult(
                    request_id=final_result.request_id,
                    status=final_result.status,
                    success=True,
                    total_portfolio_capital=final_result.total_portfolio_capital,
                    allocated_capital=final_result.allocated_capital,
                    unallocated_capital=final_result.unallocated_capital,
                    sleeve_allocations=final_result.sleeve_allocations,
                    timestamp=final_result.timestamp,
                    deterministic_digest=final_result.deterministic_digest,
                    event_delivery_failed=True,
                )
                self._allocation_results[request.request_id] = final_result

            logger.info(
                "Capital allocation applied [request_id=%s, sleeves=%d, allocated=%.2f, unallocated=%.2f]",
                request.request_id,
                len(self._sleeves),
                total_planned_cash,
                self._unallocated_cash,
            )
            return final_result

    def execute_sleeve_order(
        self,
        sleeve_id: str,
        request: PaperOrderRequest,
        quote: Optional[Quote] = None,
        reference_time: Optional[float] = None,
    ) -> PaperExecutionResult:
        """Validate and execute a simulated order within an isolated strategy sleeve.

        Guarantees:
        1. Sleeve isolation: order can only spend sleeve's available cash.
        2. Per-sleeve risk gate: notional, position exposure, and allowed symbol check.
        3. Portfolio aggregate risk gate: order cannot breach portfolio-wide symbol exposure limits.
        4. Atomic accounting: fills update sleeve and portfolio synchronously.
        5. Rejections do NOT mutate sleeve cash, portfolio cash, or ledger balances.
        """
        with self._lock:
            now = reference_time if reference_time is not None else time.time()
            order_id = f"sord_{uuid.uuid4().hex[:10]}"
            cid = request.correlation_id or uuid.uuid4().hex
            client_oid = (request.client_order_id or "").strip()

            # Idempotency & Conflict Check for Client Order ID
            if client_oid and client_oid in self._order_results:
                prev_sleeve_id, prev_req = self._order_requests[client_oid]
                prev_res = self._order_results[client_oid]

                # Verify payload match
                is_identical = (
                    prev_sleeve_id == sleeve_id.strip()
                    and prev_req.symbol.strip().upper() == request.symbol.strip().upper()
                    and prev_req.side == request.side
                    and prev_req.order_type == request.order_type
                    and abs(prev_req.quantity - request.quantity) < 1e-6
                    and prev_req.limit_price == request.limit_price
                )
                if is_identical:
                    logger.info("Returning idempotent cached execution result for client_order_id '%s'", client_oid)
                    return prev_res
                else:
                    rej_order = self._create_rejected_order(
                        order_id=order_id,
                        request=request,
                        now=now,
                        cid=cid,
                        code=PaperRejectionCode.DUPLICATE_ORDER,
                        reason=(
                            f"Client order ID '{client_oid}' has already been processed with conflicting parameters: "
                            f"previous (sleeve={prev_sleeve_id}, sym={prev_req.symbol}, side={prev_req.side.value}, qty={prev_req.quantity}) "
                            f"vs requested (sleeve={sleeve_id}, sym={request.symbol}, side={request.side.value}, qty={request.quantity})."
                        ),
                    )
                    self._record_rejection_ledger(rej_order, sleeve_id, cid)
                    return PaperExecutionResult(
                        success=False,
                        order=rej_order,
                        rejection_code=PaperRejectionCode.DUPLICATE_ORDER,
                        rejection_reason=rej_order.rejection_reason,
                    )

            # 1. Sleeve Existence Check
            sleeve = self._sleeves.get(sleeve_id.strip())
            if sleeve is None:
                rej_order = PaperOrder(
                    order_id=order_id,
                    client_order_id=request.client_order_id or order_id,
                    symbol=request.symbol,
                    side=request.side,
                    order_type=request.order_type,
                    quantity=request.quantity,
                    limit_price=request.limit_price,
                    status=PaperOrderStatus.REJECTED,
                    created_at=now,
                    updated_at=now,
                    rejection_code=PaperRejectionCode.ORDER_NOT_FOUND,
                    rejection_reason=f"Strategy sleeve '{sleeve_id}' does not exist or has not been allocated.",
                    correlation_id=cid,
                )
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(
                    success=False,
                    order=rej_order,
                    rejection_code=PaperRejectionCode.ORDER_NOT_FOUND,
                    rejection_reason=rej_order.rejection_reason,
                )

            # 2. Allowed Symbol Check for Sleeve
            norm_sym = request.symbol.strip().upper()
            if sleeve.allowed_symbols is not None and norm_sym not in sleeve.allowed_symbols:
                rej_order = PaperOrder(
                    order_id=order_id,
                    client_order_id=request.client_order_id or order_id,
                    symbol=norm_sym,
                    side=request.side,
                    order_type=request.order_type,
                    quantity=request.quantity,
                    limit_price=request.limit_price,
                    status=PaperOrderStatus.REJECTED,
                    created_at=now,
                    updated_at=now,
                    rejection_code=PaperRejectionCode.INVALID_SYMBOL,
                    rejection_reason=f"Symbol '{norm_sym}' is not authorized for sleeve '{sleeve_id}'. Authorized: {sorted(sleeve.allowed_symbols)}",
                    correlation_id=cid,
                )
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(
                    success=False,
                    order=rej_order,
                    rejection_code=PaperRejectionCode.INVALID_SYMBOL,
                    rejection_reason=rej_order.rejection_reason,
                )

            # 3. Market Quote Validation
            if quote is None:
                rej_order = self._create_rejected_order(order_id, request, now, cid, PaperRejectionCode.MISSING_MARKET_DATA, f"No quote for '{norm_sym}'")
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.MISSING_MARKET_DATA, rejection_reason=rej_order.rejection_reason)

            if quote.symbol.upper() != norm_sym:
                rej_order = self._create_rejected_order(order_id, request, now, cid, PaperRejectionCode.INVALID_MARKET_DATA, f"Quote symbol mismatch: {quote.symbol} vs {norm_sym}")
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.INVALID_MARKET_DATA, rejection_reason=rej_order.rejection_reason)

            if not math.isfinite(quote.last_price) or quote.last_price <= 0:
                rej_order = self._create_rejected_order(order_id, request, now, cid, PaperRejectionCode.INVALID_MARKET_DATA, f"Invalid quote price {quote.last_price}")
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.INVALID_MARKET_DATA, rejection_reason=rej_order.rejection_reason)

            # Spread inversion check
            if quote.bid is not None and quote.ask is not None and quote.bid > 0 and quote.ask > 0:
                if quote.ask < quote.bid:
                    rej_order = self._create_rejected_order(order_id, request, now, cid, PaperRejectionCode.INVALID_MARKET_DATA, f"Inverted spread: ask {quote.ask} < bid {quote.bid}")
                    self._record_rejection_ledger(rej_order, sleeve_id, cid)
                    return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.INVALID_MARKET_DATA, rejection_reason=rej_order.rejection_reason)

            # Quote freshness
            quote_age = max(0.0, now - quote.timestamp)
            if quote_age > sleeve.risk_budget.freshness_threshold_seconds:
                rej_order = self._create_rejected_order(order_id, request, now, cid, PaperRejectionCode.STALE_MARKET_DATA, f"Quote age {quote_age:.1f}s exceeds threshold {sleeve.risk_budget.freshness_threshold_seconds:.1f}s")
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.STALE_MARKET_DATA, rejection_reason=rej_order.rejection_reason)

            # Reference price
            if request.side == PaperOrderSide.BUY:
                est_price = quote.ask if (quote.ask is not None and quote.ask > 0) else quote.last_price
            else:
                est_price = quote.bid if (quote.bid is not None and quote.bid > 0) else quote.last_price

            order_notional = request.quantity * est_price

            # 4. Sleeve-Level Pre-Trade Risk Checks
            # 4.1 Order notional limit
            if order_notional > sleeve.risk_budget.max_order_notional:
                rej_order = self._create_rejected_order(
                    order_id, request, now, cid, PaperRejectionCode.EXCEEDS_MAX_NOTIONAL,
                    f"Order notional ({order_notional:.2f}) exceeds sleeve '{sleeve_id}' limit ({sleeve.risk_budget.max_order_notional:.2f})",
                )
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.EXCEEDS_MAX_NOTIONAL, rejection_reason=rej_order.rejection_reason)

            # 4.2 Cash Sufficiency (Sleeve Isolation)
            if request.side == PaperOrderSide.BUY:
                fee_est = round(order_notional * sleeve.risk_budget.fee_rate, 4)
                slip_est = round(order_notional * sleeve.risk_budget.slippage_rate, 4)
                total_cost_est = round(order_notional + fee_est + slip_est, 4)

                if sleeve.available_cash < total_cost_est:
                    rej_order = self._create_rejected_order(
                        order_id, request, now, cid, PaperRejectionCode.INSUFFICIENT_CASH,
                        f"Sleeve '{sleeve_id}' has insufficient cash: required {total_cost_est:.2f}, available {sleeve.available_cash:.2f}",
                    )
                    self._record_rejection_ledger(rej_order, sleeve_id, cid)
                    return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.INSUFFICIENT_CASH, rejection_reason=rej_order.rejection_reason)

                # 4.3 Sleeve Single-Instrument Position Exposure Check
                cur_pos = sleeve.get_position(norm_sym)
                cur_sleeve_pos_val = cur_pos.market_value if cur_pos else 0.0
                proj_sleeve_pos_val = cur_sleeve_pos_val + order_notional
                sleeve_equity = sleeve.total_equity

                if sleeve_equity > 0:
                    sleeve_exp_pct = (proj_sleeve_pos_val / sleeve_equity) * 100.0
                    if sleeve_exp_pct > sleeve.risk_budget.max_position_exposure_pct:
                        rej_order = self._create_rejected_order(
                            order_id, request, now, cid, PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE,
                            f"Projected exposure in sleeve '{sleeve_id}' for '{norm_sym}' ({sleeve_exp_pct:.1f}%) exceeds sleeve limit ({sleeve.risk_budget.max_position_exposure_pct:.1f}%)",
                        )
                        self._record_rejection_ledger(rej_order, sleeve_id, cid)
                        return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE, rejection_reason=rej_order.rejection_reason)

            # 4.4 Position Sufficiency for SELL (Long-only)
            elif request.side == PaperOrderSide.SELL:
                cur_pos = sleeve.get_position(norm_sym)
                held_qty = cur_pos.quantity if cur_pos else 0.0
                if held_qty <= 0 or held_qty < request.quantity:
                    rej_order = self._create_rejected_order(
                        order_id, request, now, cid, PaperRejectionCode.INSUFFICIENT_POSITION,
                        f"Cannot SELL {request.quantity} of '{norm_sym}' in sleeve '{sleeve_id}': held quantity is {held_qty}",
                    )
                    self._record_rejection_ledger(rej_order, sleeve_id, cid)
                    return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.INSUFFICIENT_POSITION, rejection_reason=rej_order.rejection_reason)

            # 5. Portfolio Aggregate Risk Checks (Cannot bypass via multiple sleeves!)
            # 5.1 Aggregate notional limit
            if order_notional > self.risk_config.max_order_notional:
                rej_order = self._create_rejected_order(
                    order_id, request, now, cid, PaperRejectionCode.EXCEEDS_MAX_NOTIONAL,
                    f"Order notional ({order_notional:.2f}) exceeds portfolio aggregate limit ({self.risk_config.max_order_notional:.2f})",
                )
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.EXCEEDS_MAX_NOTIONAL, rejection_reason=rej_order.rejection_reason)

            # 5.2 Aggregate position exposure limit across all sleeves
            if request.side == PaperOrderSide.BUY:
                agg_sym_val = sum(
                    (s.get_position(norm_sym).market_value if s.get_position(norm_sym) else 0.0)
                    for s in self._sleeves.values()
                )
                proj_agg_val = agg_sym_val + order_notional
                port_equity = self.portfolio.total_equity

                if port_equity > 0:
                    agg_exp_pct = (proj_agg_val / port_equity) * 100.0
                    if agg_exp_pct > self.risk_config.max_position_exposure_pct:
                        rej_order = self._create_rejected_order(
                            order_id, request, now, cid, PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE,
                            f"Projected portfolio aggregate exposure for '{norm_sym}' across all sleeves ({agg_exp_pct:.1f}%) exceeds portfolio limit ({self.risk_config.max_position_exposure_pct:.1f}%)",
                        )
                        self._record_rejection_ledger(rej_order, sleeve_id, cid)
                        return PaperExecutionResult(success=False, order=rej_order, rejection_code=PaperRejectionCode.EXCEEDS_MAX_POSITION_EXPOSURE, rejection_reason=rej_order.rejection_reason)

            # 6. Broker Execution
            client_oid = request.client_order_id or order_id
            pre_order = PaperOrder(
                order_id=order_id,
                client_order_id=client_oid,
                symbol=norm_sym,
                side=request.side,
                order_type=request.order_type,
                quantity=request.quantity,
                limit_price=request.limit_price,
                status=PaperOrderStatus.ACCEPTED,
                created_at=now,
                updated_at=now,
                correlation_id=cid,
            )

            is_filled, fill, fill_code, fill_reason = self.broker_adapter.execute_order(
                order=pre_order,
                quote=quote,
                reference_time=now,
            )

            if not is_filled or fill is None:
                rej_order = PaperOrder(
                    order_id=order_id,
                    client_order_id=client_oid,
                    symbol=norm_sym,
                    side=request.side,
                    order_type=request.order_type,
                    quantity=request.quantity,
                    limit_price=request.limit_price,
                    status=PaperOrderStatus.REJECTED,
                    created_at=now,
                    updated_at=now,
                    rejection_code=fill_code,
                    rejection_reason=fill_reason or "Broker execution rejected",
                    correlation_id=cid,
                )
                self._record_rejection_ledger(rej_order, sleeve_id, cid)
                return PaperExecutionResult(
                    success=False,
                    order=rej_order,
                    rejection_code=fill_code,
                    rejection_reason=fill_reason,
                )

            # 7. Apply Fill Atomically to Sleeve and Portfolio with Rollback Protection
            sleeve_snap = sleeve.snapshot()
            port_snap = _snapshot_portfolio(self.portfolio)

            try:
                sleeve.apply_fill(fill)
                self.portfolio.apply_fill(fill)

                # 8. Record in Durable Ledger
                fill_entry = PaperLedgerEntry(
                    entry_id=f"ledg_fill_{uuid.uuid4().hex[:8]}",
                    timestamp=now,
                    entry_type=LedgerEntryType.ORDER_FILL,
                    order_id=order_id,
                    client_order_id=client_oid or order_id,
                    symbol=norm_sym,
                    side=request.side.value,
                    quantity=fill.quantity,
                    price=fill.price,
                    fee=fill.fee,
                    cash_change=fill.net_cash_impact,
                    cash_balance_after=self.portfolio.available_cash,
                    realized_pnl=self.portfolio.realized_pnl,
                    idempotency_key=f"sleeve_fill_{fill.fill_id}",
                    correlation_id=cid,
                    metadata={
                        "sleeve_id": sleeve_id,
                        "fill_id": fill.fill_id,
                        "slippage": fill.slippage,
                        "order_type": request.order_type.value,
                        "sleeve_available_cash": sleeve.available_cash,
                        "sleeve_total_equity": sleeve.total_equity,
                    },
                )
                order_outbox_rec = OutboxRecord(
                    event_id=f"outbox_order_{order_id}_{int(now * 1000)}",
                    event_type=EventType.PAPER_SLEEVE_ORDER_EXECUTED.value,
                    schema_version=1,
                    correlation_id=cid,
                    causation_id=client_oid or order_id,
                    payload={
                        "order_id": order_id,
                        "client_order_id": client_oid,
                        "sleeve_id": sleeve_id.strip(),
                        "symbol": norm_sym,
                        "side": request.side.value if hasattr(request.side, "value") else str(request.side),
                        "quantity": fill.quantity,
                        "price": fill.price,
                        "fee": fill.fee,
                        "timestamp": now,
                    },
                    created_at=now,
                )

                if self.outbox and hasattr(self.ledger, "record_entry_with_outbox"):
                    self.ledger.record_entry_with_outbox(fill_entry, order_outbox_rec)
                else:
                    self.ledger.record_entry(fill_entry)
            except Exception as exc:
                sleeve.restore_snapshot(sleeve_snap)
                _restore_portfolio_snapshot(self.portfolio, port_snap)
                logger.error("Transactional rollback in execute_sleeve_order: %s", exc)
                raise

            filled_order = PaperOrder(
                order_id=order_id,
                client_order_id=client_oid,
                symbol=norm_sym,
                side=request.side,
                order_type=request.order_type,
                quantity=request.quantity,
                limit_price=request.limit_price,
                status=PaperOrderStatus.FILLED,
                created_at=now,
                updated_at=now,
                filled_at=fill.timestamp,
                fill_price=fill.price,
                filled_quantity=fill.quantity,
                fee=fill.fee,
                slippage=fill.slippage,
                correlation_id=cid,
            )

            # 9. Post-Commit Event Publication (Non-Transactional Notification)
            # Transaction Boundary Definition:
            # The financial transaction (portfolio, sleeve, durable ledger) is ALREADY COMMITTED.
            # Event delivery failure surfaces via `event_delivery_failed=True` and internal audit metrics,
            # but does NOT roll back committed financial ledger state.
            event_failed = False
            if self.event_bus:
                try:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.PAPER_SLEEVE_ORDER_EXECUTED,
                            source="portfolio_allocation_manager",
                            correlation_id=cid,
                            payload={
                                "sleeve_id": sleeve_id,
                                "order": filled_order.to_dict(),
                                "fill": fill.to_dict(),
                                "sleeve_equity": sleeve.total_equity,
                                "portfolio_equity": self.portfolio.total_equity,
                            },
                        )
                    )
                    if self.outbox:
                        self.outbox.mark_delivered(order_outbox_rec.event_id, delivered_at=now)
                except Exception as err:
                    event_failed = True
                    self._failed_event_deliveries += 1
                    self._event_delivery_failures.append({
                        "operation": "execute_sleeve_order",
                        "order_id": order_id,
                        "client_order_id": client_oid,
                        "error": str(err),
                        "timestamp": now,
                    })
                    if self.outbox:
                        self.outbox.record_failure(order_outbox_rec.event_id, error=str(err), next_retry_at=now + 2.0)
                    logger.warning(
                        "Post-commit event publication failed for sleeve order '%s': %s (financial state remains committed)",
                        order_id,
                        err,
                    )

            final_res = PaperExecutionResult(
                success=True,
                order=filled_order,
                fill=fill,
                event_delivery_failed=event_failed,
            )
            if client_oid:
                self._order_requests[client_oid] = (sleeve_id.strip(), request)
                self._order_results[client_oid] = final_res

            logger.info(
                "Sleeve order filled [sleeve_id=%s, symbol=%s, qty=%.2f, price=%.2f, net_cash=%.2f]",
                sleeve_id,
                norm_sym,
                fill.quantity,
                fill.price,
                fill.net_cash_impact,
            )
            return final_res

    def transfer_between_sleeves(
        self,
        source_sleeve_id: str,
        target_sleeve_id: str,
        amount: float,
        allow_transfer: bool = False,
        reason: Optional[str] = None,
    ) -> Tuple[bool, Optional[AllocationRejectionCode], Optional[str]]:
        """Transfer available capital between two strategy sleeves.

        By default, cross-sleeve transfers are strictly prohibited (allow_transfer=False).
        If explicitly authorized, transfers must be atomic, validated, and recorded in the audit trail.
        """
        with self._lock:
            if not allow_transfer:
                return (
                    False,
                    AllocationRejectionCode.UNAUTHORIZED_TRANSFER,
                    "Inter-sleeve capital transfers are prohibited by default.",
                )

            src_id = source_sleeve_id.strip()
            tgt_id = target_sleeve_id.strip()

            if src_id == tgt_id:
                return (
                    False,
                    AllocationRejectionCode.UNAUTHORIZED_TRANSFER,
                    "Source and target sleeves must be distinct.",
                )

            source = self._sleeves.get(src_id)
            if source is None:
                return (
                    False,
                    AllocationRejectionCode.SLEEVE_NOT_FOUND,
                    f"Source sleeve '{src_id}' does not exist.",
                )

            target = self._sleeves.get(tgt_id)
            if target is None:
                return (
                    False,
                    AllocationRejectionCode.SLEEVE_NOT_FOUND,
                    f"Target sleeve '{tgt_id}' does not exist.",
                )

            if not math.isfinite(amount) or amount <= 0:
                return (
                    False,
                    AllocationRejectionCode.INVALID_CAPITAL,
                    f"Transfer amount must be positive finite float, got {amount}.",
                )

            amt = round(float(amount), 4)
            if source.available_cash < amt:
                return (
                    False,
                    AllocationRejectionCode.INSUFFICIENT_SLEEVE_CASH,
                    f"Source sleeve '{src_id}' has insufficient cash: required {amt:.2f}, available {source.available_cash:.2f}.",
                )

            # Atomic transfer: cash leaves source and enters target with rollback boundary
            source.available_cash = round(source.available_cash - amt, 4)
            target.available_cash = round(target.available_cash + amt, 4)

            # Record in ledger (cash_change=0.0 because total portfolio cash is unchanged)
            now_ts = time.time()
            cid = f"xfer_{src_id}_{tgt_id}_{int(now_ts * 1000)}"
            xfer_entry = PaperLedgerEntry(
                entry_id=f"xfer_{uuid.uuid4().hex[:8]}",
                timestamp=now_ts,
                entry_type=LedgerEntryType.SLEEVE_TRANSFER,
                cash_change=0.0,
                cash_balance_after=self.portfolio.available_cash,
                idempotency_key=f"sleeve_xfer_{uuid.uuid4().hex}",
                correlation_id=cid,
                metadata={
                    "source_sleeve_id": src_id,
                    "target_sleeve_id": tgt_id,
                    "amount": amt,
                    "reason": reason or "Explicit authorized inter-sleeve transfer",
                    "source_balance_after": source.available_cash,
                    "target_balance_after": target.available_cash,
                },
            )
            xfer_outbox_rec = OutboxRecord(
                event_id=f"outbox_xfer_{src_id}_{tgt_id}_{int(now_ts * 1000)}",
                event_type=EventType.PAPER_SLEEVE_TRANSFER_RECORDED.value,
                schema_version=1,
                correlation_id=cid,
                causation_id=f"transfer_{src_id}_{tgt_id}_{int(now_ts * 1000)}",
                payload={
                    "source_sleeve_id": src_id,
                    "target_sleeve_id": tgt_id,
                    "amount": amt,
                    "reason": reason or "Explicit authorized inter-sleeve transfer",
                    "timestamp": now_ts,
                },
                created_at=now_ts,
            )
            try:
                if self.outbox and hasattr(self.ledger, "record_entry_with_outbox"):
                    self.ledger.record_entry_with_outbox(xfer_entry, xfer_outbox_rec)
                else:
                    self.ledger.record_entry(xfer_entry)
            except Exception as exc:
                source.available_cash = round(source.available_cash + amt, 4)
                target.available_cash = round(target.available_cash - amt, 4)
                logger.error("Transactional rollback in transfer_between_sleeves: %s", exc)
                raise

            if self.event_bus:
                try:
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.PAPER_SLEEVE_TRANSFER_RECORDED,
                            source="portfolio_allocation_manager",
                            payload={
                                "source_sleeve_id": src_id,
                                "target_sleeve_id": tgt_id,
                                "amount": amt,
                            },
                        )
                    )
                    if self.outbox:
                        self.outbox.mark_delivered(xfer_outbox_rec.event_id, delivered_at=now_ts)
                except Exception as err:
                    self._failed_event_deliveries += 1
                    self._event_delivery_failures.append({
                        "operation": "transfer_between_sleeves",
                        "source_sleeve_id": src_id,
                        "target_sleeve_id": tgt_id,
                        "amount": amt,
                        "error": str(err),
                        "timestamp": now_ts,
                    })
                    if self.outbox:
                        self.outbox.record_failure(xfer_outbox_rec.event_id, error=str(err), next_retry_at=now_ts + 2.0)
                    logger.warning(
                        "Post-commit event publication failed for transfer %s -> %s: %s (financial state remains committed)",
                        src_id,
                        tgt_id,
                        err,
                    )

            logger.info("Inter-sleeve transfer completed [%s -> %s: %.2f]", src_id, tgt_id, amt)
            return True, None, None

    def get_sleeve_exposure(self, sleeve_id: str) -> Optional[SleeveExposureSnapshot]:
        """Compute point-in-time exposure snapshot for a single sleeve."""
        with self._lock:
            sleeve = self._sleeves.get(sleeve_id.strip())
            if sleeve is None:
                return None

            return SleeveExposureSnapshot(
                sleeve_id=sleeve.sleeve_id,
                name=sleeve.name,
                allocated_capital=sleeve.allocated_capital,
                available_cash=sleeve.available_cash,
                reserved_cash=sleeve.reserved_cash,
                total_position_value=sleeve.total_position_value,
                total_equity=sleeve.total_equity,
                realized_pnl=round(sleeve.realized_pnl, 4),
                unrealized_pnl=sleeve.unrealized_pnl,
                total_fees_paid=round(sleeve.total_fees_paid, 4),
                positions={sym: pos.to_dict() for sym, pos in sleeve.positions.items()},
                utilization_pct=sleeve.utilization_pct,
                timestamp=time.time(),
            )

    def get_portfolio_exposure(self) -> PortfolioExposureSnapshot:
        """Consolidate exposures across all sleeves and compute portfolio reconciliation."""
        with self._lock:
            sleeve_snaps: Dict[str, SleeveExposureSnapshot] = {}
            for sid, s in self._sleeves.items():
                sleeve_snaps[sid] = SleeveExposureSnapshot(
                    sleeve_id=s.sleeve_id,
                    name=s.name,
                    allocated_capital=s.allocated_capital,
                    available_cash=s.available_cash,
                    reserved_cash=s.reserved_cash,
                    total_position_value=s.total_position_value,
                    total_equity=s.total_equity,
                    realized_pnl=round(s.realized_pnl, 4),
                    unrealized_pnl=s.unrealized_pnl,
                    total_fees_paid=round(s.total_fees_paid, 4),
                    positions={sym: pos.to_dict() for sym, pos in s.positions.items()},
                    utilization_pct=s.utilization_pct,
                    timestamp=time.time(),
                )

            # Aggregate symbol exposures
            all_symbols: Set[str] = set()
            for s in self._sleeves.values():
                all_symbols.update(s.positions.keys())
            all_symbols.update(self.portfolio.positions.keys())

            total_equity = self.portfolio.total_equity
            agg_symbols: Dict[str, Dict[str, Any]] = {}

            for sym in sorted(all_symbols):
                total_qty = 0.0
                total_val = 0.0
                sleeve_breakdown: Dict[str, float] = {}

                for sid, s in self._sleeves.items():
                    pos = s.get_position(sym)
                    if pos:
                        total_qty = round(total_qty + pos.quantity, 6)
                        total_val = round(total_val + pos.market_value, 4)
                        sleeve_breakdown[sid] = pos.quantity

                exp_pct = round((total_val / total_equity) * 100.0, 2) if total_equity > 0 else 0.0
                agg_symbols[sym] = {
                    "total_quantity": total_qty,
                    "market_value": total_val,
                    "exposure_pct": exp_pct,
                    "sleeve_quantities": sleeve_breakdown,
                }

            # Reconciliation check
            recon = self.reconcile()
            digest = compute_multi_sleeve_financial_digest(
                self.portfolio, self._sleeves, self._unallocated_cash
            )

            return PortfolioExposureSnapshot(
                total_portfolio_equity=self.portfolio.total_equity,
                total_allocated_capital=self.total_allocated_capital,
                unallocated_cash=self._unallocated_cash,
                total_position_value=self.portfolio.total_position_value,
                total_cash=self.portfolio.available_cash,
                realized_pnl=round(self.portfolio.realized_pnl, 4),
                unrealized_pnl=self.portfolio.unrealized_pnl,
                total_fees_paid=round(self.portfolio.total_fees_paid, 4),
                sleeves=sleeve_snaps,
                aggregate_symbol_exposures=agg_symbols,
                is_reconciled=recon["is_reconciled"],
                reconciliation_discrepancy=recon["max_discrepancy"],
                timestamp=time.time(),
                deterministic_digest=digest,
                event_delivery_failures=self._failed_event_deliveries,
            )

    def reconcile(self, tolerance: float = 1e-4) -> PortfolioSleeveReconciliationResult:
        """Perform comprehensive mathematical reconciliation between sleeve sums and portfolio."""
        with self._lock:
            discrepancies: List[str] = []
            checks: Dict[str, ReconciliationCheckDetail] = {}

            # 1. Cash Reconciliation
            sum_sleeve_cash = sum(s.available_cash for s in self._sleeves.values())
            expected_cash = round(sum_sleeve_cash + self._unallocated_cash, 4)
            actual_cash = round(self.portfolio.available_cash, 4)
            cash_diff = round(abs(expected_cash - actual_cash), 6)
            cash_ok = cash_diff <= tolerance
            if not cash_ok:
                discrepancies.append(
                    f"Cash mismatch: sleeves+unalloc={expected_cash:.4f} vs port={actual_cash:.4f} (diff={cash_diff:.6f})"
                )
            checks["cash_check"] = ReconciliationCheckDetail(
                dimension="cash",
                expected_value=expected_cash,
                actual_value=actual_cash,
                discrepancy=cash_diff,
                is_balanced=cash_ok,
                tolerance=tolerance,
                details={
                    "sum_sleeve_cash": round(sum_sleeve_cash, 4),
                    "unallocated_cash": self._unallocated_cash,
                    "portfolio_cash": actual_cash,
                },
            )

            # 2. Reserved Cash Reconciliation
            sum_sleeve_reserved = round(sum(s.reserved_cash for s in self._sleeves.values()), 4)
            actual_reserved = getattr(self.portfolio, "reserved_cash", 0.0)
            reserved_diff = round(abs(sum_sleeve_reserved - actual_reserved), 6)
            reserved_ok = reserved_diff <= tolerance
            if not reserved_ok:
                discrepancies.append(
                    f"Reserved cash mismatch: sum_sleeves={sum_sleeve_reserved:.4f} vs port={actual_reserved:.4f} (diff={reserved_diff:.6f})"
                )
            checks["reserved_cash_check"] = ReconciliationCheckDetail(
                dimension="reserved_cash",
                expected_value=sum_sleeve_reserved,
                actual_value=actual_reserved,
                discrepancy=reserved_diff,
                is_balanced=reserved_ok,
                tolerance=tolerance,
                details={"sum_sleeve_reserved": sum_sleeve_reserved},
            )

            # 3. Position Value Reconciliation
            sum_sleeve_pos_val = round(sum(s.total_position_value for s in self._sleeves.values()), 4)
            actual_pos_val = round(self.portfolio.total_position_value, 4)
            pos_val_diff = round(abs(sum_sleeve_pos_val - actual_pos_val), 6)
            pos_val_ok = pos_val_diff <= tolerance
            if not pos_val_ok:
                discrepancies.append(
                    f"Position value mismatch: sum_sleeves={sum_sleeve_pos_val:.4f} vs port={actual_pos_val:.4f} (diff={pos_val_diff:.6f})"
                )
            checks["position_value_check"] = ReconciliationCheckDetail(
                dimension="position_value",
                expected_value=sum_sleeve_pos_val,
                actual_value=actual_pos_val,
                discrepancy=pos_val_diff,
                is_balanced=pos_val_ok,
                tolerance=tolerance,
            )

            # 4. Position Quantity and Cost per Symbol
            all_symbols = set(self.portfolio.positions.keys())
            for s in self._sleeves.values():
                all_symbols.update(s.positions.keys())

            sym_discrepancies: Dict[str, Any] = {}
            max_pos_diff = 0.0
            pos_qty_ok = True
            for sym in sorted(all_symbols):
                sleeve_qty = round(sum((s.get_position(sym).quantity if s.get_position(sym) else 0.0) for s in self._sleeves.values()), 6)
                port_pos = self.portfolio.get_position(sym)
                port_qty = round(port_pos.quantity, 6) if port_pos else 0.0
                q_diff = round(abs(sleeve_qty - port_qty), 6)
                if q_diff > tolerance:
                    pos_qty_ok = False
                    discrepancies.append(f"Quantity mismatch for {sym}: sleeves={sleeve_qty:.6f} vs port={port_qty:.6f}")
                    sym_discrepancies[sym] = {"sleeve_qty": sleeve_qty, "port_qty": port_qty, "diff": q_diff}
                max_pos_diff = max(max_pos_diff, q_diff)

            checks["position_quantities_check"] = ReconciliationCheckDetail(
                dimension="position_quantities",
                expected_value=0.0,
                actual_value=max_pos_diff,
                discrepancy=max_pos_diff,
                is_balanced=pos_qty_ok,
                tolerance=tolerance,
                details=sym_discrepancies,
            )

            # 5. Total Fees Paid
            sum_fees = round(sum(s.total_fees_paid for s in self._sleeves.values()), 4)
            port_fees = round(self.portfolio.total_fees_paid, 4)
            fee_diff = round(abs(sum_fees - port_fees), 6)
            fee_ok = fee_diff <= tolerance
            if not fee_ok:
                discrepancies.append(f"Fee mismatch: sleeves={sum_fees:.4f} vs port={port_fees:.4f} (diff={fee_diff:.6f})")
            checks["fees_check"] = ReconciliationCheckDetail(
                dimension="fees",
                expected_value=sum_fees,
                actual_value=port_fees,
                discrepancy=fee_diff,
                is_balanced=fee_ok,
                tolerance=tolerance,
            )

            # 6. Realized P&L
            sum_realized = round(sum(s.realized_pnl for s in self._sleeves.values()), 4)
            port_realized = round(self.portfolio.realized_pnl, 4)
            pnl_diff = round(abs(sum_realized - port_realized), 6)
            pnl_ok = pnl_diff <= tolerance
            if not pnl_ok:
                discrepancies.append(f"Realized P&L mismatch: sleeves={sum_realized:.4f} vs port={port_realized:.4f} (diff={pnl_diff:.6f})")
            checks["realized_pnl_check"] = ReconciliationCheckDetail(
                dimension="realized_pnl",
                expected_value=sum_realized,
                actual_value=port_realized,
                discrepancy=pnl_diff,
                is_balanced=pnl_ok,
                tolerance=tolerance,
            )

            # 7. Unrealized P&L
            sum_unrealized = round(sum(s.unrealized_pnl for s in self._sleeves.values()), 4)
            port_unrealized = round(self.portfolio.unrealized_pnl, 4)
            unrealized_diff = round(abs(sum_unrealized - port_unrealized), 6)
            unrealized_ok = unrealized_diff <= tolerance
            if not unrealized_ok:
                discrepancies.append(f"Unrealized P&L mismatch: sleeves={sum_unrealized:.4f} vs port={port_unrealized:.4f} (diff={unrealized_diff:.6f})")
            checks["unrealized_pnl_check"] = ReconciliationCheckDetail(
                dimension="unrealized_pnl",
                expected_value=sum_unrealized,
                actual_value=port_unrealized,
                discrepancy=unrealized_diff,
                is_balanced=unrealized_ok,
                tolerance=tolerance,
            )

            # 8. Total Equity / Capital Reconciliation
            sum_sleeve_equity = round(sum(s.total_equity for s in self._sleeves.values()), 4)
            expected_equity = round(sum_sleeve_equity + self._unallocated_cash, 4)
            actual_equity = round(self.portfolio.total_equity, 4)
            equity_diff = round(abs(expected_equity - actual_equity), 6)
            equity_ok = equity_diff <= tolerance
            if not equity_ok:
                discrepancies.append(f"Total equity mismatch: sleeves+unalloc={expected_equity:.4f} vs port={actual_equity:.4f} (diff={equity_diff:.6f})")
            checks["total_equity_check"] = ReconciliationCheckDetail(
                dimension="total_equity",
                expected_value=expected_equity,
                actual_value=actual_equity,
                discrepancy=equity_diff,
                is_balanced=equity_ok,
                tolerance=tolerance,
            )

            max_disc = max(cash_diff, reserved_diff, pos_val_diff, max_pos_diff, fee_diff, pnl_diff, unrealized_diff, equity_diff)
            is_ok = len(discrepancies) == 0

            # Compute digest
            digest = compute_multi_sleeve_financial_digest(
                self.portfolio, self._sleeves, self._unallocated_cash
            )

            return PortfolioSleeveReconciliationResult(
                is_reconciled=is_ok,
                max_discrepancy=max_disc,
                tolerance=tolerance,
                discrepancies=discrepancies,
                checks=checks,
                portfolio_cash=actual_cash,
                sum_sleeve_cash=round(sum_sleeve_cash, 4),
                unallocated_cash=self._unallocated_cash,
                portfolio_equity=actual_equity,
                sum_sleeve_equity=sum_sleeve_equity,
                timestamp=time.time(),
                deterministic_digest=digest,
            )

    def recover_from_ledger(self, ledger: PaperLedger) -> Dict[str, Any]:
        """Reconstruct sleeve partitions, cash allocations, and trades strictly from the ledger.

        Guarantees:
        1. Idempotency: replaying multiple times produces identical state.
        2. Detects corrupted, non-finite, unknown sleeve, or conflicting records and fails closed.
        3. Fills are mapped to their respective strategy sleeves via entry metadata.
        4. Atomic rollback on recovery failure.
        """
        with self._lock:
            entries = ledger.get_entries(limit=1_000_000)
            if not entries:
                return {"recovered_sleeves": 0, "status": "no_entries"}

            # Sort entries chronologically to guarantee correct replay order
            entries = sorted(entries, key=lambda e: (e.timestamp, e.entry_id))

            # Backup state for atomicity
            sleeves_backup = {sid: copy.copy(s) for sid, s in self._sleeves.items()}
            unalloc_backup = self._unallocated_cash

            try:
                # Clear in-memory sleeves cleanly
                self._sleeves.clear()
                total_allocated = 0.0
                seen_alloc_keys: Dict[str, float] = {}  # sleeve_id -> allocated_cash

                # 1. First pass: Reconstruct sleeves from CAPITAL_ALLOCATION entries
                for entry in entries:
                    # Validate non-finite numbers in entry
                    for val, label in (
                        (entry.cash_change, "cash_change"),
                        (entry.cash_balance_after, "cash_balance_after"),
                        (entry.quantity, "quantity"),
                        (entry.price, "price"),
                        (entry.fee, "fee"),
                    ):
                        if val is not None and (not math.isfinite(val)):
                            raise ValidationError(f"Corrupted ledger entry {entry.entry_id}: non-finite {label} ({val})")

                    if entry.entry_type == LedgerEntryType.CAPITAL_ALLOCATION:
                        meta = entry.metadata
                        sid = meta.get("sleeve_id")
                        if not sid:
                            raise ValidationError(f"Corrupted CAPITAL_ALLOCATION entry {entry.entry_id}: missing sleeve_id")
                        alloc_cash = meta.get("allocated_cash", 0.0)
                        if not math.isfinite(alloc_cash) or alloc_cash <= 0:
                            raise ValidationError(f"Corrupted CAPITAL_ALLOCATION entry {entry.entry_id}: non-finite or negative cash")

                        # Check for conflicting duplicate allocation records
                        if sid in seen_alloc_keys:
                            prev_alloc = seen_alloc_keys[sid]
                            if abs(prev_alloc - alloc_cash) > 1e-4:
                                raise ValidationError(
                                    f"Conflicting duplicate CAPITAL_ALLOCATION entry for sleeve '{sid}': "
                                    f"previous {prev_alloc:.2f} vs new {alloc_cash:.2f}"
                                )
                            continue  # Idempotently skip identical duplicate

                        seen_alloc_keys[sid] = alloc_cash
                        name = meta.get("name", sid)
                        self._sleeves[sid] = StrategySleeve(
                            sleeve_id=sid,
                            name=name,
                            allocated_capital=alloc_cash,
                            risk_budget=self.risk_config and SleeveRiskBudget(
                                max_order_notional=self.risk_config.max_order_notional,
                                max_position_exposure_pct=self.risk_config.max_position_exposure_pct,
                            ) or SleeveRiskBudget(),
                        )
                        total_allocated = round(total_allocated + alloc_cash, 4)

                # 2. Second pass: Replay fills and transfers
                seen_fill_ids: Set[str] = set()

                for entry in entries:
                    if entry.entry_type == LedgerEntryType.ORDER_FILL:
                        sid = entry.metadata.get("sleeve_id")
                        fill_id = entry.metadata.get("fill_id", f"fill_{entry.order_id}")

                        # Skip duplicate fill records
                        if fill_id in seen_fill_ids:
                            continue
                        seen_fill_ids.add(fill_id)

                        if sid:
                            if sid not in self._sleeves:
                                raise ValidationError(
                                    f"Corrupted ledger history: fill entry '{entry.entry_id}' "
                                    f"references unknown strategy sleeve '{sid}'"
                                )
                            sleeve = self._sleeves[sid]
                            fill = PaperFill(
                                fill_id=fill_id,
                                order_id=entry.order_id or "unknown",
                                client_order_id=entry.client_order_id or "unknown",
                                symbol=entry.symbol or "UNKNOWN",
                                side=PaperOrderSide(entry.side) if entry.side else PaperOrderSide.BUY,
                                quantity=entry.quantity or 0.0,
                                price=entry.price or 0.0,
                                fee=entry.fee,
                                slippage=entry.metadata.get("slippage", 0.0),
                                timestamp=entry.timestamp,
                                market_data_timestamp=entry.timestamp,
                                market_data_source="replay",
                            )
                            sleeve.apply_fill(fill)
                        # Note: if sid is None, it is a legacy entry predating Step 62 (safe to skip for sleeves)

                    elif entry.entry_type == LedgerEntryType.SLEEVE_TRANSFER:
                        meta = entry.metadata
                        src_id = meta.get("source_sleeve_id")
                        tgt_id = meta.get("target_sleeve_id")
                        amt = meta.get("amount", 0.0)
                        if src_id in self._sleeves and tgt_id in self._sleeves and amt > 0:
                            self._sleeves[src_id].available_cash = round(self._sleeves[src_id].available_cash - amt, 4)
                            self._sleeves[tgt_id].available_cash = round(self._sleeves[tgt_id].available_cash + amt, 4)

                self._unallocated_cash = round(self.portfolio.available_cash - sum(s.available_cash for s in self._sleeves.values()), 4)

            except Exception as exc:
                # Rollback to pre-recovery state on corruption or interruption
                self._sleeves = sleeves_backup
                self._unallocated_cash = unalloc_backup
                logger.error("Transactional rollback during recover_from_ledger: %s", exc)
                raise

            logger.info("PortfolioAllocationManager recovered from ledger: %d sleeves", len(self._sleeves))
            recon = self.reconcile()
            return {
                "recovered_sleeves": len(self._sleeves),
                "total_allocated": total_allocated,
                "unallocated_cash": self._unallocated_cash,
                "reconciled": recon["is_reconciled"],
            }

    def mark_to_market(self, quote: Quote) -> None:
        """Propagate quote updates to all strategy sleeves and portfolio."""
        with self._lock:
            self.portfolio.mark_to_market(quote)
            for sleeve in self._sleeves.values():
                sleeve.mark_to_market(quote)

    def _create_rejected_order(
        self,
        order_id: str,
        request: PaperOrderRequest,
        now: float,
        cid: str,
        code: PaperRejectionCode,
        reason: str,
    ) -> PaperOrder:
        return PaperOrder(
            order_id=order_id,
            client_order_id=request.client_order_id or order_id,
            symbol=request.symbol,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            limit_price=request.limit_price,
            status=PaperOrderStatus.REJECTED,
            created_at=now,
            updated_at=now,
            rejection_code=code,
            rejection_reason=reason,
            correlation_id=cid,
        )

    def _init_snapshot_db(self) -> None:
        """Initialize SQLite table for equity snapshot persistence."""
        db_path = getattr(self.ledger, "db_path", None)
        if not db_path:
            return
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            with sqlite3.connect(db_path, timeout=10.0) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS paper_equity_snapshots (
                        snapshot_id TEXT PRIMARY KEY,
                        timestamp REAL NOT NULL,
                        currency TEXT NOT NULL,
                        total_equity REAL NOT NULL,
                        available_cash REAL NOT NULL,
                        reserved_cash REAL NOT NULL,
                        unallocated_cash REAL NOT NULL,
                        allocated_capital REAL NOT NULL,
                        total_position_value REAL NOT NULL,
                        realized_pnl REAL NOT NULL,
                        unrealized_pnl REAL NOT NULL,
                        total_fees_paid REAL NOT NULL,
                        is_reconciled INTEGER NOT NULL,
                        reconciliation_discrepancy REAL NOT NULL,
                        deterministic_digest TEXT NOT NULL,
                        snapshot_json TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_paper_equity_snap_time ON paper_equity_snapshots(timestamp)"
                )
        except Exception as err:
            logger.error("Failed to initialize paper equity snapshots SQLite table: %s", err)

    def recover_snapshots(self) -> int:
        """Recover persisted equity snapshots from SQLite."""
        db_path = getattr(self.ledger, "db_path", None)
        if not db_path or not Path(db_path).is_file():
            return 0
        with self._lock:
            recovered = 0
            try:
                with sqlite3.connect(db_path, timeout=5.0) as conn:
                    cursor = conn.execute(
                        """
                        SELECT snapshot_id, snapshot_json FROM paper_equity_snapshots
                        ORDER BY timestamp ASC
                        """
                    )
                    for sid, s_json in cursor.fetchall():
                        try:
                            data = json.loads(s_json)
                            snap = PortfolioEquitySnapshot(
                                snapshot_id=data["snapshot_id"],
                                timestamp=data["timestamp"],
                                currency=data.get("currency", "USD"),
                                total_equity=data["total_equity"],
                                available_cash=data["available_cash"],
                                reserved_cash=data.get("reserved_cash", 0.0),
                                unallocated_cash=data.get("unallocated_cash", 0.0),
                                allocated_capital=data.get("allocated_capital", 0.0),
                                total_position_value=data.get("total_position_value", 0.0),
                                realized_pnl=data.get("realized_pnl", 0.0),
                                unrealized_pnl=data.get("unrealized_pnl", 0.0),
                                total_fees_paid=data.get("total_fees_paid", 0.0),
                                sleeve_equities=data.get("sleeve_equities", {}),
                                positions_summary=data.get("positions_summary", {}),
                                schema_version=data.get("schema_version", 1),
                                correlation_id=data.get("correlation_id"),
                                source=data.get("source", "recovery"),
                                is_reconciled=bool(data.get("is_reconciled", True)),
                                reconciliation_discrepancy=data.get("reconciliation_discrepancy", 0.0),
                                deterministic_digest=data.get("deterministic_digest", ""),
                            )
                            self._snapshots[sid] = snap
                            recovered += 1
                        except Exception as e:
                            logger.warning("Skipping corrupted equity snapshot %s during recovery: %s", sid, e)
            except Exception as err:
                logger.error("Failed to recover equity snapshots from SQLite: %s", err)
            return recovered

    def create_equity_snapshot(
        self,
        snapshot_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        source: str = "manual",
        reference_time: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> PortfolioEquitySnapshot:
        """Capture and durably record an authoritative point-in-time equity snapshot."""
        if reference_time is not None:
            if not math.isfinite(reference_time) or reference_time <= 0:
                raise ValidationError(f"reference_time must be positive finite float, got {reference_time}")
        now = reference_time if reference_time is not None else time.time()

        with self._lock:
            # Idempotency check: duplicate snapshot_id returns existing record without duplicating events
            if snapshot_id is not None:
                clean_id = snapshot_id.strip()
                if clean_id in self._snapshots:
                    return self._snapshots[clean_id]
                target_id = clean_id
            else:
                target_id = f"peq_{uuid.uuid4().hex[:12]}"

            sleeve_eqs = {sid: round(s.total_equity, 4) for sid, s in self._sleeves.items()}
            sum_reserved = round(sum(s.reserved_cash for s in self._sleeves.values()), 4)
            pos_summary = {
                sym: {
                    "quantity": pos.quantity,
                    "market_value": round(pos.market_value, 4),
                    "current_price": round(pos.current_price, 4),
                    "unrealized_pnl": round(pos.unrealized_pnl, 4),
                }
                for sym, pos in self.portfolio.positions.items()
            }
            recon = self.reconcile()
            digest = compute_multi_sleeve_financial_digest(
                self.portfolio, self._sleeves, self._unallocated_cash
            )

            snap = PortfolioEquitySnapshot(
                snapshot_id=target_id,
                timestamp=now,
                currency="USD",
                total_equity=round(self.portfolio.total_equity, 4),
                available_cash=round(self.portfolio.available_cash, 4),
                reserved_cash=sum_reserved,
                unallocated_cash=round(self._unallocated_cash, 4),
                allocated_capital=round(self.total_allocated_capital, 4),
                total_position_value=round(self.portfolio.total_position_value, 4),
                realized_pnl=round(self.portfolio.realized_pnl, 4),
                unrealized_pnl=round(self.portfolio.unrealized_pnl, 4),
                total_fees_paid=round(self.portfolio.total_fees_paid, 4),
                sleeve_equities=sleeve_eqs,
                positions_summary=pos_summary,
                schema_version=1,
                correlation_id=correlation_id or target_id,
                source=source,
                is_reconciled=recon["is_reconciled"],
                reconciliation_discrepancy=recon["max_discrepancy"],
                deterministic_digest=digest,
            )

            outbox_payload = snap.to_dict()
            if reason:
                outbox_payload["reason"] = reason

            outbox_rec = OutboxRecord(
                event_id=snap.snapshot_id,
                event_type=EventType.PAPER_PORTFOLIO_EQUITY_SNAPSHOT.value,
                schema_version=1,
                correlation_id=correlation_id or snap.snapshot_id,
                causation_id=snap.snapshot_id,
                payload=outbox_payload,
                created_at=now,
            )

            db_path = getattr(self.ledger, "db_path", None)
            if db_path:
                try:
                    with sqlite3.connect(db_path, timeout=10.0) as conn:
                        with conn:
                            conn.execute(
                                """
                                INSERT OR REPLACE INTO paper_equity_snapshots (
                                    snapshot_id, timestamp, currency, total_equity, available_cash,
                                    reserved_cash, unallocated_cash, allocated_capital, total_position_value,
                                    realized_pnl, unrealized_pnl, total_fees_paid, is_reconciled,
                                    reconciliation_discrepancy, deterministic_digest, snapshot_json
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    snap.snapshot_id,
                                    snap.timestamp,
                                    snap.currency,
                                    snap.total_equity,
                                    snap.available_cash,
                                    snap.reserved_cash,
                                    snap.unallocated_cash,
                                    snap.allocated_capital,
                                    snap.total_position_value,
                                    snap.realized_pnl,
                                    snap.unrealized_pnl,
                                    snap.total_fees_paid,
                                    1 if snap.is_reconciled else 0,
                                    snap.reconciliation_discrepancy,
                                    snap.deterministic_digest,
                                    json.dumps(snap.to_dict()),
                                ),
                            )
                            if self.outbox:
                                self.outbox.stage_record(outbox_rec, conn=conn)
                except Exception as err:
                    logger.error("Failed to commit equity snapshot to SQLite: %s", err)
                    raise
            elif self.outbox:
                self.outbox.stage_record(outbox_rec)

            self._snapshots[snap.snapshot_id] = snap

            # Post-commit publication
            if self.event_bus:
                try:
                    self.event_bus.publish(outbox_rec.to_event())
                    if self.outbox:
                        self.outbox.mark_delivered(outbox_rec.event_id, delivered_at=time.time())
                except Exception as exc:
                    self._failed_event_deliveries += 1
                    if self.outbox:
                        self.outbox.record_failure(outbox_rec.event_id, error=str(exc), next_retry_at=time.time() + 2.0)
                    logger.warning(
                        "Post-commit equity snapshot publication failed for %s: %s (snapshot remains committed)",
                        snap.snapshot_id,
                        exc,
                    )

            logger.info("Created portfolio equity snapshot %s (equity=%.2f)", snap.snapshot_id, snap.total_equity)
            return snap

    def get_equity_snapshot(self, snapshot_id: str) -> Optional[PortfolioEquitySnapshot]:
        """Retrieve equity snapshot by ID."""
        with self._lock:
            return self._snapshots.get(snapshot_id.strip())

    def list_equity_snapshots(self, limit: int = 50, offset: int = 0) -> List[PortfolioEquitySnapshot]:
        """List tracked equity snapshots with pagination."""
        with self._lock:
            sorted_snaps = sorted(self._snapshots.values(), key=lambda s: s.timestamp, reverse=True)
            clamped_limit = max(1, min(100, limit))
            clamped_offset = max(0, offset)
            return sorted_snaps[clamped_offset : clamped_offset + clamped_limit]

    def _record_rejection_ledger(self, order: PaperOrder, sleeve_id: str, cid: str) -> None:
        """Record an immutable rejection entry in the ledger."""
        rej_entry = PaperLedgerEntry(
            entry_id=f"ledg_rej_{uuid.uuid4().hex[:8]}",
            timestamp=order.updated_at,
            entry_type=LedgerEntryType.ORDER_REJECTION,
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side.value if order.side else None,
            quantity=order.quantity,
            price=order.limit_price,
            fee=0.0,
            cash_change=0.0,
            cash_balance_after=self.portfolio.available_cash,
            realized_pnl=self.portfolio.realized_pnl,
            idempotency_key=f"rej_{order.order_id}",
            correlation_id=cid,
            metadata={
                "sleeve_id": sleeve_id,
                "rejection_code": order.rejection_code.value if order.rejection_code else None,
                "rejection_reason": order.rejection_reason,
            },
        )
        self.ledger.record_entry(rej_entry)


def create_standard_multi_sleeve_request(
    total_capital: float = 100_000.0,
    request_id: str = "alloc_std_multi_asset",
) -> PortfolioAllocationRequest:
    """Deterministic fixture creating a 3-sleeve multi-asset allocation specification."""
    s1 = StrategySleeveDefinition(
        sleeve_id="alpha_momentum",
        name="Large-Cap Momentum Equity",
        target_weight=0.50,  # 50,000
        allowed_symbols=["RELIANCE", "TCS", "INFY"],
        risk_budget=SleeveRiskBudget(
            max_order_notional=25_000.0,
            max_position_exposure_pct=40.0,
        ),
    )
    s2 = StrategySleeveDefinition(
        sleeve_id="beta_mean_reversion",
        name="Banking Mean Reversion",
        target_weight=0.30,  # 30,000
        allowed_symbols=["HDFCBANK", "ICICIBANK", "SBIN"],
        risk_budget=SleeveRiskBudget(
            max_order_notional=15_000.0,
            max_position_exposure_pct=50.0,
        ),
    )
    s3 = StrategySleeveDefinition(
        sleeve_id="gamma_etf_core",
        name="Core Index ETF Sleeve",
        target_weight=0.20,  # 20,000
        allowed_symbols=["NIFTYBEES", "BANKBEES"],
        risk_budget=SleeveRiskBudget(
            max_order_notional=10_000.0,
            max_position_exposure_pct=60.0,
        ),
    )
    return PortfolioAllocationRequest(
        request_id=request_id,
        total_capital=total_capital,
        sleeves=[s1, s2, s3],
        metadata={"description": "Standard 3-sleeve multi-asset paper trading fixture"},
    )
