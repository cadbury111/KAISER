"""KAISER — Paper Trading Ledger Domain Models.

Provides strongly-typed, immutable data structures for double-entry and audit-trailed
paper trade accounting, idempotency protection, and cash reconciliation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import math
import time
from typing import Any, Dict, Optional
import uuid

from app.core.exceptions import ValidationError


class LedgerEntryType(str, Enum):
    """Categorical classification of paper ledger entries."""

    INITIAL_CAPITAL = "INITIAL_CAPITAL"
    ORDER_SUBMISSION = "ORDER_SUBMISSION"
    ORDER_ACCEPTANCE = "ORDER_ACCEPTANCE"
    ORDER_REJECTION = "ORDER_REJECTION"
    ORDER_FILL = "ORDER_FILL"
    ORDER_CANCELLATION = "ORDER_CANCELLATION"


@dataclass(frozen=True)
class PaperLedgerEntry:
    """Immutable audit-trail record in the paper trading ledger."""

    entry_id: str
    timestamp: float
    entry_type: LedgerEntryType
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    symbol: Optional[str] = None
    side: Optional[str] = None
    quantity: Optional[float] = None
    price: Optional[float] = None
    fee: float = 0.0
    cash_change: float = 0.0
    cash_balance_after: float = 0.0
    realized_pnl: float = 0.0
    idempotency_key: str = field(default_factory=lambda: uuid.uuid4().hex)
    provenance_id: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.entry_id:
            raise ValidationError("PaperLedgerEntry entry_id is required.")
        if not self.idempotency_key:
            raise ValidationError("PaperLedgerEntry idempotency_key is required.")
        if not math.isfinite(self.cash_change):
            raise ValidationError(f"cash_change must be a finite float, got {self.cash_change}")
        if not math.isfinite(self.cash_balance_after):
            raise ValidationError(f"cash_balance_after must be a finite float, got {self.cash_balance_after}")
        if not math.isfinite(self.fee) or self.fee < 0:
            raise ValidationError(f"fee must be a non-negative finite float, got {self.fee}")
        if not math.isfinite(self.realized_pnl):
            raise ValidationError(f"realized_pnl must be a finite float, got {self.realized_pnl}")

        # Safety Invariant: Ledger must never persist credentials or authentication secrets
        forbidden_keys = {
            "api_key", "secret", "password", "token", "access_token",
            "private_key", "secret_key", "credential", "credentials", "auth_token",
        }
        for k in self.metadata:
            norm_k = str(k).lower().strip()
            if any(bad in norm_k for bad in forbidden_keys):
                raise ValidationError(f"PaperLedgerEntry metadata must not contain credential or secret keys: '{k}'")

    def to_dict(self) -> Dict[str, Any]:
        """Convert ledger entry to dictionary."""
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp,
            "entry_type": self.entry_type.value,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "fee": self.fee,
            "cash_change": self.cash_change,
            "cash_balance_after": self.cash_balance_after,
            "realized_pnl": self.realized_pnl,
            "idempotency_key": self.idempotency_key,
            "provenance_id": self.provenance_id,
            "correlation_id": self.correlation_id,
            "metadata": dict(self.metadata),
        }
