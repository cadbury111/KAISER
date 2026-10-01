"""KAISER — Market Data Quality Policies & Rules.

Implements:
1. Field completeness checks for Quote, Bar, and OrderBookSnapshot.
2. Deterministic quality precedence resolution:
   INVALID > MISSING > OUT_OF_ORDER > DUPLICATE > STALE > DEGRADED > VALID
3. Downstream safety consumption policy.
4. Centralized Quality Engine configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union

from app.market.models import Bar, OrderBookSnapshot, Quote
from app.market.quality.status import DataQualityStatus
from app.market.validators import DataFreshnessStatus


@dataclass(frozen=True)
class MarketQualityConfig:
    """Configurable parameters for the Market Data Quality Engine."""

    freshness_threshold_seconds: float = 60.0
    missing_data_timeout_seconds: float = 120.0
    duplicate_detection_enabled: bool = True
    duplicate_window_size: int = 1000
    out_of_order_detection_enabled: bool = True
    source_health_enabled: bool = True
    quarantine_out_of_order: bool = True


def evaluate_quote_completeness(quote: Quote) -> Tuple[str, List[str]]:
    """Assess field completeness for a Quote model.

    Optional fields: bid, ask, bid_size, ask_size.
    Missing optional fields degrade the record with descriptive warnings
    rather than invalidating it.
    """
    warnings: List[str] = []
    missing_fields: List[str] = []

    if quote.bid is None:
        missing_fields.append("bid")
    if quote.ask is None:
        missing_fields.append("ask")
    if quote.bid_size is None:
        missing_fields.append("bid_size")
    if quote.ask_size is None:
        missing_fields.append("ask_size")

    if missing_fields:
        warnings.append(f"Missing optional quote fields: {', '.join(missing_fields)}")
        return "PARTIAL", warnings

    return "COMPLETE", warnings


def evaluate_bar_completeness(bar: Bar) -> Tuple[str, List[str]]:
    """Assess field completeness for a Bar model."""
    warnings: List[str] = []
    # All Bar fields (open, high, low, close, volume) are structurally required
    if bar.volume == 0:
        warnings.append("Bar reported zero volume")
        return "PARTIAL", warnings
    return "COMPLETE", warnings


def evaluate_order_book_completeness(ob: OrderBookSnapshot) -> Tuple[str, List[str]]:
    """Assess field completeness for an OrderBookSnapshot."""
    warnings: List[str] = []
    if not ob.bids:
        warnings.append("Order book has empty bids side")
    if not ob.asks:
        warnings.append("Order book has empty asks side")

    if warnings:
        return "PARTIAL", warnings
    return "COMPLETE", warnings


def resolve_quality_status(
    has_errors: bool,
    freshness: DataFreshnessStatus,
    is_missing: bool = False,
    is_out_of_order: bool = False,
    is_duplicate: bool = False,
    is_degraded: bool = False,
) -> DataQualityStatus:
    """Resolve overall DataQualityStatus according to strict precedence rules.

    DETERMINISTIC PRECEDENCE:
    INVALID > MISSING > OUT_OF_ORDER > DUPLICATE > STALE > DEGRADED > VALID
    """
    if has_errors or freshness == DataFreshnessStatus.INVALID:
        return DataQualityStatus.INVALID

    if is_missing:
        return DataQualityStatus.MISSING

    if is_out_of_order:
        return DataQualityStatus.OUT_OF_ORDER

    if is_duplicate:
        return DataQualityStatus.DUPLICATE

    if freshness == DataFreshnessStatus.STALE:
        return DataQualityStatus.STALE

    if is_degraded:
        return DataQualityStatus.DEGRADED

    return DataQualityStatus.VALID


def is_safe_for_downstream(status: DataQualityStatus) -> bool:
    """Downstream safety policy determining if an assessment is safe to consume.

    Rules:
    - VALID: Safe to consume.
    - DEGRADED: Safe to consume (with warnings noted).
    - STALE: Unsafe (must not be treated as current market data).
    - INVALID: Unsafe (rejected).
    - MISSING: Unsafe (rejected).
    - DUPLICATE: Unsafe (ignored).
    - OUT_OF_ORDER: Unsafe (quarantined/rejected).
    """
    return status in (DataQualityStatus.VALID, DataQualityStatus.DEGRADED)
