"""KAISER — Market Data Quality Policies & Rules.

Implements:
1. Field completeness checks for Quote, Bar, and OrderBookSnapshot.
2. Deterministic quality precedence resolution:
   INVALID > MISSING > OUT_OF_ORDER > DUPLICATE > STALE > DEGRADED > VALID
3. Downstream safety consumption policy.
4. Deterministic, bounded numerical quality score calculation.
5. Centralized Quality Engine & Anomaly Detection configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, List, Optional, Sequence, Tuple, Union

from app.market.models import Bar, OrderBookSnapshot, Quote
from app.market.quality.models import MarketAnomaly, QualityFinding, QualitySeverity
from app.market.quality.status import DataQualityStatus
from app.market.validators import DataFreshnessStatus


@dataclass(frozen=True)
class MarketQualityConfig:
    """Configurable parameters for the Market Data Quality Engine and Anomaly Detector."""

    freshness_threshold_seconds: float = 60.0
    stale_threshold_seconds: float = 60.0
    missing_data_timeout_seconds: float = 120.0
    duplicate_detection_enabled: bool = True
    duplicate_window_size: int = 1000
    out_of_order_detection_enabled: bool = True
    source_health_enabled: bool = True
    quarantine_out_of_order: bool = True

    # Rule-Based Anomaly Detection Thresholds (Step 19)
    price_jump_threshold_pct: float = 0.15          # 15% jump between adjacent bars triggers anomaly
    volume_spike_multiplier: float = 5.0           # 5x volume spike over rolling/baseline average
    large_range_threshold_pct: float = 0.20         # Bar range exceeding 20% of open price
    repeated_identical_bars_threshold: int = 3      # Consecutive identical bars threshold
    max_allowed_gap_seconds: float = 3600.0         # 1 hour maximum interval gap before flagging ERROR
    excessive_gap_multiplier: float = 5.0          # Gap > 5x timeframe interval
    allow_zero_volume: bool = True                  # Whether zero volume is allowed (flagged as INFO)
    max_stored_anomalies: int = 200                 # Bounded in-memory anomaly history limit
    max_findings_per_assessment: int = 100          # Bounded findings retained per assessment


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


def calculate_quality_score(
    total_records: int,
    valid_records: int,
    invalid_records: int,
    duplicate_count: int,
    conflicting_duplicate_count: int,
    out_of_order_count: int,
    gap_count: int,
    is_stale: bool,
    anomalies: Sequence[MarketAnomaly] = (),
    status: Optional[DataQualityStatus] = None,
) -> float:
    """Calculate a deterministic, bounded numerical quality score in [0.0, 100.0].

    Guarantees:
    - Deterministic and explainable.
    - Missing information is NOT converted into fake 100.0 score (returns 0.0 if empty/missing).
    - Valid zero metrics remain zero.
    - Never produces NaN, +Inf, or -Inf.
    - Bounded strictly between 0.0 and 100.0.

    Deduction Weights:
    - Invalid records: proportional deduction up to 50 points.
    - Conflicting duplicates: 10 points each (up to 30 points).
    - Out of order records: 5 points each (up to 25 points).
    - Detected interval gaps: 5 points each (up to 25 points).
    - Staleness: 15 points if stale.
    - Anomalies: 5 points per WARNING, 10 points per ERROR/CRITICAL (up to 30 points).
    - Harmless duplicates: 1 point each (up to 10 points).
    """
    if total_records <= 0 or status == DataQualityStatus.MISSING:
        return 0.0

    if status == DataQualityStatus.INVALID and valid_records == 0:
        return 0.0

    score = 100.0

    # 1. Invalid records penalty (proportional)
    if total_records > 0 and invalid_records > 0:
        invalid_ratio = min(1.0, invalid_records / total_records)
        score -= invalid_ratio * 50.0

    # 2. Conflicting duplicates penalty
    if conflicting_duplicate_count > 0:
        score -= min(30.0, conflicting_duplicate_count * 10.0)

    # 3. Out-of-order records penalty
    if out_of_order_count > 0:
        score -= min(25.0, out_of_order_count * 5.0)

    # 4. Gaps penalty
    if gap_count > 0:
        score -= min(25.0, gap_count * 5.0)

    # 5. Stale data penalty
    if is_stale:
        score -= 15.0

    # 6. Anomaly deductions
    anomaly_deduction = 0.0
    for anom in anomalies:
        if anom.severity in (QualitySeverity.CRITICAL, QualitySeverity.ERROR):
            anomaly_deduction += 10.0
        elif anom.severity == QualitySeverity.WARNING:
            anomaly_deduction += 5.0
    score -= min(30.0, anomaly_deduction)

    # 7. Harmless duplicates penalty
    if duplicate_count > 0:
        score -= min(10.0, duplicate_count * 1.0)

    # Clamp to [0.0, 100.0] and ensure finite float
    if not math.isfinite(score):
        return 0.0
    return max(0.0, min(100.0, round(score, 2)))
