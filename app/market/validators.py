"""KAISER — Market Data Validation & Freshness Evaluation.

Implements domain-level integrity checks and configurable freshness status detection.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Any, Dict, Optional

from app.core.exceptions import ValidationError


class DataFreshnessStatus(str, Enum):
    """Data freshness category for incoming market data feeds."""

    FRESH = "FRESH"
    STALE = "STALE"
    INVALID = "INVALID"


def check_freshness(
    record_timestamp: float,
    reference_time: Optional[float] = None,
    threshold_seconds: float = 60.0,
    max_future_skew_seconds: float = 300.0,
) -> DataFreshnessStatus:
    """Assess whether a market record timestamp is fresh, stale, or invalid.

    Args:
        record_timestamp: POSIX UTC timestamp of the market record.
        reference_time: Comparison timestamp (defaults to current time.time()).
                        For replay feeds, reference_time should be the replay clock.
        threshold_seconds: Max acceptable age in seconds before data is marked stale.
        max_future_skew_seconds: Max acceptable clock skew into future before considered invalid.

    Returns:
        DataFreshnessStatus: FRESH, STALE, or INVALID.
    """
    if record_timestamp <= 0:
        return DataFreshnessStatus.INVALID

    now = time.time() if reference_time is None else reference_time

    # Record timestamp is in the distant future
    if record_timestamp > now + max_future_skew_seconds:
        return DataFreshnessStatus.INVALID

    # Record timestamp is older than threshold
    if now - record_timestamp > threshold_seconds:
        return DataFreshnessStatus.STALE

    return DataFreshnessStatus.FRESH


def validate_raw_quote_dict(data: Dict[str, Any]) -> None:
    """Validate mandatory fields presence and basic types in a raw quote dictionary."""
    if not isinstance(data, dict):
        raise ValidationError(f"Raw quote data must be a dictionary, got {type(data).__name__}")

    # Check for presence of symbol in common key variations
    if not any(k in data for k in ("symbol", "Symbol", "ticker", "instrument")):
        raise ValidationError("Missing mandatory symbol field in raw quote.")

    # Check for presence of price in common key variations
    if not any(k in data for k in ("last_price", "lastPrice", "price", "ltp", "close")):
        raise ValidationError("Missing mandatory price field in raw quote.")


def validate_raw_bar_dict(data: Dict[str, Any]) -> None:
    """Validate mandatory fields presence and basic types in a raw bar dictionary."""
    if not isinstance(data, dict):
        raise ValidationError(f"Raw bar data must be a dictionary, got {type(data).__name__}")

    if not any(k in data for k in ("symbol", "Symbol", "ticker", "instrument")):
        raise ValidationError("Missing mandatory symbol field in raw bar.")

    for field in ("open", "high", "low", "close"):
        if field not in data and field.capitalize() not in data:
            raise ValidationError(f"Missing mandatory OHLC field '{field}' in raw bar.")
