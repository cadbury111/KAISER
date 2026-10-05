"""KAISER — Market Data Quality & Anomaly Models.

Provides strongly-typed, immutable models for market data quality evaluation:
- QualitySeverity: Deterministic severity hierarchy (CRITICAL > ERROR > WARNING > INFO).
- AnomalyType: Standardized classification of rule-based market data anomalies.
- MarketAnomaly: Immutable record of a detected anomaly with full provenance.
- QualityFinding: Immutable structured finding from a quality rule evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
import re
import time
from typing import Any, Dict, Mapping, Optional, Tuple, Union

from app.core.exceptions import ValidationError

# Sanitization regexes to prevent secret leakage in findings and anomalies
PROHIBITED_KEY_PATTERNS = re.compile(
    r"(?i)(api[_-]?key|secret|password|private[_-]?key|auth[_-]?token|bearer|credential|token)"
)
WINDOWS_PATH_REGEX = re.compile(r"[A-Za-z]:\\[^\s\"'<>]+", re.IGNORECASE)
UNIX_PATH_REGEX = re.compile(r"/(?:Users|home|root|var|etc|tmp|opt|usr)/[^\s\"'<>]+", re.IGNORECASE)


def sanitize_message(text: str) -> str:
    """Sanitize strings by stripping out filesystem paths and potential credentials."""
    if not isinstance(text, str):
        return str(text)
    sanitized = WINDOWS_PATH_REGEX.sub("[PATH]", text)
    sanitized = UNIX_PATH_REGEX.sub("[PATH]", sanitized)
    return sanitized


class QualitySeverity(str, Enum):
    """Deterministic severity classification hierarchy."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @property
    def rank(self) -> int:
        """Deterministic integer rank for comparisons (CRITICAL > ERROR > WARNING > INFO)."""
        _RANKS = {
            QualitySeverity.INFO: 1,
            QualitySeverity.WARNING: 2,
            QualitySeverity.ERROR: 3,
            QualitySeverity.CRITICAL: 4,
        }
        return _RANKS[self]

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, QualitySeverity):
            return self.rank >= other.rank
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, QualitySeverity):
            return self.rank > other.rank
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if isinstance(other, QualitySeverity):
            return self.rank <= other.rank
        return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, QualitySeverity):
            return self.rank < other.rank
        return NotImplemented


def max_severity(s1: QualitySeverity, s2: QualitySeverity) -> QualitySeverity:
    """Return the higher of two severities deterministically."""
    return s1 if s1 >= s2 else s2


class AnomalyType(str, Enum):
    """Standardized deterministic market data anomaly classifications."""

    PRICE_JUMP = "PRICE_JUMP"
    VOLUME_SPIKE = "VOLUME_SPIKE"
    ZERO_VOLUME = "ZERO_VOLUME"
    LARGE_CANDLE_RANGE = "LARGE_CANDLE_RANGE"
    REPEATED_IDENTICAL_BARS = "REPEATED_IDENTICAL_BARS"
    EXCESSIVE_GAP = "EXCESSIVE_GAP"
    INVERTED_OHLC = "INVERTED_OHLC"
    NEGATIVE_PRICE = "NEGATIVE_PRICE"
    NON_FINITE_VALUE = "NON_FINITE_VALUE"
    OUT_OF_ORDER_TIMESTAMP = "OUT_OF_ORDER_TIMESTAMP"
    DUPLICATE_TIMESTAMP = "DUPLICATE_TIMESTAMP"
    CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"
    TIMEFRAME_MISMATCH = "TIMEFRAME_MISMATCH"
    SYMBOL_MISMATCH = "SYMBOL_MISMATCH"


@dataclass(frozen=True)
class MarketAnomaly:
    """Immutable record of a detected market data anomaly.

    Attributes:
        anomaly_id: Unique anomaly identifier.
        symbol: Market ticker symbol.
        timeframe: Bar timeframe.
        start_timestamp: Start POSIX UTC timestamp or single observation timestamp.
        end_timestamp: End POSIX UTC timestamp if anomaly spans a range.
        anomaly_type: Classified AnomalyType.
        severity: QualitySeverity ranking.
        observed_value: Observed metric value.
        threshold_or_reference: Threshold or baseline value used for comparison.
        reason: Deterministic human-readable explanation.
        provenance: Subsystem or data source origin.
        detected_at: POSIX timestamp when anomaly was identified.
    """

    anomaly_id: str
    symbol: str
    timeframe: str
    start_timestamp: float
    anomaly_type: AnomalyType
    severity: QualitySeverity
    observed_value: Any
    end_timestamp: Optional[float] = None
    threshold_or_reference: Optional[Any] = None
    reason: str = ""
    provenance: str = "market_data_quality"
    detected_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        """Validate anomaly invariants."""
        if not self.anomaly_id:
            raise ValidationError("anomaly_id must not be empty.")
        if not self.symbol:
            raise ValidationError("symbol must not be empty.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "reason", sanitize_message(self.reason))
        if not isinstance(self.anomaly_type, AnomalyType):
            object.__setattr__(self, "anomaly_type", AnomalyType(str(self.anomaly_type)))
        if not isinstance(self.severity, QualitySeverity):
            object.__setattr__(self, "severity", QualitySeverity(str(self.severity)))

    @property
    def timestamp(self) -> float:
        """Alias for start_timestamp for uniform timestamp querying."""
        return self.start_timestamp

    @property
    def iso_timestamp(self) -> str:
        """ISO-8601 UTC timestamp of start_timestamp."""
        if math.isfinite(self.start_timestamp) and self.start_timestamp > 0:
            return datetime.fromtimestamp(self.start_timestamp, tz=timezone.utc).isoformat()
        return "1970-01-01T00:00:00Z"

    def to_dict(self) -> Dict[str, Any]:
        """Convert anomaly to clean serializable dictionary."""
        return {
            "anomaly_id": self.anomaly_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "timestamp": self.start_timestamp,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "iso_timestamp": self.iso_timestamp,
            "anomaly_type": self.anomaly_type.value,
            "severity": self.severity.value,
            "observed_value": self.observed_value,
            "threshold_or_reference": self.threshold_or_reference,
            "reason": self.reason,
            "provenance": self.provenance,
            "detected_at": self.detected_at,
        }


@dataclass(frozen=True)
class QualityFinding:
    """Immutable finding representing a discrete quality observation or defect.

    Attributes:
        finding_id: Unique finding identifier.
        dimension: Quality dimension evaluated.
        rule_name: Deterministic check or rule name.
        severity: QualitySeverity ranking.
        message: Descriptive summary of the finding.
        affected_count: Number of records or occurrences impacted.
        details: Structured parameters or context.
        timestamp: Associated market timestamp if applicable.
    """

    finding_id: str
    dimension: str
    rule_name: str
    severity: QualitySeverity
    message: str
    affected_count: int = 1
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[float] = None

    def __post_init__(self) -> None:
        """Ensure types and sanitize message."""
        if not self.finding_id:
            raise ValidationError("finding_id must not be empty.")
        if not isinstance(self.severity, QualitySeverity):
            object.__setattr__(self, "severity", QualitySeverity(str(self.severity)))
        object.__setattr__(self, "message", sanitize_message(self.message))
        # Freeze details dictionary
        clean_details = {k: sanitize_message(str(v)) if isinstance(v, str) else v for k, v in self.details.items()}
        object.__setattr__(self, "details", clean_details)

    def to_dict(self) -> Dict[str, Any]:
        """Convert finding into a clean serializable dictionary."""
        return {
            "finding_id": self.finding_id,
            "dimension": self.dimension,
            "rule_name": self.rule_name,
            "severity": self.severity.value,
            "message": self.message,
            "affected_count": self.affected_count,
            "details": dict(self.details),
            "timestamp": self.timestamp,
        }
