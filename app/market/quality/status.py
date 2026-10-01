"""KAISER — Market Data Quality Status & Assessment Model.

Defines the core immutable DataQualityAssessment and DataQualityStatus enum.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Tuple

from app.market.validators import DataFreshnessStatus


class DataQualityStatus(str, Enum):
    """Categorical quality classification for normalized market data."""

    VALID = "VALID"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    INVALID = "INVALID"
    MISSING = "MISSING"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"


@dataclass(frozen=True)
class DataQualityAssessment:
    """Immutable evaluation report produced for a market data record.

    Attributes:
        assessment_id: Unique identifier for this evaluation.
        symbol: Market ticker symbol.
        data_type: Kind of record evaluated ('quote', 'bar', 'order_book').
        source: Provenance feed tag ('mock', 'replay', etc.).
        timestamp: POSIX timestamp of the underlying market record.
        quality_status: Primary synthesized DataQualityStatus outcome.
        checks_performed: Tuple of names of all validation checks executed.
        warnings: Non-fatal issues (e.g. missing optional fields).
        errors: Fatal validation or schema violations.
        freshness_status: DataFreshnessStatus (FRESH, STALE, INVALID).
        completeness_status: 'COMPLETE', 'PARTIAL', or 'INCOMPLETE'.
        sequence_status: 'IN_ORDER' or 'OUT_OF_ORDER'.
        duplicate_status: 'UNIQUE' or 'DUPLICATE'.
        assessment_timestamp: POSIX timestamp when assessment was computed.
        correlation_id: Tracing correlation identifier.
        safe_to_consume: Boolean indicating whether downstream trading intelligence
                         is permitted to safely consume this record.
    """

    assessment_id: str
    symbol: str
    data_type: str
    source: str
    timestamp: float
    quality_status: DataQualityStatus
    checks_performed: Tuple[str, ...]
    warnings: Tuple[str, ...]
    errors: Tuple[str, ...]
    freshness_status: DataFreshnessStatus
    completeness_status: str
    sequence_status: str
    duplicate_status: str
    assessment_timestamp: float
    correlation_id: str
    safe_to_consume: bool

    def __post_init__(self) -> None:
        """Ensure tuples are frozen and immutability is strictly preserved."""
        object.__setattr__(self, "checks_performed", tuple(self.checks_performed or ()))
        object.__setattr__(self, "warnings", tuple(self.warnings or ()))
        object.__setattr__(self, "errors", tuple(self.errors or ()))

    @property
    def iso_assessment_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string of the assessment."""
        return datetime.fromtimestamp(self.assessment_timestamp, tz=timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Return a clean serializable dictionary representation."""
        return {
            "assessment_id": self.assessment_id,
            "symbol": self.symbol,
            "data_type": self.data_type,
            "source": self.source,
            "timestamp": self.timestamp,
            "quality_status": self.quality_status.value,
            "checks_performed": list(self.checks_performed),
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "freshness_status": self.freshness_status.value,
            "completeness_status": self.completeness_status,
            "sequence_status": self.sequence_status,
            "duplicate_status": self.duplicate_status,
            "assessment_timestamp": self.assessment_timestamp,
            "iso_assessment_timestamp": self.iso_assessment_timestamp,
            "correlation_id": self.correlation_id,
            "safe_to_consume": self.safe_to_consume,
        }
