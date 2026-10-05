"""KAISER — Market Data Quality Status & Assessment Models.

Defines the core immutable DataQualityAssessment model and DataQualityStatus enum.
Supports both single-record streaming evaluations and multi-record dataset quality monitoring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import math
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from app.market.quality.models import MarketAnomaly, QualityFinding, QualitySeverity
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
    """Immutable evaluation report produced for market data records and datasets.

    Attributes:
        assessment_id: Unique identifier for this evaluation.
        symbol: Market ticker symbol.
        data_type: Kind of record evaluated ('quote', 'bar', 'order_book', 'series').
        source: Provenance feed tag ('mock', 'replay', 'sqlite', etc.).
        timestamp: POSIX timestamp of the underlying market record or latest bar.
        quality_status: Primary synthesized DataQualityStatus outcome.
        checks_performed: Tuple of names of all validation checks executed.
        warnings: Non-fatal issues.
        errors: Fatal validation or schema violations.
        freshness_status: DataFreshnessStatus (FRESH, STALE, INVALID).
        completeness_status: 'COMPLETE', 'PARTIAL', or 'INCOMPLETE'.
        sequence_status: 'IN_ORDER' or 'OUT_OF_ORDER'.
        duplicate_status: 'UNIQUE' or 'DUPLICATE'.
        assessment_timestamp: POSIX timestamp when assessment was computed.
        correlation_id: Tracing correlation identifier.
        safe_to_consume: Boolean indicating whether downstream trading intelligence is permitted.
        timeframe: Bar timeframe (e.g. '1m', '5m', '15m', '1h', '1D').
        evaluated_at: POSIX timestamp when assessment was executed.
        data_start: Timestamp of earliest observation in evaluated dataset.
        data_end: Timestamp of latest observation in evaluated dataset.
        total_records: Total count of records evaluated.
        valid_records: Count of records satisfying all validation invariants.
        invalid_records: Count of corrupted or structurally invalid records.
        duplicate_count: Count of harmless duplicate records.
        conflicting_duplicate_count: Count of conflicting duplicate records.
        out_of_order_count: Count of out-of-order timestamp violations.
        gap_count: Count of detected missing interval gaps.
        stale_status: Boolean indicating if data exceeds stale threshold.
        anomaly_count: Count of detected deterministic anomalies.
        highest_severity: Highest QualitySeverity level encountered across findings.
        findings: Tuple of structured QualityFinding objects.
        anomalies: Tuple of structured MarketAnomaly objects.
        provenance: Metadata regarding source, loader, or repository origin.
        quality_score: Numerical bounded quality score [0.0, 100.0].
    """

    assessment_id: str
    symbol: str
    data_type: str = "bar"
    source: str = "unknown"
    timestamp: float = 0.0
    quality_status: DataQualityStatus = DataQualityStatus.VALID
    checks_performed: Tuple[str, ...] = ()
    warnings: Tuple[str, ...] = ()
    errors: Tuple[str, ...] = ()
    freshness_status: DataFreshnessStatus = DataFreshnessStatus.FRESH
    completeness_status: str = "COMPLETE"
    sequence_status: str = "IN_ORDER"
    duplicate_status: str = "UNIQUE"
    assessment_timestamp: float = 0.0
    correlation_id: str = ""
    safe_to_consume: bool = True

    # Step 19 Dataset Dimensions & Monitoring Extensions
    timeframe: str = ""
    evaluated_at: float = 0.0
    data_start: Optional[float] = None
    data_end: Optional[float] = None
    total_records: int = 1
    valid_records: int = 1
    invalid_records: int = 0
    duplicate_count: int = 0
    conflicting_duplicate_count: int = 0
    out_of_order_count: int = 0
    gap_count: int = 0
    stale_status: bool = False
    anomaly_count: int = 0
    highest_severity: QualitySeverity = QualitySeverity.INFO
    findings: Tuple[QualityFinding, ...] = ()
    anomalies: Tuple[MarketAnomaly, ...] = ()
    provenance: Dict[str, Any] = field(default_factory=dict)
    quality_score: float = 100.0

    def __post_init__(self) -> None:
        """Enforce strict immutability, freeze tuples, and synchronize timestamps."""
        object.__setattr__(self, "checks_performed", tuple(self.checks_performed or ()))
        object.__setattr__(self, "warnings", tuple(self.warnings or ()))
        object.__setattr__(self, "errors", tuple(self.errors or ()))
        object.__setattr__(self, "findings", tuple(self.findings or ()))
        object.__setattr__(self, "anomalies", tuple(self.anomalies or ()))

        # Synchronize evaluated_at and assessment_timestamp
        eval_time = self.evaluated_at
        if eval_time <= 0.0:
            eval_time = self.assessment_timestamp if self.assessment_timestamp > 0.0 else time.time()
            object.__setattr__(self, "evaluated_at", eval_time)

        if self.assessment_timestamp <= 0.0:
            object.__setattr__(self, "assessment_timestamp", eval_time)

        # Normalize symbol
        if self.symbol:
            object.__setattr__(self, "symbol", self.symbol.strip().upper())

        # Ensure enums
        if not isinstance(self.quality_status, DataQualityStatus):
            object.__setattr__(self, "quality_status", DataQualityStatus(str(self.quality_status)))
        if not isinstance(self.highest_severity, QualitySeverity):
            object.__setattr__(self, "highest_severity", QualitySeverity(str(self.highest_severity)))

        # Freeze provenance dictionary
        object.__setattr__(self, "provenance", dict(self.provenance or {}))

        # Clamp numerical quality_score to [0.0, 100.0] and verify finite
        score = self.quality_score
        if not math.isfinite(score):
            score = 0.0
        score = max(0.0, min(100.0, round(float(score), 2)))
        object.__setattr__(self, "quality_score", score)

    @property
    def overall_quality_status(self) -> DataQualityStatus:
        """Alias for quality_status matching Step 19 terminology."""
        return self.quality_status

    @property
    def is_stale(self) -> bool:
        """Convenience property for freshness stale status."""
        return self.stale_status or self.freshness_status == DataFreshnessStatus.STALE

    @property
    def iso_assessment_timestamp(self) -> str:
        """Return the ISO-8601 UTC timestamp string of the assessment."""
        ts = self.evaluated_at if self.evaluated_at > 0.0 else self.assessment_timestamp
        if math.isfinite(ts) and ts > 0:
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        return "1970-01-01T00:00:00Z"

    def to_dict(self) -> Dict[str, Any]:
        """Return a clean serializable dictionary representation."""
        return {
            "assessment_id": self.assessment_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "data_type": self.data_type,
            "source": self.source,
            "timestamp": self.timestamp,
            "quality_status": self.quality_status.value,
            "overall_quality_status": self.quality_status.value,
            "highest_severity": self.highest_severity.value,
            "quality_score": self.quality_score,
            "evaluated_at": self.evaluated_at,
            "assessment_timestamp": self.assessment_timestamp,
            "iso_assessment_timestamp": self.iso_assessment_timestamp,
            "data_start": self.data_start,
            "data_end": self.data_end,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "duplicate_count": self.duplicate_count,
            "conflicting_duplicate_count": self.conflicting_duplicate_count,
            "out_of_order_count": self.out_of_order_count,
            "gap_count": self.gap_count,
            "stale_status": self.stale_status,
            "anomaly_count": self.anomaly_count,
            "checks_performed": list(self.checks_performed),
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "findings": [f.to_dict() for f in self.findings],
            "anomalies": [a.to_dict() for a in self.anomalies],
            "provenance": dict(self.provenance),
            "freshness_status": self.freshness_status.value,
            "completeness_status": self.completeness_status,
            "sequence_status": self.sequence_status,
            "duplicate_status": self.duplicate_status,
            "correlation_id": self.correlation_id,
            "safe_to_consume": self.safe_to_consume,
        }


# Convenient alias for multi-record assessments
DatasetQualityAssessment = DataQualityAssessment
