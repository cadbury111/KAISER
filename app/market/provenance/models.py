"""KAISER — Market Data Provenance, Lineage & Audit Models.

Provides strongly-typed, immutable data structures for end-to-end market data
traceability, deterministic lineage identification, transformation tracking,
and quality audit integration.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
import re
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from app.analysis.models import normalize_timeframe
from app.core.exceptions import ValidationError
from app.market.quality.status import DataQualityStatus

# Sanitization regexes to prevent secret leakage in provenance metadata
CREDENTIAL_KEY_PATTERNS = re.compile(
    r"(?i)(api[_-]?key|secret|password|passphrase|private[_-]?key|auth[_-]?token|authorization|bearer|credential|token|broker_token|signature)"
)
WINDOWS_PATH_REGEX = re.compile(r"[A-Za-z]:\\[^\s\"'<>]+", re.IGNORECASE)
UNIX_PATH_REGEX = re.compile(r"/(?:Users|home|root|var|etc|tmp|opt|usr)/[^\s\"'<>]+", re.IGNORECASE)


def sanitize_provenance_string(text: str) -> str:
    """Sanitize strings by stripping out filesystem paths and potential credentials."""
    if not isinstance(text, str):
        return str(text)
    sanitized = WINDOWS_PATH_REGEX.sub("[PATH]", text)
    sanitized = UNIX_PATH_REGEX.sub("[PATH]", sanitized)
    return sanitized


def sanitize_provenance_metadata(data: Mapping[str, Any]) -> Dict[str, Any]:
    """Recursively scrub sensitive keys and credential patterns from metadata dictionary."""
    clean: Dict[str, Any] = {}
    for key, value in data.items():
        key_str = str(key)
        if CREDENTIAL_KEY_PATTERNS.search(key_str):
            clean[key_str] = "***REDACTED***"
            continue

        if isinstance(value, Mapping):
            clean[key_str] = sanitize_provenance_metadata(value)
        elif isinstance(value, (list, tuple)):
            clean_list = []
            for item in value:
                if isinstance(item, Mapping):
                    clean_list.append(sanitize_provenance_metadata(item))
                elif isinstance(item, str):
                    if CREDENTIAL_KEY_PATTERNS.search(item):
                        clean_list.append("***REDACTED***")
                    else:
                        clean_list.append(sanitize_provenance_string(item))
                else:
                    clean_list.append(item)
            clean[key_str] = clean_list
        elif isinstance(value, str):
            if CREDENTIAL_KEY_PATTERNS.search(value):
                clean[key_str] = "***REDACTED***"
            else:
                clean[key_str] = sanitize_provenance_string(value)
        else:
            clean[key_str] = value
    return clean


class DataSourceType(str, Enum):
    """Categorical source classification for market data provenance."""

    MOCK = "MOCK"
    PROVIDER = "PROVIDER"
    SQLITE = "SQLITE"
    CACHE = "CACHE"
    REPLAY = "REPLAY"
    BACKTEST = "BACKTEST"
    DERIVED = "DERIVED"

    @classmethod
    def from_string(cls, val: Union[str, DataSourceType]) -> DataSourceType:
        """Parse source type flexibly from string or enum."""
        if isinstance(val, DataSourceType):
            return val
        if not val or not isinstance(val, str):
            return cls.MOCK
        v_upper = val.strip().upper()
        if v_upper in cls._value2member_map_:
            return cls(v_upper)
        # Check specific keywords before broad PROVIDER/HISTORICAL
        if "REPLAY" in v_upper:
            return cls.REPLAY
        if "CACHE" in v_upper:
            return cls.CACHE
        if "BACKTEST" in v_upper or "SIMULATION" in v_upper:
            return cls.BACKTEST
        if "DERIVED" in v_upper or "ANALYSIS" in v_upper or "INDICATOR" in v_upper:
            return cls.DERIVED
        if "SQLITE" in v_upper or "REPOSITORY" in v_upper or "DB" in v_upper:
            return cls.SQLITE
        if v_upper.startswith("PROVIDER") or "HISTORICAL" in v_upper or "FEED" in v_upper:
            return cls.PROVIDER
        return cls.MOCK


def generate_deterministic_provenance_id(
    source_type: Union[DataSourceType, str],
    source_identifier: str,
    symbol: str,
    timeframe: str,
    data_start: Optional[float] = None,
    data_end: Optional[float] = None,
    record_count: int = 0,
    transformation_name: Optional[str] = None,
    transformation_version: Optional[str] = None,
    parent_provenance_ids: Sequence[str] = (),
    quality_assessment_id: Optional[str] = None,
    schema_version: str = "1.0.0",
) -> str:
    """Generate a collision-resistant, deterministic identity hash for market data provenance.

    Guarantees:
    1. Stable for identical canonical inputs across execution sessions and environments.
    2. Independent of memory addresses, process IDs, or system clock.
    3. Collision-resistant via SHA-256 canonical digest.
    4. Does not contain raw secrets or memory pointers.
    """
    src_type_str = source_type.value if isinstance(source_type, DataSourceType) else str(source_type).upper()
    norm_sym = symbol.strip().upper() if symbol else "UNKNOWN"
    norm_tf = timeframe.strip() if timeframe else "UNKNOWN"
    norm_parents = sorted(set(str(p).strip() for p in parent_provenance_ids if p and str(p).strip()))

    canonical_dict = {
        "data_end": round(float(data_end), 4) if data_end is not None else None,
        "data_start": round(float(data_start), 4) if data_start is not None else None,
        "parent_provenance_ids": norm_parents,
        "quality_assessment_id": str(quality_assessment_id).strip() if quality_assessment_id else None,
        "record_count": int(record_count),
        "schema_version": str(schema_version).strip(),
        "source_identifier": str(source_identifier).strip().lower(),
        "source_type": src_type_str,
        "symbol": norm_sym,
        "timeframe": norm_tf,
        "transformation_name": str(transformation_name).strip().lower() if transformation_name else None,
        "transformation_version": str(transformation_version).strip() if transformation_version else None,
    }

    canonical_json = json.dumps(canonical_dict, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return f"prov_{digest[:24]}"


@dataclass(frozen=True)
class MarketDataProvenance:
    """Immutable audit record representing the complete provenance of market data.

    Enables retrospective traceability of any market-derived result to its
    original source, data range, ingestion operation, quality assessment,
    storage/replay origin, and transformation sequence without duplicating raw data.
    """

    provenance_id: str
    operation_id: str
    source_type: DataSourceType
    source_identifier: str
    symbol: str
    timeframe: str
    data_start: Optional[float] = None
    data_end: Optional[float] = None
    record_count: int = 0
    ingestion_timestamp: float = field(default_factory=time.time)
    processing_timestamp: float = field(default_factory=time.time)
    quality_assessment_id: Optional[str] = None
    quality_status: Optional[DataQualityStatus] = None
    quality_score: Optional[float] = None
    schema_version: str = "1.0.0"
    transformation_name: Optional[str] = None
    transformation_version: Optional[str] = None
    input_record_count: Optional[int] = None
    output_record_count: Optional[int] = None
    parent_provenance_ids: Tuple[str, ...] = ()
    replay_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate provenance invariants."""
        if not self.provenance_id or not isinstance(self.provenance_id, str):
            raise ValidationError("MarketDataProvenance provenance_id must be a non-empty string.")

        if not self.operation_id or not isinstance(self.operation_id, str):
            raise ValidationError("MarketDataProvenance operation_id must be a non-empty string.")

        if not isinstance(self.source_type, DataSourceType):
            object.__setattr__(self, "source_type", DataSourceType.from_string(str(self.source_type)))

        if not self.symbol or not isinstance(self.symbol, str):
            raise ValidationError("MarketDataProvenance symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        if not self.timeframe or not isinstance(self.timeframe, str):
            raise ValidationError("MarketDataProvenance timeframe must be a non-empty string.")
        try:
            object.__setattr__(self, "timeframe", normalize_timeframe(self.timeframe))
        except ValidationError:
            object.__setattr__(self, "timeframe", self.timeframe.strip())

        if self.record_count < 0:
            raise ValidationError(f"record_count cannot be negative, got {self.record_count}")

        if self.input_record_count is not None and self.input_record_count < 0:
            raise ValidationError(f"input_record_count cannot be negative, got {self.input_record_count}")

        if self.output_record_count is not None and self.output_record_count < 0:
            raise ValidationError(f"output_record_count cannot be negative, got {self.output_record_count}")

        if self.quality_score is not None:
            if not math.isfinite(self.quality_score) or self.quality_score < 0.0 or self.quality_score > 100.0:
                raise ValidationError(
                    f"quality_score must be a finite float in [0.0, 100.0], got {self.quality_score}"
                )

        if self.data_start is not None and self.data_end is not None:
            if self.data_end < self.data_start:
                raise ValidationError(
                    f"data_end ({self.data_end}) cannot be earlier than data_start ({self.data_start})"
                )

        # Enforce tuple for parent_provenance_ids and cycle/self-reference prevention
        if not isinstance(self.parent_provenance_ids, tuple):
            object.__setattr__(self, "parent_provenance_ids", tuple(self.parent_provenance_ids or ()))

        if self.provenance_id in self.parent_provenance_ids:
            raise ValidationError(
                f"Self-referential lineage defect: provenance_id '{self.provenance_id}' cannot be in parent_provenance_ids."
            )

        # Sanitize metadata to prevent credential or local filesystem leakage
        sanitized_meta = sanitize_provenance_metadata(self.metadata or {})
        object.__setattr__(self, "metadata", sanitized_meta)

    @property
    def has_quality(self) -> bool:
        """True if a quality assessment is linked."""
        return self.quality_assessment_id is not None or self.quality_status is not None

    @property
    def is_derived(self) -> bool:
        """True if this provenance was derived from upstream parent operations."""
        return len(self.parent_provenance_ids) > 0 or self.transformation_name is not None

    @property
    def parent_count(self) -> int:
        """Number of immediate upstream parent operations."""
        return len(self.parent_provenance_ids)

    def to_dict(self) -> Dict[str, Any]:
        """Convert provenance into a JSON-serializable dictionary."""
        return {
            "provenance_id": self.provenance_id,
            "operation_id": self.operation_id,
            "source_type": self.source_type.value,
            "source_identifier": self.source_identifier,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "data_start": self.data_start,
            "data_end": self.data_end,
            "record_count": self.record_count,
            "ingestion_timestamp": self.ingestion_timestamp,
            "processing_timestamp": self.processing_timestamp,
            "quality_assessment_id": self.quality_assessment_id,
            "quality_status": self.quality_status.value if self.quality_status else None,
            "quality_score": round(self.quality_score, 2) if self.quality_score is not None else None,
            "schema_version": self.schema_version,
            "transformation_name": self.transformation_name,
            "transformation_version": self.transformation_version,
            "input_record_count": self.input_record_count,
            "output_record_count": self.output_record_count,
            "parent_provenance_ids": list(self.parent_provenance_ids),
            "replay_id": self.replay_id,
            "metadata": copy.deepcopy(self.metadata),
        }


@dataclass(frozen=True)
class LineageNode:
    """Node in an upstream/downstream lineage tree."""

    provenance: MarketDataProvenance
    depth: int
    parents: Tuple[LineageNode, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize node and its parent hierarchy."""
        return {
            "provenance": self.provenance.to_dict(),
            "depth": self.depth,
            "parents": [p.to_dict() for p in self.parents],
        }


@dataclass(frozen=True)
class LineageGraph:
    """Full lineage traversal result with cycle prevention and depth clamping."""

    root_id: str
    nodes: Dict[str, Dict[str, Any]]
    edges: Tuple[Dict[str, str], ...]
    max_depth_reached: bool = False
    cycle_detected: bool = False
    direction: str = "upstream"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize graph to dictionary."""
        return {
            "root_id": self.root_id,
            "direction": self.direction,
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "max_depth_reached": self.max_depth_reached,
            "cycle_detected": self.cycle_detected,
            "nodes": self.nodes,
            "edges": list(self.edges),
        }
