"""KAISER — Reproducible Analysis Run & Experiment Tracking Domain Models.

Provides strongly-typed, immutable data structures for analytical run tracking,
deterministic configuration fingerprinting, reproducibility identification,
and lifecycle transition enforcement.
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
from app.market.provenance.models import CREDENTIAL_KEY_PATTERNS, sanitize_provenance_metadata
from app.market.quality.status import DataQualityStatus

MEMORY_ADDRESS_REGEX = re.compile(r"0x[0-9a-fA-F]+")
VOLATILE_CONFIG_KEYS = frozenset({
    "run_id",
    "request_id",
    "correlation_id",
    "timestamp",
    "created_at",
    "started_at",
    "completed_at",
    "ingestion_timestamp",
    "processing_timestamp",
    "duration_ms",
    "execution_time",
    "pacing_seconds",
})


class OperationType(str, Enum):
    """Categorical classification of analytical operations tracked in the registry."""

    TECHNICAL_ANALYSIS = "TECHNICAL_ANALYSIS"
    MULTI_TIMEFRAME_ANALYSIS = "MULTI_TIMEFRAME_ANALYSIS"
    BACKTEST = "BACKTEST"
    WALK_FORWARD = "WALK_FORWARD"
    ROBUSTNESS = "ROBUSTNESS"
    PORTFOLIO_SIMULATION = "PORTFOLIO_SIMULATION"
    PORTFOLIO_ATTRIBUTION = "PORTFOLIO_ATTRIBUTION"
    DATA_QUALITY = "DATA_QUALITY"
    REPLAY = "REPLAY"
    OTHER_ANALYSIS = "OTHER_ANALYSIS"

    @classmethod
    def from_string(cls, val: Union[str, OperationType]) -> OperationType:
        """Parse operation type safely from string or enum."""
        if isinstance(val, OperationType):
            return val
        if not val or not isinstance(val, str):
            return cls.OTHER_ANALYSIS
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        # Fuzzy keyword matching
        if "MULTI" in clean and "TIMEFRAME" in clean:
            return cls.MULTI_TIMEFRAME_ANALYSIS
        if "TECH" in clean or "INDICATOR" in clean:
            return cls.TECHNICAL_ANALYSIS
        if "WALK" in clean or "FORWARD" in clean:
            return cls.WALK_FORWARD
        if "ROBUST" in clean or "STRESS" in clean:
            return cls.ROBUSTNESS
        if "SIMULATION" in clean:
            return cls.PORTFOLIO_SIMULATION
        if "ATTRIBUTION" in clean:
            return cls.PORTFOLIO_ATTRIBUTION
        if "BACKTEST" in clean:
            return cls.BACKTEST
        if "QUALITY" in clean:
            return cls.DATA_QUALITY
        if "REPLAY" in clean:
            return cls.REPLAY
        return cls.OTHER_ANALYSIS


class RunStatus(str, Enum):
    """Deterministic lifecycle state of an analytical run."""

    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INVALID = "INVALID"

    @property
    def is_terminal(self) -> bool:
        """True if the run status represents a final, unalterable terminal state."""
        return self in (
            RunStatus.COMPLETED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.INVALID,
        )

    @property
    def is_successful(self) -> bool:
        """True if the run finished successfully."""
        return self == RunStatus.COMPLETED


ALLOWED_TRANSITIONS: Mapping[RunStatus, frozenset[RunStatus]] = {
    RunStatus.CREATED: frozenset({RunStatus.RUNNING, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.INVALID}),
    RunStatus.RUNNING: frozenset({RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}),
    RunStatus.COMPLETED: frozenset(),
    RunStatus.FAILED: frozenset(),
    RunStatus.CANCELLED: frozenset(),
    RunStatus.INVALID: frozenset(),
}


def validate_lifecycle_transition(current: RunStatus, target: RunStatus) -> None:
    """Validate that moving from `current` to `target` follows legal lifecycle transitions.

    Raises:
        ValidationError: If the transition is illegal or attempts to mutate a terminal state.
    """
    if current.is_terminal:
        raise ValidationError(
            f"Illegal state transition: Run is in terminal state '{current.value}' and cannot transition to '{target.value}'."
        )
    allowed = ALLOWED_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise ValidationError(
            f"Illegal state transition: Cannot transition run from '{current.value}' to '{target.value}'. Allowed: {[s.value for s in allowed]}"
        )


def _canonicalize_value_for_fingerprint(val: Any) -> Any:
    """Recursively canonicalize any data structure for deterministic fingerprinting."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        if math.isnan(val) or math.isinf(val):
            raise ValidationError("Configuration numbers cannot be NaN or Infinite.")
        # Exact rounding or normalized string to avoid platform float representation quirks
        return round(float(val), 8) if isinstance(val, float) else val
    if isinstance(val, Enum):
        return val.value
    if isinstance(val, str):
        # Scrub credentials and memory addresses
        clean = MEMORY_ADDRESS_REGEX.sub("[ADDR]", val)
        if CREDENTIAL_KEY_PATTERNS.search(clean):
            return "***REDACTED***"
        return clean
    if isinstance(val, Mapping):
        clean_map = {}
        for k in sorted(val.keys(), key=lambda x: str(x)):
            k_str = str(k)
            if k_str in VOLATILE_CONFIG_KEYS:
                continue
            if CREDENTIAL_KEY_PATTERNS.search(k_str):
                clean_map[k_str] = "***REDACTED***"
            else:
                clean_map[k_str] = _canonicalize_value_for_fingerprint(val[k])
        return clean_map
    if isinstance(val, (list, tuple, set, frozenset)):
        items = list(val)
        # If it's a set, sort canonical order
        if isinstance(val, (set, frozenset)):
            items.sort(key=lambda x: str(x))
        return [_canonicalize_value_for_fingerprint(item) for item in items]
    if hasattr(val, "to_dict") and callable(getattr(val, "to_dict")):
        return _canonicalize_value_for_fingerprint(val.to_dict())
    if hasattr(val, "__dict__"):
        return _canonicalize_value_for_fingerprint(vars(val))
    return str(val)


def generate_configuration_fingerprint(config: Any) -> str:
    """Compute a collision-resistant deterministic SHA-256 fingerprint for a configuration.

    Guarantees:
    1. Stable ordering of keys.
    2. Enums, tuples, lists, and dicts handled canonically.
    3. Memory addresses, process IDs, and volatile timestamps stripped.
    4. Secrets and credentials redacted.
    5. Equivalent configurations produce identical fingerprints.
    """
    if config is None:
        canonical = {}
    elif isinstance(config, Mapping):
        canonical = _canonicalize_value_for_fingerprint(config)
    elif hasattr(config, "to_dict") and callable(getattr(config, "to_dict")):
        canonical = _canonicalize_value_for_fingerprint(config.to_dict())
    elif hasattr(config, "__dict__"):
        canonical = _canonicalize_value_for_fingerprint(vars(config))
    else:
        canonical = _canonicalize_value_for_fingerprint(config)

    serialized = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def generate_reproducibility_id(
    operation_type: Union[OperationType, str],
    input_provenance_ids: Sequence[str],
    configuration_fingerprint: str,
    strategy_id: Optional[str] = None,
    strategy_version: Optional[str] = None,
    analysis_version: str = "1.0.0",
    software_version: str = "1.0.0",
    schema_version: str = "1.0.0",
) -> str:
    """Compute the immutable reproducibility identity for an analytical setup.

    The reproducibility ID answers:
    'Does this exact combination of operation, inputs, configuration, and code versions exist?'

    It excludes runtime instance data (timestamps, process IDs, run_id).
    """
    op_type = OperationType.from_string(operation_type).value
    sorted_parents = sorted(set(str(p).strip() for p in input_provenance_ids if str(p).strip()))
    cfg_fp = str(configuration_fingerprint).strip()
    strat_id = str(strategy_id).strip().lower() if strategy_id else ""
    strat_ver = str(strategy_version).strip() if strategy_version else ""

    payload = {
        "operation_type": op_type,
        "input_provenance_ids": sorted_parents,
        "configuration_fingerprint": cfg_fp,
        "strategy_id": strat_id,
        "strategy_version": strat_ver,
        "analysis_version": str(analysis_version).strip(),
        "software_version": str(software_version).strip(),
        "schema_version": str(schema_version).strip(),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return f"repro_{digest}"


@dataclass(frozen=True)
class AnalysisRun:
    """Immutable record of an analytical execution instance.

    Guarantees full auditable linkage to input data provenance, deterministic
    configuration fingerprints, code versions, quality assessments, and generated results.
    """

    run_id: str
    reproducibility_id: str
    operation_type: OperationType
    status: RunStatus
    created_at: float
    configuration_fingerprint: str
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    input_provenance_ids: Tuple[str, ...] = ()
    parent_run_ids: Tuple[str, ...] = ()
    configuration_summary: Dict[str, Any] = field(default_factory=dict)
    strategy_id: Optional[str] = None
    strategy_version: Optional[str] = None
    analysis_version: str = "1.0.0"
    schema_version: str = "1.0.0"
    software_version: str = "1.0.0"
    symbol_universe: Tuple[str, ...] = ()
    timeframe_universe: Tuple[str, ...] = ()
    data_start: Optional[float] = None
    data_end: Optional[float] = None
    input_record_count: int = 0
    quality_assessment_ids: Tuple[str, ...] = ()
    quality_status: Optional[DataQualityStatus] = None
    result_id: Optional[str] = None
    result_fingerprint: Optional[str] = None
    result_type: Optional[str] = None
    execution_metadata: Dict[str, Any] = field(default_factory=dict)
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    cancellation_reason: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate core invariants and freeze mutable fields."""
        if not self.run_id or not isinstance(self.run_id, str) or not self.run_id.strip():
            raise ValidationError("AnalysisRun run_id must be a non-empty string.")
        if not self.reproducibility_id or not isinstance(self.reproducibility_id, str) or not self.reproducibility_id.strip():
            raise ValidationError("AnalysisRun reproducibility_id must be a non-empty string.")
        if not self.configuration_fingerprint or not isinstance(self.configuration_fingerprint, str):
            raise ValidationError("AnalysisRun configuration_fingerprint must be a non-empty string.")

        if not isinstance(self.operation_type, OperationType):
            object.__setattr__(self, "operation_type", OperationType.from_string(self.operation_type))

        if not isinstance(self.status, RunStatus):
            try:
                object.__setattr__(self, "status", RunStatus(str(self.status)))
            except ValueError:
                object.__setattr__(self, "status", RunStatus.INVALID)

        if self.created_at < 0:
            raise ValidationError(f"AnalysisRun created_at timestamp cannot be negative, got {self.created_at}")

        if self.started_at is not None and self.started_at < self.created_at:
            raise ValidationError(
                f"AnalysisRun started_at ({self.started_at}) cannot precede created_at ({self.created_at})"
            )

        if self.completed_at is not None:
            ref_start = self.started_at if self.started_at is not None else self.created_at
            if self.completed_at < ref_start:
                raise ValidationError(
                    f"AnalysisRun completed_at ({self.completed_at}) cannot precede started_at/created_at ({ref_start})"
                )

        if self.input_record_count < 0:
            raise ValidationError(f"AnalysisRun input_record_count cannot be negative, got {self.input_record_count}")

        if self.data_start is not None and self.data_end is not None and self.data_end < self.data_start:
            raise ValidationError(
                f"AnalysisRun data_end ({self.data_end}) cannot be earlier than data_start ({self.data_start})"
            )

        # Freeze and normalize tuples
        object.__setattr__(self, "run_id", self.run_id.strip())
        object.__setattr__(self, "reproducibility_id", self.reproducibility_id.strip())
        object.__setattr__(self, "configuration_fingerprint", self.configuration_fingerprint.strip())
        object.__setattr__(self, "input_provenance_ids", tuple(str(p).strip() for p in self.input_provenance_ids if str(p).strip()))
        object.__setattr__(self, "parent_run_ids", tuple(str(p).strip() for p in self.parent_run_ids if str(p).strip()))
        object.__setattr__(self, "quality_assessment_ids", tuple(str(q).strip() for q in self.quality_assessment_ids if str(q).strip()))

        # Normalize symbols and timeframes
        syms = tuple(sorted(set(str(s).strip().upper() for s in self.symbol_universe if str(s).strip())))
        object.__setattr__(self, "symbol_universe", syms)

        tfs = []
        for tf in self.timeframe_universe:
            try:
                tfs.append(normalize_timeframe(tf))
            except ValidationError:
                tfs.append(str(tf).strip())
        object.__setattr__(self, "timeframe_universe", tuple(sorted(set(tfs))))

        # Sanitize metadata dictionaries
        clean_summary = sanitize_provenance_metadata(self.configuration_summary or {})
        clean_exec = sanitize_provenance_metadata(self.execution_metadata or {})
        object.__setattr__(self, "configuration_summary", clean_summary)
        object.__setattr__(self, "execution_metadata", clean_exec)

    @property
    def duration_seconds(self) -> float:
        """Return total elapsed runtime in seconds, or 0.0 if not yet completed."""
        if self.completed_at is not None and self.started_at is not None:
            return max(0.0, self.completed_at - self.started_at)
        return 0.0

    @property
    def iso_created_at(self) -> str:
        """Return ISO-8601 UTC timestamp of creation."""
        return datetime.fromtimestamp(self.created_at, tz=timezone.utc).isoformat()

    @property
    def iso_completed_at(self) -> Optional[str]:
        """Return ISO-8601 UTC timestamp of completion if available."""
        if self.completed_at is not None:
            return datetime.fromtimestamp(self.completed_at, tz=timezone.utc).isoformat()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert complete run record to JSON-serializable dictionary."""
        return {
            "run_id": self.run_id,
            "reproducibility_id": self.reproducibility_id,
            "operation_type": self.operation_type.value,
            "status": self.status.value,
            "created_at": self.created_at,
            "iso_created_at": self.iso_created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "iso_completed_at": self.iso_completed_at,
            "duration_seconds": round(self.duration_seconds, 4),
            "configuration_fingerprint": self.configuration_fingerprint,
            "configuration_summary": copy.deepcopy(self.configuration_summary),
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "analysis_version": self.analysis_version,
            "schema_version": self.schema_version,
            "software_version": self.software_version,
            "symbol_universe": list(self.symbol_universe),
            "timeframe_universe": list(self.timeframe_universe),
            "data_start": self.data_start,
            "data_end": self.data_end,
            "input_record_count": self.input_record_count,
            "input_provenance_ids": list(self.input_provenance_ids),
            "parent_run_ids": list(self.parent_run_ids),
            "quality_assessment_ids": list(self.quality_assessment_ids),
            "quality_status": self.quality_status.value if self.quality_status else None,
            "result_id": self.result_id,
            "result_fingerprint": self.result_fingerprint,
            "result_type": self.result_type,
            "execution_metadata": copy.deepcopy(self.execution_metadata),
            "error_category": self.error_category,
            "error_message": self.error_message,
            "cancellation_reason": self.cancellation_reason,
        }
