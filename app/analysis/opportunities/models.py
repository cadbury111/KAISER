"""KAISER — Opportunity Analysis & Decision-Support Domain Models.

Provides strongly-typed, immutable data structures for evidence-grounded
opportunity candidates, deterministic state machines, and analytical assessments.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support only: NEVER outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE.
2. Absolutely no order execution fields: Prohibits action, side, order, entry/exit price,
   position size, leverage, stop-loss, take-profit, or broker orders.
3. No profitability claims: No numeric probability of profit or fake confidence percentage.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
5. Immutability & Traceability: Every candidate references originating evidence report and runs.
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
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.comparison.metrics import safe_float
from app.analysis.evidence.models import (
    EvidenceItem,
    EvidenceStatus,
    KeyObservation,
    ObservationType,
    SampleCoverage,
)
from app.core.exceptions import ValidationError
from app.market.provenance.models import (
    CREDENTIAL_KEY_PATTERNS,
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)
from app.market.quality.status import DataQualityStatus

DEFAULT_MAX_OPPORTUNITY_HISTORY = 100
DEFAULT_MAX_SUPPORTING_REFS = 100
DEFAULT_MAX_CONFLICTING_REFS = 100
DEFAULT_MAX_UNCERTAINTY_REFS = 100
DEFAULT_MAX_LIMITATION_REFS = 50

# Strict blacklist of execution, trading, and recommendation fields
PROHIBITED_RECOMMENDATION_FIELDS: frozenset[str] = frozenset({
    "action",
    "side",
    "order",
    "entry",
    "exit",
    "quantity",
    "position_size",
    "leverage",
    "stop_loss",
    "take_profit",
    "broker_order",
    "execution_instruction",
    "target_price",
    "entry_price",
    "exit_price",
    "order_type",
    "trade_recommendation",
    "trade_signal",
    "buy_signal",
    "sell_signal",
})

FORBIDDEN_VOICE_WORDS: frozenset[str] = frozenset({
    "buy",
    "sell",
    "hold",
    "enter",
    "exit",
    "long",
    "short",
    "position size",
    "leverage",
    "order",
    "execute",
})


def validate_no_prohibited_fields(data: Any, path: str = "") -> None:
    """Recursively validate that no trading or execution recommendation fields exist in a payload.

    Raises:
        ValidationError: If any prohibited field or recommendation keyword is detected.
    """
    if isinstance(data, Mapping):
        for k, v in data.items():
            field_name = str(k).strip().lower()
            current_path = f"{path}.{field_name}" if path else field_name
            if field_name in PROHIBITED_RECOMMENDATION_FIELDS:
                raise ValidationError(
                    f"Prohibited trade recommendation field detected at '{current_path}'. "
                    f"KAISER is an evidence-grounded decision support system only and prohibits "
                    f"trade recommendations, execution instructions, or order fields."
                )
            validate_no_prohibited_fields(v, current_path)
    elif isinstance(data, (list, tuple, set, frozenset)):
        for idx, item in enumerate(data):
            current_path = f"{path}[{idx}]"
            validate_no_prohibited_fields(item, current_path)


def parse_data_quality_status(val: Any) -> Optional[DataQualityStatus]:
    """Parse DataQualityStatus safely from string or enum."""
    if isinstance(val, DataQualityStatus):
        return val
    if not val or not isinstance(val, str):
        return None
    clean = val.strip().upper()
    try:
        return DataQualityStatus(clean)
    except ValueError:
        return None


class OpportunityState(str, Enum):

    """Deterministic analytical state of an opportunity candidate.

    Strictly reflects evidence availability, data quality, and conflict status.
    NEVER indicates a trade recommendation or probability of profit.
    """

    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    DATA_QUALITY_LIMITED = "DATA_QUALITY_LIMITED"
    EVIDENCE_CONFLICTING = "EVIDENCE_CONFLICTING"
    EVIDENCE_PARTIAL = "EVIDENCE_PARTIAL"
    ANALYTICALLY_SUPPORTED = "ANALYTICALLY_SUPPORTED"
    REQUIRES_REVIEW = "REQUIRES_REVIEW"
    INVALIDATED = "INVALIDATED"

    @classmethod
    def from_string(cls, val: Union[str, OpportunityState]) -> OpportunityState:
        """Parse opportunity state safely from string or enum."""
        if isinstance(val, OpportunityState):
            return val
        if not val or not isinstance(val, str):
            return cls.INSUFFICIENT_EVIDENCE
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.INSUFFICIENT_EVIDENCE


class ConfidenceState(str, Enum):
    """Categorical audit confidence in the analytical evidence foundation.

    Reflects integrity of quality, provenance, reproducibility, and sample coverage.
    Strictly NOT a trading success probability.
    """

    HIGH = "HIGH"
    MODERATE = "MODERATE"
    LOW = "LOW"
    UNVERIFIED = "UNVERIFIED"

    @classmethod
    def from_string(cls, val: Union[str, ConfidenceState]) -> ConfidenceState:
        """Parse confidence state safely from string or enum."""
        if isinstance(val, ConfidenceState):
            return val
        if not val or not isinstance(val, str):
            return cls.UNVERIFIED
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.UNVERIFIED


@dataclass(frozen=True)
class ObservationWindow:
    """Temporal and sample bounds of the analyzed market observation window."""

    start_time: Optional[float] = None
    end_time: Optional[float] = None
    record_count: int = 0
    duration_seconds: Optional[float] = None

    def __post_init__(self) -> None:
        """Validate observation window parameters."""
        if self.start_time is not None:
            if math.isnan(self.start_time) or math.isinf(self.start_time):
                raise ValidationError(f"ObservationWindow start_time cannot be NaN or Infinity, got {self.start_time}")
            if self.start_time < 0:
                raise ValidationError(f"ObservationWindow start_time cannot be negative: {self.start_time}")
        if self.end_time is not None:
            if math.isnan(self.end_time) or math.isinf(self.end_time):
                raise ValidationError(f"ObservationWindow end_time cannot be NaN or Infinity, got {self.end_time}")
            if self.end_time < 0:
                raise ValidationError(f"ObservationWindow end_time cannot be negative: {self.end_time}")
        if self.record_count < 0:
            raise ValidationError(f"ObservationWindow record_count cannot be negative: {self.record_count}")
        if self.duration_seconds is not None:
            if math.isnan(self.duration_seconds) or math.isinf(self.duration_seconds):
                raise ValidationError(f"ObservationWindow duration_seconds cannot be NaN or Infinity, got {self.duration_seconds}")
            if self.duration_seconds < 0:
                raise ValidationError(f"ObservationWindow duration_seconds cannot be negative: {self.duration_seconds}")


    def to_dict(self) -> Dict[str, Any]:
        """Convert observation window to dictionary."""
        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "record_count": self.record_count,
            "duration_seconds": self.duration_seconds,
        }


@dataclass(frozen=True)
class OpportunityVoiceSummary:
    """Concise, deterministic structured speech text for voice synthesis without trading instructions."""

    headline: str
    state_speech: str
    quality_speech: str
    coverage_speech: str
    review_speech: str
    full_speech_script: str

    def __post_init__(self) -> None:
        """Enforce strict speech safety: absolutely no trading directives."""
        script_lower = self.full_speech_script.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            # Match whole words to avoid false positives on substrings
            if re.search(r"\b" + re.escape(forbidden) + r"\b", script_lower):
                raise ValidationError(
                    f"Forbidden trading instruction '{forbidden}' detected in voice summary script. "
                    f"KAISER opportunity assessment provides decision support only."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to dictionary."""
        return {
            "headline": self.headline,
            "state_speech": self.state_speech,
            "quality_speech": self.quality_speech,
            "coverage_speech": self.coverage_speech,
            "review_speech": self.review_speech,
            "full_speech_script": self.full_speech_script,
        }


@dataclass(frozen=True)
class OpportunityCandidate:
    """Immutable, evidence-grounded analytical candidate for decision support.

    Represents a structured assessment of whether an analyzed market situation
    has sufficient evidence to warrant further human investigation.

    ABSOLUTELY NOT A TRADE RECOMMENDATION OR EXECUTION DIRECTIVE.
    """

    candidate_id: str
    symbol: str
    timeframe: str
    assessment_state: OpportunityState
    evidence_status: EvidenceStatus
    evidence_completeness_score: float
    confidence_state: ConfidenceState
    source_evidence_report_id: str
    generated_at: float
    data_quality_status: Optional[DataQualityStatus] = None
    provenance_status: str = "UNVERIFIED"
    reproducibility_status: str = "UNVERIFIED"
    observation_window: Optional[ObservationWindow] = None
    sample_coverage: Optional[SampleCoverage] = None
    supporting_evidence_refs: Tuple[str, ...] = ()
    conflicting_evidence_refs: Tuple[str, ...] = ()
    uncertainty_refs: Tuple[str, ...] = ()
    limitation_refs: Tuple[str, ...] = ()
    key_observations: Tuple[str, ...] = ()
    invalidation_conditions: Tuple[str, ...] = ()
    source_analysis_run_ids: Tuple[str, ...] = ()
    reproducibility_id: Optional[str] = None
    evidence_completeness_breakdown: Dict[str, float] = field(default_factory=dict)
    multi_timeframe_alignment: Optional[str] = None
    voice_summary: Optional[OpportunityVoiceSummary] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    disclaimer: str = (
        "ANALYTICAL OPPORTUNITY ASSESSMENT ONLY: This assessment reflects evidence completeness and "
        "structural observations for human investigation. It does NOT constitute a trade recommendation, "
        "order directive, execution instruction, or profitability prediction. Settings.execution_locked remains True."
    )

    def __post_init__(self) -> None:
        """Validate core invariants, numerical bounds, and safety guards."""
        if not self.candidate_id or not isinstance(self.candidate_id, str):
            raise ValidationError("OpportunityCandidate candidate_id must be a non-empty string.")
        if not self.symbol or not isinstance(self.symbol, str):
            raise ValidationError("OpportunityCandidate symbol must be a non-empty string.")
        if not self.timeframe or not isinstance(self.timeframe, str):
            raise ValidationError("OpportunityCandidate timeframe must be a non-empty string.")
        if not self.source_evidence_report_id or not isinstance(self.source_evidence_report_id, str):
            raise ValidationError("OpportunityCandidate source_evidence_report_id must be a non-empty string.")
        if self.generated_at < 0:
            raise ValidationError(f"OpportunityCandidate generated_at timestamp cannot be negative: {self.generated_at}")

        # Validate numerical score: finite, non-NaN, bounded [0.0, 1.0]
        if math.isnan(self.evidence_completeness_score) or math.isinf(self.evidence_completeness_score):
            raise ValidationError(
                f"OpportunityCandidate evidence_completeness_score cannot be NaN or Infinity, got {self.evidence_completeness_score}"
            )
        if not (0.0 <= self.evidence_completeness_score <= 1.0):
            raise ValidationError(
                f"OpportunityCandidate evidence_completeness_score must be bounded between 0.0 and 1.0, got {self.evidence_completeness_score}"
            )

        # Enforce enums
        if not isinstance(self.assessment_state, OpportunityState):
            object.__setattr__(self, "assessment_state", OpportunityState.from_string(self.assessment_state))
        if not isinstance(self.evidence_status, EvidenceStatus):
            object.__setattr__(self, "evidence_status", EvidenceStatus.from_string(self.evidence_status))
        if not isinstance(self.confidence_state, ConfidenceState):
            object.__setattr__(self, "confidence_state", ConfidenceState.from_string(self.confidence_state))
        if self.data_quality_status and not isinstance(self.data_quality_status, DataQualityStatus):
            object.__setattr__(self, "data_quality_status", parse_data_quality_status(self.data_quality_status))

        # SAFETY GUARD: Prohibit execution and recommendation fields in metadata and kwargs
        validate_no_prohibited_fields(self.metadata, path="metadata")
        validate_no_prohibited_fields(self.evidence_completeness_breakdown, path="breakdown")

        # Freeze collections
        object.__setattr__(self, "supporting_evidence_refs", tuple(str(r).strip() for r in self.supporting_evidence_refs if str(r).strip()))
        object.__setattr__(self, "conflicting_evidence_refs", tuple(str(r).strip() for r in self.conflicting_evidence_refs if str(r).strip()))
        object.__setattr__(self, "uncertainty_refs", tuple(str(r).strip() for r in self.uncertainty_refs if str(r).strip()))
        object.__setattr__(self, "limitation_refs", tuple(str(r).strip() for r in self.limitation_refs if str(r).strip()))
        object.__setattr__(self, "key_observations", tuple(str(o).strip() for o in self.key_observations if str(o).strip()))
        object.__setattr__(self, "invalidation_conditions", tuple(str(c).strip() for c in self.invalidation_conditions if str(c).strip()))
        object.__setattr__(self, "source_analysis_run_ids", tuple(str(rid).strip() for rid in self.source_analysis_run_ids if str(rid).strip()))

        # Sanitize metadata
        clean_meta = sanitize_provenance_metadata(self.metadata or {})
        clean_breakdown = {k: safe_float(v) for k, v in (self.evidence_completeness_breakdown or {}).items() if safe_float(v) is not None}
        object.__setattr__(self, "metadata", clean_meta)
        object.__setattr__(self, "evidence_completeness_breakdown", clean_breakdown)

    @property
    def is_analytically_supported(self) -> bool:
        """True if the assessment state is ANALYTICALLY_SUPPORTED."""
        return self.assessment_state == OpportunityState.ANALYTICALLY_SUPPORTED

    @property
    def has_conflicts(self) -> bool:
        """True if any conflicting evidence references exist."""
        return len(self.conflicting_evidence_refs) > 0 or self.assessment_state == OpportunityState.EVIDENCE_CONFLICTING

    @property
    def is_invalidated(self) -> bool:
        """True if the candidate was invalidated."""
        return self.assessment_state == OpportunityState.INVALIDATED

    def to_dict(self) -> Dict[str, Any]:
        """Convert opportunity candidate to dictionary."""
        return {
            "candidate_id": self.candidate_id,
            "symbol": sanitize_provenance_string(self.symbol),
            "timeframe": self.timeframe,
            "assessment_state": self.assessment_state.value,
            "evidence_status": self.evidence_status.value,
            "evidence_completeness_score": self.evidence_completeness_score,
            "confidence_state": self.confidence_state.value,
            "data_quality_status": self.data_quality_status.value if self.data_quality_status else None,
            "provenance_status": self.provenance_status,
            "reproducibility_status": self.reproducibility_status,
            "source_evidence_report_id": self.source_evidence_report_id,
            "generated_at": self.generated_at,
            "observation_window": self.observation_window.to_dict() if self.observation_window else None,
            "sample_coverage": self.sample_coverage.to_dict() if self.sample_coverage else None,
            "supporting_evidence_refs": list(self.supporting_evidence_refs),
            "conflicting_evidence_refs": list(self.conflicting_evidence_refs),
            "uncertainty_refs": list(self.uncertainty_refs),
            "limitation_refs": list(self.limitation_refs),
            "key_observations": list(self.key_observations),
            "invalidation_conditions": list(self.invalidation_conditions),
            "source_analysis_run_ids": list(self.source_analysis_run_ids),
            "reproducibility_id": self.reproducibility_id,
            "evidence_completeness_breakdown": dict(self.evidence_completeness_breakdown),
            "multi_timeframe_alignment": self.multi_timeframe_alignment,
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "metadata": dict(self.metadata),
            "disclaimer": self.disclaimer,
        }
