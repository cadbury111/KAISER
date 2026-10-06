"""KAISER — Opportunity Prioritization & Investigation Queue Domain Models.

Provides strongly-typed, immutable data structures for evidence-grounded
opportunity prioritization, deterministic priority states, and investigation queue records.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Decision support & investigation priority only:
   Never outputs BUY, SELL, HOLD, ENTRY, EXIT, or LEVERAGE.
2. Absolutely no order execution fields: Prohibits action, side, order, entry/exit price,
   position size, leverage, stop-loss, take-profit, broker orders, expected profit, or trade probability.
3. No profitability claims: No numeric probability of profit or trade success metrics.
4. Numerical safety: NaN/Infinity values are rejected; exact zero values are preserved.
5. Immutability & Traceability: Every priority record traces back to source candidate, evidence report, and runs.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from enum import Enum
import math
import re
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union

from app.analysis.comparison.metrics import safe_float
from app.analysis.evidence.models import EvidenceStatus
from app.analysis.opportunities.models import (
    ConfidenceState,
    OpportunityCandidate,
    OpportunityState,
)
from app.core.exceptions import ValidationError
from app.market.provenance.models import (
    sanitize_provenance_metadata,
    sanitize_provenance_string,
)
from app.market.quality.status import DataQualityStatus

DEFAULT_MAX_QUEUE_CAPACITY = 100
DEFAULT_MAX_PRIORITY_HISTORY = 100

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
    "expected_profit",
    "profit_probability",
    "trade_probability",
    "win_rate",
    "predicted_pnl",
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
    "profit",
    "profitable",
    "guaranteed",
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
                    f"trade recommendations, execution instructions, or profitability predictions."
                )
            validate_no_prohibited_fields(v, current_path)
    elif isinstance(data, (list, tuple, set, frozenset)):
        for idx, item in enumerate(data):
            current_path = f"{path}[{idx}]"
            validate_no_prohibited_fields(item, current_path)


class PriorityState(str, Enum):
    """Deterministic analytical state of investigation priority.

    Strictly reflects evidence completeness and empirical support for human investigation.
    NEVER indicates a trade recommendation or probability of profit.
    """

    NOT_ELIGIBLE = "NOT_ELIGIBLE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    DATA_QUALITY_LIMITED = "DATA_QUALITY_LIMITED"
    CONFLICTING_EVIDENCE = "CONFLICTING_EVIDENCE"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    NORMAL_PRIORITY = "NORMAL_PRIORITY"
    HIGHER_REVIEW_PRIORITY = "HIGHER_REVIEW_PRIORITY"

    @classmethod
    def from_string(cls, val: Union[str, PriorityState]) -> PriorityState:
        """Parse priority state safely from string or enum."""
        if isinstance(val, PriorityState):
            return val
        if not val or not isinstance(val, str):
            return cls.INSUFFICIENT_EVIDENCE
        clean = val.strip().upper()
        if clean in cls._value2member_map_:
            return cls(clean)
        return cls.INSUFFICIENT_EVIDENCE


@dataclass(frozen=True)
class PriorityVoiceSummary:
    """Concise, deterministic structured speech text for voice synthesis without trading directives."""

    headline: str
    priority_speech: str
    reasons_speech: str
    caveats_speech: str
    full_speech_script: str

    def __post_init__(self) -> None:
        """Enforce strict speech safety: absolutely no trading directives or profitability claims."""
        script_lower = self.full_speech_script.lower()
        for forbidden in FORBIDDEN_VOICE_WORDS:
            # Match whole words to avoid false positives on substrings
            if re.search(r"\b" + re.escape(forbidden) + r"\b", script_lower):
                raise ValidationError(
                    f"Forbidden trading instruction or claim '{forbidden}' detected in voice summary script. "
                    f"KAISER opportunity prioritization provides investigation decision support only."
                )

    def to_dict(self) -> Dict[str, Any]:
        """Convert voice summary to dictionary."""
        return {
            "headline": self.headline,
            "priority_speech": self.priority_speech,
            "reasons_speech": self.reasons_speech,
            "caveats_speech": self.caveats_speech,
            "full_speech_script": self.full_speech_script,
        }


@dataclass(frozen=True)
class OpportunityPriorityRecord:
    """Immutable, evidence-grounded record of human investigation priority for an opportunity candidate.

    Represents a structured analytical determination of where an OpportunityCandidate
    ranks in the investigation queue based entirely on empirical evidence completeness.

    ABSOLUTELY NOT A TRADING RECOMMENDATION OR EXECUTION DIRECTIVE.
    """

    priority_id: str
    candidate_id: str
    symbol: str
    timeframe: str
    investigation_priority: int
    priority_state: PriorityState
    evidence_completeness: float
    evidence_priority_score: float
    data_quality_state: str
    provenance_state: str
    reproducibility_state: str
    conflict_state: str
    uncertainty_state: str
    historical_coverage_state: str
    supporting_evidence_count: int
    conflicting_evidence_count: int
    missing_evidence_count: int
    limitation_count: int
    priority_reasons: Tuple[str, ...]
    deprioritization_reasons: Tuple[str, ...]
    source_report_id: str
    source_analysis_run_ids: Tuple[str, ...]
    reproducibility_id: Optional[str] = None
    generated_at: float = 0.0
    voice_summary: Optional[PriorityVoiceSummary] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    disclaimer: str = (
        "ANALYTICAL INVESTIGATION PRIORITY ONLY: This record prioritizes candidates for human analytical "
        "investigation based on evidence completeness and data integrity. It does NOT rank trades, predict "
        "profitability, or provide execution instructions. Settings.execution_locked remains True."
    )

    def __post_init__(self) -> None:
        """Validate core invariants, numerical safety, and safety guards."""
        if not self.priority_id or not isinstance(self.priority_id, str):
            raise ValidationError("OpportunityPriorityRecord priority_id must be a non-empty string.")
        if not self.candidate_id or not isinstance(self.candidate_id, str):
            raise ValidationError("OpportunityPriorityRecord candidate_id must be a non-empty string.")
        if not self.symbol or not isinstance(self.symbol, str):
            raise ValidationError("OpportunityPriorityRecord symbol must be a non-empty string.")
        if not self.timeframe or not isinstance(self.timeframe, str):
            raise ValidationError("OpportunityPriorityRecord timeframe must be a non-empty string.")
        if not self.source_report_id or not isinstance(self.source_report_id, str):
            raise ValidationError("OpportunityPriorityRecord source_report_id must be a non-empty string.")

        if self.investigation_priority < 1:
            raise ValidationError(
                f"OpportunityPriorityRecord investigation_priority must be a positive integer (>= 1), got {self.investigation_priority}"
            )

        if self.generated_at < 0:
            raise ValidationError(
                f"OpportunityPriorityRecord generated_at timestamp cannot be negative: {self.generated_at}"
            )

        # Numerical safety: validate finite, non-NaN, non-Inf scores bounded [0.0, 1.0]
        if math.isnan(self.evidence_completeness) or math.isinf(self.evidence_completeness):
            raise ValidationError(
                f"OpportunityPriorityRecord evidence_completeness cannot be NaN or Infinity, got {self.evidence_completeness}"
            )
        if not (0.0 <= self.evidence_completeness <= 1.0):
            raise ValidationError(
                f"OpportunityPriorityRecord evidence_completeness must be bounded between 0.0 and 1.0, got {self.evidence_completeness}"
            )

        if math.isnan(self.evidence_priority_score) or math.isinf(self.evidence_priority_score):
            raise ValidationError(
                f"OpportunityPriorityRecord evidence_priority_score cannot be NaN or Infinity, got {self.evidence_priority_score}"
            )
        if not (0.0 <= self.evidence_priority_score <= 1.0):
            raise ValidationError(
                f"OpportunityPriorityRecord evidence_priority_score must be bounded between 0.0 and 1.0, got {self.evidence_priority_score}"
            )

        # Non-negative counts
        if self.supporting_evidence_count < 0:
            raise ValidationError(f"supporting_evidence_count cannot be negative: {self.supporting_evidence_count}")
        if self.conflicting_evidence_count < 0:
            raise ValidationError(f"conflicting_evidence_count cannot be negative: {self.conflicting_evidence_count}")
        if self.missing_evidence_count < 0:
            raise ValidationError(f"missing_evidence_count cannot be negative: {self.missing_evidence_count}")
        if self.limitation_count < 0:
            raise ValidationError(f"limitation_count cannot be negative: {self.limitation_count}")

        # Enforce PriorityState enum
        if not isinstance(self.priority_state, PriorityState):
            object.__setattr__(self, "priority_state", PriorityState.from_string(self.priority_state))

        # SAFETY GUARD: Prohibit execution and recommendation fields in metadata
        validate_no_prohibited_fields(self.metadata, path="metadata")

        # Freeze collections
        object.__setattr__(
            self,
            "priority_reasons",
            tuple(str(r).strip() for r in self.priority_reasons if str(r).strip()),
        )
        object.__setattr__(
            self,
            "deprioritization_reasons",
            tuple(str(r).strip() for r in self.deprioritization_reasons if str(r).strip()),
        )
        object.__setattr__(
            self,
            "source_analysis_run_ids",
            tuple(str(rid).strip() for rid in self.source_analysis_run_ids if str(rid).strip()),
        )

        # Sanitize metadata
        clean_meta = sanitize_provenance_metadata(self.metadata or {})
        object.__setattr__(self, "metadata", clean_meta)

    @property
    def is_eligible(self) -> bool:
        """True if the candidate is eligible for investigation."""
        return self.priority_state != PriorityState.NOT_ELIGIBLE

    @property
    def requires_review(self) -> bool:
        """True if investigation requires explicit caution/review."""
        return self.priority_state == PriorityState.REVIEW_REQUIRED

    @property
    def has_conflicts(self) -> bool:
        """True if conflicting evidence was detected."""
        return self.conflicting_evidence_count > 0 or self.priority_state == PriorityState.CONFLICTING_EVIDENCE

    def to_dict(self) -> Dict[str, Any]:
        """Convert priority record to dictionary."""
        return {
            "priority_id": self.priority_id,
            "candidate_id": self.candidate_id,
            "symbol": sanitize_provenance_string(self.symbol),
            "timeframe": self.timeframe,
            "investigation_priority": self.investigation_priority,
            "priority_state": self.priority_state.value,
            "evidence_completeness": self.evidence_completeness,
            "evidence_priority_score": self.evidence_priority_score,
            "data_quality_state": self.data_quality_state,
            "provenance_state": self.provenance_state,
            "reproducibility_state": self.reproducibility_state,
            "conflict_state": self.conflict_state,
            "uncertainty_state": self.uncertainty_state,
            "historical_coverage_state": self.historical_coverage_state,
            "supporting_evidence_count": self.supporting_evidence_count,
            "conflicting_evidence_count": self.conflicting_evidence_count,
            "missing_evidence_count": self.missing_evidence_count,
            "limitation_count": self.limitation_count,
            "priority_reasons": list(self.priority_reasons),
            "deprioritization_reasons": list(self.deprioritization_reasons),
            "source_report_id": self.source_report_id,
            "source_analysis_run_ids": list(self.source_analysis_run_ids),
            "reproducibility_id": self.reproducibility_id,
            "generated_at": self.generated_at,
            "voice_summary": self.voice_summary.to_dict() if self.voice_summary else None,
            "metadata": dict(self.metadata),
            "disclaimer": self.disclaimer,
        }
