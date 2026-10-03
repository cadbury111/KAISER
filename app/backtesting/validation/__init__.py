"""KAISER — Walk-Forward Validation & Out-of-Sample Evaluation Package.

Provides deterministic, chronological in-sample training and out-of-sample testing
across sequential market partitions to evaluate strategy stability and reduce overfitting.

SAFETY INVARIANTS:
1. Research and evaluation only: Evaluates historical strategy behavior. Never connects to a broker.
2. Anti-leakage: Strict chronological segregation between training and test periods.
3. No profitability claims: Walk-forward validation cannot eliminate curve-fitting or guarantee future returns.
"""

from app.backtesting.validation.models import (
    OutOfSampleSummary,
    ValidationWarning,
    ValidationWindow,
    ValidationWindowResult,
    WalkForwardConfig,
    WalkForwardResult,
    WalkForwardStatus,
    WindowMode,
)
from app.backtesting.validation.validator import WalkForwardValidator
from app.backtesting.validation.windows import (
    generate_walk_forward_windows,
    validate_window_continuity,
)

__all__ = [
    "WalkForwardValidator",
    "WalkForwardConfig",
    "WalkForwardResult",
    "WalkForwardStatus",
    "WindowMode",
    "ValidationWindow",
    "ValidationWindowResult",
    "OutOfSampleSummary",
    "ValidationWarning",
    "generate_walk_forward_windows",
    "validate_window_continuity",
]
