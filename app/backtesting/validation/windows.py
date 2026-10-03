"""KAISER — Chronological Window Generation & Boundary Management.

Constructs deterministic, strictly segregated in-sample and out-of-sample partitions
for walk-forward strategy validation without forward-looking data leakage.

ANTI-LEAKAGE INVARIANTS:
1. Sequential ordering: Windows are strictly generated in ascending chronological time.
2. Non-overlap: Training data ends before test data begins in every window.
3. Left-closed, right-open intervals: [start, end) prevents boundary double-counting.
4. Indicator warm-up safety: Warm-up bars are strictly historical (t < test_start).
"""

from __future__ import annotations

from typing import List, Sequence

from app.analysis.models import TIMEFRAME_SECONDS
from app.backtesting.validation.models import (
    ValidationWarning,
    ValidationWindow,
    WalkForwardConfig,
    WindowMode,
)
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger

logger = get_logger("backtesting.validation.windows")


def generate_walk_forward_windows(config: WalkForwardConfig) -> List[ValidationWindow]:
    """Generate sequential chronological validation windows from a WalkForwardConfig.

    Args:
        config: Validated WalkForwardConfig instance.

    Returns:
        List[ValidationWindow]: Chronologically ordered validation windows.

    Raises:
        ValidationError: If config parameters produce an impossible window partition.
    """
    windows: List[ValidationWindow] = []
    interval_sec = TIMEFRAME_SECONDS.get(config.timeframe, 60.0)

    # Compute duration of one complete single window partition
    total_window_span = (
        config.train_duration_seconds
        + config.validation_duration_seconds
        + config.test_duration_seconds
    )

    if (config.end_time - config.start_time) < total_window_span:
        raise ValidationError(
            f"Configured dataset range ({config.end_time - config.start_time:.1f}s) "
            f"cannot accommodate a single validation window ({total_window_span:.1f}s)."
        )

    step = config.step_duration_seconds or config.test_duration_seconds
    if step <= 0:
        raise ValidationError("step_duration_seconds must be positive.")

    current_cursor = config.start_time
    window_counter = 0

    while (current_cursor + total_window_span) <= (config.end_time + 0.001):
        # 1. Training (In-Sample) Boundaries
        if config.window_mode == WindowMode.EXPANDING.value:
            train_start = config.start_time  # Anchored expanding start
        else:
            train_start = current_cursor      # Rolling window start

        train_end = current_cursor + config.train_duration_seconds

        # 2. Optional Validation Boundaries
        if config.validation_duration_seconds > 0:
            val_start = train_end
            val_end = val_start + config.validation_duration_seconds
            test_start = val_end
        else:
            val_start = None
            val_end = None
            test_start = train_end

        # 3. Testing (Out-of-Sample) Boundaries
        test_end = test_start + config.test_duration_seconds

        # Guard against floating-point overrun past dataset end
        if test_end > (config.end_time + 0.001):
            break

        # 4. Optional Indicator Warm-up (Strictly preceding test_start)
        warmup_start = None
        if config.warmup_bars > 0:
            warmup_span = config.warmup_bars * interval_sec
            warmup_start = max(config.start_time, test_start - warmup_span)

        window = ValidationWindow(
            window_id=window_counter,
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end,
            val_start=val_start,
            val_end=val_end,
            warmup_start=warmup_start,
        )
        windows.append(window)
        window_counter += 1

        # Advance cursor by rolling step
        current_cursor += step

    logger.info(
        "Generated %d walk-forward validation windows for %s (%s, mode=%s, step=%.1fs)",
        len(windows),
        config.symbol,
        config.timeframe,
        config.window_mode,
        step,
    )

    return windows


def validate_window_continuity(
    windows: Sequence[ValidationWindow],
    config: WalkForwardConfig,
) -> List[ValidationWarning]:
    """Inspect generated windows for continuity, sample adequacy, and gaps."""
    warnings: List[ValidationWarning] = []

    if len(windows) == 0:
        warnings.append(
            ValidationWarning(
                code="ZERO_WINDOWS",
                message="No validation windows could be generated from the given date range and durations.",
                severity="CRITICAL",
            )
        )
        return warnings

    if len(windows) < 3:
        warnings.append(
            ValidationWarning(
                code="FEW_WINDOWS",
                message=(
                    f"Only {len(windows)} walk-forward windows generated. "
                    "Results with fewer than 3 windows carry elevated small-sample bias."
                ),
                severity="WARNING",
            )
        )

    # Check for gaps or overlaps between successive test periods
    for i in range(len(windows) - 1):
        w_curr = windows[i]
        w_next = windows[i + 1]

        diff = w_next.test_start - w_curr.test_end
        if diff > 1.0:  # Gap between OOS periods
            warnings.append(
                ValidationWarning(
                    code="OOS_GAP",
                    message=(
                        f"Gap of {diff:.1f}s detected between window {w_curr.window_id} test end "
                        f"and window {w_next.window_id} test start."
                    ),
                    window_id=w_next.window_id,
                    severity="INFO",
                )
            )
        elif diff < -1.0:  # Overlapping OOS periods
            warnings.append(
                ValidationWarning(
                    code="OOS_OVERLAP",
                    message=(
                        f"Overlap of {abs(diff):.1f}s between window {w_curr.window_id} test end "
                        f"and window {w_next.window_id} test start."
                    ),
                    window_id=w_next.window_id,
                    severity="WARNING",
                )
            )

    return warnings
