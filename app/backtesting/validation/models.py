"""KAISER — Walk-Forward Validation & Out-of-Sample Evaluation Domain Models.

Provides strongly typed, immutable data structures for chronological window generation,
in-sample/out-of-sample evaluation outcomes, structured validation warnings, and
aggregated out-of-sample performance telemetry.

SAFETY INVARIANTS:
1. Research and evaluation only: Evaluates historical strategy behavior. Never places real trades.
2. Anti-leakage: Training and test periods are strictly chronological and segregated.
3. No profitability claims: Walk-forward validation reduces overfitting risk but cannot
   eliminate it or guarantee future performance.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from app.analysis.models import normalize_timeframe
from app.backtesting.models import (
    BacktestConfig,
    BacktestResult,
    ExecutionModel,
    FeeModel,
    PerformanceMetrics,
    PositionSizeType,
)
from app.core.exceptions import ValidationError


class WalkForwardStatus(str, Enum):
    """Lifecycle status of a walk-forward validation session."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class WindowMode(str, Enum):
    """Window shifting methodology across chronological periods."""

    ROLLING = "rolling"       # In-sample window rolls forward with constant duration
    EXPANDING = "expanding"   # In-sample window start remains anchored, expanding over time


@dataclass(frozen=True)
class ValidationWarning:
    """Structured warning emitted during walk-forward validation.

    Attributes:
        code: Machine-readable warning category.
        message: Explanatory diagnostic text.
        window_id: Optional ID of the window where the warning occurred.
        severity: Severity classification ('INFO', 'WARNING', 'CRITICAL').
    """

    code: str
    message: str
    window_id: Optional[int] = None
    severity: str = "WARNING"

    def to_dict(self) -> Dict[str, Any]:
        """Convert warning to dictionary representation."""
        return {
            "code": self.code,
            "message": self.message,
            "window_id": self.window_id,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class ValidationWindow:
    """Explicit chronological partition defining in-sample and out-of-sample boundaries.

    ANTI-LEAKAGE INVARIANTS:
    1. Chronological order: train_start < train_end <= test_start < test_end.
    2. Strict non-overlap: Training data ends before test data begins.
    3. Boundary semantics: A candle at timestamp T belongs to a partition if
       start <= T < end (left-closed, right-open).

    Attributes:
        window_id: Zero-indexed sequential window counter.
        train_start: POSIX UTC start of in-sample training period.
        train_end: POSIX UTC end of in-sample training period.
        test_start: POSIX UTC start of out-of-sample testing period.
        test_end: POSIX UTC end of out-of-sample testing period.
        val_start: Optional validation period start (if used).
        val_end: Optional validation period end (if used).
        warmup_start: Optional indicator warm-up start (preceding test_start).
    """

    window_id: int
    train_start: float
    train_end: float
    test_start: float
    test_end: float
    val_start: Optional[float] = None
    val_end: Optional[float] = None
    warmup_start: Optional[float] = None

    def __post_init__(self) -> None:
        if self.train_start <= 0 or self.train_end <= 0 or self.test_start <= 0 or self.test_end <= 0:
            raise ValidationError("All window boundary timestamps must be strictly positive floats.")

        if self.train_start >= self.train_end:
            raise ValidationError(
                f"ValidationWindow [{self.window_id}]: train_start ({self.train_start}) "
                f"must be strictly less than train_end ({self.train_end})"
            )

        if self.test_start >= self.test_end:
            raise ValidationError(
                f"ValidationWindow [{self.window_id}]: test_start ({self.test_start}) "
                f"must be strictly less than test_end ({self.test_end})"
            )

        if self.train_end > self.test_start:
            raise ValidationError(
                f"ValidationWindow [{self.window_id}] data leakage violation: "
                f"train_end ({self.train_end}) cannot be greater than test_start ({self.test_start})"
            )

        if self.val_start is not None and self.val_end is not None:
            if self.train_end > self.val_start:
                raise ValidationError("train_end cannot exceed val_start.")
            if self.val_start >= self.val_end:
                raise ValidationError("val_start must be less than val_end.")
            if self.val_end > self.test_start:
                raise ValidationError("val_end cannot exceed test_start.")

        if self.warmup_start is not None and self.warmup_start > self.test_start:
            raise ValidationError("warmup_start cannot be after test_start.")

    @property
    def train_duration(self) -> float:
        """In-sample training duration in seconds."""
        return self.train_end - self.train_start

    @property
    def test_duration(self) -> float:
        """Out-of-sample testing duration in seconds."""
        return self.test_end - self.test_start

    def is_in_sample(self, timestamp: float) -> bool:
        """Check if a timestamp falls within the training period."""
        return self.train_start <= timestamp < self.train_end

    def is_out_of_sample(self, timestamp: float) -> bool:
        """Check if a timestamp falls within the out-of-sample test period."""
        return self.test_start <= timestamp < self.test_end

    def to_dict(self) -> Dict[str, Any]:
        """Serialize window boundaries to dictionary."""
        return {
            "window_id": self.window_id,
            "train_start": self.train_start,
            "train_end": self.train_end,
            "train_duration_sec": round(self.train_duration, 1),
            "test_start": self.test_start,
            "test_end": self.test_end,
            "test_duration_sec": round(self.test_duration, 1),
            "val_start": self.val_start,
            "val_end": self.val_end,
            "warmup_start": self.warmup_start,
        }


@dataclass(frozen=True)
class WalkForwardConfig:
    """Configuration parameters controlling a walk-forward validation execution.

    Attributes:
        symbol: Ticker symbol.
        timeframe: Bar timeframe ('1m', '5m', '15m', '1h', '1D').
        start_time: Absolute start timestamp for dataset.
        end_time: Absolute end timestamp for dataset.
        train_duration_seconds: In-sample training window length in seconds.
        test_duration_seconds: Out-of-sample evaluation window length in seconds.
        step_duration_seconds: Shift interval per window (defaults to test_duration_seconds).
        validation_duration_seconds: Optional buffer duration between train and test.
        window_mode: 'rolling' (fixed train length) or 'expanding' (anchored start).
        min_in_sample_bars: Minimum bars required in training window to consider it eligible.
        min_out_of_sample_bars: Minimum bars required in test window.
        warmup_bars: Optional indicator warm-up bars preceding test start.
        initial_cash: Starting virtual cash for each window (or starting capital).
        position_size_type: 'fixed_quantity', 'fixed_cash', or 'percent_equity'.
        position_size_value: Size value.
        fee_model: 'percentage' or 'fixed'.
        fee_rate: Percentage commission rate.
        fixed_fee: Dollar fee per trade.
        slippage_bps: Basis points slippage assumption.
        half_spread_bps: Half-spread assumption in basis points.
        execution_model: 'next_bar_open' (default anti-look-ahead) or 'current_bar_close'.
        reinvest_profits: If True, carry forward ending cash from one window to the next.
        validation_id: Optional unique run identifier.
        correlation_id: Tracing correlation ID.
    """

    symbol: str
    timeframe: str
    start_time: float
    end_time: float
    train_duration_seconds: float
    test_duration_seconds: float
    step_duration_seconds: Optional[float] = None
    validation_duration_seconds: float = 0.0
    window_mode: str = "rolling"
    min_in_sample_bars: int = 20
    min_out_of_sample_bars: int = 10
    warmup_bars: int = 0
    initial_cash: float = 100_000.0
    position_size_type: str = "fixed_quantity"
    position_size_value: float = 10.0
    fee_model: str = "percentage"
    fee_rate: float = 0.0005
    fixed_fee: float = 0.0
    slippage_bps: float = 2.0
    half_spread_bps: float = 1.0
    execution_model: str = "next_bar_open"
    reinvest_profits: bool = False
    validation_id: Optional[str] = None
    correlation_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("WalkForwardConfig symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        tf = normalize_timeframe(self.timeframe)
        object.__setattr__(self, "timeframe", tf)

        if self.start_time <= 0:
            raise ValidationError(f"WalkForwardConfig start_time must be positive, got {self.start_time}")
        if self.end_time <= 0:
            raise ValidationError(f"WalkForwardConfig end_time must be positive, got {self.end_time}")
        if self.end_time < self.start_time:
            raise ValidationError(
                f"WalkForwardConfig end_time ({self.end_time}) cannot be earlier than start_time ({self.start_time})"
            )

        if self.train_duration_seconds <= 0:
            raise ValidationError("WalkForwardConfig train_duration_seconds must be strictly positive.")
        if self.test_duration_seconds <= 0:
            raise ValidationError("WalkForwardConfig test_duration_seconds must be strictly positive.")

        # Default step to test duration (creating contiguous, non-overlapping out-of-sample segments)
        if self.step_duration_seconds is None or self.step_duration_seconds <= 0:
            object.__setattr__(self, "step_duration_seconds", self.test_duration_seconds)

        total_min_required = (
            self.train_duration_seconds + self.validation_duration_seconds + self.test_duration_seconds
        )
        total_span = self.end_time - self.start_time
        if total_span < total_min_required:
            raise ValidationError(
                f"Total time range ({total_span:.1f}s) is smaller than one full validation window "
                f"({total_min_required:.1f}s = train:{self.train_duration_seconds}s + val:{self.validation_duration_seconds}s + test:{self.test_duration_seconds}s)"
            )

        wm = self.window_mode.strip().lower()
        if wm not in [m.value for m in WindowMode]:
            raise ValidationError(f"Invalid window_mode '{self.window_mode}'. Allowed: {[m.value for m in WindowMode]}")
        object.__setattr__(self, "window_mode", wm)

        if self.min_in_sample_bars < 1:
            raise ValidationError("min_in_sample_bars must be at least 1.")
        if self.min_out_of_sample_bars < 1:
            raise ValidationError("min_out_of_sample_bars must be at least 1.")
        if self.warmup_bars < 0:
            raise ValidationError("warmup_bars cannot be negative.")

        if self.initial_cash <= 0:
            raise ValidationError("initial_cash must be strictly positive.")

        pst = self.position_size_type.strip().lower()
        if pst not in [p.value for p in PositionSizeType]:
            raise ValidationError(f"Invalid position_size_type: {self.position_size_type}")
        object.__setattr__(self, "position_size_type", pst)

        fm = self.fee_model.strip().lower()
        if fm not in [f.value for f in FeeModel]:
            raise ValidationError(f"Invalid fee_model: {self.fee_model}")
        object.__setattr__(self, "fee_model", fm)

        em = self.execution_model.strip().lower()
        if em not in [e.value for e in ExecutionModel]:
            raise ValidationError(f"Invalid execution_model: {self.execution_model}")
        object.__setattr__(self, "execution_model", em)

        if not self.validation_id:
            object.__setattr__(self, "validation_id", f"wf_{uuid.uuid4().hex[:12]}")
        if not self.correlation_id:
            object.__setattr__(self, "correlation_id", uuid.uuid4().hex)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "validation_id": self.validation_id,
            "correlation_id": self.correlation_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "train_duration_seconds": self.train_duration_seconds,
            "test_duration_seconds": self.test_duration_seconds,
            "step_duration_seconds": self.step_duration_seconds,
            "validation_duration_seconds": self.validation_duration_seconds,
            "window_mode": self.window_mode,
            "min_in_sample_bars": self.min_in_sample_bars,
            "min_out_of_sample_bars": self.min_out_of_sample_bars,
            "warmup_bars": self.warmup_bars,
            "initial_cash": self.initial_cash,
            "position_size_type": self.position_size_type,
            "position_size_value": self.position_size_value,
            "fee_model": self.fee_model,
            "fee_rate": self.fee_rate,
            "fixed_fee": self.fixed_fee,
            "slippage_bps": self.slippage_bps,
            "half_spread_bps": self.half_spread_bps,
            "execution_model": self.execution_model,
            "reinvest_profits": self.reinvest_profits,
        }


@dataclass(frozen=True)
class ValidationWindowResult:
    """Outcome report for an individual walk-forward validation window."""

    window: ValidationWindow
    status: str  # "COMPLETED", "SKIPPED", "FAILED"
    in_sample_bars: int = 0
    out_of_sample_bars: int = 0
    in_sample_result: Optional[BacktestResult] = None
    out_of_sample_result: Optional[BacktestResult] = None
    skip_or_fail_reason: Optional[str] = None
    warnings: Tuple[ValidationWarning, ...] = ()
    duration_ms: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "warnings", tuple(self.warnings or ()))

    @property
    def is_completed(self) -> bool:
        """True if window evaluated successfully out-of-sample."""
        return self.status == "COMPLETED" and self.out_of_sample_result is not None

    @property
    def in_sample_metrics(self) -> Optional[PerformanceMetrics]:
        """Convenience accessor to in-sample performance metrics."""
        return self.in_sample_result.metrics if self.in_sample_result else None

    @property
    def out_of_sample_metrics(self) -> Optional[PerformanceMetrics]:
        """Convenience accessor to out-of-sample performance metrics."""
        return self.out_of_sample_result.metrics if self.out_of_sample_result else None

    def to_dict(self) -> Dict[str, Any]:
        """Convert window result to dictionary."""
        return {
            "window": self.window.to_dict(),
            "status": self.status,
            "in_sample_bars": self.in_sample_bars,
            "out_of_sample_bars": self.out_of_sample_bars,
            "in_sample_metrics": self.in_sample_metrics.to_dict() if self.in_sample_metrics else None,
            "out_of_sample_metrics": self.out_of_sample_metrics.to_dict() if self.out_of_sample_metrics else None,
            "skip_or_fail_reason": self.skip_or_fail_reason,
            "warnings": [w.to_dict() for w in self.warnings],
            "duration_ms": round(self.duration_ms, 2),
        }


@dataclass(frozen=True)
class OutOfSampleSummary:
    """Aggregated evaluation metrics synthesizing performance across all out-of-sample periods.

    AGGREGATION INTEGRITY GUARANTEE:
    - Arithmetic averages across windows are reported separately from compounded sequential returns.
    - Non-aggregatable metrics (like maximum drawdown) are reported with clear semantics
      (worst single-window drawdown vs overall peak-to-trough drop).
    - Zero trades and zero losses are handled safely without NaN or infinity.
    """

    total_windows_generated: int
    eligible_windows: int
    completed_windows: int
    skipped_windows: int
    failed_windows: int
    mean_window_return_pct: float
    median_window_return_pct: float
    compounded_return_pct: float
    total_oos_net_pnl: float
    total_oos_trades: int
    total_oos_winning_trades: int
    total_oos_losing_trades: int
    overall_oos_win_rate: float
    mean_window_win_rate: float
    mean_profit_factor: Optional[float]
    overall_profit_factor: Optional[float]
    overall_profit_factor_description: str
    max_window_drawdown_pct: float
    worst_window_drawdown_amount: float
    total_transaction_costs: float
    profitable_windows_count: int
    unprofitable_windows_count: int
    window_win_rate_pct: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary representation."""
        return {
            "total_windows_generated": self.total_windows_generated,
            "eligible_windows": self.eligible_windows,
            "completed_windows": self.completed_windows,
            "skipped_windows": self.skipped_windows,
            "failed_windows": self.failed_windows,
            "mean_window_return_pct": round(self.mean_window_return_pct, 2),
            "median_window_return_pct": round(self.median_window_return_pct, 2),
            "compounded_return_pct": round(self.compounded_return_pct, 2),
            "total_oos_net_pnl": round(self.total_oos_net_pnl, 2),
            "total_oos_trades": self.total_oos_trades,
            "total_oos_winning_trades": self.total_oos_winning_trades,
            "total_oos_losing_trades": self.total_oos_losing_trades,
            "overall_oos_win_rate": round(self.overall_oos_win_rate, 2),
            "mean_window_win_rate": round(self.mean_window_win_rate, 2),
            "mean_profit_factor": round(self.mean_profit_factor, 2) if self.mean_profit_factor is not None else None,
            "overall_profit_factor": (
                round(self.overall_profit_factor, 2) if self.overall_profit_factor is not None else None
            ),
            "overall_profit_factor_description": self.overall_profit_factor_description,
            "max_window_drawdown_pct": round(self.max_window_drawdown_pct, 2),
            "worst_window_drawdown_amount": round(self.worst_window_drawdown_amount, 2),
            "total_transaction_costs": round(self.total_transaction_costs, 4),
            "profitable_windows_count": self.profitable_windows_count,
            "unprofitable_windows_count": self.unprofitable_windows_count,
            "window_win_rate_pct": round(self.window_win_rate_pct, 2),
        }


@dataclass(frozen=True)
class WalkForwardResult:
    """Comprehensive artifact bundling all walk-forward evaluation windows and aggregated outcomes."""

    validation_id: str
    status: WalkForwardStatus
    strategy_name: str
    strategy_version: str
    config: WalkForwardConfig
    summary: OutOfSampleSummary
    windows: Tuple[ValidationWindowResult, ...] = ()
    warnings: Tuple[ValidationWarning, ...] = ()
    data_quality_summary: Dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0
    error_message: Optional[str] = None
    disclaimer: str = (
        "RESEARCH ONLY: Walk-forward validation simulates sequential out-of-sample strategy "
        "evaluation to reduce curve-fitting risk. It does NOT eliminate overfitting, guarantee "
        "future market profitability, or execute live orders."
    )
    run_id: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "windows", tuple(self.windows or ()))
        object.__setattr__(self, "warnings", tuple(self.warnings or ()))

    @property
    def is_success(self) -> bool:
        """True if validation finished without fatal error."""
        return self.status == WalkForwardStatus.COMPLETED

    @property
    def summary_text(self) -> str:
        """Concise human/voice readable walk-forward summary."""
        return (
            f"Walk-Forward Validation [{self.validation_id}] for {self.config.symbol} ({self.config.timeframe}) "
            f"Strategy={self.strategy_name}: Status={self.status.value}, "
            f"Windows={self.summary.completed_windows}/{self.summary.total_windows_generated} "
            f"(WinRate={self.summary.window_win_rate_pct:.1f}%), "
            f"MeanReturn={self.summary.mean_window_return_pct:+.2f}%, "
            f"CompoundedReturn={self.summary.compounded_return_pct:+.2f}%, "
            f"WorstMaxDD={self.summary.max_window_drawdown_pct:.2f}%, "
            f"TotalOOSTrades={self.summary.total_oos_trades}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert complete result to serializable dictionary."""
        return {
            "validation_id": self.validation_id,
            "status": self.status.value,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "config": self.config.to_dict(),
            "summary": self.summary.to_dict(),
            "windows_count": len(self.windows),
            "windows": [w.to_dict() for w in self.windows],
            "warnings_count": len(self.warnings),
            "warnings": [w.to_dict() for w in self.warnings],
            "data_quality_summary": dict(self.data_quality_summary),
            "duration_ms": round(self.duration_ms, 2),
            "error_message": self.error_message,
            "summary_text": self.summary_text,
            "disclaimer": self.disclaimer,
            "run_id": self.run_id,
        }
