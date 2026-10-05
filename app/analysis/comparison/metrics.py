"""KAISER — Numerical Metric Comparison Engine & Safe Extractors.

Provides mathematically safe metric comparison routines:
- Rejects NaN/Infinity.
- Preserves exact zero values.
- Explicitly flags undefined percentage changes (e.g. zero baseline).
- Implements neutral directional classification (INCREASED, DECREASED, UNCHANGED, UNAVAILABLE).
- Normalizes metric extraction across Backtest, Simulation, Walk-Forward, and Robustness results.
"""

from __future__ import annotations

import collections
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from app.analysis.comparison.models import MetricDifference, MetricDirection


def safe_float(val: Any) -> Optional[float]:
    """Convert an arbitrary value to float with strict NaN/Infinity rejection.

    Returns:
        float if valid finite number, None if None, non-numeric, NaN, or Infinity.
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError, OverflowError):
        return None


def compute_metric_difference(
    metric_name: str,
    left_raw: Any,
    right_raw: Any,
) -> MetricDifference:
    """Compute safe absolute and relative deltas between two metric observations.

    Guarantees:
    - Never emits NaN or Infinity.
    - Zero baseline emits relative_difference=None with reason 'Baseline value is zero'.
    - Preserves legitimate zero values (0.0).
    - Neutral terminology: INCREASED, DECREASED, UNCHANGED, UNAVAILABLE.
    """
    left_val = safe_float(left_raw)
    right_val = safe_float(right_raw)

    if left_val is None or right_val is None:
        missing_side = "both runs" if (left_val is None and right_val is None) else (
            "baseline (left) run" if left_val is None else "candidate (right) run"
        )
        return MetricDifference(
            metric_name=metric_name,
            left_value=left_val,
            right_value=right_val,
            absolute_difference=None,
            relative_difference=None,
            direction=MetricDirection.UNAVAILABLE,
            is_available=False,
            reason=f"Metric '{metric_name}' is unavailable on {missing_side}.",
        )

    # Both values are valid finite numbers
    abs_diff = round(right_val - left_val, 6)

    # Relative difference calculation
    rel_diff: Optional[float] = None
    reason: Optional[str] = None

    if abs(left_val) < 1e-12:
        # Zero baseline: division by zero is undefined
        rel_diff = None
        reason = "Baseline value is zero; relative percentage change is mathematically undefined."
    else:
        rel_diff = round(((right_val - left_val) / abs(left_val)) * 100.0, 4)

    # Directional classification
    if abs(abs_diff) < 1e-9:
        direction = MetricDirection.UNCHANGED
    elif abs_diff > 0:
        direction = MetricDirection.INCREASED
    else:
        direction = MetricDirection.DECREASED

    return MetricDifference(
        metric_name=metric_name,
        left_value=left_val,
        right_value=right_val,
        absolute_difference=abs_diff,
        relative_difference=rel_diff,
        direction=direction,
        is_available=True,
        reason=reason,
    )


# Standard metric name aliases for consistent normalization
METRIC_ALIASES: Dict[str, str] = {
    "net_return_pct": "total_return_pct",
    "total_return": "total_return_pct",
    "net_pnl": "net_profit",
    "gross_pnl": "gross_profit",
    "max_drawdown_pct": "max_drawdown_pct",
    "maximum_drawdown": "max_drawdown_pct",
    "max_drawdown": "max_drawdown_pct",
    "total_trades": "trade_count",
    "total_transaction_costs": "total_costs",
    "fees": "total_fees",
    "slippage": "total_slippage",
    "sharpe": "sharpe_ratio",
    "sortino": "sortino_ratio",
    "calmar": "calmar_ratio",
}


def extract_metrics_from_result(
    result: Any,
    execution_metadata: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Optional[float]]:
    """Extract and normalize numeric metrics from a result object, dictionary, or metadata.

    Supports:
    - BacktestResult
    - PortfolioSimulationResult
    - WalkForwardResult
    - RobustnessTestResult
    - PortfolioAttributionReport
    - TechnicalFeatureVector
    - Raw dictionary payloads
    """
    metrics: Dict[str, Optional[float]] = {}

    # 1. Inspect direct dict or to_dict
    raw_dict: Dict[str, Any] = {}
    if isinstance(result, Mapping):
        raw_dict = dict(result)
    elif hasattr(result, "to_dict") and callable(result.to_dict):
        try:
            raw_dict = result.to_dict()
        except Exception:
            pass
    elif hasattr(result, "__dict__"):
        try:
            raw_dict = vars(result)
        except Exception:
            pass

    # 2. Extract metrics directly from raw_dict (including aliases)
    for k, v in raw_dict.items():
        if k == "summary" and isinstance(v, Mapping):
            for sk, sv in v.items():
                c_name = METRIC_ALIASES.get(sk, sk)
                f_val = safe_float(sv)
                if f_val is not None and c_name not in metrics:
                    metrics[c_name] = f_val
            continue
        c_name = METRIC_ALIASES.get(k, k)
        f_val = safe_float(v)
        if f_val is not None:
            metrics[c_name] = f_val

    # 3. Check attributes on result object for standard metric candidates
    metric_candidates = [
        "net_return_pct",
        "total_return_pct",
        "net_profit",
        "net_pnl",
        "gross_profit",
        "gross_pnl",
        "max_drawdown_pct",
        "max_drawdown_amount",
        "win_rate",
        "profit_factor",
        "sharpe",
        "sharpe_ratio",
        "sortino",
        "sortino_ratio",
        "calmar",
        "calmar_ratio",
        "volatility",
        "trade_count",
        "total_trades",
        "winning_trades",
        "losing_trades",
        "total_fees",
        "total_slippage",
        "total_transaction_costs",
        "average_trade_pnl",
        "bars_processed",
        "compounded_return_pct",
        "mean_window_return_pct",
        "median_window_return_pct",
        "overall_oos_win_rate",
        "overall_profit_factor",
        "worst_window_drawdown_pct",
        "baseline_return_pct",
        "mean_stressed_return_pct",
        "worst_stressed_return_pct",
        "fragility_score",
        "active_return_pct",
        "allocation_effect_pct",
        "selection_effect_pct",
        "information_ratio",
    ]

    for cand in metric_candidates:
        c_name = METRIC_ALIASES.get(cand, cand)
        if c_name not in metrics and hasattr(result, cand):
            val = getattr(result, cand, None)
            f_val = safe_float(val)
            if f_val is not None:
                metrics[c_name] = f_val

    # 4. Fallback to execution_metadata
    if execution_metadata:
        for k, v in execution_metadata.items():
            canonical_name = METRIC_ALIASES.get(k, k)
            if canonical_name not in metrics:
                f_val = safe_float(v)
                if f_val is not None:
                    metrics[canonical_name] = f_val

    return metrics


def compare_metrics_dicts(
    left_metrics: Mapping[str, Any],
    right_metrics: Mapping[str, Any],
) -> List[MetricDifference]:
    """Compare two normalized metric dictionaries across all union metric keys.

    Returns:
        List[MetricDifference] sorted deterministically by metric name.
    """
    all_names = sorted(set(left_metrics.keys()) | set(right_metrics.keys()))
    diffs: List[MetricDifference] = []

    for name in all_names:
        l_val = left_metrics.get(name)
        r_val = right_metrics.get(name)
        diff = compute_metric_difference(name, l_val, r_val)
        diffs.append(diff)

    return diffs
