"""KAISER — Portfolio Analytics, Correlation & Diversification Engine.

Provides mathematically robust, deterministic calculation of pairwise return correlations,
concentration indices, and diversification analytics across historical strategies and instruments.

SAFETY INVARIANTS:
1. No fabricated observations: correlations are computed strictly across real, aligned timestamps.
2. Safe zero handling: series with zero variance or insufficient samples return None safely (no NaN/Inf).
3. Descriptive disclaimer: historical correlation never guarantees future diversification.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence, Tuple

from app.backtesting.portfolio_simulation.models import (
    DiversificationMetrics,
    StrategyAllocation,
)


def calculate_pearson_correlation(
    x: Sequence[float],
    y: Sequence[float],
    min_observations: int = 10,
) -> Optional[float]:
    """Calculate Pearson correlation coefficient between two numeric sequences.

    Args:
        x: First sequence of return observations.
        y: Second sequence of return observations.
        min_observations: Minimum required pairs.

    Returns:
        Optional[float]: Correlation in [-1.0, 1.0], or None if undefined / insufficient data.
    """
    n = len(x)
    if n != len(y) or n < min_observations:
        return None

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    cov_xy = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    var_x = sum((xi - mean_x) ** 2 for xi in x)
    var_y = sum((yi - mean_y) ** 2 for yi in y)

    # If either series is constant (zero variance), correlation is mathematically undefined
    if var_x < 1e-12 or var_y < 1e-12:
        return None

    denominator = math.sqrt(var_x * var_y)
    if denominator <= 0.0:
        return None

    r = cov_xy / denominator
    # Clamp to valid [-1.0, 1.0] due to floating-point rounding
    return round(max(-1.0, min(1.0, r)), 4)


def compute_diversification_metrics(
    allocations: Sequence[StrategyAllocation],
    instrument_returns: Dict[str, Dict[float, float]],
    strategy_returns: Dict[str, Dict[float, float]],
    min_observations: int = 10,
) -> DiversificationMetrics:
    """Compute pairwise correlation matrices, HHI concentration, and diversification summary.

    Args:
        allocations: Strategy allocations configured in simulation.
        instrument_returns: Dict mapping symbol -> {timestamp: return}.
        strategy_returns: Dict mapping strategy_id -> {timestamp: return}.
        min_observations: Configurable minimum aligned observations.

    Returns:
        DiversificationMetrics: Validated, strongly typed diversification model.
    """
    # 1. Herfindahl-Hirschman Index (HHI) of capital allocation weights
    total_w = sum(alloc.weight for alloc in allocations)
    if total_w > 0:
        norm_weights = [alloc.weight / total_w for alloc in allocations]
        hhi = sum(w ** 2 for w in norm_weights)
        effective_count = (1.0 / hhi) if hhi > 0 else 1.0
    else:
        hhi = 1.0
        effective_count = 1.0

    # 2. Instrument Pairwise Return Correlations
    symbols = sorted(list(instrument_returns.keys()))
    inst_corrs: Dict[str, Dict[str, Optional[float]]] = {}
    total_aligned_inst_obs = 0

    for s1 in symbols:
        inst_corrs[s1] = {}
        for s2 in symbols:
            if s1 == s2:
                inst_corrs[s1][s2] = 1.0
                continue

            r1_map = instrument_returns[s1]
            r2_map = instrument_returns[s2]
            common_ts = sorted(set(r1_map.keys()) & set(r2_map.keys()))
            total_aligned_inst_obs = max(total_aligned_inst_obs, len(common_ts))

            if len(common_ts) >= min_observations:
                s1_vals = [r1_map[ts] for ts in common_ts]
                s2_vals = [r2_map[ts] for ts in common_ts]
                inst_corrs[s1][s2] = calculate_pearson_correlation(s1_vals, s2_vals, min_observations)
            else:
                inst_corrs[s1][s2] = None

    # 3. Strategy Pairwise Return Correlations
    strat_ids = sorted(list(strategy_returns.keys()))
    strat_corrs: Dict[str, Dict[str, Optional[float]]] = {}
    total_aligned_strat_obs = 0

    for st1 in strat_ids:
        strat_corrs[st1] = {}
        for st2 in strat_ids:
            if st1 == st2:
                strat_corrs[st1][st2] = 1.0
                continue

            s1_map = strategy_returns[st1]
            s2_map = strategy_returns[st2]
            common_ts = sorted(set(s1_map.keys()) & set(s2_map.keys()))
            total_aligned_strat_obs = max(total_aligned_strat_obs, len(common_ts))

            if len(common_ts) >= min_observations:
                v1 = [s1_map[ts] for ts in common_ts]
                v2 = [s2_map[ts] for ts in common_ts]
                strat_corrs[st1][st2] = calculate_pearson_correlation(v1, v2, min_observations)
            else:
                strat_corrs[st1][st2] = None

    max_obs = max(total_aligned_inst_obs, total_aligned_strat_obs)
    warning: Optional[str] = None
    is_valid = True

    if max_obs < min_observations:
        is_valid = False
        warning = (
            f"Insufficient aligned historical observations ({max_obs} < {min_observations}) "
            "to compute reliable pairwise correlations."
        )

    return DiversificationMetrics(
        strategy_correlations=strat_corrs,
        instrument_correlations=inst_corrs,
        observation_count=max_obs,
        is_valid=is_valid,
        warning=warning,
        herfindahl_index=hhi,
        effective_strategies_count=effective_count,
    )
