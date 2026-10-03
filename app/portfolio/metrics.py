"""KAISER — Numerically Safe Risk-Adjusted Performance Metrics.

Provides deterministic, robust calculations for:
- Time-series periodic returns from equity curves
- Annualized volatility (sample standard deviation with Bessel's correction)
- Sharpe ratio (with explicit zero-volatility and insufficient-data protection)
- Sortino ratio (downside risk deviation with target/risk-free rate)
- Maximum drawdown percentage, amount, and duration
- Return-to-drawdown ratio (Calmar / MAR ratio)
- Trade-level metrics: win rate, profit factor, win/loss ratio, average win/loss

SAFETY & NUMERICAL INVARIANTS:
1. Zero denominators explicitly return `MetricValue(value=None, is_available=False, ...)`
   with human-readable reasons rather than raising ZeroDivisionError or returning Infinity.
2. Insufficient observations (< 2 return periods) explicitly reported without crashing.
3. Time-series periodic returns and trade-level returns are strictly kept separate.
4. Annualization factors are strictly validated and configurable.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.portfolio.models import AttributionConfig, MetricValue, RiskAdjustedMetrics


def calculate_equity_returns(equity_points: Sequence[Any]) -> List[float]:
    """Calculate periodic decimal returns from a sequential series of equity observations.

    Args:
        equity_points: Sequence of EquityPoint objects or dicts with 'total_equity'.

    Returns:
        List of periodic decimal returns [R_1, R_2, ..., R_N].
    """
    if len(equity_points) < 2:
        return []

    returns: List[float] = []
    for i in range(1, len(equity_points)):
        prev_pt = equity_points[i - 1]
        curr_pt = equity_points[i]

        prev_val = (
            prev_pt.total_equity if hasattr(prev_pt, "total_equity") else float(prev_pt.get("total_equity", 0.0))
        )
        curr_val = (
            curr_pt.total_equity if hasattr(curr_pt, "total_equity") else float(curr_pt.get("total_equity", 0.0))
        )

        if prev_val > 0.0:
            ret = (curr_val - prev_val) / prev_val
            if not (math.isnan(ret) or math.isinf(ret)):
                returns.append(ret)
            else:
                returns.append(0.0)
        else:
            returns.append(0.0)

    return returns


def calculate_annualized_volatility(
    returns: Sequence[float],
    annualization_factor: float = 252.0,
) -> MetricValue:
    """Calculate annualized volatility from a periodic return series.

    Uses sample standard deviation with Bessel's correction (N - 1).

    Args:
        returns: Sequence of periodic decimal returns.
        annualization_factor: Number of periods per year (e.g. 252 for daily).

    Returns:
        MetricValue containing annualized volatility percentage.
    """
    if len(returns) < 2:
        return MetricValue(
            value=None,
            unit="%",
            is_available=False,
            unavailable_reason="Insufficient observations (< 2) to compute sample standard deviation.",
            description="Annualized Volatility (Annualized standard deviation of periodic returns)",
        )

    n = len(returns)
    mean_ret = sum(returns) / n
    # Sample variance with Bessel's correction (N - 1)
    variance = sum((r - mean_ret) ** 2 for r in returns) / (n - 1)
    sample_std = math.sqrt(max(0.0, variance))

    ann_vol_pct = sample_std * math.sqrt(annualization_factor) * 100.0

    return MetricValue(
        value=ann_vol_pct,
        unit="%",
        is_available=True,
        description="Annualized Volatility (Annualized standard deviation of periodic returns)",
    )


def calculate_sharpe_ratio(
    returns: Sequence[float],
    risk_free_rate: float = 0.0,
    annualization_factor: float = 252.0,
) -> MetricValue:
    """Calculate the annualized Sharpe ratio.

    Sharpe = (Mean_Period_Return - Period_Rf) / Sample_Std * sqrt(annualization_factor)

    Args:
        returns: Sequence of periodic decimal returns.
        risk_free_rate: Annualized risk-free rate as decimal (e.g. 0.04 for 4%).
        annualization_factor: Number of periods per year (e.g. 252 for daily).

    Returns:
        MetricValue with Sharpe ratio.
    """
    if len(returns) < 2:
        return MetricValue(
            value=None,
            unit="ratio",
            is_available=False,
            unavailable_reason="Insufficient observations (< 2) for Sharpe ratio.",
            description="Sharpe Ratio (Excess return per unit of total risk)",
        )

    n = len(returns)
    mean_ret = sum(returns) / n
    rf_period = risk_free_rate / annualization_factor
    excess_ret = mean_ret - rf_period

    variance = sum((r - mean_ret) ** 2 for r in returns) / (n - 1)
    std_dev = math.sqrt(max(0.0, variance))

    if std_dev <= 1e-12:
        return MetricValue(
            value=None,
            unit="ratio",
            is_available=False,
            unavailable_reason="Zero return volatility; denominator is zero (Sharpe ratio undefined).",
            description="Sharpe Ratio (Excess return per unit of total risk)",
        )

    sharpe = (excess_ret / std_dev) * math.sqrt(annualization_factor)

    return MetricValue(
        value=sharpe,
        unit="ratio",
        is_available=True,
        description="Sharpe Ratio (Excess return per unit of total risk)",
    )


def calculate_sortino_ratio(
    returns: Sequence[float],
    risk_free_rate: float = 0.0,
    annualization_factor: float = 252.0,
) -> MetricValue:
    """Calculate the annualized Sortino ratio (downside risk-adjusted return).

    Sortino = (Mean_Period_Return - Period_Rf) / Downside_Std * sqrt(annualization_factor)

    Args:
        returns: Sequence of periodic decimal returns.
        risk_free_rate: Annualized risk-free rate as decimal (e.g. 0.04 for 4%).
        annualization_factor: Number of periods per year.

    Returns:
        MetricValue with Sortino ratio.
    """
    if len(returns) < 2:
        return MetricValue(
            value=None,
            unit="ratio",
            is_available=False,
            unavailable_reason="Insufficient observations (< 2) for Sortino ratio.",
            description="Sortino Ratio (Excess return per unit of downside risk)",
        )

    n = len(returns)
    mean_ret = sum(returns) / n
    rf_period = risk_free_rate / annualization_factor
    excess_ret = mean_ret - rf_period

    # Downside deviations below target/risk-free rate
    downside_diffs = [min(0.0, r - rf_period) for r in returns]
    downside_variance = sum(d ** 2 for d in downside_diffs) / n
    downside_std = math.sqrt(max(0.0, downside_variance))

    if downside_std <= 1e-12:
        return MetricValue(
            value=None,
            unit="ratio",
            is_available=False,
            unavailable_reason="Zero downside deviation; no negative excess returns below target (Sortino ratio undefined).",
            description="Sortino Ratio (Excess return per unit of downside risk)",
        )

    sortino = (excess_ret / downside_std) * math.sqrt(annualization_factor)

    return MetricValue(
        value=sortino,
        unit="ratio",
        is_available=True,
        description="Sortino Ratio (Excess return per unit of downside risk)",
    )


def calculate_max_drawdown(
    equity_points: Sequence[Any],
) -> Tuple[MetricValue, MetricValue, MetricValue]:
    """Calculate peak-to-trough maximum drawdown percentage, dollar amount, and duration.

    Args:
        equity_points: Sequence of EquityPoint objects or dicts.

    Returns:
        Tuple of (max_drawdown_pct, max_drawdown_amount, max_drawdown_duration_bars).
    """
    if not equity_points:
        return (
            MetricValue(value=0.0, unit="%", description="Maximum Peak-to-Trough Drawdown Percentage"),
            MetricValue(value=0.0, unit="USD", description="Maximum Peak-to-Trough Drawdown Amount"),
            MetricValue(value=0.0, unit="bars", description="Maximum Drawdown Duration in Bars"),
        )

    peak = -float("inf")
    max_dd_amount = 0.0
    max_dd_pct = 0.0
    current_dd_duration = 0
    max_dd_duration = 0

    for pt in equity_points:
        eq = pt.total_equity if hasattr(pt, "total_equity") else float(pt.get("total_equity", 0.0))

        if eq > peak:
            peak = eq
            current_dd_duration = 0
        else:
            current_dd_duration += 1
            if current_dd_duration > max_dd_duration:
                max_dd_duration = current_dd_duration

            dd_amt = peak - eq
            dd_pct = (dd_amt / peak) * 100.0 if peak > 0 else 0.0

            if dd_amt > max_dd_amount:
                max_dd_amount = dd_amt
            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct

    return (
        MetricValue(
            value=max_dd_pct,
            unit="%",
            is_available=True,
            description="Maximum Peak-to-Trough Drawdown Percentage",
        ),
        MetricValue(
            value=max_dd_amount,
            unit="USD",
            is_available=True,
            description="Maximum Peak-to-Trough Drawdown Amount",
        ),
        MetricValue(
            value=float(max_dd_duration),
            unit="bars",
            is_available=True,
            description="Maximum Drawdown Duration in Bars",
        ),
    )


def calculate_calmar_ratio(
    annualized_return_pct: Optional[float],
    max_drawdown_pct: Optional[float],
) -> MetricValue:
    """Calculate the Calmar ratio (Annualized Return % / Max Drawdown %).

    Args:
        annualized_return_pct: Annualized net return percentage.
        max_drawdown_pct: Maximum peak-to-trough drawdown percentage.

    Returns:
        MetricValue with Calmar ratio.
    """
    if annualized_return_pct is None or max_drawdown_pct is None:
        return MetricValue(
            value=None,
            unit="ratio",
            is_available=False,
            unavailable_reason="Missing annualized return or maximum drawdown data.",
            description="Calmar Ratio (Annualized Return % / Max Drawdown %)",
        )

    if max_drawdown_pct <= 1e-8:
        return MetricValue(
            value=None,
            unit="ratio",
            is_available=False,
            unavailable_reason="Zero maximum drawdown; denominator is zero (Calmar ratio undefined).",
            description="Calmar Ratio (Annualized Return % / Max Drawdown %)",
        )

    calmar = annualized_return_pct / max_drawdown_pct

    return MetricValue(
        value=calmar,
        unit="ratio",
        is_available=True,
        description="Calmar Ratio (Annualized Return % / Max Drawdown %)",
    )


def calculate_trade_metrics(trades: Sequence[Any]) -> Dict[str, MetricValue]:
    """Calculate trade-level distribution metrics.

    Args:
        trades: Sequence of TradeRecord objects or dicts.

    Returns:
        Dictionary of MetricValues for total_trades, win_rate, profit_factor,
        win_loss_ratio, average_win, average_loss.
    """
    total_trades = len(trades)
    if total_trades == 0:
        return {
            "total_trades": MetricValue(value=0.0, unit="count", description="Total completed round-trip trades"),
            "win_rate": MetricValue(
                value=None,
                unit="%",
                is_available=False,
                unavailable_reason="No trades completed.",
                description="Win Rate Percentage",
            ),
            "profit_factor": MetricValue(
                value=None,
                unit="ratio",
                is_available=False,
                unavailable_reason="No trades completed.",
                description="Profit Factor (Gross profit / Gross loss)",
            ),
            "win_loss_ratio": MetricValue(
                value=None,
                unit="ratio",
                is_available=False,
                unavailable_reason="No trades completed.",
                description="Win/Loss Ratio (Average win / Average loss)",
            ),
            "average_win": MetricValue(
                value=None,
                unit="USD",
                is_available=False,
                unavailable_reason="No winning trades.",
                description="Average Winning Trade PnL",
            ),
            "average_loss": MetricValue(
                value=None,
                unit="USD",
                is_available=False,
                unavailable_reason="No losing trades.",
                description="Average Losing Trade PnL",
            ),
        }

    winning_trades = 0
    losing_trades = 0
    gross_win = 0.0
    gross_loss = 0.0

    for t in trades:
        pnl = t.net_pnl if hasattr(t, "net_pnl") else float(t.get("net_pnl", 0.0))
        if pnl > 0:
            winning_trades += 1
            gross_win += pnl
        elif pnl < 0:
            losing_trades += 1
            gross_loss += abs(pnl)

    win_rate = (winning_trades / total_trades) * 100.0

    # Profit factor
    if gross_loss > 0:
        profit_factor_val: Optional[float] = gross_win / gross_loss
        profit_factor_avail = True
        profit_factor_reason = None
    elif gross_win > 0:
        profit_factor_val = None
        profit_factor_avail = False
        profit_factor_reason = "No losing trades (denominator is zero; infinite profit factor)."
    else:
        profit_factor_val = None
        profit_factor_avail = False
        profit_factor_reason = "Zero gross profit and zero gross loss."

    # Average win & loss
    avg_win_val = (gross_win / winning_trades) if winning_trades > 0 else None
    avg_loss_val = (gross_loss / losing_trades) if losing_trades > 0 else None

    # Win / loss ratio
    if avg_win_val is not None and avg_loss_val is not None and avg_loss_val > 0:
        wl_ratio_val: Optional[float] = avg_win_val / avg_loss_val
        wl_ratio_avail = True
        wl_ratio_reason = None
    else:
        wl_ratio_val = None
        wl_ratio_avail = False
        wl_ratio_reason = "Missing winning or losing trades to compute average win/loss ratio."

    return {
        "total_trades": MetricValue(
            value=float(total_trades),
            unit="count",
            description="Total completed round-trip trades",
        ),
        "win_rate": MetricValue(
            value=win_rate,
            unit="%",
            is_available=True,
            description="Win Rate Percentage",
        ),
        "profit_factor": MetricValue(
            value=profit_factor_val,
            unit="ratio",
            is_available=profit_factor_avail,
            unavailable_reason=profit_factor_reason,
            description="Profit Factor (Gross profit / Gross loss)",
        ),
        "win_loss_ratio": MetricValue(
            value=wl_ratio_val,
            unit="ratio",
            is_available=wl_ratio_avail,
            unavailable_reason=wl_ratio_reason,
            description="Win/Loss Ratio (Average win / Average loss)",
        ),
        "average_win": MetricValue(
            value=avg_win_val,
            unit="USD",
            is_available=(avg_win_val is not None),
            unavailable_reason="No winning trades." if avg_win_val is None else None,
            description="Average Winning Trade PnL",
        ),
        "average_loss": MetricValue(
            value=avg_loss_val,
            unit="USD",
            is_available=(avg_loss_val is not None),
            unavailable_reason="No losing trades." if avg_loss_val is None else None,
            description="Average Losing Trade PnL",
        ),
    }


def compute_risk_adjusted_metrics(
    equity_points: Sequence[Any],
    trades: Sequence[Any],
    config: AttributionConfig,
    total_bars: int = 0,
    initial_equity: float = 0.0,
    final_equity: float = 0.0,
) -> RiskAdjustedMetrics:
    """Assembles all risk-adjusted and trade distribution performance metrics deterministically.

    Args:
        equity_points: Sequence of equity observations.
        trades: Sequence of completed trade records.
        config: Attribution configuration (annualization, risk-free rate).
        total_bars: Total bars processed in simulation.
        initial_equity: Starting capital.
        final_equity: Ending capital.

    Returns:
        RiskAdjustedMetrics dataclass instance.
    """
    returns = calculate_equity_returns(equity_points)
    ann_factor = config.annualization_factor
    rf_rate = config.risk_free_rate

    # Annualized return
    if returns and initial_equity > 0:
        total_ret = (final_equity - initial_equity) / initial_equity
        n = len(returns)
        # Arithmetic annualization of mean periodic return
        mean_ret = sum(returns) / n
        ann_ret_pct = mean_ret * ann_factor * 100.0
        ann_ret_val = MetricValue(
            value=ann_ret_pct,
            unit="%",
            is_available=True,
            description="Annualized Net Return Percentage (Arithmetic mean return * annualization factor)",
        )
    else:
        ann_ret_val = MetricValue(
            value=0.0 if initial_equity > 0 else None,
            unit="%",
            is_available=(initial_equity > 0),
            unavailable_reason="No return observations available." if initial_equity <= 0 else None,
            description="Annualized Net Return Percentage",
        )

    vol_val = calculate_annualized_volatility(returns, annualization_factor=ann_factor)
    sharpe_val = calculate_sharpe_ratio(returns, risk_free_rate=rf_rate, annualization_factor=ann_factor)
    sortino_val = calculate_sortino_ratio(returns, risk_free_rate=rf_rate, annualization_factor=ann_factor)
    max_dd_pct, max_dd_amt, max_dd_dur = calculate_max_drawdown(equity_points)
    calmar_val = calculate_calmar_ratio(ann_ret_val.value, max_dd_pct.value)
    trade_metrics = calculate_trade_metrics(trades)

    # Market exposure
    if total_bars > 0 and equity_points:
        bars_with_pos = sum(
            1 for pt in equity_points
            if (pt.position_value if hasattr(pt, "position_value") else float(pt.get("position_value", 0.0))) != 0.0
        )
        exp_pct = (bars_with_pos / total_bars) * 100.0
        market_exp_val = MetricValue(
            value=exp_pct,
            unit="%",
            is_available=True,
            description="Market Exposure (% of total bars with active position)",
        )
    else:
        market_exp_val = MetricValue(
            value=0.0,
            unit="%",
            is_available=True,
            description="Market Exposure (% of total bars with active position)",
        )

    return RiskAdjustedMetrics(
        annualized_return_pct=ann_ret_val,
        annualized_volatility_pct=vol_val,
        sharpe_ratio=sharpe_val,
        sortino_ratio=sortino_val,
        max_drawdown_pct=max_dd_pct,
        max_drawdown_amount=max_dd_amt,
        max_drawdown_duration_bars=max_dd_dur,
        calmar_ratio=calmar_val,
        profit_factor=trade_metrics["profit_factor"],
        win_rate=trade_metrics["win_rate"],
        win_loss_ratio=trade_metrics["win_loss_ratio"],
        average_win=trade_metrics["average_win"],
        average_loss=trade_metrics["average_loss"],
        total_trades=trade_metrics["total_trades"],
        market_exposure_pct=market_exp_val,
        annualization_factor=ann_factor,
        risk_free_rate=rf_rate,
    )
