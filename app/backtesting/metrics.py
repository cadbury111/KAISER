"""KAISER — Backtesting Performance Metrics Calculator.

Computes comprehensive, mathematically robust performance and risk metrics
from simulated trade logs and equity curves.

CRITICAL INVARIANTS:
1. Division-by-zero protection: Safely handles zero trades, zero losses, and flat curves.
2. No NaN or infinity values: JSON serializable and safe for telemetry/APIs.
3. Explicit disclaimer: Historical backtest metrics never imply guaranteed future returns.
"""

from __future__ import annotations

import math
from typing import Optional, Sequence

from app.backtesting.models import EquityPoint, PerformanceMetrics, TradeRecord


def calculate_performance_metrics(
    initial_equity: float,
    final_equity: float,
    trades: Sequence[TradeRecord],
    equity_curve: Sequence[EquityPoint],
    total_bars: int = 0,
    bars_with_position: int = 0,
    total_fees: float = 0.0,
    total_slippage: float = 0.0,
    first_bar_price: Optional[float] = None,
    last_bar_price: Optional[float] = None,
) -> PerformanceMetrics:
    """Calculate all performance metrics across completed trades and equity history.

    Args:
        initial_equity: Starting virtual portfolio cash.
        final_equity: Ending portfolio equity.
        trades: List of completed round-trip trade records.
        equity_curve: Sequential equity snapshots.
        total_bars: Number of bars evaluated.
        bars_with_position: Number of bars with open position.
        total_fees: Cumulative fees paid.
        total_slippage: Cumulative slippage incurred.
        first_bar_price: Opening price of the first bar.
        last_bar_price: Closing price of the final bar.

    Returns:
        PerformanceMetrics: Validated, strongly typed metrics container.
    """
    # 1. PnL and Returns
    net_profit = final_equity - initial_equity
    total_return_pct = (net_profit / initial_equity) * 100.0 if initial_equity > 0 else 0.0

    # 2. Trade Breakdown
    total_trades = len(trades)
    winning_trades = sum(1 for t in trades if t.net_pnl > 0.0)
    losing_trades = sum(1 for t in trades if t.net_pnl < 0.0)
    even_trades = sum(1 for t in trades if t.net_pnl == 0.0)

    gross_profit = sum(t.gross_pnl for t in trades if t.gross_pnl > 0.0)
    gross_loss = abs(sum(t.gross_pnl for t in trades if t.gross_pnl < 0.0))

    win_rate = (winning_trades / total_trades) * 100.0 if total_trades > 0 else 0.0

    # 3. Profit Factor with Safe Zero Handling
    profit_factor: Optional[float] = None
    if total_trades == 0:
        pf_description = "N/A (No trades executed)"
    elif gross_loss == 0.0:
        if gross_profit > 0.0:
            profit_factor = None  # Represent mathematically infinite as None with explicit descriptor
            pf_description = "Infinite (No losing trades)"
        else:
            profit_factor = 0.0
            pf_description = "0.0 (Zero gross profit and loss)"
    else:
        calc_pf = gross_profit / gross_loss
        profit_factor = round(calc_pf, 4)
        pf_description = f"{calc_pf:.2f}"

    # 4. Maximum Drawdown Calculation
    max_dd_pct = 0.0
    max_dd_amount = 0.0

    if equity_curve:
        peak = equity_curve[0].total_equity
        for pt in equity_curve:
            if pt.total_equity > peak:
                peak = pt.total_equity
            dd_amt = peak - pt.total_equity
            dd_pct = (dd_amt / peak) * 100.0 if peak > 0 else 0.0

            if dd_amt > max_dd_amount:
                max_dd_amount = dd_amt
            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct

    # 5. Market Exposure (% of time with open position)
    if total_bars > 0:
        market_exposure_pct = (bars_with_position / total_bars) * 100.0
    else:
        market_exposure_pct = 0.0

    # 6. Transaction Costs
    total_tx_costs = total_fees + total_slippage

    # 7. Buy and Hold Benchmark
    buy_and_hold_return_pct: Optional[float] = None
    if first_bar_price is not None and last_bar_price is not None and first_bar_price > 0:
        buy_and_hold_return_pct = ((last_bar_price - first_bar_price) / first_bar_price) * 100.0

    # 8. Sharpe Ratio (Simple bar-level annualized estimate if enough observations)
    sharpe_ratio: Optional[float] = None
    if len(equity_curve) >= 5:
        returns = []
        for i in range(1, len(equity_curve)):
            prev_eq = equity_curve[i - 1].total_equity
            curr_eq = equity_curve[i].total_equity
            if prev_eq > 0:
                returns.append((curr_eq - prev_eq) / prev_eq)

        if returns:
            mean_ret = sum(returns) / len(returns)
            variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
            std_dev = math.sqrt(variance)
            if std_dev > 1e-8:
                # Annualization factor approx sqrt(252) for daily or 1.0 for general
                sharpe = (mean_ret / std_dev) * math.sqrt(252)
                sharpe_ratio = round(sharpe, 2)

    return PerformanceMetrics(
        initial_equity=initial_equity,
        final_equity=final_equity,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        net_profit=net_profit,
        total_return_pct=total_return_pct,
        max_drawdown_pct=max_dd_pct,
        max_drawdown_amount=max_dd_amount,
        total_trades=total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        even_trades=even_trades,
        win_rate=win_rate,
        profit_factor=profit_factor,
        profit_factor_description=pf_description,
        total_fees=total_fees,
        total_slippage=total_slippage,
        total_transaction_costs=total_tx_costs,
        market_exposure_pct=market_exposure_pct,
        buy_and_hold_return_pct=buy_and_hold_return_pct,
        sharpe_ratio=sharpe_ratio,
    )
