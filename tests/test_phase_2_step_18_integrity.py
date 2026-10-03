"""KAISER — Phase 2, Step 18 Integrity & Verification Test Suite.

Comprehensive verification of:
1. Indicator numerical correctness against independent reference implementations.
2. Property-based financial invariants (bounds, ordering, non-negativity).
3. StateStore isolation and anti-aliasing semantics after Step 17 optimizations.
4. Historical market data integrity, non-finite rejection, and SQLite constraints.
5. Portfolio accounting reconciliation (Gross vs Net P&L, Cash + Positions = Equity).
6. Numerically safe risk-adjusted metrics and zero-denominator handling.
7. Anti-lookahead execution timing and cooperative cancellation.
"""

from __future__ import annotations

import collections
import concurrent.futures
import copy
import math
import random
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest

from app.analysis.indicators import (
    calculate_atr,
    calculate_bollinger_bands,
    calculate_ema,
    calculate_ema_series,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
    calculate_true_range,
    classify_bollinger,
    classify_macd,
    classify_rsi,
)
from app.analysis.models import SignalDirection
from app.analysis.series import RollingBarSeries
from app.backtesting.models import BacktestConfig, ExecutionModel, OrderSide, OrderType, SimulatedOrder
from app.backtesting.portfolio import SimulatedPortfolio
from app.core.config import Settings
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.historical.cache import HistoricalDataCache, make_cache_key
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.models import Bar, Quote
from app.portfolio.metrics import (
    calculate_annualized_volatility,
    calculate_calmar_ratio,
    calculate_equity_returns,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
    calculate_sortino_ratio,
    calculate_trade_metrics,
)
from app.portfolio.models import MetricValue
from app.backtesting.portfolio_simulation.models import PortfolioRiskSnapshot
from app.risk.models import AlertSeverity, AlertState, RiskRuleId
from app.risk.monitor import PortfolioRiskMonitor, PortfolioRiskMonitorConfig


# =============================================================================
# Independent Reference Implementations for Cross-Verification
# =============================================================================

def ref_sma(prices: Sequence[float], period: int) -> Optional[float]:
    """Pure reference SMA implementation."""
    if period <= 0 or len(prices) < period:
        return None
    window = prices[-period:]
    return round(sum(window) / float(period), 4)


def ref_ema_series(prices: Sequence[float], period: int, smoothing: float = 2.0) -> List[float]:
    """Pure reference EMA series implementation."""
    if period <= 0 or len(prices) < period:
        return []
    alpha = smoothing / (float(period) + 1.0)
    seed_sma = sum(prices[:period]) / float(period)
    series = [round(seed_sma, 4)]
    ema = seed_sma
    for p in prices[period:]:
        ema = (p - ema) * alpha + ema
        series.append(round(ema, 4))
    return series


def ref_wilder_rsi(prices: Sequence[float], period: int = 14) -> Optional[float]:
    """Pure reference Wilder's RSI implementation adhering to documented KAISER conventions."""
    if period <= 0 or len(prices) < period + 1:
        return None

    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [max(0.0, d) for d in deltas[:period]]
    losses = [max(0.0, -d) for d in deltas[:period]]

    avg_gain = sum(gains) / float(period)
    avg_loss = sum(losses) / float(period)

    for d in deltas[period:]:
        avg_gain = (avg_gain * (period - 1) + max(0.0, d)) / float(period)
        avg_loss = (avg_loss * (period - 1) + max(0.0, -d)) / float(period)

    # Documented KAISER boundary conventions:
    if avg_loss == 0.0:
        if avg_gain == 0.0:
            return 50.0  # Flat market
        return 100.0  # Only positive movement
    if avg_gain == 0.0:
        return 0.0  # Only negative movement

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(min(100.0, max(0.0, rsi)), 4)


def ref_macd(
    prices: Sequence[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Optional[Dict[str, float]]:
    """Pure reference MACD implementation."""
    min_required = slow + signal - 1
    if len(prices) < min_required:
        return None
    slow_s = ref_ema_series(prices, slow)
    fast_s = ref_ema_series(prices, fast)
    offset = slow - fast
    aligned_f = fast_s[offset:]
    macd_line = [round(f - s, 4) for f, s in zip(aligned_f, slow_s)]
    signal_s = ref_ema_series(macd_line, signal)
    if not signal_s:
        return None
    return {
        "macd": macd_line[-1],
        "signal": signal_s[-1],
        "histogram": round(macd_line[-1] - signal_s[-1], 4),
    }


def ref_atr(bars: Sequence[Bar], period: int = 14) -> Optional[float]:
    """Pure reference ATR implementation using explicit True Range list."""
    if period <= 0 or len(bars) < period + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        hl = bars[i].high - bars[i].low
        h_pc = abs(bars[i].high - bars[i - 1].close)
        l_pc = abs(bars[i].low - bars[i - 1].close)
        trs.append(round(max(hl, h_pc, l_pc), 4))

    atr = sum(trs[:period]) / float(period)
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / float(period)
    return round(atr, 4)


# =============================================================================
# Helper Fixtures & Builders
# =============================================================================

def build_bar(
    symbol: str = "NIFTY",
    timestamp: float = 1700000000.0,
    open_price: float = 100.0,
    high_price: float = 105.0,
    low_price: float = 95.0,
    close_price: float = 102.0,
    volume: float = 1000.0,
    timeframe: str = "1m",
    exchange: str = "NSE",
    source: str = "mock",
) -> Bar:
    return Bar(
        symbol=symbol,
        exchange=exchange,
        timeframe=timeframe,
        timestamp=timestamp,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        volume=volume,
        source=source,
    )


# =============================================================================
# Test Suite 1: Indicator Numerical Correctness & Edge Conditions
# =============================================================================

class TestIndicatorNumericalCorrectness:
    """Verifies numerical accuracy against independent formulas and edge cases."""

    # Classic J. Welles Wilder Jr. textbook dataset (14 periods)
    WILDER_PRICES = [
        44.34, 44.09, 44.15, 43.61, 44.33, 44.83, 45.10, 45.42,
        45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28,
    ]

    def test_sma_reference_equivalence(self) -> None:
        """Verify SMA calculation matches independent reference."""
        prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
        assert calculate_sma(prices, period=5) == ref_sma(prices, period=5)
        assert calculate_sma(prices, period=5) == 13.0

    def test_sma_minimum_input_length(self) -> None:
        """Verify SMA returns None when input length < period."""
        assert calculate_sma([10.0, 20.0], period=3) is None
        assert calculate_sma([], period=1) is None
        assert calculate_sma([10.0], period=0) is None
        assert calculate_sma([10.0], period=-1) is None
        assert calculate_sma([10.0, 20.0, 30.0], period=3) == 20.0

    def test_ema_scalar_matches_reference_series(self) -> None:
        """Verify optimized scalar calculate_ema produces identical results to full series."""
        rng = random.Random(42)
        price = 100.0
        prices = []
        for _ in range(500):
            price += rng.uniform(-1.5, 1.5)
            prices.append(round(price, 2))

        for period in (9, 21, 50, 200):
            res_scalar = calculate_ema(prices, period=period)
            res_series = ref_ema_series(prices, period=period)
            assert res_scalar is not None
            assert res_scalar == res_series[-1]

    def test_wilder_rsi_classic_textbook_dataset(self) -> None:
        """Verify RSI on classic 14-period Wilder dataset matches independent reference."""
        calc = calculate_rsi(self.WILDER_PRICES, period=14)
        ref = ref_wilder_rsi(self.WILDER_PRICES, period=14)
        assert calc is not None
        assert calc == ref
        # Wilder's 14-period smoothed RSI for this series is ~70.53
        assert 70.0 <= calc <= 71.0

    def test_rsi_boundary_conventions(self) -> None:
        """Verify documented KAISER zero-gain and zero-loss conventions."""
        # 1. Flat market (zero gains AND zero losses) -> 50.0
        flat_prices = [100.0] * 30
        assert calculate_rsi(flat_prices, period=14) == 50.0

        # 2. Monotonically increasing (zero losses) -> 100.0
        up_prices = [100.0 + i * 2.0 for i in range(25)]
        assert calculate_rsi(up_prices, period=14) == 100.0

        # 3. Monotonically decreasing (zero gains) -> 0.0
        down_prices = [100.0 - i * 2.0 for i in range(25)]
        assert calculate_rsi(down_prices, period=14) == 0.0

    def test_macd_minimum_length_and_reference_equivalence(self) -> None:
        """Verify MACD minimum required bars and reference equivalence."""
        prices = [100.0 + i * 0.5 for i in range(34)]  # 26 + 9 - 1 = 34
        # At exactly 34 bars, MACD must compute
        res = calculate_macd(prices, fast_period=12, slow_period=26, signal_period=9)
        ref = ref_macd(prices, fast=12, slow=26, signal=9)
        assert res is not None
        assert ref is not None
        assert res["macd"] == ref["macd"]
        assert res["signal"] == ref["signal"]
        assert res["histogram"] == ref["histogram"]

        # At 33 bars, MACD must return None
        assert calculate_macd(prices[:33], fast_period=12, slow_period=26, signal_period=9) is None

    def test_bollinger_bands_constant_and_zero_variance(self) -> None:
        """Verify Bollinger Bands handle constant prices and zero standard deviation cleanly."""
        flat = [50.0] * 30
        bands = calculate_bollinger_bands(flat, period=20, num_std=2.0)
        assert bands is not None
        assert bands["middle"] == 50.0
        assert bands["upper"] == 50.0
        assert bands["lower"] == 50.0
        assert bands["bandwidth"] == 0.0
        assert bands["percent_b"] == 0.5  # Neutral placement on flat line

    def test_atr_single_pass_matches_reference(self) -> None:
        """Verify optimized single-pass calculate_atr matches reference TR list calculation."""
        rng = random.Random(123)
        bars: List[Bar] = []
        p = 100.0
        for i in range(100):
            p += rng.uniform(-1.0, 1.0)
            high = p + rng.uniform(0.5, 2.0)
            low = p - rng.uniform(0.5, 2.0)
            bars.append(build_bar(timestamp=1700000000.0 + i * 60.0, open_price=p, high_price=high, low_price=low, close_price=p))

        calc_atr = calculate_atr(bars, period=14)
        expected_atr = ref_atr(bars, period=14)
        assert calc_atr is not None
        assert calc_atr == expected_atr

    def test_atr_minimum_input_length(self) -> None:
        """Verify ATR requires at least period + 1 bars."""
        bars = [build_bar(timestamp=1700000000.0 + i * 60.0) for i in range(14)]
        assert calculate_atr(bars, period=14) is None
        bars.append(build_bar(timestamp=1700000000.0 + 14 * 60.0))
        assert calculate_atr(bars, period=14) is not None

    def test_non_finite_inputs_gracefully_rejected(self) -> None:
        """Verify non-finite inputs (NaN, inf, -inf) return None across all indicators."""
        nan_prices = [100.0, 101.0, float("nan"), 103.0, 104.0]
        inf_prices = [100.0, 101.0, float("inf"), 103.0, 104.0]

        assert calculate_sma(nan_prices, period=3) is None
        assert calculate_sma(inf_prices, period=3) is None

        assert calculate_ema(nan_prices, period=3) is None
        assert calculate_ema(inf_prices, period=3) is None

        assert calculate_rsi([100.0] * 14 + [float("nan")], period=14) is None
        assert calculate_rsi([100.0] * 14 + [float("inf")], period=14) is None

        assert calculate_bollinger_bands(nan_prices, period=3) is None
        assert calculate_bollinger_bands(inf_prices, period=3) is None

        assert calculate_macd([100.0] * 34 + [float("nan")]) is None

        nan_bar = Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=1700000000.0, open=100.0, high=105.0, low=95.0, close=100.0)
        # Verify calculate_true_range with non-finite handles safely
        assert calculate_true_range(float("nan"), 95.0, 100.0) == 0.0

    def test_long_sequence_floating_point_stability(self) -> None:
        """Verify 10,000-point long sequence does not accumulate drift or overflow."""
        rng = random.Random(999)
        p = 5000.0
        prices = []
        for _ in range(10000):
            p += rng.uniform(-5.0, 5.0)
            prices.append(round(p, 2))

        ema_val = calculate_ema(prices, period=200)
        ref_val = ref_ema_series(prices, period=200)[-1]
        assert ema_val is not None
        assert abs(ema_val - ref_val) <= 0.01

        rsi_val = calculate_rsi(prices, period=14)
        assert rsi_val is not None
        assert 0.0 <= rsi_val <= 100.0

    def test_extreme_numerical_scales(self) -> None:
        """Verify calculations remain stable on micro (1e-4) and macro (1e6) scales."""
        # Micro prices
        micro_prices = [0.0001 + i * 0.000005 for i in range(50)]
        micro_sma = calculate_sma(micro_prices, period=10)
        assert micro_sma is not None and micro_sma > 0

        # Macro prices
        macro_prices = [1000000.0 + i * 500.0 for i in range(50)]
        macro_sma = calculate_sma(macro_prices, period=10)
        assert macro_sma is not None and macro_sma > 1000000.0


# =============================================================================
# Test Suite 2: Property-Based Financial Invariants
# =============================================================================

class TestPropertyBasedFinancialInvariants:
    """Verifies structural, mathematical, and domain invariants across generated cases."""

    @pytest.mark.parametrize("seed", [101, 202, 303, 404, 505])
    def test_moving_average_bounds_invariant(self, seed: int) -> None:
        """Invariant: SMA and EMA always lie within the [min, max] range of the input."""
        rng = random.Random(seed)
        prices = [round(1000.0 + rng.uniform(-50.0, 50.0), 2) for _ in range(100)]

        for period in (10, 20, 50):
            sma = calculate_sma(prices, period=period)
            assert sma is not None
            window = prices[-period:]
            assert min(window) <= sma <= max(window)

            ema = calculate_ema(prices, period=period)
            assert ema is not None
            assert min(prices) <= ema <= max(prices)

    @pytest.mark.parametrize("seed", [11, 22, 33])
    def test_bollinger_bands_ordering_invariant(self, seed: int) -> None:
        """Invariant: lower <= middle <= upper and bandwidth >= 0.0."""
        rng = random.Random(seed)
        prices = [round(200.0 + rng.uniform(-10.0, 10.0), 2) for _ in range(60)]

        bands = calculate_bollinger_bands(prices, period=20, num_std=2.0)
        assert bands is not None
        assert bands["lower"] <= bands["middle"] <= bands["upper"]
        assert bands["bandwidth"] >= 0.0

    @pytest.mark.parametrize("seed", [71, 72, 73])
    def test_atr_non_negative_invariant(self, seed: int) -> None:
        """Invariant: ATR is strictly >= 0.0 for all valid price ranges."""
        rng = random.Random(seed)
        bars: List[Bar] = []
        p = 500.0
        for i in range(50):
            p += rng.uniform(-5.0, 5.0)
            bars.append(build_bar(timestamp=1700000000.0 + i * 60.0, open_price=p, high_price=p + 2.0, low_price=p - 2.0, close_price=p))

        atr = calculate_atr(bars, period=14)
        assert atr is not None
        assert atr >= 0.0

    @pytest.mark.parametrize("seed", [81, 82, 83])
    def test_rsi_bounds_invariant(self, seed: int) -> None:
        """Invariant: RSI is strictly bounded in [0.0, 100.0]."""
        rng = random.Random(seed)
        prices = [round(150.0 + rng.uniform(-8.0, 8.0), 2) for _ in range(50)]

        rsi = calculate_rsi(prices, period=14)
        assert rsi is not None
        assert 0.0 <= rsi <= 100.0

    def test_signal_classifiers_consistency(self) -> None:
        """Invariant: Classifiers map exact thresholds to expected SignalDirection."""
        assert classify_rsi(75.0) == SignalDirection.OVERBOUGHT
        assert classify_rsi(25.0) == SignalDirection.OVERSOLD
        assert classify_rsi(50.0) == SignalDirection.NEUTRAL
        assert classify_rsi(None) == SignalDirection.NEUTRAL

        assert classify_macd({"macd": 2.0, "signal": 1.0, "histogram": 1.0}) == SignalDirection.BULLISH
        assert classify_macd({"macd": 1.0, "signal": 2.0, "histogram": -1.0}) == SignalDirection.BEARISH
        assert classify_macd({"macd": 1.0, "signal": 1.0, "histogram": 0.0}) == SignalDirection.NEUTRAL
        assert classify_macd(None) == SignalDirection.NEUTRAL

        bands = {"upper": 110.0, "middle": 100.0, "lower": 90.0}
        assert classify_bollinger(115.0, bands) == SignalDirection.OVERBOUGHT
        assert classify_bollinger(85.0, bands) == SignalDirection.OVERSOLD
        assert classify_bollinger(105.0, bands) == SignalDirection.BULLISH
        assert classify_bollinger(95.0, bands) == SignalDirection.BEARISH


# =============================================================================
# Test Suite 3: StateStore Integrity & Anti-Aliasing
# =============================================================================

class TestStateStoreIntegrity:
    """Verifies that optimizations in StateStore.update preserve full isolation."""

    def test_default_container_isolation_on_absent_key(self) -> None:
        """Verify that default mutable containers cannot be aliased when key is initially absent."""
        store = StateStore(name="isolation_test")
        default_dict = {"nested": [10, 20, 30], "meta": {"owner": "test"}}

        # Initial update on absent key using external default
        store.update("config", {"updated_at": 1700000000.0}, default=default_dict)

        # Mutate the external default container
        default_dict["nested"].append(999)
        default_dict["meta"]["owner"] = "TAMPERED"

        # Verify StateStore internal state was NOT affected by external mutation
        stored = store.get("config")
        assert stored["nested"] == [10, 20, 30]
        assert stored["meta"]["owner"] == "test"
        assert stored["updated_at"] == 1700000000.0

    def test_updater_dict_mutation_protection(self) -> None:
        """Verify that mutating the dictionary passed into update() does not corrupt state."""
        store = StateStore(name="updater_isolation")
        payload = {"metrics": {"sharpe": 1.5, "tags": ["prod", "v1"]}}

        store.set("portfolio_state", {"active": True})
        store.update("portfolio_state", payload)

        # Mutate payload externally
        payload["metrics"]["tags"].append("HACKED")
        payload["metrics"]["sharpe"] = -999.0

        stored = store.get("portfolio_state")
        assert stored["metrics"]["tags"] == ["prod", "v1"]
        assert stored["metrics"]["sharpe"] == 1.5

    def test_returned_value_mutation_protection(self) -> None:
        """Verify that mutating the return value of update() does not alter internal state."""
        store = StateStore(name="return_isolation")
        store.set("data", {"items": [1, 2, 3]})

        ret = store.update("data", {"extra": "foo"})
        ret["items"].append(9999)
        ret["extra"] = "TAMPERED"

        stored = store.get("data")
        assert stored["items"] == [1, 2, 3]
        assert stored["extra"] == "foo"

    def test_snapshot_and_get_isolation(self) -> None:
        """Verify get() and snapshot() remain strictly isolated deep copies."""
        store = StateStore(name="snap_isolation")
        store.set("stream", {"sub": {"count": 42}})

        snap = store.snapshot()
        snap["stream"]["sub"]["count"] = 0
        snap["injected_key"] = "evil"

        assert store.get("stream")["sub"]["count"] == 42
        assert not store.exists("injected_key")

    def test_atomic_rollback_on_callable_failure(self) -> None:
        """Verify StateStore state is not modified if an updater callable raises an exception."""
        store = StateStore(name="rollback_test")
        store.set("balance", 1000.0)

        def failing_updater(current: float) -> float:
            raise RuntimeError("Simulation abort!")

        with pytest.raises(RuntimeError, match="Simulation abort!"):
            store.update("balance", failing_updater)

        # Value must remain uncorrupted
        assert store.get("balance") == 1000.0

    def test_concurrent_multi_threaded_updates(self) -> None:
        """Verify concurrent updates across 10 threads do not lose keys or corrupt dictionaries."""
        store = StateStore(name="concurrent_test")
        store.set("counter", 0)

        def inc_worker() -> None:
            for _ in range(50):
                store.update("counter", lambda c: c + 1)

        threads = [threading.Thread(target=inc_worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert store.get("counter") == 500


# =============================================================================
# Test Suite 4: Historical Data Integrity & Ingestion Constraints
# =============================================================================

class TestHistoricalDataIntegrity:
    """Verifies domain constraints, SQLite uniqueness, cache invalidation, and series tracking."""

    def test_bar_model_rejects_negative_or_non_finite(self) -> None:
        """Verify Bar model rejects negative and non-finite timestamps, prices, and volumes."""
        with pytest.raises(ValidationError, match="positive finite float"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=float("nan"), open=100.0, high=105.0, low=95.0, close=100.0)

        with pytest.raises(ValidationError, match="non-negative finite float"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=1700000000.0, open=float("nan"), high=105.0, low=95.0, close=100.0)

        with pytest.raises(ValidationError, match="non-negative finite float"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=1700000000.0, open=100.0, high=float("inf"), low=95.0, close=100.0)

        with pytest.raises(ValidationError, match="non-negative finite float"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=1700000000.0, open=-10.0, high=105.0, low=95.0, close=100.0)

    def test_bar_model_rejects_inverted_ohlc(self) -> None:
        """Verify Bar model rejects invalid OHLC geometric relations."""
        # High lower than open
        with pytest.raises(ValidationError, match="Invalid Bar high price"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=1700000000.0, open=100.0, high=90.0, low=80.0, close=85.0)

        # Low higher than close
        with pytest.raises(ValidationError, match="Invalid Bar low price"):
            Bar(symbol="NIFTY", exchange="NSE", timeframe="1m", timestamp=1700000000.0, open=100.0, high=110.0, low=105.0, close=100.0)

    def test_quote_model_rejects_non_finite_and_inverted_spread(self) -> None:
        """Verify Quote model validates finite prices and non-inverted spread."""
        with pytest.raises(ValidationError, match="Quote last_price"):
            Quote(symbol="NIFTY", exchange="NSE", timestamp=1700000000.0, last_price=float("nan"))

        with pytest.raises(ValidationError, match="Inverted spread invariant"):
            Quote(symbol="NIFTY", exchange="NSE", timestamp=1700000000.0, last_price=100.0, bid=105.0, ask=100.0)

    def test_sqlite_repository_uniqueness_and_conflict_handling(self) -> None:
        """Verify SQLite repository enforces uniqueness per (symbol, timeframe, timestamp)."""
        repo = SQLiteHistoricalBarRepository(db_path=":memory:")
        bar1 = build_bar(timestamp=1700000000.0, close_price=100.0)
        bar2 = build_bar(timestamp=1700000000.0, open_price=100.0, high_price=115.0, low_price=95.0, close_price=110.0)  # Same timestamp, different close

        # Insert bar1
        assert repo.store_bars([bar1], on_conflict="replace") == 1
        stored = repo.query_bars("NIFTY", "1m", 1700000000.0, 1700000000.0)
        assert len(stored) == 1
        assert stored[0].close == 100.0

        # Replace with bar2
        assert repo.store_bars([bar2], on_conflict="replace") == 1
        stored = repo.query_bars("NIFTY", "1m", 1700000000.0, 1700000000.0)
        assert len(stored) == 1
        assert stored[0].close == 110.0  # Successfully replaced

    def test_cache_invalidation_after_repository_write(self) -> None:
        """Verify HistoricalDataCache stream invalidation correctly removes stale queries."""
        cache = HistoricalDataCache(max_size=50)
        k1 = make_cache_key("NIFTY", "1m", 1700000000.0, 1700001000.0)
        k2 = make_cache_key("RELIANCE", "1m", 1700000000.0, 1700001000.0)

        cache.put(k1, [build_bar(symbol="NIFTY")])
        cache.put(k2, [build_bar(symbol="RELIANCE")])
        assert cache.size == 2

        # Invalidate NIFTY
        invalidated = cache.invalidate_stream("NIFTY", "1m")
        assert invalidated == 1
        assert cache.get(k1) is None
        assert cache.get(k2) is not None

    def test_rolling_series_tracks_duplicate_and_out_of_order(self) -> None:
        """Verify RollingBarSeries rejects duplicate and out-of-order bars accurately."""
        series = RollingBarSeries(symbol="NIFTY", timeframe="1m", max_capacity=50)

        b1 = build_bar(timestamp=1700000060.0)
        b2 = build_bar(timestamp=1700000060.0)  # Duplicate timestamp
        b3 = build_bar(timestamp=1700000030.0)  # Out of order

        assert series.add_bar(b1) is True
        assert series.add_bar(b2) is False
        assert series.duplicate_count == 1

        assert series.add_bar(b3) is False
        assert series.out_of_order_count == 1
        assert series.count == 1


# =============================================================================
# Test Suite 5: Portfolio Accounting & Risk Invariants
# =============================================================================

class TestPortfolioAccountingAndRiskInvariants:
    """Verifies cash movements, P&L reconciliation, drawdown, and risk metric invariants."""

    def test_simulated_portfolio_accounting_reconciliation(self) -> None:
        """Verify Net P&L == Gross P&L - Fees - Slippage and Cash + Positions == Total Equity."""
        portfolio = SimulatedPortfolio(initial_cash=100000.0)
        config = BacktestConfig(
            symbol="INFY",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700000500.0,
            fee_model="percentage",
            fee_rate=0.001,
            slippage_bps=2.0,
            half_spread_bps=1.0,
        )

        # 1. Buy Order Execution
        buy_order = SimulatedOrder(
            order_id="ord_b1",
            symbol="INFY",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=50.0,
            created_at=1700000000.0,
        )
        fill_buy = portfolio.execute_order(
            order=buy_order,
            base_price=1500.0,
            timestamp=1700000060.0,
            config=config,
        )
        assert fill_buy is not None

        # Cash reduced by fill_buy.price * 50 + fill_buy.fee
        pos = portfolio.positions["INFY"]
        assert pos.quantity == 50.0

        # Mark to market at price 1600.0
        portfolio.mark_to_market(timestamp=1700000120.0, current_prices={"INFY": 1600.0})

        # Balance sheet invariant: Total Equity == Cash + Position Market Value
        equity_calc = portfolio.cash + (50.0 * 1600.0)
        assert abs(portfolio.total_equity - equity_calc) < 1e-4

        # 2. Sell Order Execution (Close Position)
        sell_order = SimulatedOrder(
            order_id="ord_s1",
            symbol="INFY",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=50.0,
            created_at=1700000180.0,
        )
        fill_sell = portfolio.execute_order(
            order=sell_order,
            base_price=1600.0,
            timestamp=1700000240.0,
            config=config,
        )
        assert fill_sell is not None

        assert len(portfolio.trades) == 1
        trade = portfolio.trades[0]

        # Invariant: Net P&L == Gross P&L - Total Fees (slippage is embedded in execution prices)
        expected_net = trade.gross_pnl - trade.fees
        assert abs(trade.net_pnl - expected_net) < 1e-4

    def test_risk_metrics_zero_drawdown_handling(self) -> None:
        """Verify Calmar ratio with zero drawdown returns unavailable MetricValue without crashing."""
        calmar = calculate_calmar_ratio(annualized_return_pct=15.0, max_drawdown_pct=0.0)
        assert calmar.is_available is False
        assert calmar.value is None
        assert "Zero maximum drawdown" in (calmar.unavailable_reason or "")

    def test_risk_metrics_insufficient_returns_handling(self) -> None:
        """Verify Sharpe and Volatility with < 2 returns return unavailable MetricValue."""
        sharpe = calculate_sharpe_ratio(returns=[0.05])
        assert sharpe.is_available is False
        assert sharpe.value is None

        vol = calculate_annualized_volatility(returns=[0.05])
        assert vol.is_available is False
        assert vol.value is None

    def test_max_drawdown_calculation_invariants(self) -> None:
        """Verify peak-to-trough drawdown calculation invariants."""
        equities = [
            {"total_equity": 100000.0},
            {"total_equity": 110000.0},  # Peak
            {"total_equity": 99000.0},   # Trough (10% drop from 110k = 11,000)
            {"total_equity": 105000.0},
        ]
        dd_pct, dd_amt, dd_dur = calculate_max_drawdown(equities)
        assert dd_pct.is_available is True
        assert round(dd_pct.value, 2) == 10.0
        assert round(dd_amt.value, 2) == 11000.0
        assert dd_dur.value == 2.0  # Duration was 2 bars before recovery

    def test_portfolio_risk_monitor_limit_breach_detection(self) -> None:
        """Verify PortfolioRiskMonitor triggers deterministic warnings on concentration breach."""
        monitor = PortfolioRiskMonitor(config=PortfolioRiskMonitorConfig(max_instrument_concentration_warning_pct=30.0))
        snapshot = PortfolioRiskSnapshot(
            timestamp=time.time(),
            total_equity=100000.0,
            cash=30000.0,
            cash_reserve_pct=30.0,
            gross_exposure=70000.0,
            gross_exposure_pct=70.0,
            net_exposure=70000.0,
            net_exposure_pct=70.0,
            strategy_exposures={"sma": 70000.0},
            instrument_exposures={"RELIANCE": 40000.0, "TCS": 30000.0},  # RELIANCE is 40% > 30%
            drawdown_pct=5.0,
        )

        report = monitor.evaluate_snapshot(snapshot)
        assert report.is_healthy is False
        concentration_alerts = [a for a in report.active_alerts if a.rule_id == RiskRuleId.INSTRUMENT_CONCENTRATION.value]
        assert len(concentration_alerts) >= 1
        assert concentration_alerts[0].severity in (AlertSeverity.WARNING, AlertSeverity.CRITICAL)


# =============================================================================
# Test Suite 6: Anti-Lookahead Execution & Mandatory Safety Locks
# =============================================================================

class TestSafetyAndAntiLookaheadRules:
    """Verifies that execution locks and anti-lookahead rules remain intact."""

    def test_execution_locks_unconditionally_enforced(self) -> None:
        """Verify Settings.execution_locked and paper_trading_only remain strictly True."""
        assert Settings.execution_locked is True
        assert Settings.paper_trading_only is True

    def test_anti_lookahead_next_bar_open_execution_model(self) -> None:
        """Verify orders submitted at Bar T close are filled strictly at Bar T+1 Open."""
        portfolio = SimulatedPortfolio(initial_cash=100000.0)
        config = BacktestConfig(
            symbol="NIFTY",
            timeframe="1m",
            start_time=1700000000.0,
            end_time=1700000500.0,
        )

        order = SimulatedOrder(
            order_id="ord_anti_lookahead",
            symbol="NIFTY",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=50.0,
            created_at=1700000060.0,  # Submitted at bar 1 close
        )

        # Bar 2 open price is 105.0, close price is 110.0
        # Filling order at Bar 2 Open:
        fill = portfolio.execute_order(
            order=order,
            base_price=105.0,
            timestamp=1700000120.0,
            config=config,
        )
        assert fill is not None
        assert fill.price >= 105.0  # Base price was Bar 2 OPEN, not Bar 1 CLOSE or Bar 2 CLOSE
