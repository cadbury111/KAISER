"""KAISER — Tests for Level 2 Market Depth Analytics & Imbalance Indicators (Phase 3 Step 87).

Verifies:
1. Balanced order book liquidity (OBI == 0.0, NEUTRAL).
2. Bid-dominant order book liquidity (OBI > 0.0, BUY_PRESSURE).
3. Ask-dominant order book liquidity (OBI < 0.0, SELL_PRESSURE).
4. Strict mathematical bounds [-1.0, 1.0] across extreme asymmetry.
5. Empty books and zero liquidity handling (returns 0.0, never NaN/Inf or ZeroDivisionError).
6. Configurable depth limits (k=1, 5, 10, all).
7. Cumulative quantities and depth slope (Delta Q / Delta P) on bid and ask ladders.
8. Stoikov micro-price calculation, volume weighting, and crossed/locked book rejection.
9. Absolute, relative (bps), and volume-weighted depth spread analytics.
10. Immutability, fail-closed validation, and serialization.
11. Multi-instrument depth analytics engine coordination and EventBus dispatch.
12. Read-only specialist consensus and TechnicalAnalysisEngine integration.
13. Strict execution safety invariants (execution_locked=True, broker_provider="none").
"""

from __future__ import annotations

import math
import time
from typing import List

import pytest

from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.indicators import classify_order_book_imbalance, extract_depth_features
from app.analysis.models import IndicatorType, SignalDirection
from app.core.config import Settings, get_settings
from app.core.events import EventBus, EventType
from app.core.exceptions import ValidationError
from app.market.depth_analytics import (
    DepthSlope,
    ImbalanceInterpretation,
    L2DepthAnalyticsEngine,
    MarketDepthAnalytics,
    MicroPrice,
    OrderBookImbalance,
    WeightedSpread,
    calculate_depth_slope,
    calculate_micro_price,
    calculate_order_book_imbalance,
    calculate_weighted_spread,
    compute_depth_analytics,
)
from app.market.models import OrderBookLevel, OrderBookSnapshot


def _create_snapshot(
    symbol: str = "RELIANCE",
    bids: List[tuple[float, float]] = None,
    asks: List[tuple[float, float]] = None,
    timestamp: float = 1700000000.0,
    is_valid: bool = True,
) -> OrderBookSnapshot:
    """Helper to create test OrderBookSnapshot."""
    bid_levels = tuple(OrderBookLevel(price=p, quantity=q) for p, q in (bids or []))
    ask_levels = tuple(OrderBookLevel(price=p, quantity=q) for p, q in (asks or []))
    return OrderBookSnapshot(
        symbol=symbol,
        exchange="NSE",
        timestamp=timestamp,
        bids=bid_levels,
        asks=ask_levels,
        source="mock_depth",
        is_valid=is_valid,
    )


class TestOrderBookImbalance:
    """Tests for OrderBookImbalance (OBI) calculation and bounds."""

    def test_balanced_liquidity_is_neutral(self) -> None:
        """Equal bid and ask quantities must yield OBI = 0.0 and NEUTRAL."""
        snapshot = _create_snapshot(
            bids=[(100.0, 50.0), (99.0, 50.0)],
            asks=[(101.0, 50.0), (102.0, 50.0)],
        )
        obi = calculate_order_book_imbalance(snapshot)
        assert obi.value == 0.0
        assert obi.interpretation == ImbalanceInterpretation.NEUTRAL
        assert obi.is_neutral is True
        assert obi.is_buy_pressure is False
        assert obi.is_sell_pressure is False
        assert obi.bid_quantity == 100.0
        assert obi.ask_quantity == 100.0
        assert obi.total_quantity == 200.0

    def test_bid_dominant_liquidity(self) -> None:
        """Bid quantity > ask quantity must yield positive OBI and BUY_PRESSURE."""
        snapshot = _create_snapshot(
            bids=[(100.0, 150.0)],
            asks=[(101.0, 50.0)],
        )
        obi = calculate_order_book_imbalance(snapshot)
        # (150 - 50) / (150 + 50) = 100 / 200 = 0.5
        assert obi.value == 0.5
        assert obi.interpretation == ImbalanceInterpretation.BUY_PRESSURE
        assert obi.is_buy_pressure is True
        assert obi.is_neutral is False
        assert obi.is_sell_pressure is False

    def test_ask_dominant_liquidity(self) -> None:
        """Ask quantity > bid quantity must yield negative OBI and SELL_PRESSURE."""
        snapshot = _create_snapshot(
            bids=[(100.0, 20.0)],
            asks=[(101.0, 80.0)],
        )
        obi = calculate_order_book_imbalance(snapshot)
        # (20 - 80) / (20 + 80) = -60 / 100 = -0.6
        assert obi.value == -0.6
        assert obi.interpretation == ImbalanceInterpretation.SELL_PRESSURE
        assert obi.is_sell_pressure is True
        assert obi.is_buy_pressure is False

    def test_extreme_bounds_positive_and_negative_one(self) -> None:
        """Only bids yields +1.0; only asks yields -1.0."""
        # Only bids
        snap_bids_only = _create_snapshot(bids=[(100.0, 100.0)], asks=[])
        obi_bids = calculate_order_book_imbalance(snap_bids_only)
        assert obi_bids.value == 1.0
        assert obi_bids.interpretation == ImbalanceInterpretation.BUY_PRESSURE

        # Only asks
        snap_asks_only = _create_snapshot(bids=[], asks=[(101.0, 100.0)])
        obi_asks = calculate_order_book_imbalance(snap_asks_only)
        assert obi_asks.value == -1.0
        assert obi_asks.interpretation == ImbalanceInterpretation.SELL_PRESSURE

    def test_empty_snapshot_zero_liquidity_no_division_error(self) -> None:
        """Completely empty order book returns 0.0 without ZeroDivisionError."""
        snapshot = _create_snapshot(bids=[], asks=[])
        obi = calculate_order_book_imbalance(snapshot)
        assert obi.value == 0.0
        assert obi.interpretation == ImbalanceInterpretation.NEUTRAL
        assert obi.bid_quantity == 0.0
        assert obi.ask_quantity == 0.0
        assert obi.total_quantity == 0.0

    def test_zero_quantity_levels_handled_safely(self) -> None:
        """OrderBookLevels with 0.0 quantity are handled safely."""
        snapshot = _create_snapshot(
            bids=[(100.0, 0.0)],
            asks=[(101.0, 0.0)],
        )
        obi = calculate_order_book_imbalance(snapshot)
        assert obi.value == 0.0
        assert obi.interpretation == ImbalanceInterpretation.NEUTRAL

    def test_configurable_depth_limits(self) -> None:
        """Depth limit restricts evaluated levels strictly to top-k."""
        snapshot = _create_snapshot(
            bids=[(100.0, 10.0), (99.0, 20.0), (98.0, 50.0)],
            asks=[(101.0, 10.0), (102.0, 40.0), (103.0, 100.0)],
        )
        # Depth limit k=1: top-of-book (10 vs 10 -> balanced)
        obi_k1 = calculate_order_book_imbalance(snapshot, depth_limit=1)
        assert obi_k1.depth_levels == 1
        assert obi_k1.value == 0.0
        assert obi_k1.bid_quantity == 10.0
        assert obi_k1.ask_quantity == 10.0

        # Depth limit k=2: top 2 levels (bids: 10+20=30, asks: 10+40=50) -> (30-50)/80 = -0.25
        obi_k2 = calculate_order_book_imbalance(snapshot, depth_limit=2)
        assert obi_k2.depth_levels == 2
        assert obi_k2.value == -0.25
        assert obi_k2.bid_quantity == 30.0
        assert obi_k2.ask_quantity == 50.0

    def test_domain_model_invariants_and_serialization(self) -> None:
        """OrderBookImbalance validates boundaries and serializes to clean dict."""
        obi = OrderBookImbalance(
            symbol="INFY",
            value=0.25,
            bid_quantity=100.0,
            ask_quantity=60.0,
            depth_levels=5,
            interpretation=ImbalanceInterpretation.BUY_PRESSURE,
            timestamp=1700000000.0,
        )
        d = obi.to_dict()
        assert d["symbol"] == "INFY"
        assert d["value"] == 0.25
        assert d["interpretation"] == "BUY_PRESSURE"
        assert d["total_quantity"] == 160.0
        assert "iso_timestamp" in d

        # Frozen immutability check
        with pytest.raises(Exception):
            obi.value = 0.5  # type: ignore

        # Out of bounds check
        with pytest.raises(ValidationError):
            OrderBookImbalance(
                symbol="INFY",
                value=1.5,  # > 1.0 invalid
                bid_quantity=10.0,
                ask_quantity=10.0,
                depth_levels=1,
                interpretation=ImbalanceInterpretation.BUY_PRESSURE,
                timestamp=100.0,
            )


class TestCumulativeDepthSlope:
    """Tests for cumulative depth slope (Delta Q / Delta P)."""

    def test_depth_slope_calculation(self) -> None:
        """Valid ladder calculates bid slope, ask slope, and normalized ratio."""
        snapshot = _create_snapshot(
            bids=[(100.0, 50.0), (99.0, 100.0), (98.0, 150.0)],  # span = 100 - 98 = 2.0, qty = 300 -> slope = 150.0
            asks=[(101.0, 25.0), (102.0, 50.0), (103.0, 75.0)],  # span = 103 - 101 = 2.0, qty = 150 -> slope = 75.0
        )
        slope = calculate_depth_slope(snapshot, depth_limit=3)
        assert slope.bid_slope == 150.0
        assert slope.ask_slope == 75.0
        assert slope.bid_depth_span == 2.0
        assert slope.ask_depth_span == 2.0
        # Ratio = (150 - 75) / (150 + 75) = 75 / 225 = 0.333333
        assert pytest.approx(slope.ratio, rel=1e-3) == 0.333333

    def test_single_level_or_empty_ladder_slope_is_zero(self) -> None:
        """Ladder with only 1 level has zero price span; slope safely defaults to 0.0."""
        snapshot = _create_snapshot(
            bids=[(100.0, 50.0)],
            asks=[(101.0, 50.0)],
        )
        slope = calculate_depth_slope(snapshot)
        assert slope.bid_slope == 0.0
        assert slope.ask_slope == 0.0
        assert slope.ratio == 0.0


class TestMicroPrice:
    """Tests for Stoikov micro-price calculation."""

    def test_micro_price_shifts_towards_ask_when_bids_dominate(self) -> None:
        """When bid size > ask size, micro-price is higher than mid-price (pulled toward ask)."""
        # best_bid=100 (qty=300), best_ask=102 (qty=100) -> mid=101.0
        # micro_price = (102*300 + 100*100) / 400 = (30600 + 10000) / 400 = 40600 / 400 = 101.5
        snapshot = _create_snapshot(
            bids=[(100.0, 300.0)],
            asks=[(102.0, 100.0)],
        )
        mp = calculate_micro_price(snapshot)
        assert mp.is_valid is True
        assert mp.mid_price == 101.0
        assert mp.price == 101.5
        assert mp.bid_weight == 0.75
        assert mp.ask_weight == 0.25
        assert mp.price > mp.mid_price

    def test_micro_price_shifts_towards_bid_when_asks_dominate(self) -> None:
        """When ask size > bid size, micro-price is lower than mid-price (pulled toward bid)."""
        # best_bid=100 (qty=100), best_ask=102 (qty=300) -> mid=101.0
        # micro_price = (102*100 + 100*300) / 400 = (10200 + 30000) / 400 = 40200 / 400 = 100.5
        snapshot = _create_snapshot(
            bids=[(100.0, 100.0)],
            asks=[(102.0, 300.0)],
        )
        mp = calculate_micro_price(snapshot)
        assert mp.is_valid is True
        assert mp.price == 100.5
        assert mp.price < mp.mid_price

    def test_micro_price_equals_mid_when_sizes_equal(self) -> None:
        """Equal bid and ask sizes yield micro-price equal to mid-price."""
        snapshot = _create_snapshot(
            bids=[(100.0, 100.0)],
            asks=[(102.0, 100.0)],
        )
        mp = calculate_micro_price(snapshot)
        assert mp.is_valid is True
        assert mp.price == 101.0
        assert mp.price == mp.mid_price

    def test_micro_price_rejects_crossed_or_locked_book(self) -> None:
        """Crossed book (bid >= ask) is rejected with is_valid=False and price=None."""
        # Locked book (bid == ask)
        snap_locked = _create_snapshot(
            bids=[(100.0, 50.0)],
            asks=[(100.0, 50.0)],
        )
        mp_locked = calculate_micro_price(snap_locked)
        assert mp_locked.is_valid is False
        assert mp_locked.price is None

        # Inverted / crossed book (bid > ask)
        # Note: OrderBookSnapshot __post_init__ allows creating snapshot for analysis testing
        snap_crossed = _create_snapshot(
            bids=[(105.0, 50.0)],
            asks=[(100.0, 50.0)],
        )
        mp_crossed = calculate_micro_price(snap_crossed)
        assert mp_crossed.is_valid is False
        assert mp_crossed.price is None

    def test_micro_price_zero_liquidity_fallback(self) -> None:
        """Zero top-of-book liquidity falls back safely to mid-price."""
        snapshot = _create_snapshot(
            bids=[(100.0, 0.0)],
            asks=[(102.0, 0.0)],
        )
        mp = calculate_micro_price(snapshot)
        assert mp.is_valid is True
        assert mp.price == 101.0


class TestWeightedSpread:
    """Tests for absolute, relative bps, and volume-weighted spread metrics."""

    def test_weighted_spread_metrics(self) -> None:
        """Verify absolute spread, bps relative to mid, and depth VWAP spread."""
        # best_bid=100.0, best_ask=101.0 -> abs_spread = 1.0, mid = 100.5
        # relative_spread_bps = (1.0 / 100.5) * 10000 = 99.50 bps
        # bids: (100.0, 10) + (99.0, 10) -> vwap_bid = (1000 + 990)/20 = 99.5
        # asks: (101.0, 10) + (102.0, 10) -> vwap_ask = (1010 + 1020)/20 = 101.5
        # volume_weighted_spread = 101.5 - 99.5 = 2.0
        snapshot = _create_snapshot(
            bids=[(100.0, 10.0), (99.0, 10.0)],
            asks=[(101.0, 10.0), (102.0, 10.0)],
        )
        ws = calculate_weighted_spread(snapshot, depth_limit=2)
        assert ws.absolute_spread == 1.0
        assert ws.relative_spread_bps == 99.50
        assert ws.vwap_bid == 99.5
        assert ws.vwap_ask == 101.5
        assert ws.volume_weighted_spread == 2.0


class TestMarketDepthAnalyticsAggregate:
    """Tests for compute_depth_analytics aggregate and engine."""

    def test_compute_depth_analytics_valid_snapshot(self) -> None:
        """Valid snapshot produces full aggregate MarketDepthAnalytics."""
        snapshot = _create_snapshot(
            symbol="TCS",
            bids=[(3500.0, 100.0), (3495.0, 200.0)],
            asks=[(3505.0, 100.0), (3510.0, 150.0)],
        )
        analytics = compute_depth_analytics(snapshot, depth_limit=5)
        assert analytics.symbol == "TCS"
        assert analytics.is_valid is True
        assert analytics.error is None
        assert analytics.imbalance is not None
        assert analytics.micro_price is not None
        assert analytics.depth_slope is not None
        assert analytics.weighted_spread is not None
        assert analytics.cumulative_bid_quantity == 300.0
        assert analytics.cumulative_ask_quantity == 250.0

        d = analytics.to_dict()
        assert d["symbol"] == "TCS"
        assert d["is_valid"] is True
        assert "imbalance" in d
        assert "micro_price" in d

    def test_compute_depth_analytics_fails_closed_on_crossed_book(self) -> None:
        """Crossed book is flagged is_valid=False with diagnostic error."""
        snapshot = _create_snapshot(
            symbol="TCS",
            bids=[(3510.0, 100.0)],
            asks=[(3500.0, 100.0)],  # best_bid > best_ask
        )
        analytics = compute_depth_analytics(snapshot)
        assert analytics.is_valid is False
        assert "crossed" in (analytics.error or "").lower()

    def test_l2_depth_analytics_engine_multi_symbol_coordination(self) -> None:
        """Engine processes snapshots for multiple symbols independently."""
        bus = EventBus("test_bus")
        received_events = []
        bus.subscribe(
            EventType.MARKET_DEPTH_ANALYTICS_UPDATED,
            lambda e: received_events.append(e),
        )

        engine = L2DepthAnalyticsEngine(event_bus=bus)

        snap_rel = _create_snapshot(symbol="RELIANCE", bids=[(2500.0, 100.0)], asks=[(2505.0, 50.0)])
        snap_tcs = _create_snapshot(symbol="TCS", bids=[(3500.0, 50.0)], asks=[(3505.0, 150.0)])

        res_rel = engine.evaluate_snapshot(snap_rel)
        res_tcs = engine.evaluate_snapshot(snap_tcs)

        assert res_rel.symbol == "RELIANCE"
        assert res_rel.imbalance.is_buy_pressure is True
        assert res_tcs.symbol == "TCS"
        assert res_tcs.imbalance.is_sell_pressure is True

        assert engine.get_latest("RELIANCE") == res_rel
        assert engine.get_latest("TCS") == res_tcs
        assert engine.total_evaluated == 2
        assert len(received_events) == 2

        # Verify history
        hist = engine.get_history("RELIANCE")
        assert len(hist) == 1
        assert hist[0].symbol == "RELIANCE"

        # Clear engine
        engine.clear()
        assert engine.total_evaluated == 0
        assert engine.get_latest("RELIANCE") is None


class TestSpecialistConsensusIntegration:
    """Tests read-only feature extraction and specialist consensus integration."""

    def test_extract_depth_features_from_valid_analytics(self) -> None:
        """Extracts dictionary of clean features from MarketDepthAnalytics."""
        snapshot = _create_snapshot(
            symbol="INFY",
            bids=[(1500.0, 200.0), (1495.0, 100.0)],
            asks=[(1502.0, 100.0), (1505.0, 100.0)],
        )
        analytics = compute_depth_analytics(snapshot)
        feats = extract_depth_features(analytics)

        assert feats["depth_available"] is True
        assert feats["order_book_imbalance"] > 0
        assert feats["order_book_pressure"] == "BUY_PRESSURE"
        assert feats["micro_price"] is not None
        assert feats["cumulative_bid_quantity"] == 300.0
        assert feats["cumulative_ask_quantity"] == 200.0

    def test_extract_depth_features_none_safe_defaults(self) -> None:
        """Extracting features from None safely returns neutral defaults."""
        feats = extract_depth_features(None)
        assert feats["depth_available"] is False
        assert feats["order_book_imbalance"] == 0.0
        assert feats["order_book_pressure"] == "NEUTRAL"
        assert feats["micro_price"] is None

    def test_classify_order_book_imbalance_signals(self) -> None:
        """Classification maps OBI values to BULLISH, BEARISH, or NEUTRAL."""
        assert classify_order_book_imbalance(0.20) == SignalDirection.BULLISH
        assert classify_order_book_imbalance(-0.20) == SignalDirection.BEARISH
        assert classify_order_book_imbalance(0.01) == SignalDirection.NEUTRAL
        assert classify_order_book_imbalance(None) == SignalDirection.NEUTRAL
        assert classify_order_book_imbalance(float("nan")) == SignalDirection.NEUTRAL

    def test_technical_analysis_engine_depth_integration(self) -> None:
        """TechnicalAnalysisEngine caches depth analytics and extracts features."""
        engine = TechnicalAnalysisEngine()
        assert engine.get_depth_analytics("RELIANCE") is None

        # Features default to depth_available=False
        init_feats = engine.get_depth_features("RELIANCE")
        assert init_feats["depth_available"] is False

        # Populate analytics
        snapshot = _create_snapshot(
            symbol="RELIANCE",
            bids=[(2500.0, 120.0)],
            asks=[(2502.0, 80.0)],
        )
        analytics = compute_depth_analytics(snapshot)
        engine.set_depth_analytics(analytics)

        assert engine.get_depth_analytics("RELIANCE") == analytics
        upd_feats = engine.get_depth_features("RELIANCE")
        assert upd_feats["depth_available"] is True
        assert upd_feats["order_book_imbalance"] == 0.20
        assert upd_feats["order_book_pressure"] == "BUY_PRESSURE"


class TestExecutionSafetyInvariants:
    """Verifies that Phase 3 Step 87 preserves all non-negotiable safety invariants."""

    def test_safety_invariants_strictly_enforced(self) -> None:
        """Confirm execution_locked=True, broker_provider='none', paper_execution_enabled=False."""
        settings = get_settings()
        assert settings.execution_locked is True, "execution_locked must remain strictly True"
        assert settings.broker_provider == "none", "broker_provider must remain strictly 'none'"
        assert settings.paper_execution_enabled is False, "paper_execution_enabled must remain strictly False"
