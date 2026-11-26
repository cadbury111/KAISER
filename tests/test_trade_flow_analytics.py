"""KAISER — Tests for Trade Flow Imbalance, Aggressor Volume Analytics & Consensus Integration (Phase 3 Step 88).

Verifies:
1. TradeTick immutability, field validation, and quantity alias.
2. Explicit aggressor-side classification (BUY_INITIATED, SELL_INITIATED, UNKNOWN).
3. UNKNOWN aggressor volume preservation without misclassification as buy or sell.
4. Trade Flow Imbalance (TFI) bounds [-1.0, 1.0], zero-volume protection, and pressure categories.
5. Rolling window volume metrics and TFI calculations.
6. Exponential Weighted Moving Average (EWMA) of TFI.
7. Duplicate trade ID rejection and fault tolerance.
8. Sequence gap detection and out-of-order trade accounting.
9. Multi-symbol isolation in TradeFlowEngine.
10. Thread safety under concurrent tick processing.
11. Adaptive depth level selection and safety in depth analytics.
12. Read-only consensus-layer feature visibility (report.depth_features, report.trade_flow_features).
13. Strict execution safety invariants (execution_locked=True, broker_provider="none").
"""

from __future__ import annotations

import concurrent.futures
import math
import time
from typing import List

import pytest

from app.agents.consensus.models import AgentConsensusReport, AgentFinding, ConsensusStatus
from app.agents.consensus.service import MultiAgentCoordinationService
from app.agents.regime import MarketRegimeAgent
from app.agents.technical import TechnicalAnalysisAgent
from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.indicators import classify_trade_flow_imbalance, extract_trade_flow_features
from app.analysis.models import SignalDirection
from app.core.config import get_settings
from app.core.events import EventBus, EventType
from app.core.exceptions import ValidationError
from app.market.depth_analytics import (
    AdaptiveDepthProfile,
    L2DepthAnalyticsEngine,
    compute_adaptive_depth_analytics,
    compute_depth_analytics,
)
from app.market.models import AggressorSide, OrderBookLevel, OrderBookSnapshot, TradeTick
from app.market.trade_flow import (
    CombinedPressureDiagnostic,
    TradeFlowAnalytics,
    TradeFlowEngine,
    TradeFlowInterpretation,
    calculate_ewma,
    calculate_trade_flow_imbalance,
    evaluate_combined_microstructure_pressure,
)


def _create_tick(
    symbol: str = "RELIANCE",
    price: float = 2500.0,
    volume: float = 100.0,
    aggressor_side: AggressorSide = AggressorSide.BUY_INITIATED,
    trade_id: str = "t1",
    timestamp: float = 1700000000.0,
    sequence_number: int = 1,
) -> TradeTick:
    """Helper to create a valid TradeTick."""
    return TradeTick(
        symbol=symbol,
        price=price,
        volume=volume,
        timestamp=timestamp,
        received_timestamp=timestamp + 0.005,
        trade_id=trade_id,
        sequence_number=sequence_number,
        aggressor_side=aggressor_side,
        exchange="NSE",
        source="mock_trade",
    )


class TestTradeTickDomainModel:
    """Tests for TradeTick immutability, field validation, and aliases."""

    def test_trade_tick_valid_instantiation_and_aliases(self) -> None:
        """Valid tick instantiates correctly with quantity alias and ISO timestamp."""
        tick = _create_tick(
            symbol="infy",
            price=1500.50,
            volume=250.0,
            aggressor_side=AggressorSide.BUY_INITIATED,
            trade_id="tr_1001",
        )
        assert tick.symbol == "INFY"
        assert tick.price == 1500.50
        assert tick.volume == 250.0
        assert tick.quantity == 250.0  # Alias
        assert tick.aggressor_side == AggressorSide.BUY_INITIATED
        assert tick.trade_id == "tr_1001"
        assert "iso_timestamp" in tick.to_dict()
        assert tick.to_dict()["aggressor_side"] == "BUY_INITIATED"

    def test_trade_tick_immutability(self) -> None:
        """TradeTick is frozen and rejects field mutation."""
        tick = _create_tick()
        with pytest.raises(Exception):
            tick.price = 2600.0  # type: ignore

    def test_trade_tick_validates_positive_price_and_volume(self) -> None:
        """Rejects zero or negative prices and volumes fail-closed."""
        with pytest.raises(ValidationError, match="price must be a finite positive float"):
            _create_tick(price=0.0)
        with pytest.raises(ValidationError, match="price must be a finite positive float"):
            _create_tick(price=-10.0)
        with pytest.raises(ValidationError, match="volume must be a finite positive float"):
            _create_tick(volume=0.0)
        with pytest.raises(ValidationError, match="volume must be a finite positive float"):
            _create_tick(volume=-5.0)

    def test_trade_tick_validates_aggressor_side(self) -> None:
        """Normalizes strings and rejects invalid aggressor sides."""
        tick_str = _create_tick(aggressor_side="sell_initiated")  # type: ignore
        assert tick_str.aggressor_side == AggressorSide.SELL_INITIATED

        with pytest.raises(ValidationError, match="Invalid TradeTick aggressor_side"):
            _create_tick(aggressor_side="INVALID_SIDE")  # type: ignore


class TestTradeFlowImbalanceCalculation:
    """Tests for pure TFI calculation and boundary behaviors."""

    def test_balanced_volume_is_neutral(self) -> None:
        """Equal buy and sell volumes produce TFI = 0.0 and NEUTRAL."""
        tfi, interp = calculate_trade_flow_imbalance(buy_volume=100.0, sell_volume=100.0)
        assert tfi == 0.0
        assert interp == TradeFlowInterpretation.NEUTRAL

    def test_buy_dominant_volume(self) -> None:
        """Buy volume > sell volume produces positive TFI and BUY_PRESSURE."""
        # (300 - 100) / 400 = 200 / 400 = 0.5
        tfi, interp = calculate_trade_flow_imbalance(buy_volume=300.0, sell_volume=100.0)
        assert tfi == 0.5
        assert interp == TradeFlowInterpretation.BUY_PRESSURE

    def test_sell_dominant_volume(self) -> None:
        """Sell volume > buy volume produces negative TFI and SELL_PRESSURE."""
        # (50 - 150) / 200 = -100 / 200 = -0.5
        tfi, interp = calculate_trade_flow_imbalance(buy_volume=50.0, sell_volume=150.0)
        assert tfi == -0.5
        assert interp == TradeFlowInterpretation.SELL_PRESSURE

    def test_extreme_bounds_positive_and_negative_one(self) -> None:
        """100% buy yields +1.0; 100% sell yields -1.0."""
        tfi_buy, _ = calculate_trade_flow_imbalance(buy_volume=500.0, sell_volume=0.0)
        assert tfi_buy == 1.0

        tfi_sell, _ = calculate_trade_flow_imbalance(buy_volume=0.0, sell_volume=500.0)
        assert tfi_sell == -1.0

    def test_zero_classified_volume_no_division_by_zero(self) -> None:
        """Zero volume safely returns 0.0 and NEUTRAL."""
        tfi, interp = calculate_trade_flow_imbalance(buy_volume=0.0, sell_volume=0.0)
        assert tfi == 0.0
        assert interp == TradeFlowInterpretation.NEUTRAL


class TestEWMAAndRollingAnalytics:
    """Tests for EWMA calculation and rolling-window analytics."""

    def test_ewma_calculation(self) -> None:
        """EWMA correctly updates with smoothing factor alpha."""
        # First point: seed is current value
        ewma_0 = calculate_ewma(current_value=0.5, prior_ewma=None, alpha=0.2)
        assert ewma_0 == 0.5

        # Second point: 0.2 * 1.0 + 0.8 * 0.5 = 0.2 + 0.4 = 0.6
        ewma_1 = calculate_ewma(current_value=1.0, prior_ewma=ewma_0, alpha=0.2)
        assert pytest.approx(ewma_1, rel=1e-4) == 0.6

        # Third point: 0.2 * 0.0 + 0.8 * 0.6 = 0.48
        ewma_2 = calculate_ewma(current_value=0.0, prior_ewma=ewma_1, alpha=0.2)
        assert pytest.approx(ewma_2, rel=1e-4) == 0.48

    def test_combined_microstructure_diagnostic(self) -> None:
        """Verifies agreement and divergence between OBI and TFI."""
        # Both positive -> AGREEMENT_BUY
        assert evaluate_combined_microstructure_pressure(0.4, 0.5) == CombinedPressureDiagnostic.AGREEMENT_BUY
        # Both negative -> AGREEMENT_SELL
        assert evaluate_combined_microstructure_pressure(-0.3, -0.4) == CombinedPressureDiagnostic.AGREEMENT_SELL
        # Opposing signs -> DIVERGENCE (absorption)
        assert evaluate_combined_microstructure_pressure(0.3, -0.4) == CombinedPressureDiagnostic.DIVERGENCE_ABSORPTION
        assert evaluate_combined_microstructure_pressure(-0.3, 0.4) == CombinedPressureDiagnostic.DIVERGENCE_ABSORPTION
        # Neutral
        assert evaluate_combined_microstructure_pressure(0.01, 0.4) == CombinedPressureDiagnostic.NEUTRAL


class TestTradeFlowEngine:
    """Tests for stateful TradeFlowEngine operations, deduplication, and fault handling."""

    def test_engine_processes_buy_sell_and_unknown_ticks(self) -> None:
        """Engine accumulates buy, sell, and unknown volumes without misclassification."""
        engine = TradeFlowEngine(rolling_window_size=10, ewma_alpha=0.5)

        # 1. Buy tick
        t1 = _create_tick(symbol="TCS", price=3500.0, volume=100.0, aggressor_side=AggressorSide.BUY_INITIATED, trade_id="t1")
        res1 = engine.process_tick(t1)
        assert res1.buy_aggressor_volume == 100.0
        assert res1.sell_aggressor_volume == 0.0
        assert res1.unknown_volume == 0.0
        assert res1.total_traded_volume == 100.0
        assert res1.trade_flow_imbalance == 1.0

        # 2. Unknown tick (must NOT count as buy or sell)
        t2 = _create_tick(symbol="TCS", price=3501.0, volume=50.0, aggressor_side=AggressorSide.UNKNOWN, trade_id="t2")
        res2 = engine.process_tick(t2)
        assert res2.buy_aggressor_volume == 100.0
        assert res2.sell_aggressor_volume == 0.0
        assert res2.unknown_volume == 50.0
        assert res2.total_traded_volume == 150.0
        assert res2.trade_flow_imbalance == 1.0  # TFI still 1.0 because unknown is excluded from classified ratio

        # 3. Sell tick
        t3 = _create_tick(symbol="TCS", price=3500.0, volume=100.0, aggressor_side=AggressorSide.SELL_INITIATED, trade_id="t3")
        res3 = engine.process_tick(t3)
        assert res3.buy_aggressor_volume == 100.0
        assert res3.sell_aggressor_volume == 100.0
        assert res3.unknown_volume == 50.0
        assert res3.total_traded_volume == 250.0
        assert res3.trade_flow_imbalance == 0.0  # (100 - 100) / 200 = 0.0

    def test_engine_rejects_duplicate_trade_id(self) -> None:
        """Duplicate trade_id is rejected without double counting volume."""
        engine = TradeFlowEngine()
        t1 = _create_tick(symbol="INFY", volume=100.0, trade_id="dup_id_1")
        res1 = engine.process_tick(t1)
        assert res1.total_traded_volume == 100.0
        assert res1.duplicates_rejected == 0

        # Re-send same trade_id
        res2 = engine.process_tick(t1)
        assert res2.total_traded_volume == 100.0
        assert res2.duplicates_rejected == 1
        assert engine.total_ticks_processed == 1

    def test_engine_detects_sequence_gaps_and_out_of_order(self) -> None:
        """Detects sequence gaps and out-of-order ticks gracefully."""
        engine = TradeFlowEngine()
        t1 = _create_tick(symbol="WIPRO", sequence_number=1, trade_id="w1", timestamp=100.0)
        engine.process_tick(t1)

        # Sequence gap: jump to sequence 5
        t2 = _create_tick(symbol="WIPRO", sequence_number=5, trade_id="w2", timestamp=105.0)
        res2 = engine.process_tick(t2)
        assert res2.sequence_gaps_detected == 1

        # Out of order: older timestamp and lower sequence
        t3 = _create_tick(symbol="WIPRO", sequence_number=3, trade_id="w3", timestamp=95.0)
        res3 = engine.process_tick(t3)
        assert res3.out_of_order_count >= 1

    def test_multi_symbol_isolation(self) -> None:
        """Engine maintains strictly isolated states across different symbols."""
        engine = TradeFlowEngine()
        t_rel = _create_tick(symbol="RELIANCE", volume=100.0, aggressor_side=AggressorSide.BUY_INITIATED, trade_id="rel_1")
        t_tcs = _create_tick(symbol="TCS", volume=200.0, aggressor_side=AggressorSide.SELL_INITIATED, trade_id="tcs_1")

        engine.process_tick(t_rel)
        engine.process_tick(t_tcs)

        rel_state = engine.get_latest("RELIANCE")
        tcs_state = engine.get_latest("TCS")

        assert rel_state is not None
        assert tcs_state is not None
        assert rel_state.symbol == "RELIANCE"
        assert rel_state.trade_flow_imbalance == 1.0
        assert tcs_state.symbol == "TCS"
        assert tcs_state.trade_flow_imbalance == -1.0

    def test_concurrent_tick_processing_thread_safety(self) -> None:
        """Concurrent tick processing does not corrupt totals or cause race conditions."""
        engine = TradeFlowEngine()

        def submit_ticks(thread_idx: int) -> None:
            for i in range(25):
                t = _create_tick(
                    symbol="THREAD_TEST",
                    volume=10.0,
                    aggressor_side=AggressorSide.BUY_INITIATED,
                    trade_id=f"th_{thread_idx}_{i}",
                )
                engine.process_tick(t)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(submit_ticks, idx) for idx in range(4)]
            concurrent.futures.wait(futures)

        state = engine.get_latest("THREAD_TEST")
        assert state is not None
        assert state.trade_count == 100
        assert state.total_traded_volume == 1000.0
        assert state.buy_aggressor_volume == 1000.0


class TestAdaptiveDepthSelection:
    """Tests for adaptive depth level selection in depth analytics."""

    def test_adaptive_depth_adapts_to_available_levels(self) -> None:
        """When requested depth exceeds book depth, adapts gracefully to available depth."""
        # Order book with only 3 levels
        snapshot = OrderBookSnapshot(
            symbol="ADAPTIVE_TEST",
            exchange="NSE",
            timestamp=1700000000.0,
            bids=(OrderBookLevel(100.0, 10.0), OrderBookLevel(99.0, 10.0), OrderBookLevel(98.0, 10.0)),
            asks=(OrderBookLevel(101.0, 10.0), OrderBookLevel(102.0, 10.0), OrderBookLevel(103.0, 10.0)),
            source="mock",
            is_valid=True,
        )

        # Request 20 levels (only 3 available)
        analytics = compute_adaptive_depth_analytics(snapshot, requested_levels=20, min_levels=1)
        assert analytics.depth_levels == 3
        assert analytics.is_valid is True
        assert analytics.cumulative_bid_quantity == 30.0
        assert analytics.cumulative_ask_quantity == 30.0

    def test_l2_depth_analytics_engine_adaptive_method(self) -> None:
        """L2DepthAnalyticsEngine.evaluate_adaptive_snapshot works seamlessly."""
        engine = L2DepthAnalyticsEngine()
        snapshot = OrderBookSnapshot(
            symbol="ENGINE_ADAPTIVE",
            exchange="NSE",
            timestamp=1700000000.0,
            bids=(OrderBookLevel(500.0, 50.0),),
            asks=(OrderBookLevel(505.0, 50.0),),
            is_valid=True,
        )
        res = engine.evaluate_adaptive_snapshot(snapshot, requested_levels=10)
        assert res.depth_levels == 1
        assert res.is_valid is True


class TestConsensusLayerMicrostructureVisibility:
    """Verifies that L2 depth and trade flow analytics reach the consensus layer cleanly."""

    def test_specialist_and_consensus_feature_extraction(self) -> None:
        """Verifies feature extraction dictionaries for specialists."""
        engine = TradeFlowEngine()
        tick = _create_tick(symbol="CONSENSUS_SYM", volume=500.0, aggressor_side=AggressorSide.BUY_INITIATED)
        analytics = engine.process_tick(tick)

        feats = extract_trade_flow_features(analytics)
        assert feats["trade_flow_available"] is True
        assert feats["trade_flow_imbalance"] == 1.0
        assert feats["buy_aggressor_volume"] == 500.0
        assert feats["trade_flow_pressure"] == "BUY_PRESSURE"

        # None input safe fallback
        none_feats = extract_trade_flow_features(None)
        assert none_feats["trade_flow_available"] is False
        assert none_feats["trade_flow_imbalance"] == 0.0

    def test_consensus_report_microstructure_visibility(self) -> None:
        """AgentConsensusReport exposes depth_features and trade_flow_features via clean accessors."""
        tech_finding = AgentFinding(
            agent_name="technical_analysis_specialist",
            agent_type="technical_analysis",
            posture="BULLISH",
            confidence="HIGH",
            summary="Technical indicators bullish.",
            facts={
                "timeframes": {"1m": {}},
                "depth": {
                    "depth_available": True,
                    "order_book_imbalance": 0.45,
                    "micro_price": 2505.20,
                },
                "trade_flow": {
                    "trade_flow_available": True,
                    "trade_flow_imbalance": 0.60,
                    "tfi_ewma": 0.55,
                },
            },
            is_valid=True,
        )

        regime_finding = AgentFinding(
            agent_name="market_regime_agent",
            agent_type="market_regime",
            posture="BULLISH",
            confidence="HIGH",
            summary="Market regime trending.",
            facts={},
            is_valid=True,
        )

        report = AgentConsensusReport(
            consensus_id="cons_test_101",
            symbol="RELIANCE",
            timestamp=1700000000.0,
            iso_timestamp="2023-11-14T22:13:20+00:00",
            status=ConsensusStatus.AGREEMENT,
            synthesized_posture="BULLISH",
            participating_agents=["technical_analysis_specialist", "market_regime_agent"],
            agent_findings={
                "technical_analysis_specialist": tech_finding,
                "market_regime_agent": regime_finding,
            },
            agreement_points=["Both agents agree on BULLISH posture."],
            disagreement_points=[],
            limitations=[],
            voice_summary="Specialist consensus reached for RELIANCE: BULLISH.",
        )

        # Verify accessors on consensus report
        assert report.depth_features is not None
        assert report.depth_features["order_book_imbalance"] == 0.45
        assert report.depth_features["micro_price"] == 2505.20

        assert report.trade_flow_features is not None
        assert report.trade_flow_features["trade_flow_imbalance"] == 0.60
        assert report.trade_flow_features["tfi_ewma"] == 0.55

        context = report.microstructure_context
        assert "depth" in context
        assert "trade_flow" in context

        # Decision authority and status remain unchanged
        assert report.status == ConsensusStatus.AGREEMENT
        assert report.synthesized_posture == "BULLISH"


class TestExecutionSafetyInvariants:
    """Verifies that Phase 3 Step 88 strictly preserves execution lock and broker isolation."""

    def test_safety_invariants_strictly_enforced(self) -> None:
        """Confirm execution_locked=True, broker_provider='none', paper_execution_enabled=False."""
        settings = get_settings()
        assert settings.execution_locked is True, "execution_locked must remain strictly True"
        assert settings.broker_provider == "none", "broker_provider must remain strictly 'none'"
        assert settings.paper_execution_enabled is False, "paper_execution_enabled must remain strictly False"
