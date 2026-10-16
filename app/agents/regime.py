"""KAISER — Market Regime Specialist Agent.

Deterministic specialist agent that analyzes macro/micro market regimes, trend strength,
volatility states (compression vs expansion), and structural price conditions across timeframes.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Analytical only: ZERO direct trade execution, order routing, or broker interfaces.
2. Evidence-grounded: Every regime classification is tied to verifiable mathematical facts.
3. Strict non-prescriptive contract: Absolutely no BUY, SELL, position sizing, or target prices.
4. Independent failure containment: Internal errors never crash orchestrator or other agents.
5. Quality-gated: Insufficient or unhealthy bar data explicitly yields INSUFFICIENT_DATA.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from enum import Enum
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid

from app.agents.base import BaseAgent
from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.models import (
    IndicatorType,
    SUPPORTED_TIMEFRAMES,
    TechnicalFeatureVector,
    TimeframeHealth,
    TimeframeHealthStatus,
    normalize_timeframe,
)
from app.analysis.opportunities.models import validate_no_prohibited_fields
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.quality import MarketDataQualityEngine
from app.orchestrator.models import OrchestratorRequest

logger = get_logger("agents.regime")


# -----------------------------------------------------------------------------
# Domain Enums
# -----------------------------------------------------------------------------

class MarketRegime(str, Enum):
    """Structural market regime classification."""

    BULLISH_TRENDING = "BULLISH_TRENDING"
    BEARISH_TRENDING = "BEARISH_TRENDING"
    RANGING_COMPRESSED = "RANGING_COMPRESSED"
    RANGING_VOLATILE = "RANGING_VOLATILE"
    NEUTRAL_TRANSITIONAL = "NEUTRAL_TRANSITIONAL"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class RegimePosture(str, Enum):
    """Directional bias of the market regime."""

    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    CONFLICTING = "CONFLICTING"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class VolatilityState(str, Enum):
    """Volatility expansion or compression state."""

    COMPRESSED = "COMPRESSED"
    NORMAL = "NORMAL"
    EXPANDED = "EXPANDED"
    EXTREME = "EXTREME"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class TrendStrength(str, Enum):
    """Quantitative trend momentum tier."""

    STRONG = "STRONG"
    MODERATE = "MODERATE"
    WEAK = "WEAK"
    NONE = "NONE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


# -----------------------------------------------------------------------------
# Structured Report Models
# -----------------------------------------------------------------------------

class TimeframeRegimeEvaluation:
    """Detailed regime assessment for a single timeframe."""

    def __init__(
        self,
        timeframe: str,
        regime: MarketRegime,
        posture: RegimePosture,
        volatility_state: VolatilityState,
        trend_strength: TrendStrength,
        bar_count: int,
        last_close: float,
        bandwidth: Optional[float] = None,
        atr_ratio: Optional[float] = None,
        trend_spread: Optional[float] = None,
        evidence: Optional[Sequence[str]] = None,
        warnings: Optional[Sequence[str]] = None,
        facts: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.timeframe = normalize_timeframe(timeframe)
        self.regime = regime
        self.posture = posture
        self.volatility_state = volatility_state
        self.trend_strength = trend_strength
        self.bar_count = int(bar_count)
        self.last_close = float(last_close)
        self.bandwidth = float(bandwidth) if bandwidth is not None else None
        self.atr_ratio = float(atr_ratio) if atr_ratio is not None else None
        self.trend_spread = float(trend_spread) if trend_spread is not None else None
        self.evidence = tuple(str(e) for e in (evidence or ()))
        self.warnings = tuple(str(w) for w in (warnings or ()))
        self.facts = copy.deepcopy(facts or {})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "regime": self.regime.value,
            "posture": self.posture.value,
            "volatility_state": self.volatility_state.value,
            "trend_strength": self.trend_strength.value,
            "bar_count": self.bar_count,
            "last_close": self.last_close,
            "bandwidth": self.bandwidth,
            "atr_ratio": self.atr_ratio,
            "trend_spread": self.trend_spread,
            "evidence": list(self.evidence),
            "warnings": list(self.warnings),
            "facts": copy.deepcopy(self.facts),
        }


class MarketRegimeReport:
    """Immutable aggregate market regime intelligence report."""

    def __init__(
        self,
        symbol: str,
        timestamp: float,
        iso_timestamp: str,
        overall_regime: MarketRegime,
        overall_posture: RegimePosture,
        overall_volatility: VolatilityState,
        overall_trend_strength: TrendStrength,
        timeframe_evaluations: Dict[str, TimeframeRegimeEvaluation],
        timeframe_coverage: Dict[str, str],
        facts: Dict[str, Any],
        interpretation: str,
        uncertainty_caveats: Sequence[str],
        voice_summary: str,
    ) -> None:
        self.symbol = symbol.strip().upper()
        self.timestamp = float(timestamp)
        self.iso_timestamp = str(iso_timestamp)
        self.overall_regime = overall_regime
        self.overall_posture = overall_posture
        self.overall_volatility = overall_volatility
        self.overall_trend_strength = overall_trend_strength
        self.timeframe_evaluations = copy.deepcopy(timeframe_evaluations)
        self.timeframe_coverage = copy.deepcopy(timeframe_coverage)
        self.facts = copy.deepcopy(facts)
        self.interpretation = str(interpretation)
        self.uncertainty_caveats = tuple(str(c) for c in uncertainty_caveats)
        self.voice_summary = str(voice_summary)

        # Enforce non-prescriptive safety invariant
        validate_no_prohibited_fields(self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "overall_regime": self.overall_regime.value,
            "overall_posture": self.overall_posture.value,
            "overall_volatility": self.overall_volatility.value,
            "overall_trend_strength": self.overall_trend_strength.value,
            "timeframe_evaluations": {k: v.to_dict() for k, v in self.timeframe_evaluations.items()},
            "timeframe_coverage": copy.deepcopy(self.timeframe_coverage),
            "facts": copy.deepcopy(self.facts),
            "interpretation": self.interpretation,
            "uncertainty_caveats": list(self.uncertainty_caveats),
            "voice_summary": self.voice_summary,
        }


# -----------------------------------------------------------------------------
# Market Regime Specialist Agent
# -----------------------------------------------------------------------------

class MarketRegimeAgent(BaseAgent):
    """Specialist intelligence agent for structural market regime and volatility assessment."""

    def __init__(
        self,
        analysis_engine: TechnicalAnalysisEngine,
        quality_engine: Optional[MarketDataQualityEngine] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        name: str = "market_regime_agent",
    ) -> None:
        super().__init__(name=name, event_bus=event_bus, state_store=state_store)
        self.analysis_engine = analysis_engine
        self.quality_engine = quality_engine
        self._evaluations_count = 0

    @property
    def agent_type(self) -> str:
        return "market_regime"

    def get_status(self) -> Dict[str, Any]:
        """Return operational telemetry."""
        with self._lock:
            return {
                "name": self.name,
                "agent_type": self.agent_type,
                "evaluations_count": self._evaluations_count,
                "analysis_engine": self.analysis_engine.name,
                "supported_timeframes": list(SUPPORTED_TIMEFRAMES),
            }

    def generate_report(
        self,
        symbol: str,
        timeframes: Optional[Sequence[str]] = None,
        reference_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> MarketRegimeReport:
        """Evaluate structural market regime across timeframes and produce an immutable report."""
        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("MarketRegimeAgent symbol must be a non-empty string.")

        clean_symbol = symbol.strip().upper()
        now = time.time() if reference_time is None else reference_time
        cid = correlation_id or uuid.uuid4().hex

        target_tfs: List[str] = []
        if timeframes:
            for tf in timeframes:
                target_tfs.append(normalize_timeframe(tf))
        else:
            target_tfs = list(SUPPORTED_TIMEFRAMES)

        with self._lock:
            self._evaluations_count += 1

        # Multi-timeframe health audit
        sync_state = self.analysis_engine.get_multi_timeframe_sync(
            symbol=clean_symbol,
            timeframes=target_tfs,
            reference_time=now,
        )

        timeframe_coverage: Dict[str, str] = {}
        evaluations: Dict[str, TimeframeRegimeEvaluation] = {}
        all_facts: Dict[str, Any] = {
            "symbol": clean_symbol,
            "reference_timestamp": now,
            "timeframes": {},
        }

        # Evaluate each timeframe
        for tf in target_tfs:
            health = sync_state.timeframes.get(tf)
            if not health or health.bar_count == 0:
                timeframe_coverage[tf] = "MISSING"
                continue

            if health.status == TimeframeHealthStatus.STALE:
                timeframe_coverage[tf] = "STALE"
            elif health.status in (TimeframeHealthStatus.UNHEALTHY, TimeframeHealthStatus.DEGRADED):
                timeframe_coverage[tf] = health.status.value
            elif health.status == TimeframeHealthStatus.INSUFFICIENT_DATA or health.bar_count < 20:
                timeframe_coverage[tf] = "INSUFFICIENT_DATA"
            else:
                timeframe_coverage[tf] = "AVAILABLE"

            vector = self.analysis_engine.get_feature_vector(clean_symbol, tf)
            tf_eval = self._evaluate_timeframe_regime(
                symbol=clean_symbol,
                timeframe=tf,
                health=health,
                vector=vector,
            )
            evaluations[tf] = tf_eval
            all_facts["timeframes"][tf] = tf_eval.facts

        # Aggregate overall regime & posture
        overall_regime, overall_posture, overall_vol, overall_strength = self._synthesize_overall_regime(
            evaluations=evaluations,
            coverage=timeframe_coverage,
        )

        uncertainty_caveats = self._generate_uncertainty_caveats(
            coverage=timeframe_coverage,
            overall_regime=overall_regime,
            evaluations=evaluations,
        )

        voice_summary = self._generate_voice_summary(
            symbol=clean_symbol,
            overall_regime=overall_regime,
            overall_posture=overall_posture,
            overall_vol=overall_vol,
            overall_strength=overall_strength,
            evaluations=evaluations,
        )

        iso_str = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
        report = MarketRegimeReport(
            symbol=clean_symbol,
            timestamp=now,
            iso_timestamp=iso_str,
            overall_regime=overall_regime,
            overall_posture=overall_posture,
            overall_volatility=overall_vol,
            overall_trend_strength=overall_strength,
            timeframe_evaluations=evaluations,
            timeframe_coverage=timeframe_coverage,
            facts=all_facts,
            interpretation=f"Market regime for {clean_symbol} evaluated as {overall_regime.value} with {overall_vol.value} volatility.",
            uncertainty_caveats=uncertainty_caveats,
            voice_summary=voice_summary,
        )

        # StateStore caching
        if self.state_store:
            self.state_store.update(
                "market_regime_reports",
                {clean_symbol: report.to_dict()},
                default={},
            )

        return report

    def _evaluate_timeframe_regime(
        self,
        symbol: str,
        timeframe: str,
        health: TimeframeHealth,
        vector: Optional[TechnicalFeatureVector],
    ) -> TimeframeRegimeEvaluation:
        """Deterministic evaluation of regime for a single timeframe."""
        if not vector or not health.is_valid_for_analysis or health.bar_count < 20:
            warnings = list(health.warnings)
            if health.bar_count < 20:
                warnings.append(f"Insufficient bar history ({health.bar_count} bars; minimum 20 required).")

            return TimeframeRegimeEvaluation(
                timeframe=timeframe,
                regime=MarketRegime.INSUFFICIENT_DATA,
                posture=RegimePosture.INSUFFICIENT_DATA,
                volatility_state=VolatilityState.INSUFFICIENT_DATA,
                trend_strength=TrendStrength.INSUFFICIENT_DATA,
                bar_count=health.bar_count,
                last_close=vector.last_close if vector else 0.0,
                evidence=("Insufficient data to evaluate structural market regime.",),
                warnings=tuple(warnings),
                facts={"bar_count": health.bar_count},
            )

        last_close = vector.last_close
        evidence: List[str] = []
        warnings: List[str] = list(health.warnings)
        facts: Dict[str, Any] = {
            "last_close": last_close,
            "bar_count": vector.bar_count,
        }

        # 1. Volatility Assessment (Bollinger Bandwidth & ATR)
        bb = None
        atr = None
        for ind in vector.indicators.values():
            if ind.indicator_type == IndicatorType.BOLLINGER_BANDS:
                bb = ind
            elif ind.indicator_type == IndicatorType.ATR:
                atr = ind

        bandwidth: Optional[float] = None
        if bb and bb.is_valid and bb.values:
            bandwidth = bb.values.get("bandwidth")
            facts["bandwidth"] = bandwidth

        atr_ratio: Optional[float] = None
        if atr and atr.is_valid and atr.value is not None and last_close > 0:
            atr_ratio = round(atr.value / last_close, 6)
            facts["atr_ratio"] = atr_ratio

        # Determine VolatilityState
        vol_state = VolatilityState.NORMAL
        if bandwidth is not None:
            if bandwidth < 0.03:
                vol_state = VolatilityState.COMPRESSED
                evidence.append(f"Bollinger Bandwidth is compressed ({bandwidth:.4f} < 0.03).")
            elif bandwidth <= 0.08:
                vol_state = VolatilityState.NORMAL
                evidence.append(f"Bollinger Bandwidth is normal ({bandwidth:.4f}).")
            elif bandwidth <= 0.15:
                vol_state = VolatilityState.EXPANDED
                evidence.append(f"Bollinger Bandwidth is expanded ({bandwidth:.4f} > 0.08).")
            else:
                vol_state = VolatilityState.EXTREME
                evidence.append(f"Bollinger Bandwidth is extreme ({bandwidth:.4f} > 0.15).")
                warnings.append("Extreme volatility detected; regime stability reduced.")
        elif atr_ratio is not None:
            if atr_ratio < 0.005:
                vol_state = VolatilityState.COMPRESSED
            elif atr_ratio <= 0.015:
                vol_state = VolatilityState.NORMAL
            else:
                vol_state = VolatilityState.EXPANDED

        # 2. Trend Alignment & Strength (SMA 20, 50, 200, EMA 9, 21)
        sma_20 = vector.get_indicator("SMA_20")
        sma_50 = vector.get_indicator("SMA_50")
        sma_200 = vector.get_indicator("SMA_200")
        ema_9 = vector.get_indicator("EMA_9")
        ema_21 = vector.get_indicator("EMA_21")

        trend_spread: Optional[float] = None
        regime = MarketRegime.NEUTRAL_TRANSITIONAL
        posture = RegimePosture.NEUTRAL
        strength = TrendStrength.WEAK

        if sma_20 and sma_20.is_valid and sma_20.value is not None and last_close > 0:
            v20 = sma_20.value
            facts["sma_20"] = v20

            if sma_50 and sma_50.is_valid and sma_50.value is not None:
                v50 = sma_50.value
                trend_spread = round((v20 - v50) / last_close, 6)
                facts["trend_spread"] = trend_spread
                facts["sma_50"] = v50

                # Full Bullish trend stack: Close > SMA20 > SMA50
                if last_close > v20 and v20 > v50:
                    regime = MarketRegime.BULLISH_TRENDING
                    posture = RegimePosture.BULLISH
                    if trend_spread >= 0.005:
                        strength = TrendStrength.STRONG
                        evidence.append(f"Strong upward trend alignment: Close ({last_close:.2f}) > SMA20 ({v20:.2f}) > SMA50 ({v50:.2f}).")
                    else:
                        strength = TrendStrength.MODERATE
                        evidence.append(f"Moderate upward trend alignment: Close ({last_close:.2f}) > SMA20 ({v20:.2f}) > SMA50 ({v50:.2f}).")

                # Full Bearish trend stack: Close < SMA20 < SMA50
                elif last_close < v20 and v20 < v50:
                    regime = MarketRegime.BEARISH_TRENDING
                    posture = RegimePosture.BEARISH
                    if abs(trend_spread) >= 0.005:
                        strength = TrendStrength.STRONG
                        evidence.append(f"Strong downward trend alignment: Close ({last_close:.2f}) < SMA20 ({v20:.2f}) < SMA50 ({v50:.2f}).")
                    else:
                        strength = TrendStrength.MODERATE
                        evidence.append(f"Moderate downward trend alignment: Close ({last_close:.2f}) < SMA20 ({v20:.2f}) < SMA50 ({v50:.2f}).")

                # Non-trending: Ranging or Transitional
                else:
                    strength = TrendStrength.NONE
                    if vol_state == VolatilityState.COMPRESSED:
                        regime = MarketRegime.RANGING_COMPRESSED
                        posture = RegimePosture.NEUTRAL
                        evidence.append("Compressed ranging regime: Moving averages convergent with low volatility squeeze.")
                    elif vol_state in (VolatilityState.EXPANDED, VolatilityState.EXTREME):
                        regime = MarketRegime.RANGING_VOLATILE
                        posture = RegimePosture.NEUTRAL
                        evidence.append("Volatile ranging regime: Moving averages divergent without directional order.")
                    else:
                        regime = MarketRegime.NEUTRAL_TRANSITIONAL
                        posture = RegimePosture.NEUTRAL
                        evidence.append("Transitional market state: Moving averages crossing without established trend.")
            else:
                # Short-term regime when SMA50 has insufficient bars (< 50 bars)
                if ema_9 and ema_9.is_valid and ema_9.value is not None and ema_21 and ema_21.is_valid and ema_21.value is not None:
                    if last_close > v20 and ema_9.value > ema_21.value:
                        regime = MarketRegime.BULLISH_TRENDING
                        posture = RegimePosture.BULLISH
                        strength = TrendStrength.MODERATE
                        evidence.append(f"Short-term upward trend alignment: Close ({last_close:.2f}) > SMA20 ({v20:.2f}) and EMA9 > EMA21.")
                    elif last_close < v20 and ema_9.value < ema_21.value:
                        regime = MarketRegime.BEARISH_TRENDING
                        posture = RegimePosture.BEARISH
                        strength = TrendStrength.MODERATE
                        evidence.append(f"Short-term downward trend alignment: Close ({last_close:.2f}) < SMA20 ({v20:.2f}) and EMA9 < EMA21.")
                    else:
                        regime = MarketRegime.NEUTRAL_TRANSITIONAL
                        posture = RegimePosture.NEUTRAL
                        strength = TrendStrength.WEAK
                        evidence.append("Short-term moving averages neutral or conflicting.")
                elif last_close > v20 * 1.005:
                    regime = MarketRegime.BULLISH_TRENDING
                    posture = RegimePosture.BULLISH
                    strength = TrendStrength.MODERATE
                    evidence.append(f"Short-term upward trend: Close ({last_close:.2f}) > SMA20 ({v20:.2f}).")
                elif last_close < v20 * 0.995:
                    regime = MarketRegime.BEARISH_TRENDING
                    posture = RegimePosture.BEARISH
                    strength = TrendStrength.MODERATE
                    evidence.append(f"Short-term downward trend: Close ({last_close:.2f}) < SMA20 ({v20:.2f}).")
                else:
                    regime = MarketRegime.NEUTRAL_TRANSITIONAL
                    posture = RegimePosture.NEUTRAL
                    strength = TrendStrength.NONE
                    evidence.append("Close price convergent with SMA20.")
        else:
            evidence.append("Insufficient moving average data for trend alignment.")

        return TimeframeRegimeEvaluation(
            timeframe=timeframe,
            regime=regime,
            posture=posture,
            volatility_state=vol_state,
            trend_strength=strength,
            bar_count=vector.bar_count,
            last_close=last_close,
            bandwidth=bandwidth,
            atr_ratio=atr_ratio,
            trend_spread=trend_spread,
            evidence=tuple(evidence),
            warnings=tuple(warnings),
            facts=facts,
        )

    def _synthesize_overall_regime(
        self,
        evaluations: Dict[str, TimeframeRegimeEvaluation],
        coverage: Dict[str, str],
    ) -> Tuple[MarketRegime, RegimePosture, VolatilityState, TrendStrength]:
        """Aggregate multi-timeframe regime evaluations into overall posture."""
        valid_evals = [e for e in evaluations.values() if e.regime != MarketRegime.INSUFFICIENT_DATA]
        if not valid_evals:
            return (
                MarketRegime.INSUFFICIENT_DATA,
                RegimePosture.INSUFFICIENT_DATA,
                VolatilityState.INSUFFICIENT_DATA,
                TrendStrength.INSUFFICIENT_DATA,
            )

        postures = [e.posture for e in valid_evals]
        regimes = [e.regime for e in valid_evals]
        vols = [e.volatility_state for e in valid_evals]
        strengths = [e.trend_strength for e in valid_evals]

        # Check unanimous or majority posture
        bull_count = sum(1 for p in postures if p == RegimePosture.BULLISH)
        bear_count = sum(1 for p in postures if p == RegimePosture.BEARISH)
        neutral_count = sum(1 for p in postures if p == RegimePosture.NEUTRAL)

        overall_vol = VolatilityState.NORMAL
        if VolatilityState.EXTREME in vols:
            overall_vol = VolatilityState.EXTREME
        elif VolatilityState.EXPANDED in vols:
            overall_vol = VolatilityState.EXPANDED
        elif all(v == VolatilityState.COMPRESSED for v in vols):
            overall_vol = VolatilityState.COMPRESSED

        overall_strength = TrendStrength.MODERATE
        if TrendStrength.STRONG in strengths:
            overall_strength = TrendStrength.STRONG
        elif all(s in (TrendStrength.WEAK, TrendStrength.NONE) for s in strengths):
            overall_strength = TrendStrength.WEAK

        if bull_count > 0 and bear_count > 0:
            return MarketRegime.NEUTRAL_TRANSITIONAL, RegimePosture.CONFLICTING, overall_vol, TrendStrength.WEAK
        elif bull_count > bear_count and bull_count >= len(valid_evals) / 2.0:
            return MarketRegime.BULLISH_TRENDING, RegimePosture.BULLISH, overall_vol, overall_strength
        elif bear_count > bull_count and bear_count >= len(valid_evals) / 2.0:
            return MarketRegime.BEARISH_TRENDING, RegimePosture.BEARISH, overall_vol, overall_strength
        else:
            # Check if compressed ranging
            if overall_vol == VolatilityState.COMPRESSED:
                return MarketRegime.RANGING_COMPRESSED, RegimePosture.NEUTRAL, overall_vol, TrendStrength.NONE
            elif overall_vol in (VolatilityState.EXPANDED, VolatilityState.EXTREME):
                return MarketRegime.RANGING_VOLATILE, RegimePosture.NEUTRAL, overall_vol, TrendStrength.WEAK
            return MarketRegime.NEUTRAL_TRANSITIONAL, RegimePosture.NEUTRAL, overall_vol, TrendStrength.WEAK

    def _generate_uncertainty_caveats(
        self,
        coverage: Dict[str, str],
        overall_regime: MarketRegime,
        evaluations: Dict[str, TimeframeRegimeEvaluation],
    ) -> Tuple[str, ...]:
        caveats: List[str] = [
            "Market regime classification is an analytical estimation and does not forecast future price direction.",
            "Historical volatility and trend metrics are subject to regime transitions and structural shifts.",
        ]
        missing = [tf for tf, st in coverage.items() if st in ("MISSING", "INSUFFICIENT_DATA", "STALE")]
        if missing:
            caveats.append(f"Data for timeframes {', '.join(missing)} was unavailable or insufficient, increasing uncertainty.")

        return tuple(caveats)

    def _generate_voice_summary(
        self,
        symbol: str,
        overall_regime: MarketRegime,
        overall_posture: RegimePosture,
        overall_vol: VolatilityState,
        overall_strength: TrendStrength,
        evaluations: Dict[str, TimeframeRegimeEvaluation],
    ) -> str:
        if overall_regime == MarketRegime.INSUFFICIENT_DATA:
            return f"Insufficient market data to evaluate structural regime for {symbol}."

        regime_desc = overall_regime.value.lower().replace("_", " ")
        vol_desc = overall_vol.value.lower()
        strength_desc = overall_strength.value.lower()

        if overall_posture == RegimePosture.BULLISH:
            return f"{symbol} is in a {strength_desc} bullish trending regime with {vol_desc} volatility."
        elif overall_posture == RegimePosture.BEARISH:
            return f"{symbol} is in a {strength_desc} bearish trending regime with {vol_desc} volatility."
        elif overall_posture == RegimePosture.CONFLICTING:
            return f"{symbol} exhibits conflicting cross-timeframe regime signals with {vol_desc} volatility."
        else:
            return f"{symbol} is in a {regime_desc} regime with {vol_desc} volatility."


# -----------------------------------------------------------------------------
# Orchestrator Command Handler Registration
# -----------------------------------------------------------------------------

def register_regime_agent_handlers(
    orchestrator: Any,
    agent: MarketRegimeAgent,
) -> None:
    """Register market regime specialist agent commands on MasterOrchestrator."""

    def handle_market_regime_analysis(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handle market_regime_analysis command."""
        params = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or {})
        symbol = params.get("symbol")
        timeframes_param = params.get("timeframes")
        reference_time = params.get("reference_time")

        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("Missing or invalid 'symbol' parameter for market_regime_analysis.")

        parsed_tfs: Optional[List[str]] = None
        if timeframes_param:
            if isinstance(timeframes_param, (list, tuple)):
                parsed_tfs = [str(t) for t in timeframes_param]
            elif isinstance(timeframes_param, str):
                parsed_tfs = [t.strip() for t in timeframes_param.split(",") if t.strip()]

        ref_t: Optional[float] = None
        if reference_time is not None:
            try:
                ref_t = float(reference_time)
            except (ValueError, TypeError):
                ref_t = None

        report = agent.generate_report(
            symbol=symbol.strip().upper(),
            timeframes=parsed_tfs,
            reference_time=ref_t,
            correlation_id=req.correlation_id,
        )

        return {
            "status": "success",
            "message": report.voice_summary,
            "symbol": report.symbol,
            "overall_regime": report.overall_regime.value,
            "overall_posture": report.overall_posture.value,
            "overall_volatility": report.overall_volatility.value,
            "overall_trend_strength": report.overall_trend_strength.value,
            "timeframe_coverage": report.timeframe_coverage,
            "data": report.to_dict(),
        }

    orchestrator.register_handler(
        command="market_regime_analysis",
        handler=handle_market_regime_analysis,
        capability="analysis",
        allow_override=True,
    )
