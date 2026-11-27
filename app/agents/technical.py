"""KAISER — Technical Analysis Specialist Agent.

Deterministic, evidence-based specialist agent that evaluates technical features
across multiple isolated timeframes (1m, 5m, 15m, 1h, 1D), checks synchronization
and data quality health, explains indicator agreement and conflict, and synthesizes
structured intelligence reports with explicit uncertainty communication.

Key Architectural Invariants:
1. Purely analytical: Zero direct trade execution or order routing capabilities.
2. Evidence-based: Every signal is tied to verifiable mathematical facts.
3. Conflict awareness: Reconciles or explicitly reports disagreements rather than forcing bias.
4. Strict quality gating: Incomplete, missing, or unhealthy timeframes downgrade confidence.
5. Absolute honesty: Never fabricates missing candles or claims certainty of future outcomes.
"""

from __future__ import annotations

import copy
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from app.agents.base import BaseAgent
from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.models import (
    SUPPORTED_TIMEFRAMES,
    TIMEFRAME_SECONDS,
    IndicatorType,
    IndicatorValue,
    MultiTimeframeSyncState,
    TechnicalFeatureVector,
    TimeframeHealth,
    TimeframeHealthStatus,
    normalize_timeframe,
)
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.quality import MarketDataQualityEngine
from app.orchestrator.models import OrchestratorRequest, RequestStatus

logger = get_logger("agents.technical")


# -----------------------------------------------------------------------------
# Signal and Assessment Enums
# -----------------------------------------------------------------------------

class SignalCondition(str, Enum):
    """Categorical directional posture or health state."""

    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    CONFLICTING = "CONFLICTING"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class CrossTimeframeAgreement(str, Enum):
    """Cross-timeframe consensus evaluation."""

    AGREEMENT = "AGREEMENT"
    DISAGREEMENT = "DISAGREEMENT"
    MIXED = "MIXED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


class ConfidenceLevel(str, Enum):
    """Calibrated confidence tier reflecting data coverage and signal alignment."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NONE = "NONE"


# -----------------------------------------------------------------------------
# Structured Intelligence Report Domain Models
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class TimeframeSignalReport:
    """Detailed technical assessment for an individual timeframe.

    Attributes:
        timeframe: Canonical timeframe string.
        condition: Categorical condition (BULLISH, BEARISH, NEUTRAL, CONFLICTING, INSUFFICIENT_DATA).
        last_close: Most recent closing price.
        bar_count: Total bars retained in the rolling series.
        latest_bar_timestamp: UTC timestamp of latest bar if available.
        freshness: Freshness status ('FRESH', 'STALE', 'EMPTY').
        health_status: Data quality health assessment.
        facts: Pure calculated mathematical outputs (SMAs, EMAs, RSI, MACD, BB, ATR).
        evidence: Concrete factual statements supporting the condition.
        conflicts: Explicit explanations of any indicator disagreements.
        warnings: Quality or data insufficiency warnings.
        interpretation: Narrative synthesis for this specific timeframe.
    """

    timeframe: str
    condition: SignalCondition
    last_close: float
    bar_count: int
    latest_bar_timestamp: Optional[float] = None
    freshness: str = "FRESH"
    health_status: str = "HEALTHY"
    facts: Dict[str, Any] = field(default_factory=dict)
    evidence: Tuple[str, ...] = ()
    conflicts: Tuple[str, ...] = ()
    warnings: Tuple[str, ...] = ()
    interpretation: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "timeframe", normalize_timeframe(self.timeframe))
        object.__setattr__(self, "facts", copy.deepcopy(dict(self.facts or {})))
        object.__setattr__(self, "evidence", tuple(self.evidence or ()))
        object.__setattr__(self, "conflicts", tuple(self.conflicts or ()))
        object.__setattr__(self, "warnings", tuple(self.warnings or ()))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "condition": self.condition.value,
            "last_close": self.last_close,
            "bar_count": self.bar_count,
            "latest_bar_timestamp": self.latest_bar_timestamp,
            "freshness": self.freshness,
            "health_status": self.health_status,
            "facts": copy.deepcopy(self.facts),
            "evidence": list(self.evidence),
            "conflicts": list(self.conflicts),
            "warnings": list(self.warnings),
            "interpretation": self.interpretation,
        }


@dataclass(frozen=True)
class TechnicalIntelligenceReport:
    """Comprehensive multi-timeframe technical intelligence report.

    Strictly separates calculated facts from subjective interpretations and
    explicitly communicates uncertainty, data gaps, and risks.

    Attributes:
        symbol: Ticker symbol.
        timestamp: Report creation POSIX UTC timestamp.
        iso_timestamp: Human-readable ISO UTC timestamp.
        overall_condition: Aggregate multi-timeframe directional condition.
        confidence: Evaluated confidence level.
        timeframe_coverage: Mapping of timeframe -> coverage status (AVAILABLE, MISSING, STALE, UNHEALTHY).
        timeframe_reports: Detailed per-timeframe signal reports.
        cross_timeframe_agreement: Consensus status (AGREEMENT, DISAGREEMENT, MIXED, INSUFFICIENT_DATA).
        cross_timeframe_summary: Narrative summarizing cross-timeframe alignment or divergence.
        quality_warnings: System-level or stream-level data quality warnings.
        facts: Global calculated facts across all monitored timeframes.
        interpretation: Synthesis explaining the current technical landscape.
        uncertainty_caveats: Explicit disclaimers, limitations, and risk statements.
        voice_summary: Concise, natural explanation ready for voice synthesis.
    """

    symbol: str
    timestamp: float
    iso_timestamp: str
    overall_condition: SignalCondition
    confidence: ConfidenceLevel
    timeframe_coverage: Dict[str, str]
    timeframe_reports: Dict[str, TimeframeSignalReport]
    cross_timeframe_agreement: CrossTimeframeAgreement
    cross_timeframe_summary: str
    quality_warnings: Tuple[str, ...] = ()
    facts: Dict[str, Any] = field(default_factory=dict)
    interpretation: str = ""
    uncertainty_caveats: Tuple[str, ...] = ()
    voice_summary: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "timeframe_coverage", copy.deepcopy(dict(self.timeframe_coverage or {})))
        object.__setattr__(self, "timeframe_reports", copy.deepcopy(dict(self.timeframe_reports or {})))
        object.__setattr__(self, "quality_warnings", tuple(self.quality_warnings or ()))
        object.__setattr__(self, "facts", copy.deepcopy(dict(self.facts or {})))
        object.__setattr__(self, "uncertainty_caveats", tuple(self.uncertainty_caveats or ()))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "overall_condition": self.overall_condition.value,
            "confidence": self.confidence.value,
            "timeframe_coverage": copy.deepcopy(self.timeframe_coverage),
            "timeframe_reports": {k: v.to_dict() for k, v in self.timeframe_reports.items()},
            "cross_timeframe_agreement": self.cross_timeframe_agreement.value,
            "cross_timeframe_summary": self.cross_timeframe_summary,
            "quality_warnings": list(self.quality_warnings),
            "facts": copy.deepcopy(self.facts),
            "interpretation": self.interpretation,
            "uncertainty_caveats": list(self.uncertainty_caveats),
            "voice_summary": self.voice_summary,
        }


# -----------------------------------------------------------------------------
# Specialist Agent Implementation
# -----------------------------------------------------------------------------

class TechnicalAnalysisAgent(BaseAgent):
    """Autonomous specialist agent for multi-timeframe technical intelligence.

    Analyzes SMA, EMA, RSI, MACD, Bollinger Bands, and ATR across isolated timeframes,
    evaluates synchronization quality, flags data health issues, and formats
    structured intelligence reports.
    """

    def __init__(
        self,
        analysis_engine: TechnicalAnalysisEngine,
        quality_engine: Optional[MarketDataQualityEngine] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        name: str = "technical_analysis_agent",
    ) -> None:
        super().__init__(name=name, event_bus=event_bus, state_store=state_store)
        self.analysis_engine = analysis_engine
        self.quality_engine = quality_engine
        self._reports_generated = 0

    @property
    def agent_type(self) -> str:
        return "technical_analysis"

    def get_status(self) -> Dict[str, Any]:
        """Return operational telemetry."""
        with self._lock:
            return {
                "name": self.name,
                "agent_type": self.agent_type,
                "reports_generated": self._reports_generated,
                "analysis_engine": self.analysis_engine.name,
                "supported_timeframes": list(SUPPORTED_TIMEFRAMES),
            }

    def generate_report(
        self,
        symbol: str,
        timeframes: Optional[Sequence[str]] = None,
        reference_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> TechnicalIntelligenceReport:
        """Evaluate multi-timeframe technical features and produce a structured report.

        Args:
            symbol: Ticker symbol to analyze.
            timeframes: Sequence of timeframes to evaluate (defaults to all supported: 1m, 5m, 15m, 1h, 1D).
            reference_time: Reference clock for deterministic replay testing.
            correlation_id: Tracing correlation ID.

        Returns:
            TechnicalIntelligenceReport: Immutable structured intelligence report.
        """
        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("TechnicalAnalysisAgent symbol must be a non-empty string.")

        clean_symbol = symbol.strip().upper()
        now = time.time() if reference_time is None else reference_time
        cid = correlation_id or uuid.uuid4().hex

        # Resolve and normalize target timeframes
        target_tfs: List[str] = []
        if timeframes:
            for tf in timeframes:
                target_tfs.append(normalize_timeframe(tf))
        else:
            target_tfs = list(SUPPORTED_TIMEFRAMES)

        with self._lock:
            self._reports_generated += 1

        # 1. Multi-Timeframe Health & Synchronization Audit
        sync_state = self.analysis_engine.get_multi_timeframe_sync(
            symbol=clean_symbol,
            timeframes=target_tfs,
            reference_time=now,
        )

        timeframe_coverage: Dict[str, str] = {}
        timeframe_reports: Dict[str, TimeframeSignalReport] = {}
        all_facts: Dict[str, Any] = {
            "symbol": clean_symbol,
            "reference_timestamp": now,
            "timeframes": {},
        }
        quality_warnings: List[str] = list(sync_state.warnings)

        # 2. Evaluate Each Timeframe
        for tf in target_tfs:
            health = sync_state.timeframes.get(tf)
            if not health or health.bar_count == 0:
                timeframe_coverage[tf] = "MISSING"
                continue

            if health.status == TimeframeHealthStatus.STALE:
                timeframe_coverage[tf] = "STALE"
            elif health.status in (TimeframeHealthStatus.UNHEALTHY, TimeframeHealthStatus.DEGRADED):
                timeframe_coverage[tf] = health.status.value
            elif health.status == TimeframeHealthStatus.INSUFFICIENT_DATA:
                timeframe_coverage[tf] = "INSUFFICIENT_DATA"
            else:
                timeframe_coverage[tf] = "AVAILABLE"

            vector = self.analysis_engine.get_feature_vector(clean_symbol, tf)
            tf_report = self._evaluate_timeframe_features(
                symbol=clean_symbol,
                timeframe=tf,
                health=health,
                vector=vector,
            )
            timeframe_reports[tf] = tf_report
            all_facts["timeframes"][tf] = tf_report.facts

        # Attach read-only Level 2 depth features if available
        if hasattr(self.analysis_engine, "get_depth_features"):
            depth_feats = self.analysis_engine.get_depth_features(clean_symbol)
            if depth_feats.get("depth_available"):
                all_facts["depth"] = depth_feats

        # Attach read-only trade flow features if available
        if hasattr(self.analysis_engine, "get_trade_flow_features"):
            tf_feats = self.analysis_engine.get_trade_flow_features(clean_symbol)
            if tf_feats.get("trade_flow_available"):
                all_facts["trade_flow"] = tf_feats

        # Attach read-only synchronized multi-stream context if available
        if hasattr(self.analysis_engine, "get_synchronized_context_features"):
            sync_feats = self.analysis_engine.get_synchronized_context_features(clean_symbol)
            if sync_feats.get("context_available"):
                all_facts["synchronized_context"] = sync_feats

        # 3. Cross-Timeframe Synthesis & Agreement Assessment
        overall_condition, agreement, agreement_summary, confidence, cross_interpretation = (
            self._synthesize_cross_timeframe(
                symbol=clean_symbol,
                target_tfs=target_tfs,
                coverage=timeframe_coverage,
                reports=timeframe_reports,
                sync_state=sync_state,
            )
        )

        # 4. Formulate Explicit Uncertainty Caveats
        uncertainty_caveats = self._generate_uncertainty_caveats(
            coverage=timeframe_coverage,
            confidence=confidence,
            reports=timeframe_reports,
            overall_condition=overall_condition,
        )

        # 5. Format Concise Voice-Friendly Explanation
        voice_summary = self._generate_voice_summary(
            symbol=clean_symbol,
            overall_condition=overall_condition,
            confidence=confidence,
            agreement=agreement,
            coverage=timeframe_coverage,
            reports=timeframe_reports,
        )

        # 6. Construct Immutable Report
        iso_str = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
        report = TechnicalIntelligenceReport(
            symbol=clean_symbol,
            timestamp=now,
            iso_timestamp=iso_str,
            overall_condition=overall_condition,
            confidence=confidence,
            timeframe_coverage=timeframe_coverage,
            timeframe_reports=timeframe_reports,
            cross_timeframe_agreement=agreement,
            cross_timeframe_summary=agreement_summary,
            quality_warnings=tuple(quality_warnings),
            facts=all_facts,
            interpretation=cross_interpretation,
            uncertainty_caveats=tuple(uncertainty_caveats),
            voice_summary=voice_summary,
        )

        # 7. StateStore Caching
        if self.state_store:
            self.state_store.update(
                "technical_intelligence_reports",
                {clean_symbol: report.to_dict()},
                default={},
            )

        # 8. EventBus Distribution
        if self.event_bus:
            payload = report.to_dict()
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.AGENT_RESPONSE,
                    source=self.name,
                    correlation_id=cid,
                    payload={"agent": self.name, "report": payload},
                )
            )
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.ANALYSIS_MULTI_TIMEFRAME_READY,
                    source=self.name,
                    correlation_id=cid,
                    payload=payload,
                )
            )

        return report

    # --------------------------------------------------------------------------
    # Timeframe Analysis Helpers
    # --------------------------------------------------------------------------

    def _evaluate_timeframe_features(
        self,
        symbol: str,
        timeframe: str,
        health: TimeframeHealth,
        vector: Optional[TechnicalFeatureVector],
    ) -> TimeframeSignalReport:
        """Deterministic evaluation of indicators on a single timeframe."""
        if not vector or not health.is_valid_for_analysis:
            warnings = list(health.warnings)
            if not vector:
                warnings.append(f"No feature vector computed for {symbol} ({timeframe}).")

            facts = {
                "last_close": vector.last_close if vector else 0.0,
                "bar_count": health.bar_count,
            }
            return TimeframeSignalReport(
                timeframe=timeframe,
                condition=SignalCondition.INSUFFICIENT_DATA,
                last_close=vector.last_close if vector else 0.0,
                bar_count=health.bar_count,
                latest_bar_timestamp=health.latest_timestamp,
                freshness="STALE" if health.status == TimeframeHealthStatus.STALE else "FRESH",
                health_status=health.status.value,
                facts=facts,
                evidence=("Insufficient data to calculate reliable technical indicators.",),
                conflicts=(),
                warnings=tuple(warnings),
                interpretation=(
                    f"Timeframe {timeframe} has insufficient or unhealthy data "
                    f"({health.bar_count} bars). Indicators cannot be validated."
                ),
            )

        last_close = vector.last_close
        evidence: List[str] = []
        conflicts: List[str] = []
        warnings: List[str] = list(health.warnings)
        facts: Dict[str, Any] = {
            "last_close": last_close,
            "bar_count": vector.bar_count,
            "indicators": {},
        }

        bullish_votes = 0
        bearish_votes = 0
        neutral_votes = 0

        # Extract SMA facts & signals
        sma_20 = vector.get_indicator("SMA_20")
        sma_50 = vector.get_indicator("SMA_50")
        sma_200 = vector.get_indicator("SMA_200")
        for sma in (sma_20, sma_50, sma_200):
            if sma and sma.is_valid and sma.value is not None:
                facts["indicators"][sma.name] = sma.value

        if sma_20 and sma_20.is_valid and sma_20.value is not None:
            if last_close > sma_20.value:
                bullish_votes += 1
                evidence.append(f"Price ({last_close:.2f}) is above 20-period SMA ({sma_20.value:.2f}).")
            elif last_close < sma_20.value:
                bearish_votes += 1
                evidence.append(f"Price ({last_close:.2f}) is below 20-period SMA ({sma_20.value:.2f}).")
            else:
                neutral_votes += 1

        if (
            sma_20
            and sma_50
            and sma_20.is_valid
            and sma_50.is_valid
            and sma_20.value is not None
            and sma_50.value is not None
        ):
            if sma_20.value > sma_50.value:
                bullish_votes += 1
                evidence.append(f"SMA 20 ({sma_20.value:.2f}) is above SMA 50 ({sma_50.value:.2f}) - positive slope.")
            elif sma_20.value < sma_50.value:
                bearish_votes += 1
                evidence.append(f"SMA 20 ({sma_20.value:.2f}) is below SMA 50 ({sma_50.value:.2f}) - negative slope.")

        # Extract EMA facts & signals
        ema_9 = vector.get_indicator("EMA_9")
        ema_21 = vector.get_indicator("EMA_21")
        ema_55 = vector.get_indicator("EMA_55")
        for ema in (ema_9, ema_21, ema_55):
            if ema and ema.is_valid and ema.value is not None:
                facts["indicators"][ema.name] = ema.value

        if (
            ema_9
            and ema_21
            and ema_9.is_valid
            and ema_21.is_valid
            and ema_9.value is not None
            and ema_21.value is not None
        ):
            if ema_9.value > ema_21.value:
                bullish_votes += 1
                evidence.append(f"Short EMA 9 ({ema_9.value:.2f}) is above EMA 21 ({ema_21.value:.2f}).")
            elif ema_9.value < ema_21.value:
                bearish_votes += 1
                evidence.append(f"Short EMA 9 ({ema_9.value:.2f}) is below EMA 21 ({ema_21.value:.2f}).")

        # Extract RSI facts & signals
        rsi = None
        for ind in vector.indicators.values():
            if ind.indicator_type == IndicatorType.RSI:
                rsi = ind
                break

        if rsi and rsi.is_valid and rsi.value is not None:
            facts["indicators"][rsi.name] = rsi.value
            rsi_val = rsi.value
            if rsi_val >= 70.0:
                evidence.append(f"RSI is overbought at {rsi_val:.1f} (potential upside exhaustion).")
                bullish_votes += 1
                conflicts.append("RSI is in overbought territory (>70), indicating elevated pullback risk despite upward trend.")
            elif rsi_val <= 30.0:
                evidence.append(f"RSI is oversold at {rsi_val:.1f} (potential downside exhaustion).")
                bearish_votes += 1
                conflicts.append("RSI is in oversold territory (<30), indicating potential bounce despite downward trend.")
            elif rsi_val > 50.0:
                bullish_votes += 1
                evidence.append(f"RSI ({rsi_val:.1f}) shows positive momentum above midline 50.")
            elif rsi_val < 50.0:
                bearish_votes += 1
                evidence.append(f"RSI ({rsi_val:.1f}) shows negative momentum below midline 50.")
            else:
                neutral_votes += 1

        # Extract MACD facts & signals
        macd = None
        for ind in vector.indicators.values():
            if ind.indicator_type == IndicatorType.MACD:
                macd = ind
                break

        if macd and macd.is_valid and macd.values:
            facts["indicators"]["MACD"] = macd.values
            facts["indicators"][macd.name] = macd.values
            m_line = macd.values.get("macd", 0.0)
            s_line = macd.values.get("signal", 0.0)
            hist = macd.values.get("histogram", 0.0)
            if hist > 0.001:
                bullish_votes += 1
                evidence.append(f"MACD histogram is positive (+{hist:.2f}) with MACD line above signal.")
            elif hist < -0.001:
                bearish_votes += 1
                evidence.append(f"MACD histogram is negative ({hist:.2f}) with MACD line below signal.")
            else:
                neutral_votes += 1
        elif macd and not macd.is_valid:
            facts["indicators"]["MACD"] = None
            evidence.append("Insufficient bars for MACD calculation (minimum 35 bars required).")

        # Extract Bollinger Bands facts & signals
        bb = None
        for ind in vector.indicators.values():
            if ind.indicator_type == IndicatorType.BOLLINGER_BANDS:
                bb = ind
                break

        if bb and bb.is_valid and bb.values:
            facts["indicators"]["BollingerBands"] = bb.values
            facts["indicators"][bb.name] = bb.values
            mid = bb.values.get("middle", 0.0)
            upper = bb.values.get("upper", 0.0)
            lower = bb.values.get("lower", 0.0)
            pb = bb.values.get("percent_b", 0.5)
            bw = bb.values.get("bandwidth", 0.0)
            if pb > 1.0:
                evidence.append(f"Price is trading above upper Bollinger Band ({upper:.2f}, %B={pb:.2f}).")
                bullish_votes += 1
            elif pb < 0.0:
                evidence.append(f"Price is trading below lower Bollinger Band ({lower:.2f}, %B={pb:.2f}).")
                bearish_votes += 1
            elif pb > 0.5:
                bullish_votes += 1
                evidence.append(f"Price is in upper Bollinger half (above middle {mid:.2f}).")
            elif pb < 0.5:
                bearish_votes += 1
                evidence.append(f"Price is in lower Bollinger half (below middle {mid:.2f}).")
            else:
                neutral_votes += 1
        elif bb and not bb.is_valid:
            facts["indicators"]["BollingerBands"] = None

        # Extract ATR volatility facts
        atr = None
        for ind in vector.indicators.values():
            if ind.indicator_type == IndicatorType.ATR:
                atr = ind
                break

        if atr and atr.is_valid and atr.value is not None:
            facts["indicators"]["ATR_14"] = atr.value
            facts["indicators"]["ATR"] = atr.value
            atr_pct = (atr.value / last_close) * 100.0 if last_close > 0 else 0.0
            facts["indicators"]["ATR_pct"] = round(atr_pct, 3)
            evidence.append(f"14-period ATR is {atr.value:.2f} ({atr_pct:.2f}% of price) reflecting current volatility.")
        elif atr and not atr.is_valid:
            facts["indicators"]["ATR"] = None

        # Condition Resolution & Conflict Detection
        # Check if indicators directly conflict with each other
        if bullish_votes >= 2 and bearish_votes >= 2:
            condition = SignalCondition.CONFLICTING
            conflicts.append(
                f"Indicator disagreement: {bullish_votes} bullish signals vs {bearish_votes} bearish signals."
            )
            interpretation = (
                f"On the {timeframe} timeframe, indicators present conflicting evidence. "
                f"Trend and momentum signals disagree ({bullish_votes} bullish vs {bearish_votes} bearish), "
                "precluding a directional bias."
            )
        elif bullish_votes >= 2 and bearish_votes == 0:
            condition = SignalCondition.BULLISH
            interpretation = (
                f"On the {timeframe} timeframe, technical structure is bullish with consistent support "
                f"across {bullish_votes} trend and momentum indicators."
            )
        elif bearish_votes >= 2 and bullish_votes == 0:
            condition = SignalCondition.BEARISH
            interpretation = (
                f"On the {timeframe} timeframe, technical structure is bearish with downward momentum "
                f"across {bearish_votes} trend and momentum indicators."
            )
        elif bullish_votes > bearish_votes and bullish_votes >= 2:
            # Predominantly bullish with minor counter-signal
            condition = SignalCondition.BULLISH
            interpretation = (
                f"On the {timeframe} timeframe, technical posture leans bullish ({bullish_votes} bullish vs "
                f"{bearish_votes} bearish counter-signals)."
            )
        elif bearish_votes > bullish_votes and bearish_votes >= 2:
            # Predominantly bearish with minor counter-signal
            condition = SignalCondition.BEARISH
            interpretation = (
                f"On the {timeframe} timeframe, technical posture leans bearish ({bearish_votes} bearish vs "
                f"{bullish_votes} bullish counter-signals)."
            )
        else:
            condition = SignalCondition.NEUTRAL
            interpretation = (
                f"On the {timeframe} timeframe, technical indicators are neutral or oscillating within range."
            )

        return TimeframeSignalReport(
            timeframe=timeframe,
            condition=condition,
            last_close=last_close,
            bar_count=vector.bar_count,
            latest_bar_timestamp=vector.timestamp,
            freshness="STALE" if health.status == TimeframeHealthStatus.STALE else "FRESH",
            health_status=health.status.value,
            facts=facts,
            evidence=tuple(evidence),
            conflicts=tuple(conflicts),
            warnings=tuple(warnings),
            interpretation=interpretation,
        )

    # --------------------------------------------------------------------------
    # Cross-Timeframe Synthesis
    # --------------------------------------------------------------------------

    def _synthesize_cross_timeframe(
        self,
        symbol: str,
        target_tfs: Sequence[str],
        coverage: Dict[str, str],
        reports: Dict[str, TimeframeSignalReport],
        sync_state: MultiTimeframeSyncState,
    ) -> Tuple[SignalCondition, CrossTimeframeAgreement, str, ConfidenceLevel, str]:
        """Aggregate multi-timeframe reports and resolve cross-timeframe consensus."""
        valid_reports = {
            tf: rep for tf, rep in reports.items()
            if rep.condition != SignalCondition.INSUFFICIENT_DATA and coverage.get(tf) == "AVAILABLE"
        }

        # 1. Total or severe lack of data
        if not valid_reports:
            return (
                SignalCondition.INSUFFICIENT_DATA,
                CrossTimeframeAgreement.INSUFFICIENT_DATA,
                f"No healthy timeframe data available for {symbol}.",
                ConfidenceLevel.NONE,
                f"Unable to synthesize multi-timeframe technical view for {symbol} due to insufficient bar history.",
            )

        # 2. Extract conditions from valid healthy timeframes
        conditions = [rep.condition for rep in valid_reports.values()]
        bullish_tfs = [tf for tf, rep in valid_reports.items() if rep.condition == SignalCondition.BULLISH]
        bearish_tfs = [tf for tf, rep in valid_reports.items() if rep.condition == SignalCondition.BEARISH]
        neutral_tfs = [tf for tf, rep in valid_reports.items() if rep.condition == SignalCondition.NEUTRAL]
        conflicting_tfs = [tf for tf, rep in valid_reports.items() if rep.condition == SignalCondition.CONFLICTING]

        has_missing = len(sync_state.missing_timeframes) > 0
        has_stale = len(sync_state.stale_timeframes) > 0
        has_unhealthy = len(sync_state.unhealthy_timeframes) > 0
        data_degraded = has_missing or has_stale or has_unhealthy or (len(valid_reports) < len(target_tfs))

        # 3. Resolve agreement / disagreement
        if bullish_tfs and bearish_tfs:
            agreement = CrossTimeframeAgreement.DISAGREEMENT
            overall_condition = SignalCondition.CONFLICTING
            summary = (
                f"Cross-timeframe disagreement detected: {', '.join(bullish_tfs)} indicate bullish posture, "
                f"while {', '.join(bearish_tfs)} indicate bearish posture."
            )
            confidence = ConfidenceLevel.LOW
            interpretation = (
                f"Cross-timeframe analysis for {symbol} reveals conflicting trends. Lower timeframes and higher "
                "timeframes are out of phase, signaling market transition, divergence, or range-bound indecision. "
                "Directional confidence is low."
            )

        elif len(bullish_tfs) == len(valid_reports) and len(bullish_tfs) > 0:
            agreement = CrossTimeframeAgreement.AGREEMENT
            overall_condition = SignalCondition.BULLISH
            summary = f"Bullish alignment across all available timeframes ({', '.join(bullish_tfs)})."

            # Confidence depends on completeness and health
            if not data_degraded and len(valid_reports) >= 4:
                confidence = ConfidenceLevel.HIGH
            elif len(valid_reports) >= 2 and not has_stale:
                confidence = ConfidenceLevel.MEDIUM
            else:
                confidence = ConfidenceLevel.LOW

            interpretation = (
                f"Technical indicators demonstrate bullish alignment across monitored timeframes ({', '.join(bullish_tfs)}). "
                f"Trend and momentum indicators are uniformly supportive."
            )

        elif len(bearish_tfs) == len(valid_reports) and len(bearish_tfs) > 0:
            agreement = CrossTimeframeAgreement.AGREEMENT
            overall_condition = SignalCondition.BEARISH
            summary = f"Bearish alignment across all available timeframes ({', '.join(bearish_tfs)})."

            if not data_degraded and len(valid_reports) >= 4:
                confidence = ConfidenceLevel.HIGH
            elif len(valid_reports) >= 2 and not has_stale:
                confidence = ConfidenceLevel.MEDIUM
            else:
                confidence = ConfidenceLevel.LOW

            interpretation = (
                f"Technical indicators demonstrate bearish alignment across monitored timeframes ({', '.join(bearish_tfs)}). "
                f"Downward pressure is confirmed across available horizons."
            )

        elif conflicting_tfs or (len(neutral_tfs) > 0 and (bullish_tfs or bearish_tfs)):
            agreement = CrossTimeframeAgreement.MIXED
            overall_condition = SignalCondition.CONFLICTING if conflicting_tfs else SignalCondition.NEUTRAL
            active_desc = []
            if bullish_tfs:
                active_desc.append(f"{', '.join(bullish_tfs)} bullish")
            if bearish_tfs:
                active_desc.append(f"{', '.join(bearish_tfs)} bearish")
            if neutral_tfs:
                active_desc.append(f"{', '.join(neutral_tfs)} neutral")
            if conflicting_tfs:
                active_desc.append(f"{', '.join(conflicting_tfs)} conflicting")
            summary = f"Mixed timeframe posture: {'; '.join(active_desc)}."
            confidence = ConfidenceLevel.LOW
            interpretation = (
                f"Mixed cross-timeframe structure for {symbol}. Indicators do not show unified consensus; "
                "market exhibits consolidation or localized divergence."
            )
        else:
            agreement = CrossTimeframeAgreement.MIXED
            overall_condition = SignalCondition.NEUTRAL
            summary = "Neutral posture across evaluated timeframes."
            confidence = ConfidenceLevel.LOW if data_degraded else ConfidenceLevel.MEDIUM
            interpretation = f"Indicators reflect neutral or range-bound behavior across available timeframes."

        # Invariant enforcement: If data is incomplete or unhealthy, confidence CANNOT be HIGH
        if data_degraded and confidence == ConfidenceLevel.HIGH:
            confidence = ConfidenceLevel.MEDIUM

        return overall_condition, agreement, summary, confidence, interpretation

    # --------------------------------------------------------------------------
    # Uncertainty & Voice Formatting
    # --------------------------------------------------------------------------

    def _generate_uncertainty_caveats(
        self,
        coverage: Dict[str, str],
        confidence: ConfidenceLevel,
        reports: Dict[str, TimeframeSignalReport],
        overall_condition: SignalCondition,
    ) -> List[str]:
        """Formulate explicit uncertainty statements and model limitations."""
        caveats = [
            "Technical indicators reflect past mathematical price action and do not guarantee future trajectory.",
            "All signals are calculated deterministic observations for intelligence purposes, not trading recommendations.",
        ]

        missing = [tf for tf, st in coverage.items() if st == "MISSING"]
        if missing:
            caveats.append(
                f"Higher/lower timeframe context is incomplete: {', '.join(missing)} data is missing. "
                "Macro trend cannot be fully validated."
            )

        stale = [tf for tf, st in coverage.items() if st == "STALE"]
        if stale:
            caveats.append(
                f"Timeframe(s) {', '.join(stale)} are stale. Older bars may not reflect recent market shifts."
            )

        if confidence in (ConfidenceLevel.LOW, ConfidenceLevel.NONE):
            caveats.append(
                "Overall confidence is restricted to LOW or NONE due to signal conflict or incomplete timeframe data."
            )

        if overall_condition == SignalCondition.CONFLICTING:
            caveats.append(
                "Direct indicator conflict detected. Directional certainty is explicitly disclaimed."
            )

        return caveats

    def _generate_voice_summary(
        self,
        symbol: str,
        overall_condition: SignalCondition,
        confidence: ConfidenceLevel,
        agreement: CrossTimeframeAgreement,
        coverage: Dict[str, str],
        reports: Dict[str, TimeframeSignalReport],
    ) -> str:
        """Construct natural, concise voice-friendly summary."""
        parts = [f"Technical intelligence report for {symbol}."]

        available_reports = [rep for rep in reports.values() if rep.condition != SignalCondition.INSUFFICIENT_DATA]
        if not available_reports:
            parts.append("Insufficient historical bar data is available across monitored timeframes.")
            return " ".join(parts)

        # Highlight key timeframes
        tf_summaries = []
        for rep in available_reports:
            cond_str = rep.condition.value.lower()
            tf_summaries.append(f"{rep.timeframe} is {cond_str}")

        parts.append(f"Coverage reflects {', '.join(tf_summaries)}.")

        if overall_condition == SignalCondition.BULLISH:
            parts.append(f"Overall posture is bullish with {confidence.value.lower()} confidence.")
        elif overall_condition == SignalCondition.BEARISH:
            parts.append(f"Overall posture is bearish with {confidence.value.lower()} confidence.")
        elif overall_condition == SignalCondition.CONFLICTING:
            parts.append(f"Overall signals are conflicting across timeframes; confidence is low.")
        else:
            parts.append(f"Overall stance is neutral with {confidence.value.lower()} confidence.")

        missing = [tf for tf, st in coverage.items() if st == "MISSING"]
        if missing:
            parts.append(f"Note that {', '.join(missing)} data is currently missing.")

        parts.append("Past technical indicators do not guarantee future movement.")
        return " ".join(parts)


# -----------------------------------------------------------------------------
# Orchestrator Command Handler Registration
# -----------------------------------------------------------------------------

def register_technical_agent_handlers(
    orchestrator: Any,
    agent: TechnicalAnalysisAgent,
) -> None:
    """Register specialist technical analysis agent commands on the MasterOrchestrator."""

    def handle_multi_timeframe_analysis(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handle multi_timeframe_analysis command across configured horizons."""
        params = dict(getattr(req, "context", {}) or getattr(req, "parameters", {}) or {})
        symbol = params.get("symbol")
        timeframes_param = params.get("timeframes")
        reference_time = params.get("reference_time")

        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("Missing or invalid 'symbol' parameter for multi_timeframe_analysis.")

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
            "overall_condition": report.overall_condition.value,
            "confidence": report.confidence.value,
            "cross_timeframe_agreement": report.cross_timeframe_agreement.value,
            "timeframe_coverage": report.timeframe_coverage,
            "quality_warnings": list(report.quality_warnings),
            "data": report.to_dict(),
        }

    orchestrator.register_handler(
        command="multi_timeframe_analysis",
        handler=handle_multi_timeframe_analysis,
        capability="analysis",
        allow_override=True,
    )
