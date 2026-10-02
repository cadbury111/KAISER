"""KAISER — Technical Analysis Engine.

Central analytical coordinator managing rolling bar histories, enforcing
market data quality gates, executing deterministic indicator calculations,
dispatching analysis events, and caching technical feature vectors.
"""

from __future__ import annotations

import threading
import uuid
from typing import Any, Dict, List, Optional, Tuple

from app.analysis.indicators import (
    calculate_atr,
    calculate_bollinger_bands,
    calculate_ema,
    calculate_macd,
    calculate_rsi,
    calculate_sma,
    classify_bollinger,
    classify_macd,
    classify_rsi,
)
from app.analysis.models import (
    AnalysisConfig,
    IndicatorType,
    IndicatorValue,
    TechnicalFeatureVector,
)
from app.analysis.series import RollingBarSeries
from app.core.events import Event, EventBus, EventType
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.models import Bar

logger = get_logger("analysis.engine")


class TechnicalAnalysisEngine:
    """Thread-safe, deterministic technical and quantitative analysis engine.

    Enforces data quality invariants: only verified `safe_to_consume` bars are
    admitted into the rolling historical calculation window.
    """

    def __init__(
        self,
        config: Optional[AnalysisConfig] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        name: str = "technical_analysis_engine",
    ) -> None:
        self.name = name
        self.config = config or AnalysisConfig()
        self.event_bus = event_bus
        self.state_store = state_store

        self._lock = threading.RLock()
        self._series: Dict[Tuple[str, str], RollingBarSeries] = {}
        self._latest_vectors: Dict[Tuple[str, str], TechnicalFeatureVector] = {}

        self._counters = {
            "bars_processed": 0,
            "bars_rejected_unhealthy": 0,
            "feature_vectors_computed": 0,
            "indicators_calculated": 0,
        }

    def _get_or_create_series(self, symbol: str, timeframe: str) -> RollingBarSeries:
        """Internal helper to retrieve or allocate a RollingBarSeries under lock."""
        key = (symbol.strip().upper(), timeframe.strip())
        if key not in self._series:
            self._series[key] = RollingBarSeries(
                symbol=symbol,
                timeframe=timeframe,
                max_capacity=self.config.max_bar_history,
            )
        return self._series[key]

    def add_bar(
        self,
        bar: Bar,
        safe_to_consume: bool = True,
        correlation_id: Optional[str] = None,
    ) -> Optional[TechnicalFeatureVector]:
        """Ingest a new Bar into the analysis engine.

        CRITICAL INVARIANT:
        If `safe_to_consume` is False (quarantined by MarketDataQualityEngine),
        the bar is rejected immediately without entering the calculation window.

        Args:
            bar: Validated Bar record.
            safe_to_consume: Quality Gate flag from MarketDataQualityEngine.
            correlation_id: Tracing correlation ID.

        Returns:
            Optional[TechnicalFeatureVector]: Newly evaluated feature vector,
            or None if the bar was rejected or duplicate.
        """
        cid = correlation_id or uuid.uuid4().hex

        # 1. Quality Gate Check
        if not safe_to_consume:
            with self._lock:
                self._counters["bars_rejected_unhealthy"] += 1
            logger.warning(
                "Rejected unsafe bar for %s (%s) from analysis series [cid=%s]",
                bar.symbol,
                bar.timeframe,
                cid,
            )
            return None

        # 2. Add to Rolling Bar Series
        with self._lock:
            series = self._get_or_create_series(bar.symbol, bar.timeframe)
            accepted = series.add_bar(bar)
            if not accepted:
                logger.debug(
                    "Bar for %s (%s) rejected (duplicate or out-of-order) [cid=%s]",
                    bar.symbol,
                    bar.timeframe,
                    cid,
                )
                return None

            self._counters["bars_processed"] += 1

            # 3. Compute Feature Vector
            vector = self._compute_feature_vector(series, correlation_id=cid)
            key = (bar.symbol.upper(), bar.timeframe)
            self._latest_vectors[key] = vector
            self._counters["feature_vectors_computed"] += 1

        # 4. StateStore Synchronization
        self._sync_state_store(vector)

        # 5. EventBus Distribution
        self._publish_events(vector, cid)

        return vector

    def calculate_feature_vector(
        self,
        symbol: str,
        timeframe: str = "1m",
        correlation_id: Optional[str] = None,
    ) -> Optional[TechnicalFeatureVector]:
        """Calculate technical indicators over current in-memory history on-demand."""
        cid = correlation_id or uuid.uuid4().hex
        key = (symbol.strip().upper(), timeframe.strip())

        with self._lock:
            series = self._series.get(key)
            if not series or series.count == 0:
                return None
            vector = self._compute_feature_vector(series, correlation_id=cid)
            self._latest_vectors[key] = vector
            self._counters["feature_vectors_computed"] += 1

        self._sync_state_store(vector)
        self._publish_events(vector, cid)
        return vector

    def get_feature_vector(
        self,
        symbol: str,
        timeframe: str = "1m",
    ) -> Optional[TechnicalFeatureVector]:
        """Retrieve the latest cached TechnicalFeatureVector."""
        key = (symbol.strip().upper(), timeframe.strip())
        with self._lock:
            return self._latest_vectors.get(key)

    def get_indicator(
        self,
        symbol: str,
        indicator_name: str,
        timeframe: str = "1m",
    ) -> Optional[IndicatorValue]:
        """Query a single indicator value from the latest evaluated feature vector."""
        vector = self.get_feature_vector(symbol, timeframe)
        if not vector:
            return None
        return vector.get_indicator(indicator_name)

    def get_history(self, symbol: str, timeframe: str = "1m") -> List[Bar]:
        """Return the current rolling sequence of bars for a stream."""
        key = (symbol.strip().upper(), timeframe.strip())
        with self._lock:
            series = self._series.get(key)
            if not series:
                return []
            return series.get_bars()

    def reset(self, symbol: Optional[str] = None, timeframe: Optional[str] = None) -> None:
        """Reset historical series and cached vectors."""
        with self._lock:
            if symbol and timeframe:
                key = (symbol.strip().upper(), timeframe.strip())
                if key in self._series:
                    self._series[key].clear()
                self._latest_vectors.pop(key, None)
            elif symbol:
                sym_upper = symbol.strip().upper()
                keys_to_remove = [k for k in self._series if k[0] == sym_upper]
                for k in keys_to_remove:
                    self._series[k].clear()
                    self._latest_vectors.pop(k, None)
            else:
                for s in self._series.values():
                    s.clear()
                self._series.clear()
                self._latest_vectors.clear()

    def get_counters(self) -> Dict[str, int]:
        """Return operational telemetry counters."""
        with self._lock:
            return dict(self._counters)

    # --------------------------------------------------------------------------
    # Internal Calculation & Distribution Workers
    # --------------------------------------------------------------------------

    def _compute_feature_vector(
        self,
        series: RollingBarSeries,
        correlation_id: str,
    ) -> TechnicalFeatureVector:
        """Execute all configured indicator algorithms over the series."""
        bars = series.get_bars()
        closes = series.get_closes()
        last_bar = bars[-1]
        symbol = series.symbol
        timeframe = series.timeframe
        ts = last_bar.timestamp

        indicators: Dict[str, IndicatorValue] = {}
        signals: Dict[str, str] = {}
        calc_count = 0

        # 1. Simple Moving Averages (SMA)
        for period in self.config.sma_periods:
            val = calculate_sma(closes, period=period)
            calc_count += 1
            name = f"SMA_{period}"
            indicators[name] = IndicatorValue(
                name=name,
                indicator_type=IndicatorType.SMA,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=ts,
                value=val,
                is_valid=(val is not None),
                metadata={"period": period},
            )

        # 2. Exponential Moving Averages (EMA)
        for period in self.config.ema_periods:
            val = calculate_ema(closes, period=period)
            calc_count += 1
            name = f"EMA_{period}"
            indicators[name] = IndicatorValue(
                name=name,
                indicator_type=IndicatorType.EMA,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=ts,
                value=val,
                is_valid=(val is not None),
                metadata={"period": period},
            )

        # 3. Relative Strength Index (RSI)
        rsi_val = calculate_rsi(closes, period=self.config.rsi_period)
        calc_count += 1
        rsi_name = f"RSI_{self.config.rsi_period}"
        indicators[rsi_name] = IndicatorValue(
            name=rsi_name,
            indicator_type=IndicatorType.RSI,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=ts,
            value=rsi_val,
            is_valid=(rsi_val is not None),
            metadata={"period": self.config.rsi_period},
        )
        signals["rsi"] = classify_rsi(rsi_val).value

        # 4. Moving Average Convergence Divergence (MACD)
        macd_dict = calculate_macd(
            closes,
            fast_period=self.config.macd_fast,
            slow_period=self.config.macd_slow,
            signal_period=self.config.macd_signal,
        )
        calc_count += 1
        macd_name = f"MACD_{self.config.macd_fast}_{self.config.macd_slow}_{self.config.macd_signal}"
        indicators[macd_name] = IndicatorValue(
            name=macd_name,
            indicator_type=IndicatorType.MACD,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=ts,
            value=macd_dict.get("macd") if macd_dict else None,
            values=macd_dict or {},
            is_valid=(macd_dict is not None),
            metadata={
                "fast": self.config.macd_fast,
                "slow": self.config.macd_slow,
                "signal": self.config.macd_signal,
            },
        )
        signals["macd"] = classify_macd(macd_dict).value

        # 5. Bollinger Bands
        bb_dict = calculate_bollinger_bands(
            closes,
            period=self.config.bollinger_period,
            num_std=self.config.bollinger_std_dev,
        )
        calc_count += 1
        bb_name = f"BB_{self.config.bollinger_period}_{self.config.bollinger_std_dev}"
        indicators[bb_name] = IndicatorValue(
            name=bb_name,
            indicator_type=IndicatorType.BOLLINGER_BANDS,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=ts,
            value=bb_dict.get("middle") if bb_dict else None,
            values=bb_dict or {},
            is_valid=(bb_dict is not None),
            metadata={
                "period": self.config.bollinger_period,
                "num_std": self.config.bollinger_std_dev,
            },
        )
        signals["bollinger"] = classify_bollinger(last_bar.close, bb_dict).value

        # 6. Average True Range (ATR)
        atr_val = calculate_atr(bars, period=self.config.atr_period)
        calc_count += 1
        atr_name = f"ATR_{self.config.atr_period}"
        indicators[atr_name] = IndicatorValue(
            name=atr_name,
            indicator_type=IndicatorType.ATR,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=ts,
            value=atr_val,
            is_valid=(atr_val is not None),
            metadata={"period": self.config.atr_period},
        )

        with self._lock:
            self._counters["indicators_calculated"] += calc_count

        return TechnicalFeatureVector(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=ts,
            bar_count=series.count,
            last_close=last_bar.close,
            indicators=indicators,
            signals=signals,
            metadata={"correlation_id": correlation_id},
        )

    def _sync_state_store(self, vector: TechnicalFeatureVector) -> None:
        """Update operational state in StateStore."""
        if not self.state_store:
            return

        cache_key = f"{vector.symbol}_{vector.timeframe}"
        self.state_store.update(
            "technical_analysis",
            {cache_key: vector.to_dict()},
            default={},
        )
        self.state_store.update(
            "analysis_counters",
            self.get_counters(),
            default={},
        )

    def _publish_events(self, vector: TechnicalFeatureVector, correlation_id: str) -> None:
        """Publish feature vector ready event on EventBus."""
        if not self.event_bus:
            return

        self.event_bus.publish(
            Event.create(
                event_type=EventType.ANALYSIS_FEATURE_VECTOR_READY,
                source="technical_analysis_engine",
                correlation_id=correlation_id,
                payload=vector.to_dict(),
            )
        )
