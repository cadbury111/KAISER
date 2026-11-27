"""KAISER — Technical Analysis Engine.

Central analytical coordinator managing rolling bar histories, enforcing
market data quality gates, executing deterministic indicator calculations,
dispatching analysis events, and caching technical feature vectors.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

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
    extract_depth_features,
    extract_trade_flow_features,
    extract_synchronized_context_features,
)
from app.analysis.models import (
    SUPPORTED_TIMEFRAMES,
    TIMEFRAME_SECONDS,
    AnalysisConfig,
    IndicatorType,
    IndicatorValue,
    MultiTimeframeSyncState,
    TechnicalFeatureVector,
    TimeframeHealth,
    TimeframeHealthStatus,
    normalize_timeframe,
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
        provenance_tracker: Optional[Any] = None,
        run_registry: Optional[Any] = None,
        name: str = "technical_analysis_engine",
    ) -> None:
        self.name = name
        self.config = config or AnalysisConfig()
        self.event_bus = event_bus
        self.state_store = state_store
        self.provenance_tracker = provenance_tracker
        self.run_registry = run_registry

        self._lock = threading.RLock()
        self._series: Dict[Tuple[str, str], RollingBarSeries] = {}
        self._latest_vectors: Dict[Tuple[str, str], TechnicalFeatureVector] = {}
        self._stream_rejected_unhealthy: Dict[Tuple[str, str], int] = {}
        self._depth_analytics: Dict[str, Any] = {}
        self._trade_flow_analytics: Dict[str, Any] = {}
        self._synchronized_contexts: Dict[str, Any] = {}

        self._counters = {
            "bars_processed": 0,
            "bars_rejected_unhealthy": 0,
            "feature_vectors_computed": 0,
            "indicators_calculated": 0,
        }

    def set_depth_analytics(self, analytics: Any) -> None:
        """Store latest evaluated Level 2 depth analytics for a symbol."""
        if analytics is not None and hasattr(analytics, "symbol"):
            sym = str(analytics.symbol).strip().upper()
            with self._lock:
                self._depth_analytics[sym] = analytics

    def get_depth_analytics(self, symbol: str) -> Optional[Any]:
        """Retrieve latest Level 2 depth analytics for a symbol if present."""
        with self._lock:
            return self._depth_analytics.get(symbol.strip().upper())

    def get_depth_features(self, symbol: str) -> Dict[str, Any]:
        """Extract standard read-only depth features for a symbol."""
        analytics = self.get_depth_analytics(symbol)
        return extract_depth_features(analytics)

    def set_trade_flow_analytics(self, analytics: Any) -> None:
        """Store latest evaluated trade flow analytics for a symbol."""
        if analytics is not None and hasattr(analytics, "symbol"):
            sym = str(analytics.symbol).strip().upper()
            with self._lock:
                self._trade_flow_analytics[sym] = analytics

    def get_trade_flow_analytics(self, symbol: str) -> Optional[Any]:
        """Retrieve latest trade flow analytics for a symbol if present."""
        with self._lock:
            return self._trade_flow_analytics.get(symbol.strip().upper())

    def get_trade_flow_features(self, symbol: str) -> Dict[str, Any]:
        """Extract standard read-only trade flow features for a symbol."""
        analytics = self.get_trade_flow_analytics(symbol)
        return extract_trade_flow_features(analytics)

    def set_synchronized_context(self, context: Any) -> None:
        """Store latest evaluated synchronized multi-stream context for a symbol."""
        if context is not None and hasattr(context, "symbol"):
            sym = str(context.symbol).strip().upper()
            with self._lock:
                self._synchronized_contexts[sym] = context

    def get_synchronized_context(self, symbol: str) -> Optional[Any]:
        """Retrieve latest synchronized multi-stream context for a symbol if present."""
        with self._lock:
            return self._synchronized_contexts.get(symbol.strip().upper())

    def get_synchronized_context_features(self, symbol: str) -> Dict[str, Any]:
        """Extract standard read-only synchronization features for a symbol."""
        context = self.get_synchronized_context(symbol)
        return extract_synchronized_context_features(context)

    def _normalize_tf(self, timeframe: str) -> str:
        """Helper to obtain canonical timeframe with fallback."""
        try:
            return normalize_timeframe(timeframe)
        except Exception:
            return timeframe.strip()

    def _get_or_create_series(self, symbol: str, timeframe: str) -> RollingBarSeries:
        """Internal helper to retrieve or allocate a RollingBarSeries under lock."""
        norm_tf = self._normalize_tf(timeframe)
        key = (symbol.strip().upper(), norm_tf)
        if key not in self._series:
            self._series[key] = RollingBarSeries(
                symbol=symbol,
                timeframe=norm_tf,
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
        norm_tf = self._normalize_tf(bar.timeframe)
        sym_upper = bar.symbol.strip().upper()
        key = (sym_upper, norm_tf)

        # 1. Quality Gate Check
        if not safe_to_consume:
            with self._lock:
                self._counters["bars_rejected_unhealthy"] += 1
                self._stream_rejected_unhealthy[key] = self._stream_rejected_unhealthy.get(key, 0) + 1
            logger.warning(
                "Rejected unsafe bar for %s (%s) from analysis series [cid=%s]",
                bar.symbol,
                norm_tf,
                cid,
            )
            return None

        # 2. Add to Rolling Bar Series
        with self._lock:
            series = self._get_or_create_series(sym_upper, norm_tf)
            accepted = series.add_bar(bar)
            if not accepted:
                logger.debug(
                    "Bar for %s (%s) rejected (duplicate or out-of-order) [cid=%s]",
                    bar.symbol,
                    norm_tf,
                    cid,
                )
                return None

            self._counters["bars_processed"] += 1

            # 3. Compute Feature Vector
            vector = self._compute_feature_vector(series, correlation_id=cid)
            self._latest_vectors[key] = vector
            raw_key = (sym_upper, bar.timeframe.strip())
            if raw_key != key:
                self._latest_vectors[raw_key] = vector
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
        norm_tf = self._normalize_tf(timeframe)
        key = (symbol.strip().upper(), norm_tf)

        with self._lock:
            series = self._series.get(key) or self._series.get((symbol.strip().upper(), timeframe.strip()))
            if not series or series.count == 0:
                return None
            vector = self._compute_feature_vector(series, correlation_id=cid)
            self._latest_vectors[key] = vector
            self._counters["feature_vectors_computed"] += 1

        self._sync_state_store(vector)
        self._publish_events(vector, cid)

        if getattr(self, "run_registry", None) is not None:
            try:
                from app.analysis.runs.models import OperationType
                prov_ids = ()
                if getattr(self, "provenance_tracker", None) is not None:
                    p = self.provenance_tracker.get_latest(vector.symbol, vector.timeframe)
                    if p:
                        prov_ids = (p.provenance_id,)
                bars = self.get_history(vector.symbol, vector.timeframe)
                d_start = bars[0].timestamp if bars else None
                d_end = bars[-1].timestamp if bars else None
                run = self.run_registry.create_run(
                    operation_type=OperationType.TECHNICAL_ANALYSIS,
                    configuration=self.config,
                    input_provenance_ids=prov_ids,
                    symbol_universe=(vector.symbol,),
                    timeframe_universe=(vector.timeframe,),
                    input_record_count=vector.bar_count,
                    data_start=d_start,
                    data_end=d_end,
                )
                self.run_registry.start_run(run.run_id)
                self.run_registry.complete_run(
                    run.run_id,
                    result_id=f"vec_{vector.symbol}_{vector.timeframe}_{int(vector.timestamp)}",
                    result_type="TechnicalFeatureVector",
                )
            except Exception as exc:
                logger.warning("Failed to record analysis run in evaluate_feature_vector: %s", exc)

        return vector

    def get_feature_vector(
        self,
        symbol: str,
        timeframe: str = "1m",
    ) -> Optional[TechnicalFeatureVector]:
        """Retrieve the latest cached TechnicalFeatureVector."""
        norm_tf = self._normalize_tf(timeframe)
        key = (symbol.strip().upper(), norm_tf)
        raw_key = (symbol.strip().upper(), timeframe.strip())
        with self._lock:
            return self._latest_vectors.get(key) or self._latest_vectors.get(raw_key)

    def get_latest_vector(
        self,
        symbol: str,
        timeframe: str = "1m",
    ) -> Optional[TechnicalFeatureVector]:
        """Alias for get_feature_vector."""
        return self.get_feature_vector(symbol, timeframe)

    def get_series(
        self,
        symbol: str,
        timeframe: str = "1m",
    ) -> Optional[RollingBarSeries]:
        """Retrieve the RollingBarSeries container for a stream."""
        norm_tf = self._normalize_tf(timeframe)
        key = (symbol.strip().upper(), norm_tf)
        raw_key = (symbol.strip().upper(), timeframe.strip())
        with self._lock:
            return self._series.get(key) or self._series.get(raw_key)

    def reset_stream(self, symbol: str, timeframe: str = "1m") -> None:
        """Reset rolling series, feature vectors, and quarantine count for a stream."""
        norm_tf = self._normalize_tf(timeframe)
        sym_upper = symbol.strip().upper()
        key = (sym_upper, norm_tf)
        raw_key = (sym_upper, timeframe.strip())
        with self._lock:
            series = self._series.pop(key, None)
            if series:
                series.clear()
            raw_series = self._series.pop(raw_key, None)
            if raw_series and raw_series is not series:
                raw_series.clear()
            self._latest_vectors.pop(key, None)
            self._latest_vectors.pop(raw_key, None)
            self._stream_rejected_unhealthy.pop(key, None)
            self._stream_rejected_unhealthy.pop(raw_key, None)

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
        norm_tf = self._normalize_tf(timeframe)
        key = (symbol.strip().upper(), norm_tf)
        raw_key = (symbol.strip().upper(), timeframe.strip())
        with self._lock:
            series = self._series.get(key) or self._series.get(raw_key)
            if not series:
                return []
            return series.get_bars()

    def get_available_timeframes(self, symbol: str) -> List[str]:
        """Return list of canonical timeframes with active bars for the given symbol."""
        sym_upper = symbol.strip().upper()
        available: List[str] = []
        with self._lock:
            for (s, tf), series in self._series.items():
                if s == sym_upper and series.count > 0:
                    norm_tf = self._normalize_tf(tf)
                    if norm_tf not in available:
                        available.append(norm_tf)
        return sorted(available, key=lambda t: SUPPORTED_TIMEFRAMES.index(t) if t in SUPPORTED_TIMEFRAMES else 99)

    def get_all_feature_vectors(self, symbol: str) -> Dict[str, TechnicalFeatureVector]:
        """Return a mapping of timeframe -> TechnicalFeatureVector for all available streams."""
        sym_upper = symbol.strip().upper()
        res: Dict[str, TechnicalFeatureVector] = {}
        with self._lock:
            for (s, tf), vec in self._latest_vectors.items():
                if s == sym_upper:
                    norm_tf = self._normalize_tf(tf)
                    if norm_tf not in res:
                        res[norm_tf] = vec
        return res

    def check_timeframe_health(
        self,
        symbol: str,
        timeframe: str,
        reference_time: Optional[float] = None,
        stale_threshold_seconds: Optional[float] = None,
    ) -> TimeframeHealth:
        """Evaluate data quality and synchronization health for a specific (symbol, timeframe)."""
        sym_upper = symbol.strip().upper()
        norm_tf = self._normalize_tf(timeframe)
        key = (sym_upper, norm_tf)
        now = time.time() if reference_time is None else reference_time

        with self._lock:
            series = self._series.get(key) or self._series.get((sym_upper, timeframe.strip()))
            rejected_unhealthy = self._stream_rejected_unhealthy.get(key, 0)

        warnings: List[str] = []
        if rejected_unhealthy > 0:
            warnings.append(f"Stream had {rejected_unhealthy} quarantined bar(s) rejected by quality gate.")

        if not series or series.count == 0:
            if rejected_unhealthy > 0:
                status = TimeframeHealthStatus.UNHEALTHY
            else:
                status = TimeframeHealthStatus.EMPTY
            warnings.append(f"No bar data available for {sym_upper} ({norm_tf}).")
            return TimeframeHealth(
                symbol=sym_upper,
                timeframe=norm_tf,
                status=status,
                bar_count=0,
                latest_timestamp=None,
                age_seconds=None,
                missing_intervals_count=0,
                unhealthy_bars_rejected=rejected_unhealthy,
                warnings=tuple(warnings),
                is_valid_for_analysis=False,
            )

        bar_count = series.count
        latest_bar = series.latest_bar
        latest_ts = latest_bar.timestamp if latest_bar else None
        age = max(0.0, now - latest_ts) if latest_ts is not None else None
        missing_gaps = series.missing_intervals_count

        if missing_gaps > 0:
            warnings.append(f"Detected {missing_gaps} missing bar interval(s) in chronological history.")

        # Freshness determination
        interval_sec = TIMEFRAME_SECONDS.get(norm_tf, 60.0)
        thresh = stale_threshold_seconds or max(120.0, interval_sec * 2.5)
        is_stale = False
        if age is not None and age > thresh:
            is_stale = True
            warnings.append(
                f"Data is stale: latest bar is {age:.1f}s old (freshness threshold is {thresh:.1f}s)."
            )

        # Health status resolution
        if is_stale:
            status = TimeframeHealthStatus.STALE
            is_valid = False
        elif bar_count < 15:
            status = TimeframeHealthStatus.INSUFFICIENT_DATA
            warnings.append(
                f"Insufficient bar history ({bar_count} bars); minimum 15 bars required for reliable indicator analysis."
            )
            is_valid = False
        elif rejected_unhealthy > 0 or missing_gaps > 0:
            status = TimeframeHealthStatus.DEGRADED
            is_valid = True
        else:
            status = TimeframeHealthStatus.HEALTHY
            is_valid = True

        return TimeframeHealth(
            symbol=sym_upper,
            timeframe=norm_tf,
            status=status,
            bar_count=bar_count,
            latest_timestamp=latest_ts,
            age_seconds=age,
            missing_intervals_count=missing_gaps,
            unhealthy_bars_rejected=rejected_unhealthy,
            warnings=tuple(warnings),
            is_valid_for_analysis=is_valid,
        )

    def get_multi_timeframe_sync(
        self,
        symbol: str,
        timeframes: Optional[Sequence[str]] = None,
        reference_time: Optional[float] = None,
    ) -> MultiTimeframeSyncState:
        """Evaluate synchronization and completeness across all target timeframes."""
        sym_upper = symbol.strip().upper()
        target_tfs = tuple(timeframes) if timeframes else SUPPORTED_TIMEFRAMES
        now = time.time() if reference_time is None else reference_time

        tf_health_map: Dict[str, TimeframeHealth] = {}
        available_tfs: List[str] = []
        missing_tfs: List[str] = []
        stale_tfs: List[str] = []
        unhealthy_tfs: List[str] = []
        cross_warnings: List[str] = []

        for tf in target_tfs:
            norm_tf = self._normalize_tf(tf)
            h = self.check_timeframe_health(sym_upper, norm_tf, reference_time=now)
            tf_health_map[norm_tf] = h

            if h.bar_count > 0:
                available_tfs.append(norm_tf)
            else:
                missing_tfs.append(norm_tf)
                cross_warnings.append(f"Timeframe {norm_tf} is missing (0 bars).")

            if h.status == TimeframeHealthStatus.STALE:
                stale_tfs.append(norm_tf)
                cross_warnings.append(f"Timeframe {norm_tf} is stale.")
            elif h.status in (TimeframeHealthStatus.UNHEALTHY, TimeframeHealthStatus.DEGRADED):
                unhealthy_tfs.append(norm_tf)
                cross_warnings.append(f"Timeframe {norm_tf} is {h.status.value.lower()}.")

        all_present = len(missing_tfs) == 0
        no_stale = len(stale_tfs) == 0
        no_unhealthy = len(unhealthy_tfs) == 0
        all_valid = all(h.is_valid_for_analysis for h in tf_health_map.values())
        is_fully_synchronized = all_present and no_stale and no_unhealthy and all_valid

        # Sync quality score: [0.0, 1.0]
        if not target_tfs:
            quality = 0.0
        else:
            healthy_count = sum(1 for h in tf_health_map.values() if h.status == TimeframeHealthStatus.HEALTHY)
            degraded_count = sum(1 for h in tf_health_map.values() if h.status == TimeframeHealthStatus.DEGRADED)
            quality = max(0.0, min(1.0, (healthy_count * 1.0 + degraded_count * 0.6) / len(target_tfs)))

        sync_state = MultiTimeframeSyncState(
            symbol=sym_upper,
            reference_timestamp=now,
            timeframes=tf_health_map,
            available_timeframes=tuple(available_tfs),
            missing_timeframes=tuple(missing_tfs),
            stale_timeframes=tuple(stale_tfs),
            unhealthy_timeframes=tuple(unhealthy_tfs),
            is_fully_synchronized=is_fully_synchronized,
            sync_quality=quality,
            warnings=tuple(cross_warnings),
        )

        if getattr(self, "run_registry", None) is not None:
            try:
                from app.analysis.runs.models import OperationType
                prov_ids = []
                if getattr(self, "provenance_tracker", None) is not None:
                    for tf in target_tfs:
                        p = self.provenance_tracker.get_latest(sym_upper, tf)
                        if p:
                            prov_ids.append(p.provenance_id)
                run = self.run_registry.create_run(
                    operation_type=OperationType.MULTI_TIMEFRAME_ANALYSIS,
                    configuration={"target_timeframes": list(target_tfs)},
                    input_provenance_ids=prov_ids,
                    symbol_universe=(sym_upper,),
                    timeframe_universe=tuple(target_tfs),
                )
                self.run_registry.start_run(run.run_id)
                self.run_registry.complete_run(
                    run.run_id,
                    result_id=f"sync_{sym_upper}_{int(now)}",
                    result_type="MultiTimeframeSyncState",
                )
            except Exception as exc:
                logger.warning("Failed to record analysis run in synchronize_timeframes: %s", exc)

        return sync_state

    def reset(self, symbol: Optional[str] = None, timeframe: Optional[str] = None) -> None:
        """Reset historical series and cached vectors."""
        with self._lock:
            if symbol and timeframe:
                norm_tf = self._normalize_tf(timeframe)
                key = (symbol.strip().upper(), norm_tf)
                raw_key = (symbol.strip().upper(), timeframe.strip())
                for k in (key, raw_key):
                    if k in self._series:
                        self._series[k].clear()
                    self._latest_vectors.pop(k, None)
                    self._stream_rejected_unhealthy.pop(k, None)
            elif symbol:
                sym_upper = symbol.strip().upper()
                keys_to_remove = [k for k in self._series if k[0] == sym_upper]
                for k in keys_to_remove:
                    self._series[k].clear()
                    self._latest_vectors.pop(k, None)
                    self._stream_rejected_unhealthy.pop(k, None)
            else:
                for s in self._series.values():
                    s.clear()
                self._series.clear()
                self._latest_vectors.clear()
                self._stream_rejected_unhealthy.clear()

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
        self.state_store.set(
            "analysis_counters",
            self.get_counters(),
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
